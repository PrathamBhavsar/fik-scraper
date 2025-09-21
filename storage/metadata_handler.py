"""
Metadata Handler for FikFap Scraper - Phase 4
JSON-based metadata persistence and processing history tracking
"""
import asyncio
import aiofiles
import json
import uuid
from pathlib import Path
from typing import Optional, Dict, Any, List, Set
from datetime import datetime, timedelta
import time

from core.config import config
from core.exceptions import *
from data.models import (
    VideoPost, StorageMetadata, ProcessingRecord, ProcessingStatus, 
    DirectoryStructure, DownloadJob, DownloadStatus
)
from utils.logger import setup_logger
from utils.helpers import format_bytes

class MetadataHandler:
    """
    Comprehensive metadata handler for processing tracking and persistence

    Features:
    - JSON-based metadata persistence
    - Processing history tracking
    - Duplicate detection and prevention
    - Processing statistics and analytics
    """

    def __init__(self):
        """Initialize metadata handler"""
        self.logger = setup_logger("metadata_handler", config.log_level)

        # Configuration
        self.base_path = Path(config.get('storage.base_path', './downloads'))

        # Internal paths
        self.metadata_dir = self.base_path / '.metadata'
        self.processing_log_file = self.metadata_dir / 'processing_log.json'
        self.processed_posts_file = self.metadata_dir / 'processed_posts.json'
        self.download_history_file = self.metadata_dir / 'download_history.json'

        # In-memory caches
        self.processed_posts_cache: Set[int] = set()
        self.processing_records_cache: Dict[int, ProcessingRecord] = {}
        self._cache_loaded = False

        # Initialize metadata system
        self._initialize_metadata_system()

        self.logger.info("MetadataHandler initialized")

    def _initialize_metadata_system(self):
        """Initialize metadata directory structure and files"""
        try:
            # Create metadata directory
            self.metadata_dir.mkdir(parents=True, exist_ok=True)

            # Initialize files if they don't exist
            for file_path in [self.processing_log_file, self.processed_posts_file, self.download_history_file]:
                if not file_path.exists():
                    with open(file_path, 'w') as f:
                        json.dump([], f)

            self.logger.debug("Metadata system initialized")

        except Exception as e:
            self.logger.error(f"Error initializing metadata system: {e}")
            raise Exception(f"Cannot initialize metadata system: {e}")

    async def load_processed_posts_cache(self) -> Set[int]:
        """Load processed posts cache from file"""
        if self._cache_loaded:
            return self.processed_posts_cache

        try:
            if self.processed_posts_file.exists():
                async with aiofiles.open(self.processed_posts_file, 'r') as f:
                    content = await f.read()
                    processed_list = json.loads(content)
                    self.processed_posts_cache = set(processed_list)

                self.logger.debug(f"Loaded {len(self.processed_posts_cache)} processed posts from cache")

            self._cache_loaded = True
            return self.processed_posts_cache

        except Exception as e:
            self.logger.error(f"Error loading processed posts cache: {e}")
            return set()

    async def save_processed_posts_cache(self):
        """Save processed posts cache to file"""
        try:
            async with aiofiles.open(self.processed_posts_file, 'w') as f:
                await f.write(json.dumps(list(self.processed_posts_cache), indent=2))

            self.logger.debug(f"Saved {len(self.processed_posts_cache)} processed posts to cache")

        except Exception as e:
            self.logger.error(f"Error saving processed posts cache: {e}")
            raise Exception(f"Cannot save processed posts cache: {e}")

    async def is_post_processed(self, post_id: int) -> bool:
        """Check if a post has been processed"""
        await self.load_processed_posts_cache()
        return post_id in self.processed_posts_cache

    async def mark_post_processed(self, post_id: int):
        """Mark a post as processed"""
        await self.load_processed_posts_cache()
        self.processed_posts_cache.add(post_id)
        await self.save_processed_posts_cache()

        self.logger.debug(f"Marked post {post_id} as processed")

    async def create_processing_record(self, video_post: VideoPost) -> ProcessingRecord:
        """Create a new processing record"""
        try:
            processing_id = str(uuid.uuid4())

            record = ProcessingRecord(
                postId=video_post.postId,
                processingId=processing_id,
                status=ProcessingStatus.NEW,
                startedAt=datetime.now()
            )

            # Cache the record
            self.processing_records_cache[video_post.postId] = record

            # Save to persistent storage
            await self._save_processing_record(record)

            self.logger.info(f"Created processing record for post {video_post.postId}")
            return record

        except Exception as e:
            self.logger.error(f"Error creating processing record: {e}")
            raise Exception(f"Cannot create processing record: {e}")

    async def update_processing_record(
        self, 
        post_id: int, 
        status: ProcessingStatus,
        error_message: Optional[str] = None,
        download_jobs: Optional[List[str]] = None,
        stored_files: Optional[List[str]] = None
    ) -> Optional[ProcessingRecord]:
        """Update processing record status and information"""
        try:
            record = self.processing_records_cache.get(post_id)
            if not record:
                # Try to load from persistent storage
                record = await self._load_processing_record(post_id)
                if not record:
                    self.logger.warning(f"No processing record found for post {post_id}")
                    return None

            # Update record
            record.status = status
            if error_message:
                record.lastError = error_message
            if download_jobs:
                record.downloadJobs.extend(download_jobs)
            if stored_files:
                record.storedFiles.extend(stored_files)

            # Update completion time for completed/failed status
            if status in [ProcessingStatus.COMPLETED, ProcessingStatus.FAILED]:
                record.completedAt = datetime.now()

                # Mark post as processed if completed
                if status == ProcessingStatus.COMPLETED:
                    await self.mark_post_processed(post_id)

            # Update cache and persistent storage
            self.processing_records_cache[post_id] = record
            await self._save_processing_record(record)

            self.logger.debug(f"Updated processing record for post {post_id}: {status}")
            return record

        except Exception as e:
            self.logger.error(f"Error updating processing record: {e}")
            raise Exception(f"Cannot update processing record: {e}")

    async def _save_processing_record(self, record: ProcessingRecord):
        """Save processing record to persistent storage"""
        try:
            # Load existing records
            records = []
            if self.processing_log_file.exists():
                async with aiofiles.open(self.processing_log_file, 'r') as f:
                    content = await f.read()
                    records = json.loads(content)

            # Update or add record
            record_dict = record.dict()
            record_dict['startedAt'] = record.startedAt.isoformat()
            if record.completedAt:
                record_dict['completedAt'] = record.completedAt.isoformat()

            # Find existing record and update, or append new
            found = False
            for i, existing_record in enumerate(records):
                if existing_record['postId'] == record.postId:
                    records[i] = record_dict
                    found = True
                    break

            if not found:
                records.append(record_dict)

            # Save back to file
            async with aiofiles.open(self.processing_log_file, 'w') as f:
                await f.write(json.dumps(records, indent=2))

        except Exception as e:
            self.logger.error(f"Error saving processing record: {e}")
            raise Exception(f"Cannot save processing record: {e}")

    async def _load_processing_record(self, post_id: int) -> Optional[ProcessingRecord]:
        """Load processing record from persistent storage"""
        try:
            if not self.processing_log_file.exists():
                return None

            async with aiofiles.open(self.processing_log_file, 'r') as f:
                content = await f.read()
                records = json.loads(content)

            # Find record by post ID
            for record_dict in records:
                if record_dict['postId'] == post_id:
                    # Convert datetime strings back to datetime objects
                    record_dict['startedAt'] = datetime.fromisoformat(record_dict['startedAt'])
                    if record_dict.get('completedAt'):
                        record_dict['completedAt'] = datetime.fromisoformat(record_dict['completedAt'])

                    record = ProcessingRecord(**record_dict)
                    self.processing_records_cache[post_id] = record
                    return record

            return None

        except Exception as e:
            self.logger.error(f"Error loading processing record: {e}")
            return None

    async def save_video_metadata(
        self, 
        metadata: StorageMetadata, 
        directory_structure: DirectoryStructure
    ) -> bool:
        """Save video metadata to JSON file"""
        try:
            metadata_path = Path(directory_structure.metadataPath)

            # Prepare metadata dictionary
            metadata_dict = metadata.dict()

            # Convert datetime to ISO format
            metadata_dict['publishedAt'] = metadata.publishedAt.isoformat()
            metadata_dict['downloadedAt'] = metadata.downloadedAt.isoformat()

            # Add directory structure info
            metadata_dict['directoryStructure'] = directory_structure.dict()
            metadata_dict['savedAt'] = datetime.now().isoformat()

            # Save to file atomically
            temp_path = metadata_path.with_suffix('.tmp')

            async with aiofiles.open(temp_path, 'w') as f:
                await f.write(json.dumps(metadata_dict, indent=2))

            # Atomic rename
            temp_path.rename(metadata_path)

            self.logger.debug(f"Saved metadata: {metadata_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving video metadata: {e}")
            return False

    async def load_video_metadata(self, metadata_path: Path) -> Optional[StorageMetadata]:
        """Load video metadata from JSON file"""
        try:
            if not metadata_path.exists():
                return None

            async with aiofiles.open(metadata_path, 'r') as f:
                content = await f.read()
                metadata_dict = json.loads(content)

            # Convert ISO format back to datetime
            metadata_dict['publishedAt'] = datetime.fromisoformat(metadata_dict['publishedAt'])
            metadata_dict['downloadedAt'] = datetime.fromisoformat(metadata_dict['downloadedAt'])

            # Remove directory structure (not part of StorageMetadata)
            metadata_dict.pop('directoryStructure', None)
            metadata_dict.pop('savedAt', None)

            metadata = StorageMetadata(**metadata_dict)
            return metadata

        except Exception as e:
            self.logger.error(f"Error loading video metadata: {e}")
            return None

    async def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics"""
        try:
            await self.load_processed_posts_cache()

            stats = {
                'total_processed': len(self.processed_posts_cache),
                'processing_records': len(self.processing_records_cache),
                'status_breakdown': {status.value: 0 for status in ProcessingStatus},
                'recent_activity': [],
                'success_rate': 0.0,
                'last_updated': datetime.now().isoformat()
            }

            # Analyze processing records
            successful_count = 0
            failed_count = 0

            for record in self.processing_records_cache.values():
                stats['status_breakdown'][record.status.value] += 1

                if record.is_completed:
                    successful_count += 1
                elif record.is_failed:
                    failed_count += 1

                # Recent activity (last 24 hours)
                if record.startedAt > datetime.now() - timedelta(hours=24):
                    stats['recent_activity'].append({
                        'postId': record.postId,
                        'status': record.status.value,
                        'startedAt': record.startedAt.isoformat()
                    })

            # Calculate success rate
            total_completed = successful_count + failed_count
            if total_completed > 0:
                stats['success_rate'] = (successful_count / total_completed) * 100

            # Sort recent activity by start time
            stats['recent_activity'].sort(key=lambda x: x['startedAt'], reverse=True)
            stats['recent_activity'] = stats['recent_activity'][:50]  # Last 50 activities

            return stats

        except Exception as e:
            self.logger.error(f"Error getting processing statistics: {e}")
            return {'error': str(e)}

    async def get_duplicate_posts(self) -> List[int]:
        """Get list of posts that would be duplicates"""
        try:
            await self.load_processed_posts_cache()
            return list(self.processed_posts_cache)
        except Exception as e:
            self.logger.error(f"Error getting duplicate posts: {e}")
            return []

    async def cleanup_old_records(self, days_to_keep: int = 30) -> int:
        """Clean up old processing records"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            removed_count = 0

            # Clean up in-memory cache
            to_remove = []
            for post_id, record in self.processing_records_cache.items():
                if record.startedAt < cutoff_date:
                    to_remove.append(post_id)

            for post_id in to_remove:
                del self.processing_records_cache[post_id]
                removed_count += 1

            # Clean up persistent storage
            if self.processing_log_file.exists():
                async with aiofiles.open(self.processing_log_file, 'r') as f:
                    content = await f.read()
                    records = json.loads(content)

                # Filter out old records
                filtered_records = []
                for record_dict in records:
                    started_at = datetime.fromisoformat(record_dict['startedAt'])
                    if started_at >= cutoff_date:
                        filtered_records.append(record_dict)
                    else:
                        removed_count += 1

                # Save filtered records back
                async with aiofiles.open(self.processing_log_file, 'w') as f:
                    await f.write(json.dumps(filtered_records, indent=2))

            self.logger.info(f"Cleaned up {removed_count} old processing records")
            return removed_count

        except Exception as e:
            self.logger.error(f"Error cleaning up old records: {e}")
            return 0
