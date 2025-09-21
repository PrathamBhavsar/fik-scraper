"""
File Manager for FikFap Scraper - Phase 4
Advanced file system operations with atomic operations and proper error handling
"""
import asyncio
import aiofiles
import shutil
import json
import hashlib
import time
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple
from datetime import datetime
import uuid
import tempfile
import re

from core.config import config
from core.exceptions import *
from data.models import VideoPost, StorageMetadata, DirectoryStructure, CleanupSummary, StorageFormat, ProcessingStatus, VideoCodec
from utils.logger import setup_logger
from utils.helpers import sanitize_filename, format_bytes

class FileManager:
    """
    Advanced file manager for storage operations

    Features:
    - Atomic file operations
    - Directory structure management 
    - Duplicate handling
    - File integrity verification
    - Cleanup operations
    - Cross-platform path handling
    """

    def __init__(self):
        """Initialize file manager with configuration"""
        self.logger = setup_logger("file_manager", config.log_level)

        # Configuration from settings
        self.base_path = Path(config.get('storage.base_path', './downloads'))
        self.organize_by_author = config.get('storage.organize_by_author', True)
        self.organize_by_date = config.get('storage.organize_by_date', False)
        self.filename_template = config.get('storage.filename_template', '{author}_{title}_{resolution}_{postId}')
        self.max_filename_length = config.get('storage.max_filename_length', 200)
        self.sanitize_filenames = config.get('storage.sanitize_filenames', True)

        # Initialize base directory
        self._ensure_base_directory()

        self.logger.info(f"FileManager initialized - Base path: {self.base_path}")

    def _ensure_base_directory(self):
        """Ensure base storage directory exists"""
        try:
            self.base_path.mkdir(parents=True, exist_ok=True)

            # Test write permissions
            test_file = self.base_path / '.write_test'
            try:
                test_file.touch()
                test_file.unlink()
            except Exception as e:
                raise Exception(f"No write permission for storage directory: {self.base_path}")

        except Exception as e:
            raise Exception(f"Cannot create or access storage directory: {e}")

    async def create_directory_structure(self, video_post: VideoPost) -> DirectoryStructure:
        """
        Create organized directory structure for a video post

        Args:
            video_post: Video post to create structure for

        Returns:
            DirectoryStructure object with all paths
        """
        try:
            base_path = self.base_path

            # Author organization
            author_path = None
            if self.organize_by_author and video_post.author:
                author_name = self._sanitize_path_component(video_post.author.username)
                author_path = base_path / author_name
                base_path = author_path

            # Date organization
            date_path = None
            if self.organize_by_date:
                date_str = video_post.publishedAt.strftime('%Y-%m-%d')
                date_path = base_path / date_str
                base_path = date_path

            # Post-specific directory
            post_dir_name = f"post_{video_post.postId}"
            post_path = base_path / post_dir_name

            # M3U8 subdirectory (following postId/m3u8/quality structure)
            m3u8_path = post_path / "m3u8"

            # Quality-specific subdirectories
            quality_paths = {}
            for quality in video_post.availableQualities:
                quality_dir = self._sanitize_path_component(f"{quality.resolution}_{quality.codec.value}")
                quality_paths[quality.resolution] = str(m3u8_path / quality_dir)

            # Metadata path
            metadata_path = post_path / "metadata.json"

            # Create directory structure
            structure = DirectoryStructure(
                postId=video_post.postId,
                basePath=str(self.base_path),
                authorPath=str(author_path) if author_path else None,
                datePath=str(date_path) if date_path else None,
                postPath=str(post_path),
                m3u8Path=str(m3u8_path),
                qualityPaths=quality_paths,
                metadataPath=str(metadata_path)
            )

            # Create physical directories
            await self._create_physical_directories(structure)

            self.logger.info(f"Created directory structure for post {video_post.postId}")
            return structure

        except Exception as e:
            self.logger.error(f"Error creating directory structure: {e}")
            raise Exception(f"Failed to create directory structure: {e}")

    async def _create_physical_directories(self, structure: DirectoryStructure):
        """Create physical directory structure"""
        directories_to_create = [
            Path(structure.postPath),
            Path(structure.m3u8Path)
        ]

        # Add quality directories
        for quality_path in structure.qualityPaths.values():
            directories_to_create.append(Path(quality_path))

        # Create directories
        for directory in directories_to_create:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                self.logger.debug(f"Created directory: {directory}")
            except Exception as e:
                raise Exception(f"Cannot create directory {directory}: {e}")

    async def store_video_file(
        self,
        source_path: Path,
        target_path: Path,
        video_post: VideoPost,
        quality: str,
        codec: str,
        move_file: bool = True
    ) -> StorageMetadata:
        """
        Store video file with metadata and integrity verification

        Args:
            source_path: Path to source file
            target_path: Target storage path
            video_post: Associated video post
            quality: Video quality
            codec: Video codec
            move_file: Whether to move (True) or copy (False) the file

        Returns:
            StorageMetadata object
        """
        try:
            if not source_path.exists():
                raise Exception(f"Source file does not exist: {source_path}")

            # Ensure target directory exists
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Handle duplicates by skipping if exists
            if target_path.exists():
                self.logger.info(f"File already exists, skipping: {target_path.name}")
                # Return metadata for existing file
                file_size = target_path.stat().st_size
                checksum = await self._calculate_file_checksum(target_path)
            else:
                # Calculate source file checksum
                source_checksum = await self._calculate_file_checksum(source_path)
                source_size = source_path.stat().st_size

                # Atomic file operation
                temp_path = target_path.with_suffix('.tmp')

                try:
                    if move_file:
                        # Move file atomically
                        shutil.move(str(source_path), str(temp_path))
                    else:
                        # Copy file
                        await self._copy_file_async(source_path, temp_path)

                    # Verify integrity
                    target_checksum = await self._calculate_file_checksum(temp_path)
                    if source_checksum != target_checksum:
                        raise Exception("File integrity check failed - checksums don't match")

                    # Atomic rename to final location
                    temp_path.rename(target_path)

                    file_size = source_size
                    checksum = target_checksum

                    self.logger.info(f"Stored video file: {target_path.name} ({format_bytes(file_size)})")

                except Exception as e:
                    # Cleanup temp file on error
                    if temp_path.exists():
                        try:
                            temp_path.unlink()
                        except Exception:
                            pass
                    raise

            # Create metadata
            metadata = StorageMetadata(
                postId=video_post.postId,
                mediaId=video_post.mediaId,
                title=video_post.label,
                author=video_post.author.username if video_post.author else 'Unknown',
                authorId=video_post.author.userId if video_post.author else 'unknown',
                publishedAt=video_post.publishedAt,
                fileSize=file_size,
                filePath=str(target_path.relative_to(self.base_path)),
                fileName=target_path.name,
                quality=quality,
                resolution=quality,
                codec=VideoCodec(codec) if codec in [c.value for c in VideoCodec] else VideoCodec.UNKNOWN,
                duration=video_post.duration,
                format=StorageFormat.MP4,
                checksum=checksum,
                tags=video_post.hashtags,
                processingStatus=ProcessingStatus.COMPLETED
            )

            return metadata

        except Exception as e:
            self.logger.error(f"Error storing video file: {e}")
            raise Exception(f"Failed to store video file: {e}")

    async def _copy_file_async(self, source: Path, target: Path, chunk_size: int = 64 * 1024):
        """Copy file asynchronously"""
        async with aiofiles.open(source, 'rb') as src:
            async with aiofiles.open(target, 'wb') as dst:
                while True:
                    chunk = await src.read(chunk_size)
                    if not chunk:
                        break
                    await dst.write(chunk)

    async def _calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of file asynchronously"""
        hash_md5 = hashlib.md5()

        async with aiofiles.open(file_path, 'rb') as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                hash_md5.update(chunk)

        return hash_md5.hexdigest()

    def _sanitize_path_component(self, component: str) -> str:
        """Sanitize a path component for filesystem safety"""
        if not self.sanitize_filenames:
            return component

        # Use existing sanitize_filename function
        sanitized = sanitize_filename(component)

        # Additional path-specific sanitization
        sanitized = sanitized.replace('..', '_')  # Prevent directory traversal
        sanitized = sanitized.strip('.')  # Remove leading/trailing dots

        # Ensure not empty
        if not sanitized:
            sanitized = 'unnamed'

        return sanitized

    def generate_filename(self, video_post: VideoPost, quality: str, codec: str, format: str = 'mp4') -> str:
        """Generate filename based on template and video metadata"""
        try:
            # Prepare template variables
            template_vars = {
                'title': video_post.label,
                'author': video_post.author.username if video_post.author else 'Unknown',
                'postId': video_post.postId,
                'quality': quality,
                'resolution': quality,
                'codec': codec,
                'date': video_post.publishedAt.strftime('%Y%m%d'),
                'timestamp': int(video_post.publishedAt.timestamp())
            }

            # Sanitize template variables
            if self.sanitize_filenames:
                for key, value in template_vars.items():
                    if isinstance(value, str):
                        template_vars[key] = self._sanitize_path_component(value)

            # Generate filename from template
            try:
                filename = self.filename_template.format(**template_vars)
            except KeyError as e:
                self.logger.warning(f"Invalid filename template variable: {e}")
                filename = f"{template_vars['author']}_{template_vars['title']}_{template_vars['quality']}"

            # Add extension
            filename += f".{format}"

            # Enforce length limit
            if len(filename) > self.max_filename_length:
                # Truncate title while preserving important info
                excess = len(filename) - self.max_filename_length
                title = template_vars['title']
                if len(title) > excess + 20:  # Keep at least 20 chars of title
                    template_vars['title'] = title[:-(excess + 3)] + "..."
                    filename = self.filename_template.format(**template_vars) + f".{format}"
                else:
                    # Fallback to simple filename
                    filename = f"{template_vars['postId']}_{template_vars['quality']}.{format}"

            return filename

        except Exception as e:
            self.logger.error(f"Error generating filename: {e}")
            # Ultimate fallback
            return f"video_{video_post.postId}_{quality}.{format}"

    async def cleanup_incomplete_downloads(self, directory: Optional[Path] = None) -> CleanupSummary:
        """Clean up incomplete downloads and temporary files"""
        start_time = time.time()
        cleanup_dir = directory or self.base_path

        summary = CleanupSummary(
            operation="cleanup_incomplete",
            timestamp=datetime.now()
        )

        try:
            self.logger.info(f"Starting cleanup of incomplete downloads in: {cleanup_dir}")

            # Find temporary files
            temp_patterns = ['*.tmp', '*.partial', '*.downloading']
            temp_files = []

            for pattern in temp_patterns:
                temp_files.extend(cleanup_dir.rglob(pattern))

            # Clean up temp files
            for temp_file in temp_files:
                try:
                    if temp_file.is_file():
                        file_size = temp_file.stat().st_size
                        temp_file.unlink()
                        summary.filesRemoved += 1
                        summary.bytesFreed += file_size
                        self.logger.debug(f"Removed temp file: {temp_file}")
                except Exception as e:
                    summary.errors.append(f"Cannot remove {temp_file}: {e}")
                    self.logger.warning(f"Cannot remove temp file {temp_file}: {e}")

            # Find and remove empty directories
            await self._cleanup_empty_directories(cleanup_dir, summary)

            summary.duration = time.time() - start_time

            self.logger.info(f"Cleanup completed: {summary.filesRemoved} files removed, "
                           f"{format_bytes(summary.bytesFreed)} freed, "
                           f"{summary.directoriesRemoved} directories removed")

            return summary

        except Exception as e:
            summary.errors.append(f"Cleanup operation failed: {e}")
            summary.duration = time.time() - start_time
            self.logger.error(f"Cleanup operation failed: {e}")
            return summary

    async def _cleanup_empty_directories(self, base_dir: Path, summary: CleanupSummary):
        """Remove empty directories recursively"""
        try:
            # Get all directories, sorted by depth (deepest first)
            all_dirs = [d for d in base_dir.rglob('*') if d.is_dir()]
            all_dirs.sort(key=lambda x: len(x.parts), reverse=True)

            for directory in all_dirs:
                try:
                    # Skip base directory
                    if directory == base_dir:
                        continue

                    # Check if directory is empty
                    if not any(directory.iterdir()):
                        directory.rmdir()
                        summary.directoriesRemoved += 1
                        self.logger.debug(f"Removed empty directory: {directory}")

                except OSError as e:
                    # Directory not empty or permission error
                    self.logger.debug(f"Cannot remove directory {directory}: {e}")
                except Exception as e:
                    summary.errors.append(f"Cannot remove directory {directory}: {e}")

        except Exception as e:
            summary.errors.append(f"Error during directory cleanup: {e}")

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        try:
            stats = {
                'base_path': str(self.base_path),
                'total_files': 0,
                'total_size': 0,
                'directories': 0,
                'file_types': {},
                'last_updated': datetime.now().isoformat()
            }

            # Walk directory tree
            for item in self.base_path.rglob('*'):
                if item.is_file():
                    stats['total_files'] += 1
                    file_size = item.stat().st_size
                    stats['total_size'] += file_size

                    # Track file types
                    ext = item.suffix.lower()
                    if ext not in stats['file_types']:
                        stats['file_types'][ext] = {'count': 0, 'size': 0}
                    stats['file_types'][ext]['count'] += 1
                    stats['file_types'][ext]['size'] += file_size

                elif item.is_dir():
                    stats['directories'] += 1

            self.logger.debug(f"Storage stats calculated: {stats['total_files']} files, {format_bytes(stats['total_size'])}")
            return stats

        except Exception as e:
            self.logger.error(f"Error calculating storage stats: {e}")
            return {'error': str(e)}
