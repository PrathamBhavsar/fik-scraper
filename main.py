#!/usr/bin/env python3
"""
FikFap Scraper - Enhanced Main Entry Point with Phase 4 Storage Integration
Complete integration of all phases with storage & file management
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from core.config import config
from core.base_scraper import BaseScraper
from core.exceptions import *
from storage.file_manager import FileManager
from storage.metadata_handler import MetadataHandler
from utils.monitoring import SystemMonitor, DiskMonitor
from utils.logger import setup_logger
from data.models import VideoPost, ProcessingStatus
from typing import Optional, List, Dict, Any, Union, Literal


class FikFapScraperPhase4:
    """
    Complete FikFap scraper with Phase 4 storage integration

    Features:
    - All Phase 1-3 capabilities
    - Advanced storage and file management
    - System monitoring and health checks
    - Metadata persistence and processing history
    """

    def __init__(self):
        """Initialize complete scraper system"""
        self.logger = setup_logger("fikfap_scraper", config.log_level, config.log_file)

        # Phase 1-3: Core scraping and downloading
        self.scraper: BaseScraper = None

        # Phase 4: Storage & monitoring
        self.file_manager = FileManager()
        self.metadata_handler = MetadataHandler()
        self.system_monitor = SystemMonitor()

        # System state
        self.is_running = False

        self.logger.info("FikFap Scraper Phase 4 initialized - Storage & File Management ready")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.stop()

    async def start(self):
        """Start all scraper components"""
        try:
            self.logger.info("🚀 Starting FikFap Scraper with Phase 4 capabilities...")

            # Check system health before starting
            await self._perform_health_checks()

            # Initialize core scraper
            self.scraper = BaseScraper()
            await self.scraper.start_session()

            # Load metadata caches
            await self.metadata_handler.load_processed_posts_cache()

            self.is_running = True
            self.logger.info("✅ FikFap Scraper started successfully")

        except Exception as e:
            self.logger.error(f"❌ Error starting scraper: {e}")
            raise

    async def stop(self):
        """Stop all scraper components"""
        try:
            self.logger.info("🛑 Stopping FikFap Scraper...")

            self.is_running = False

            # Stop components
            if self.scraper:
                await self.scraper.close_session()

            # Save metadata
            await self.metadata_handler.save_processed_posts_cache()

            self.logger.info("✅ FikFap Scraper stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping scraper: {e}")

    async def _perform_health_checks(self):
        """Perform system health checks before starting"""
        self.logger.info("🏥 Performing system health checks...")

        # Check system health
        is_healthy, issues = self.system_monitor.check_system_health()

        if not is_healthy:
            self.logger.warning("⚠️  System health issues detected:")
            for issue in issues:
                self.logger.warning(f"   - {issue}")

            # Get recommendations
            recommendations = self.system_monitor.get_storage_recommendations()
            if recommendations:
                self.logger.info("💡 Recommendations:")
                for rec in recommendations[:3]:  # Show top 3
                    self.logger.info(f"   - {rec}")
        else:
            self.logger.info("✅ System health check passed")

        # Check disk space
        disk_summary = self.system_monitor.disk_monitor.get_usage_summary()
        if disk_summary['critical_paths']:
            raise Exception("❌ Critical disk space issue - cannot start scraper")

    async def process_video_with_storage(self, post_id: int) -> bool:
        """
        Process a single video through complete pipeline with Phase 4 storage

        Args:
            post_id: Post ID to process

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            self.logger.info(f"📹 Processing video with storage: {post_id}")

            # Check if already processed
            if await self.metadata_handler.is_post_processed(post_id):
                self.logger.info(f"⏭️  Video {post_id} already processed, skipping")
                return True

            # Get video information using existing scraper
            try:
                # This would normally call the scraper's actual method
                self.logger.info(f"📊 Extracting video data for post {post_id}...")

                # For now, we'll demonstrate with a mock video post
                # In real usage, this would be: video_post = await self.scraper.get_video_post(post_id)
                mock_video_post = self._create_mock_video_post(post_id)

                if not mock_video_post:
                    self.logger.error(f"❌ Could not get video info for post {post_id}")
                    return False

                video_post = mock_video_post

            except Exception as e:
                self.logger.error(f"❌ Error extracting video data: {e}")
                return False

            # Create processing record
            processing_record = await self.metadata_handler.create_processing_record(video_post)

            # Update status to processing
            await self.metadata_handler.update_processing_record(
                post_id, 
                ProcessingStatus.PROCESSING
            )

            # Create directory structure
            directory_structure = await self.file_manager.create_directory_structure(video_post)
            self.logger.info(f"📁 Created directory structure at: {directory_structure.postPath}")

            # Simulate downloading and storage (Phase 3 integration point)
            try:
                # This is where Phase 3 M3U8 downloader would be called
                self.logger.info(f"📥 Simulating video download for qualities: {len(video_post.availableQualities)}")

                stored_files = []
                successful_downloads = 0

                # Simulate processing each quality
                for quality in video_post.availableQualities:
                    try:
                        # Generate filename
                        filename = self.file_manager.generate_filename(
                            video_post, 
                            quality.resolution, 
                            quality.codec.value
                        )

                        # Get target path
                        quality_dir = Path(directory_structure.qualityPaths[quality.resolution])
                        target_path = quality_dir / filename

                        # Simulate creating a small file (in real usage, this would be the downloaded video)
                        temp_content = f"Mock video content for {quality.resolution} - Post {post_id}".encode()
                        temp_file = target_path.parent / f"temp_{filename}"

                        # Create temp file
                        temp_file.parent.mkdir(parents=True, exist_ok=True)
                        with open(temp_file, 'wb') as f:
                            f.write(temp_content * 100)  # Make it a bit larger

                        # Store file with metadata using Phase 4 file manager
                        storage_metadata = await self.file_manager.store_video_file(
                            temp_file,
                            target_path,
                            video_post,
                            quality.resolution,
                            quality.codec.value,
                            move_file=True
                        )

                        # Save metadata
                        await self.metadata_handler.save_video_metadata(
                            storage_metadata,
                            directory_structure
                        )

                        stored_files.append(str(target_path))
                        successful_downloads += 1

                        self.logger.info(f"✅ Stored: {filename} ({quality.resolution})")

                    except Exception as e:
                        self.logger.error(f"❌ Error storing {quality.resolution}: {e}")

                # Update processing record
                if successful_downloads > 0:
                    await self.metadata_handler.update_processing_record(
                        post_id,
                        ProcessingStatus.COMPLETED,
                        stored_files=stored_files
                    )

                    self.logger.info(f"✅ Successfully processed video {post_id}: "
                                   f"{successful_downloads} qualities stored")
                    return True
                else:
                    await self.metadata_handler.update_processing_record(
                        post_id,
                        ProcessingStatus.FAILED,
                        error_message="No successful downloads"
                    )

                    self.logger.error(f"❌ Failed to process video {post_id}: no successful downloads")
                    return False

            except Exception as e:
                await self.metadata_handler.update_processing_record(
                    post_id,
                    ProcessingStatus.FAILED,
                    error_message=str(e)
                )
                raise e

        except Exception as e:
            self.logger.error(f"❌ Error processing video {post_id}: {e}")

            # Update processing record with error
            try:
                await self.metadata_handler.update_processing_record(
                    post_id,
                    ProcessingStatus.FAILED,
                    error_message=str(e)
                )
            except Exception:
                pass  # Don't fail on metadata update error

            return False

    def _create_mock_video_post(self, post_id: int) -> VideoPost:
        """Create a mock video post for demonstration (replace with real scraper call)"""
        from data.models import Author, VideoQuality, VideoCodec

        mock_author = Author(
            userId=f"user_{post_id}",
            username=f"creator_{post_id}"
        )

        mock_qualities = [
            VideoQuality(
                resolution="720p",
                codec=VideoCodec.H264,
                playlist_url=f"https://api.fikfap.com/video/{post_id}/720p.m3u8"
            ),
            VideoQuality(
                resolution="1080p", 
                codec=VideoCodec.H264,
                playlist_url=f"https://api.fikfap.com/video/{post_id}/1080p.m3u8"
            )
        ]

        mock_video_post = VideoPost(
            postId=post_id,
            mediaId=f"media_{post_id}",
            bunnyVideoId=f"bunny_{post_id}",
            userId=f"user_{post_id}",
            label=f"Phase 4 Demo Video - Post {post_id}",
            videoStreamUrl=f"https://api.fikfap.com/video/{post_id}/master.m3u8",
            publishedAt=datetime.now(),
            author=mock_author,
            availableQualities=mock_qualities
        )

        return mock_video_post

    async def process_multiple_videos(self, post_ids: List[int]) -> Dict[str, Any]:
        """Process multiple videos"""
        results = {
            'processed': 0,
            'skipped': 0,
            'failed': 0,
            'total': len(post_ids)
        }

        self.logger.info(f"📥 Processing {len(post_ids)} videos...")

        for post_id in post_ids:
            success = await self.process_video_with_storage(post_id)

            if success:
                # Check if it was actually processed or skipped
                if await self.metadata_handler.is_post_processed(post_id):
                    if post_id in [record.postId for record in self.metadata_handler.processing_records_cache.values() 
                                  if record.status == ProcessingStatus.COMPLETED]:
                        results['processed'] += 1
                    else:
                        results['skipped'] += 1
            else:
                results['failed'] += 1

        self.logger.info(f"📊 Processing complete - "
                       f"Processed: {results['processed']}, "
                       f"Skipped: {results['skipped']}, "
                       f"Failed: {results['failed']}")

        return results

    async def cleanup_storage(self) -> Dict[str, Any]:
        """Perform storage cleanup operations"""
        self.logger.info("🧹 Starting storage cleanup...")

        cleanup_results = {
            'incomplete_downloads': await self.file_manager.cleanup_incomplete_downloads(),
            'old_records': await self.metadata_handler.cleanup_old_records(30),
            'storage_stats': self.file_manager.get_storage_stats()
        }

        self.logger.info("✅ Storage cleanup completed")
        return cleanup_results

    async def get_system_report(self) -> Dict[str, Any]:
        """Get comprehensive system report"""
        try:
            return {
                'scraper_status': {
                    'is_running': self.is_running,
                    'components': {
                        'scraper': self.scraper is not None,
                        'file_manager': True,
                        'metadata_handler': True,
                        'system_monitor': True
                    }
                },
                'system_status': self.system_monitor.get_system_status().dict(),
                'processing_stats': await self.metadata_handler.get_processing_statistics(),
                'storage_stats': self.file_manager.get_storage_stats(),
                'disk_usage': self.system_monitor.disk_monitor.get_usage_summary(),
                'health_check': {
                    'is_healthy': self.system_monitor.check_system_health()[0],
                    'issues': self.system_monitor.check_system_health()[1]
                },
                'recommendations': self.system_monitor.get_storage_recommendations(),
                'generated_at': datetime.now().isoformat()
            }

        except Exception as e:
            return {'error': f"Error generating system report: {e}"}


async def main():
    """Main entry point for Phase 4 demonstration"""
    try:
        logger = setup_logger("fikfap_main", config.log_level, config.log_file)

        logger.info("🎬 FikFap API Scraper - Phase 4: Storage & File Management")
        logger.info("=" * 80)

        # Create necessary directories
        config.create_directories()

        async with FikFapScraperPhase4() as scraper:
            # Show system report
            logger.info("📊 System Report:")
            report = await scraper.get_system_report()

            # System status
            system_status = report['system_status']
            logger.info(f"   💾 Disk Space: {system_status['diskSpaceGb']:.2f}GB free")
            logger.info(f"   🧠 Memory: {system_status['memoryUsagePercent']:.1f}% used")
            logger.info(f"   🔧 CPU: {system_status['cpuUsagePercent']:.1f}% used")
            logger.info(f"   ✅ Health: {'Healthy' if report['health_check']['is_healthy'] else 'Issues detected'}")

            # Processing stats
            processing_stats = report['processing_stats']
            logger.info(f"   📋 Processed Posts: {processing_stats['total_processed']}")
            logger.info(f"   📈 Success Rate: {processing_stats['success_rate']:.1f}%")

            # Recommendations
            if report['recommendations']:
                logger.info("💡 Recommendations:")
                for rec in report['recommendations'][:3]:
                    logger.info(f"   - {rec}")

            logger.info("=" * 80)

            # Demo: Process some videos with Phase 4 storage
            logger.info("🎯 Phase 4 Demo: Processing videos with storage...")

            demo_post_ids = [12345, 12346, 12347]  # Example post IDs

            results = await scraper.process_multiple_videos(demo_post_ids)
            logger.info(f"📊 Demo Results: {results}")

            # Perform cleanup
            cleanup_results = await scraper.cleanup_storage()
            logger.info(f"🧹 Cleanup: {cleanup_results['incomplete_downloads'].filesRemoved} temp files removed")

            # Final storage stats
            storage_stats = cleanup_results['storage_stats']
            logger.info(f"📁 Storage: {storage_stats['total_files']} files, "
                       f"{storage_stats['total_size']:,} bytes total")

            logger.info("=" * 80)
            logger.info("🎉 Phase 4 Demo Complete!")
            logger.info("✅ Storage & File Management System fully operational")
            logger.info("📋 Features demonstrated:")
            logger.info("   - Directory structure creation (postId/m3u8/quality/)")
            logger.info("   - File storage with integrity verification")
            logger.info("   - JSON metadata persistence")
            logger.info("   - Processing history tracking")
            logger.info("   - Duplicate prevention")
            logger.info("   - System health monitoring")
            logger.info("   - Automated cleanup operations")
            logger.info("=" * 80)

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
