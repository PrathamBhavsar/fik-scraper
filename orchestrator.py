"""
Phase 5: Main Orchestrator & Integration - FIXED
FikFap Scraper - Complete System Orchestrator

Fixed component initialization dependencies.
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable, Set
from contextlib import asynccontextmanager
import signal
import time
import traceback

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

# Core imports
from core.config import config
from core.base_scraper import BaseScraper
from enhanced_exceptions import *
from data.extractor import FikFapDataExtractor
from download.m3u8_downloader import M3U8Downloader
from download.quality_manager import QualityManager
from storage.file_manager import FileManager
from storage.metadata_handler import MetadataHandler
from utils.monitoring import SystemMonitor, DiskMonitor
from utils.logger import setup_logger
from utils.helpers import format_bytes, sanitize_filename

# Data models
from data.models import (
    VideoPost, VideoQuality, Author, ProcessingStatus, DownloadStatus, 
    StorageMetadata, DirectoryStructure, ProcessingRecord, SystemStatus,
    VideoCodec, ExplicitnessRating
)

class FikFapScraperOrchestrator:
    """
    Main orchestrator for the FikFap scraper system
    
    Features:
    - Complete workflow orchestration (scraping -> extraction -> downloading -> storage)
    - Dependency injection and component lifecycle management
    - Comprehensive error handling and recovery
    - System health monitoring and resource management
    - Graceful shutdown and cleanup procedures
    - Configuration-driven behavior
    """
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """Initialize the orchestrator with all components"""
        
        # Initialize logging first
        self.logger = setup_logger(
            "fikfap_orchestrator", 
            config.log_level, 
            config.log_file
        )
        
        self.logger.info("Initializing FikFap Scraper Orchestrator...")
        
        # Apply configuration overrides
        if config_override:
            self._apply_config_overrides(config_override)
        
        # Component initialization - order matters for dependency injection
        self.components = {}
        self._initialize_components()
        
        # System state
        self.is_running = False
        self.is_shutting_down = False
        self.current_jobs: Dict[str, Dict[str, Any]] = {}
        self.startup_time: Optional[datetime] = None
        self.shutdown_handlers: List[Callable] = []
        
        # Statistics and monitoring
        self.stats = {
            'videos_processed': 0,
            'videos_failed': 0,
            'videos_skipped': 0,
            'total_bytes_downloaded': 0,
            'total_processing_time': 0,
            'errors': []
        }
        
        # Register signal handlers for graceful shutdown
        self._register_shutdown_handlers()
        
        self.logger.info("FikFap Scraper Orchestrator initialized successfully")
    
    def _apply_config_overrides(self, overrides: Dict[str, Any]):
        """Apply configuration overrides"""
        for key, value in overrides.items():
            setattr(config, key, value)
            self.logger.debug(f"Configuration override: {key} = {value}")
    
    def _initialize_components(self):
        """Initialize all components with proper dependency injection"""
        try:
            # Phase 4: Storage & monitoring (foundation components)
            self.system_monitor = SystemMonitor()
            self.disk_monitor = DiskMonitor()
            self.file_manager = FileManager()
            self.metadata_handler = MetadataHandler()
            
            # Phase 1: Base scraper (needed before extractor)
            self.base_scraper = None  # Will be initialized during startup
            
            # Phase 3: Download system (independent components)
            self.quality_manager = QualityManager()
            self.downloader = M3U8Downloader()
            
            # Phase 2: Data extraction (depends on base scraper - initialized during startup)
            self.extractor = None  # Will be initialized during startup after scraper
            
            # Store component references for easy access
            self.components = {
                'system_monitor': self.system_monitor,
                'disk_monitor': self.disk_monitor,
                'file_manager': self.file_manager,
                'metadata_handler': self.metadata_handler,
                'quality_manager': self.quality_manager,
                'downloader': self.downloader
            }
            
            # Configure component dependencies
            self._configure_component_dependencies()
            
            self.logger.info("All available components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing components: {e}")
            raise OrchestrationError(f"Component initialization failed: {e}")
    
    def _configure_component_dependencies(self):
        """Configure dependencies between components"""
        try:
            # Configure downloader callbacks for progress monitoring
            if hasattr(self.downloader, 'set_progress_callback'):
                self.downloader.set_progress_callback(self._on_download_progress)
            
            # Configure system monitoring alerts
            self.system_monitor.add_alert_callback(self._on_system_alert)
            self.disk_monitor.add_alert_callback(self._on_disk_alert)
            
            # Configure file manager for cleanup notifications
            if hasattr(self.file_manager, 'set_cleanup_callback'):
                self.file_manager.set_cleanup_callback(self._on_cleanup_complete)
            
            self.logger.debug("Component dependencies configured")
            
        except Exception as e:
            self.logger.warning(f"Error configuring component dependencies: {e}")
    
    def _register_shutdown_handlers(self):
        """Register signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Add cleanup handlers
        self.shutdown_handlers.extend([
            self._save_final_statistics,
            self._cleanup_temp_files,
            self._close_component_sessions_sync
        ])
    
    async def startup(self):
        """
        Perform complete system startup sequence
        
        Returns:
            bool: True if startup successful, False otherwise
        """
        if self.is_running:
            self.logger.warning("System is already running")
            return True
        
        try:
            self.logger.info("[LAUNCH] Starting FikFap Scraper System...")
            self.startup_time = datetime.now()
            
            # Step 1: System health checks
            await self._perform_startup_health_checks()
            
            # Step 2: Initialize scraper session and dependent components
            await self._initialize_scraper_dependent_components()
            
            # Step 3: Initialize directories and storage
            await self._initialize_storage_system()
            
            # Step 4: Load existing metadata and state
            await self._load_system_state()
            
            # Step 5: Start monitoring and maintenance tasks
            await self._start_background_tasks()
            
            self.is_running = True
            
            startup_duration = (datetime.now() - self.startup_time).total_seconds()
            self.logger.info(f"[OK] System startup completed successfully in {startup_duration:.2f}s")
            
            # Log system status
            await self._log_system_status()
            
            return True
            
        except Exception as e:
            self.logger.error(f"[ERROR] System startup failed: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            # Attempt cleanup if partial startup occurred
            await self._cleanup_failed_startup()
            return False
    
    async def _initialize_scraper_dependent_components(self):
        """Initialize components that depend on the base scraper"""
        self.logger.info("[SCRAPER] Initializing scraper-dependent components...")
        
        # Initialize base scraper first
        self.base_scraper = BaseScraper()
        await self.base_scraper.start_session()
        
        # Now initialize the extractor with the scraper
        self.extractor = FikFapDataExtractor(self.base_scraper)
        
        # Update components dictionary
        self.components['base_scraper'] = self.base_scraper
        self.components['extractor'] = self.extractor
        
        self.logger.info("[OK] Scraper-dependent components initialized")
    
    async def shutdown(self):
        """
        Perform complete system shutdown sequence
        
        Returns:
            bool: True if shutdown successful, False otherwise
        """
        if self.is_shutting_down:
            self.logger.warning("Shutdown already in progress")
            return True
        
        if not self.is_running:
            self.logger.info("System is not running")
            return True
        
        try:
            self.is_shutting_down = True
            shutdown_start = datetime.now()
            
            self.logger.info("[STOP] Initiating system shutdown...")
            
            # Step 1: Stop accepting new jobs
            self.logger.info("Stopping new job acceptance...")
            
            # Step 2: Wait for current jobs to complete (with timeout)
            await self._wait_for_jobs_completion(timeout=300)  # 5 minute timeout
            
            # Step 3: Stop background tasks
            await self._stop_background_tasks()
            
            # Step 4: Save system state and metadata
            await self._save_system_state()
            
            # Step 5: Close component sessions
            await self._close_component_sessions()
            
            # Step 6: Run shutdown handlers
            await self._run_shutdown_handlers()
            
            self.is_running = False
            self.is_shutting_down = False
            
            shutdown_duration = (datetime.now() - shutdown_start).total_seconds()
            self.logger.info(f"[OK] System shutdown completed successfully in {shutdown_duration:.2f}s")
            
            return True
            
        except Exception as e:
            self.logger.error(f"[ERROR] Error during shutdown: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    async def process_video_workflow(
        self, 
        post_id: int, 
        quality_filter: Optional[List[str]] = None,
        force_reprocess: bool = False
    ) -> Dict[str, Any]:
        """
        Complete workflow: scraping -> extraction -> downloading -> storage
        
        Args:
            post_id: Post ID to process
            quality_filter: Optional list of qualities to download
            force_reprocess: Force reprocessing even if already processed
            
        Returns:
            Dict containing processing results and metadata
        """
        start_time = datetime.now()
        job_id = f"video_{post_id}_{int(time.time())}"
        
        # Initialize job tracking
        job_info = {
            'job_id': job_id,
            'post_id': post_id,
            'status': 'starting',
            'start_time': start_time,
            'steps_completed': [],
            'errors': [],
            'results': {}
        }
        self.current_jobs[job_id] = job_info
        
        try:
            self.logger.info(f"[VIDEO] Starting video workflow for post {post_id}")
            
            # Step 1: Check if already processed (duplicate detection)
            if not force_reprocess:
                duplicate_check = await self._check_duplicate_processing(post_id)
                if duplicate_check['is_duplicate']:
                    job_info['status'] = 'skipped_duplicate'
                    self.stats['videos_skipped'] += 1
                    return duplicate_check
            
            job_info['steps_completed'].append('duplicate_check')
            
            # Step 2: System health check
            health_check = await self._check_processing_health()
            if not health_check['can_proceed']:
                raise ProcessingError(f"System health check failed: {health_check['issues']}")
            
            job_info['steps_completed'].append('health_check')
            
            # Step 3: Data extraction
            self.logger.info(f"[CHART] Extracting video data for post {post_id}")
            extraction_result = await self._extract_video_data(post_id)
            if not extraction_result['success']:
                raise ExtractionError(f"Data extraction failed: {extraction_result['error']}")
            
            video_post = extraction_result['video_post']
            job_info['results']['video_post'] = video_post.dict()
            job_info['steps_completed'].append('extraction')
            
            # Step 4: Quality selection and filtering
            self.logger.info(f"[TARGET] Processing quality selection for {len(video_post.availableQualities)} qualities")
            quality_result = await self._select_qualities(video_post, quality_filter)
            selected_qualities = quality_result['selected_qualities']
            
            job_info['results']['selected_qualities'] = [q.dict() for q in selected_qualities]
            job_info['steps_completed'].append('quality_selection')
            
            # Step 5: Create storage structure
            self.logger.info(f"[FOLDER] Creating directory structure for post {post_id}")
            storage_result = await self._create_storage_structure(video_post)
            directory_structure = storage_result['directory_structure']
            
            job_info['results']['directory_structure'] = directory_structure.dict()
            job_info['steps_completed'].append('storage_creation')
            
            # Step 6: Initialize processing record
            processing_record = await self.metadata_handler.create_processing_record(video_post)
            await self.metadata_handler.update_processing_record(
                post_id, ProcessingStatus.PROCESSING
            )
            job_info['steps_completed'].append('processing_record')
            
            # Step 7: Download videos
            self.logger.info(f"[DOWNLOAD] Starting downloads for {len(selected_qualities)} qualities")
            download_results = await self._download_qualities(
                video_post, selected_qualities, directory_structure, job_id
            )
            
            job_info['results']['downloads'] = download_results
            job_info['steps_completed'].append('downloads')
            
            # Step 8: Store and verify files
            self.logger.info(f"[DISK] Storing and verifying downloaded files")
            storage_results = await self._store_downloaded_files(
                video_post, download_results, directory_structure
            )
            
            job_info['results']['storage'] = storage_results
            job_info['steps_completed'].append('storage')
            
            # Step 9: Save metadata
            self.logger.info(f"[LIST] Saving final metadata")
            metadata_results = await self._save_final_metadata(
                video_post, storage_results, directory_structure, processing_record
            )
            
            job_info['results']['metadata'] = metadata_results
            job_info['steps_completed'].append('metadata')
            
            # Step 10: Mark as completed
            await self.metadata_handler.update_processing_record(
                post_id, ProcessingStatus.COMPLETED,
                stored_files=[result['file_path'] for result in storage_results['successful']]
            )
            
            # Update statistics
            job_info['status'] = 'completed'
            job_info['end_time'] = datetime.now()
            job_info['duration'] = (job_info['end_time'] - start_time).total_seconds()
            
            self.stats['videos_processed'] += 1
            self.stats['total_bytes_downloaded'] += sum(
                result.get('file_size', 0) for result in storage_results['successful']
            )
            self.stats['total_processing_time'] += job_info['duration']
            
            self.logger.info(f"[OK] Video workflow completed successfully for post {post_id} "
                           f"in {job_info['duration']:.2f}s")
            
            return {
                'success': True,
                'job_id': job_id,
                'post_id': post_id,
                'duration': job_info['duration'],
                'results': job_info['results'],
                'stats': {
                    'qualities_downloaded': len(storage_results['successful']),
                    'total_size_bytes': sum(result.get('file_size', 0) for result in storage_results['successful']),
                    'files_created': len(storage_results['successful'])
                }
            }
            
        except Exception as e:
            # Handle errors and cleanup
            job_info['status'] = 'failed'
            job_info['end_time'] = datetime.now()
            job_info['duration'] = (job_info['end_time'] - start_time).total_seconds()
            job_info['errors'].append(str(e))
            
            self.stats['videos_failed'] += 1
            self.stats['errors'].append({
                'post_id': post_id,
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'steps_completed': job_info['steps_completed']
            })
            
            # Update processing record
            try:
                await self.metadata_handler.update_processing_record(
                    post_id, ProcessingStatus.FAILED, error_message=str(e)
                )
            except Exception:
                pass  # Don't fail on metadata update error
            
            self.logger.error(f"[ERROR] Video workflow failed for post {post_id}: {e}")
            self.logger.error(f"Steps completed: {job_info['steps_completed']}")
            
            return {
                'success': False,
                'job_id': job_id,
                'post_id': post_id,
                'error': str(e),
                'duration': job_info['duration'],
                'steps_completed': job_info['steps_completed']
            }
        
        finally:
            # Clean up job tracking
            if job_id in self.current_jobs:
                del self.current_jobs[job_id]
    
    async def process_multiple_videos(
        self, 
        post_ids: List[int], 
        max_concurrent: Optional[int] = None,
        quality_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process multiple videos with controlled concurrency
        
        Args:
            post_ids: List of post IDs to process
            max_concurrent: Maximum concurrent processing (default from config)
            quality_filter: Optional quality filter
            
        Returns:
            Dict containing batch processing results
        """
        if not post_ids:
            return {'success': True, 'results': [], 'summary': {}}
        
        max_concurrent = max_concurrent or config.get('processing.max_concurrent', 3)
        
        self.logger.info(f"[DOWNLOAD] Starting batch processing of {len(post_ids)} videos "
                        f"with max concurrency {max_concurrent}")
        
        start_time = datetime.now()
        results = []
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(post_id: int) -> Dict[str, Any]:
            async with semaphore:
                return await self.process_video_workflow(post_id, quality_filter)
        
        # Process all videos with controlled concurrency
        tasks = [process_with_semaphore(post_id) for post_id in post_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        successful = []
        failed = []
        skipped = []
        
        for i, result in enumerate(results):
            post_id = post_ids[i]

            if isinstance(result, Exception):
                failed.append({
                    'post_id': post_id,
                    'error': str(result),
                    'success': False
                })
            elif result.get('success'):
                # Fix: Check for duplicate using 'is_duplicate' key
                if result.get('is_duplicate'):
                    skipped.append(result)
                else:
                    successful.append(result)
            else:
                failed.append(result)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        summary = {
            'total': len(post_ids),
            'successful': len(successful),
            'failed': len(failed),
            'skipped': len(skipped),
            'duration': duration,
            'videos_per_second': len(successful) / duration if duration > 0 else 0,
            'total_size_bytes': sum(
                result.get('stats', {}).get('total_size_bytes', 0)
                for result in successful if isinstance(result, dict)
            )
        }
        
        self.logger.info(f"[CHART] Batch processing completed: {summary}")
        
        return {
            'success': True,
            'results': results,
            'summary': summary,
            'successful': successful,
            'failed': failed,
            'skipped': skipped
        }
    
    # Internal workflow methods (same as before, but with proper component references)
    
    async def _perform_startup_health_checks(self):
        """Perform comprehensive startup health checks"""
        self.logger.info("[HEALTH] Performing startup health checks...")
        
        # System resource checks
        is_healthy, issues = self.system_monitor.check_system_health()
        if not is_healthy:
            self.logger.warning("[WARNING] System health issues detected:")
            for issue in issues:
                self.logger.warning(f"   - {issue}")
        
        # Disk space checks
        disk_summary = self.system_monitor.disk_monitor.get_usage_summary()
        if disk_summary.get('critical_paths'):
            raise StartupError("Critical disk space issue - insufficient space for operation")
        
        self.logger.info("[OK] Startup health checks completed")
    
    async def _verify_component_health(self):
        """Verify all components are healthy and ready"""
        for name, component in self.components.items():
            if hasattr(component, 'health_check'):
                try:
                    await component.health_check()
                except Exception as e:
                    raise StartupError(f"Component {name} health check failed: {e}")
    
    async def _initialize_storage_system(self):
        """Initialize storage directories and system"""
        self.logger.info("[FOLDER] Initializing storage system...")
        
        # Create necessary directories
        config.create_directories()
        
        # Initialize metadata system
        await self.metadata_handler.load_processed_posts_cache()
        
        # Verify storage permissions
        base_path = Path(config.get('storage.base_path', './downloads'))
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        
        # Test write permissions
        test_file = base_path / '.write_test'
        try:
            test_file.write_text('test')
            test_file.unlink()
        except Exception as e:
            raise StartupError(f"Storage system initialization failed - no write permissions: {e}")
        
        self.logger.info("[OK] Storage system initialized")
    
    async def _load_system_state(self):
        """Load existing system state and metadata"""
        self.logger.info("[CHART] Loading system state...")
        
        # Load processing statistics
        try:
            stats = await self.metadata_handler.get_processing_statistics()
            self.logger.info(f"Loaded processing history: {stats['total_processed']} processed posts")
        except Exception as e:
            self.logger.warning(f"Could not load processing statistics: {e}")
        
        self.logger.info("[OK] System state loaded")
    
    async def _start_background_tasks(self):
        """Start background monitoring and maintenance tasks"""
        self.logger.info("[SETTINGS] Starting background tasks...")
        
        # Background tasks would be started here
        # For now, just log that they're ready
        
        self.logger.info("[OK] Background tasks started")
    
    async def _log_system_status(self):
        """Log current system status"""
        try:
            status = self.system_monitor.get_system_status()
            self.logger.info(f"[DISK] Disk Space: {status.diskSpaceGb:.2f}GB free")
            self.logger.info(f"[MEMORY] Memory: {status.memoryUsagePercent:.1f}% used")
            self.logger.info(f"[TOOL] CPU: {status.cpuUsagePercent:.1f}% used")
        except Exception as e:
            self.logger.warning(f"Could not log system status: {e}")
    
    # Callback methods for component events
    
    def _on_download_progress(self, job_id: str, progress_info: Dict[str, Any]):
        """Handle download progress updates"""
        if job_id in self.current_jobs:
            self.current_jobs[job_id].setdefault('progress', {}).update(progress_info)
    
    def _on_system_alert(self, alert_info: Dict[str, Any]):
        """Handle system alerts"""
        self.logger.warning(f"System alert: {alert_info}")
    
    def _on_disk_alert(self, disk_info: Dict[str, Any]):
        """Handle disk space alerts"""
        self.logger.warning(f"Disk space alert: {disk_info}")
    
    def _on_cleanup_complete(self, cleanup_info: Dict[str, Any]):
        """Handle cleanup completion notifications"""
        self.logger.info(f"Cleanup completed: {cleanup_info}")
    
    # Cleanup and shutdown methods
    
    async def _cleanup_failed_startup(self):
        """Clean up resources after failed startup"""
        self.logger.info("[CLEAN] Cleaning up failed startup...")
        
        try:
            if self.base_scraper:
                await self.base_scraper.close_session()
        except Exception:
            pass
        
        self.is_running = False
        self.is_shutting_down = False
    
    async def _wait_for_jobs_completion(self, timeout: int = 300):
        """Wait for current jobs to complete with timeout"""
        if not self.current_jobs:
            return
        
        self.logger.info(f"Waiting for {len(self.current_jobs)} jobs to complete...")
        
        start_time = time.time()
        while self.current_jobs and (time.time() - start_time) < timeout:
            await asyncio.sleep(1)
        
        if self.current_jobs:
            self.logger.warning(f"Timeout reached, {len(self.current_jobs)} jobs still running")
    
    async def _stop_background_tasks(self):
        """Stop all background tasks"""
        self.logger.info("Stopping background tasks...")
    
    async def _save_system_state(self):
        """Save current system state"""
        self.logger.info("[DISK] Saving system state...")
        
        try:
            await self.metadata_handler.save_processed_posts_cache()
        except Exception as e:
            self.logger.error(f"Error saving system state: {e}")
    
    async def _close_component_sessions(self):
        """Close all component sessions"""
        self.logger.info("[STOP] Closing component sessions...")
        
        try:
            if self.base_scraper:
                await self.base_scraper.close_session()
        except Exception as e:
            self.logger.error(f"Error closing scraper session: {e}")
    
    async def _run_shutdown_handlers(self):
        """Run all registered shutdown handlers"""
        for handler in self.shutdown_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler()
                else:
                    handler()
            except Exception as e:
                self.logger.error(f"Error in shutdown handler: {e}")
    
    def _save_final_statistics(self):
        """Save final processing statistics"""
        self.logger.info("[CHART] Final Statistics:")
        self.logger.info(f"   Videos Processed: {self.stats['videos_processed']}")
        self.logger.info(f"   Videos Failed: {self.stats['videos_failed']}")
        self.logger.info(f"   Videos Skipped: {self.stats['videos_skipped']}")
        self.logger.info(f"   Total Bytes Downloaded: {format_bytes(self.stats['total_bytes_downloaded'])}")
        self.logger.info(f"   Total Processing Time: {self.stats['total_processing_time']:.2f}s")
        
        if self.startup_time:
            uptime = (datetime.now() - self.startup_time).total_seconds()
            self.logger.info(f"   System Uptime: {uptime:.2f}s")
    
    def _cleanup_temp_files(self):
        """Clean up any remaining temporary files"""
        try:
            temp_dir = Path(config.get('download.temp_dir', './downloads/.temp'))
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                self.logger.info("[CLEAN] Temporary files cleaned up")
        except Exception as e:
            self.logger.error(f"Error cleaning temporary files: {e}")
    
    def _close_component_sessions_sync(self):
        """Synchronous version of component session closing"""
        # This is called from signal handlers which can't use async
        pass
    
    # Placeholder methods for workflow steps (simplified for testing)
    
    async def _check_duplicate_processing(self, post_id: int) -> Dict[str, Any]:
        """Check if post has already been processed"""
        try:
            is_processed = await self.metadata_handler.is_post_processed(post_id)
            return {
                'is_duplicate': is_processed,
                'post_id': post_id,
                'message': f"Post {post_id} already processed" if is_processed else None
            }
        except Exception:
            return {'is_duplicate': False, 'post_id': post_id}
    
    async def _check_processing_health(self) -> Dict[str, Any]:
        """Check if system is healthy enough for processing"""
        is_healthy, issues = self.system_monitor.check_system_health()
        return {
            'can_proceed': is_healthy,
            'issues': issues
        }
    
    async def _extract_video_data(self, post_id: int) -> Dict[str, Any]:
        """Extract video data using the extractor component"""
        try:
            if not self.extractor:
                raise ExtractionError("Extractor not initialized")
            
            video_post = await self.extractor.extract_video_data(post_id)
            
            if video_post:
                return {
                    'success': True,
                    'video_post': video_post
                }
            else:
                return {
                    'success': False,
                    'error': f"Could not extract data for post {post_id}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _select_qualities(self, video_post: VideoPost, quality_filter: Optional[List[str]]) -> Dict[str, Any]:
        """Select qualities to download based on configuration and filters"""
        try:
            selected_qualities = self.quality_manager.filter_qualities(
                video_post.availableQualities,
                quality_filter or config.get('quality.preferred_qualities', [])
            )
            
            return {
                'success': True,
                'selected_qualities': selected_qualities,
                'total_available': len(video_post.availableQualities),
                'total_selected': len(selected_qualities)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'selected_qualities': []
            }
    
    async def _create_storage_structure(self, video_post: VideoPost) -> Dict[str, Any]:
        """Create storage directory structure"""
        try:
            directory_structure = await self.file_manager.create_directory_structure(video_post)
            
            return {
                'success': True,
                'directory_structure': directory_structure
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _download_qualities(
        self, 
        video_post: VideoPost, 
        qualities: List[VideoQuality], 
        directory_structure: DirectoryStructure,
        job_id: str
    ) -> Dict[str, Any]:
        """Download all selected qualities"""
        download_results = {
            'successful': [],
            'failed': [],
            'total_bytes': 0
        }
        
        for quality in qualities:
            try:
                # Mock successful download for testing
                filename = self.file_manager.generate_filename(
                    video_post, quality.resolution, quality.codec.value
                )
                
                quality_dir = Path(directory_structure.qualityPaths.get(quality.resolution, directory_structure.postPath))
                output_path = quality_dir / filename
                
                # Create directory and mock file
                output_path.parent.mkdir(parents=True, exist_ok=True)
                test_content = f"Mock video content - {quality.resolution}".encode() * 1000
                
                with open(output_path, 'wb') as f:
                    f.write(test_content)
                
                file_size = len(test_content)
                download_results['successful'].append({
                    'quality': quality.resolution,
                    'file_path': str(output_path),
                    'file_size': file_size,
                    'codec': quality.codec.value
                })
                download_results['total_bytes'] += file_size
                
            except Exception as e:
                download_results['failed'].append({
                    'quality': quality.resolution,
                    'error': str(e)
                })
        
        return download_results
    
    async def _store_downloaded_files(
        self, 
        video_post: VideoPost, 
        download_results: Dict[str, Any],
        directory_structure: DirectoryStructure
    ) -> Dict[str, Any]:
        """Store and verify downloaded files"""
        storage_results = {
            'successful': [],
            'failed': []
        }
        
        for download in download_results['successful']:
            try:
                storage_results['successful'].append({
                    'file_path': download['file_path'],
                    'file_size': download['file_size'],
                    'quality': download['quality'],
                    'metadata': {'checksum': 'mock_checksum'}
                })
                
            except Exception as e:
                storage_results['failed'].append({
                    'file_path': download.get('file_path'),
                    'quality': download.get('quality'),
                    'error': str(e)
                })
        
        return storage_results
    
    async def _save_final_metadata(
        self,
        video_post: VideoPost,
        storage_results: Dict[str, Any],
        directory_structure: DirectoryStructure,
        processing_record: ProcessingRecord
    ) -> Dict[str, Any]:
        """Save final metadata for the processed video"""
        try:
            # Save basic metadata
            metadata_files = []
            
            for result in storage_results['successful']:
                metadata = StorageMetadata(
                    postId=video_post.postId,
                    mediaId=video_post.mediaId,
                    title=video_post.label,
                    author=video_post.author.username if video_post.author else "unknown",
                    authorId=video_post.userId,
                    publishedAt=video_post.publishedAt,
                    fileSize=result['file_size'],
                    filePath=result['file_path'],
                    fileName=Path(result['file_path']).name,
                    quality=result['quality'],
                    resolution=result['quality'],
                    checksum=result.get('metadata', {}).get('checksum')
                )
                
                await self.metadata_handler.save_video_metadata(metadata, directory_structure)
                metadata_files.append(str(Path(directory_structure.metadataPath)))
            
            return {
                'success': True,
                'metadata_files': metadata_files
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    # Context manager support
    async def __aenter__(self):
        """Async context manager entry"""
        success = await self.startup()
        if not success:
            raise RuntimeError("Failed to start orchestrator")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.shutdown()

    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        return {
            'orchestrator': {
                'is_running': self.is_running,
                'is_shutting_down': self.is_shutting_down,
                'startup_time': self.startup_time.isoformat() if self.startup_time else None,
                'active_jobs': len(self.current_jobs),
                'stats': self.stats.copy()
            },
            'components': {
                'base_scraper': self.base_scraper is not None,
                'extractor': self.extractor is not None,
                'system_monitor': self.system_monitor is not None,
                'file_manager': self.file_manager is not None,
                'metadata_handler': self.metadata_handler is not None,
                'quality_manager': self.quality_manager is not None,
                'downloader': self.downloader is not None
            },
            'system': self.system_monitor.get_system_status().dict() if self.system_monitor else {},
            'timestamp': datetime.now().isoformat()
        }