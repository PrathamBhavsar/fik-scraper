# FIXED fikfap_workflow_integrator.py - Added async context manager support

"""
FikFap Workflow Integrator with EXTREME DEBUGGING + ASYNC CONTEXT MANAGER SUPPORT

Every method has numbered steps with detailed logs to track exactly where failures occur.
FIXED: Added __aenter__ and __aexit__ methods for async with support.
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

# Import your existing components
from enhanced_exceptions import ComponentError, ProcessingError, ScrapingError, StartupError
from orchestrator import FikFapScraperOrchestrator
from fikfap_api_scraper import FikFapAPIScraper
from core.config import Config
from core.exceptions import *
from data.models import VideoPost, ProcessingStatus, ProcessingRecord
from utils.logger import setup_logger


class FikFapWorkflowIntegrator:
    """Complete workflow integrator with EXTREME debugging + ASYNC CONTEXT MANAGER."""
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        print("🔧 [WORKFLOW-DEBUG-001] Starting FikFapWorkflowIntegrator.__init__()")
        try:
            self.logger = setup_logger(self.__class__.__name__)
            print("🔧 [WORKFLOW-DEBUG-002] Logger setup completed")
            
            self.config = Config()
            print("🔧 [WORKFLOW-DEBUG-003] Config loaded")
            
            if config_override:
                self.config.update(config_override)
                print(f"🔧 [WORKFLOW-DEBUG-004] Config overrides applied: {len(config_override)} items")
            
            # Initialize components
            self.api_scraper: Optional[FikFapAPIScraper] = None
            self.orchestrator: Optional[FikFapScraperOrchestrator] = None
            print("🔧 [WORKFLOW-DEBUG-005] Component references initialized as None")
            
            # State management
            self.is_initialized = False
            self.current_cycle = 0
            self.total_posts_processed = 0
            print("🔧 [WORKFLOW-DEBUG-006] State management initialized")
            
            # Workflow statistics
            self.workflow_stats = {
                "cycles_completed": 0,
                "total_posts_scraped": 0,
                "total_posts_processed": 0,
                "total_posts_failed": 0,
                "successful_cycles": 0,
                "failed_cycles": 0,
                "average_cycle_duration": 0.0,
                "start_time": None,
                "last_cycle_time": None
            }
            print("🔧 [WORKFLOW-DEBUG-007] Workflow stats initialized")
            
            print("✅ [WORKFLOW-DEBUG-008] FikFapWorkflowIntegrator.__init__() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-001] FikFapWorkflowIntegrator.__init__() FAILED: {e}")
            raise
    
    # ASYNC CONTEXT MANAGER METHODS - THIS IS THE FIX!
    async def __aenter__(self):
        """Async context manager entry - FIXED METHOD."""
        print("🚀 [WORKFLOW-DEBUG-AENTER-001] Starting __aenter__() - async with support")
        try:
            await self.initialize()
            print("✅ [WORKFLOW-DEBUG-AENTER-002] __aenter__() completed successfully")
            return self
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-AENTER-ERROR] __aenter__() failed: {e}")
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - FIXED METHOD."""
        print("🚀 [WORKFLOW-DEBUG-AEXIT-001] Starting __aexit__() - cleanup")
        try:
            await self.cleanup()
            print("✅ [WORKFLOW-DEBUG-AEXIT-002] __aexit__() completed successfully")
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-AEXIT-ERROR] __aexit__() cleanup failed: {e}")
        return False  # Don't suppress exceptions
    
    async def initialize(self):
        """Initialize all workflow components with EXTREME debugging."""
        print("🚀 [WORKFLOW-DEBUG-009] Starting FikFapWorkflowIntegrator.initialize()")
        try:
            self.logger.info("🚀 [WORKFLOW-DEBUG-010] Initializing FikFap Workflow Integrator")
            
            # STEP 1: Initialize API scraper
            print("🔧 [WORKFLOW-DEBUG-011] STEP 1: Initializing API scraper")
            self.logger.info("🔧 [WORKFLOW-DEBUG-012] Initializing API scraper")
            
            print("🔧 [WORKFLOW-DEBUG-013] Creating FikFapAPIScraper instance")
            self.api_scraper = FikFapAPIScraper()
            print("✅ [WORKFLOW-DEBUG-014] FikFapAPIScraper instance created")
            
            print("🔧 [WORKFLOW-DEBUG-015] Calling await self.api_scraper.start()")
            await self.api_scraper.start()
            print("✅ [WORKFLOW-DEBUG-016] API scraper started successfully")
            
            # STEP 2: Initialize orchestrator
            print("🔧 [WORKFLOW-DEBUG-017] STEP 2: Initializing orchestrator")
            self.logger.info("🔧 [WORKFLOW-DEBUG-018] Initializing orchestrator")
            
            print("🔧 [WORKFLOW-DEBUG-019] Creating FikFapScraperOrchestrator instance")
            self.orchestrator = FikFapScraperOrchestrator()
            print("✅ [WORKFLOW-DEBUG-020] FikFapScraperOrchestrator instance created")
            
            print("🔧 [WORKFLOW-DEBUG-021] Calling await self.orchestrator.startup()")
            await self.orchestrator.startup()
            print("✅ [WORKFLOW-DEBUG-022] Orchestrator started successfully")
            
            # STEP 3: Setup integration hooks
            print("🔧 [WORKFLOW-DEBUG-023] STEP 3: Setting up integration hooks")
            await self._setup_integration_hooks()
            print("✅ [WORKFLOW-DEBUG-024] Integration hooks setup completed")
            
            # STEP 4: Finalize initialization
            print("🔧 [WORKFLOW-DEBUG-025] STEP 4: Finalizing initialization")
            self.is_initialized = True
            self.workflow_stats["start_time"] = datetime.now()
            print("✅ [WORKFLOW-DEBUG-026] Initialization finalized")
            
            self.logger.info("✅ [WORKFLOW-DEBUG-027] FikFap Workflow Integrator initialized successfully")
            print("🎉 [WORKFLOW-DEBUG-028] FikFapWorkflowIntegrator.initialize() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-002] FikFapWorkflowIntegrator.initialize() FAILED: {e}")
            self.logger.error(f"Failed to initialize workflow integrator: {e}")
            raise StartupError(f"Workflow integrator initialization failed: {e}")
    
    async def _setup_integration_hooks(self):
        """Setup integration between API scraper and orchestrator with debugging."""
        print("🚀 [WORKFLOW-DEBUG-029] Starting _setup_integration_hooks()")
        try:
            print("🔧 [WORKFLOW-DEBUG-030] Setting up scraped_posts_cache")
            self.orchestrator.scraped_posts_cache = {}
            print("✅ [WORKFLOW-DEBUG-031] scraped_posts_cache initialized")
            
            print("🔧 [WORKFLOW-DEBUG-032] Getting original extract method")
            original_extract = self.orchestrator._extract_video_data
            print("✅ [WORKFLOW-DEBUG-033] Original extract method obtained")
            
            async def integrated_extract_video_data(post_id: int):
                """Integrated extraction that uses scraped data."""
                print(f"🔧 [WORKFLOW-DEBUG-034] integrated_extract_video_data() called for post_id: {post_id}")
                
                if post_id in self.orchestrator.scraped_posts_cache:
                    post = self.orchestrator.scraped_posts_cache[post_id]
                    print(f"✅ [WORKFLOW-DEBUG-035] Using scraped data for post {post_id}")
                    self.logger.debug(f"Using scraped data for post {post_id}")
                    return post
                
                print(f"🔧 [WORKFLOW-DEBUG-036] Using original method for post {post_id}")
                return await original_extract(post_id)
            
            print("🔧 [WORKFLOW-DEBUG-037] Replacing orchestrator extract method")
            self.orchestrator._extract_video_data = integrated_extract_video_data
            print("✅ [WORKFLOW-DEBUG-038] Method replacement completed")
            
            self.logger.debug("Integration hooks setup completed")
            print("🎉 [WORKFLOW-DEBUG-039] _setup_integration_hooks() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-003] _setup_integration_hooks() FAILED: {e}")
            self.logger.error(f"Failed to setup integration hooks: {e}")
            raise ComponentError(f"Integration hooks setup failed: {e}")
    
    async def run_single_cycle(self) -> Dict[str, Any]:
        """Run a single complete workflow cycle with EXTREME debugging."""
        print("🚀 [WORKFLOW-DEBUG-040] Starting run_single_cycle()")
        try:
            cycle_start_time = time.time()
            self.current_cycle += 1
            
            print(f"🔧 [WORKFLOW-DEBUG-041] Starting cycle #{self.current_cycle}")
            self.logger.info(f"🔧 [WORKFLOW-DEBUG-042] Starting workflow cycle #{self.current_cycle}")
            
            if not self.is_initialized:
                print("❌ [WORKFLOW-DEBUG-043] Workflow integrator not initialized")
                raise ProcessingError("Workflow integrator not initialized")
            
            # STEP 1: Scrape posts from FikFap API
            print("🔧 [WORKFLOW-DEBUG-044] STEP 1: About to call _scrape_posts_from_api()")
            self.logger.info("🔧 [WORKFLOW-DEBUG-045] Step 1: Scraping posts from FikFap API")
            scraped_posts = await self._scrape_posts_from_api()
            print(f"✅ [WORKFLOW-DEBUG-046] STEP 1 RESULT: Got {len(scraped_posts) if scraped_posts else 0} scraped posts")
            
            if not scraped_posts:
                print("❌ [WORKFLOW-DEBUG-047] No posts scraped from API")
                return {
                    "success": False,
                    "cycle": self.current_cycle,
                    "error": "No posts scraped from API",
                    "posts_scraped": 0,
                    "posts_processed": 0
                }
            
            self.logger.info(f"✅ [WORKFLOW-DEBUG-048] Step 1 completed: {len(scraped_posts)} posts scraped")
            
            # STEP 2: Cache scraped posts for orchestrator
            print("🔧 [WORKFLOW-DEBUG-049] STEP 2: About to call _cache_scraped_posts()")
            self.logger.info("🔧 [WORKFLOW-DEBUG-050] Step 2: Caching posts for orchestrator integration")
            self._cache_scraped_posts(scraped_posts)
            print("✅ [WORKFLOW-DEBUG-051] STEP 2 COMPLETED: Posts cached")
            
            # STEP 3: Process posts through orchestrator
            print("🔧 [WORKFLOW-DEBUG-052] STEP 3: Processing through orchestrator")
            self.logger.info("🔧 [WORKFLOW-DEBUG-053] Step 3: Processing posts through orchestrator pipeline")
            
            post_ids = [post.post_id for post in scraped_posts]
            print(f"🔧 [WORKFLOW-DEBUG-054] Post IDs to process: {post_ids}")
            
            print("🔧 [WORKFLOW-DEBUG-055] Calling orchestrator.process_multiple_videos()")
            processing_result = await self.orchestrator.process_multiple_videos(
                post_ids=post_ids,
                max_concurrent=self.config.get('processing.max_concurrent', 2),
                quality_filter=None
            )
            print(f"✅ [WORKFLOW-DEBUG-056] STEP 3 RESULT: {processing_result.successful} processed successfully")
            
            self.logger.info(f"✅ [WORKFLOW-DEBUG-057] Step 3 completed: {processing_result.successful} processed successfully")
            
            # STEP 4: Update workflow statistics
            print("🔧 [WORKFLOW-DEBUG-058] STEP 4: Updating workflow statistics")
            cycle_duration = time.time() - cycle_start_time
            await self._update_workflow_stats(len(scraped_posts), processing_result, cycle_duration)
            print("✅ [WORKFLOW-DEBUG-059] STEP 4 COMPLETED: Statistics updated")
            
            # STEP 5: Prepare results
            print("🔧 [WORKFLOW-DEBUG-060] STEP 5: Preparing final results")
            cycle_result = {
                "success": True,
                "cycle": self.current_cycle,
                "posts_scraped": len(scraped_posts),
                "posts_processed": processing_result.successful,
                "posts_failed": processing_result.failed,
                "posts_skipped": processing_result.skipped,
                "cycle_duration": cycle_duration,
                "processing_records": [record.__dict__ for record in processing_result.processing_records],
                "pagination_state": self.api_scraper.get_pagination_state()
            }
            print(f"✅ [WORKFLOW-DEBUG-061] STEP 5 RESULT: Final results prepared")
            
            self.logger.info(
                f"✅ [WORKFLOW-DEBUG-062] Cycle #{self.current_cycle} completed successfully: "
                f"{len(scraped_posts)} scraped, {processing_result.successful} processed "
                f"in {cycle_duration:.2f}s"
            )
            
            print("🎉 [WORKFLOW-DEBUG-063] run_single_cycle() COMPLETED SUCCESSFULLY")
            return cycle_result
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-004] run_single_cycle() FAILED: {e}")
            self.logger.error(f"Workflow cycle #{self.current_cycle} failed: {e}")
            return {
                "success": False,
                "cycle": self.current_cycle,
                "error": str(e),
                "posts_scraped": 0,
                "posts_processed": 0
            }
        
        finally:
            # Clear cache after processing
            print("🔧 [WORKFLOW-DEBUG-064] Clearing cache")
            if hasattr(self.orchestrator, 'scraped_posts_cache'):
                self.orchestrator.scraped_posts_cache.clear()
                print("✅ [WORKFLOW-DEBUG-065] Cache cleared")
    
    async def _scrape_posts_from_api(self) -> List[VideoPost]:
        """Scrape posts using the FikFap API scraper with EXTREME debugging."""
        print("🚀 [WORKFLOW-DEBUG-066] Starting _scrape_posts_from_api()")
        try:
            self.logger.info("🚀 [WORKFLOW-DEBUG-067] Starting API scraping workflow (Pipeline Style)")
            
            # STEP A: Use API scraper to get posts in pipeline format
            print("🔧 [WORKFLOW-DEBUG-068] STEP A: Calling api_scraper.scrape_and_extract_pipeline_style()")
            scraping_results = await self.api_scraper.scrape_and_extract_pipeline_style()
            print(f"✅ [WORKFLOW-DEBUG-069] STEP A RESULT: scrape_and_extract_pipeline_style() returned")
            print(f"🔧 [WORKFLOW-DEBUG-070] Success: {scraping_results.get('success', False)}")
            
            if not scraping_results.get("success", False):
                error_msg = scraping_results.get("error", "Unknown error")
                print(f"❌ [WORKFLOW-DEBUG-071] Pipeline-style API scraping failed: {error_msg}")
                raise ScrapingError(f"Pipeline-style API scraping failed: {error_msg}")
            
            extracted_posts = scraping_results.get("extracted_posts", [])
            print(f"✅ [WORKFLOW-DEBUG-072] Got {len(extracted_posts)} extracted posts")
            
            if not extracted_posts:
                print("❌ [WORKFLOW-DEBUG-073] No posts extracted from pipeline-style API scraping")
                raise ScrapingError("No posts extracted from pipeline-style API scraping")
            
            self.logger.info(
                f"✅ [WORKFLOW-DEBUG-074] Pipeline-style API scraping completed: {len(extracted_posts)} posts extracted and saved to {scraping_results.get('filename', 'integrated_extracted_posts.json')}"
            )
            
            # STEP B: Convert extracted posts back to VideoPost objects
            print("🔧 [WORKFLOW-DEBUG-075] STEP B: Converting extracted posts to VideoPost objects")
            video_posts = await self._convert_extracted_to_video_posts(extracted_posts)
            print(f"✅ [WORKFLOW-DEBUG-076] STEP B RESULT: Converted to {len(video_posts)} VideoPost objects")
            
            print("🎉 [WORKFLOW-DEBUG-077] _scrape_posts_from_api() COMPLETED SUCCESSFULLY")
            return video_posts
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-005] _scrape_posts_from_api() FAILED: {e}")
            self.logger.error(f"Failed to scrape posts from API (Pipeline Style): {e}")
            raise ScrapingError(f"Pipeline-style API scraping failed: {e}")
    
    async def _convert_extracted_to_video_posts(self, extracted_posts: List[Dict[str, Any]]) -> List[VideoPost]:
        """Convert extracted posts from pipeline format back to VideoPost objects with debugging."""
        print("🚀 [WORKFLOW-DEBUG-078] Starting _convert_extracted_to_video_posts()")
        try:
            print(f"🔧 [WORKFLOW-DEBUG-079] Input: {len(extracted_posts)} extracted posts to convert")
            self.logger.info(f"🔧 [WORKFLOW-DEBUG-080] Converting {len(extracted_posts)} extracted posts to VideoPost objects")
            
            video_posts = []
            
            for i, extracted_post in enumerate(extracted_posts, 1):
                print(f"🔧 [WORKFLOW-DEBUG-081-{i}] Processing extracted post {i}/{len(extracted_posts)}")
                try:
                    post_id = extracted_post.get('postId', 'unknown')
                    print(f"🔧 [WORKFLOW-DEBUG-082-{i}] Post ID: {post_id}")
                    
                    # Create a simplified VideoPost object from extracted data
                    print(f"🔧 [WORKFLOW-DEBUG-083-{i}] Creating VideoPost object for post {post_id}")
                    
                    video_post = self._create_simple_video_post(extracted_post)
                    print(f"✅ [WORKFLOW-DEBUG-084-{i}] VideoPost created: {video_post is not None}")
                    
                    if video_post:
                        video_posts.append(video_post)
                        print(f"✅ [WORKFLOW-DEBUG-085-{i}] Post {i} added to video_posts list")
                    else:
                        print(f"❌ [WORKFLOW-DEBUG-086-{i}] Post {i} could not be converted")
                        
                except Exception as e:
                    print(f"❌ [WORKFLOW-DEBUG-ERROR-006-{i}] Error converting extracted post {extracted_post.get('postId', 'unknown')}: {e}")
                    self.logger.error(f"Error converting extracted post {extracted_post.get('postId', 'unknown')}: {e}")
                    continue
            
            print(f"✅ [WORKFLOW-DEBUG-087] Final conversion result: {len(video_posts)}/{len(extracted_posts)} posts converted")
            self.logger.info(f"✅ [WORKFLOW-DEBUG-088] Successfully converted {len(video_posts)}/{len(extracted_posts)} posts to VideoPost objects")
            
            print("🎉 [WORKFLOW-DEBUG-089] _convert_extracted_to_video_posts() COMPLETED SUCCESSFULLY")
            return video_posts
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-007] _convert_extracted_to_video_posts() FAILED: {e}")
            self.logger.error(f"Failed to convert extracted posts to VideoPost objects: {e}")
            raise ProcessingError(f"Post conversion failed: {e}")
    
    def _create_simple_video_post(self, extracted_post: Dict[str, Any]) -> Optional[VideoPost]:
        """Create a simple VideoPost object from extracted post data."""
        print("🔧 [WORKFLOW-DEBUG-090] Creating simple VideoPost object")
        try:
            # Create a VideoPost-like object with minimal required fields
            class SimpleVideoPost:
                def __init__(self, post_data):
                    self.post_id = post_data.get("postId")
                    self.title = post_data.get("title", "")
                    self.author = post_data.get("author", {})
                    self.video_urls = post_data.get("videoUrls", {})
                    self.thumbnail_url = post_data.get("thumbnail")
                    self.duration = post_data.get("duration", 0)
                    self.tags = post_data.get("tags", [])
                    self.score = post_data.get("score", 0)
                    self.views = post_data.get("views", 0)
                    self.created_at = post_data.get("createdAt", "")
            
            video_post = SimpleVideoPost(extracted_post)
            print(f"✅ [WORKFLOW-DEBUG-091] SimpleVideoPost created for post {video_post.post_id}")
            return video_post
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-008] Error creating simple VideoPost: {e}")
            return None
    
    def _cache_scraped_posts(self, scraped_posts: List[VideoPost]):
        """Cache scraped posts for orchestrator integration with debugging."""
        print("🚀 [WORKFLOW-DEBUG-092] Starting _cache_scraped_posts()")
        try:
            print(f"🔧 [WORKFLOW-DEBUG-093] Caching {len(scraped_posts)} posts")
            
            for i, post in enumerate(scraped_posts, 1):
                post_id = post.post_id
                self.orchestrator.scraped_posts_cache[post_id] = post
                print(f"🔧 [WORKFLOW-DEBUG-094-{i}] Cached post {post_id}")
            
            print(f"✅ [WORKFLOW-DEBUG-095] Cached {len(scraped_posts)} posts for orchestrator")
            self.logger.debug(f"Cached {len(scraped_posts)} posts for orchestrator")
            
            print("🎉 [WORKFLOW-DEBUG-096] _cache_scraped_posts() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-009] _cache_scraped_posts() FAILED: {e}")
            self.logger.error(f"Failed to cache scraped posts: {e}")
    
    async def _update_workflow_stats(self, posts_scraped: int, processing_result, cycle_duration: float):
        """Update workflow statistics with debugging."""
        print("🚀 [WORKFLOW-DEBUG-097] Starting _update_workflow_stats()")
        try:
            print(f"🔧 [WORKFLOW-DEBUG-098] Updating stats: {posts_scraped} scraped, {processing_result.successful} processed")
            
            self.workflow_stats["cycles_completed"] += 1
            self.workflow_stats["total_posts_scraped"] += posts_scraped
            self.workflow_stats["total_posts_processed"] += processing_result.successful
            self.workflow_stats["total_posts_failed"] += processing_result.failed
            
            if processing_result.successful > 0:
                self.workflow_stats["successful_cycles"] += 1
            else:
                self.workflow_stats["failed_cycles"] += 1
            
            # Update average cycle duration
            cycles = self.workflow_stats["cycles_completed"]
            old_avg = self.workflow_stats["average_cycle_duration"]
            self.workflow_stats["average_cycle_duration"] = (old_avg * (cycles - 1) + cycle_duration) / cycles
            
            self.workflow_stats["last_cycle_time"] = datetime.now()
            
            print("✅ [WORKFLOW-DEBUG-099] Workflow statistics updated successfully")
            print("🎉 [WORKFLOW-DEBUG-100] _update_workflow_stats() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-010] _update_workflow_stats() FAILED: {e}")
            self.logger.error(f"Failed to update workflow stats: {e}")
    
    async def cleanup(self):
        """Clean up all workflow components with debugging."""
        print("🚀 [WORKFLOW-DEBUG-101] Starting cleanup()")
        try:
            self.logger.info("🔧 [WORKFLOW-DEBUG-102] Cleaning up FikFap Workflow Integrator")
            
            if self.orchestrator:
                print("🔧 [WORKFLOW-DEBUG-103] Shutting down orchestrator")
                await self.orchestrator.shutdown()
                print("✅ [WORKFLOW-DEBUG-104] Orchestrator shutdown completed")
            
            if self.api_scraper:
                print("🔧 [WORKFLOW-DEBUG-105] Closing API scraper")
                await self.api_scraper.close()
                print("✅ [WORKFLOW-DEBUG-106] API scraper closed")
            
            self.is_initialized = False
            print("✅ [WORKFLOW-DEBUG-107] Cleanup flags set")
            
            self.logger.info("✅ [WORKFLOW-DEBUG-108] FikFap Workflow Integrator cleanup completed")
            print("🎉 [WORKFLOW-DEBUG-109] cleanup() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [WORKFLOW-DEBUG-ERROR-011] cleanup() FAILED: {e}")
            self.logger.error(f"Error during workflow cleanup: {e}")
    
    def get_workflow_stats(self) -> Dict[str, Any]:
        """Get current workflow statistics."""
        stats = dict(self.workflow_stats)
        
        if self.workflow_stats["start_time"]:
            runtime = (datetime.now() - self.workflow_stats["start_time"]).total_seconds()
            stats["total_runtime_seconds"] = runtime
            stats["cycles_per_hour"] = (self.workflow_stats["cycles_completed"] / runtime) * 3600 if runtime > 0 else 0
            stats["posts_per_hour"] = (self.workflow_stats["total_posts_processed"] / runtime) * 3600 if runtime > 0 else 0
        
        stats["success_rate"] = (
            (self.workflow_stats["successful_cycles"] / self.workflow_stats["cycles_completed"]) * 100
            if self.workflow_stats["cycles_completed"] > 0 else 0
        )
        
        return stats


# For backward compatibility, also create the FikFapContinuousRunner class
class FikFapContinuousRunner:
    """Continuous runner for the workflow integrator."""
    
    def __init__(self, integrator: FikFapWorkflowIntegrator, config_override: Optional[Dict[str, Any]] = None):
        self.integrator = integrator
        self.config_override = config_override or {}
        self.stop_requested = False
        
    def request_stop(self):
        """Request stop of continuous loop."""
        self.stop_requested = True
        
    async def run_continuous_loop(self):
        """Run continuous loop."""
        interval = self.config_override.get("continuous.loop_interval", 300)
        
        while not self.stop_requested:
            try:
                await self.integrator.run_single_cycle()
                await asyncio.sleep(interval)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Continuous loop error: {e}")
                await asyncio.sleep(60)  # Wait before retry