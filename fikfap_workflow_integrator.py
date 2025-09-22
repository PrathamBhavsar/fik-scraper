# fikfap_workflow_integrator.py
"""
FikFap Workflow Integrator - Complete integration with existing orchestrator system.

This module:
1. Uses FikFapAPIScraper to get 5+9 posts from API calls
2. Integrates with existing FikFapScraperOrchestrator for processing
3. Handles the complete workflow from scraping to downloading
4. Supports both single cycles and continuous operation
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
    """
    Complete workflow integrator that combines API scraping with your existing pipeline.
    
    This class:
    - Manages the FikFap API scraper
    - Integrates with your existing orchestrator
    - Handles the complete 5+9 posts workflow
    - Provides both single-cycle and continuous operation modes
    """
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        self.logger = setup_logger(self.__class__.__name__)
        self.config = Config()
        
        # Apply config overrides if provided
        if config_override:
            self.config.update(config_override)
        
        # Initialize components
        self.api_scraper: Optional[FikFapAPIScraper] = None
        self.orchestrator: Optional[FikFapScraperOrchestrator] = None
        
        # State management
        self.is_initialized = False
        self.current_cycle = 0
        self.total_posts_processed = 0
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
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()
    
    async def initialize(self):
        """Initialize all workflow components."""
        try:
            self.logger.info("Initializing FikFap Workflow Integrator")
            
            # Initialize API scraper
            self.logger.info("Initializing API scraper")
            self.api_scraper = FikFapAPIScraper()  # Fixed: No session parameter needed
            await self.api_scraper.start()
            
            # Initialize your existing orchestrator
            self.logger.info("Initializing orchestrator")
            self.orchestrator = FikFapScraperOrchestrator()
            await self.orchestrator.startup()
            
            # Setup integration hooks
            await self._setup_integration_hooks()
            
            self.is_initialized = True
            self.workflow_stats["start_time"] = datetime.now()
            
            self.logger.info("FikFap Workflow Integrator initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize workflow integrator: {e}")
            raise StartupError(f"Workflow integrator initialization failed: {e}")
    
    async def cleanup(self):
        """Clean up all workflow components."""
        try:
            self.logger.info("Cleaning up FikFap Workflow Integrator")
            
            if self.orchestrator:
                await self.orchestrator.shutdown()
                
            if self.api_scraper:
                await self.api_scraper.close()
            
            self.is_initialized = False
            
            self.logger.info("FikFap Workflow Integrator cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error during workflow cleanup: {e}")
    
    async def _setup_integration_hooks(self):
        """Setup integration between API scraper and orchestrator."""
        try:
            # Override the orchestrator's _extract_video_data method
            # This allows the orchestrator to get data from our scraped posts
            self.orchestrator.scraped_posts_cache = {}
            
            original_extract = self.orchestrator._extract_video_data
            
            async def integrated_extract_video_data(post_id: int):
                """Integrated extraction that uses scraped data."""
                # Check if we have this post in our scraped cache
                if post_id in self.orchestrator.scraped_posts_cache:
                    post = self.orchestrator.scraped_posts_cache[post_id]
                    self.logger.debug(f"Using scraped data for post {post_id}")
                    return post
                
                # Fallback to original method
                return await original_extract(post_id)
            
            # Replace the method
            self.orchestrator._extract_video_data = integrated_extract_video_data
            
            self.logger.debug("Integration hooks setup completed")
            
        except Exception as e:
            self.logger.error(f"Failed to setup integration hooks: {e}")
            raise ComponentError(f"Integration hooks setup failed: {e}")
    
    async def run_single_cycle(self) -> Dict[str, Any]:
        """
        Run a single complete workflow cycle.
        
        This method:
        1. Scrapes 5+9 posts from FikFap API
        2. Processes all posts through your existing orchestrator
        3. Downloads videos, thumbnails, and metadata
        4. Returns comprehensive results
        
        Returns:
            Dict containing cycle results and statistics
        """
        try:
            cycle_start_time = time.time()
            self.current_cycle += 1
            
            self.logger.info(f"Starting workflow cycle #{self.current_cycle}")
            
            if not self.is_initialized:
                raise ProcessingError("Workflow integrator not initialized")
            
            # Step 1: Scrape posts from FikFap API
            self.logger.info("Step 1: Scraping posts from FikFap API")
            scraped_posts = await self._scrape_posts_from_api()
            
            if not scraped_posts:
                return {
                    "success": False,
                    "cycle": self.current_cycle,
                    "error": "No posts scraped from API",
                    "posts_scraped": 0,
                    "posts_processed": 0
                }
            
            self.logger.info(f"Step 1 completed: {len(scraped_posts)} posts scraped")
            
            # Step 2: Cache scraped posts for orchestrator
            self.logger.info("Step 2: Caching posts for orchestrator integration")
            self._cache_scraped_posts(scraped_posts)
            
            # Step 3: Process posts through your existing orchestrator
            self.logger.info("Step 3: Processing posts through orchestrator pipeline")
            post_ids = [post.post_id for post in scraped_posts]
            
            # Use your orchestrator's batch processing
            processing_result = await self.orchestrator.process_multiple_videos(
                post_ids=post_ids,
                max_concurrent=self.config.get('processing.max_concurrent', 2),
                quality_filter=None
            )
            
            self.logger.info(f"Step 3 completed: {processing_result.successful} processed successfully")
            
            # Step 4: Update workflow statistics
            cycle_duration = time.time() - cycle_start_time
            await self._update_workflow_stats(len(scraped_posts), processing_result, cycle_duration)
            
            # Step 5: Prepare results
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
            
            self.logger.info(
                f"Cycle #{self.current_cycle} completed successfully: "
                f"{len(scraped_posts)} scraped, {processing_result.successful} processed "
                f"in {cycle_duration:.2f}s"
            )
            
            return cycle_result
            
        except Exception as e:
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
            if hasattr(self.orchestrator, 'scraped_posts_cache'):
                self.orchestrator.scraped_posts_cache.clear()
    
    async def _scrape_posts_from_api(self) -> List[VideoPost]:
        """Scrape posts using the FikFap API scraper."""
        try:
            self.logger.info("Starting API scraping workflow")
            
            # Use the API scraper to get all 14 posts (5+9)
            scraped_posts = await self.api_scraper.scrape_complete_workflow()
            
            self.logger.info(
                f"API scraping completed: {len(scraped_posts)} posts retrieved and validated"
            )
            
            return scraped_posts
            
        except Exception as e:
            self.logger.error(f"Failed to scrape posts from API: {e}")
            raise ScrapingError(f"API scraping failed: {e}")
    
    def _cache_scraped_posts(self, scraped_posts: List[VideoPost]):
        """Cache scraped posts for orchestrator integration."""
        try:
            # Store posts in orchestrator cache by post_id
            for post in scraped_posts:
                self.orchestrator.scraped_posts_cache[post.post_id] = post
            
            self.logger.debug(f"Cached {len(scraped_posts)} posts for orchestrator")
            
        except Exception as e:
            self.logger.error(f"Failed to cache scraped posts: {e}")
    
    async def _update_workflow_stats(self, posts_scraped: int, processing_result, cycle_duration: float):
        """Update workflow statistics."""
        try:
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
            
        except Exception as e:
            self.logger.error(f"Failed to update workflow stats: {e}")
    
    def get_workflow_stats(self) -> Dict[str, Any]:
        """Get current workflow statistics."""
        stats = dict(self.workflow_stats)
        
        # Add calculated metrics
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
    
    async def run_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check."""
        try:
            health_status = {
                "workflow_integrator": {
                    "initialized": self.is_initialized,
                    "current_cycle": self.current_cycle
                },
                "api_scraper": {"healthy": False},
                "orchestrator": {"healthy": False},
                "overall_health": False
            }
            
            # Check API scraper
            if self.api_scraper:
                scraper_health = await self.api_scraper.health_check()
                health_status["api_scraper"] = scraper_health
            
            # Check orchestrator
            if self.orchestrator:
                orchestrator_status = self.orchestrator.get_system_status()
                health_status["orchestrator"] = orchestrator_status
            
            # Determine overall health
            health_status["overall_health"] = (
                self.is_initialized and
                health_status["api_scraper"].get("status") == "healthy" and
                health_status["orchestrator"].get("orchestrator", {}).get("running", False)
            )
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "overall_health": False,
                "error": str(e)
            }


class FikFapContinuousRunner:
    """
    Continuous runner that executes the workflow in a loop.
    
    Features:
    - Configurable loop intervals
    - Error recovery and retry logic
    - Graceful shutdown handling
    - Comprehensive monitoring and statistics
    """
    
    def __init__(self, workflow_integrator: FikFapWorkflowIntegrator, config_override: Optional[Dict[str, Any]] = None):
        self.logger = setup_logger(self.__class__.__name__)
        self.workflow_integrator = workflow_integrator
        self.config = Config()
        
        if config_override:
            self.config.update(config_override)
        
        # State management
        self.is_running = False
        self.should_stop = False
        self.pause_requested = False
        
        # Configuration
        self.loop_interval = self.config.get('continuous.loop_interval', 300)  # 5 minutes default
        self.max_consecutive_failures = self.config.get('continuous.max_consecutive_failures', 5)
        self.consecutive_failures = 0
        self.recovery_delay = self.config.get('continuous.recovery_delay', 60)  # 1 minute default
        
        # Statistics
        self.runner_stats = {
            "start_time": None,
            "total_cycles_attempted": 0,
            "successful_cycles": 0,
            "failed_cycles": 0,
            "consecutive_failures": 0,
            "last_cycle_time": None,
            "average_posts_per_cycle": 0.0,
            "uptime_seconds": 0.0
        }
    
    async def run_continuous_loop(self):
        """Run the continuous processing loop."""
        try:
            self.logger.info("Starting continuous FikFap processing loop")
            self.is_running = True
            self.runner_stats["start_time"] = datetime.now()
            
            while not self.should_stop:
                try:
                    # Handle pause requests
                    if self.pause_requested:
                        await self._handle_pause()
                        continue
                    
                    # Pre-cycle health check
                    if not await self._pre_cycle_health_check():
                        await self._handle_health_failure()
                        continue
                    
                    # Run workflow cycle
                    self.logger.info("Starting new workflow cycle")
                    cycle_result = await self.workflow_integrator.run_single_cycle()
                    
                    # Update statistics
                    self.runner_stats["total_cycles_attempted"] += 1
                    self.runner_stats["last_cycle_time"] = datetime.now()
                    
                    if cycle_result.get("success", False):
                        self.consecutive_failures = 0
                        self.runner_stats["successful_cycles"] += 1
                        
                        # Update average posts per cycle
                        posts_processed = cycle_result.get("posts_processed", 0)
                        total_cycles = self.runner_stats["successful_cycles"]
                        old_avg = self.runner_stats["average_posts_per_cycle"]
                        self.runner_stats["average_posts_per_cycle"] = (
                            (old_avg * (total_cycles - 1) + posts_processed) / total_cycles
                        )
                        
                        self.logger.info(f"Cycle completed successfully: {posts_processed} posts processed")
                    else:
                        self.consecutive_failures += 1
                        self.runner_stats["failed_cycles"] += 1
                        
                        error = cycle_result.get("error", "Unknown error")
                        self.logger.error(f"Cycle failed: {error}")
                        
                        # Check for too many consecutive failures
                        if self.consecutive_failures >= self.max_consecutive_failures:
                            await self._handle_consecutive_failures()
                            continue
                    
                    # Wait for next cycle
                    if not self.should_stop:
                        await self._wait_for_next_cycle()
                
                except Exception as e:
                    self.logger.error(f"Error in continuous loop cycle: {e}")
                    self.consecutive_failures += 1
                    self.runner_stats["failed_cycles"] += 1
                    
                    await self._handle_cycle_error(e)
            
            self.logger.info("Continuous processing loop ended")
            
        except Exception as e:
            self.logger.error(f"Fatal error in continuous loop: {e}")
            raise ProcessingError(f"Continuous loop failed: {e}")
        finally:
            self.is_running = False
            
            # Update final stats
            if self.runner_stats["start_time"]:
                self.runner_stats["uptime_seconds"] = (
                    datetime.now() - self.runner_stats["start_time"]
                ).total_seconds()
    
    async def _pre_cycle_health_check(self) -> bool:
        """Perform health check before starting a cycle."""
        try:
            health_status = await self.workflow_integrator.run_health_check()
            return health_status.get("overall_health", False)
        except Exception as e:
            self.logger.error(f"Pre-cycle health check failed: {e}")
            return False
    
    async def _handle_health_failure(self):
        """Handle health check failure."""
        self.logger.warning("Health check failed, waiting before retry")
        await asyncio.sleep(self.recovery_delay)
    
    async def _handle_consecutive_failures(self):
        """Handle too many consecutive failures."""
        self.logger.error(
            f"Too many consecutive failures ({self.consecutive_failures}), "
            f"entering recovery mode for {self.recovery_delay * 2}s"
        )
        
        await asyncio.sleep(self.recovery_delay * 2)
        self.consecutive_failures = 0
    
    async def _handle_cycle_error(self, error: Exception):
        """Handle errors that occur during a cycle."""
        self.logger.error(f"Cycle error: {error}")
        await asyncio.sleep(self.recovery_delay)
    
    async def _wait_for_next_cycle(self):
        """Wait for the configured interval before the next cycle."""
        self.logger.info(f"Waiting {self.loop_interval}s before next cycle")
        
        # Wait in smaller increments to allow for graceful shutdown
        wait_chunks = max(1, self.loop_interval // 10)
        chunk_duration = self.loop_interval / wait_chunks
        
        for _ in range(int(wait_chunks)):
            if self.should_stop or self.pause_requested:
                break
            await asyncio.sleep(chunk_duration)
    
    async def _handle_pause(self):
        """Handle pause requests."""
        self.logger.info("Processing paused")
        while self.pause_requested and not self.should_stop:
            await asyncio.sleep(1)
        if not self.should_stop:
            self.logger.info("Processing resumed")
    
    def request_stop(self):
        """Request graceful shutdown."""
        self.logger.info("Graceful shutdown requested")
        self.should_stop = True
    
    def request_pause(self):
        """Request pause."""
        self.pause_requested = True
    
    def request_resume(self):
        """Request resume from pause."""
        self.pause_requested = False
    
    def get_runner_stats(self) -> Dict[str, Any]:
        """Get current runner statistics."""
        stats = dict(self.runner_stats)
        stats["is_running"] = self.is_running
        stats["should_stop"] = self.should_stop
        stats["pause_requested"] = self.pause_requested
        stats["consecutive_failures"] = self.consecutive_failures
        
        # Add calculated metrics
        if self.runner_stats["start_time"]:
            uptime = (datetime.now() - self.runner_stats["start_time"]).total_seconds()
            stats["uptime_seconds"] = uptime
            
            if uptime > 0:
                stats["cycles_per_hour"] = (self.runner_stats["total_cycles_attempted"] / uptime) * 3600
        
        return stats