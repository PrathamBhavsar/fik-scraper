# fikfap_api_scraper.py
"""
FikFap API Scraper - Intercepts API calls and integrates with existing pipeline.

This module handles:
1. Browser automation with Playwright
2. API interception for both initial (5 posts) and paginated (9 posts) calls
3. Data extraction and validation
4. Integration with existing orchestrator workflow
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Response

# Import your existing components
from core.base_scraper import BaseScraper
from core.config import Config
from data.models import VideoPost, ProcessingStatus
from data.extractor import FikFapDataExtractor
from data.validator import DataValidator
from enhanced_exceptions import ExtractionError, ScrapingError
from utils.logger import setup_logger


class FikFapAPIScraper(BaseScraper):
    """
    FikFap-specific API scraper that intercepts API calls and processes responses.
    
    This scraper:
    - Opens fikfap.com in a browser
    - Intercepts the first API call: /cached-high-quality/posts?amount=5
    - Extracts 5 posts and gets the last postId for pagination
    - Triggers scrolling to get the second API call: /posts?amount=9&afterId={id}
    - Processes all 14 posts through your existing pipeline
    """
    
    def __init__(self):
        # Call parent constructor (no parameters needed)
        super().__init__()
        self.logger = setup_logger(self.__class__.__name__)
        self.config = Config()
        
        # Initialize your existing components
        
        self.base_scraper = BaseScraper()
        self.extractor = FikFapDataExtractor(self.base_scraper)
        self.validator = DataValidator()
        
        # Browser automation components
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        
        # State management
        self.intercepted_responses: Dict[str, Any] = {}
        self.pagination_state = {"last_post_id": None, "has_more": True}
        self.current_posts: List[VideoPost] = []
        
        # API endpoints
        self.site_url = "https://fikfap.com"
        self.initial_api_pattern = "/cached-high-quality/posts"
        self.pagination_api_pattern = "/posts?amount=9&afterId="
        
    async def start(self):
        """Initialize browser and setup API interception."""
        try:
            self.logger.info("Initializing FikFap API Scraper")
            
            # Initialize the parent session first
            await self.start_session()
            
            # Initialize Playwright
            self.playwright = await async_playwright().start()
            
            # Launch browser with appropriate settings
            browser_config = {
                "headless": self.config.get('scraper.headless', True),
                "slow_mo": self.config.get('scraper.slow_mo', 0),
                "timeout": self.config.get('scraper.timeout', 30000),
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled"
                ]
            }
            
            self.browser = await self.playwright.chromium.launch(**browser_config)
            
            # Create context with realistic settings
            context_config = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "extra_http_headers": {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
            }
            
            self.context = await self.browser.new_context(**context_config)
            self.page = await self.context.new_page()
            
            # Setup API response interception
            await self._setup_api_interception()
            
            self.logger.info("FikFap API Scraper initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize FikFap API scraper: {e}")
            raise ScrapingError(f"Scraper initialization failed: {e}")
    
    async def close(self):
        """Clean up browser resources."""
        try:
            self.logger.info("Closing FikFap API Scraper")
            
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
                
            # Close parent session
            await self.close_session()
                
            self.logger.info("FikFap API Scraper closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing FikFap API scraper: {e}")
    
    async def _setup_api_interception(self):
        """Setup request/response interception for FikFap API calls."""
        try:
            self.logger.debug("Setting up API interception")
            
            async def handle_response(response: Response):
                """Handle intercepted API responses."""
                url = response.url
                
                # Check if this is a FikFap API endpoint we're interested in
                if self._is_target_api_endpoint(url):
                    try:
                        # Parse response data
                        response_data = await response.json()
                        endpoint_key = self._get_endpoint_key(url)
                        
                        # Store intercepted response
                        self.intercepted_responses[endpoint_key] = {
                            "url": url,
                            "status": response.status,
                            "data": response_data,
                            "headers": dict(response.headers),
                            "timestamp": time.time()
                        }
                        
                        self.logger.info(
                            f"Intercepted API response: {endpoint_key} "
                            f"({len(response_data)} posts, status: {response.status})"
                        )
                        
                    except Exception as e:
                        self.logger.error(f"Failed to process intercepted response: {e}")
            
            # Register response handler
            self.page.on("response", handle_response)
            
            self.logger.debug("API interception setup completed")
            
        except Exception as e:
            self.logger.error(f"Failed to setup API interception: {e}")
            raise ScrapingError(f"API interception setup failed: {e}")
    
    def _is_target_api_endpoint(self, url: str) -> bool:
        """Check if URL is a target FikFap API endpoint."""
        target_patterns = [
            "/cached-high-quality/posts",
            "/posts?amount=9&afterId=",
            "api.fikfap.com"
        ]
        return any(pattern in url for pattern in target_patterns)
    
    def _get_endpoint_key(self, url: str) -> str:
        """Generate a key for the intercepted endpoint."""
        if "/cached-high-quality/posts" in url:
            return "initial_batch"
        elif "/posts?amount=9&afterId=" in url:
            return "pagination_batch"
        else:
            return "unknown_api"
    
    async def scrape_complete_workflow(self) -> List[VideoPost]:
        """
        Execute the complete FikFap scraping workflow.
        
        This is the main method that:
        1. Opens fikfap.com
        2. Waits for first API call (5 posts)
        3. Triggers scrolling for second API call (9 posts)
        4. Processes all 14 posts
        5. Returns validated VideoPost objects
        
        Returns:
            List[VideoPost]: 14 processed and validated video posts
        """
        try:
            self.logger.info("Starting complete FikFap scraping workflow")
            
            # Step 1: Navigate to FikFap and wait for initial API call
            self.logger.info("Step 1: Navigating to FikFap and waiting for initial API call")
            initial_posts = await self._scrape_initial_batch()
            
            if not initial_posts:
                raise ScrapingError("Failed to get initial batch of posts")
            
            self.logger.info(f"Step 1 completed: Retrieved {len(initial_posts)} initial posts")
            
            # Step 2: Extract pagination ID and trigger second API call
            self.logger.info("Step 2: Extracting pagination ID and triggering scroll for next batch")
            pagination_id = self._extract_pagination_id(initial_posts)
            
            if not pagination_id:
                self.logger.warning("No pagination ID found, returning only initial batch")
                return await self._process_posts_to_video_objects(initial_posts)
            
            # Step 3: Trigger scrolling and wait for second API call
            next_posts = await self._scrape_next_batch(pagination_id)
            
            if not next_posts:
                self.logger.warning("Failed to get next batch, returning only initial batch")
                return await self._process_posts_to_video_objects(initial_posts)
            
            self.logger.info(f"Step 3 completed: Retrieved {len(next_posts)} additional posts")
            
            # Step 4: Combine all posts (5 + 9 = 14)
            all_posts = initial_posts + next_posts
            self.logger.info(f"Step 4: Combined total posts: {len(all_posts)}")
            
            # Step 5: Process into VideoPost objects and validate
            processed_posts = await self._process_posts_to_video_objects(all_posts)
            
            self.logger.info(
                f"Complete workflow finished successfully: "
                f"{len(processed_posts)} posts processed and ready for pipeline"
            )
            
            return processed_posts
            
        except Exception as e:
            self.logger.error(f"Complete workflow failed: {e}")
            raise ScrapingError(f"FikFap scraping workflow failed: {e}")
    
    async def _scrape_initial_batch(self) -> List[Dict[str, Any]]:
        """Scrape the initial batch of 5 posts."""
        try:
            self.logger.info("Scraping initial batch (5 posts)")
            
            # Clear any previous responses
            self.intercepted_responses.clear()
            
            # Navigate to FikFap
            self.logger.debug("Navigating to FikFap.com")
            await self.page.goto(
                self.site_url, 
                wait_until="networkidle",
                timeout=30000
            )
            
            # Wait for the initial API call to be intercepted
            self.logger.debug("Waiting for initial API call to be intercepted")
            initial_response = await self._wait_for_api_response("initial_batch", timeout=30)
            
            if not initial_response:
                # Try refreshing the page if no API call was intercepted
                self.logger.warning("No initial API call intercepted, refreshing page")
                await self.page.reload(wait_until="networkidle")
                initial_response = await self._wait_for_api_response("initial_batch", timeout=20)
            
            if not initial_response:
                raise ScrapingError("Failed to intercept initial API call")
            
            posts_data = initial_response.get("data", [])
            self.logger.info(f"Successfully intercepted initial batch: {len(posts_data)} posts")
            
            return posts_data
            
        except Exception as e:
            self.logger.error(f"Failed to scrape initial batch: {e}")
            raise ScrapingError(f"Initial batch scraping failed: {e}")
    
    def _extract_pagination_id(self, posts_data: List[Dict[str, Any]]) -> Optional[int]:
        """Extract the last postId for pagination from the posts data."""
        try:
            if not posts_data:
                return None
                
            # Get the last post's ID
            last_post = posts_data[-1]
            pagination_id = last_post.get("postId")
            
            if pagination_id:
                self.logger.debug(f"Extracted pagination ID: {pagination_id}")
                self.pagination_state["last_post_id"] = pagination_id
                
            return pagination_id
            
        except Exception as e:
            self.logger.error(f"Failed to extract pagination ID: {e}")
            return None
    
    async def _scrape_next_batch(self, after_id: int) -> List[Dict[str, Any]]:
        """Scrape the next batch of 9 posts using pagination."""
        try:
            self.logger.info(f"Scraping next batch (9 posts) after ID: {after_id}")
            
            # Clear previous pagination response
            if "pagination_batch" in self.intercepted_responses:
                del self.intercepted_responses["pagination_batch"]
            
            # Trigger scrolling to load more content
            await self._trigger_pagination(after_id)
            
            # Wait for pagination API call
            pagination_response = await self._wait_for_api_response("pagination_batch", timeout=30)
            
            if not pagination_response:
                # Try alternative pagination methods
                self.logger.warning("Pagination API call not intercepted, trying alternative methods")
                await self._alternative_pagination_trigger(after_id)
                pagination_response = await self._wait_for_api_response("pagination_batch", timeout=20)
            
            if not pagination_response:
                raise ScrapingError("Failed to intercept pagination API call")
            
            posts_data = pagination_response.get("data", [])
            self.logger.info(f"Successfully intercepted next batch: {len(posts_data)} posts")
            
            return posts_data
            
        except Exception as e:
            self.logger.error(f"Failed to scrape next batch: {e}")
            raise ScrapingError(f"Next batch scraping failed: {e}")
    
    async def _trigger_pagination(self, after_id: int):
        """Trigger pagination by scrolling and other methods."""
        try:
            self.logger.debug("Triggering pagination through scrolling")
            
            # Method 1: Gradual scrolling to trigger infinite scroll
            for i in range(5):
                await self.page.evaluate("window.scrollBy(0, 1000)")
                await asyncio.sleep(1)
                
                # Check if API call was triggered
                if "pagination_batch" in self.intercepted_responses:
                    self.logger.debug("Pagination triggered by scrolling")
                    return
            
            # Method 2: Scroll to bottom rapidly
            await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)
            
            # Method 3: Multiple rapid scrolls
            for i in range(10):
                await self.page.evaluate("window.scrollBy(0, 500)")
                await asyncio.sleep(0.3)
                
                if "pagination_batch" in self.intercepted_responses:
                    self.logger.debug("Pagination triggered by rapid scrolling")
                    return
            
            self.logger.debug("Standard pagination methods completed")
            
        except Exception as e:
            self.logger.error(f"Failed to trigger pagination: {e}")
    
    async def _alternative_pagination_trigger(self, after_id: int):
        """Alternative methods to trigger pagination."""
        try:
            self.logger.debug("Trying alternative pagination methods")
            
            # Method 1: Look for and click load more buttons
            load_more_selectors = [
                'button:has-text("Load")',
                'button:has-text("More")',
                '[data-testid*="load"]',
                '.load-more',
                '.infinite-scroll-trigger'
            ]
            
            for selector in load_more_selectors:
                try:
                    if await self.page.locator(selector).count() > 0:
                        await self.page.click(selector, timeout=2000)
                        await asyncio.sleep(2)
                        
                        if "pagination_batch" in self.intercepted_responses:
                            self.logger.debug(f"Pagination triggered by clicking: {selector}")
                            return
                except:
                    continue
            
            # Method 2: Simulate mouse wheel scrolling
            await self.page.mouse.wheel(0, 5000)
            await asyncio.sleep(2)
            
            # Method 3: JavaScript-based pagination triggers
            js_triggers = [
                "window.dispatchEvent(new Event('scroll'))",
                "document.dispatchEvent(new Event('scroll'))",
                f"window.history.pushState(null, null, '?afterId={after_id}')",
            ]
            
            for js in js_triggers:
                try:
                    await self.page.evaluate(js)
                    await asyncio.sleep(1)
                    
                    if "pagination_batch" in self.intercepted_responses:
                        self.logger.debug("Pagination triggered by JavaScript")
                        return
                except:
                    continue
            
            self.logger.debug("Alternative pagination methods completed")
            
        except Exception as e:
            self.logger.error(f"Alternative pagination methods failed: {e}")
    
    async def _wait_for_api_response(self, endpoint_key: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Wait for a specific API response to be intercepted."""
        try:
            start_time = time.time()
            
            while (time.time() - start_time) < timeout:
                if endpoint_key in self.intercepted_responses:
                    response = self.intercepted_responses[endpoint_key]
                    self.logger.debug(f"API response received for {endpoint_key}")
                    return response
                
                await asyncio.sleep(0.1)
            
            self.logger.warning(f"Timeout waiting for API response: {endpoint_key}")
            return None
            
        except Exception as e:
            self.logger.error(f"Error waiting for API response: {e}")
            return None
    
    async def _process_posts_to_video_objects(self, posts_data: List[Dict[str, Any]]) -> List[VideoPost]:
        """Process raw API response data into VideoPost objects."""
        try:
            self.logger.info(f"Processing {len(posts_data)} posts into VideoPost objects")
            
            processed_posts = []
            
            for i, post_data in enumerate(posts_data):
                try:
                    # Use your existing extractor to create VideoPost object
                    video_post = self.extractor.extract_video_data(post_data)
                    
                    if video_post:
                        # Use your existing validator to validate the post
                        is_valid, validation_errors = self.validator.validate_post(video_post)
                        
                        if is_valid:
                            processed_posts.append(video_post)
                            self.logger.debug(f"Successfully processed post {video_post.post_id}")
                        else:
                            self.logger.warning(
                                f"Post {post_data.get('postId', 'unknown')} failed validation: "
                                f"{validation_errors}"
                            )
                    else:
                        self.logger.warning(f"Failed to extract video data from post {i+1}")
                        
                except Exception as e:
                    self.logger.error(f"Error processing post {i+1}: {e}")
                    continue
            
            self.logger.info(
                f"Successfully processed {len(processed_posts)}/{len(posts_data)} posts"
            )
            
            return processed_posts
            
        except Exception as e:
            self.logger.error(f"Failed to process posts to VideoPost objects: {e}")
            raise ExtractionError(f"Post processing failed: {e}")
    
    def get_pagination_state(self) -> Dict[str, Any]:
        """Get current pagination state."""
        return {
            **self.pagination_state,
            "intercepted_responses_count": len(self.intercepted_responses),
            "current_posts_count": len(self.current_posts)
        }
    
    def reset_pagination_state(self):
        """Reset pagination state for a new scraping cycle."""
        self.pagination_state = {"last_post_id": None, "has_more": True}
        self.intercepted_responses.clear()
        self.current_posts.clear()
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on scraper components."""
        try:
            health_status = {
                "browser_active": self.browser is not None,
                "context_active": self.context is not None,
                "page_active": self.page is not None,
                "intercepted_responses_count": len(self.intercepted_responses),
                "pagination_state": self.pagination_state,
                "status": "healthy"
            }
            
            # Check if browser is actually responsive
            if self.page:
                try:
                    await self.page.title()
                    health_status["page_responsive"] = True
                except:
                    health_status["page_responsive"] = False
                    health_status["status"] = "degraded"
            
            # Overall health
            if not all([self.browser, self.context, self.page]):
                health_status["status"] = "unhealthy"
            
            return health_status
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e)
            }