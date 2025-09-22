# WORKING FIX - fikfap_api_scraper.py 
"""
FIXED: Updated pagination method for FikFap's new API structure

ISSUE IDENTIFIED: The pagination API call is not being triggered by scrolling.
FIX: Use direct API call construction instead of waiting for triggered calls.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Response, Request

# Import your existing components
from core.base_scraper import BaseScraper
from core.config import Config
from data.models import VideoPost, ProcessingStatus
from data.extractor import FikFapDataExtractor
from data.validator import DataValidator
from enhanced_exceptions import ExtractionError, ScrapingError
from utils.logger import setup_logger


class FikFapAPIScraper(BaseScraper):
    """FikFap-specific API scraper with FIXED PAGINATION."""
    
    def __init__(self):
        print("🔧 [DEBUG-001] Starting FikFapAPIScraper.__init__()")
        try:
            super().__init__()
            print("🔧 [DEBUG-002] super().__init__() completed")
            
            self.logger = setup_logger(self.__class__.__name__)
            print("🔧 [DEBUG-003] Logger setup completed")
            
            self.config = Config()
            print("🔧 [DEBUG-004] Config loaded")
            
            self.base_scraper = BaseScraper()
            print("🔧 [DEBUG-005] BaseScraper initialized")
            
            self.extractor = FikFapDataExtractor(self.base_scraper)
            print("🔧 [DEBUG-006] FikFapDataExtractor initialized")
            
            self.validator = DataValidator()
            print("🔧 [DEBUG-007] DataValidator initialized")
            
            # Browser automation components
            self.playwright = None
            self.browser: Optional[Browser] = None
            self.context: Optional[BrowserContext] = None
            self.page: Optional[Page] = None
            print("🔧 [DEBUG-008] Browser components initialized as None")
            
            # State management
            self.intercepted_responses: Dict[str, Any] = {}
            self.pagination_state = {"last_post_id": None, "has_more": True}
            self.current_posts: List[VideoPost] = []
            print("🔧 [DEBUG-009] State management initialized")
            
            # API endpoints
            self.site_url = "https://fikfap.com"
            self.api_base_url = "https://api.fikfap.com"
            self.view_api_base_url = "https://view-api.fikfap.com"
            print("🔧 [DEBUG-010] API endpoints set")
            
            # Track all network requests for debugging
            self.all_requests: List[Dict] = []
            self.all_responses: List[Dict] = []
            print("🔧 [DEBUG-011] Request tracking initialized")
            
            # Pipeline integration
            self.extracted_posts: List[Dict[str, Any]] = []
            print("🔧 [DEBUG-012] Pipeline integration initialized")
            
            print("✅ [DEBUG-013] FikFapAPIScraper.__init__() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-001] FikFapAPIScraper.__init__() FAILED: {e}")
            raise
    
    async def start(self):
        """Initialize browser and setup API interception."""
        print("🚀 [DEBUG-014] Starting FikFapAPIScraper.start()")
        try:
            self.logger.info("🚀 [DEBUG-015] Initializing FikFap API Scraper")
            
            await self.start_session()
            print("✅ [DEBUG-017] Step 1: Parent session initialized")
            
            self.playwright = await async_playwright().start()
            print("✅ [DEBUG-019] Step 2: Playwright started successfully")
            
            browser_config = {
                "headless": False,
                "slow_mo": 1000,
                "timeout": 60000,
                "args": [
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--allow-running-insecure-content",
                    "--disable-features=VizDisplayCompositor"
                ]
            }
            print(f"🔧 [DEBUG-021] Browser config: {browser_config}")
            
            self.browser = await self.playwright.chromium.launch(**browser_config)
            print("✅ [DEBUG-023] Step 3: Browser launched successfully")
            
            context_config = {
                "viewport": {"width": 1920, "height": 1080},
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "extra_http_headers": {
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-site"
                },
                "ignore_https_errors": True
            }
            
            self.context = await self.browser.new_context(**context_config)
            print("✅ [DEBUG-026] Step 4: Browser context created")
            
            self.page = await self.context.new_page()
            print("✅ [DEBUG-028] Step 5: Page created successfully")
            
            await self.page.route("**/*", self._handle_request)
            print("✅ [DEBUG-030] Step 6: Request interception setup")
            
            await self._setup_api_interception()
            print("✅ [DEBUG-032] Step 7: API interception setup completed")
            
            self.logger.info("✅ [DEBUG-033] FikFap API Scraper initialized successfully")
            print("🎉 [DEBUG-034] FikFapAPIScraper.start() COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-002] FikFapAPIScraper.start() FAILED: {e}")
            self.logger.error(f"Failed to initialize FikFap API scraper: {e}")
            raise ScrapingError(f"Scraper initialization failed: {e}")
    
    async def scrape_and_extract_pipeline_style(self) -> Dict[str, Any]:
        """Complete scraping and extraction in pipeline style with FIXED PAGINATION."""
        print("🚀 [DEBUG-035] Starting scrape_and_extract_pipeline_style()")
        try:
            self.logger.info("🚀 [DEBUG-036] Starting FikFap API scraping and extraction (Pipeline Style)")
            
            # STEP 1: Scrape raw posts from API with FIXED PAGINATION
            print("🔧 [DEBUG-037] STEP 1: About to call _scrape_complete_workflow_fixed()")
            self.logger.info("📡 [DEBUG-038] Step 1: Scraping posts from FikFap API...")
            raw_posts = await self._scrape_complete_workflow_fixed()
            print(f"✅ [DEBUG-039] STEP 1 RESULT: _scrape_complete_workflow_fixed() returned {len(raw_posts) if raw_posts else 0} posts")
            
            if not raw_posts:
                print("❌ [DEBUG-040] STEP 1 FAILED: No raw posts returned")
                return {"error": "No posts scraped from API", "extracted_posts": []}
            
            self.logger.info(f"✅ [DEBUG-041] Scraped {len(raw_posts)} raw posts from API")
            
            # STEP 2: Extract posts using pipeline-style processing
            print("🔧 [DEBUG-042] STEP 2: About to call _extract_posts_pipeline_style()")
            self.logger.info("🔥 [DEBUG-043] Step 2: Processing posts (Pipeline-Style Extraction)...")
            extracted_posts = await self._extract_posts_pipeline_style(raw_posts)
            print(f"✅ [DEBUG-044] STEP 2 RESULT: _extract_posts_pipeline_style() returned {len(extracted_posts) if extracted_posts else 0} posts")
            
            self.logger.info(f"✅ [DEBUG-045] Extracted {len(extracted_posts)} posts using pipeline processing")
            
            # STEP 3: Save to JSON file
            print("🔧 [DEBUG-046] STEP 3: About to call _save_pipeline_format()")
            self.logger.info("💾 [DEBUG-047] Step 3: Saving extracted posts to integrated_extracted_posts.json...")
            filename = await self._save_pipeline_format(extracted_posts)
            print(f"✅ [DEBUG-048] STEP 3 RESULT: _save_pipeline_format() returned filename: {filename}")
            
            # STEP 4: Prepare results
            print("🔧 [DEBUG-049] STEP 4: Preparing final results")
            results = {
                "success": True,
                "extracted_posts": extracted_posts,
                "posts_count": len(extracted_posts),
                "filename": filename,
                "timestamp": datetime.now().isoformat(),
                "source": "api_scraper_pipeline"
            }
            print(f"✅ [DEBUG-050] STEP 4 RESULT: Final results prepared with {len(extracted_posts)} posts")
            
            self.logger.info(f"🎉 [DEBUG-051] Pipeline-style extraction completed: {len(extracted_posts)} posts saved to {filename}")
            print("🎉 [DEBUG-052] scrape_and_extract_pipeline_style() COMPLETED SUCCESSFULLY")
            return results
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-003] scrape_and_extract_pipeline_style() FAILED: {e}")
            self.logger.error(f"Pipeline-style extraction failed: {e}")
            return {"error": str(e), "extracted_posts": []}
    
    async def _scrape_complete_workflow_fixed(self) -> List[Dict[str, Any]]:
        """FIXED: Scrape the complete 5+9 posts workflow with new pagination method."""
        print("🚀 [DEBUG-053] Starting _scrape_complete_workflow_fixed()")
        try:
            self.logger.info("🚀 [DEBUG-054] Starting complete FikFap API scraping (FIXED)...")
            
            # STEP A: Get initial posts (this works)
            print("🔧 [DEBUG-055] STEP A: About to call _scrape_initial_batch()")
            initial_posts = await self._scrape_initial_batch()
            print(f"✅ [DEBUG-056] STEP A RESULT: _scrape_initial_batch() returned {len(initial_posts) if initial_posts else 0} posts")
            
            if not initial_posts:
                print("❌ [DEBUG-057] STEP A FAILED: No initial posts")
                raise ScrapingError("Failed to get initial batch of posts")
            
            self.logger.info(f"✅ [DEBUG-058] Retrieved {len(initial_posts)} initial posts")
            
            # STEP B: Extract pagination ID (this works)
            print("🔧 [DEBUG-059] STEP B: About to call _extract_pagination_id()")
            pagination_id = self._extract_pagination_id(initial_posts)
            print(f"✅ [DEBUG-060] STEP B RESULT: _extract_pagination_id() returned {pagination_id}")
            
            if not pagination_id:
                print("⚠️ [DEBUG-061] STEP B WARNING: No pagination ID, returning only initial batch")
                self.logger.warning("No pagination ID found, returning only initial batch")
                # Save initial posts to file
                await self._save_all_raw_posts(initial_posts)
                return initial_posts
            
            # STEP C: FIXED - Direct API call instead of scrolling
            print("🔧 [DEBUG-062] STEP C: About to call _scrape_next_batch_fixed()")
            next_posts = await self._scrape_next_batch_fixed(pagination_id)
            print(f"✅ [DEBUG-063] STEP C RESULT: _scrape_next_batch_fixed() returned {len(next_posts) if next_posts else 0} posts")
            
            if not next_posts:
                print("⚠️ [DEBUG-064] STEP C WARNING: No next posts, returning only initial batch")
                self.logger.warning("Failed to get next batch, returning only initial batch")
                await self._save_all_raw_posts(initial_posts)
                return initial_posts
            
            self.logger.info(f"✅ [DEBUG-065] Retrieved {len(next_posts)} additional posts")
            
            # STEP D: Combine all posts
            print("🔧 [DEBUG-066] STEP D: Combining all posts")
            all_posts = initial_posts + next_posts
            print(f"✅ [DEBUG-067] STEP D RESULT: Combined {len(initial_posts)} + {len(next_posts)} = {len(all_posts)} total posts")
            
            self.logger.info(f"✅ [DEBUG-068] Combined total posts: {len(all_posts)}")
            print("🎉 [DEBUG-069] _scrape_complete_workflow_fixed() COMPLETED SUCCESSFULLY")
            
            # Save all posts to file
            await self._save_all_raw_posts(all_posts)
            
            return all_posts
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-004] _scrape_complete_workflow_fixed() FAILED: {e}")
            self.logger.error(f"Complete workflow failed: {e}")
            raise ScrapingError(f"FikFap scraping workflow failed: {e}")
    
    def _get_auth_headers(self) -> dict:
        """Extract required auth headers from intercepted requests."""
        # Find the last intercepted request to the API
        for req in reversed(self.all_requests):
            if "api.fikfap.com" in req["url"]:
                headers = req["headers"]
                # Only include if present
                auth_headers = {}
                for key in ["authorization-anonymous", "isloggedin", "ispwa"]:
                    if key in headers:
                        auth_headers[key] = headers[key]
                return auth_headers
        return {}

    async def _scrape_next_batch_fixed(self, after_id: int) -> List[Dict[str, Any]]:
        """FIXED: Direct API call method instead of scrolling-triggered pagination."""
        print(f"🚀 [DEBUG-FIXED-001] Starting _scrape_next_batch_fixed() with after_id: {after_id}")
        try:
            self.logger.info(f"🔧 [FIXED] Using direct API call for pagination after ID: {after_id}")

            print("🔧 [DEBUG-FIXED-002] Making direct pagination API call via browser context.request")

            pagination_url = f"https://api.fikfap.com/posts?amount=9&afterId={after_id}&sort=random"
            print(f"🔧 [DEBUG-FIXED-003] Pagination URL: {pagination_url}")

            # Get required auth headers from intercepted requests
            auth_headers = self._get_auth_headers()
            print(f"🔧 [DEBUG-FIXED-004] Using auth headers: {auth_headers}")

            # Compose all headers
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Referer': 'https://fikfap.com/',
                'Origin': 'https://fikfap.com',
                'User-Agent': await self.page.evaluate("navigator.userAgent"),
                **auth_headers
            }

            api_response = await self.context.request.get(
                pagination_url,
                headers=headers,
                timeout=30000
            )

            print(f"✅ [DEBUG-FIXED-005] Browser API call completed: {api_response.ok}")

            if not api_response.ok:
                print(f"❌ [DEBUG-FIXED-006] API call failed with status {api_response.status}")
                raise ScrapingError(f"Direct API call failed with status {api_response.status}")

            try:
                posts_data = await api_response.json()
            except Exception as e:
                print(f"❌ [DEBUG-FIXED-007] Failed to parse JSON: {e}")
                raise ScrapingError("Failed to parse JSON from pagination response")

            # Extract posts from response
            if isinstance(posts_data, list):
                print(f"✅ [DEBUG-FIXED-008] Response is array with {len(posts_data)} items")
            elif isinstance(posts_data, dict) and 'data' in posts_data:
                posts_data = posts_data['data']
                print(f"✅ [DEBUG-FIXED-009] Response has data field with {len(posts_data)} items")
            elif isinstance(posts_data, dict) and 'posts' in posts_data:
                posts_data = posts_data['posts']
                print(f"✅ [DEBUG-FIXED-010] Response has posts field with {len(posts_data)} items")
            else:
                posts_data = posts_data if isinstance(posts_data, list) else []
                print(f"✅ [DEBUG-FIXED-011] Using response as-is: {len(posts_data)} items")

            if not posts_data:
                print("❌ [DEBUG-FIXED-012] No posts data extracted from response")
                raise ScrapingError("No posts found in pagination response")

            print(f"🎉 [DEBUG-FIXED-013] Successfully got {len(posts_data)} posts via direct API call")
            self.logger.info(f"✅ [FIXED] Successfully retrieved {len(posts_data)} posts via direct API call")

            return posts_data

        except Exception as e:
            print(f"❌ [DEBUG-FIXED-ERROR] _scrape_next_batch_fixed() failed: {e}")
            self.logger.error(f"Fixed pagination method failed: {e}")
            raise ScrapingError(f"Fixed pagination failed: {e}")
    
    # Keep all the existing methods for initial batch, extraction, etc.
    async def _scrape_initial_batch(self) -> List[Dict[str, Any]]:
        """Scrape the initial batch of 5 posts - UNCHANGED (this works)."""
        print("🚀 [DEBUG-070] Starting _scrape_initial_batch()")
        try:
            self.logger.info("🚀 [DEBUG-071] Scraping initial batch (5 posts)")
            
            self.intercepted_responses.clear()
            self.all_requests.clear()
            self.all_responses.clear()
            print("✅ [DEBUG-073] Previous responses cleared")
            
            print("🔧 [DEBUG-074] Navigating to FikFap.com...")
            self.logger.info("🌐 [DEBUG-075] Navigating to FikFap.com...")
            await self.page.goto(
                self.site_url, 
                wait_until="networkidle",
                timeout=60000
            )
            print("✅ [DEBUG-076] Navigation completed")
            
            await asyncio.sleep(3)
            print("✅ [DEBUG-078] Wait completed")
            
            title = await self.page.title()
            print(f"✅ [DEBUG-080] Page title: {title}")
            self.logger.info(f"📄 [DEBUG-081] Page loaded: {title}")
            
            print("🔧 [DEBUG-082] Waiting for initial API call interception")
            self.logger.info("⏳ [DEBUG-083] Waiting for initial API call to be intercepted...")
            initial_response = await self._wait_for_api_response("initial_batch", timeout=30)
            print(f"✅ [DEBUG-084] API wait result: {initial_response is not None}")
            
            if not initial_response:
                print("⚠️ [DEBUG-085] No initial API call intercepted")
                # Try refreshing
                await self.page.reload(wait_until="networkidle", timeout=30000)
                await asyncio.sleep(5)
                initial_response = await self._wait_for_api_response("initial_batch", timeout=20)
                print(f"✅ [DEBUG-097] Retry result: {initial_response is not None}")
            
            if not initial_response:
                print("💀 [DEBUG-101] FINAL FAILURE - No API call intercepted")
                raise ScrapingError("Failed to intercept initial API call after all attempts")
            
            posts_data = initial_response.get("data", [])
            print(f"✅ [DEBUG-105] Extracted {len(posts_data)} posts from response")
            self.logger.info(f"✅ [DEBUG-106] Successfully intercepted initial batch: {len(posts_data)} posts")
            
            print("🎉 [DEBUG-107] _scrape_initial_batch() COMPLETED SUCCESSFULLY")
            return posts_data
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-005] _scrape_initial_batch() FAILED: {e}")
            self.logger.error(f"Failed to scrape initial batch: {e}")
            raise ScrapingError(f"Initial batch scraping failed: {e}")
    
    # Keep all other existing methods unchanged
    async def _extract_posts_pipeline_style(self, raw_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract posts using pipeline-style processing - UNCHANGED."""
        print("🚀 [DEBUG-108] Starting _extract_posts_pipeline_style()")
        try:
            print(f"🔧 [DEBUG-109] Input: {len(raw_posts)} raw posts to process")
            self.logger.info(f"🔥 [DEBUG-110] Processing {len(raw_posts)} posts using pipeline-style extraction...")
            
            extracted_posts = []
            
            for i, post_data in enumerate(raw_posts, 1):
                print(f"🔧 [DEBUG-111-{i}] Processing post {i}/{len(raw_posts)}")
                try:
                    post_id = post_data.get('postId', 'unknown')
                    print(f"🔧 [DEBUG-112-{i}] Post ID: {post_id}")
                    self.logger.debug(f"Processing post {i}/{len(raw_posts)}: {post_id}")
                    
                    extracted_post = await self._extract_single_post_pipeline_style(post_data)
                    print(f"✅ [DEBUG-114-{i}] Extraction result: {extracted_post is not None}")
                    
                    if extracted_post:
                        extracted_posts.append(extracted_post)
                        print(f"✅ [DEBUG-115-{i}] Post {i} extracted successfully")
                        self.logger.debug(f"✅ Post {i} extracted successfully")
                    else:
                        print(f"❌ [DEBUG-116-{i}] Post {i} extraction failed")
                        self.logger.warning(f"❌ [ERROR] Post {i} extraction failed")
                        
                except Exception as e:
                    print(f"❌ [DEBUG-ERROR-006-{i}] Error processing post {i}: {e}")
                    self.logger.error(f"Error processing post {i}: {e}")
                    continue
            
            print(f"✅ [DEBUG-117] Final result: {len(extracted_posts)}/{len(raw_posts)} posts extracted")
            self.logger.info(f"🔥 [DEBUG-118] Pipeline extraction completed: {len(extracted_posts)}/{len(raw_posts)} posts extracted")
            
            print("🎉 [DEBUG-119] _extract_posts_pipeline_style() COMPLETED SUCCESSFULLY")
            return extracted_posts
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-007] _extract_posts_pipeline_style() FAILED: {e}")
            self.logger.error(f"Pipeline-style extraction failed: {e}")
            raise ExtractionError(f"Pipeline extraction failed: {e}")
    
    async def _extract_single_post_pipeline_style(self, post_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract a single post - UNCHANGED."""
        print("🚀 [DEBUG-120] Starting _extract_single_post_pipeline_style()")
        try:
            post_id = post_data.get("postId")
            print(f"✅ [DEBUG-122] Post ID: {post_id}")
            
            if not post_id:
                print("❌ [DEBUG-123] No postId found in post data")
                return None
            
            print("🔧 [DEBUG-124] Creating extracted post structure")
            extracted_post = {
                "postId": post_id,
                "postUrl": f"https://fikfap.com/post/{post_id}",
                "author": {
                    "username": post_data.get("author", {}).get("username", "unknown"),
                    "displayName": post_data.get("author", {}).get("displayName", "unknown"),
                    "avatar": post_data.get("author", {}).get("avatar", None)
                },
                "title": post_data.get("title", ""),
                "description": post_data.get("description", ""),
                "tags": post_data.get("tags", []),
                "score": post_data.get("score", 0),
                "views": post_data.get("viewCount", 0),
                "likes": post_data.get("likeCount", 0),
                "comments": post_data.get("commentCount", 0),
                "thumbnail": post_data.get("thumbnail", {}).get("url") if post_data.get("thumbnail") else None,
                "duration": post_data.get("duration", 0),
                "quality": "unknown",
                "videoUrls": self._extract_video_urls_direct(post_data),
                "createdAt": post_data.get("createdAt", ""),
                "extractedAt": datetime.now().isoformat(),
                "source": "api_scraper_direct"
            }
            print("✅ [DEBUG-125] Extracted post structure created successfully")
            
            self.logger.debug(f"✅ [DEBUG-126] Post {post_id} extracted successfully (direct method)")
            print("🎉 [DEBUG-127] _extract_single_post_pipeline_style() COMPLETED SUCCESSFULLY")
            return extracted_post
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-008] _extract_single_post_pipeline_style() FAILED: {e}")
            return None
    
    def _extract_video_urls_direct(self, post_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract video URLs directly from post_data - UNCHANGED."""
        print("🚀 [DEBUG-128] Starting _extract_video_urls_direct()")
        try:
            video_urls = {}
            video_data = post_data.get("video", {})
            print(f"✅ [DEBUG-130] Video data found: {video_data is not None}")
            
            if "playlist" in video_data:
                video_urls["m3u8"] = video_data["playlist"]
                print("✅ [DEBUG-132] Found playlist URL")
            elif "playlistUrl" in video_data:
                video_urls["m3u8"] = video_data["playlistUrl"]
                print("✅ [DEBUG-133] Found playlistUrl")
            elif "url" in video_data:
                video_urls["m3u8"] = video_data["url"]
                print("✅ [DEBUG-134] Found url")
            
            if "qualities" in video_data:
                qualities = video_data["qualities"]
                print(f"✅ [DEBUG-137] Found {len(qualities)} quality options")
                for quality in qualities:
                    if "height" in quality and "url" in quality:
                        quality_key = f"{quality['height']}p"
                        video_urls[quality_key] = quality["url"]
                        print(f"✅ [DEBUG-138] Added quality URL: {quality_key}")
            
            print(f"✅ [DEBUG-142] Total video URLs extracted: {len(video_urls)}")
            print("🎉 [DEBUG-143] _extract_video_urls_direct() COMPLETED SUCCESSFULLY")
            return video_urls
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-009] _extract_video_urls_direct() FAILED: {e}")
            return {}
    
    async def _save_pipeline_format(self, extracted_posts: List[Dict[str, Any]]) -> str:
        """Save extracted posts to JSON file - UNCHANGED."""
        print("🚀 [DEBUG-144] Starting _save_pipeline_format()")
        try:
            print(f"🔧 [DEBUG-145] Input: {len(extracted_posts)} extracted posts to save")
            
            pipeline_data = {
                'pipeline_info': {
                    'timestamp': datetime.now().isoformat(),
                    'total_posts': len(extracted_posts),
                    'scrape_method': 'api_interception_fixed',
                    'source': 'fikfap_api_scraper'
                },
                'posts': extracted_posts
            }
            print("✅ [DEBUG-147] Pipeline data structure created")
            
            filename = "integrated_extracted_posts.json"
            print(f"🔧 [DEBUG-148] Saving to file: {filename}")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(pipeline_data, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"✅ [DEBUG-149] File saved successfully")
            self.logger.info(f"💾 [DEBUG-150] Saved {len(extracted_posts)} posts to {filename} (Pipeline Format)")
            
            print("🎉 [DEBUG-151] _save_pipeline_format() COMPLETED SUCCESSFULLY")
            return filename
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-010] _save_pipeline_format() FAILED: {e}")
            self.logger.error(f"Failed to save pipeline format: {e}")
            raise ScrapingError(f"Pipeline save failed: {e}")
    
    async def _save_all_raw_posts(self, posts: List[Dict[str, Any]]) -> None:
        """Save all raw posts (initial + paginated) to a file."""
        try:
            filename = "all_raw_posts.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(posts, f, indent=2, ensure_ascii=False, default=str)
            print(f"✅ [DEBUG-SAVE-RAW] Saved {len(posts)} raw posts to {filename}")
            self.logger.info(f"Saved {len(posts)} raw posts to {filename}")
        except Exception as e:
            print(f"❌ [DEBUG-SAVE-RAW-ERROR] Failed to save raw posts: {e}")
            self.logger.error(f"Failed to save raw posts: {e}")
    
    # Keep all the helper methods unchanged
    async def _handle_request(self, route, request):
        """Handle and log all requests for debugging."""
        try:
            url = request.url
            method = request.method
            
            request_info = {
                "url": url,
                "method": method,
                "headers": dict(request.headers),
                "timestamp": datetime.now().isoformat()
            }
            self.all_requests.append(request_info)
            
            if any(pattern in url for pattern in ["api.fikfap.com", "view-api.fikfap.com", "/posts"]):
                print(f"🌐 [DEBUG-API-REQUEST] {method} {url}")
                self.logger.info(f"🌐 REQUEST: {method} {url}")
            
            await route.continue_()
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-REQUEST] Error handling request: {e}")
            await route.continue_()
    
    async def _setup_api_interception(self):
        """Setup request/response interception."""
        try:
            print("🔧 [DEBUG-152] Setting up API interception")
            
            async def handle_response(response: Response):
                try:
                    url = response.url
                    status = response.status
                    
                    response_info = {
                        "url": url,
                        "status": status,
                        "headers": dict(response.headers),
                        "timestamp": datetime.now().isoformat()
                    }
                    self.all_responses.append(response_info)
                    
                    if self._is_target_api_endpoint(url):
                        print(f"🎯 [DEBUG-API-RESPONSE] TARGET INTERCEPTED: {status} {url}")
                        self.logger.info(f"🎯 [TARGET] RESPONSE INTERCEPTED: {status} {url}")
                        
                        try:
                            response_data = await response.json()
                            endpoint_key = self._get_endpoint_key(url)
                            
                            self.intercepted_responses[endpoint_key] = {
                                "url": url,
                                "status": status,
                                "data": response_data,
                                "headers": dict(response.headers),
                                "timestamp": time.time()
                            }
                            
                            print(f"✅ [DEBUG-API-STORED] {endpoint_key}: {len(response_data)} items")
                            self.logger.info(f"✅ [OK] API DATA STORED: {endpoint_key} ({len(response_data)} items, status: {status})")
                            
                        except Exception as e:
                            print(f"❌ [DEBUG-ERROR-API-PROCESS] Failed to process response: {e}")
                    
                    elif "fikfap" in url.lower():
                        print(f"🔍 [DEBUG-OTHER-FIKFAP] {status} {url}")
                    
                except Exception as e:
                    print(f"❌ [DEBUG-ERROR-RESPONSE] Error in response handler: {e}")
            
            self.page.on("response", handle_response)
            print("✅ [DEBUG-153] API interception setup completed")
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-011] Failed to setup API interception: {e}")
            raise ScrapingError(f"API interception setup failed: {e}")
    
    def _is_target_api_endpoint(self, url: str) -> bool:
        """Check if URL is a target FikFap API endpoint."""
        target_patterns = [
            "api.fikfap.com/cached-high-quality/posts",
            "/cached-high-quality/posts?amount=5",
            "cached-high-quality/posts"
        ]
        
        for pattern in target_patterns:
            if pattern in url:
                print(f"🎯 [DEBUG-URL-MATCH] Pattern '{pattern}' matched in {url}")
                return True
        
        return False
    
    def _get_endpoint_key(self, url: str) -> str:
        """Generate a key for the intercepted endpoint."""
        if "cached-high-quality/posts" in url and "amount=5" in url:
            return "initial_batch"
        elif "amount=9" in url:
            return "pagination_batch"
        else:
            return "initial_batch"  # Default to initial batch
    
    async def _wait_for_api_response(self, endpoint_key: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """Wait for API response."""
        print(f"🔧 [DEBUG-154] Waiting for API response: {endpoint_key} (timeout: {timeout}s)")
        try:
            start_time = time.time()
            
            while (time.time() - start_time) < timeout:
                if endpoint_key in self.intercepted_responses:
                    response = self.intercepted_responses[endpoint_key]
                    print(f"✅ [DEBUG-155] API response received for {endpoint_key}")
                    return response
                
                await asyncio.sleep(0.5)
            
            print(f"⏰ [DEBUG-156] Timeout waiting for {endpoint_key}")
            return None
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-012] Error waiting for API response: {e}")
            return None
    
    def _extract_pagination_id(self, posts_data: List[Dict[str, Any]]) -> Optional[int]:
        """Extract pagination ID."""
        print("🔧 [DEBUG-158] Extracting pagination ID")
        try:
            if not posts_data:
                print("⚠️ [DEBUG-159] No posts data provided")
                return None
                
            last_post = posts_data[-1]
            pagination_id = last_post.get("postId")
            
            if pagination_id:
                print(f"✅ [DEBUG-160] Extracted pagination ID: {pagination_id}")
                self.pagination_state["last_post_id"] = pagination_id
                
            return pagination_id
            
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-014] Failed to extract pagination ID: {e}")
            return None
    
    async def close(self):
        """Clean up browser resources."""
        print("🔧 [DEBUG-168] Starting cleanup")
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
            
            await self.close_session()
            print("✅ [DEBUG-169] Cleanup completed successfully")
                
        except Exception as e:
            print(f"❌ [DEBUG-ERROR-017] Error during cleanup: {e}")
    
    def get_pagination_state(self) -> Dict[str, Any]:
        """Get current pagination state."""
        return {
            **self.pagination_state,
            "intercepted_responses_count": len(self.intercepted_responses),
            "current_posts_count": len(self.current_posts),
            "total_requests_captured": len(self.all_requests),
            "total_responses_captured": len(self.all_responses)
        }