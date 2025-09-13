import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
from typing import Dict, List, Any, Optional
from pathlib import Path

class FikFapDataExtractor:
    """
    Extracts post data from FikFap API responses and formats them into structured JSON
    """

    def __init__(self, api_data: dict = None):
        self.raw_api_data = api_data
        self.extracted_posts = []

    def load_api_data_from_dict(self, api_data: dict):
        """Load API data from dictionary (for integration)"""
        self.raw_api_data = api_data
        print(f"✅ Loaded API data from scraper")
        print(f"📊 Found {len(self.raw_api_data.get('api_responses', []))} API responses")

    def identify_post_endpoints(self) -> List[Dict]:
        """Identify API endpoints that contain post data"""
        if not self.raw_api_data:
            return []

        post_endpoints = []

        for response in self.raw_api_data.get('api_responses', []):
            url = response.get('url', '')

            # Look for endpoints that likely contain post data
            if any(pattern in url for pattern in [
                '/posts', '/cached-high-quality', '/hashtags/', 
                'godpussy', 'trending', 'for-you'
            ]):
                if response.get('json') and response.get('status') == 200:
                    post_endpoints.append(response)

        print(f"🎯 Found {len(post_endpoints)} endpoints with post data")
        return post_endpoints

    def extract_posts_from_response(self, response_data: Dict) -> List[Dict]:
        """Extract individual posts from a single API response"""
        posts = []
        json_data = response_data.get('json')

        if not json_data:
            return posts

        # Handle different response structures
        if isinstance(json_data, list):
            # Direct array of posts
            posts.extend(self._process_post_array(json_data))
        elif isinstance(json_data, dict):
            # Look for posts in various nested structures
            for key, value in json_data.items():
                if key in ['posts', 'data', 'results', 'items'] and isinstance(value, list):
                    posts.extend(self._process_post_array(value))
                elif isinstance(value, list) and len(value) > 0:
                    # Check if this array contains post-like objects
                    if self._looks_like_post_array(value):
                        posts.extend(self._process_post_array(value))

        return posts

    def _looks_like_post_array(self, arr: List) -> bool:
        """Check if an array contains post-like objects"""
        if not arr or not isinstance(arr[0], dict):
            return False

        first_item = arr[0]
        post_indicators = ['postId', 'label', 'author', 'userId', 'mediaId', 'videoStreamUrl']

        return any(key in first_item for key in post_indicators)

    def _process_post_array(self, posts_array: List[Dict]) -> List[Dict]:
        """Process an array of raw post objects"""
        processed_posts = []

        for raw_post in posts_array:
            if isinstance(raw_post, dict):
                processed_post = self._extract_post_data(raw_post)
                if processed_post:
                    processed_posts.append(processed_post)

        return processed_posts

    def _extract_post_data(self, raw_post: Dict) -> Optional[Dict]:
        """Extract and structure data from a single raw post object"""
        try:
            # Basic post info
            post_data = {
                'postId': self._safe_get(raw_post, 'postId'),
                'label': self._safe_get(raw_post, 'label', ''),
                'score': self._safe_get(raw_post, 'score', 0),
                'likesCount': self._safe_get(raw_post, 'likesCount', 0),
                'userId': self._safe_get(raw_post, 'userId'),
                'mediaId': self._safe_get(raw_post, 'mediaId'),
                'duration': self._safe_get(raw_post, 'duration'),
                'viewsCount': self._safe_get(raw_post, 'viewsCount', 0),
                'bunnyVideoId': self._safe_get(raw_post, 'bunnyVideoId'),
                'isBunnyVideoReady': self._safe_get(raw_post, 'isBunnyVideoReady', False),
                'videoStreamUrl': self._safe_get(raw_post, 'videoStreamUrl'),
                'thumbnailStreamUrl': self._safe_get(raw_post, 'thumbnailStreamUrl'),
                'publishedAt': self._safe_get(raw_post, 'publishedAt'),
                'explicitnessRating': self._safe_get(raw_post, 'explicitnessRating'),
            }

            # Author information
            author_data = raw_post.get('author', {})
            if author_data:
                post_data['author'] = self._extract_author_data(author_data)

            # Hashtags
            hashtags_data = raw_post.get('hashtags', [])
            if hashtags_data:
                post_data['hashtags'] = self._extract_hashtags_data(hashtags_data)

            # Additional fields that might be useful
            additional_fields = [
                'createdAt', 'updatedAt', 'inCollectionsCount', 
                'commentsCount', 'sexualOrientation', 'uploadMethod'
            ]

            for field in additional_fields:
                if field in raw_post:
                    post_data[field] = raw_post[field]

            return post_data

        except Exception as e:
            print(f"⚠️ Error extracting post data: {e}")
            return None

    def _extract_author_data(self, author: Dict) -> Dict:
        """Extract author information"""
        return {
            'userId': self._safe_get(author, 'userId'),
            'username': self._safe_get(author, 'username', ''),
            'isVerified': self._safe_get(author, 'isVerified', False),
            'isPartner': self._safe_get(author, 'isPartner', False),
            'description': self._safe_get(author, 'description', ''),
            'thumbnailUrl': self._safe_get(author, 'thumbnailUrl'),
            'countPosts': self._safe_get(author, 'countPosts', 0),
            'countIncomingLikes': self._safe_get(author, 'countIncomingLikes', 0),
            'countIncomingFollows': self._safe_get(author, 'countIncomingFollows', 0),
            'countTotalViews': self._safe_get(author, 'countTotalViews', 0),
            'profileLinks': self._safe_get(author, 'profileLinks', [])
        }

    def _extract_hashtags_data(self, hashtags: List[Dict]) -> List[Dict]:
        """Extract hashtags information"""
        extracted_hashtags = []

        for hashtag in hashtags:
            if isinstance(hashtag, dict):
                extracted_hashtags.append({
                    'hashtagId': self._safe_get(hashtag, 'hashtagId'),
                    'label': self._safe_get(hashtag, 'label', ''),
                    'description': self._safe_get(hashtag, 'description', ''),
                    'countPosts': self._safe_get(hashtag, 'countPosts', 0),
                    'countFollows': self._safe_get(hashtag, 'countFollows', 0)
                })

        return extracted_hashtags

    def _safe_get(self, data: Dict, key: str, default=None):
        """Safely get a value from dictionary"""
        return data.get(key, default)

    def extract_all_posts(self) -> List[Dict]:
        """Main method to extract all posts from loaded API data"""
        if not self.raw_api_data:
            print("❌ No API data loaded")
            return []

        print("\n🔍 Starting post extraction...")

        # Find all endpoints with post data
        post_endpoints = self.identify_post_endpoints()

        all_posts = []

        for i, endpoint in enumerate(post_endpoints, 1):
            url = endpoint.get('url', '')
            print(f"[{i}/{len(post_endpoints)}] Processing: {url}")

            posts = self.extract_posts_from_response(endpoint)
            all_posts.extend(posts)
            print(f"  ➤ Extracted {len(posts)} posts")

        # Remove duplicates based on postId
        unique_posts = self._remove_duplicates(all_posts)

        self.extracted_posts = unique_posts

        print(f"\n✅ Extraction completed!")
        print(f"📊 Total unique posts extracted: {len(unique_posts)}")

        return unique_posts

    def _remove_duplicates(self, posts: List[Dict]) -> List[Dict]:
        """Remove duplicate posts based on postId"""
        seen_ids = set()
        unique_posts = []

        for post in posts:
            post_id = post.get('postId')
            if post_id and post_id not in seen_ids:
                seen_ids.add(post_id)
                unique_posts.append(post)

        duplicates_removed = len(posts) - len(unique_posts)
        if duplicates_removed > 0:
            print(f"🔄 Removed {duplicates_removed} duplicate posts")

        return unique_posts

    def filter_posts(self, posts: List[Dict] = None, **filters) -> List[Dict]:
        """Filter posts based on criteria"""
        if posts is None:
            posts = self.extracted_posts

        filtered_posts = posts

        # Apply filters
        if 'min_score' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('score', 0) >= filters['min_score']]

        if 'min_likes' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('likesCount', 0) >= filters['min_likes']]

        if 'min_views' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('viewsCount', 0) >= filters['min_views']]

        if filters.get('verified_authors_only'):
            filtered_posts = [p for p in filtered_posts if p.get('author', {}).get('isVerified', False)]

        if filters.get('partner_authors_only'):
            filtered_posts = [p for p in filtered_posts if p.get('author', {}).get('isPartner', False)]

        if 'explicitness_rating' in filters:
            filtered_posts = [p for p in filtered_posts if p.get('explicitnessRating') == filters['explicitness_rating']]

        if 'hashtag_labels' in filters:
            hashtag_labels = filters['hashtag_labels']
            filtered_posts = [
                p for p in filtered_posts 
                if any(
                    h.get('label', '').lower() in [hl.lower() for hl in hashtag_labels]
                    for h in p.get('hashtags', [])
                )
            ]

        print(f"🔽 Filtered from {len(posts)} to {len(filtered_posts)} posts")
        return filtered_posts

    def sort_posts(self, posts: List[Dict] = None, by: str = 'score', reverse: bool = True) -> List[Dict]:
        """Sort posts by specified criteria"""
        if posts is None:
            posts = self.extracted_posts

        def get_sort_value(post):
            if by == 'publishedAt':
                # Handle datetime sorting
                date_str = post.get(by)
                if date_str:
                    try:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    except:
                        return datetime.min
                return datetime.min
            else:
                return post.get(by, 0)

        sorted_posts = sorted(posts, key=get_sort_value, reverse=reverse)
        print(f"📊 Sorted {len(sorted_posts)} posts by {by} ({'desc' if reverse else 'asc'})")

        return sorted_posts

    def save_extracted_data(self, posts: List[Dict] = None, filename: str = None) -> str:
        """Save extracted posts to JSON file"""
        if posts is None:
            posts = self.extracted_posts

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"extracted_posts_{timestamp}.json"

        # Prepare the output data
        output_data = {
            'extraction_info': {
                'timestamp': datetime.now().isoformat(),
                'total_posts': len(posts),
                'source': 'integrated_scraper'
            },
            'posts': posts
        }

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)

            print(f"💾 Saved {len(posts)} posts to: {filename}")
            return filename

        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return None

    def print_summary(self, posts: List[Dict] = None):
        """Print a formatted summary of extracted posts"""
        if posts is None:
            posts = self.extracted_posts

        if not posts:
            print("📊 No posts to summarize")
            return

        # Calculate statistics
        scores = [p.get('score', 0) for p in posts]
        likes = [p.get('likesCount', 0) for p in posts]
        views = [p.get('viewsCount', 0) for p in posts]

        # Count by explicitness rating
        explicitness_counts = {}
        for post in posts:
            rating = post.get('explicitnessRating', 'UNKNOWN')
            explicitness_counts[rating] = explicitness_counts.get(rating, 0) + 1

        # Count verified authors
        verified_authors = sum(1 for p in posts if p.get('author', {}).get('isVerified', False))
        partner_authors = sum(1 for p in posts if p.get('author', {}).get('isPartner', False))

        # Get unique hashtags
        all_hashtags = set()
        for post in posts:
            for hashtag in post.get('hashtags', []):
                all_hashtags.add(hashtag.get('label', ''))

        print("\n" + "="*80)
        print("📊 EXTRACTION SUMMARY")
        print("="*80)
        print(f"Total Posts: {len(posts)}")

        if len(posts) > 0:
            print(f"\n📈 SCORE STATS:")
            print(f"  Min: {min(scores) if scores else 0:,}")
            print(f"  Max: {max(scores) if scores else 0:,}")
            print(f"  Avg: {sum(scores) / len(scores) if scores else 0:,.1f}")

            print(f"\n❤️ LIKES STATS:")
            print(f"  Min: {min(likes) if likes else 0:,}")
            print(f"  Max: {max(likes) if likes else 0:,}")
            print(f"  Avg: {sum(likes) / len(likes) if likes else 0:,.1f}")

            print(f"\n👀 VIEWS STATS:")
            print(f"  Min: {min(views) if views else 0:,}")
            print(f"  Max: {max(views) if views else 0:,}")
            print(f"  Avg: {sum(views) / len(views) if views else 0:,.1f}")

            print(f"\n🔞 EXPLICITNESS BREAKDOWN:")
            for rating, count in explicitness_counts.items():
                print(f"  {rating}: {count}")

            print(f"\n👤 AUTHORS:")
            print(f"  Verified: {verified_authors}")
            print(f"  Partners: {partner_authors}")

            print(f"\n🏷️ HASHTAGS:")
            print(f"  Unique hashtags: {len(all_hashtags)}")
            if all_hashtags:
                print(f"  Top tags: {', '.join(list(all_hashtags)[:10])}")

        print("="*80)


class IntegratedFikFapScraper:
    """
    Integrated scraper that captures API calls and immediately extracts post data
    """

    def __init__(self, base_url):
        self.base_url = base_url
        self.api_requests = []
        self.api_responses = []
        self.all_requests = []
        self.extracted_posts = []
        self.extractor = None

    def is_api_request(self, url):
        """Determine if a request is an API call"""
        url_lower = url.lower()

        # Skip obvious non-API requests
        skip_patterns = [
            '.css', '.js', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg',
            '.woff', '.woff2', '.ttf', '.otf', '.eot',
            '.mp4', '.mp3', '.wav', '.avi', '.mov',
            'analytics.google.com',
            'fonts.gstatic.com',
            'cdn-cgi/challenge',
            'googletagmanager.com',
            'facebook.com/tr',
            'doubleclick.net',
            'googlesyndication.com',
            'chaturbate.com/cdn-cgi',
            'web.static.mmcdn.com',
            'jpeg.live.mmcdn.com/stream'  # Streaming images
        ]

        if any(pattern in url_lower for pattern in skip_patterns):
            return False

        # Look for API patterns
        api_patterns = [
            '/api/', '/v1/', '/v2/', '/v3/', '/v4/',
            'graphql', 'api.',
            '/endpoint/', '/data/', '/service/',
            'fikfap.com',  # Domain-specific API calls
            '.json',
            'ajax', 'xhr'
        ]

        return any(pattern in url_lower for pattern in api_patterns)

    async def intercept_request(self, request):
        """Handle intercepted requests"""
        try:
            # Log all requests for debugging
            self.all_requests.append({
                'url': request.url,
                'method': request.method,
                'resource_type': request.resource_type,
                'timestamp': datetime.now().isoformat()
            })

            # Only process API requests
            if self.is_api_request(request.url):
                headers = await request.all_headers()

                request_data = {
                    'url': request.url,
                    'method': request.method,
                    'headers': headers,
                    'post_data': request.post_data,
                    'resource_type': request.resource_type,
                    'timestamp': datetime.now().isoformat()
                }

                self.api_requests.append(request_data)
                print(f"🎯 API Request: {request.method} {request.url}")
        except Exception as e:
            print(f"Error intercepting request: {e}")

    async def intercept_response(self, response):
        """Handle intercepted responses"""
        try:
            if self.is_api_request(response.url):
                print(f"📡 API Response: {response.status} {response.url}")
                headers = await response.all_headers()

                response_data = {
                    'url': response.url,
                    'status': response.status,
                    'status_text': response.status_text,
                    'headers': headers,
                    'timestamp': datetime.now().isoformat()
                }

                # Try to get response body
                try:
                    body = await response.body()
                    body_text = body.decode('utf-8', errors='ignore')
                    response_data['body'] = body_text

                    # Try to parse as JSON
                    if body_text and self.is_json_content(body_text, headers):
                        try:
                            response_data['json'] = json.loads(body_text)
                            print(f"💎 Got JSON response from: {response.url}")
                        except json.JSONDecodeError:
                            print(f"⚠️ Failed to parse JSON from: {response.url}")
                except Exception as e:
                    print(f"⚠️ Could not get body for {response.url}: {e}")
                    response_data['body_error'] = str(e)

                self.api_responses.append(response_data)
        except Exception as e:
            print(f"Error intercepting response: {e}")

    def is_json_content(self, body, headers):
        """Check if content is JSON"""
        content_type = headers.get('content-type', '').lower()
        return ('application/json' in content_type or
                'text/json' in content_type or
                (body.strip().startswith(('{', '[')) and
                 body.strip().endswith(('}', ']'))))

    async def scrape_and_extract(self, wait_time=15, extract_data=True):
        """Main method: scrape API calls and extract data"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)  # Show browser
            page = await browser.new_page()

            # Set up request/response interception
            page.on("request", self.intercept_request)
            page.on("response", self.intercept_response)

            try:
                print(f"🚀 Navigating to: {self.base_url}")

                # Navigate to the page
                await page.goto(self.base_url, wait_until='domcontentloaded')

                print(f"⏳ Waiting {wait_time} seconds for API calls...")
                await page.wait_for_timeout(wait_time * 1000)

                # Try scrolling to trigger more API calls
                print("📜 Scrolling to load more content...")
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(5000)

                # Try clicking on elements that might trigger API calls
                try:
                    # Look for buttons or clickable elements
                    buttons = await page.query_selector_all('button, .btn, [onclick]')
                    if buttons:
                        print(f"🖱️ Found {len(buttons)} clickable elements, trying first few...")
                        for i, button in enumerate(buttons[:3]):
                            try:
                                await button.click()
                                await page.wait_for_timeout(2000)
                                print(f"  Clicked element {i+1}")
                            except:
                                pass
                except Exception as e:
                    print(f"⚠️ Could not interact with page elements: {e}")

                print("✅ Scraping completed!")

            except Exception as e:
                print(f"Error during scraping: {e}")
            finally:
                await browser.close()

        # Print scraping results
        self.print_scraping_results()

        # Save API capture results
        api_filename = self.save_api_results()

        # Extract data if requested
        if extract_data and self.api_responses:
            print("\n" + "🔥"*80)
            print("🔥 STARTING DATA EXTRACTION")
            print("🔥"*80)

            # Create API data structure for extractor
            api_data = {
                'url': self.base_url,
                'timestamp': datetime.now().strftime("%Y%m%d_%H%M%S"),
                'api_requests': self.api_requests,
                'api_responses': self.api_responses,
                'summary': {
                    'total_requests': len(self.all_requests),
                    'api_requests': len(self.api_requests),
                    'api_responses': len(self.api_responses)
                }
            }

            # Initialize extractor with scraped data
            self.extractor = FikFapDataExtractor()
            self.extractor.load_api_data_from_dict(api_data)

            # Extract posts
            self.extracted_posts = self.extractor.extract_all_posts()

            # Save extracted posts
            if self.extracted_posts:
                extracted_filename = self.extractor.save_extracted_data(
                    self.extracted_posts, 
                    "integrated_extracted_posts.json"
                )

                # Print extraction summary
                self.extractor.print_summary(self.extracted_posts)

                # Show some example filtering
                self.demonstrate_filtering()
            else:
                print("❌ No posts were extracted")

        return {
            'api_filename': api_filename if 'api_filename' in locals() else None,
            'extracted_posts': self.extracted_posts,
            'total_posts': len(self.extracted_posts)
        }

    def demonstrate_filtering(self):
        """Demonstrate some filtering capabilities"""
        if not self.extracted_posts:
            return

        print("\n🎯 FILTERING DEMONSTRATIONS:")
        print("-"*60)

        # High quality posts
        high_quality = self.extractor.filter_posts(
            self.extracted_posts,
            min_score=5000,
            verified_authors_only=True
        )
        if high_quality:
            print(f"✨ High quality posts (score>5000, verified): {len(high_quality)}")
            self.extractor.save_extracted_data(high_quality, "high_quality_posts.json")

        # Popular posts by views
        popular = self.extractor.filter_posts(
            self.extracted_posts,
            min_views=1000000
        )
        if popular:
            print(f"🔥 Popular posts (views>1M): {len(popular)}")
            top_popular = self.extractor.sort_posts(popular, by='viewsCount', reverse=True)
            self.extractor.save_extracted_data(top_popular[:10], "top_popular_posts.json")

        # Posts by explicitness
        explicit = self.extractor.filter_posts(
            self.extracted_posts,
            explicitness_rating='FULLY_EXPLICIT'
        )
        if explicit:
            print(f"🔞 Fully explicit posts: {len(explicit)}")

        print("💾 Additional filtered files saved!")

    def print_scraping_results(self):
        """Print captured API calls and responses"""
        print("\n" + "="*80)
        print("🎯 API SCRAPING RESULTS")
        print("="*80)
        print(f"📊 SUMMARY:")
        print(f"  Total requests captured: {len(self.all_requests)}")
        print(f"  API requests found: {len(self.api_requests)}")
        print(f"  API responses captured: {len(self.api_responses)}")

        # Show API requests
        if self.api_requests:
            print(f"\n🔥 API REQUESTS ({len(self.api_requests)} found):")
            print("-" * 60)
            for i, req in enumerate(self.api_requests, 1):
                print(f"[{i:02d}] {req['method']} {req['url']}")
                if req.get('post_data'):
                    print(f"     POST: {req['post_data'][:100]}...")

        # Show API responses with JSON data
        json_responses = [r for r in self.api_responses if r.get('json')]
        if json_responses:
            print(f"\n💎 JSON RESPONSES ({len(json_responses)} found):")
            print("-" * 60)
            for i, resp in enumerate(json_responses, 1):
                print(f"[{i:02d}] {resp['status']} {resp['url']}")

        print("="*80 + "\n")

    def save_api_results(self):
        """Save API scraping results to JSON files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save API calls and responses
        api_data = {
            'url': self.base_url,
            'timestamp': timestamp,
            'api_requests': self.api_requests,
            'api_responses': self.api_responses,
            'summary': {
                'total_requests': len(self.all_requests),
                'api_requests': len(self.api_requests),
                'api_responses': len(self.api_responses)
            }
        }

        api_filename = f"integrated_api_capture_{timestamp}.json"
        with open(api_filename, "w") as f:
            json.dump(api_data, f, indent=2, default=str)

        print(f"💾 API results saved to: {api_filename}")
        return api_filename


# Usage
async def main():
    """Main function to run integrated scraper"""
    print("🚀 STARTING INTEGRATED FIKFAP SCRAPER")
    print("="*80)

    # Initialize integrated scraper
    scraper = IntegratedFikFapScraper("https://fikfap.com")

    # Scrape and extract in one go
    results = await scraper.scrape_and_extract(
        wait_time=20,  # Wait 20 seconds for API calls
        extract_data=True  # Automatically extract posts
    )

    print("\n🎉 INTEGRATION COMPLETED!")
    print("="*80)
    print(f"📄 API data saved")
    print(f"📊 Posts extracted: {results['total_posts']}")
    print("📁 Check generated files:")
    print("   • integrated_api_capture_TIMESTAMP.json (raw API data)")
    print("   • integrated_extracted_posts.json (extracted posts)")
    print("   • high_quality_posts.json (filtered high quality)")
    print("   • top_popular_posts.json (most popular)")

    return results

if __name__ == "__main__":
    asyncio.run(main())