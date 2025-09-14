
import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
import re
from urllib.parse import urljoin, urlparse

class VideoPageAPIScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.video_urls = []
        self.all_video_data = []
        self.consolidated_requests = []
        self.consolidated_responses = []

    def should_capture_request(self, url, resource_type):
        """More permissive filtering - capture most requests"""
        url_lower = url.lower()

        # Only skip obvious static assets that never have useful data
        skip_patterns = [
            r'\.(css|woff2?|ttf|otf|eot)$',
            r'fonts\.(googleapis|gstatic)\.com'
        ]

        return not any(re.search(pattern, url_lower, re.IGNORECASE) for pattern in skip_patterns)

    async def extract_video_urls(self):
        """Extract all video page URLs from the main page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                # Wait for videos to load
                print("⏳ Waiting for video list to load...")
                await page.wait_for_timeout(5000)

                # Use the specific XPath to find video container
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'

                # Wait for the container to be present
                try:
                    await page.wait_for_selector('#custom_list_videos_most_recent_videos_items', timeout=10000)
                    print("✅ Video container found!")
                except:
                    print("⚠️ Video container not found, trying alternative approach...")

                # Extract video URLs using XPath
                video_elements = await page.query_selector_all(f'xpath={xpath}')
                print(f"📹 Found {len(video_elements)} video elements")

                video_urls = []

                for i, element in enumerate(video_elements):
                    try:
                        # Look for the main video link within each video item
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                                print(f"[{i+1:02d}] Found video: {full_url}")
                    except Exception as e:
                        print(f"⚠️ Error extracting URL from video {i+1}: {e}")

                self.video_urls = video_urls
                print(f"\n🎯 Successfully extracted {len(self.video_urls)} video URLs")

            except Exception as e:
                print(f"❌ Error loading main page: {e}")
            finally:
                await browser.close()

        return self.video_urls

    async def scrape_single_video_page(self, video_url, video_index):
        """Scrape a single video page and capture all requests/responses"""
        page_requests = []
        page_responses = []

        async def intercept_request(request):
            try:
                if self.should_capture_request(request.url, request.resource_type):
                    request_data = {
                        'url': request.url,
                        'method': request.method,
                        'resource_type': request.resource_type,
                        'timestamp': datetime.now().isoformat(),
                        'video_url': video_url,
                        'video_index': video_index
                    }

                    try:
                        headers = dict(await request.all_headers())
                        request_data['headers'] = headers
                        request_data['post_data'] = request.post_data
                    except:
                        pass

                    page_requests.append(request_data)
                    self.consolidated_requests.append(request_data)
            except Exception as e:
                print(f"Error intercepting request: {e}")

        async def intercept_response(response):
            try:
                if self.should_capture_request(response.url, response.request.resource_type):
                    response_data = {
                        'url': response.url,
                        'status': response.status,
                        'status_text': response.status_text,
                        'timestamp': datetime.now().isoformat(),
                        'request_method': response.request.method,
                        'resource_type': response.request.resource_type,
                        'video_url': video_url,
                        'video_index': video_index
                    }

                    try:
                        headers = dict(await response.all_headers())
                        response_data['headers'] = headers

                        # Multiple strategies to get response body
                        body_captured = False

                        # Strategy 1: Try text()
                        if not body_captured:
                            try:
                                body_text = await response.text()
                                if body_text:
                                    response_data['body'] = body_text
                                    response_data['body_size'] = len(body_text)
                                    body_captured = True

                                    # Try to parse JSON
                                    if self.is_json_response(body_text, headers):
                                        try:
                                            response_data['json'] = json.loads(body_text)
                                        except json.JSONDecodeError:
                                            pass
                            except Exception as e1:
                                # Strategy 2: Try body() as bytes
                                try:
                                    body_bytes = await response.body()
                                    if body_bytes:
                                        try:
                                            body_text = body_bytes.decode('utf-8', errors='ignore')
                                            response_data['body'] = body_text
                                            body_captured = True
                                        except:
                                            # Store as base64 for binary
                                            import base64
                                            response_data['body_base64'] = base64.b64encode(body_bytes).decode('ascii')
                                            response_data['body_size'] = len(body_bytes)
                                            body_captured = True
                                except Exception as e2:
                                    response_data['body_error'] = f"text() error: {e1}, body() error: {e2}"

                        if not body_captured:
                            response_data['body_error'] = "Could not retrieve response body"

                    except Exception as e:
                        response_data['headers_error'] = str(e)

                    page_responses.append(response_data)
                    self.consolidated_responses.append(response_data)

            except Exception as e:
                print(f"Error intercepting response: {e}")

        # Now scrape the individual video page
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720}
            )
            page = await context.new_page()

            # Set up interception
            page.on("request", intercept_request)
            page.on("response", intercept_response)

            try:
                print(f"[{video_index:02d}] 🎬 Loading video page: {video_url}")
                await page.goto(video_url, wait_until='domcontentloaded')

                # Wait for page to fully load
                await page.wait_for_timeout(8000)

                # Interact with the page to trigger more requests
                try:
                    # Scroll down
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)

                    # Try to interact with video player if present
                    video_player = await page.query_selector('video, .video-player, #player')
                    if video_player:
                        try:
                            await video_player.hover()
                            await page.wait_for_timeout(1000)
                        except:
                            pass

                    # Look for and hover over related videos or thumbnails
                    related_elements = await page.query_selector_all('img[src*="screenshot"], .thumb, .related-video')
                    for element in related_elements[:5]:  # First 5
                        try:
                            await element.hover()
                            await page.wait_for_timeout(500)
                        except:
                            pass

                except Exception as e:
                    print(f"⚠️ Error during page interactions: {e}")

                # Final wait
                await page.wait_for_timeout(3000)

                print(f"[{video_index:02d}] ✅ Captured {len(page_requests)} requests, {len(page_responses)} responses")

            except Exception as e:
                print(f"[{video_index:02d}] ❌ Error loading video page: {e}")
            finally:
                await browser.close()

        return {
            'video_url': video_url,
            'video_index': video_index,
            'requests': page_requests,
            'responses': page_responses,
            'summary': {
                'total_requests': len(page_requests),
                'total_responses': len(page_responses)
            }
        }

    def is_json_response(self, body_text, headers):
        """Check if response contains JSON"""
        content_type = headers.get('content-type', '').lower()
        return (
            'application/json' in content_type or
            'text/json' in content_type or
            (body_text.strip().startswith(('{', '['))) and 
            body_text.strip().endswith(('}', ']'))
        )

    async def scrape_all_video_pages(self, max_videos=35, delay_between_pages=2):
        """Scrape all video pages sequentially"""
        if not self.video_urls:
            await self.extract_video_urls()

        # Limit to max_videos
        videos_to_process = self.video_urls[:max_videos]
        print(f"\n🎯 Starting to scrape {len(videos_to_process)} video pages...")

        for i, video_url in enumerate(videos_to_process, 1):
            try:
                video_data = await self.scrape_single_video_page(video_url, i)
                self.all_video_data.append(video_data)

                # Delay between pages to be respectful
                if i < len(videos_to_process):
                    print(f"⏸️ Waiting {delay_between_pages} seconds before next video...")
                    await asyncio.sleep(delay_between_pages)

            except Exception as e:
                print(f"❌ Error processing video {i}: {e}")
                continue

        print(f"\n🎉 Completed scraping {len(self.all_video_data)} video pages!")
        return self.all_video_data

    def print_comprehensive_results(self):
        """Print detailed results from all video pages"""
        print("\n" + "="*100)
        print("🎬 VIDEO PAGE API CAPTURE RESULTS")
        print("="*100)

        print(f"📊 OVERALL SUMMARY:")
        print(f"   Videos processed: {len(self.all_video_data)}")
        print(f"   Total requests captured: {len(self.consolidated_requests)}")
        print(f"   Total responses captured: {len(self.consolidated_responses)}")

        # Show per-video summary
        print(f"\n📹 PER-VIDEO BREAKDOWN:")
        print("-" * 100)
        for video_data in self.all_video_data:
            idx = video_data['video_index']
            url = video_data['video_url']
            req_count = len(video_data['requests'])
            resp_count = len(video_data['responses'])
            print(f"[{idx:02d}] {req_count:2d} req, {resp_count:2d} resp - {url}")

        # Show resource type breakdown
        resource_types = {}
        for req in self.consolidated_requests:
            rt = req.get('resource_type', 'unknown')
            resource_types[rt] = resource_types.get(rt, 0) + 1

        print(f"\n📈 RESOURCE TYPES ACROSS ALL VIDEOS:")
        for rt, count in sorted(resource_types.items(), key=lambda x: x[1], reverse=True):
            print(f"   {rt}: {count}")

        # Show some sample requests
        print(f"\n🔥 SAMPLE REQUESTS (first 20):")
        print("-" * 100)
        for i, req in enumerate(self.consolidated_requests[:20], 1):
            video_idx = req.get('video_index', '?')
            print(f"[{i:02d}] Video {video_idx} - {req['method']} {req['url']}")
            print(f"     Type: {req.get('resource_type', 'unknown')}")

        # Show some sample responses with data
        responses_with_data = [r for r in self.consolidated_responses if r.get('body') or r.get('json') or r.get('body_base64')]

        if responses_with_data:
            print(f"\n💎 SAMPLE RESPONSES WITH DATA (first 10):")
            print("-" * 100)
            for i, resp in enumerate(responses_with_data[:10], 1):
                video_idx = resp.get('video_index', '?')
                print(f"[{i:02d}] Video {video_idx} - {resp['status']} {resp['url']}")

                if resp.get('json'):
                    json_str = json.dumps(resp['json'], indent=2)
                    preview = json_str[:200] + ("..." if len(json_str) > 200 else "")
                    print(f"     JSON: {preview}")
                elif resp.get('body'):
                    body_preview = str(resp['body'])[:150] + ("..." if len(str(resp['body'])) > 150 else "")
                    print(f"     Body: {body_preview}")
                elif resp.get('body_base64'):
                    print(f"     Binary data: {resp.get('body_size', 'unknown')} bytes")
                print()

        print("="*100 + "\n")

    def save_all_results(self):
        """Save comprehensive results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Main comprehensive results
        main_results = {
            'url': self.base_url,
            'timestamp': timestamp,
            'summary': {
                'videos_processed': len(self.all_video_data),
                'total_requests': len(self.consolidated_requests),
                'total_responses': len(self.consolidated_responses)
            },
            'video_urls': self.video_urls,
            'per_video_data': self.all_video_data,
            'consolidated_requests': self.consolidated_requests,
            'consolidated_responses': self.consolidated_responses
        }

        with open(f"video_pages_capture_{timestamp}.json", "w") as f:
            json.dump(main_results, f, indent=2, default=str)

        # Simplified summary for quick review
        summary_results = {
            'timestamp': timestamp,
            'summary': main_results['summary'],
            'video_urls': self.video_urls,
            'per_video_summary': [
                {
                    'video_index': vd['video_index'],
                    'video_url': vd['video_url'], 
                    'requests_count': len(vd['requests']),
                    'responses_count': len(vd['responses'])
                } for vd in self.all_video_data
            ]
        }

        with open(f"video_pages_summary_{timestamp}.json", "w") as f:
            json.dump(summary_results, f, indent=2, default=str)

        print(f"💾 Results saved:")
        print(f"   • video_pages_capture_{timestamp}.json (complete data)")
        print(f"   • video_pages_summary_{timestamp}.json (summary)")

# Usage
async def main():
    scraper = VideoPageAPIScraper("https://rule34video.com/")

    # Step 1: Extract video URLs
    video_urls = await scraper.extract_video_urls()

    if video_urls:
        print(f"\n✅ Found {len(video_urls)} video URLs")

        # Step 2: Scrape each video page
        await scraper.scrape_all_video_pages(max_videos=35, delay_between_pages=3)

        # Step 3: Show and save results
        scraper.print_comprehensive_results()
        scraper.save_all_results()
    else:
        print("❌ No video URLs found. Check the page structure.")

if __name__ == "__main__":
    asyncio.run(main())