import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
import re
from urllib.parse import urljoin

class StaticVideoAPIScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.video_urls = []
        self.structured_video_data = []

    def should_capture_request(self, url, resource_type):
        """Only capture API-like requests, skip obvious static assets"""
        url_lower = url.lower()

        # Skip static assets that never contain useful data
        skip_patterns = [
            r'\.(css|woff2?|ttf|otf|eot|ico)$',
            r'fonts\.(googleapis|gstatic)\.com',
            r'\.(png|jpg|jpeg|gif|webp|svg)$'  # Skip images
        ]

        # Focus on likely API endpoints
        api_patterns = [
            r'/api/',
            r'/get/',
            r'/ajax/',
            r'/endpoint/',
            r'/data/',
            r'\.json',
            r'/collect',
            r'/track',
            r'/analytics'
        ]

        # Skip if it matches skip patterns
        if any(re.search(pattern, url_lower, re.IGNORECASE) for pattern in skip_patterns):
            return False

        # Include if it matches API patterns OR is fetch/xhr resource type
        return (any(re.search(pattern, url_lower) for pattern in api_patterns) or 
                resource_type in ['fetch', 'xhr', 'websocket'])

    async def extract_video_urls(self):
        """Extract video URLs from main page quickly"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Headless for speed
            page = await browser.new_page()

            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                # Minimal wait for content
                await page.wait_for_timeout(3000)

                # Extract using XPath
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                video_elements = await page.query_selector_all(f'xpath={xpath}')

                print(f"📹 Found {len(video_elements)} video elements")

                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        link_element = await element.query_selector('a.th.js-open-popup')
                        if link_element:
                            href = await link_element.get_attribute('href')
                            if href:
                                full_url = urljoin(self.base_url, href)
                                video_urls.append(full_url)
                    except Exception as e:
                        print(f"⚠️ Error extracting URL from video {i+1}: {e}")

                self.video_urls = video_urls
                print(f"🎯 Extracted {len(self.video_urls)} video URLs")

            except Exception as e:
                print(f"❌ Error loading main page: {e}")
            finally:
                await browser.close()

        return self.video_urls

    async def scrape_static_video_data(self, video_url, video_index):
        """Scrape static API responses from a single video page (NO INTERACTIONS)"""
        static_requests = []
        static_responses = []

        async def intercept_request(request):
            try:
                if self.should_capture_request(request.url, request.resource_type):
                    request_data = {
                        'url': request.url,
                        'method': request.method,
                        'resource_type': request.resource_type,
                        'timestamp': datetime.now().isoformat()
                    }

                    try:
                        headers = dict(await request.all_headers())
                        request_data['headers'] = headers
                        if request.post_data:
                            request_data['post_data'] = request.post_data
                    except:
                        pass

                    static_requests.append(request_data)
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
                        'resource_type': response.request.resource_type
                    }

                    try:
                        headers = dict(await response.all_headers())
                        response_data['headers'] = headers

                        # Get response body efficiently
                        try:
                            body_text = await response.text()
                            if body_text:
                                response_data['body'] = body_text
                                response_data['body_size'] = len(body_text)

                                # Parse JSON if applicable
                                if self.is_json_response(body_text, headers):
                                    try:
                                        response_data['json'] = json.loads(body_text)
                                    except json.JSONDecodeError:
                                        pass
                        except Exception:
                            # Fallback to binary
                            try:
                                body_bytes = await response.body()
                                if body_bytes:
                                    response_data['body'] = body_bytes.decode('utf-8', errors='ignore')
                                    response_data['body_size'] = len(body_bytes)
                            except Exception:
                                response_data['body_error'] = "Could not retrieve response body"
                    except Exception:
                        pass

                    static_responses.append(response_data)

            except Exception as e:
                print(f"Error intercepting response: {e}")

        # Scrape the video page with minimal interactions
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Headless for speed
            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = await context.new_page()

            page.on("request", intercept_request)
            page.on("response", intercept_response)

            try:
                print(f"[{video_index:02d}] 📄 Loading: {video_url}")
                await page.goto(video_url, wait_until='domcontentloaded')

                # ONLY wait for static content to load - NO INTERACTIONS
                await page.wait_for_timeout(5000)  # Just wait for natural loading

                print(f"[{video_index:02d}] ✅ {len(static_requests)} requests, {len(static_responses)} responses")

            except Exception as e:
                print(f"[{video_index:02d}] ❌ Error: {e}")
            finally:
                await browser.close()

        return {
            'video_index': video_index,
            'video_url': video_url,
            'static_requests': static_requests,
            'static_responses': static_responses,
            'summary': {
                'requests_count': len(static_requests),
                'responses_count': len(static_responses),
                'responses_with_data': len([r for r in static_responses if r.get('body') or r.get('json')])
            }
        }

    def is_json_response(self, body_text, headers):
        """Check if response is JSON"""
        content_type = headers.get('content-type', '').lower()
        return (
            'application/json' in content_type or
            'text/json' in content_type or
            (body_text.strip().startswith(('{', '['))) and 
            body_text.strip().endswith(('}', ']'))
        )

    async def process_all_videos(self, max_videos=35):
        """Process all videos efficiently with structured output"""
        if not self.video_urls:
            await self.extract_video_urls()

        videos_to_process = self.video_urls[:max_videos]
        print(f"\n🎯 Processing {len(videos_to_process)} videos (static content only)...")

        for i, video_url in enumerate(videos_to_process, 1):
            try:
                video_data = await self.scrape_static_video_data(video_url, i)
                self.structured_video_data.append(video_data)

                # Minimal delay for politeness
                if i < len(videos_to_process):
                    await asyncio.sleep(1)  # Reduced delay

            except Exception as e:
                print(f"❌ Error processing video {i}: {e}")
                continue

        print(f"\n🎉 Processed {len(self.structured_video_data)} videos!")
        return self.structured_video_data

    def create_structured_output(self):
        """Create structured video response data format"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Calculate aggregated stats
        total_requests = sum(len(vd['static_requests']) for vd in self.structured_video_data)
        total_responses = sum(len(vd['static_responses']) for vd in self.structured_video_data)
        responses_with_data = sum(vd['summary']['responses_with_data'] for vd in self.structured_video_data)

        # Collect all unique API endpoints
        unique_endpoints = set()
        api_patterns = {}

        for video_data in self.structured_video_data:
            for response in video_data['static_responses']:
                url = response['url']
                unique_endpoints.add(url)

                # Categorize API patterns
                if '/get/' in url.lower():
                    api_patterns['GET_ENDPOINTS'] = api_patterns.get('GET_ENDPOINTS', 0) + 1
                elif '/api/' in url.lower():
                    api_patterns['API_ENDPOINTS'] = api_patterns.get('API_ENDPOINTS', 0) + 1
                elif 'analytics' in url.lower():
                    api_patterns['ANALYTICS'] = api_patterns.get('ANALYTICS', 0) + 1
                elif '.json' in url.lower():
                    api_patterns['JSON_ENDPOINTS'] = api_patterns.get('JSON_ENDPOINTS', 0) + 1
                else:
                    api_patterns['OTHER'] = api_patterns.get('OTHER', 0) + 1

        structured_output = {
            'scraper_info': {
                'timestamp': timestamp,
                'base_url': self.base_url,
                'scraper_type': 'static_video_api',
                'total_videos_processed': len(self.structured_video_data)
            },
            'aggregated_stats': {
                'total_requests': total_requests,
                'total_responses': total_responses,
                'responses_with_data': responses_with_data,
                'unique_endpoints': len(unique_endpoints),
                'api_pattern_breakdown': api_patterns
            },
            'video_data': [
                {
                    'video_info': {
                        'index': vd['video_index'],
                        'url': vd['video_url'],
                        'processing_summary': vd['summary']
                    },
                    'api_requests': vd['static_requests'],
                    'api_responses': vd['static_responses']
                }
                for vd in self.structured_video_data
            ],
            'unique_endpoints_list': sorted(list(unique_endpoints))
        }

        return structured_output

    def save_structured_results(self):
        """Save results in structured format"""
        structured_data = self.create_structured_output()
        timestamp = structured_data['scraper_info']['timestamp']

        # Main structured results
        filename = f"structured_video_api_{timestamp}.json"
        with open(filename, "w") as f:
            json.dump(structured_data, f, indent=2, default=str)

        # Quick summary
        summary = {
            'timestamp': timestamp,
            'total_videos': structured_data['scraper_info']['total_videos_processed'],
            'total_api_calls': structured_data['aggregated_stats']['total_responses'],
            'unique_endpoints': structured_data['aggregated_stats']['unique_endpoints'],
            'api_patterns': structured_data['aggregated_stats']['api_pattern_breakdown'],
            'per_video_summary': [
                {
                    'video_index': vd['video_info']['index'],
                    'video_url': vd['video_info']['url'],
                    'api_responses_count': len(vd['api_responses']),
                    'responses_with_data': vd['video_info']['processing_summary']['responses_with_data']
                }
                for vd in structured_data['video_data']
            ]
        }

        summary_filename = f"video_api_summary_{timestamp}.json"
        with open(summary_filename, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        print(f"💾 Results saved:")
        print(f"   • {filename} (complete structured data)")
        print(f"   • {summary_filename} (quick summary)")

        return filename, summary_filename

    def print_results(self):
        """Print concise results"""
        if not self.structured_video_data:
            print("No data to display")
            return

        structured = self.create_structured_output()

        print("\n" + "="*80)
        print("📊 STATIC VIDEO API SCRAPING RESULTS")
        print("="*80)

        print(f"Videos processed: {structured['scraper_info']['total_videos_processed']}")
        print(f"Total API responses: {structured['aggregated_stats']['total_responses']}")
        print(f"Responses with data: {structured['aggregated_stats']['responses_with_data']}")
        print(f"Unique endpoints: {structured['aggregated_stats']['unique_endpoints']}")

        print(f"\n📈 API Pattern Breakdown:")
        for pattern, count in structured['aggregated_stats']['api_pattern_breakdown'].items():
            print(f"   {pattern}: {count}")

        print(f"\n📹 Per-Video Results:")
        for vd in structured['video_data'][:10]:  # Show first 10
            info = vd['video_info']
            print(f"[{info['index']:02d}] {len(vd['api_responses']):2d} API responses - {info['url']}")

        if len(structured['video_data']) > 10:
            print(f"   ... and {len(structured['video_data']) - 10} more videos")

        print("="*80)

# Usage
async def main():
    scraper = StaticVideoAPIScraper("https://rule34video.com/")

    # Extract video URLs
    await scraper.extract_video_urls()

    if scraper.video_urls:
        # Process all videos (static content only)
        await scraper.process_all_videos(max_videos=35)

        # Show and save results
        scraper.print_results()
        scraper.save_structured_results()
    else:
        print("❌ No video URLs found")

if __name__ == "__main__":
    asyncio.run(main())