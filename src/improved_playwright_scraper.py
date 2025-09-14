import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright
import re

class ImprovedPlaywrightScraper:
    def __init__(self, base_url, capture_mode='all'):
        self.base_url = base_url
        self.api_requests = []
        self.api_responses = []
        self.all_requests = []
        self.all_responses = []
        self.capture_mode = capture_mode  # 'all', 'api_only', 'custom'

    def should_capture_request(self, url, resource_type):
        """Determine if a request should be captured based on mode"""
        if self.capture_mode == 'all':
            return True

        if self.capture_mode == 'api_only':
            return self.is_likely_api_request(url, resource_type)

        # Custom mode - exclude only obvious static assets
        if self.capture_mode == 'custom':
            exclude_patterns = [
                r'\.(css|js|png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|otf|eot)$',
                r'fonts\.(googleapis|gstatic)\.com',
                r'analytics\.google\.com',
                r'googletagmanager\.com',
                r'doubleclick\.net'
            ]
            return not any(re.search(pattern, url, re.IGNORECASE) for pattern in exclude_patterns)

        return True

    def is_likely_api_request(self, url, resource_type):
        """More inclusive API detection"""
        url_lower = url.lower()

        # Definite API patterns
        api_patterns = [
            r'/api/',
            r'/v\d+/',
            r'graphql',
            r'\.json',
            r'/ajax/',
            r'/xhr/',
            r'/endpoint/',
            r'/data/',
            r'/service/',
            r'api\.',
            r'/get/',
            r'/post/',
            r'/fetch/'
        ]

        # Resource types that might be API calls
        api_resource_types = ['fetch', 'xhr', 'websocket']

        return (any(re.search(pattern, url_lower) for pattern in api_patterns) or 
                resource_type in api_resource_types)

    async def intercept_request(self, request):
        """Handle intercepted requests"""
        try:
            request_data = {
                'url': request.url,
                'method': request.method,
                'resource_type': request.resource_type,
                'timestamp': datetime.now().isoformat()
            }

            # Always log all requests
            self.all_requests.append(request_data)

            # Capture detailed info if it matches our criteria
            if self.should_capture_request(request.url, request.resource_type):
                try:
                    headers = dict(await request.all_headers())
                    detailed_request = request_data.copy()
                    detailed_request.update({
                        'headers': headers,
                        'post_data': request.post_data,
                    })
                    self.api_requests.append(detailed_request)
                    print(f"🎯 Capturing Request: {request.method} {request.url}")
                except Exception as e:
                    print(f"⚠️ Error getting request details for {request.url}: {e}")

        except Exception as e:
            print(f"Error intercepting request: {e}")

    async def intercept_response(self, response):
        """Handle intercepted responses with better error handling"""
        try:
            response_data = {
                'url': response.url,
                'status': response.status,
                'status_text': response.status_text,
                'timestamp': datetime.now().isoformat(),
                'request_method': response.request.method,
                'resource_type': response.request.resource_type
            }

            # Always log basic response info
            self.all_responses.append(response_data)

            # Capture detailed info if it matches our criteria
            if self.should_capture_request(response.url, response.request.resource_type):
                try:
                    headers = dict(await response.all_headers())
                    detailed_response = response_data.copy()
                    detailed_response['headers'] = headers

                    # Attempt to get response body with multiple strategies
                    body_captured = False

                    # Strategy 1: Try to get body as text
                    if not body_captured:
                        try:
                            body_text = await response.text()
                            if body_text:
                                detailed_response['body'] = body_text
                                detailed_response['body_size'] = len(body_text)
                                body_captured = True

                                # Try to parse as JSON
                                if self.is_json_response(body_text, headers):
                                    try:
                                        detailed_response['json'] = json.loads(body_text)
                                        print(f"💎 JSON Response: {response.status} {response.url}")
                                    except json.JSONDecodeError as je:
                                        print(f"⚠️ JSON parse error for {response.url}: {je}")

                        except Exception as e:
                            print(f"⚠️ Could not get text body for {response.url}: {e}")

                    # Strategy 2: Try to get body as bytes if text failed
                    if not body_captured:
                        try:
                            body_bytes = await response.body()
                            if body_bytes:
                                # Try to decode as UTF-8
                                try:
                                    body_text = body_bytes.decode('utf-8')
                                    detailed_response['body'] = body_text
                                    body_captured = True
                                except UnicodeDecodeError:
                                    # Store as base64 for binary data
                                    import base64
                                    detailed_response['body_base64'] = base64.b64encode(body_bytes).decode('ascii')
                                    detailed_response['body_size'] = len(body_bytes)
                                    body_captured = True

                        except Exception as e:
                            print(f"⚠️ Could not get binary body for {response.url}: {e}")

                    if not body_captured:
                        detailed_response['body_error'] = 'Could not retrieve response body'

                    self.api_responses.append(detailed_response)
                    print(f"📡 Response Captured: {response.status} {response.url} ({response.request.resource_type})")

                except Exception as e:
                    print(f"Error getting response details for {response.url}: {e}")

        except Exception as e:
            print(f"Error intercepting response: {e}")

    def is_json_response(self, body_text, headers):
        """Check if response contains JSON"""
        content_type = headers.get('content-type', '').lower()
        return (
            'application/json' in content_type or
            'text/json' in content_type or
            (body_text.strip().startswith(('{', '['))) and 
            body_text.strip().endswith(('}', ']'))
        )

    async def scrape_with_interactions(self, wait_time=15):
        """Enhanced scraping with better page interactions"""
        async with async_playwright() as p:
            # Use a more realistic browser setup
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--no-sandbox'
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1280, 'height': 720}
            )

            page = await context.new_page()

            # Set up request/response interception
            page.on("request", self.intercept_request)
            page.on("response", self.intercept_response)

            try:
                print(f"🚀 Navigating to: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                print(f"⏳ Initial wait of {wait_time} seconds...")
                await page.wait_for_timeout(wait_time * 1000)

                # Enhanced page interactions
                print("🖱️ Performing page interactions...")

                # Scroll down in stages
                for i in range(3):
                    await page.evaluate(f"window.scrollTo(0, {(i+1) * 500})")
                    await page.wait_for_timeout(2000)

                # Try to hover over elements that might trigger API calls
                try:
                    # Look for video thumbnails, cards, etc.
                    interactive_selectors = [
                        'img[src*="screenshot"]',
                        '.video-item',
                        '.thumb',
                        'a[href*="video"]',
                        'div[class*="card"]'
                    ]

                    for selector in interactive_selectors:
                        elements = await page.query_selector_all(selector)
                        if elements:
                            print(f"Found {len(elements)} {selector} elements")
                            # Hover over first few elements
                            for i, element in enumerate(elements[:5]):
                                try:
                                    await element.hover()
                                    await page.wait_for_timeout(1000)
                                except:
                                    pass
                            break

                except Exception as e:
                    print(f"⚠️ Error during interactions: {e}")

                # Final wait
                print("⌛ Final wait for any remaining requests...")
                await page.wait_for_timeout(5000)

                print("✅ Collection completed!")

            except Exception as e:
                print(f"Error during scraping: {e}")
            finally:
                await browser.close()

    def print_detailed_results(self):
        """Print comprehensive results"""
        print("\n" + "="*80)
        print("🎯 IMPROVED PLAYWRIGHT CAPTURE RESULTS")
        print("="*80)

        print(f"📊 SUMMARY:")
        print(f"   Total requests: {len(self.all_requests)}")
        print(f"   Detailed requests captured: {len(self.api_requests)}")
        print(f"   Total responses: {len(self.all_responses)}")
        print(f"   Detailed responses captured: {len(self.api_responses)}")
        print(f"   Capture mode: {self.capture_mode}")

        # Show resource type breakdown
        resource_types = {}
        for req in self.all_requests:
            rt = req.get('resource_type', 'unknown')
            resource_types[rt] = resource_types.get(rt, 0) + 1

        print(f"\n📈 RESOURCE TYPES:")
        for rt, count in sorted(resource_types.items(), key=lambda x: x[1], reverse=True):
            print(f"   {rt}: {count}")

        # Show detailed requests
        if self.api_requests:
            print(f"\n🔥 DETAILED REQUESTS ({len(self.api_requests)}):")
            print("-" * 80)
            for i, req in enumerate(self.api_requests[:20], 1):  # Show first 20
                print(f"[{i:02d}] {req['method']} {req['url']}")
                print(f"     Type: {req.get('resource_type', 'unknown')}")
                if req.get('post_data'):
                    print(f"     POST: {str(req['post_data'])[:100]}...")

        # Show detailed responses with content
        if self.api_responses:
            print(f"\n💎 DETAILED RESPONSES ({len(self.api_responses)}):")
            print("-" * 80)
            for i, resp in enumerate(self.api_responses[:20], 1):  # Show first 20
                print(f"[{i:02d}] {resp['status']} {resp['url']}")
                print(f"     Method: {resp.get('request_method', 'GET')} | Type: {resp.get('resource_type', 'unknown')}")

                if resp.get('json'):
                    json_str = json.dumps(resp['json'], indent=2)
                    preview = json_str[:300] + ("..." if len(json_str) > 300 else "")
                    print(f"     JSON: {preview}")
                elif resp.get('body'):
                    body_preview = str(resp['body'])[:200] + ("..." if len(str(resp['body'])) > 200 else "")
                    print(f"     Body: {body_preview}")
                elif resp.get('body_base64'):
                    print(f"     Binary data: {resp.get('body_size', 'unknown')} bytes")
                elif resp.get('body_error'):
                    print(f"     Body error: {resp['body_error']}")

                print()

        print("="*80 + "\n")

    def save_comprehensive_results(self):
        """Save all results with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Main results file
        results = {
            'url': self.base_url,
            'timestamp': timestamp,
            'capture_mode': self.capture_mode,
            'summary': {
                'total_requests': len(self.all_requests),
                'detailed_requests': len(self.api_requests), 
                'total_responses': len(self.all_responses),
                'detailed_responses': len(self.api_responses)
            },
            'detailed_requests': self.api_requests,
            'detailed_responses': self.api_responses
        }

        with open(f"comprehensive_capture_{timestamp}.json", "w") as f:
            json.dump(results, f, indent=2, default=str)

        # All requests for debugging
        with open(f"all_requests_debug_{timestamp}.json", "w") as f:
            json.dump({
                'url': self.base_url,
                'timestamp': timestamp,
                'all_requests': self.all_requests,
                'all_responses': self.all_responses
            }, f, indent=2, default=str)

        print(f"💾 Results saved:")
        print(f"   • comprehensive_capture_{timestamp}.json")
        print(f"   • all_requests_debug_{timestamp}.json")

# Usage example - simple single run
async def run_single_capture(mode='all', wait_time=15):
    """Run a single capture session"""
    scraper = ImprovedPlaywrightScraper("https://rule34video.com/", capture_mode=mode)
    await scraper.scrape_with_interactions(wait_time=wait_time)
    scraper.print_detailed_results()
    scraper.save_comprehensive_results()
    return scraper

# Usage example - test all modes
async def test_all_modes():
    """Test different capture modes"""
    modes = ['all', 'custom', 'api_only']

    for mode in modes:
        print(f"\n{'='*60}")
        print(f"TESTING CAPTURE MODE: {mode.upper()}")
        print('='*60)

        scraper = ImprovedPlaywrightScraper("https://rule34video.com/", capture_mode=mode)
        await scraper.scrape_with_interactions(wait_time=10)
        scraper.print_detailed_results()
        scraper.save_comprehensive_results()

        if mode != modes[-1]:
            print("\n⏸️ Waiting 5 seconds before next test...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    # Simple usage - just run one capture in 'all' mode
    asyncio.run(run_single_capture(mode='all', wait_time=20))