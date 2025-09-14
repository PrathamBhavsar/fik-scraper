import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright

class FixedPlaywrightAPIScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.api_requests = []
        self.api_responses = []
        self.all_requests = []

    def is_api_request(self, url):
        """More permissive API request detection"""
        url_lower = url.lower()

        # FIXED: Less restrictive filtering - only skip obvious static assets
        skip_patterns = [
            '.css', '.woff', '.woff2', '.ttf', '.otf', '.eot',  # Fonts and CSS only
            'fonts.gstatic.com', 'fonts.googleapis.com',       # Google fonts only
        ]

        if any(pattern in url_lower for pattern in skip_patterns):
            return False

        # FIXED: More inclusive API patterns including the site's actual patterns
        api_patterns = [
            '/api/', '/v1/', '/v2/', '/v3/', '/v4/',
            'graphql', 'api.',
            '/endpoint/', '/data/', '/service/',
            '.json',
            'ajax', 'xhr',
            '/get/',      # ADDED: The site uses /get/ endpoints
            '/chicken.',  # ADDED: Site-specific patterns from your data
            '/solid.',    # ADDED: Site-specific patterns
            '/sn/',       # ADDED: Site-specific patterns
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

            # FIXED: Capture MORE requests, not just "API" ones
            if self.is_api_request(request.url) or request.resource_type in ['fetch', 'xhr']:
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
        """FIXED: Better response handling"""
        try:
            # FIXED: Capture responses for the same URLs we capture requests for
            if self.is_api_request(response.url) or response.request.resource_type in ['fetch', 'xhr']:
                print(f"📡 API Response: {response.status} {response.url}")
                headers = await response.all_headers()
                response_data = {
                    'url': response.url,
                    'status': response.status,
                    'status_text': response.status_text,
                    'headers': headers,
                    'timestamp': datetime.now().isoformat(),
                    'resource_type': response.request.resource_type
                }

                # FIXED: Multiple strategies to get response body
                body_retrieved = False

                # Strategy 1: Try text() first
                try:
                    body_text = await response.text()
                    if body_text:
                        response_data['body'] = body_text
                        body_retrieved = True

                        # Try to parse JSON
                        if self.is_json_content(body_text, headers):
                            try:
                                response_data['json'] = json.loads(body_text)
                                print(f"💎 Got JSON response from: {response.url}")
                            except json.JSONDecodeError:
                                print(f"⚠️ Failed to parse JSON from: {response.url}")

                except Exception as e1:
                    # Strategy 2: Try body() if text() fails
                    try:
                        body_bytes = await response.body()
                        if body_bytes:
                            try:
                                body_text = body_bytes.decode('utf-8', errors='ignore')
                                response_data['body'] = body_text
                                body_retrieved = True
                            except Exception as e2:
                                # Store as base64 for binary content
                                import base64
                                response_data['body_base64'] = base64.b64encode(body_bytes).decode('ascii')
                                response_data['body_size'] = len(body_bytes)
                                body_retrieved = True
                    except Exception as e2:
                        response_data['body_error'] = f"text() error: {e1}, body() error: {e2}"

                if not body_retrieved:
                    response_data['body_error'] = "Could not retrieve response body"

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

    async def scrape_api_calls(self, wait_time=15):
        """Scrape using pure Playwright"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            # Set up request/response interception
            page.on("request", self.intercept_request)
            page.on("response", self.intercept_response)

            try:
                print(f"🚀 Navigating to: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                print(f"⏳ Waiting {wait_time} seconds for API calls...")
                await page.wait_for_timeout(wait_time * 1000)

                # FIXED: Better page interaction
                print("📜 Scrolling and interacting with page...")

                # Scroll in stages
                for i in range(3):
                    await page.evaluate(f"window.scrollTo(0, {i * 800})")
                    await page.wait_for_timeout(3000)

                # Try hovering over images/videos
                try:
                    images = await page.query_selector_all('img[src*="screenshot"], img[src*="thumb"]')
                    if images:
                        print(f"🖱️ Found {len(images)} images, hovering over them...")
                        for i, img in enumerate(images[:10]):  # First 10
                            try:
                                await img.hover()
                                await page.wait_for_timeout(1000)
                            except:
                                pass
                except Exception as e:
                    print(f"⚠️ Could not interact with images: {e}")

                print("✅ Collection completed!")

            except Exception as e:
                print(f"Error during scraping: {e}")
            finally:
                await browser.close()

    def print_results(self):
        """Print captured API calls and responses"""
        print("\n" + "="*80)
        print("🎯 FIXED PLAYWRIGHT API CAPTURE RESULTS")
        print("="*80)
        print(f"📊 SUMMARY:")
        print(f" Total requests captured: {len(self.all_requests)}")
        print(f" API requests found: {len(self.api_requests)}")
        print(f" API responses captured: {len(self.api_responses)}")

        # Show what we're capturing
        if self.api_requests:
            print(f"\n🔥 API REQUESTS ({len(self.api_requests)} found):")
            print("-" * 80)
            for i, req in enumerate(self.api_requests, 1):
                print(f"[{i:02d}] {req['method']} {req['url']}")
                print(f" Type: {req['resource_type']}")
                if req.get('post_data'):
                    print(f" POST: {req['post_data'][:150]}...")
                print()

        # Show API responses with data
        if self.api_responses:
            print(f"\n💎 API RESPONSES ({len(self.api_responses)} found):")
            print("-" * 80)
            for i, resp in enumerate(self.api_responses, 1):
                print(f"[{i:02d}] {resp['status']} {resp['url']}")
                if resp.get('json'):
                    json_preview = json.dumps(resp['json'], indent=2)[:500]
                    print(f" JSON Data:\n{json_preview}")
                    if len(json.dumps(resp['json'])) > 500:
                        print(" ... (truncated)")
                elif resp.get('body'):
                    body_preview = resp['body'][:300]
                    print(f" Body: {body_preview}...")
                elif resp.get('body_base64'):
                    print(f" Binary data: {resp.get('body_size', 'unknown')} bytes")
                elif resp.get('body_error'):
                    print(f" Body Error: {resp['body_error']}")
                print()

        print("="*80 + "\n")

    def save_results(self):
        """Save results to JSON files"""
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

        with open(f"fixed_api_capture_{timestamp}.json", "w") as f:
            json.dump(api_data, f, indent=2, default=str)

        # Save all requests for debugging
        with open(f"all_requests_fixed_{timestamp}.json", "w") as f:
            json.dump({
                'url': self.base_url,
                'timestamp': timestamp,
                'all_requests': self.all_requests
            }, f, indent=2, default=str)

        print(f"💾 Results saved:")
        print(f" • fixed_api_capture_{timestamp}.json")
        print(f" • all_requests_fixed_{timestamp}.json")

# Usage
async def main():
    scraper = FixedPlaywrightAPIScraper("https://rule34video.com/")
    await scraper.scrape_api_calls(wait_time=20)
    scraper.print_results()
    scraper.save_results()

if __name__ == "__main__":
    asyncio.run(main())