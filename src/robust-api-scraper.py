import asyncio
import json
from datetime import datetime
from playwright.async_api import async_playwright

class PlaywrightAPIScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.api_requests = []
        self.api_responses = []
        self.all_requests = []
        
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
                            print(f"⚠️  Failed to parse JSON from: {response.url}")
                            
                except Exception as e:
                    print(f"⚠️  Could not get body for {response.url}: {e}")
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
    
    async def scrape_api_calls(self, wait_time=15):
        """Scrape using pure Playwright"""
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
                        print(f"🖱️  Found {len(buttons)} clickable elements, trying first few...")
                        for i, button in enumerate(buttons[:3]):
                            try:
                                await button.click()
                                await page.wait_for_timeout(2000)
                                print(f"   Clicked element {i+1}")
                            except:
                                pass
                except Exception as e:
                    print(f"⚠️  Could not interact with page elements: {e}")
                
                print("✅ Collection completed!")
                
            except Exception as e:
                print(f"Error during scraping: {e}")
            
            finally:
                await browser.close()
    
    def print_results(self):
        """Print captured API calls and responses"""
        print("\n" + "="*80)
        print("🎯 PLAYWRIGHT API CAPTURE RESULTS")
        print("="*80)
        
        print(f"📊 SUMMARY:")
        print(f"   Total requests captured: {len(self.all_requests)}")
        print(f"   API requests found: {len(self.api_requests)}")
        print(f"   API responses captured: {len(self.api_responses)}")
        
        # Show API requests
        if self.api_requests:
            print(f"\n🔥 API REQUESTS ({len(self.api_requests)} found):")
            print("-" * 80)
            for i, req in enumerate(self.api_requests, 1):
                print(f"[{i:02d}] {req['method']} {req['url']}")
                if req.get('post_data'):
                    print(f"     POST: {req['post_data'][:150]}...")
                print(f"     Type: {req['resource_type']}")
                print()
        
        # Show API responses with data
        if self.api_responses:
            print(f"\n💎 API RESPONSES ({len(self.api_responses)} found):")
            print("-" * 80)
            for i, resp in enumerate(self.api_responses, 1):
                print(f"[{i:02d}] {resp['status']} {resp['url']}")
                
                if resp.get('json'):
                    json_preview = json.dumps(resp['json'], indent=2)[:500]
                    print(f"     JSON Data:\n{json_preview}")
                    if len(json.dumps(resp['json'])) > 500:
                        print("     ... (truncated)")
                elif resp.get('body'):
                    body_preview = resp['body'][:200]
                    print(f"     Body: {body_preview}...")
                elif resp.get('body_error'):
                    print(f"     Body Error: {resp['body_error']}")
                print()
        
        # Show sample of all requests for debugging
        print(f"\n🔍 ALL REQUESTS SAMPLE ({min(10, len(self.all_requests))} of {len(self.all_requests)}):")
        print("-" * 80)
        for i, req in enumerate(self.all_requests[:10], 1):
            print(f"[{i:02d}] {req['method']} {req['resource_type']} {req['url'][:80]}...")
        
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
        
        with open(f"playwright_api_capture_{timestamp}.json", "w") as f:
            json.dump(api_data, f, indent=2, default=str)
        
        # Save all requests for debugging
        with open(f"all_requests_{timestamp}.json", "w") as f:
            json.dump({
                'url': self.base_url,
                'timestamp': timestamp,
                'all_requests': self.all_requests
            }, f, indent=2, default=str)
        
        print(f"💾 Results saved:")
        print(f"   • playwright_api_capture_{timestamp}.json")
        print(f"   • all_requests_{timestamp}.json")

# Usage
async def main():
    scraper = PlaywrightAPIScraper("https://fikfap.com")
    await scraper.scrape_api_calls(wait_time=20)  # Wait 20 seconds
    
    scraper.print_results()
    scraper.save_results()

if __name__ == "__main__":
    asyncio.run(main())