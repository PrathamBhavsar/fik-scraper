import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urljoin

class VideoURLExtractor:
    def __init__(self, base_url):
        self.base_url = base_url

    async def test_video_extraction(self):
        """Test video URL extraction without doing full scraping"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')

                # Wait for page to load
                print("⏳ Waiting for page to load completely...")
                await page.wait_for_timeout(8000)

                # First, let's check if the container exists
                container = await page.query_selector('#custom_list_videos_most_recent_videos_items')
                if not container:
                    print("❌ Container #custom_list_videos_most_recent_videos_items not found!")
                    # Try alternative selectors
                    alt_selectors = [
                        '.list_videos_most_recent_videos_items',
                        '[class*="most_recent"]',
                        '[class*="videos_items"]',
                        '.video-list',
                        '.videos-container'
                    ]

                    for selector in alt_selectors:
                        alt_container = await page.query_selector(selector)
                        if alt_container:
                            print(f"✅ Found alternative container: {selector}")
                            break
                    else:
                        print("❌ No video container found with any selector")
                        return []
                else:
                    print("✅ Found main container!")

                # Use XPath to find video items
                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                video_elements = await page.query_selector_all(f'xpath={xpath}')

                print(f"📹 Found {len(video_elements)} video elements with XPath")

                # If XPath didn't work, try CSS selector
                if len(video_elements) == 0:
                    print("🔄 XPath returned 0 results, trying CSS selector...")
                    video_elements = await page.query_selector_all('#custom_list_videos_most_recent_videos_items > div')
                    print(f"📹 CSS selector found {len(video_elements)} video elements")

                # Extract URLs
                video_urls = []
                for i, element in enumerate(video_elements):
                    try:
                        # Look for the video link
                        link_selectors = [
                            'a.th.js-open-popup',
                            'a[href*="/video/"]',
                            'a.th',
                            'a[href*="rule34video.com/video/"]'
                        ]

                        href = None
                        for selector in link_selectors:
                            link_element = await element.query_selector(selector)
                            if link_element:
                                href = await link_element.get_attribute('href')
                                if href:
                                    print(f"[{i+1:02d}] Found link with selector '{selector}': {href}")
                                    break

                        if href:
                            full_url = urljoin(self.base_url, href)
                            video_urls.append(full_url)
                        else:
                            print(f"[{i+1:02d}] ❌ No video link found in this element")
                            # Debug: show what's in this element
                            element_html = await element.inner_html()
                            print(f"     Element HTML preview: {element_html[:200]}...")

                    except Exception as e:
                        print(f"[{i+1:02d}] ⚠️ Error extracting from element: {e}")

                print(f"\n🎯 EXTRACTION RESULTS:")
                print(f"   Total video elements found: {len(video_elements)}")
                print(f"   Video URLs extracted: {len(video_urls)}")

                if video_urls:
                    print(f"\n📝 EXTRACTED VIDEO URLS:")
                    for i, url in enumerate(video_urls[:10], 1):  # Show first 10
                        print(f"[{i:02d}] {url}")

                    if len(video_urls) > 10:
                        print(f"     ... and {len(video_urls) - 10} more")
                else:
                    print("\n❌ No video URLs were extracted!")

                    # Debug: Show page structure
                    print("\n🔍 DEBUG INFO:")
                    page_title = await page.title()
                    print(f"Page title: {page_title}")

                    # Check if there are any video-related elements
                    debug_selectors = [
                        'a[href*="/video/"]',
                        '.video',
                        '.thumb', 
                        '[class*="video"]',
                        'img[src*="screenshot"]'
                    ]

                    for selector in debug_selectors:
                        elements = await page.query_selector_all(selector)
                        print(f"   {selector}: {len(elements)} elements")

                return video_urls

            except Exception as e:
                print(f"❌ Error during extraction: {e}")
                return []
            finally:
                await browser.close()

# Test function
async def test_extraction():
    extractor = VideoURLExtractor("https://rule34video.com/")
    urls = await extractor.test_video_extraction()

    if urls:
        print(f"\n✅ SUCCESS! Found {len(urls)} video URLs")
        print("\n🎬 You can now run the full video_page_api_scraper.py")
    else:
        print("\n❌ FAILED! No video URLs found.")
        print("\nPlease check:")
        print("1. Is the website structure different?")
        print("2. Are there any loading delays?")
        print("3. Do we need to handle any popups or cookies?")

if __name__ == "__main__":
    asyncio.run(test_extraction())