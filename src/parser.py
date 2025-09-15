import asyncio
import json
import re
import html
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse

class VideoParser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.video_urls = []
        self.parsed_videos = []

    async def get_video_urls(self):
        """Get all video URLs from the main page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(3000)

                xpath = '//*[@id="custom_list_videos_most_recent_videos_items"]/div'
                video_elements = await page.query_selector_all(f'xpath={xpath}')
                print(f"📹 Found {len(video_elements)} videos")

                video_urls = []
                for element in video_elements:
                    try:
                        link = await element.query_selector('a.th.js-open-popup')
                        if link:
                            href = await link.get_attribute('href')
                            if href:
                                video_urls.append(urljoin(self.base_url, href))
                    except:
                        continue

                self.video_urls = video_urls
                print(f"✅ Collected {len(self.video_urls)} video URLs")

            except Exception as e:
                print(f"❌ Error: {e}")
            finally:
                await browser.close()

        return self.video_urls

    def get_json_data(self, html_content):
        """Extract JSON-LD data"""
        pattern = r'<script[^>]*type=["\']application/ld\\+json["\'][^>]*>(.*?)</script>'
        match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)

        if match:
            try:
                json_str = match.group(1).replace(r'\/', '/')
                return json.loads(json_str)
            except:
                pass
        return None

    def get_download_url(self, html_content):
        """Get highest quality download URL from Download section"""
        try:
            # Find Download section
            pattern = r'<div class="label">Download</div>(.*?)(?=<div class="label">|<div class="col">|</div>\s*</div>)'
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)

            if not match:
                return None

            download_section = match.group(1)

            # Extract all quality links
            link_pattern = r'<a[^>]*href="([^"]+)"[^>]*>MP4\s+(\d+)p</a>'
            links = re.findall(link_pattern, download_section, re.IGNORECASE)

            if not links:
                return None

            # Sort by quality (highest first)
            quality_links = [(int(quality), html.unescape(url)) for url, quality in links]
            quality_links.sort(key=lambda x: x[0], reverse=True)

            # Get highest quality and remove &download=true
            _, best_url = quality_links[0]
            best_url = best_url.replace('&download=true', '').replace('&amp;download=true', '')

            return best_url

        except:
            return None

    def get_views_and_duration(self, html_content):
        """Extract views and duration"""
        views = "0"
        duration = "Unknown"

        try:
            # Views pattern
            views_match = re.search(r'<span>([0-9.,KM]+)\s*\(([0-9,]+)\)</span>', html_content)
            if views_match:
                views = int(views_match.group(2).replace(',', ''))

            # Duration pattern  
            duration_match = re.search(r'<span>(\d+:\d{2})</span>', html_content)
            if duration_match:
                duration = duration_match.group(1)

        except:
            pass

        return views, duration

    async def parse_video(self, video_url):
        """Parse individual video"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(video_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(2000)
                html_content = await page.content()

                # Extract video ID
                video_id = re.search(r'/video/(\d+)/', video_url)
                video_id = video_id.group(1) if video_id else "unknown"

                # Get JSON data
                json_data = self.get_json_data(html_content)

                # Get basic info
                title = json_data.get('name', 'Unknown') if json_data else 'Unknown'
                thumbnail = json_data.get('thumbnailUrl', '') if json_data else ''
                description = json_data.get('description', '') if json_data else ''

                # Get views and duration
                views, duration = self.get_views_and_duration(html_content)

                # Get download URL
                video_src = self.get_download_url(html_content)

                return {
                    "video_id": video_id,
                    "url": video_url,
                    "title": title,
                    "duration": duration,
                    "views": str(views),
                    "thumbnail_src": thumbnail,
                    "video_src": video_src or "",
                    "description": description
                }

            except Exception as e:
                print(f"❌ Error parsing {video_url}: {e}")
                return None
            finally:
                await browser.close()

    async def parse_all_videos(self):
        """Parse all videos with progress tracking"""
        if not self.video_urls:
            print("⚠️ No URLs found. Run get_video_urls() first.")
            return []

        print(f"🚀 Parsing {len(self.video_urls)} videos...")

        for i, video_url in enumerate(self.video_urls, 1):
            try:
                print(f"📝 Parsing {i}/{len(self.video_urls)}: {video_url.split('/')[-2]}")

                video_data = await self.parse_video(video_url)
                if video_data:
                    self.parsed_videos.append(video_data)
                    quality = "Unknown"
                    if video_data['video_src']:
                        quality_match = re.search(r'_(\d+)p', video_data['video_src'])
                        quality = quality_match.group(1) + 'p' if quality_match else 'Unknown'

                    print(f"   ✅ {video_data['title'][:50]}... ({quality})")
                else:
                    print(f"   ❌ Failed to parse")

                # Small delay to be respectful
                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue

        print(f"✅ Parsed {len(self.parsed_videos)} videos successfully")
        return self.parsed_videos

    def save_data(self, filename="videos.json"):
        """Save parsed data"""
        if not self.parsed_videos:
            print("⚠️ No data to save")
            return

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_videos, f, indent=2, ensure_ascii=False)

        print(f"💾 Saved {len(self.parsed_videos)} videos to {filename}")

async def main():
    parser = VideoParser("https://rule34video.com/")

    # Get all video URLs
    await parser.get_video_urls()

    # Parse all videos (no limit)
    await parser.parse_all_videos()

    # Save data  
    parser.save_data("videos.json")

if __name__ == "__main__":
    asyncio.run(main())
