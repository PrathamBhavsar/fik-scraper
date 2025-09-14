import asyncio
import json
import re
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse

class VideoDataParser:
    def __init__(self, base_url):
        self.base_url = base_url
        self.video_urls = []
        self.parsed_video_data = []

    async def extract_video_urls(self):
        """Extract video URLs from main page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"🚀 Loading main page: {self.base_url}")
                await page.goto(self.base_url, wait_until='domcontentloaded')
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

    def extract_json_ld_data(self, html_content):
        """Extract data from JSON-LD script tag"""
        # Find JSON-LD script tag with VideoObject

        json_ld_pattern = (
            r'<script[^>]*type=(["\'])application/ld\+json\1[^>]*>(.*?)</script>'
        )

        match = re.search(json_ld_pattern, html_content, re.DOTALL | re.IGNORECASE)

        try:
            if match:
                json_str = match.group(1)
                # Clean up the JSON string
                json_str = json_str.replace(r'\/', '/')
                json_data = json.loads(json_str)
                return json_data

            return None
        except Exception as e:
            print(f"⚠️ Error parsing JSON-LD: {e}")
            return None

    def extract_player_data(self, html_content):
        """Extract video URLs from player flashvars - FIXED VERSION"""
        try:
            # Find the flashvars object in the player script
            flashvars_pattern = r'var flashvars\s*=\s*\{([^}]+)\}'
            match = re.search(flashvars_pattern, html_content, re.DOTALL)

            if match:
                flashvars_content = match.group(1)

                # Extract video URLs with different qualities
                video_urls = {}

                # Extract video_url (usually 360p)
                video_url_match = re.search(r"video_url\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if video_url_match:
                    url = video_url_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['360p'] = url

                # Extract video_alt_url (usually 480p)
                alt_url_match = re.search(r"video_alt_url\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if alt_url_match:
                    url = alt_url_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['480p'] = url

                # Extract video_alt_url2 (usually 720p)
                alt_url2_match = re.search(r"video_alt_url2\s*:\s*['\"]([^'\"]+)['\"]", flashvars_content)
                if alt_url2_match:
                    url = alt_url2_match.group(1)
                    if url.startswith('function/0/'):
                        url = url[11:]
                    video_urls['720p'] = url

                # Return the highest quality available
                for quality in ['720p', '480p', '360p']:
                    if quality in video_urls and video_urls[quality]:
                        return video_urls[quality]

            return None

        except Exception as e:
            print(f"⚠️ Error parsing player data: {e}")
            return None

    def extract_views_and_duration(self, html_content):
        """Extract views and duration from HTML spans - NEW METHOD"""
        try:
            views = "0"
            duration = "Unknown"
            
            # Extract views from pattern like "1.2K (1,194)"
            # Look for the specific pattern in spans
            views_pattern = r'<span>([0-9.,KM]+)\s*\(([0-9,]+)\)</span>'
            views_match = re.search(views_pattern, html_content)
            
            if views_match:
                # Get the number in parentheses (more accurate)
                views_str = views_match.group(2)
                # Remove commas and convert to integer
                views = int(views_str.replace(',', ''))
            else:
                # Fallback: look for just the parentheses part
                fallback_pattern = r'\(([0-9,]+)\)'
                fallback_match = re.search(fallback_pattern, html_content)
                if fallback_match:
                    views_str = fallback_match.group(1)
                    views = int(views_str.replace(',', ''))
            
            # Extract duration from pattern like "6:48"
            duration_pattern = r'<span>(\d+:\d{2})</span>'
            duration_match = re.search(duration_pattern, html_content)
            
            if duration_match:
                duration = duration_match.group(1)
            
            return views, duration
            
        except Exception as e:
            print(f"⚠️ Error extracting views and duration: {e}")
            return "0", "Unknown"

    def convert_iso_duration_to_readable(self, iso_duration):
        """Convert ISO 8601 duration (PT0H6M48S) to readable format (6:48) - NEW METHOD"""
        try:
            if not iso_duration or not iso_duration.startswith('PT'):
                return "Unknown"
            
            # Remove PT prefix
            duration_str = iso_duration[2:]
            
            hours = 0
            minutes = 0
            seconds = 0
            
            # Extract hours
            hours_match = re.search(r'(\d+)H', duration_str)
            if hours_match:
                hours = int(hours_match.group(1))
            
            # Extract minutes
            minutes_match = re.search(r'(\d+)M', duration_str)
            if minutes_match:
                minutes = int(minutes_match.group(1))
                
            # Extract seconds
            seconds_match = re.search(r'(\d+)S', duration_str)
            if seconds_match:
                seconds = int(seconds_match.group(1))
            
            # Format as H:MM:SS or M:SS
            if hours > 0:
                return f"{hours}:{minutes:02d}:{seconds:02d}"
            else:
                return f"{minutes}:{seconds:02d}"
                
        except Exception as e:
            print(f"⚠️ Error converting duration: {e}")
            return "Unknown"

    def extract_artist_and_uploader(self, html_content):
        """Extract artist and uploader from the cols structure - FIXED VERSION"""
        try:
            artist_name = "Anonymous"
            uploader_name = "Anonymous"

            # Find the cols div using a more robust method
            cols_start = html_content.find('<div class="cols">')
            if cols_start == -1:
                return artist_name, uploader_name

            # Find the content after the opening tag
            content_start = cols_start + len('<div class="cols">')

            # Count opening and closing div tags to find the matching closing tag
            div_count = 1
            pos = content_start
            cols_end = -1

            while pos < len(html_content) and div_count > 0:
                # Find next div tag (opening or closing)
                next_open = html_content.find('<div', pos)
                next_close = html_content.find('</div>', pos)

                if next_close == -1:
                    break

                if next_open != -1 and next_open < next_close:
                    # Found opening div first
                    div_count += 1
                    pos = next_open + 4
                else:
                    # Found closing div first  
                    div_count -= 1
                    if div_count == 0:
                        cols_end = next_close
                        break
                    pos = next_close + 6

            if cols_end == -1:
                return artist_name, uploader_name

            # Extract the full cols content
            cols_content = html_content[content_start:cols_end]

            # Look for Artist section
            artist_match = re.search(r'<div class="label">Artist</div>(.*?)(?=<div class="col">|</div>\s*$)', cols_content, re.DOTALL | re.IGNORECASE)
            if artist_match:
                artist_section = artist_match.group(1)

                # Look for span with class "name"
                artist_name_match = re.search(r'<span class="name">([^<]+)</span>', artist_section, re.IGNORECASE)
                if artist_name_match:
                    artist_name = artist_name_match.group(1).strip()
                else:
                    # Fallback: look for alt attribute
                    artist_alt_match = re.search(r'alt="([^"]+)"', artist_section)
                    if artist_alt_match:
                        artist_name = artist_alt_match.group(1).strip()

            # Look for Uploaded by section
            uploader_match = re.search(r'<div class="label">Uploaded by</div>(.*?)(?=<div class="col">|</div>\s*$)', cols_content, re.DOTALL | re.IGNORECASE)
            if uploader_match:
                uploader_section = uploader_match.group(1)

                # Remove image tags and verified status divs first
                clean_section = re.sub(r'<img[^>]*>', '', uploader_section)
                clean_section = re.sub(r'<div class="verified-status">.*?</div>', '', clean_section, flags=re.DOTALL)

                # Look for text content within the anchor tag
                uploader_text_match = re.search(r'<a[^>]*>(.*?)</a>', clean_section, re.DOTALL | re.IGNORECASE)
                if uploader_text_match:
                    uploader_text = uploader_text_match.group(1)
                    # Clean up whitespace, newlines, and remaining HTML
                    uploader_text = re.sub(r'<[^>]*>', '', uploader_text)  # Remove any remaining HTML
                    uploader_name = re.sub(r'\s+', ' ', uploader_text).strip()

                    # If still empty, try alt attribute
                    if not uploader_name:
                        uploader_alt_match = re.search(r'alt="([^"]+)"', uploader_section)
                        if uploader_alt_match:
                            uploader_name = uploader_alt_match.group(1).strip()

            return artist_name, uploader_name

        except Exception as e:
            print(f"⚠️ Error extracting artist/uploader: {e}")
            return "Anonymous", "Anonymous"

    def extract_tags_from_html(self, html_content):
        """Extract tags from the HTML content"""
        try:
            tags = []
            # Find tags in the tag items
            tag_pattern = r'<a class="tag_item" href="[^"]*">([^<]+)</a>'
            tag_matches = re.findall(tag_pattern, html_content)
            tags = [tag.strip() for tag in tag_matches if tag.strip()]
            return tags
        except Exception as e:
            print(f"⚠️ Error extracting tags: {e}")
            return []

    def extract_categories_from_html(self, html_content):
        """Extract categories from the HTML content"""
        try:
            categories = []
            # Look for category links
            category_pattern = r'<a class="item btn_link" href="[^"]*categories/[^"]*">([^<]+)</a>'
            category_matches = re.findall(category_pattern, html_content)
            categories = [cat.strip() for cat in category_matches if cat.strip()]
            return categories
        except Exception as e:
            print(f"⚠️ Error extracting categories: {e}")
            return []

    async def parse_individual_video(self, video_url):
        """Parse data from individual video page - COMPLETE FIXED VERSION"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                print(f"🔍 Parsing: {video_url}")
                await page.goto(video_url, wait_until='domcontentloaded')
                await page.wait_for_timeout(2000)

                # Get the HTML content
                html_content = await page.content()

                # Extract video ID from URL
                video_id_match = re.search(r'/video/(\d+)/', video_url)
                video_id = video_id_match.group(1) if video_id_match else "unknown"

                # Extract JSON-LD data
                json_ld_data = self.extract_json_ld_data(html_content)

                # Extract views and duration from HTML
                views, duration = self.extract_views_and_duration(html_content)

                # Get title from JSON-LD or fallback to HTML
                title = "Unknown"
                thumbnail_url = ""
                description = ""
                upload_date = None
                
                if json_ld_data:
                    title = json_ld_data.get('name', 'Unknown')
                    description = json_ld_data.get('description', '')
                    thumbnail_url = json_ld_data.get('thumbnailUrl', '')
                    
                    # Extract upload date
                    upload_date_str = json_ld_data.get('uploadDate', '')
                    if upload_date_str:
                        try:
                            upload_date = int(datetime.fromisoformat(upload_date_str.replace('Z', '+00:00')).timestamp() * 1000)
                        except:
                            upload_date = None
                    
                    # Get views from JSON-LD if HTML extraction failed
                    if views == "0" and 'interactionStatistic' in json_ld_data:
                        for stat in json_ld_data['interactionStatistic']:
                            if stat.get('interactionType') == 'http://schema.org/WatchAction':
                                try:
                                    views = int(stat.get('userInteractionCount', '0'))
                                except:
                                    pass
                    
                    # Get duration from JSON-LD if HTML extraction failed
                    if duration == "Unknown" and 'duration' in json_ld_data:
                        duration = self.convert_iso_duration_to_readable(json_ld_data['duration'])

                # Extract video source URL
                video_src = self.extract_player_data(html_content)

                # Extract artist and uploader
                artist_name, uploader_name = self.extract_artist_and_uploader(html_content)

                # Extract tags and categories
                tags = self.extract_tags_from_html(html_content)
                categories = self.extract_categories_from_html(html_content)

                # Create video data object
                video_data = {
                    "video_id": video_id,
                    "url": video_url,
                    "title": title,
                    "duration": duration,
                    "views": str(views) if isinstance(views, int) else views,
                    "tags": tags,
                    "categories": categories,
                    "description": description,
                    "thumbnail_src": thumbnail_url,
                    "video_src": video_src or "",
                    "uploaded_by": uploader_name,
                    "artists": [artist_name] if artist_name != "Anonymous" else [],
                    "upload_date": upload_date
                }

                print(f"✅ Successfully parsed: {title} - Views: {views}, Duration: {duration}")
                return video_data

            except Exception as e:
                print(f"❌ Error parsing video {video_url}: {e}")
                return None

            finally:
                await browser.close()

    async def parse_all_videos(self, limit=None):
        """Parse all extracted video URLs"""
        if not self.video_urls:
            print("⚠️ No video URLs to parse. Run extract_video_urls() first.")
            return []

        urls_to_process = self.video_urls[:limit] if limit else self.video_urls
        print(f"🚀 Starting to parse {len(urls_to_process)} videos...")

        for i, video_url in enumerate(urls_to_process):
            try:
                video_data = await self.parse_individual_video(video_url)
                if video_data:
                    self.parsed_video_data.append(video_data)

                print(f"📊 Progress: {i+1}/{len(urls_to_process)}")

            except Exception as e:
                print(f"❌ Error processing video {i+1}: {e}")

            # Add delay between requests to be respectful
            if i < len(urls_to_process) - 1:
                await asyncio.sleep(1)

        print(f"🎉 Completed parsing {len(self.parsed_video_data)} videos")
        return self.parsed_video_data

    def save_data(self, filename="parsed_videos.json"):
        """Save parsed data to JSON file"""
        if not self.parsed_video_data:
            print("⚠️ No data to save")
            return

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_video_data, f, indent=2, ensure_ascii=False)

        print(f"💾 Saved {len(self.parsed_video_data)} videos to {filename}")

# Example usage
async def main():
    parser = VideoDataParser("https://rule34video.com/")

    # Extract video URLs from main page
    await parser.extract_video_urls()

    # Parse data from first 5 videos (for testing)
    await parser.parse_all_videos(limit=5)

    # Save the parsed data
    parser.save_data("fixed_videos.json")

if __name__ == "__main__":
    asyncio.run(main())