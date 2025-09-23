
"""
FikFap Video Downloader using Playwright Browser Integration

This version uses Playwright to leverage the existing browser session,
ensuring we have the same authentication context that loaded the videos.
"""

import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin
import time

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class PlaywrightVideoDownloader:
    """Video downloader that uses Playwright browser context for authentication"""

    def __init__(self, base_download_path: str = "./downloads"):
        self.base_download_path = Path(base_download_path)
        self.base_download_path.mkdir(parents=True, exist_ok=True)

        # Playwright components
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        # Quality patterns
        self.quality_patterns = {
            '240p': ['240', '240p'],
            '360p': ['360', '360p'], 
            '480p': ['480', '480p'],
            '720p': ['720', '720p'],
            '1080p': ['1080', '1080p', '1920'],
            '1440p': ['1440', '1440p'],
            '2160p': ['2160', '2160p', '4k']
        }

        self.vp9_patterns = ['vp9', 'vp09', 'webm']

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def start(self):
        """Initialize Playwright browser"""
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is required for this downloader. Run: pip install playwright && playwright install chromium")

        print("🚀 Starting Playwright browser for video downloading...")

        self.playwright = await async_playwright().start()

        # Launch browser with same settings as your scraper
        self.browser = await self.playwright.chromium.launch(
            headless=True,  # Set to False for debugging
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--disable-gpu'
            ]
        )

        # Create context that mimics a real browser session
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
            extra_http_headers={
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
            }
        )

        # Create a page for downloading
        self.page = await self.context.new_page()

        # Navigate to FikFap to establish session
        print("🔗 Establishing session with FikFap...")
        await self.page.goto('https://fikfap.com/', wait_until='networkidle')
        await asyncio.sleep(3)  # Wait for any auth cookies to be set

    async def close(self):
        """Close Playwright browser"""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def download_and_organize_post(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """Download and organize a single post using browser context"""
        try:
            post_id = str(post_data.get('postId', 'unknown'))
            print(f"\n🎯 Processing post {post_id}: {post_data.get('label', '')[:50]}...")

            # Create post directory structure
            post_dir = self.base_download_path / post_id
            post_dir.mkdir(parents=True, exist_ok=True)

            # Create m3u8 subdirectory
            m3u8_dir = post_dir / "m3u8"
            m3u8_dir.mkdir(parents=True, exist_ok=True)

            # Save metadata
            await self._save_metadata(post_data, post_dir / "data.json")

            # Get main playlist URL
            video_stream_url = post_data.get('videoStreamUrl')
            if not video_stream_url:
                return {"success": False, "error": "No video stream URL found", "post_id": post_id}

            # Check if video is ready
            if not post_data.get('isBunnyVideoReady', False):
                print(f"⚠️ Video not ready for post {post_id}, skipping...")
                return {"success": False, "error": "Video not ready (isBunnyVideoReady=false)", "post_id": post_id}

            # Download and analyze main playlist using browser
            playlist_result = await self._download_main_playlist_with_browser(video_stream_url, m3u8_dir)
            if not playlist_result["success"]:
                return {"success": False, "error": playlist_result["error"], "post_id": post_id}

            # Download all quality variants
            qualities_result = await self._download_all_qualities_with_browser(
                playlist_result["qualities"], 
                m3u8_dir, 
                video_stream_url
            )

            result = {
                "success": True,
                "post_id": post_id,
                "post_dir": str(post_dir),
                "main_playlist": playlist_result.get("main_playlist_path"),
                "qualities_downloaded": qualities_result["successful"],
                "qualities_failed": qualities_result["failed"],
                "total_files": qualities_result["total_files"]
            }

            print(f"✅ Post {post_id} completed: {len(qualities_result['successful'])} qualities downloaded")
            return result

        except Exception as e:
            print(f"❌ Error processing post {post_id}: {e}")
            return {"success": False, "error": str(e), "post_id": post_id}

    async def _save_metadata(self, post_data: Dict[str, Any], metadata_path: Path):
        """Save post metadata to data.json"""
        try:
            # Clean up metadata
            clean_metadata = self._clean_metadata_for_json(post_data)

            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(clean_metadata, f, indent=2, ensure_ascii=False, default=str)

            print(f"💾 Metadata saved: {metadata_path}")

        except Exception as e:
            print(f"❌ Failed to save metadata: {e}")

    def _clean_metadata_for_json(self, data: Any) -> Any:
        """Recursively clean data for JSON serialization"""
        if isinstance(data, dict):
            return {k: self._clean_metadata_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_metadata_for_json(item) for item in data]
        elif isinstance(data, (str, int, float, bool)) or data is None:
            return data
        else:
            return str(data)

    async def _download_main_playlist_with_browser(self, video_stream_url: str, m3u8_dir: Path) -> Dict[str, Any]:
        """Download main playlist using browser context"""
        try:
            print(f"📡 Downloading main playlist via browser...")
            print(f"🔗 URL: {video_stream_url[:80]}...")

            # Use Playwright's request context which has proper authentication
            response = await self.page.request.get(video_stream_url)

            print(f"📊 Browser response status: {response.status}")

            if response.status != 200:
                return {"success": False, "error": f"HTTP {response.status} fetching main playlist"}

            playlist_content = await response.text()
            print(f"✅ Successfully downloaded playlist via browser ({len(playlist_content)} characters)")

            # Save main playlist
            main_playlist_path = m3u8_dir / "playlist.m3u8"
            with open(main_playlist_path, 'w', encoding='utf-8') as f:
                f.write(playlist_content)

            print(f"💾 Main playlist saved: {main_playlist_path}")

            # Parse playlist to extract quality variants
            qualities = self._parse_master_playlist(playlist_content, video_stream_url)

            return {
                "success": True,
                "main_playlist_path": str(main_playlist_path),
                "qualities": qualities
            }

        except Exception as e:
            print(f"❌ Failed to download main playlist via browser: {e}")
            return {"success": False, "error": str(e)}

    def _parse_master_playlist(self, content: str, base_url: str) -> List[Dict[str, Any]]:
        """Parse master playlist to extract quality information"""
        qualities = []
        lines = content.strip().split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            if line.startswith('#EXT-X-STREAM-INF'):
                stream_info = self._parse_stream_info(line)

                if i + 1 < len(lines):
                    url_line = lines[i + 1].strip()
                    if url_line and not url_line.startswith('#'):
                        absolute_url = urljoin(base_url, url_line)
                        quality_info = self._determine_quality_info(stream_info, url_line, absolute_url)
                        quality_info['url'] = absolute_url
                        qualities.append(quality_info)
                        print(f"🎬 Found quality: {quality_info['resolution']} ({quality_info['codec']})")

            i += 1

        print(f"📊 Total qualities found: {len(qualities)}")
        return qualities

    def _parse_stream_info(self, stream_line: str) -> Dict[str, str]:
        """Parse #EXT-X-STREAM-INF line"""
        info = {}
        stream_line = stream_line.replace('#EXT-X-STREAM-INF:', '')

        pairs = []
        current_pair = ""
        in_quotes = False

        for char in stream_line:
            if char == '"':
                in_quotes = not in_quotes
                current_pair += char
            elif char == ',' and not in_quotes:
                pairs.append(current_pair.strip())
                current_pair = ""
            else:
                current_pair += char

        if current_pair.strip():
            pairs.append(current_pair.strip())

        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=', 1)
                info[key.strip()] = value.strip().strip('"')

        return info

    def _determine_quality_info(self, stream_info: Dict[str, str], url_path: str, full_url: str) -> Dict[str, Any]:
        """Determine resolution and codec information"""
        resolution = "unknown"
        codec = "h264"
        is_vp9 = False

        # Check for VP9
        combined_text = f"{stream_info} {url_path} {full_url}".lower()
        for vp9_pattern in self.vp9_patterns:
            if vp9_pattern in combined_text:
                codec = "vp9"
                is_vp9 = True
                break

        # Extract resolution
        if 'RESOLUTION' in stream_info:
            resolution_str = stream_info['RESOLUTION']
            if 'x' in resolution_str:
                width, height = resolution_str.split('x')
                height = int(height.strip())

                if height <= 240:
                    resolution = "240p"
                elif height <= 360:
                    resolution = "360p"
                elif height <= 480:
                    resolution = "480p"
                elif height <= 720:
                    resolution = "720p"
                elif height <= 1080:
                    resolution = "1080p"
                elif height <= 1440:
                    resolution = "1440p"
                else:
                    resolution = "2160p"

        if resolution == "unknown":
            resolution = self._extract_resolution_from_url(url_path) or "720p"

        return {
            'resolution': resolution,
            'codec': codec,
            'is_vp9': is_vp9,
            'stream_info': stream_info
        }

    def _extract_resolution_from_url(self, url: str) -> Optional[str]:
        """Extract resolution from URL patterns"""
        url_lower = url.lower()
        for resolution, patterns in self.quality_patterns.items():
            for pattern in patterns:
                if pattern in url_lower:
                    return resolution
        return None

    async def _download_all_qualities_with_browser(
        self, 
        qualities: List[Dict[str, Any]], 
        m3u8_dir: Path,
        base_url: str
    ) -> Dict[str, Any]:
        """Download all quality variants using browser"""
        successful = []
        failed = []
        total_files = 0

        for quality in qualities:
            try:
                print(f"\n🎬 Downloading quality: {quality['resolution']} ({quality['codec']})")

                # Determine directory name
                if quality['is_vp9']:
                    quality_dir_name = f"vp9_{quality['resolution']}"
                else:
                    quality_dir_name = quality['resolution']

                # Create quality directory
                quality_dir = m3u8_dir / quality_dir_name
                quality_dir.mkdir(parents=True, exist_ok=True)

                # Download the quality variant
                result = await self._download_quality_variant_with_browser(quality, quality_dir)

                if result["success"]:
                    successful.append({
                        "resolution": quality['resolution'],
                        "codec": quality['codec'],
                        "directory": quality_dir_name,
                        "files": result["files"]
                    })
                    total_files += result["file_count"]
                    print(f"✅ {quality['resolution']} completed: {result['file_count']} files")
                else:
                    failed.append({
                        "resolution": quality['resolution'],
                        "error": result["error"]
                    })
                    print(f"❌ {quality['resolution']} failed: {result['error']}")

            except Exception as e:
                failed.append({
                    "resolution": quality.get('resolution', 'unknown'),
                    "error": str(e)
                })
                print(f"❌ {quality.get('resolution', 'unknown')} failed: {e}")

        return {
            "successful": successful,
            "failed": failed,
            "total_files": total_files
        }

    async def _download_quality_variant_with_browser(
        self, 
        quality: Dict[str, Any], 
        quality_dir: Path
    ) -> Dict[str, Any]:
        """Download a specific quality variant using browser"""
        try:
            playlist_url = quality['url']

            # Download the quality playlist using browser
            response = await self.page.request.get(playlist_url)
            if response.status != 200:
                return {"success": False, "error": f"HTTP {response.status}"}

            playlist_content = await response.text()

            # Save quality playlist
            playlist_path = quality_dir / "video.m3u8"
            with open(playlist_path, 'w', encoding='utf-8') as f:
                f.write(playlist_content)

            # Parse segments
            segments = self._parse_playlist_segments(playlist_content, playlist_url)

            if not segments:
                return {"success": False, "error": "No segments found in playlist"}

            print(f"📊 Found {len(segments)} segments to download")

            # Download segments using browser
            downloaded_files = ["video.m3u8"]

            for i, segment_url in enumerate(segments, 1):
                try:
                    segment_filename = f"video{i}.m4s"
                    segment_path = quality_dir / segment_filename

                    # Download segment via browser
                    segment_response = await self.page.request.get(segment_url)
                    if segment_response.status == 200:
                        segment_content = await segment_response.body()

                        with open(segment_path, 'wb') as f:
                            f.write(segment_content)

                        downloaded_files.append(segment_filename)

                        if i % 10 == 0:
                            print(f"⏳ Downloaded {i}/{len(segments)} segments...")
                    else:
                        print(f"⚠️ Failed to download segment {i}: HTTP {segment_response.status}")

                except Exception as e:
                    print(f"⚠️ Error downloading segment {i}: {e}")
                    continue

            return {
                "success": True,
                "file_count": len(downloaded_files),
                "files": downloaded_files
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _parse_playlist_segments(self, playlist_content: str, base_url: str) -> List[str]:
        """Parse playlist to extract segment URLs"""
        segments = []
        lines = playlist_content.strip().split('\n')

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            segment_url = urljoin(base_url, line)
            segments.append(segment_url)

        return segments

    async def process_all_posts(self, posts_file: str = "all_raw_posts.json") -> Dict[str, Any]:
        """Process all posts from the JSON file using browser"""
        try:
            with open(posts_file, 'r', encoding='utf-8') as f:
                posts = json.load(f)

            print(f"🚀 Starting processing of {len(posts)} posts using browser...")

            results = {
                "total_posts": len(posts),
                "successful": [],
                "failed": [],
                "summary": {}
            }

            for i, post in enumerate(posts, 1):
                print(f"\n{'='*50}")
                print(f"Processing post {i}/{len(posts)}")
                print(f"{'='*50}")

                result = await self.download_and_organize_post(post)

                if result["success"]:
                    results["successful"].append(result)
                else:
                    results["failed"].append(result)

                await asyncio.sleep(3)  # Longer delay for browser-based downloading

            # Generate summary
            results["summary"] = {
                "successful_count": len(results["successful"]),
                "failed_count": len(results["failed"]),
                "success_rate": f"{len(results['successful'])/len(posts)*100:.1f}%",
                "total_qualities": sum(len(r.get("qualities_downloaded", [])) for r in results["successful"]),
                "total_files": sum(r.get("total_files", 0) for r in results["successful"])
            }

            # Save results
            results_file = f"download_results_browser_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=str)

            print(f"\n🎉 Processing completed!")
            print(f"📊 Results: {results['summary']['successful_count']}/{results['summary']['total_posts']} successful ({results['summary']['success_rate']})")
            print(f"📁 Total files downloaded: {results['summary']['total_files']}")
            print(f"💾 Results saved to: {results_file}")

            return results

        except Exception as e:
            print(f"❌ Error processing posts: {e}")
            return {"error": str(e)}


# Usage example:
async def main():
    """Main function to run the browser-based downloader"""
    async with PlaywrightVideoDownloader("./downloads") as downloader:
        results = await downloader.process_all_posts()
        return results

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
