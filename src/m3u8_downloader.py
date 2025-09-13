"""
Complete M3U8 Downloader System for FikFap
Uses exact headers with proper compression handling
Follows original.md specifications exactly
"""

import asyncio
import json
import os
import re
import ssl
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse
import aiofiles
import aiohttp
from aiohttp import TCPConnector
from crawl4ai import AsyncWebCrawler


class FolderManager:
    """Handles folder structure creation and management"""

    def __init__(self, base_download_folder: str):
        self.base_folder = Path(base_download_folder)

    def create_post_folder(self, post_id: str) -> Path:
        """Create folder structure for a post"""
        post_folder = self.base_folder / str(post_id)
        m3u8_folder = post_folder / "m3u8"

        # Create folders
        post_folder.mkdir(parents=True, exist_ok=True)
        m3u8_folder.mkdir(parents=True, exist_ok=True)

        return post_folder

    def create_quality_folder(self, post_folder: Path, quality: str) -> Path:
        """Create quality-specific folder (e.g., 720p, vp9_1080p)"""
        quality_folder = post_folder / "m3u8" / quality
        quality_folder.mkdir(parents=True, exist_ok=True)
        return quality_folder

    async def save_data_json(self, post_folder: Path, post_data: Dict):
        """Save post metadata as data.json"""
        data_file = post_folder / "data.json"
        async with aiofiles.open(data_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(post_data, indent=2, ensure_ascii=False, default=str))
        print(f"💾 Saved data.json for post {post_data.get('postId')}")


class PlaylistParser:
    """Parses M3U8 playlists and extracts stream information"""

    def parse_master_playlist(self, content: str) -> List[Dict]:
        """Parse master playlist and extract stream variants"""
        streams = []
        lines = content.strip().split('\n')

        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Look for stream info lines
            if line.startswith('#EXT-X-STREAM-INF:'):
                stream_info = self._parse_stream_inf(line)

                # Next line should be the playlist URL
                if i + 1 < len(lines):
                    playlist_url = lines[i + 1].strip()
                    if playlist_url and not playlist_url.startswith('#'):
                        stream_info['playlist_url'] = playlist_url
                        streams.append(stream_info)
                        i += 2
                        continue
            i += 1

        return streams

    def _parse_stream_inf(self, line: str) -> Dict:
        """Parse EXT-X-STREAM-INF line to extract stream properties"""
        info = {}

        # Extract resolution
        resolution_match = re.search(r'RESOLUTION=([0-9]+x[0-9]+)', line)
        if resolution_match:
            info['resolution'] = resolution_match.group(1)
            width, height = resolution_match.group(1).split('x')
            info['width'] = int(width)
            info['height'] = int(height)

        # Extract codecs
        codecs_match = re.search(r'CODECS="([^"]+)"', line)
        if codecs_match:
            info['codecs'] = codecs_match.group(1)

        # Extract bandwidth
        bandwidth_match = re.search(r'BANDWIDTH=([0-9]+)', line)
        if bandwidth_match:
            info['bandwidth'] = int(bandwidth_match.group(1))

        # Extract frame rate
        frame_rate_match = re.search(r'FRAME-RATE=([0-9.]+)', line)
        if frame_rate_match:
            info['frame_rate'] = float(frame_rate_match.group(1))

        return info

    def parse_quality_playlist(self, content: str) -> List[str]:
        """Parse quality-specific playlist and extract segment URLs"""
        segments = []
        lines = content.strip().split('\n')

        for line in lines:
            line = line.strip()
            # Skip comments and empty lines
            if line and not line.startswith('#'):
                segments.append(line)

        return segments

    def determine_quality_folder_name(self, stream: Dict) -> str:
        """Determine folder name for stream quality"""
        # Check if VP9
        codecs = stream.get('codecs', '').lower()
        height = stream.get('height', 0)

        if 'vp09' in codecs or 'vp9' in stream.get('playlist_url', '').lower():
            return f"vp9_{height}p"
        else:
            return f"{height}p"


class CodecFilter:
    """Filters video streams based on codec preferences"""

    def __init__(self, exclude_vp9: bool = True):
        self.exclude_vp9 = exclude_vp9

    def filter_streams(self, streams: List[Dict]) -> List[Dict]:
        """Filter streams based on codec preferences"""
        filtered = []

        for stream in streams:
            if self.exclude_vp9 and self._is_vp9_stream(stream):
                print(f"🚫 Skipping VP9 stream: {stream.get('resolution', 'unknown')}")
                continue
            filtered.append(stream)

        return filtered

    def _is_vp9_stream(self, stream: Dict) -> bool:
        """Check if stream uses VP9 codec"""
        codecs = stream.get('codecs', '').lower()
        playlist_url = stream.get('playlist_url', '').lower()

        return ('vp09' in codecs or 
                'vp9_' in playlist_url or 
                'vp9/' in playlist_url)


class ProgressTracker:
    """Tracks download progress for videos"""

    def __init__(self):
        self.downloads = {}

    def start_post(self, post_id: str, total_streams: int):
        """Start tracking a post download"""
        self.downloads[post_id] = {
            'total_streams': total_streams,
            'completed_streams': 0,
            'current_stream': None,
            'segments_total': 0,
            'segments_completed': 0
        }
        print(f"📥 Starting download for post {post_id} ({total_streams} streams)")

    def start_stream(self, post_id: str, quality: str, total_segments: int):
        """Start tracking a stream download"""
        if post_id in self.downloads:
            self.downloads[post_id].update({
                'current_stream': quality,
                'segments_total': total_segments,
                'segments_completed': 0
            })
            print(f"🎥 Downloading {quality} ({total_segments} segments)")

    def complete_segment(self, post_id: str):
        """Mark a segment as completed"""
        if post_id in self.downloads:
            self.downloads[post_id]['segments_completed'] += 1
            progress = self.downloads[post_id]
            pct = (progress['segments_completed'] / progress['segments_total']) * 100
            print(f"📦 Segment {progress['segments_completed']}/{progress['segments_total']} ({pct:.1f}%)")

    def complete_stream(self, post_id: str):
        """Mark a stream as completed"""
        if post_id in self.downloads:
            self.downloads[post_id]['completed_streams'] += 1
            progress = self.downloads[post_id]
            print(f"✅ Completed {progress['current_stream']} ({progress['completed_streams']}/{progress['total_streams']} streams)")

    def complete_post(self, post_id: str):
        """Mark post download as completed"""
        if post_id in self.downloads:
            print(f"🎉 Completed all downloads for post {post_id}")
            del self.downloads[post_id]


class VideoDownloader:
    """Downloads video files with exact headers and proper compression handling"""

    def __init__(self, max_retries: int = 3, timeout: int = 30):
        self.max_retries = max_retries
        self.timeout = timeout

        # Exact headers from the working browser request
        self.headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://fikfap.com',
            'priority': 'u=1, i',
            'referer': 'https://fikfap.com/',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }

        # Simplified headers without compression (fallback)
        self.headers_no_compression = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://fikfap.com',
            'referer': 'https://fikfap.com/',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
        }

    async def download_playlist_content(self, url: str, session_id: str = None) -> Optional[str]:
        """Download playlist content with compression handling"""
        print(f"🌐 Downloading playlist: {url[:60]}...")

        # Try aiohttp with compression
        content = await self._download_with_compression(url)
        if content and self._is_valid_m3u8(content):
            print(f"✅ aiohttp (compressed): Got {len(content)} chars of M3U8 content")
            return content

        # Try aiohttp without compression
        content = await self._download_without_compression(url)
        if content and self._is_valid_m3u8(content):
            print(f"✅ aiohttp (uncompressed): Got {len(content)} chars of M3U8 content")
            return content

        # Fallback to crawl4ai
        print("🔄 Trying crawl4ai fallback...")
        content = await self._download_with_crawl4ai_exact_headers(url, session_id)
        if content and self._is_valid_m3u8(content):
            print(f"✅ crawl4ai: Got {len(content)} chars of M3U8 content")
            return content

        print("❌ Failed to get valid M3U8 content from all methods")
        return None

    async def _download_with_compression(self, url: str) -> Optional[str]:
        """Download with compression enabled (original headers)"""
        for attempt in range(self.max_retries):
            try:
                connector = TCPConnector(
                    ssl=False,
                    limit=100,
                    limit_per_host=30,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True
                )

                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers=self.headers
                ) as session:
                    async with session.get(url) as response:
                        print(f"📡 Request status: {response.status} (compressed)")
                        print(f"📥 Content-Encoding: {response.headers.get('Content-Encoding', 'none')}")

                        if response.status == 200:
                            try:
                                # aiohttp automatically decompresses
                                content = await response.text()
                                print(f"📥 Success! Got {len(content)} characters (auto-decompressed)")
                                return content
                            except UnicodeDecodeError as e:
                                print(f"⚠️ UTF-8 decode failed: {e}")
                                # Try reading as bytes and manual decode
                                try:
                                    raw_bytes = await response.read()
                                    print(f"📥 Got {len(raw_bytes)} raw bytes, first 10: {raw_bytes[:10]}")

                                    # Try different encodings
                                    for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                                        try:
                                            content = raw_bytes.decode(encoding)
                                            print(f"✅ Success with {encoding} encoding!")
                                            return content
                                        except UnicodeDecodeError:
                                            continue
                                    print("❌ All encoding attempts failed")
                                except Exception as inner_e:
                                    print(f"⚠️ Raw bytes read failed: {inner_e}")
                        else:
                            print(f"⚠️ HTTP {response.status}: {response.reason}")

            except Exception as e:
                print(f"⚠️ Compressed attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)

        return None

    async def _download_without_compression(self, url: str) -> Optional[str]:
        """Download without compression (simplified headers)"""
        print("🔄 Trying without compression...")

        for attempt in range(self.max_retries):
            try:
                connector = TCPConnector(
                    ssl=False,
                    limit=100,
                    limit_per_host=30,
                    keepalive_timeout=30,
                    enable_cleanup_closed=True
                )

                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers=self.headers_no_compression  # No compression headers
                ) as session:
                    async with session.get(url) as response:
                        print(f"📡 Request status: {response.status} (uncompressed)")

                        if response.status == 200:
                            try:
                                content = await response.text()
                                print(f"📥 Success! Got {len(content)} characters (uncompressed)")
                                return content
                            except Exception as e:
                                print(f"⚠️ Text decode failed: {e}")
                        else:
                            print(f"⚠️ HTTP {response.status}: {response.reason}")

            except Exception as e:
                print(f"⚠️ Uncompressed attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)

        return None

    async def _download_with_crawl4ai_exact_headers(self, url: str, session_id: str = None) -> Optional[str]:
        """Fallback using crawl4ai with exact headers"""
        try:
            async with AsyncWebCrawler(
                verbose=False,
                browser_type="chromium",
                headless=True,
                page_timeout=self.timeout * 1000
            ) as crawler:
                result = await crawler.arun(
                    url=url,
                    only_text=True,
                    session_id=session_id,
                    extra_headers=self.headers_no_compression  # Use simpler headers for crawl4ai
                )

                if result.success:
                    # Try different content attributes
                    for attr in ['text', 'cleaned_html', 'html']:
                        if hasattr(result, attr):
                            content = getattr(result, attr)
                            if content and not content.startswith('<html'):
                                print(f"📥 crawl4ai: Got {len(content)} characters from {attr}")
                                return content
                    print("⚠️ crawl4ai: All content appears to be HTML error pages")

        except Exception as e:
            print(f"⚠️ crawl4ai failed: {e}")

        return None

    def _is_valid_m3u8(self, content: str) -> bool:
        """Check if content looks like valid M3U8"""
        if not content:
            return False

        # Check for M3U8 markers
        m3u8_markers = ['#EXTM3U', '#EXT-X-STREAM-INF', '#EXT-X-VERSION', 'EXT-X-']
        has_markers = any(marker in content for marker in m3u8_markers)

        if has_markers:
            print("✅ Content contains M3U8 markers")
        else:
            print("⚠️ Content missing M3U8 markers")
            print(f"📝 Content preview: {repr(content[:200])}")

        return has_markers

    async def download_file(self, url: str, file_path: Path, session_id: str = None) -> bool:
        """Download any file with proper encoding handling"""
        for attempt in range(self.max_retries):
            try:
                connector = TCPConnector(ssl=False, limit=100, limit_per_host=30)
                timeout = aiohttp.ClientTimeout(total=self.timeout)

                # Use simpler headers for file downloads
                async with aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    headers=self.headers_no_compression
                ) as session:
                    async with session.get(url) as response:
                        if response.status == 200:
                            if file_path.suffix in ['.m3u8', '.m3u']:
                                # Text file (M3U8 playlist)
                                content = await response.text()
                                async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                                    await f.write(content)
                            else:
                                # Binary file (.m4s video segment)  
                                content = await response.read()
                                async with aiofiles.open(file_path, 'wb') as f:
                                    await f.write(content)
                            return True
                        else:
                            print(f"⚠️ HTTP {response.status} for {url}")

            except Exception as e:
                print(f"⚠️ File download attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)

        return False


class M3U8Downloader:
    """Main M3U8 downloader coordinator"""

    def __init__(self, config_file: str = 'config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)

        self.download_folder = self.config['download_folder']
        self.m3u8_settings = self.config.get('m3u8_settings', {})

        # Initialize components
        self.folder_manager = FolderManager(self.download_folder)
        self.playlist_parser = PlaylistParser()
        self.codec_filter = CodecFilter(exclude_vp9=self.m3u8_settings.get('exclude_vp9', True))
        self.video_downloader = VideoDownloader(
            max_retries=self.m3u8_settings.get('max_retries', 3),
            timeout=self.m3u8_settings.get('timeout', 30)
        )
        self.progress_tracker = ProgressTracker()

    async def download_post(self, post_data: Dict) -> bool:
        """Download all video qualities for a single post"""
        post_id = str(post_data['postId'])
        video_stream_url = post_data['videoStreamUrl']

        print(f"\n🚀 Starting download for Post {post_id}")
        print(f"📺 Video URL: {video_stream_url}")

        try:
            # Create post folder
            post_folder = self.folder_manager.create_post_folder(post_id)

            # Save metadata
            await self.folder_manager.save_data_json(post_folder, post_data)

            # Download and parse master playlist
            master_content = await self.video_downloader.download_playlist_content(video_stream_url)
            if not master_content:
                print(f"❌ Failed to download master playlist for post {post_id}")
                return False

            print(f"📄 Master playlist content preview:")
            print(f"   {repr(master_content[:200])}...")

            # Save master playlist
            master_playlist_path = post_folder / "m3u8" / "playlist.m3u8"
            async with aiofiles.open(master_playlist_path, 'w', encoding='utf-8') as f:
                await f.write(master_content)

            # Parse streams
            streams = self.playlist_parser.parse_master_playlist(master_content)
            if not streams:
                print(f"⚠️ No streams found in master playlist for post {post_id}")
                print(f"🔍 Content analysis:")
                print(f"   Length: {len(master_content)} chars")
                print(f"   Lines: {len(master_content.split('\n'))}")
                print(f"   Contains #EXTM3U: {'#EXTM3U' in master_content}")
                print(f"   Contains EXT-X-STREAM-INF: {'EXT-X-STREAM-INF' in master_content}")
                return False

            print(f"🎯 Found {len(streams)} streams")
            for i, stream in enumerate(streams, 1):
                print(f"   [{i}] {stream.get('resolution', 'unknown')} - {stream.get('codecs', 'unknown')}")

            # Filter streams
            filtered_streams = self.codec_filter.filter_streams(streams)
            print(f"🔽 After filtering: {len(filtered_streams)} streams")

            if not filtered_streams:
                print(f"⚠️ No streams left after filtering for post {post_id}")
                return False

            # Start progress tracking
            self.progress_tracker.start_post(post_id, len(filtered_streams))

            # Download each stream
            success_count = 0
            for stream in filtered_streams:
                if await self._download_stream(post_id, post_folder, stream, video_stream_url):
                    success_count += 1
                    self.progress_tracker.complete_stream(post_id)

            self.progress_tracker.complete_post(post_id)

            if success_count > 0:
                print(f"✅ Successfully downloaded {success_count}/{len(filtered_streams)} streams for post {post_id}")
                return True
            else:
                print(f"❌ Failed to download any streams for post {post_id}")
                return False

        except Exception as e:
            print(f"❌ Error downloading post {post_id}: {e}")
            return False

    async def _download_stream(self, post_id: str, post_folder: Path, stream: Dict, base_url: str) -> bool:
        """Download a single stream (quality)"""
        try:
            # Determine quality folder name
            quality = self.playlist_parser.determine_quality_folder_name(stream)
            quality_folder = self.folder_manager.create_quality_folder(post_folder, quality)

            # Build full playlist URL
            playlist_url = urljoin(base_url, stream['playlist_url'])

            print(f"📥 Downloading {quality} stream from {playlist_url}")

            # Download quality playlist
            quality_content = await self.video_downloader.download_playlist_content(playlist_url)
            if not quality_content:
                print(f"❌ Failed to download {quality} playlist")
                return False

            # Save quality playlist
            quality_playlist_path = quality_folder / "video.m3u8"
            async with aiofiles.open(quality_playlist_path, 'w', encoding='utf-8') as f:
                await f.write(quality_content)

            # Parse segments
            segments = self.playlist_parser.parse_quality_playlist(quality_content)
            if not segments:
                print(f"⚠️ No segments found in {quality} playlist")
                return False

            print(f"🎬 Found {len(segments)} segments for {quality}")
            self.progress_tracker.start_stream(post_id, quality, len(segments))

            # Download segments
            success_count = 0
            for i, segment in enumerate(segments):
                segment_url = urljoin(playlist_url, segment)
                segment_filename = f"video{i+1}.m4s"
                segment_path = quality_folder / segment_filename

                if await self.video_downloader.download_file(segment_url, segment_path):
                    success_count += 1
                    self.progress_tracker.complete_segment(post_id)
                else:
                    print(f"⚠️ Failed to download segment {segment_filename}")

            if success_count == len(segments):
                print(f"✅ Successfully downloaded all {len(segments)} segments for {quality}")
                return True
            else:
                print(f"⚠️ Downloaded {success_count}/{len(segments)} segments for {quality}")
                return success_count > len(segments) * 0.8  # Consider success if >80% downloaded

        except Exception as e:
            print(f"❌ Error downloading {stream.get('resolution', 'unknown')} stream: {e}")
            return False

    async def download_all_posts(self, posts_file: str = 'integrated_extracted_posts.json') -> Dict:
        """Download all posts from extracted posts file"""
        try:
            # Load posts
            with open(posts_file, 'r') as f:
                data = json.load(f)

            posts = data['posts']
            total_posts = len(posts)

            print(f"🎯 Starting bulk download for {total_posts} posts")
            print(f"📂 Download folder: {self.download_folder}")
            print(f"🛡️ SSL verification: DISABLED")
            print(f"🔧 Compression handling: ENABLED")
            print(f"🎯 Headers: Referer + Origin from fikfap.com")

            results = {
                'total_posts': total_posts,
                'successful_downloads': 0,
                'failed_downloads': 0,
                'skipped_posts': 0
            }

            # Download each post
            for i, post in enumerate(posts, 1):
                post_id = post['postId']
                print(f"\n[{i}/{total_posts}] Processing Post {post_id}")

                # Check if video is ready
                if not post.get('isBunnyVideoReady', False):
                    print(f"⏭️ Skipping post {post_id} - video not ready")
                    results['skipped_posts'] += 1
                    continue

                # Check if videoStreamUrl exists
                if not post.get('videoStreamUrl'):
                    print(f"⏭️ Skipping post {post_id} - no video stream URL")
                    results['skipped_posts'] += 1
                    continue

                # Download post
                if await self.download_post(post):
                    results['successful_downloads'] += 1
                else:
                    results['failed_downloads'] += 1

                # Small delay between posts
                await asyncio.sleep(1)

            # Print final results
            self._print_results(results)
            return results

        except Exception as e:
            print(f"❌ Error in bulk download: {e}")
            return {'error': str(e)}

    def _print_results(self, results: Dict):
        """Print download results summary"""
        print("\n" + "="*80)
        print("📊 DOWNLOAD RESULTS SUMMARY")
        print("="*80)
        print(f"Total Posts: {results['total_posts']}")
        print(f"✅ Successful Downloads: {results['successful_downloads']}")
        print(f"❌ Failed Downloads: {results['failed_downloads']}")
        print(f"⏭️ Skipped Posts: {results['skipped_posts']}")

        if results['total_posts'] > 0:
            success_rate = (results['successful_downloads'] / results['total_posts']) * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")

        print(f"📂 Files saved to: {self.download_folder}/")
        print("="*80)


# Main execution function
async def main():
    """Main function to run M3U8 downloader"""
    print("🚀 STARTING M3U8 DOWNLOADER (COMPRESSION FIX)")
    print("="*80)
    print("🛡️ SSL Certificate verification: DISABLED")
    print("🗜️ Compression handling: THREE-TIER APPROACH")
    print("🔧 Headers: EXACT from working browser request")
    print("🎯 Referer: https://fikfap.com/")
    print("🎯 Origin: https://fikfap.com")

    # Initialize downloader
    downloader = M3U8Downloader()

    # Download all posts
    results = await downloader.download_all_posts()

    if 'error' not in results:
        print("\n🎉 M3U8 DOWNLOAD COMPLETED!")
        return results
    else:
        print(f"\n❌ M3U8 DOWNLOAD FAILED: {results['error']}")
        return None

if __name__ == "__main__":
    asyncio.run(main())