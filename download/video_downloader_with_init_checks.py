"""

FIXED FikFap Video Downloader and Organizer - Progress Tracking Only

This module downloads videos from M3U8 playlists and maintains ONLY a progress.json file.

Removed all unnecessary JSON file creation and fixed 'total_posts' error.

"""

import asyncio

import aiohttp

import json

import re

import os

from pathlib import Path

from datetime import datetime

from typing import Dict, List, Optional, Any, Tuple

from urllib.parse import urlparse, urljoin, parse_qs, unquote

import time

import uuid

# Import the progress tracker

try:

    from utils.progress import ProgressTracker

except ImportError:

    # Fallback if utils module not found

    import json

    from pathlib import Path

    class ProgressTracker:

        def __init__(self, progress_file: str = "progress.json"):

            self.progress_file = Path(progress_file)

            self.ensure_progress_file()

        def ensure_progress_file(self):

            if not self.progress_file.exists():

                initial_data = {"downloaded_video_ids": [], "total_downloaded": 0}

                with open(self.progress_file, 'w', encoding='utf-8') as f:

                    json.dump(initial_data, f, indent=2)

        def load_progress(self):

            try:

                with open(self.progress_file, 'r', encoding='utf-8') as f:

                    data = json.load(f)

                    if "downloaded_video_ids" not in data:

                        data["downloaded_video_ids"] = []

                    if "total_downloaded" not in data:

                        data["total_downloaded"] = len(data.get("downloaded_video_ids", []))

                    return data

            except Exception as e:

                print(f"Error loading progress: {e}")

                return {"downloaded_video_ids": [], "total_downloaded": 0}

        def save_progress(self, data):

            try:

                with open(self.progress_file, 'w', encoding='utf-8') as f:

                    json.dump(data, f, indent=2, ensure_ascii=False)

            except Exception as e:

                print(f"Error saving progress: {e}")

        def add_downloaded_video(self, video_id: str):

            progress = self.load_progress()

            if video_id not in progress["downloaded_video_ids"]:

                progress["downloaded_video_ids"].append(video_id)

                progress["total_downloaded"] = len(progress["downloaded_video_ids"])

                self.save_progress(progress)

        def is_video_downloaded(self, video_id: str) -> bool:

            progress = self.load_progress()

            return video_id in progress["downloaded_video_ids"]

        def get_stats(self):

            progress = self.load_progress()

            return {"total_downloaded": progress["total_downloaded"], "downloaded_count": len(progress["downloaded_video_ids"])}

class VideoOrganizer:

    """Complete video downloader and organizer for FikFap posts with ONLY progress.json tracking"""

    def __init__(self, base_download_path: str = "./downloads"):

        self.base_download_path = Path(base_download_path)

        self.base_download_path.mkdir(parents=True, exist_ok=True)

        self.session: Optional[aiohttp.ClientSession] = None

        # Quality mappings for different resolutions

        self.quality_patterns = {

            "240p": ["240", "240p"],

            "360p": ["360", "360p"],

            "480p": ["480", "480p"],

            "720p": ["720", "720p"],

            "1080p": ["1080", "1080p", "1920"],

            "1440p": ["1440", "1440p"],

            "2160p": ["2160", "2160p", "4k"]

        }

        # VP9 codec detection patterns

        self.vp9_patterns = ["vp9", "vp09", "webm"]

        # Initialize progress tracker

        self.progress_tracker = ProgressTracker()

    async def __aenter__(self):

        """Async context manager entry"""

        await self.start()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):

        """Async context manager exit"""

        await self.close()

    async def start(self):

        """Initialize HTTP session with proper CDN headers"""

        if not self.session:

            timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout

            connector = aiohttp.TCPConnector(limit=10, verify_ssl=False)

            # Enhanced headers that mimic browser behavior for CDN authentication

            headers = {

                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

                "Accept": "*/*",

                "Accept-Language": "en-US,en;q=0.9",

                "Accept-Encoding": "gzip, deflate, br",

                "Connection": "keep-alive",

                "Sec-Fetch-Dest": "empty",

                "Sec-Fetch-Mode": "cors",

                "Sec-Fetch-Site": "cross-site",

                "Cache-Control": "no-cache",

                "Pragma": "no-cache",

                # Key: Add referrer for CDN authentication

                "Referer": "https://fikfap.com",

                "Origin": "https://fikfap.com"

            }

            self.session = aiohttp.ClientSession(

                connector=connector,

                timeout=timeout,

                headers=headers

            )

    async def close(self):

        """Close HTTP session"""

        if self.session:

            await self.session.close()

            self.session = None

    def parse_videostream_url_fixed(self, video_stream_url: str) -> Dict[str, str]:

        """

        FIXED: Parse the videoStreamUrl to extract tokens

        URL format: https://vz-{host_id}.b-cdn.net/bcdn_token={token}&token_countries={country}&token_path={path}&expires={time}/{video_uuid}/playlist.m3u8

        """

        try:

            parsed_url = urlparse(video_stream_url)

            # Extract host UUID from hostname (e.g., "vz-5d293dac-178.b-cdn.net")

            host_uuid_match = re.match(r'^vz-([^.]+)', parsed_url.hostname)

            if not host_uuid_match:

                raise ValueError(f"Could not extract host UUID from hostname: {parsed_url.hostname}")

            host_uuid = host_uuid_match.group(1)

            # Extract path components

            path_parts = parsed_url.path.strip('/').split('/')

            if len(path_parts) < 2:

                raise ValueError(f"Invalid path structure: {parsed_url.path}")

            # First part contains all the token parameters (like query parameters but in path)

            full_token_part = path_parts[0]

            # Second part should be the video UUID

            video_uuid = path_parts[1]

            # Parse token parameters from the first path component

            if '&' not in full_token_part:

                raise ValueError(f"No token parameters found in path: {full_token_part}")

            token_params = {}

            for param in full_token_part.split('&'):

                if '=' in param:

                    key, value = param.split('=', 1)

                    # Store both raw and decoded versions

                    token_params[key] = value

                    token_params[f"{key}_decoded"] = unquote(value)

            # Extract required parameters

            bcdn_token = token_params.get('bcdn_token')

            token_path = token_params.get('token_path_decoded', '')  # Use decoded version

            if not bcdn_token:

                raise ValueError("bcdn_token not found in URL parameters")

            if not token_path:

                raise ValueError("token_path not found in URL parameters")

            print(f"Debug: FIXED parsing of videoStreamUrl:")

            print(f"  Host UUID: {host_uuid}")

            print(f"  Video UUID: {video_uuid}")

            print(f"  bcdn_token: {bcdn_token}")

            print(f"  token_path (decoded): {token_path}")

            print(f"  Full token part: {full_token_part}")

            return {

                'host_uuid': host_uuid,

                'video_uuid': video_uuid,

                'bcdn_token': bcdn_token,

                'token_path': token_path.strip('/'),  # Clean version

                'full_token_part': full_token_part,  # Preserve original for reconstruction

                'all_token_params': token_params  # All parameters for debugging

            }

        except Exception as e:

            print(f"Error in FIXED parsing of videoStreamUrl '{video_stream_url}': {e}")

            return {}

    def construct_init_url_fixed(self, video_tokens: Dict[str, str], quality: str) -> str:

        """

        FIXED: Construct the init.mp4 URL using the preserved token parameters

        Format: https://vz-{host_uuid}.b-cdn.net/{full_token_part}/{video_uuid}/{quality}/init.mp4

        """

        required_keys = ['host_uuid', 'video_uuid', 'full_token_part']

        if not all(key in video_tokens for key in required_keys):

            missing = [key for key in required_keys if key not in video_tokens]

            raise ValueError(f"Missing required tokens to construct init URL: {missing}")

        # Use the full token part as-is from the original URL to maintain exact authentication

        url = (f"https://vz-{video_tokens['host_uuid']}.b-cdn.net/"

               f"{video_tokens['full_token_part']}/"

               f"{video_tokens['video_uuid']}/{quality}/init.mp4")

        print(f"Debug: FIXED constructed init.mp4 URL for {quality}: {url}")

        return url

    async def download_init_file(self, url: str, file_path: Path, quality: str, max_retries: int = 3) -> bool:

        """

        Download init.mp4 file for a specific quality with retry logic

        """

        for attempt in range(max_retries):

            try:

                print(f"Debug: Downloading init.mp4 for {quality} (attempt {attempt + 1}/{max_retries})")

                print(f"Debug: URL: {url}")

                request_headers = {

                    "Referer": "https://fikfap.com",

                    "Origin": "https://fikfap.com",

                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

                    "Accept": "*/*",

                    "Accept-Language": "en-US,en;q=0.9"

                }

                async with self.session.get(url, headers=request_headers) as response:

                    print(f"Debug: Response status for {quality} init.mp4: {response.status}")

                    print(f"Debug: Response headers: {dict(response.headers)}")

                    if response.status == 200:

                        # Ensure directory exists

                        file_path.parent.mkdir(parents=True, exist_ok=True)

                        with open(file_path, 'wb') as f:

                            async for chunk in response.content.iter_chunked(8192):

                                f.write(chunk)

                        file_size = file_path.stat().st_size

                        print(f"Debug: Successfully downloaded {quality} init.mp4 ({file_size} bytes)")



                        # Verify the file was actually written and has content

                        if file_size > 0:

                            return True

                        else:

                            print(f"Debug: Downloaded file is empty, retrying...")

                            if attempt < max_retries - 1:

                                await asyncio.sleep(2 ** attempt)  # Exponential backoff

                                continue

                    else:

                        print(f"Debug: Failed to download {quality} init.mp4: HTTP {response.status}")

                        # Try to get response content for debugging

                        try:

                            content = await response.text()

                            print(f"Debug: Response content: {content[:200]}...")

                        except:

                            pass



                        # Retry on certain status codes

                        if response.status in [403, 429, 500, 502, 503, 504] and attempt < max_retries - 1:

                            wait_time = 2 ** attempt

                            print(f"Debug: Retryable error, waiting {wait_time} seconds before retry...")

                            await asyncio.sleep(wait_time)

                            continue



                return False

            except Exception as e:

                print(f"Debug: Error downloading {quality} init.mp4 (attempt {attempt + 1}): {e}")

                if attempt < max_retries - 1:

                    wait_time = 2 ** attempt

                    print(f"Debug: Waiting {wait_time} seconds before retry...")

                    await asyncio.sleep(wait_time)

                    continue



        print(f"Debug: Failed to download {quality} init.mp4 after {max_retries} attempts")

        return False

    async def ensure_init_file_exists(self, quality_dir: Path, video_tokens: Dict[str, str], quality: str, max_retries: int = 5) -> bool:

        """

        Ensure init.mp4 file exists in the quality directory, with multiple retry attempts

        """

        init_file_path = quality_dir / "init.mp4"



        # Check if file already exists and is valid

        if init_file_path.exists() and init_file_path.stat().st_size > 0:

            print(f"Debug: init.mp4 already exists for {quality} ({init_file_path.stat().st_size} bytes)")

            return True



        # File doesn't exist or is empty, try to download it

        if not video_tokens:

            print(f"Debug: No video tokens available, cannot download init.mp4 for {quality}")

            return False



        try:

            init_url = self.construct_init_url_fixed(video_tokens, quality)

            success = await self.download_init_file(init_url, init_file_path, quality, max_retries)



            # Final verification

            if success and init_file_path.exists() and init_file_path.stat().st_size > 0:

                print(f"Debug: Successfully ensured init.mp4 exists for {quality}")

                return True

            else:

                print(f"Debug: Failed to ensure init.mp4 exists for {quality}")

                return False



        except Exception as e:

            print(f"Debug: Error ensuring init.mp4 exists for {quality}: {e}")

            return False

    async def download_and_organize_post(self, post_data: Dict[str, Any]) -> Dict[str, Any]:

        """Download and organize a single post according to the folder structure, including audio-only streams if present"""

        try:

            post_id = str(post_data.get("postId", "unknown"))

            print(f"Processing post {post_id}: {str(post_data.get('label', ''))[:50]}...")

            # Check if already downloaded

            if self.progress_tracker.is_video_downloaded(post_id):

                print(f"Post {post_id} already downloaded, skipping...")

                return {"success": True, "post_id": post_id, "skipped": True, "reason": "already_downloaded"}

            # Create post directory structure

            post_dir = self.base_download_path / post_id

            post_dir.mkdir(parents=True, exist_ok=True)

            # Create m3u8 subdirectory

            m3u8_dir = post_dir / "m3u8"

            m3u8_dir.mkdir(parents=True, exist_ok=True)

            # Save metadata

            await self.save_metadata(post_data, post_dir / "data.json")

            # Get main playlist URL

            video_stream_url = post_data.get("videoStreamUrl")

            if not video_stream_url:

                return {"success": False, "error": "No video stream URL found", "post_id": post_id}

            # Check if video is ready

            if not post_data.get("isBunnyVideoReady", False):

                print(f"Video not ready for post {post_id}, skipping...")

                return {"success": False, "error": "Video not ready (isBunnyVideoReady=false)", "post_id": post_id}

            # Download and analyze main playlist

            playlist_result = await self.download_main_playlist(video_stream_url, m3u8_dir, post_data)

            if not playlist_result["success"]:

                return {"success": False, "error": playlist_result["error"], "post_id": post_id}

            # Download all quality variants

            qualities_result = await self.download_all_qualities(

                playlist_result["qualities"],

                m3u8_dir,

                video_stream_url,

                post_data

            )

            # --- AUDIO-ONLY STREAM EXTRACTION AND DOWNLOAD ---

            audio_result = await self.download_audio_stream(

                m3u8_dir,

                playlist_result.get("main_playlist_path"),

                video_stream_url

            )

            # Mark as successfully downloaded in progress

            self.progress_tracker.add_downloaded_video(post_id)

            result = {

                "success": True,

                "post_id": post_id,

                "post_dir": str(post_dir),

                "main_playlist": playlist_result.get("main_playlist_path"),

                "qualities_downloaded": qualities_result["successful"],

                "qualities_failed": qualities_result["failed"],

                "total_files": qualities_result["total_files"],

                "audio": audio_result

            }

            print(f"Post {post_id} completed: {len(qualities_result['successful'])} qualities downloaded")

            if audio_result.get("audio_found"):

                print(f"Audio stream found and downloaded: {audio_result.get('audio_playlist_path')}")

            else:

                print("No audio-only stream found.")

            return result

        except Exception as e:

            print(f"Error processing post {post_id}: {e}")

            return {"success": False, "error": str(e), "post_id": post_id}

    async def download_audio_stream(self, m3u8_dir: Path, main_playlist_path: str, video_stream_url: str) -> Dict[str, Any]:

        """Parse master playlist for audio-only streams, download audio playlist and all .m4a segments"""

        try:

            # Read master playlist

            if not main_playlist_path or not os.path.exists(main_playlist_path):

                return {"audio_found": False, "reason": "No master playlist found"}

            with open(main_playlist_path, 'r', encoding='utf-8') as f:

                master_content = f.read()

            # Parse for audio-only variants

            audio_uri = None

            audio_group_id = None

            audio_playlist_name = "audio.m3u8"

            lines = master_content.strip().split('\n')

            # 1. Look for #EXT-X-MEDIA:TYPE=AUDIO

            for line in lines:

                if line.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in line:

                    # Parse attributes

                    attrs = self.parse_m3u8_attributes(line)

                    if "URI" in attrs:

                        audio_uri = attrs["URI"].strip('"')

                        audio_group_id = attrs.get("GROUP-ID", None)

                        break

            # 2. If not found, look for audio-only #EXT-X-STREAM-INF

            if not audio_uri:

                for i, line in enumerate(lines):

                    if line.startswith("#EXT-X-STREAM-INF"):

                        attrs = self.parse_m3u8_attributes(line)

                        # If CODECS contains only audio codecs (e.g., mp4a)

                        codecs = attrs.get("CODECS", "")

                        if codecs and codecs.lower().startswith("mp4a"):

                            # Next line should be the URI

                            if i + 1 < len(lines):

                                candidate = lines[i + 1].strip()

                                if candidate and not candidate.startswith("#"):

                                    audio_uri = candidate

                                    break

            if not audio_uri:

                return {"audio_found": False, "reason": "No audio stream found in master playlist"}

            # Resolve audio playlist URL

            audio_playlist_url = self.resolve_m3u8_url(audio_uri, video_stream_url)

            # Download audio playlist file

            audio_playlist_path = m3u8_dir / audio_playlist_name

            audio_playlist_success = await self.download_file_with_l̥l̥retries(

                audio_playlist_url, audio_playlist_path, is_binary=False, max_retries=3

            )

            if not audio_playlist_success:

                return {"audio_found": True, "audio_playlist_path": str(audio_playlist_path), "audio_success": False, "reason": "Failed to download audio playlist"}

            # Parse audio playlist for .m4a segments

            with open(audio_playlist_path, 'r', encoding='utf-8') as f:

                audio_playlist_content = f.read()

            audio_segments = self.parse_audio_segments(audio_playlist_content, audio_playlist_url)

            # Download all .m4a segments

            audio_files = []

            audio_success = True

            for idx, seg_url in enumerate(audio_segments, 1):

                seg_name = f"audio{idx}.m4a"

                seg_path = m3u8_dir / seg_name

                seg_ok = await self.download_file_with_retries(seg_url, seg_path, is_binary=True, max_retries=3)

                if seg_ok:

                    audio_files.append(seg_name)

                else:

                    audio_success = False

                    print(f"Failed to download audio segment: {seg_url}")

            return {

                "audio_found": True,

                "audio_playlist_path": str(audio_playlist_path),

                "audio_success": audio_success,

                "audio_files": audio_files,

                "audio_segments_count": len(audio_segments)

            }

        except Exception as e:

            print(f"Error in download_audio_stream: {e}")

            return {"audio_found": False, "error": str(e)}
        
        
    def parse_audio_segments(self, playlist_content: str, playlist_url: str) -> List[str]:
        """
        Parse audio segments from m3u8 playlist content
        Returns list of absolute URLs for audio segments
        """
        segments = []
        lines = playlist_content.strip().split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('#'):
                continue
            
            # This line should be a segment URL
            if not line.startswith('#'):
                # Resolve relative URLs to absolute
                segment_url = self.resolve_m3u8_url(line, playlist_url)
                segments.append(segment_url)
        
        return segments

    async def download_main_playlist(self, video_stream_url: str, m3u8_dir: Path, post_data: Dict) -> Dict:
        """
        Download and parse the main playlist.m3u8
        """
        try:
            main_playlist_path = m3u8_dir / "playlist.m3u8"
            
            # Download main playlist
            success = await self.download_file_with_retries(
                video_stream_url, 
                main_playlist_path, 
                is_binary=False,
                max_retries=3
            )
            
            if not success:
                return {"success": False, "error": "Failed to download main playlist"}
            
            # Parse for quality variants
            with open(main_playlist_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            qualities = self.parse_quality_variants(content)
            
            # Parse video tokens for init.mp4 downloads
            video_tokens = self.parse_videostream_url_fixed(video_stream_url)
            
            return {
                "success": True,
                "main_playlist_path": str(main_playlist_path),
                "qualities": qualities,
                "video_tokens": video_tokens
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    def parse_quality_variants(self, playlist_content: str) -> List[Dict]:
        """
        Parse quality variants from master playlist
        """
        qualities = []
        lines = playlist_content.strip().split('\n')
        
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                # Parse stream info
                attrs = self.parse_m3u8_attributes(line)
                
                # Get the URI from next line
                if i + 1 < len(lines):
                    uri = lines[i + 1].strip()
                    if uri and not uri.startswith("#"):
                        # Extract quality from URI (e.g., "720p/video.m3u8" -> "720p")
                        quality_match = re.search(r'(\d+p?)', uri)
                        quality = quality_match.group(1) if quality_match else "unknown"
                        
                        # Ensure 'p' suffix
                        if quality.isdigit():
                            quality = f"{quality}p"
                        
                        qualities.append({
                            "quality": quality,
                            "uri": uri,
                            "bandwidth": attrs.get("BANDWIDTH", "0"),
                            "resolution": attrs.get("RESOLUTION", ""),
                            "codecs": attrs.get("CODECS", "")
                        })
        
        return qualities

    async def download_all_qualities(self, qualities: List[Dict], m3u8_dir: Path, 
                                    video_stream_url: str, post_data: Dict) -> Dict:
        """
        Download all quality variants including video segments and audio
        """
        successful = []
        failed = []
        total_files = 0
        
        # Get video tokens for init.mp4 downloads
        video_tokens = self.parse_videostream_url_fixed(video_stream_url)
        
        for quality_info in qualities:
            quality = quality_info["quality"]
            uri = quality_info["uri"]
            
            try:
                # Create quality directory
                quality_dir = m3u8_dir / quality
                quality_dir.mkdir(parents=True, exist_ok=True)
                
                # Resolve quality playlist URL
                quality_playlist_url = self.resolve_m3u8_url(uri, video_stream_url)
                
                # Download quality playlist
                quality_playlist_path = quality_dir / "video.m3u8"
                playlist_success = await self.download_file_with_retries(
                    quality_playlist_url,
                    quality_playlist_path,
                    is_binary=False,
                    max_retries=3
                )
                
                if not playlist_success:
                    failed.append(quality)
                    continue
                
                # Parse and download video segments
                with open(quality_playlist_path, 'r', encoding='utf-8') as f:
                    playlist_content = f.read()
                
                video_segments = self.parse_video_segments(playlist_content, quality_playlist_url)
                
                # Download init.mp4 if needed
                if video_tokens:
                    await self.ensure_init_file_exists(quality_dir, video_tokens, quality)
                
                # Download video segments
                segment_count = 0
                for idx, seg_url in enumerate(video_segments, 1):
                    seg_name = f"video{idx}.m4s"
                    seg_path = quality_dir / seg_name
                    
                    if await self.download_file_with_retries(seg_url, seg_path, is_binary=True, max_retries=2):
                        segment_count += 1
                
                # Download audio for this quality
                audio_result = await self.download_quality_audio(
                    quality_dir, 
                    video_stream_url,
                    quality
                )
                
                total_files += segment_count
                if audio_result.get("audio_found"):
                    total_files += audio_result.get("audio_segments_count", 0)
                
                successful.append({
                    "quality": quality,
                    "video_segments": segment_count,
                    "audio": audio_result
                })
                
                print(f"  {quality}: {segment_count} video segments, "
                    f"{audio_result.get('audio_segments_count', 0)} audio segments")
                
            except Exception as e:
                print(f"  Error downloading {quality}: {e}")
                failed.append(quality)
        
        return {
            "successful": successful,
            "failed": failed,
            "total_files": total_files
        }

    def parse_video_segments(self, playlist_content: str, playlist_url: str) -> List[str]:
        """
        Parse video segments from quality-specific playlist
        """
        segments = []
        lines = playlist_content.strip().split('\n')
        
        for i, line in enumerate(lines):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue
            
            # This should be a segment URL
            segment_url = self.resolve_m3u8_url(line, playlist_url)
            segments.append(segment_url)
        
        return segments

    async def download_quality_audio(self, quality_dir: Path, video_stream_url: str, quality: str) -> Dict:
        """
        Download audio stream for a specific quality
        """
        try:
            # Read the main playlist to find audio stream
            main_playlist_path = quality_dir.parent / "playlist.m3u8"
            if not main_playlist_path.exists():
                return {"audio_found": False, "reason": "No main playlist found"}
            
            with open(main_playlist_path, 'r', encoding='utf-8') as f:
                master_content = f.read()
            
            # Look for audio URI
            audio_uri = None
            lines = master_content.strip().split('\n')
            
            # First check for dedicated audio stream
            for line in lines:
                if line.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in line:
                    attrs = self.parse_m3u8_attributes(line)
                    if "URI" in attrs:
                        audio_uri = attrs["URI"].strip('"')
                        break
            
            # If no dedicated audio stream, extract from quality-specific stream
            if not audio_uri:
                # Construct audio URI based on quality pattern
                # Usually follows pattern like: {quality}/audio.m3u8
                audio_uri = f"{quality}/audio.m3u8"
            
            # Resolve audio playlist URL
            audio_playlist_url = self.resolve_m3u8_url(audio_uri, video_stream_url)
            
            # Download audio playlist
            audio_playlist_path = quality_dir / "audio.m3u8"
            audio_success = await self.download_file_with_retries(
                audio_playlist_url,
                audio_playlist_path,
                is_binary=False,
                max_retries=3
            )
            
            if not audio_success:
                # Try alternative audio path
                audio_uri_alt = audio_uri.replace("/audio.m3u8", "/audio/audio.m3u8")
                audio_playlist_url_alt = self.resolve_m3u8_url(audio_uri_alt, video_stream_url)
                
                audio_success = await self.download_file_with_retries(
                    audio_playlist_url_alt,
                    audio_playlist_path,
                    is_binary=False,
                    max_retries=2
                )
                
                if not audio_success:
                    return {"audio_found": False, "reason": "Failed to download audio playlist"}
                
                audio_playlist_url = audio_playlist_url_alt
            
            # Parse and download audio segments
            with open(audio_playlist_path, 'r', encoding='utf-8') as f:
                audio_content = f.read()
            
            audio_segments = self.parse_audio_segments(audio_content, audio_playlist_url)
            
            # Download audio segments
            audio_files = []
            for idx, seg_url in enumerate(audio_segments, 1):
                seg_name = f"audio{idx}.m4a"
                seg_path = quality_dir / seg_name
                
                if await self.download_file_with_retries(seg_url, seg_path, is_binary=True, max_retries=2):
                    audio_files.append(seg_name)
            
            return {
                "audio_found": True,
                "audio_playlist_path": str(audio_playlist_path),
                "audio_files": audio_files,
                "audio_segments_count": len(audio_files)
            }
            
        except Exception as e:
            print(f"  Audio download error for {quality}: {e}")
            return {"audio_found": False, "error": str(e)}

    async def save_metadata(self, post_data: Dict, file_path: Path):
        """
        Save post metadata to JSON file
        """
        try:
            # Clean metadata - remove large binary data if present
            clean_data = {
                k: v for k, v in post_data.items() 
                if not isinstance(v, bytes) and k not in ['thumbnail_data', 'video_data']
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(clean_data, f, indent=2, ensure_ascii=False, default=str)
                
        except Exception as e:
            print(f"Error saving metadata: {e}")


    async def download_file_with_retries(self, url: str, file_path: Path, is_binary: bool = True, max_retries: int = 3) -> bool:
        """
        Download a file with retry logic
        """
        for attempt in range(max_retries):
            try:
                request_headers = {
                    "Referer": "https://fikfap.com",
                    "Origin": "https://fikfap.com",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9"
                }
                
                async with self.session.get(url, headers=request_headers) as response:
                    if response.status == 200:
                        file_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        if is_binary:
                            with open(file_path, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    f.write(chunk)
                        else:
                            content = await response.text()
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(content)
                        
                        if file_path.stat().st_size > 0:
                            return True
                        
                    elif response.status in [403, 429, 500, 502, 503, 504] and attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                        
            except Exception as e:
                print(f"Download error (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
        
        return False



    def parse_m3u8_attributes(self, line: str) -> Dict[str, str]:

        """Parse key=value pairs from a M3U8 attribute line"""

        attrs = {}

        if ':' in line:

            line = line.split(':', 1)[1]

        pairs = []

        current = ''

        in_quotes = False

        for c in line:

            if c == '"':

                in_quotes = not in_quotes

                current += c

            elif c == ',' and not in_quotes:

                pairs.append(current.strip())

                current = ''

            else:

                current += c

        if current.strip():

            pairs.append(current.strip())

        for pair in pairs:

            if '=' in pair:

                k, v = pair.split('=', 1)

                attrs[k.strip()] = v.strip()

        return attrs

    def resolve_m3u8_url(self, uri: str, base_url: str) -> str:

        """Resolve relative or absolute URI against base_url"""

        if uri.startswith("http://") or uri.startswith("https://"):

            return uri

        # If starts with '/', join with scheme+netloc

        parsed = urlparse(base_url)

        if uri.startswith("/"):
            # Join with scheme and netloc to form absolute URL
            return f"{parsed.scheme}://{parsed.netloc}{uri}"
