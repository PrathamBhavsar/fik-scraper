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

        """Download and organize a single post according to the folder structure"""

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

            # Mark as successfully downloaded in progress

            self.progress_tracker.add_downloaded_video(post_id)

            result = {

                "success": True,

                "post_id": post_id,

                "post_dir": str(post_dir),

                "main_playlist": playlist_result.get("main_playlist_path"),

                "qualities_downloaded": qualities_result["successful"],

                "qualities_failed": qualities_result["failed"],

                "total_files": qualities_result["total_files"]

            }

            print(f"Post {post_id} completed: {len(qualities_result['successful'])} qualities downloaded")

            return result

        except Exception as e:

            print(f"Error processing post {post_id}: {e}")

            return {"success": False, "error": str(e), "post_id": post_id}

    async def save_metadata(self, post_data: Dict[str, Any], metadata_path: Path):

        """Save post metadata to data.json"""

        try:

            # Clean up metadata (remove any non-serializable data)

            clean_metadata = self.clean_metadata_for_json(post_data)

            with open(metadata_path, 'w', encoding='utf-8') as f:

                json.dump(clean_metadata, f, indent=2, ensure_ascii=False, default=str)

            print(f"Metadata saved: {metadata_path}")

        except Exception as e:

            print(f"Failed to save metadata: {e}")

    def clean_metadata_for_json(self, data: Any) -> Any:

        """Recursively clean data for JSON serialization"""

        if isinstance(data, dict):

            return {k: self.clean_metadata_for_json(v) for k, v in data.items()}

        elif isinstance(data, list):

            return [self.clean_metadata_for_json(item) for item in data]

        elif isinstance(data, (str, int, float, bool)) or data is None:

            return data

        else:

            return str(data)  # Convert any other type to string

    async def download_main_playlist(self, video_stream_url: str, m3u8_dir: Path, post_data: Dict[str, Any]) -> Dict[str, Any]:

        """Download and parse the main M3U8 playlist with enhanced CDN authentication"""

        try:

            print("Downloading main playlist from CDN...")

            print(f"URL: {video_stream_url[:80]}...")

            # Extract CDN information for better authentication

            parsed_url = urlparse(video_stream_url)

            bunny_video_id = post_data.get("bunnyVideoId", "")

            # Enhanced headers specifically for this request

            request_headers = {

                "Referer": "https://fikfap.com",

                "Origin": "https://fikfap.com",

                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

                "Accept": "*/*",

                "Accept-Language": "en-US,en;q=0.9",

                "Accept-Encoding": "gzip, deflate, br",

                "Cache-Control": "no-cache",

                "Pragma": "no-cache",

                "Sec-Fetch-Dest": "empty",

                "Sec-Fetch-Mode": "cors",

                "Sec-Fetch-Site": "cross-site"

            }

            print("Using enhanced CDN authentication headers...")

            # Make request with retries

            for attempt in range(3):

                try:

                    async with self.session.get(video_stream_url, headers=request_headers) as response:

                        print(f"Response status: {response.status}")

                        print(f"Response headers: {dict(response.headers)}")

                        if response.status == 200:

                            playlist_content = await response.text()

                            print(f"Successfully downloaded playlist: {len(playlist_content)} characters")

                            break

                        elif response.status == 403:

                            print(f"HTTP 403 - CDN authentication failed (attempt {attempt + 1}/3)")

                            if attempt < 2:  # Don't sleep on last attempt

                                await asyncio.sleep(2 ** attempt)  # Exponential backoff

                                continue

                        else:

                            return {"success": False, "error": f"HTTP {response.status} fetching main playlist"}

                except Exception as e:

                    print(f"Request failed (attempt {attempt + 1}/3): {e}")

                    if attempt < 2:

                        await asyncio.sleep(2 ** attempt)

                        continue

                else:

                    return {"success": False, "error": "HTTP 403 fetching main playlist - CDN authentication failed after 3 attempts"}

            # If we get here, all attempts failed

            else:

                return {"success": False, "error": "HTTP 403 fetching main playlist - CDN authentication failed after 3 attempts"}

            # Save main playlist

            main_playlist_path = m3u8_dir / "playlist.m3u8"

            with open(main_playlist_path, 'w', encoding='utf-8') as f:

                f.write(playlist_content)

            print(f"Main playlist saved: {main_playlist_path}")

            # Parse playlist to extract quality variants

            qualities = self.parse_master_playlist(playlist_content, video_stream_url)

            return {

                "success": True,

                "main_playlist_path": str(main_playlist_path),

                "qualities": qualities

            }

        except Exception as e:

            print(f"Failed to download main playlist: {e}")

            return {"success": False, "error": str(e)}

    def parse_master_playlist(self, content: str, base_url: str) -> List[Dict[str, Any]]:

        """Parse master playlist to extract quality information"""

        qualities = []

        lines = content.strip().split('\n')

        i = 0

        while i < len(lines):

            line = lines[i].strip()

            # Look for stream info lines

            if line.startswith("#EXT-X-STREAM-INF"):

                # Parse stream information

                stream_info = self.parse_stream_info(line)

                # Next line should contain the URL

                if i + 1 < len(lines):

                    url_line = lines[i + 1].strip()

                    if url_line and not url_line.startswith("#"):

                        # Resolve relative URL

                        absolute_url = urljoin(base_url, url_line)

                        # Determine quality and codec

                        quality_info = self.determine_quality_info(stream_info, url_line, absolute_url)

                        quality_info["url"] = absolute_url

                        qualities.append(quality_info)

                        print(f"Found quality: {quality_info['resolution']} ({quality_info['codec']})")

                i += 1

            i += 1

        print(f"Total qualities found: {len(qualities)}")

        return qualities

    def parse_stream_info(self, stream_line: str) -> Dict[str, str]:

        """Parse EXT-X-STREAM-INF line to extract stream information"""

        info = {}

        # Remove the EXT-X-STREAM-INF prefix

        stream_line = stream_line.replace("#EXT-X-STREAM-INF:", "")

        # Parse comma-separated key=value pairs

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

        # Parse each pair

        for pair in pairs:

            if "=" in pair:

                key, value = pair.split("=", 1)

                info[key.strip()] = value.strip().strip('"')

        return info

    def determine_quality_info(self, stream_info: Dict[str, str], url_path: str, full_url: str) -> Dict[str, Any]:

        """Determine resolution and codec information"""

        # Default values

        resolution = "unknown"

        codec = "h264"  # Default codec

        is_vp9 = False

        # Check for VP9 codec patterns

        combined_text = f"{stream_info} {url_path} {full_url}".lower()

        for vp9_pattern in self.vp9_patterns:

            if vp9_pattern in combined_text:

                codec = "vp9"

                is_vp9 = True

                break

        # Extract resolution from RESOLUTION field

        if "RESOLUTION" in stream_info:

            resolution_str = stream_info["RESOLUTION"]

            if "x" in resolution_str:

                width, height = resolution_str.split("x")

                height = int(height.strip())

                # Map height to standard resolution names

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

        # Fallback: try to extract resolution from URL or other info

        if resolution == "unknown":

            resolution = self.extract_resolution_from_url(url_path) or "720p"

        return {

            "resolution": resolution,

            "codec": codec,

            "is_vp9": is_vp9,

            "stream_info": stream_info

        }

    def extract_resolution_from_url(self, url: str) -> Optional[str]:

        """Try to extract resolution from URL patterns"""

        url_lower = url.lower()

        # Check for common resolution patterns

        for resolution, patterns in self.quality_patterns.items():

            for pattern in patterns:

                if pattern in url_lower:

                    return resolution

        return None

    async def download_all_qualities(self, qualities: List[Dict[str, Any]], m3u8_dir: Path, base_url: str, post_data: Dict[str, Any]) -> Dict[str, Any]:

        """Download all quality variants with enhanced authentication"""

        successful = []

        failed = []

        total_files = 0

        for quality in qualities:

            try:

                print(f"Downloading quality: {quality['resolution']} ({quality['codec']})")

                # Determine directory name

                if quality["is_vp9"]:

                    quality_dirname = f"vp9_{quality['resolution']}"

                else:

                    quality_dirname = quality["resolution"]

                # Create quality directory

                quality_dir = m3u8_dir / quality_dirname

                quality_dir.mkdir(parents=True, exist_ok=True)

                # Download this quality variant

                result = await self.download_quality_variant(quality, quality_dir, base_url, post_data)

                if result["success"]:

                    successful.append({

                        "resolution": quality["resolution"],

                        "codec": quality["codec"],

                        "directory": quality_dirname,

                        "files": result["files"]

                    })

                    total_files += result["file_count"]

                    print(f"{quality['resolution']} completed: {result['file_count']} files")

                else:

                    failed.append({

                        "resolution": quality["resolution"],

                        "error": result["error"]

                    })

                    print(f"{quality['resolution']} failed: {result['error']}")

            except Exception as e:

                failed.append({

                    "resolution": quality.get("resolution", "unknown"),

                    "error": str(e)

                })

                print(f"{quality.get('resolution', 'unknown')} failed: {e}")

        return {

            "successful": successful,

            "failed": failed,

            "total_files": total_files

        }

    async def download_quality_variant(self, quality: Dict[str, Any], quality_dir: Path, base_url: str, post_data: Dict[str, Any]) -> Dict[str, Any]:

        """Download a specific quality variant with enhanced authentication + IMPROVED init.mp4 download"""

        try:

            playlist_url = quality["url"]



            # Enhanced headers for quality playlist request

            request_headers = {

                "Referer": "https://fikfap.com",

                "Origin": "https://fikfap.com",

                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

                "Accept": "*/*",

                "Accept-Language": "en-US,en;q=0.9"

            }



            # Download the quality playlist

            async with self.session.get(playlist_url, headers=request_headers) as response:

                if response.status != 200:

                    return {"success": False, "error": f"HTTP {response.status}"}



                playlist_content = await response.text()



            # Save quality playlist

            playlist_path = quality_dir / "video.m3u8"

            with open(playlist_path, 'w', encoding='utf-8') as f:

                f.write(playlist_content)



            # Parse segments from playlist

            segments = self.parse_playlist_segments(playlist_content, playlist_url)



            if not segments:

                return {"success": False, "error": "No segments found in playlist"}



            print(f"Found {len(segments)} segments to download")



            # =============================================================================

            # IMPROVED: Download init.mp4 file for this quality with enhanced retry logic

            # =============================================================================



            # Parse the videoStreamUrl to extract tokens using FIXED parsing

            video_stream_url = post_data.get("videoStreamUrl", "")

            video_tokens = self.parse_videostream_url_fixed(video_stream_url)



            # Ensure init.mp4 file exists with robust retry mechanism

            init_success = await self.ensure_init_file_exists(quality_dir, video_tokens, quality["resolution"])



            if not init_success:

                print(f"WARNING: Could not download init.mp4 for {quality['resolution']} - playlist may not be playable")



            # =============================================================================

            # END: IMPROVED init.mp4 download logic

            # =============================================================================



            # Download segments

            downloaded_files = ["video.m3u8"]  # Include the playlist file



            # Add init.mp4 to downloaded files if it exists

            init_file_path = quality_dir / "init.mp4"

            if init_file_path.exists() and init_file_path.stat().st_size > 0:

                downloaded_files.append("init.mp4")

                print(f"Debug: init.mp4 confirmed present for {quality['resolution']} ({init_file_path.stat().st_size} bytes)")

            else:

                print(f"WARNING: init.mp4 missing for {quality['resolution']} - this may affect playback")



            # Download video segments

            for i, segment_url in enumerate(segments, 1):

                try:

                    segment_filename = f"video{i}.m4s"

                    segment_path = quality_dir / segment_filename



                    # Download segment with enhanced headers

                    async with self.session.get(segment_url, headers=request_headers) as response:

                        if response.status == 200:

                            with open(segment_path, 'wb') as f:

                                async for chunk in response.content.iter_chunked(8192):

                                    f.write(chunk)

                            downloaded_files.append(segment_filename)



                            # Progress update every 10 segments

                            if i % 10 == 0:

                                print(f"Downloaded {i}/{len(segments)} segments...")

                        else:

                            print(f"Failed to download segment {i}: HTTP {response.status}")



                except Exception as e:

                    print(f"Error downloading segment {i}: {e}")

                    continue



            # Final verification that init.mp4 exists

            init_file_path = quality_dir / "init.mp4"

            if not (init_file_path.exists() and init_file_path.stat().st_size > 0):

                # Last attempt to download init.mp4

                print(f"Final attempt to ensure init.mp4 exists for {quality['resolution']}...")

                final_init_success = await self.ensure_init_file_exists(quality_dir, video_tokens, quality["resolution"], max_retries=3)

                if final_init_success and init_file_path.exists() and init_file_path.stat().st_size > 0:

                    if "init.mp4" not in downloaded_files:

                        downloaded_files.append("init.mp4")

                    print(f"Final attempt successful: init.mp4 now present for {quality['resolution']}")

                else:

                    print(f"FINAL WARNING: init.mp4 still missing for {quality['resolution']} after all attempts")



            return {

                "success": True,

                "file_count": len(downloaded_files),

                "files": downloaded_files

            }



        except Exception as e:

            return {"success": False, "error": str(e)}

    def parse_playlist_segments(self, playlist_content: str, base_url: str) -> List[str]:

        """Parse playlist to extract segment URLs"""

        segments = []

        lines = playlist_content.strip().split('\n')



        for line in lines:

            line = line.strip()

            # Skip comments and empty lines

            if not line or line.startswith("#"):

                continue



            # This is a segment URL

            segment_url = urljoin(base_url, line)

            segments.append(segment_url)



        return segments

    async def process_all_posts(self, posts_file: str = "all_raw_posts.json") -> Dict[str, Any]:

        """Process all posts from the JSON file - FIXED to remove unnecessary JSON files"""

        try:

            # Load posts data

            with open(posts_file, 'r', encoding='utf-8') as f:

                posts = json.load(f)

            print(f"Starting processing of {len(posts)} posts...")

            results = {

                "successful": [],

                "failed": [],

                "summary": {}

            }

            # Process each post

            for i, post in enumerate(posts, 1):

                print("-" * 50)

                print(f"Processing post {i}/{len(posts)}")

                print("-" * 50)

                result = await self.download_and_organize_post(post)

                if result["success"]:

                    results["successful"].append(result)

                else:

                    results["failed"].append(result)

                # Small delay between posts to be nice to the server

                await asyncio.sleep(2)  # Increased delay to avoid rate limiting

            # Generate summary - FIXED: Use len() instead of 'total_posts'

            progress_stats = self.progress_tracker.get_stats()

            results["summary"] = {

                "successful_count": len(results["successful"]),

                "failed_count": len(results["failed"]),

                "success_rate": f"{(len(results['successful']) / len(posts) * 100):.1f}%",

                "total_qualities": sum(len(r.get("qualities_downloaded", [])) for r in results["successful"]),

                "total_files": sum(r.get("total_files", 0) for r in results["successful"]),

                "total_downloaded_ever": progress_stats["total_downloaded"]  # From progress tracker

            }

            print("Processing completed!")

            print(f"Results: {results['summary']['successful_count']}/{len(posts)} successful ({results['summary']['success_rate']})")

            print(f"Total files downloaded: {results['summary']['total_files']}")

            print(f"Total videos downloaded ever: {results['summary']['total_downloaded_ever']}")

            # DO NOT save download_results.json - only maintain progress.json

            return results

        except Exception as e:

            print(f"Error processing posts: {e}")

            return {"error": str(e)}

# Usage example

async def main():

    """Main function to run the downloader"""

    async with VideoOrganizer("./downloads") as downloader:

        results = await downloader.process_all_posts()

        return results

if __name__ == "__main__":

    import asyncio

    asyncio.run(main())
