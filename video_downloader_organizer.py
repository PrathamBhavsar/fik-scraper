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

from playlist_manager import download_and_organize_post_with_custom_playlist

# Import the progress tracker
try:
    from utils.progress import ProgressTracker
except ImportError:
    # Fallback if utils module not found
    import json
    from pathlib import Path



class VideoDownloaderOrganizer:
    """Complete video downloader and organizer for FikFap posts with ONLY progress.json tracking"""

    def __init__(self, base_download_path: str = "./downloads"):
        self.base_download_path = Path(base_download_path)
        self.base_download_path.mkdir(parents=True, exist_ok=True)
        self.session: Optional[aiohttp.ClientSession] = None
        self.original_download_and_organize_post = self.download_and_organize_post
        self.download_and_organize_post = lambda post_data: download_and_organize_post_with_custom_playlist(self, post_data)

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

    async def download_init_file(self, url: str, file_path: Path, quality: str) -> bool:
        """
        Download init.mp4 file for a specific quality
        """
        try:
            print(f"Debug: Downloading init.mp4 for {quality}")
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
                    return True
                else:
                    print(f"Debug: Failed to download {quality} init.mp4: HTTP {response.status}")
                    # Try to get response content for debugging
                    try:
                        content = await response.text()
                        print(f"Debug: Response content: {content[:200]}...")
                    except:
                        pass
                    return False

        except Exception as e:
            print(f"Debug: Error downloading {quality} init.mp4: {e}")
            return False
        
    def construct_audio_init_url(self, video_tokens: Dict[str, str]) -> str:
        """
        Construct the audio init.mp4 URL using the preserved token parameters
        Format: https://vz-{host_uuid}.b-cdn.net/{full_token_part}/{video_uuid}/audio/init.mp4
        """
        required_keys = ['host_uuid', 'video_uuid', 'full_token_part']
        if not all(key in video_tokens for key in required_keys):
            missing = [key for key in required_keys if key not in video_tokens]
            raise ValueError(f"Missing required tokens to construct audio init URL: {missing}")

        # Construct URL with /audio/ path for audio init.mp4
        url = (f"https://vz-{video_tokens['host_uuid']}.b-cdn.net/"
            f"{video_tokens['full_token_part']}/"
            f"{video_tokens['video_uuid']}/audio/init.mp4")

        print(f"Debug: Constructed audio init.mp4 URL: {url}")
        return url

    async def download_audio_init(self, video_tokens: Dict[str, str], audio_dir: Path) -> bool:
        """
        Download audio init.mp4 file - now saved as 'init.mp4' in audio directory
        """
        try:
            # Construct audio init URL
            audio_init_url = self.construct_audio_init_url(video_tokens)
            # Changed filename from audio_init.mp4 to init.mp4
            audio_init_path = audio_dir / "init.mp4"
            
            print(f"Downloading audio init.mp4...")
            
            # Download using the existing download_init_file method
            success = await self.download_init_file(audio_init_url, audio_init_path, "audio")
            
            if success:
                print(f"Successfully downloaded audio init.mp4 to {audio_init_path}")
            else:
                print(f"Failed to download audio init.mp4")
                
            return success
            
        except Exception as e:
            print(f"Error downloading audio init.mp4: {e}")
            return False



    async def download_and_organize_post(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updated method that includes audio download after video qualities
        Replace the existing download_and_organize_post method with this
        """
        try:
            post_id = str(post_data.get("postId", "unknown"))
            print(f"\nProcessing post {post_id}: {str(post_data.get('label', ''))[:50]}...")
            
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
            
            # === NEW: Download audio stream ===
            audio_result = await self.download_audio_stream(
                m3u8_dir,
                playlist_result.get("main_playlist_path"),
                video_stream_url
            )
            
            # Mark as successfully downloaded in progress
            self.progress_tracker.add_downloaded_video(post_id)
            
            # Calculate total files including audio
            total_files = qualities_result["total_files"]
            if audio_result.get("success"):
                total_files += len(audio_result.get("audio_files", []))
            
            result = {
                "success": True,
                "post_id": post_id,
                "post_dir": str(post_dir),
                "main_playlist": playlist_result.get("main_playlist_path"),
                "qualities_downloaded": qualities_result["successful"],
                "qualities_failed": qualities_result["failed"],
                "audio": audio_result,  # Add audio result
                "total_files": total_files,
                "summary": {
                    "video_qualities": len(qualities_result["successful"]),
                    "video_segments": qualities_result["total_files"],
                    "audio_found": audio_result.get("audio_found", False),
                    "audio_segments": audio_result.get("downloaded_segments", 0)
                }
            }
            
            print(f"\n✓ Post {post_id} completed:")
            print(f"  - Video: {len(qualities_result['successful'])} qualities, {qualities_result['total_files']} files")
            if audio_result.get("audio_found"):
                print(f"  - Audio: {audio_result.get('downloaded_segments', 0)}/{audio_result.get('total_segments', 0)} segments")
            else:
                print(f"  - Audio: Not found")
            
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


    async def download_file_with_retries(self, url: str, file_path: Path, is_binary: bool = True, max_retries: int = 3) -> bool:
        """
        Generic file downloader with retry logic for both binary and text files
        """
        for attempt in range(max_retries):
            try:
                request_headers = {
                    "accept": "*/*",
                    "accept-language": "en-US,en;q=0.9",
                    "origin": "https://fikfap.com",
                    "referer": "https://fikfap.com/",
                    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "cross-site",
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
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
                        
                        # Verify file was written
                        if file_path.exists() and file_path.stat().st_size > 0:
                            return True
                            
                    elif response.status in [403, 429, 500, 502, 503, 504] and attempt < max_retries - 1:
                        wait_time = min(2 ** attempt, 10)  # Max 10 seconds wait
                        print(f"  Retryable error {response.status}, waiting {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        print(f"  Failed with status {response.status}")
                        
            except Exception as e:
                print(f"  Download error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue
        
        return False

    def parse_m3u8_attributes(self, line: str) -> Dict[str, str]:
        """
        Parse key=value pairs from M3U8 attribute lines
        Handles quoted values and comma separation correctly
        """
        attrs = {}
        if ':' in line:
            line = line.split(':', 1)[1]
        
        # Manual parsing to handle quoted values with commas
        pairs = []
        current = ''
        in_quotes = False
        
        for c in line:
            if c == '"':
                in_quotes = not in_quotes
                current += c
            elif c == ',' and not in_quotes:
                if current.strip():
                    pairs.append(current.strip())
                current = ''
            else:
                current += c
        
        if current.strip():
            pairs.append(current.strip())
        
        # Parse each pair
        for pair in pairs:
            if '=' in pair:
                k, v = pair.split('=', 1)
                # Remove quotes from value if present
                v = v.strip()
                if v.startswith('"') and v.endswith('"'):
                    v = v[1:-1]
                attrs[k.strip()] = v
        
        return attrs

    def resolve_audio_url(self, uri: str, base_url: str) -> str:
        """
        Resolve audio URL - handles both relative and absolute URLs
        Special handling for audio paths
        """
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        
        # For audio, we need to construct based on the CDN pattern
        parsed = urlparse(base_url)
        
        # If URI starts with /, it's absolute from domain
        if uri.startswith("/"):
            return f"{parsed.scheme}://{parsed.netloc}{uri}"
        
        # For relative paths like "audio/audio.m3u8"
        # We need to insert it into the correct position in the URL
        base_parts = parsed.path.rstrip('/').split('/')
        
        # Remove 'playlist.m3u8' if it's at the end
        if base_parts and base_parts[-1].endswith('.m3u8'):
            base_parts.pop()
        
        # Construct the new path
        new_path = '/'.join(base_parts) + '/' + uri
        
        return f"{parsed.scheme}://{parsed.netloc}{new_path}"

    def parse_audio_segments(self, playlist_content: str, playlist_url: str) -> List[str]:
        """
        Parse audio segments from audio.m3u8 playlist content
        Returns list of absolute URLs for .m4a segments
        """
        segments = []
        lines = playlist_content.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines and M3U8 directives
            if not line or line.startswith('#'):
                continue
            
            # This should be a segment URL (usually like segment0.m4a, segment1.m4a, etc.)
            segment_url = self.resolve_audio_url(line, playlist_url)
            segments.append(segment_url)
        
        return segments

    async def download_audio_stream(self, m3u8_dir: Path, main_playlist_path: str, video_stream_url: str) -> Dict[str, Any]:
        """
        Complete audio stream downloader with audio init.mp4 support
        Downloads audio files to [videoid]/m3u8/audio/ subdirectory
        """
        try:
            print("\nChecking for audio stream...")
            
            # Read the main playlist to find audio streams
            if not main_playlist_path or not Path(main_playlist_path).exists():
                return {"audio_found": False, "reason": "No master playlist found"}
            
            with open(main_playlist_path, 'r', encoding='utf-8') as f:
                master_content = f.read()
            
            # Method 1: Look for explicit audio media definition
            audio_uri = None
            lines = master_content.strip().split('\n')
            
            for line in lines:
                if line.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in line:
                    attrs = self.parse_m3u8_attributes(line)
                    if "URI" in attrs:
                        audio_uri = attrs["URI"]
                        print(f"Found audio stream in #EXT-X-MEDIA: {audio_uri}")
                        break
            
            # Method 2: Based on your cURL example, construct audio URL directly
            if not audio_uri:
                # Parse the video stream URL to construct audio URL
                parsed = urlparse(video_stream_url)
                path_parts = parsed.path.strip('/').split('/')
                
                if len(path_parts) >= 2:
                    # Replace 'playlist.m3u8' with 'audio/audio.m3u8'
                    audio_uri = "audio/audio.m3u8"
                    print(f"Constructing audio URI from pattern: {audio_uri}")
            
            if not audio_uri:
                return {"audio_found": False, "reason": "No audio stream detected"}
            
            # Construct full audio playlist URL
            audio_playlist_url = self.resolve_audio_url(audio_uri, video_stream_url)
            print(f"Audio playlist URL: {audio_playlist_url}")
            
            # Create audio subdirectory
            audio_dir = m3u8_dir / "audio"
            audio_dir.mkdir(parents=True, exist_ok=True)
            print(f"Created audio directory: {audio_dir}")
            
            # Download audio playlist to audio subdirectory
            audio_playlist_path = audio_dir / "audio.m3u8"
            print(f"Downloading audio playlist...")
            
            audio_playlist_success = await self.download_file_with_retries(
                audio_playlist_url,
                audio_playlist_path,
                is_binary=False,
                max_retries=3
            )
            
            if not audio_playlist_success:
                print(f"Failed to download audio playlist from {audio_playlist_url}")
                return {
                    "audio_found": True,
                    "audio_playlist_url": audio_playlist_url,
                    "success": False,
                    "reason": "Failed to download audio playlist"
                }
            
            # =====================================================
            # Download audio init.mp4 to audio subdirectory
            # =====================================================
            
            # Parse video stream URL to get tokens
            video_tokens = self.parse_videostream_url_fixed(video_stream_url)
            audio_init_success = False
            
            if video_tokens:
                audio_init_success = await self.download_audio_init(video_tokens, audio_dir)
            else:
                print("Warning: Could not parse videoStreamUrl tokens, skipping audio init.mp4")
            
            # =====================================================
            # END: Audio init.mp4 download
            # =====================================================
            
            # Parse audio playlist for segments
            with open(audio_playlist_path, 'r', encoding='utf-8') as f:
                audio_playlist_content = f.read()
            
            audio_segments = self.parse_audio_segments(audio_playlist_content, audio_playlist_url)
            
            if not audio_segments:
                return {
                    "audio_found": True,
                    "audio_playlist_path": str(audio_playlist_path),
                    "audio_init_downloaded": audio_init_success,
                    "success": False,
                    "reason": "No audio segments found in playlist"
                }
            
            print(f"Found {len(audio_segments)} audio segments to download")
            
            # Download all audio segments to audio subdirectory
            audio_files = []
            failed_segments = []
            
            # Add audio.m3u8 to files list
            audio_files.append("audio.m3u8")
            
            # Add init.mp4 if it was downloaded (renamed from audio_init.mp4)
            if audio_init_success:
                audio_files.append("init.mp4")
            
            for idx, segment_url in enumerate(audio_segments, 1):
                segment_name = f"audio{idx}.m4a"
                segment_path = audio_dir / segment_name  # Save to audio subdirectory
                
                # Progress indicator
                if idx % 10 == 0 or idx == 1:
                    print(f"  Downloading audio segment {idx}/{len(audio_segments)}...")
                
                success = await self.download_file_with_retries(
                    segment_url,
                    segment_path,
                    is_binary=True,
                    max_retries=2
                )
                
                if success:
                    audio_files.append(segment_name)
                else:
                    failed_segments.append(idx)
                    print(f"  Failed to download audio segment {idx}: {segment_url}")
            
            # Calculate success
            segments_downloaded = len(audio_files) - (2 if audio_init_success else 1)  # Subtract playlist and init files
            success_rate = segments_downloaded / len(audio_segments) * 100 if audio_segments else 0
            
            result = {
                "audio_found": True,
                "audio_dir": str(audio_dir),
                "audio_playlist_path": str(audio_playlist_path),
                "audio_playlist_url": audio_playlist_url,
                "audio_init_downloaded": audio_init_success,
                "success": segments_downloaded > 0,
                "audio_files": audio_files,
                "total_segments": len(audio_segments),
                "downloaded_segments": segments_downloaded,
                "failed_segments": failed_segments,
                "success_rate": f"{success_rate:.1f}%"
            }
            
            if segments_downloaded > 0:
                print(f"✔ Audio downloaded to {audio_dir}: {segments_downloaded}/{len(audio_segments)} segments ({success_rate:.1f}%)")
                if audio_init_success:
                    print(f"  - Audio init.mp4: Downloaded")
            else:
                print(f"✗ Audio download failed: 0/{len(audio_segments)} segments")
            
            return result
            
        except Exception as e:
            print(f"Error in audio download: {e}")
            import traceback
            traceback.print_exc()
            return {"audio_found": False, "error": str(e)}

      
    async def download_quality_variant(self, quality: Dict[str, Any], quality_dir: Path, base_url: str, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """Download a specific quality variant with video init.mp4"""
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

            # Download video init.mp4 file for this quality
            video_stream_url = post_data.get("videoStreamUrl", "")
            video_tokens = self.parse_videostream_url_fixed(video_stream_url)
            video_init_success = False

            if video_tokens:
                try:
                    # Construct init.mp4 URL for this quality
                    init_url = self.construct_init_url_fixed(video_tokens, quality["resolution"])
                    init_file_path = quality_dir / "init.mp4"

                    # Download init.mp4
                    video_init_success = await self.download_init_file(init_url, init_file_path, quality["resolution"])

                    if video_init_success:
                        print(f"Successfully downloaded video init.mp4 for {quality['resolution']}")
                    else:
                        print(f"Failed to download video init.mp4 for {quality['resolution']}")

                except Exception as e:
                    print(f"Error downloading video init.mp4 for {quality['resolution']}: {e}")
            else:
                print(f"Warning: Could not parse videoStreamUrl tokens, skipping video init.mp4 for {quality['resolution']}")

            # Download segments
            downloaded_files = ["video.m3u8"]  # Include the playlist file

            # Add init.mp4 to downloaded files if it exists
            if video_init_success:
                downloaded_files.append("init.mp4")

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
                                print(f"  Downloaded {i}/{len(segments)} segments...")
                        else:
                            print(f"  Failed to download segment {i}: HTTP {response.status}")

                except Exception as e:
                    print(f"  Error downloading segment {i}: {e}")
                    continue

            return {
                "success": True,
                "file_count": len(downloaded_files),
                "files": downloaded_files,
                "video_init_downloaded": video_init_success
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
    async with VideoDownloaderOrganizer("./downloads") as downloader:
        results = await downloader.process_all_posts()
        return results


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())