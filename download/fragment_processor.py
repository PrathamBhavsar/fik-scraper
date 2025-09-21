"""
Fragment Processor for FikFap Scraper - Phase 3
High-performance concurrent M3U8 fragment downloading and processing
"""
import asyncio
import aiohttp
import aiofiles
import hashlib
import time
from typing import List, Dict, Any, Optional, Callable, Tuple, Set
from pathlib import Path
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field
import re

from core.config import config
from core.exceptions import FragmentError, NetworkError, StorageError
from utils.logger import logger

@dataclass
class FragmentInfo:
    """Information about a single M3U8 fragment"""
    index: int
    url: str
    duration: float = 0.0
    size_bytes: Optional[int] = None
    sequence: Optional[int] = None
    discontinuity: bool = False
    key_url: Optional[str] = None
    key_iv: Optional[str] = None

@dataclass
class DownloadProgress:
    """Track download progress for fragments"""
    total_fragments: int = 0
    completed_fragments: int = 0
    failed_fragments: int = 0
    total_bytes: int = 0
    downloaded_bytes: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage"""
        if self.total_fragments == 0:
            return 0.0
        return (self.completed_fragments / self.total_fragments) * 100

    @property
    def download_speed_mbps(self) -> float:
        """Calculate download speed in Mbps"""
        elapsed = time.time() - self.start_time
        if elapsed <= 0 or self.downloaded_bytes <= 0:
            return 0.0
        return (self.downloaded_bytes * 8) / (elapsed * 1_000_000)

    @property
    def eta_seconds(self) -> float:
        """Estimate time to completion"""
        if self.completed_fragments == 0:
            return 0.0

        avg_time_per_fragment = (time.time() - self.start_time) / self.completed_fragments
        remaining_fragments = self.total_fragments - self.completed_fragments
        return avg_time_per_fragment * remaining_fragments

class FragmentProcessor:
    """
    High-performance M3U8 fragment processor with concurrent downloading

    Features:
    - Concurrent fragment downloading with semaphore control
    - Resume capability for interrupted downloads
    - Comprehensive error handling and retry logic
    - Real-time progress tracking
    - Memory-efficient streaming
    - Fragment integrity verification
    """

    def __init__(self, session: aiohttp.ClientSession):
        """
        Initialize fragment processor

        Args:
            session: aiohttp session for downloads
        """
        self.session = session
        self.logger = logger

        # Configuration
        self.concurrent_fragments = config.get('download.concurrent_fragments', 10)
        self.fragment_timeout = config.get('download.fragment_timeout', 15)
        self.max_retries = config.get('download.max_fragment_retries', 3)
        self.chunk_size = config.get('download.chunk_size', 8192)
        self.enable_resume = config.get('download.enable_resume', True)
        self.temp_dir = Path(config.get('download.temp_dir', './downloads/.temp'))

        # Progress tracking
        self.progress_callback: Optional[Callable[[DownloadProgress], None]] = None
        self.progress_update_interval = config.get('download.progress_update_interval', 1.0)

        # Internal state
        self.semaphore = asyncio.Semaphore(self.concurrent_fragments)
        self.fragment_cache: Dict[str, bytes] = {}
        self.active_downloads: Set[str] = set()

        # Ensure temp directory exists
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Fragment Processor initialized - Concurrent: {self.concurrent_fragments}, "
                        f"Timeout: {self.fragment_timeout}s")

    async def download_playlist_fragments(
        self,
        playlist_url: str,
        output_file: Path,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None
    ) -> bool:
        """
        Download all fragments from an M3U8 playlist and combine them

        Args:
            playlist_url: URL to the M3U8 playlist
            output_file: Output file path for the combined video
            progress_callback: Optional callback for progress updates

        Returns:
            True if successful, False otherwise
        """
        try:
            self.progress_callback = progress_callback
            start_time = time.time()

            self.logger.info(f"Starting fragment download from: {playlist_url}")
            self.logger.info(f"Output file: {output_file}")

            # Parse playlist and extract fragment information
            fragments = await self._parse_playlist_fragments(playlist_url)
            if not fragments:
                raise FragmentError(f"No fragments found in playlist: {playlist_url}")

            self.logger.info(f"Found {len(fragments)} fragments to download")

            # Initialize progress tracking
            progress = DownloadProgress(total_fragments=len(fragments))

            # Check for existing partial download
            temp_fragments = []
            if self.enable_resume:
                temp_fragments = await self._check_existing_fragments(fragments, output_file)

            # Download fragments concurrently
            await self._download_fragments_concurrent(fragments, temp_fragments, progress)

            if progress.failed_fragments > 0:
                self.logger.warning(f"Download completed with {progress.failed_fragments} failed fragments")

            # Combine fragments into final video file
            await self._combine_fragments(temp_fragments, output_file)

            # Cleanup temporary files
            await self._cleanup_temp_fragments(temp_fragments)

            elapsed_time = time.time() - start_time
            self.logger.info(f"Fragment download completed in {elapsed_time:.2f}s - "
                           f"Speed: {progress.download_speed_mbps:.2f} Mbps")

            return True

        except Exception as e:
            self.logger.error(f"Fragment download failed: {e}")
            return False

    async def _parse_playlist_fragments(self, playlist_url: str) -> List[FragmentInfo]:
        """Parse M3U8 playlist and extract fragment information"""
        try:
            # Download playlist content
            async with self.session.get(playlist_url) as response:
                if response.status != 200:
                    raise FragmentError(f"Failed to fetch playlist: HTTP {response.status}")

                playlist_content = await response.text()

            # Parse M3U8 content
            fragments = []
            base_url = urljoin(playlist_url, '.')

            lines = playlist_content.strip().split('\n')
            current_duration = 0.0
            current_sequence = 0
            fragment_index = 0

            i = 0
            while i < len(lines):
                line = lines[i].strip()

                if line.startswith('#EXTINF:'):
                    # Extract duration
                    duration_match = re.search(r'#EXTINF:([\d.]+)', line)
                    if duration_match:
                        current_duration = float(duration_match.group(1))

                elif line.startswith('#EXT-X-MEDIA-SEQUENCE:'):
                    # Extract starting sequence number
                    sequence_match = re.search(r'#EXT-X-MEDIA-SEQUENCE:(\d+)', line)
                    if sequence_match:
                        current_sequence = int(sequence_match.group(1))

                elif line and not line.startswith('#'):
                    # This is a fragment URL
                    fragment_url = urljoin(base_url, line)

                    fragment = FragmentInfo(
                        index=fragment_index,
                        url=fragment_url,
                        duration=current_duration,
                        sequence=current_sequence + fragment_index
                    )

                    fragments.append(fragment)
                    fragment_index += 1
                    current_duration = 0.0  # Reset for next fragment

                i += 1

            self.logger.info(f"Parsed {len(fragments)} fragments from playlist")
            return fragments

        except Exception as e:
            self.logger.error(f"Error parsing playlist fragments: {e}")
            raise FragmentError(f"Playlist parsing failed: {e}")

    async def _check_existing_fragments(self, fragments: List[FragmentInfo], output_file: Path) -> List[Path]:
        """Check for existing fragment files to enable resume"""
        temp_fragments = []
        existing_count = 0

        for fragment in fragments:
            fragment_filename = f"{output_file.stem}_fragment_{fragment.index:06d}.ts"
            fragment_path = self.temp_dir / fragment_filename

            if fragment_path.exists() and fragment_path.stat().st_size > 0:
                # Verify fragment integrity
                if await self._verify_fragment(fragment_path):
                    existing_count += 1
                    self.logger.debug(f"Found existing fragment: {fragment_filename}")
                else:
                    # Remove corrupted fragment
                    fragment_path.unlink()
                    self.logger.debug(f"Removed corrupted fragment: {fragment_filename}")

            temp_fragments.append(fragment_path)

        if existing_count > 0:
            self.logger.info(f"Found {existing_count} existing fragments, resuming download")

        return temp_fragments

    async def _download_fragments_concurrent(
        self, 
        fragments: List[FragmentInfo], 
        temp_fragments: List[Path],
        progress: DownloadProgress
    ):
        """Download fragments concurrently with progress tracking"""

        # Create download tasks for missing fragments
        download_tasks = []

        for i, (fragment, temp_path) in enumerate(zip(fragments, temp_fragments)):
            if not temp_path.exists() or temp_path.stat().st_size == 0:
                task = asyncio.create_task(
                    self._download_single_fragment(fragment, temp_path, progress)
                )
                download_tasks.append(task)
            else:
                # Fragment already exists, count as completed
                progress.completed_fragments += 1
                progress.downloaded_bytes += temp_path.stat().st_size

        # Start progress reporting
        progress_task = asyncio.create_task(self._report_progress(progress))

        try:
            # Execute downloads
            if download_tasks:
                self.logger.info(f"Starting {len(download_tasks)} concurrent fragment downloads")
                await asyncio.gather(*download_tasks, return_exceptions=True)

            # Stop progress reporting
            progress_task.cancel()

        except Exception as e:
            progress_task.cancel()
            raise FragmentError(f"Concurrent download failed: {e}")

    async def _download_single_fragment(
        self, 
        fragment: FragmentInfo, 
        output_path: Path,
        progress: DownloadProgress
    ):
        """Download a single fragment with retry logic"""
        async with self.semaphore:  # Control concurrency
            fragment_id = f"{fragment.index}_{fragment.url.split('/')[-1]}"
            self.active_downloads.add(fragment_id)

            try:
                for attempt in range(self.max_retries):
                    try:
                        await self._download_fragment_attempt(fragment, output_path, progress)
                        progress.completed_fragments += 1
                        break

                    except Exception as e:
                        if attempt == self.max_retries - 1:
                            progress.failed_fragments += 1
                            self.logger.error(f"Fragment {fragment.index} failed after {self.max_retries} attempts: {e}")
                            raise
                        else:
                            self.logger.warning(f"Fragment {fragment.index} attempt {attempt + 1} failed: {e}")
                            await asyncio.sleep(2 ** attempt)  # Exponential backoff

            finally:
                self.active_downloads.discard(fragment_id)

    async def _download_fragment_attempt(
        self, 
        fragment: FragmentInfo, 
        output_path: Path,
        progress: DownloadProgress
    ):
        """Single download attempt for a fragment"""
        timeout = aiohttp.ClientTimeout(total=self.fragment_timeout)

        async with self.session.get(fragment.url, timeout=timeout) as response:
            if response.status != 200:
                raise FragmentError(f"HTTP {response.status} for fragment {fragment.index}")

            # Stream download to file
            temp_file = output_path.with_suffix('.tmp')
            fragment_size = 0

            async with aiofiles.open(temp_file, 'wb') as f:
                async for chunk in response.content.iter_chunked(self.chunk_size):
                    await f.write(chunk)
                    fragment_size += len(chunk)
                    progress.downloaded_bytes += len(chunk)

            # Atomic move to final location
            temp_file.rename(output_path)
            fragment.size_bytes = fragment_size

            self.logger.debug(f"Downloaded fragment {fragment.index}: {fragment_size:,} bytes")

    async def _verify_fragment(self, fragment_path: Path) -> bool:
        """Verify fragment file integrity"""
        try:
            if not fragment_path.exists():
                return False

            # Basic size check
            size = fragment_path.stat().st_size
            if size < 100:  # Fragments should be at least 100 bytes
                return False

            # Check for valid TS/MP4 fragment header
            async with aiofiles.open(fragment_path, 'rb') as f:
                header = await f.read(16)

                # Check for TS sync byte (0x47) or MP4 box signatures
                if header.startswith(b'\x47') or b'ftyp' in header or b'styp' in header:
                    return True

            return False

        except Exception as e:
            self.logger.debug(f"Fragment verification failed: {e}")
            return False

    async def _combine_fragments(self, fragment_paths: List[Path], output_file: Path):
        """Combine downloaded fragments into final video file"""
        try:
            self.logger.info(f"Combining {len(fragment_paths)} fragments into: {output_file}")

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            total_size = 0
            combined_fragments = 0

            async with aiofiles.open(output_file, 'wb') as output:
                for fragment_path in fragment_paths:
                    if fragment_path.exists():
                        try:
                            async with aiofiles.open(fragment_path, 'rb') as fragment:
                                while True:
                                    chunk = await fragment.read(self.chunk_size * 10)  # Larger chunks for combining
                                    if not chunk:
                                        break
                                    await output.write(chunk)
                                    total_size += len(chunk)

                            combined_fragments += 1

                        except Exception as e:
                            self.logger.warning(f"Error reading fragment {fragment_path}: {e}")
                    else:
                        self.logger.warning(f"Missing fragment: {fragment_path}")

            self.logger.info(f"Combined {combined_fragments} fragments into {output_file.name} "
                           f"({total_size:,} bytes)")

            if combined_fragments < len(fragment_paths) * 0.9:  # Allow 10% fragment loss
                self.logger.warning(f"Only {combined_fragments}/{len(fragment_paths)} fragments were combined")

        except Exception as e:
            self.logger.error(f"Error combining fragments: {e}")
            raise StorageError(f"Fragment combination failed: {e}")

    async def _cleanup_temp_fragments(self, fragment_paths: List[Path]):
        """Clean up temporary fragment files"""
        try:
            cleaned_count = 0
            for fragment_path in fragment_paths:
                if fragment_path.exists():
                    try:
                        fragment_path.unlink()
                        cleaned_count += 1
                    except Exception as e:
                        self.logger.warning(f"Could not delete fragment {fragment_path}: {e}")

            self.logger.info(f"Cleaned up {cleaned_count} temporary fragments")

        except Exception as e:
            self.logger.warning(f"Fragment cleanup error: {e}")

    async def _report_progress(self, progress: DownloadProgress):
        """Report download progress at regular intervals"""
        try:
            last_update = 0

            while progress.completed_fragments + progress.failed_fragments < progress.total_fragments:
                current_time = time.time()

                if current_time - last_update >= self.progress_update_interval:
                    if self.progress_callback:
                        try:
                            self.progress_callback(progress)
                        except Exception as e:
                            self.logger.warning(f"Progress callback error: {e}")

                    # Log progress
                    self.logger.info(
                        f"Progress: {progress.progress_percentage:.1f}% "
                        f"({progress.completed_fragments}/{progress.total_fragments}) - "
                        f"Speed: {progress.download_speed_mbps:.2f} Mbps - "
                        f"ETA: {progress.eta_seconds:.0f}s"
                    )

                    last_update = current_time

                await asyncio.sleep(0.5)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Progress reporting error: {e}")

    def get_active_download_count(self) -> int:
        """Get number of currently active fragment downloads"""
        return len(self.active_downloads)

    def cancel_active_downloads(self):
        """Cancel all active fragment downloads"""
        self.logger.warning("Cancelling all active fragment downloads")
        self.active_downloads.clear()

    async def estimate_download_size(self, playlist_url: str) -> Optional[int]:
        """Estimate total download size by sampling fragments"""
        try:
            fragments = await self._parse_playlist_fragments(playlist_url)
            if not fragments:
                return None

            # Sample first few fragments to estimate size
            sample_size = min(3, len(fragments))
            total_sample_size = 0

            for fragment in fragments[:sample_size]:
                try:
                    async with self.session.head(fragment.url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                        if response.status == 200 and 'content-length' in response.headers:
                            total_sample_size += int(response.headers['content-length'])
                except Exception:
                    continue

            if total_sample_size > 0:
                avg_fragment_size = total_sample_size / sample_size
                estimated_size = int(avg_fragment_size * len(fragments))

                self.logger.info(f"Estimated download size: {estimated_size:,} bytes "
                               f"({len(fragments)} fragments)")
                return estimated_size

            return None

        except Exception as e:
            self.logger.warning(f"Size estimation failed: {e}")
            return None
