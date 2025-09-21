"""
M3U8 Downloader for FikFap Scraper - Phase 3
Complete M3U8 download system with quality management and progress tracking
"""
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from datetime import datetime
import uuid
import time
import re

from core.config import config
from core.exceptions import DownloadError, QualityNotFoundError, StorageError, NetworkError
from data.models import VideoPost, VideoQuality, DownloadJob, DownloadStatus
from utils.logger import logger
from utils.helpers import sanitize_filename

from .quality_manager import QualityManager
from .fragment_processor import FragmentProcessor, DownloadProgress

class M3U8Downloader:
    """
    Complete M3U8 download system

    Features:
    - Master playlist parsing and quality selection
    - Concurrent multi-quality downloads
    - Progress tracking and callbacks
    - Resume capability for interrupted downloads
    - Intelligent quality filtering and selection
    - File organization and metadata storage
    """

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        """
        Initialize M3U8 downloader

        Args:
            session: Optional aiohttp session (will create one if not provided)
        """
        self.session = session
        self._own_session = session is None
        self.logger = logger

        # Initialize components
        self.quality_manager = QualityManager()
        self.fragment_processor: Optional[FragmentProcessor] = None

        # Configuration
        self.base_download_path = Path(config.get('storage.base_path', './downloads'))
        self.organize_by_author = config.get('storage.organize_by_author', True)
        self.organize_by_date = config.get('storage.organize_by_date', False)
        self.filename_template = config.get('storage.filename_template', 
                                          '{author}_{title}_{resolution}_{postId}')
        self.max_filename_length = config.get('storage.max_filename_length', 200)
        self.sanitize_filenames = config.get('storage.sanitize_filenames', True)

        # Progress tracking
        self.active_downloads: Dict[str, DownloadJob] = {}
        self.progress_callbacks: Dict[str, Callable[[DownloadProgress], None]] = {}

        # Ensure base download path exists
        self.base_download_path.mkdir(parents=True, exist_ok=True)

        self.logger.info("M3U8 Downloader initialized")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()

    async def start(self):
        """Initialize downloader components"""
        if not self.session:
            connector = aiohttp.TCPConnector(
                limit=config.get('download.concurrent_downloads', 5),
                verify_ssl=config.get('download.verify_ssl', True)
            )

            timeout = aiohttp.ClientTimeout(total=config.get('api.timeout', 30))

            headers = {
                'User-Agent': config.get('download.user_agent', 'FikFap-Scraper/2.0')
            }

            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )

        self.fragment_processor = FragmentProcessor(self.session)
        self.logger.info("M3U8 Downloader started")

    async def close(self):
        """Close downloader and cleanup resources"""
        # Cancel active downloads
        for job_id in list(self.active_downloads.keys()):
            await self.cancel_download(job_id)

        # Close session if we own it
        if self._own_session and self.session:
            await self.session.close()
            self.session = None

        self.logger.info("M3U8 Downloader closed")

    async def download_video(
        self,
        video_post: VideoPost,
        selected_qualities: Optional[List[VideoQuality]] = None,
        progress_callback: Optional[Callable[[str, DownloadProgress], None]] = None
    ) -> List[DownloadJob]:
        """
        Download video in selected qualities

        Args:
            video_post: Video post with metadata and available qualities
            selected_qualities: Specific qualities to download (None for auto-selection)
            progress_callback: Optional progress callback function

        Returns:
            List of download jobs created
        """
        try:
            if not self.session:
                await self.start()

            self.logger.info(f"Starting download for video: {video_post.label} (ID: {video_post.postId})")

            # Filter and select qualities if not provided
            if selected_qualities is None:
                filtered_qualities = await self.quality_manager.filter_qualities(video_post.availableQualities)
                selected_qualities = self.quality_manager.select_qualities_for_download(filtered_qualities)

            if not selected_qualities:
                raise QualityNotFoundError("No suitable qualities found for download")

            self.logger.info(f"Selected {len(selected_qualities)} qualities for download")

            # Create download directory structure
            download_dir = await self._create_download_directory(video_post)

            # Create download jobs for each quality
            download_jobs = []
            for quality in selected_qualities:
                job = await self._create_download_job(video_post, quality, download_dir)
                download_jobs.append(job)

                if progress_callback:
                    self.progress_callbacks[job.jobId] = lambda progress, job_id=job.jobId: progress_callback(job_id, progress)

            # Start downloads concurrently
            download_tasks = [
                asyncio.create_task(self._execute_download_job(job))
                for job in download_jobs
            ]

            # Wait for all downloads to complete
            results = await asyncio.gather(*download_tasks, return_exceptions=True)

            # Process results
            successful_jobs = []
            failed_jobs = []

            for job, result in zip(download_jobs, results):
                if isinstance(result, Exception):
                    job.status = DownloadStatus.FAILED
                    job.errorMessage = str(result)
                    failed_jobs.append(job)
                    self.logger.error(f"Download job {job.jobId} failed: {result}")
                else:
                    successful_jobs.append(job)

            self.logger.info(f"Download completed: {len(successful_jobs)} successful, {len(failed_jobs)} failed")

            return download_jobs

        except Exception as e:
            self.logger.error(f"Video download failed: {e}")
            raise DownloadError(f"Video download failed: {e}")

    async def download_quality(
        self,
        video_post: VideoPost,
        quality: VideoQuality,
        output_path: Optional[Path] = None,
        progress_callback: Optional[Callable[[DownloadProgress], None]] = None
    ) -> DownloadJob:
        """
        Download a single quality

        Args:
            video_post: Video post metadata
            quality: Quality to download
            output_path: Optional custom output path
            progress_callback: Optional progress callback

        Returns:
            Download job object
        """
        try:
            if not self.session:
                await self.start()

            # Create output path if not provided
            if output_path is None:
                download_dir = await self._create_download_directory(video_post)
                output_path = download_dir / self._generate_filename(video_post, quality)

            # Create download job
            job = await self._create_download_job(video_post, quality, output_path.parent, output_path.name)

            if progress_callback:
                self.progress_callbacks[job.jobId] = progress_callback

            # Execute download
            await self._execute_download_job(job)

            return job

        except Exception as e:
            self.logger.error(f"Quality download failed: {e}")
            raise DownloadError(f"Quality download failed: {e}")

    async def _create_download_job(
        self,
        video_post: VideoPost,
        quality: VideoQuality,
        download_dir: Path,
        filename: Optional[str] = None
    ) -> DownloadJob:
        """Create a download job for a specific quality"""

        if filename is None:
            filename = self._generate_filename(video_post, quality)

        job_id = str(uuid.uuid4())
        output_path = download_dir / filename

        job = DownloadJob(
            jobId=job_id,
            postId=video_post.postId,
            videoUrl=quality.playlist_url,
            outputPath=str(output_path),
            status=DownloadStatus.PENDING,
            progress=0.0,
            createdAt=datetime.now()
        )

        self.active_downloads[job_id] = job

        self.logger.info(f"Created download job {job_id}: {filename}")
        return job

    async def _execute_download_job(self, job: DownloadJob) -> bool:
        """Execute a download job"""
        try:
            job.status = DownloadStatus.DOWNLOADING
            output_path = Path(job.outputPath)

            self.logger.info(f"Executing download job {job.jobId}: {output_path.name}")

            # Get progress callback for this job
            progress_callback = self.progress_callbacks.get(job.jobId)

            def job_progress_callback(fragment_progress: DownloadProgress):
                # Update job progress
                job.progress = fragment_progress.progress_percentage

                # Call external callback if provided
                if progress_callback:
                    progress_callback(fragment_progress)

            # Download fragments
            success = await self.fragment_processor.download_playlist_fragments(
                str(job.videoUrl),
                output_path,
                job_progress_callback
            )

            if success:
                job.status = DownloadStatus.COMPLETED
                job.completedAt = datetime.now()
                job.progress = 100.0
                self.logger.info(f"Download job {job.jobId} completed successfully")
            else:
                job.status = DownloadStatus.FAILED
                job.errorMessage = "Fragment download failed"
                self.logger.error(f"Download job {job.jobId} failed")

            return success

        except Exception as e:
            job.status = DownloadStatus.FAILED
            job.errorMessage = str(e)
            self.logger.error(f"Download job {job.jobId} execution failed: {e}")
            return False

        finally:
            # Clean up progress callback
            self.progress_callbacks.pop(job.jobId, None)

    async def _create_download_directory(self, video_post: VideoPost) -> Path:
        """Create organized directory structure for downloads"""
        base_path = self.base_download_path

        # Organize by author if enabled
        if self.organize_by_author and video_post.author:
            author_name = self._sanitize_filename(video_post.author.username)
            base_path = base_path / author_name

        # Organize by date if enabled
        if self.organize_by_date:
            date_str = video_post.publishedAt.strftime('%Y-%m-%d')
            base_path = base_path / date_str

        # Create directory
        base_path.mkdir(parents=True, exist_ok=True)

        return base_path

    def _generate_filename(self, video_post: VideoPost, quality: VideoQuality) -> str:
        """Generate filename for downloaded video"""

        # Prepare template variables
        template_vars = {
            'title': video_post.label,
            'author': video_post.author.username if video_post.author else 'Unknown',
            'resolution': quality.resolution,
            'codec': quality.codec.value,
            'postId': video_post.postId,
            'date': video_post.publishedAt.strftime('%Y%m%d'),
            'timestamp': int(video_post.publishedAt.timestamp())
        }

        # Sanitize template variables
        if self.sanitize_filenames:
            template_vars = {k: self._sanitize_filename(str(v)) for k, v in template_vars.items()}

        # Generate filename from template
        try:
            filename = self.filename_template.format(**template_vars)
        except KeyError as e:
            self.logger.warning(f"Invalid filename template variable: {e}")
            filename = f"{template_vars['author']}_{template_vars['title']}_{template_vars['resolution']}_{template_vars['postId']}"

        # Add file extension
        filename += ".mp4"

        # Enforce length limit
        if len(filename) > self.max_filename_length:
            # Truncate title while preserving other important info
            excess = len(filename) - self.max_filename_length
            if len(template_vars['title']) > excess + 10:  # Keep at least 10 chars of title
                template_vars['title'] = template_vars['title'][:-(excess + 3)] + "..."
                filename = self.filename_template.format(**template_vars) + ".mp4"
            else:
                # Fallback to simple filename
                filename = f"{template_vars['postId']}_{template_vars['resolution']}.mp4"

        return filename

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem compatibility"""
        if not self.sanitize_filenames:
            return filename

        # Remove or replace invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)

        # Remove control characters
        filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)

        # Replace multiple spaces/underscores with single underscore
        filename = re.sub(r'[\s_]+', '_', filename)

        # Remove leading/trailing spaces and dots
        filename = filename.strip('. ')

        # Ensure filename is not empty
        if not filename:
            filename = "untitled"

        return filename

    async def cancel_download(self, job_id: str) -> bool:
        """Cancel an active download job"""
        job = self.active_downloads.get(job_id)
        if not job:
            self.logger.warning(f"Download job {job_id} not found")
            return False

        if job.status in [DownloadStatus.COMPLETED, DownloadStatus.FAILED, DownloadStatus.CANCELLED]:
            self.logger.warning(f"Download job {job_id} cannot be cancelled (status: {job.status})")
            return False

        job.status = DownloadStatus.CANCELLED
        job.errorMessage = "Cancelled by user"

        # Remove from active downloads
        self.active_downloads.pop(job_id, None)
        self.progress_callbacks.pop(job_id, None)

        # Cancel fragment downloads if fragment processor is available
        if self.fragment_processor:
            self.fragment_processor.cancel_active_downloads()

        self.logger.info(f"Download job {job_id} cancelled")
        return True

    async def get_download_status(self, job_id: str) -> Optional[DownloadJob]:
        """Get status of a download job"""
        return self.active_downloads.get(job_id)

    async def list_active_downloads(self) -> List[DownloadJob]:
        """List all active download jobs"""
        return list(self.active_downloads.values())

    async def estimate_download_size(self, video_post: VideoPost) -> Dict[str, int]:
        """Estimate download sizes for available qualities"""
        if not self.fragment_processor:
            await self.start()

        size_estimates = {}

        for quality in video_post.availableQualities:
            try:
                estimated_size = await self.fragment_processor.estimate_download_size(str(quality.playlist_url))
                if estimated_size:
                    size_estimates[quality.resolution] = estimated_size
            except Exception as e:
                self.logger.debug(f"Size estimation failed for {quality.resolution}: {e}")

        return size_estimates

    async def analyze_video_qualities(self, video_post: VideoPost) -> Dict[str, Any]:
        """Analyze available video qualities"""
        return await self.quality_manager.analyze_quality_distribution(video_post)

    def get_quality_summary(self, video_post: VideoPost) -> str:
        """Get human-readable summary of available qualities"""
        return self.quality_manager.get_quality_summary(video_post.availableQualities)

    async def validate_download_prerequisites(self, video_post: VideoPost) -> Tuple[bool, List[str]]:
        """Validate that video can be downloaded"""
        issues = []

        # Check if video post has qualities
        if not video_post.availableQualities:
            issues.append("No video qualities available")

        # Check if any qualities pass filtering
        try:
            filtered_qualities = await self.quality_manager.filter_qualities(video_post.availableQualities)
            if not filtered_qualities:
                issues.append("No qualities pass current filtering criteria")
        except Exception as e:
            issues.append(f"Quality filtering error: {e}")

        # Check video stream URL
        if not video_post.videoStreamUrl:
            issues.append("No video stream URL available")

        # Check disk space
        if config.get('monitoring.check_disk_space', True):
            try:
                import shutil
                free_space = shutil.disk_usage(self.base_download_path).free
                min_space = config.get('monitoring.min_disk_space_gb', 5.0) * 1024**3

                if free_space < min_space:
                    issues.append(f"Insufficient disk space: {free_space / 1024**3:.2f}GB available")
            except Exception as e:
                issues.append(f"Disk space check failed: {e}")

        # Check network connectivity
        if self.session:
            try:
                # Test connectivity with a simple request
                async with self.session.head(str(video_post.videoStreamUrl), timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status >= 400:
                        issues.append(f"Video stream URL not accessible: HTTP {response.status}")
            except Exception as e:
                issues.append(f"Network connectivity issue: {e}")

        is_valid = len(issues) == 0
        return is_valid, issues
