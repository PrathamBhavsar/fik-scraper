import asyncio
import aiohttp
import aiofiles
import os
import json
import ssl
from pathlib import Path
from typing import List, Dict, Optional
import time
from urllib.parse import urlparse


class OptimalMediaDownloader:
    """
    Optimal media downloader for video metadata, thumbnails, and video files.
    Downloads files in parallel batches with proper error handling and progress tracking.
    FIXED: SSL certificate verification issues
    """
    
    def __init__(self, base_download_dir: str = "downloads", max_concurrent: int = 5, chunk_size: int = 8192):
        """
        Initialize the media downloader.
        
        Args:
            base_download_dir: Base directory for downloads
            max_concurrent: Maximum concurrent downloads
            chunk_size: Chunk size for streaming downloads
        """
        self.base_download_dir = Path(base_download_dir)
        self.max_concurrent = max_concurrent
        self.chunk_size = chunk_size
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.download_stats = {
            'total_files': 0,
            'completed': 0,
            'failed': 0,
            'start_time': None
        }
    
    async def __aenter__(self):
        """Async context manager entry - FIXED SSL ISSUES"""
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Create connector with SSL fix
        connector = aiohttp.TCPConnector(
            limit=50, 
            limit_per_host=10,
            ssl=ssl_context,  # Use custom SSL context
            force_close=True,  # Force close connections
            enable_cleanup_closed=True  # Clean up closed connections
        )
        
        # Increase timeout for better reliability
        timeout = aiohttp.ClientTimeout(total=600, connect=60)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    def create_video_directory(self, video_id: str) -> Path:
        """Create directory structure for a video"""
        video_dir = self.base_download_dir / video_id
        video_dir.mkdir(parents=True, exist_ok=True)
        return video_dir
    
    async def download_file_with_retry(self, url: str, file_path: Path, max_retries: int = 3) -> bool:
        """
        Download a file with retry logic and progress tracking.
        FIXED: Better error handling and SSL issues
        
        Args:
            url: URL to download from
            file_path: Local file path to save to
            max_retries: Maximum number of retry attempts
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not url or url.strip() == "":
            print(f"⚠️ Empty URL provided for {file_path.name}")
            return False
            
        async with self.semaphore:
            for attempt in range(max_retries + 1):
                try:
                    print(f"📥 Downloading {file_path.name} (attempt {attempt + 1}/{max_retries + 1})")
                    print(f"    URL: {url[:100]}{'...' if len(url) > 100 else ''}")
                    
                    async with self.session.get(
                        url, 
                        ssl=False,  # Explicitly disable SSL verification for this request
                        allow_redirects=True
                    ) as response:
                        if response.status == 200:
                            file_size = response.headers.get('content-length')
                            if file_size:
                                file_size = int(file_size)
                                print(f"    Size: {self._format_size(file_size)}")
                            
                            # Stream download to file
                            async with aiofiles.open(file_path, 'wb') as file:
                                downloaded = 0
                                async for chunk in response.content.iter_chunked(self.chunk_size):
                                    await file.write(chunk)
                                    downloaded += len(chunk)
                                    
                                    # Show progress for large files
                                    if file_size and file_size > 1024 * 1024:  # > 1MB
                                        progress = (downloaded / file_size) * 100
                                        if downloaded % (self.chunk_size * 50) == 0:  # Update every ~400KB
                                            print(f"    Progress: {progress:.1f}%")
                            
                            print(f"✅ Successfully downloaded {file_path.name}")
                            self.download_stats['completed'] += 1
                            return True
                        elif response.status == 404:
                            print(f"❌ File not found (404): {file_path.name}")
                            self.download_stats['failed'] += 1
                            return False
                        else:
                            print(f"⚠️ HTTP {response.status} for {url}")
                            if attempt == max_retries:
                                print(f"❌ Failed to download {file_path.name} after {max_retries + 1} attempts")
                                self.download_stats['failed'] += 1
                                return False
                
                except asyncio.TimeoutError:
                    print(f"⏰ Timeout downloading {file_path.name} (attempt {attempt + 1})")
                except aiohttp.ClientSSLError as e:
                    print(f"🔒 SSL Error downloading {file_path.name}: {e} (attempt {attempt + 1})")
                except aiohttp.ClientConnectorError as e:
                    print(f"🌐 Connection Error downloading {file_path.name}: {e} (attempt {attempt + 1})")
                except Exception as e:
                    print(f"⚠️ Error downloading {file_path.name}: {e} (attempt {attempt + 1})")
                
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"    Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
            
            print(f"❌ Failed to download {file_path.name} after all attempts")
            self.download_stats['failed'] += 1
            return False
    
    async def save_json_metadata(self, video_data: Dict, file_path: Path) -> bool:
        """Save video metadata as JSON file"""
        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                await file.write(json.dumps(video_data, indent=2, ensure_ascii=False))
            print(f"✅ Saved metadata: {file_path.name}")
            return True
        except Exception as e:
            print(f"❌ Error saving metadata {file_path.name}: {e}")
            return False
    
    async def download_video_files(self, video_data: Dict) -> Dict[str, bool]:
        """
        Download all files for a single video.
        
        Args:
            video_data: Video metadata dictionary
            
        Returns:
            Dict with download results for each file type
        """
        video_id = video_data.get('video_id', 'unknown')
        video_dir = self.create_video_directory(video_id)
        
        results = {
            'metadata': False,
            'thumbnail': False,
            'video': False
        }
        
        print(f"🎬 Processing video: {video_data.get('title', 'Unknown')} (ID: {video_id})")
        
        # Create file paths
        json_path = video_dir / f"{video_id}.json"
        jpg_path = video_dir / f"{video_id}.jpg"
        mp4_path = video_dir / f"{video_id}.mp4"
        
        # Download tasks
        tasks = []
        
        # 1. Save metadata (always first)
        results['metadata'] = await self.save_json_metadata(video_data, json_path)
        
        # 2. Download thumbnail
        thumbnail_url = video_data.get('thumbnail_src', '')
        if thumbnail_url:
            tasks.append(self._download_thumbnail(thumbnail_url, jpg_path, video_id))
        else:
            print(f"⚠️ No thumbnail URL for video {video_id}")
        
        # 3. Download video
        video_url = video_data.get('video_src', '')
        if video_url:
            tasks.append(self._download_video(video_url, mp4_path, video_id))
        else:
            print(f"⚠️ No video URL for video {video_id}")
        
        # Execute downloads concurrently
        if tasks:
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Update results based on task completion
            task_index = 0
            if thumbnail_url:
                results['thumbnail'] = task_results[task_index] if not isinstance(task_results[task_index], Exception) else False
                task_index += 1
            if video_url:
                results['video'] = task_results[task_index] if not isinstance(task_results[task_index], Exception) else False
        
        # Summary
        success_count = sum(results.values())
        total_count = len([k for k, v in results.items() if k == 'metadata' or 
                          (k == 'thumbnail' and thumbnail_url) or 
                          (k == 'video' and video_url)])
        
        print(f"📊 Video {video_id} completed: {success_count}/{total_count} files successful")
        return results
    
    async def _download_thumbnail(self, url: str, file_path: Path, video_id: str) -> bool:
        """Download thumbnail with specific handling"""
        return await self.download_file_with_retry(url, file_path)
    
    async def _download_video(self, url: str, file_path: Path, video_id: str) -> bool:
        """Download video with specific handling"""
        return await self.download_file_with_retry(url, file_path)
    
    async def download_all_videos(self, videos_data: List[Dict]) -> Dict[str, Dict]:
        """
        Download all files for multiple videos.
        
        Args:
            videos_data: List of video metadata dictionaries
            
        Returns:
            Dict with results for each video
        """
        if not videos_data:
            print("❌ No video data provided")
            return {}
        
        # Initialize stats
        self.download_stats['total_files'] = len(videos_data) * 3  # JSON + JPG + MP4 per video
        self.download_stats['completed'] = 0
        self.download_stats['failed'] = 0
        self.download_stats['start_time'] = time.time()
        
        print(f"🚀 Starting download of {len(videos_data)} videos...")
        print(f"📁 Download directory: {self.base_download_dir.absolute()}")
        print(f"⚡ Max concurrent downloads: {self.max_concurrent}")
        print(f"🔒 SSL verification: DISABLED (for compatibility)")
        
        # Create base download directory
        self.base_download_dir.mkdir(parents=True, exist_ok=True)
        
        # Download videos in batches to avoid overwhelming the server
        batch_size = min(self.max_concurrent, 10)  # Process up to 10 videos simultaneously
        results = {}
        
        for i in range(0, len(videos_data), batch_size):
            batch = videos_data[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = (len(videos_data) + batch_size - 1) // batch_size
            
            print(f"\n📦 Processing batch {batch_number}/{total_batches} ({len(batch)} videos)")
            
            # Process batch concurrently
            batch_tasks = [self.download_video_files(video_data) for video_data in batch]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Store results
            for j, result in enumerate(batch_results):
                if not isinstance(result, Exception):
                    video_id = batch[j].get('video_id', f'unknown_{i+j}')
                    results[video_id] = result
                else:
                    print(f"❌ Batch error for video {batch[j].get('video_id', 'unknown')}: {result}")
            
            # Show progress
            completed_videos = len(results)
            progress = (completed_videos / len(videos_data)) * 100
            print(f"📈 Overall progress: {completed_videos}/{len(videos_data)} videos ({progress:.1f}%)")
            
            # Brief pause between batches to be server-friendly
            if i + batch_size < len(videos_data):
                await asyncio.sleep(2)
        
        # Final statistics
        elapsed_time = time.time() - self.download_stats['start_time']
        successful_videos = sum(1 for r in results.values() if any(r.values()))
        
        print(f"\n🎯 DOWNLOAD COMPLETE!")
        print(f"📊 Statistics:")
        print(f"   ✅ Successful videos: {successful_videos}/{len(videos_data)}")
        print(f"   ✅ Files completed: {self.download_stats['completed']}")
        print(f"   ❌ Files failed: {self.download_stats['failed']}")
        print(f"   ⏱️  Total time: {elapsed_time:.1f} seconds")
        print(f"   💾 Download location: {self.base_download_dir.absolute()}")
        
        return results
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} TB"
    
    def get_download_summary(self, results: Dict[str, Dict]) -> Dict:
        """Generate download summary statistics"""
        total_videos = len(results)
        successful_metadata = sum(1 for r in results.values() if r.get('metadata', False))
        successful_thumbnails = sum(1 for r in results.values() if r.get('thumbnail', False))
        successful_videos = sum(1 for r in results.values() if r.get('video', False))
        
        return {
            'total_videos': total_videos,
            'successful_metadata': successful_metadata,
            'successful_thumbnails': successful_thumbnails,
            'successful_videos': successful_videos,
            'fully_successful': sum(1 for r in results.values() if all(r.values())),
            'download_directory': str(self.base_download_dir.absolute())
        }


# Integration class that combines parsing and downloading
class VideoParserAndDownloader:
    """
    Combined class that handles both parsing and downloading of videos.
    Provides option to parse all first or process each video immediately.
    """
    
    def __init__(self, base_url: str, download_dir: str = "downloads", max_concurrent: int = 5):
        self.parser = None  # Will be initialized with OptimizedVideoDataParser
        self.downloader = OptimalMediaDownloader(download_dir, max_concurrent)
        self.base_url = base_url
    
    async def parse_and_download_all(self, parse_first: bool = True) -> Dict:
        """
        Parse and download all videos.
        
        Args:
            parse_first: If True, parse all videos first then download.
                        If False, parse and download each video immediately.
        
        Returns:
            Dict with parsing and download results
        """
        # Note: This would require the OptimizedVideoDataParser to be imported
        # from optimzed_parser import OptimizedVideoDataParser
        # self.parser = OptimizedVideoDataParser(self.base_url)
        
        print(f"🎬 Starting {'parse-first' if parse_first else 'streaming'} approach")
        
        if parse_first:
            return await self._parse_first_then_download()
        else:
            return await self._stream_parse_and_download()
    
    async def _parse_first_then_download(self) -> Dict:
        """Parse all videos first, then download all files"""
        print("📋 Phase 1: Parsing all video metadata...")
        
        # Parse all videos (this would require the parser)
        # await self.parser.extract_video_urls()
        # videos_data = await self.parser.parse_all_videos()
        
        # For now, assume we have the data
        videos_data = []  # Would be populated by parser
        
        if not videos_data:
            print("❌ No videos found to download")
            return {"error": "No videos found"}
        
        print(f"✅ Parsed {len(videos_data)} videos")
        print("📥 Phase 2: Downloading all media files...")
        
        # Download all files
        async with self.downloader as downloader:
            download_results = await downloader.download_all_videos(videos_data)
        
        return {
            "approach": "parse_first",
            "parsed_videos": len(videos_data),
            "download_results": download_results,
            "summary": self.downloader.get_download_summary(download_results)
        }
    
    async def _stream_parse_and_download(self) -> Dict:
        """Parse and download each video immediately (streaming approach)"""
        print("🌊 Streaming approach: Parse and download each video immediately")
        
        # This would require integration with the parser's individual video parsing
        # The parser would yield each video as it's parsed, and we'd download immediately
        
        results = {
            "approach": "streaming",
            "download_results": {},
            "summary": {}
        }
        
        # Implementation would go here
        print("⚠️ Streaming approach requires parser integration")
        
        return results


# Usage example and main execution
async def main():
    """Example usage of the media downloader"""
    
    # Example video data (would come from the parser)
    sample_videos = [
        {
            "video_id": "4029101",
            "url": "https://rule34video.com/video/4029101/creature-cumforts-s01e11/",
            "title": "Creature Cumforts - S01E11",
            "thumbnail_src": "https://rule34video.com/contents/videos_screenshots/4029000/4029101/preview.jpg",
            "video_src": "https://rule34video.com/get_file/51/28df50ff460fd71c08839640bb4872f2ed1e45540f/4029000/4029101/4029101_720p.mp4/?br=2103",
            "duration": "6:48",
            "views": 1194,
            "description": "Sample description...",
            "uploaded_by": "Moomoo Taboo"
        }
    ]
    
    # Download videos
    async with OptimalMediaDownloader(base_download_dir="downloads", max_concurrent=3) as downloader:
        results = await downloader.download_all_videos(sample_videos)
        summary = downloader.get_download_summary(results)
        
        print("\n📋 FINAL SUMMARY:")
        for key, value in summary.items():
            print(f"   {key}: {value}")

if __name__ == "__main__":
    asyncio.run(main())