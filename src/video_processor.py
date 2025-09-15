# Integration file that combines the OptimizedVideoDataParser with OptimalMediaDownloader
# This provides a complete solution for parsing video metadata and downloading media files
# FIXED: SSL certificate verification issues

import asyncio
import sys
from pathlib import Path

# Import the parser (assuming it's in the same directory)
try:
    from src.main import OptimizedVideoDataParser
except ImportError:
    print("❌ Could not import OptimizedVideoDataParser from optimzd_parser.py")
    print("   Please ensure optimzd_parser.py is in the same directory")
    sys.exit(1)

# Import the FIXED downloader
try:
    from src.media_downloader import OptimalMediaDownloader
except ImportError:
    print("❌ Could not import OptimalMediaDownloader from optimal_media_downloader.py")
    print("   Please ensure optimal_media_downloader.py is in the same directory")
    sys.exit(1)


class CompleteVideoProcessor:
    """
    Complete video processing solution that combines parsing and downloading.
    FIXED: SSL certificate verification issues for downloads
    
    Provides two processing strategies:
    1. Parse All First: Parse all video metadata, then download all files
    2. Stream Processing: Parse and download each video immediately
    """
    
    def __init__(self, base_url: str, download_dir: str = "downloads", max_concurrent: int = 5):
        """
        Initialize the complete video processor.
        
        Args:
            base_url: Base URL of the video site
            download_dir: Directory to save downloaded files
            max_concurrent: Maximum concurrent downloads
        """
        self.base_url = base_url
        self.download_dir = download_dir
        self.max_concurrent = max_concurrent
        self.parser = OptimizedVideoDataParser(base_url)
        self.downloader = OptimalMediaDownloader(download_dir, max_concurrent)
        
    async def process_all_videos(self, strategy: str = "parse_first") -> dict:
        """
        Process all videos using the specified strategy.
        
        Args:
            strategy: Either "parse_first" or "stream"
                     "parse_first": Parse all videos first, then download all files
                     "stream": Parse and download each video immediately
        
        Returns:
            Dict with processing results and statistics
        """
        
        print(f"🎬 Starting complete video processing with '{strategy}' strategy")
        print(f"🌐 Source URL: {self.base_url}")
        print(f"📁 Download directory: {Path(self.download_dir).absolute()}")
        print(f"⚡ Max concurrent downloads: {self.max_concurrent}")
        print(f"🔒 SSL verification: DISABLED (for compatibility)")
        print("="*80)
        
        if strategy == "parse_first":
            return await self._parse_first_strategy()
        elif strategy == "stream":
            return await self._stream_strategy()
        else:
            raise ValueError("Strategy must be either 'parse_first' or 'stream'")
    
    async def _parse_first_strategy(self) -> dict:
        """
        Strategy 1: Parse all video metadata first, then download all files.
        
        Advantages:
        - Can validate all data before downloading
        - Better for planning storage requirements
        - Easier progress tracking
        - Can prioritize downloads based on metadata
        
        Returns:
            Dict with complete processing results
        """
        
        print("📋 PHASE 1: Extracting video URLs...")
        
        # Extract video URLs from main page
        video_urls = await self.parser.extract_video_urls()
        
        if not video_urls:
            return {
                "success": False,
                "error": "No video URLs found",
                "strategy": "parse_first"
            }
        
        print(f"✅ Found {len(video_urls)} video URLs")
        print("\n📝 PHASE 2: Parsing all video metadata...")
        
        # Parse all video metadata
        videos_data = await self.parser.parse_all_videos()
        
        if not videos_data:
            return {
                "success": False,
                "error": "No video metadata could be parsed",
                "strategy": "parse_first",
                "urls_found": len(video_urls)
            }
        
        print(f"✅ Successfully parsed {len(videos_data)} videos")
        
        # Filter out videos without required download URLs
        downloadable_videos = []
        for video in videos_data:
            if video.get('thumbnail_src') or video.get('video_src'):
                downloadable_videos.append(video)
            else:
                print(f"⚠️ Skipping video {video.get('video_id', 'unknown')} - no download URLs")
        
        if not downloadable_videos:
            return {
                "success": False,
                "error": "No videos have downloadable content",
                "strategy": "parse_first",
                "parsed_count": len(videos_data)
            }
        
        print(f"📥 PHASE 3: Downloading media files for {len(downloadable_videos)} videos...")
        
        # Download all media files with SSL fix
        async with self.downloader as downloader:
            download_results = await downloader.download_all_videos(downloadable_videos)
        
        # Generate final summary
        summary = self.downloader.get_download_summary(download_results)
        
        return {
            "success": True,
            "strategy": "parse_first",
            "urls_found": len(video_urls),
            "videos_parsed": len(videos_data),
            "videos_downloadable": len(downloadable_videos),
            "download_results": download_results,
            "summary": summary,
            "download_directory": str(Path(self.download_dir).absolute())
        }
    
    async def _stream_strategy(self) -> dict:
        """
        Strategy 2: Parse and download each video immediately (streaming).
        
        Advantages:
        - Lower memory usage
        - Starts downloading immediately
        - More resilient to individual failures
        - Better for very large video lists
        
        Returns:
            Dict with complete processing results
        """
        
        print("🌊 STREAMING STRATEGY: Parse and download each video immediately")
        
        # Extract video URLs from main page
        print("📋 Extracting video URLs...")
        video_urls = await self.parser.extract_video_urls()
        
        if not video_urls:
            return {
                "success": False,
                "error": "No video URLs found",
                "strategy": "stream"
            }
        
        print(f"✅ Found {len(video_urls)} video URLs")
        print(f"🌊 Processing videos in streaming mode...")
        
        # Process videos one by one
        results = {}
        successful_downloads = 0
        failed_downloads = 0
        
        async with self.downloader as downloader:
            for i, video_url in enumerate(video_urls, 1):
                try:
                    print(f"\n🎬 Processing video {i}/{len(video_urls)}: {video_url}")
                    
                    # Parse individual video
                    video_data = await self.parser.parse_individual_video(video_url)
                    
                    if not video_data:
                        print(f"❌ Failed to parse video {i}")
                        failed_downloads += 1
                        continue
                    
                    # Check if video has downloadable content
                    if not video_data.get('thumbnail_src') and not video_data.get('video_src'):
                        print(f"⚠️ Video {video_data.get('video_id', 'unknown')} has no downloadable content")
                        failed_downloads += 1
                        continue
                    
                    # Download immediately
                    download_result = await downloader.download_video_files(video_data)
                    
                    video_id = video_data.get('video_id', f'unknown_{i}')
                    results[video_id] = download_result
                    
                    if any(download_result.values()):
                        successful_downloads += 1
                    else:
                        failed_downloads += 1
                    
                    # Show progress
                    progress = (i / len(video_urls)) * 100
                    print(f"📈 Overall progress: {i}/{len(video_urls)} ({progress:.1f}%)")
                    
                except Exception as e:
                    print(f"❌ Error processing video {i}: {e}")
                    failed_downloads += 1
                    continue
        
        # Generate summary
        summary = self.downloader.get_download_summary(results)
        
        return {
            "success": True,
            "strategy": "stream",
            "urls_found": len(video_urls),
            "successful_downloads": successful_downloads,
            "failed_downloads": failed_downloads,
            "download_results": results,
            "summary": summary,
            "download_directory": str(Path(self.download_dir).absolute())
        }
    
    def print_final_report(self, results: dict):
        """Print a comprehensive final report of the processing results"""
        
        print("\n" + "="*80)
        print("🎯 COMPLETE VIDEO PROCESSING REPORT")
        print("="*80)
        
        if not results.get("success"):
            print("❌ PROCESSING FAILED")
            print(f"Error: {results.get('error', 'Unknown error')}")
            return
        
        strategy = results.get("strategy", "unknown")
        summary = results.get("summary", {})
        
        print(f"📊 Strategy Used: {strategy.upper()}")
        print(f"🌐 Source URL: {self.base_url}")
        print(f"📁 Download Directory: {results.get('download_directory', 'Unknown')}")
        
        print(f"\n📈 PROCESSING STATISTICS:")
        print(f"   🔍 URLs Found: {results.get('urls_found', 0)}")
        if strategy == "parse_first":
            print(f"   📝 Videos Parsed: {results.get('videos_parsed', 0)}")
            print(f"   📥 Videos Downloadable: {results.get('videos_downloadable', 0)}")
        
        print(f"\n💾 DOWNLOAD STATISTICS:")
        print(f"   📁 Total Videos: {summary.get('total_videos', 0)}")
        print(f"   📄 Metadata Files: {summary.get('successful_metadata', 0)}")
        print(f"   🖼️  Thumbnail Files: {summary.get('successful_thumbnails', 0)}")
        print(f"   🎥 Video Files: {summary.get('successful_videos', 0)}")
        print(f"   ✅ Fully Complete: {summary.get('fully_successful', 0)}")
        
        success_rate = 0
        if summary.get('total_videos', 0) > 0:
            success_rate = (summary.get('fully_successful', 0) / summary.get('total_videos', 1)) * 100
        
        print(f"\n🎯 SUCCESS RATE: {success_rate:.1f}%")
        
        if success_rate < 100:
            failed_videos = summary.get('total_videos', 0) - summary.get('fully_successful', 0)
            print(f"⚠️  {failed_videos} videos had partial or complete failures")
        
        print(f"\n📁 Files saved to: {summary.get('download_directory', 'Unknown')}")
        print("="*80)


async def main():
    """
    Main execution function with example usage.
    Modify the URL and settings as needed for your use case.
    """
    
    # Configuration
    BASE_URL = "https://rule34video.com"  # Change this to your target URL
    DOWNLOAD_DIR = "downloads"            # Directory to save files
    MAX_CONCURRENT = 3                    # Reduced for better SSL reliability
    STRATEGY = "parse_first"              # "parse_first" or "stream"
    
    print("🔒 SSL CERTIFICATE FIX APPLIED!")
    print("   - Certificate verification disabled")
    print("   - Custom SSL context configured")
    print("   - Enhanced error handling for SSL issues")
    print("")
    
    # Create processor
    processor = CompleteVideoProcessor(
        base_url=BASE_URL,
        download_dir=DOWNLOAD_DIR,
        max_concurrent=MAX_CONCURRENT
    )
    
    try:
        # Process all videos
        results = await processor.process_all_videos(strategy=STRATEGY)
        
        # Print comprehensive report
        processor.print_final_report(results)
        
        # Return results for further processing if needed
        return results
        
    except KeyboardInterrupt:
        print("\n⏹️  Processing interrupted by user")
        return {"success": False, "error": "Interrupted by user"}
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    """
    Run the complete video processor with SSL fixes.
    
    Usage:
    python complete_video_processor.py
    
    Make sure you have:
    1. optimzd_parser.py in the same directory
    2. optimal_media_downloader.py in the same directory
    3. Required packages: aiohttp, aiofiles, playwright
    """
    
    print("🎬 Complete Video Processor (SSL FIXED)")
    print("=" * 60)
    print("🔒 SSL Certificate Issues: RESOLVED")
    print("✅ Downloads will work with certificate problems")
    print("=" * 60)
    print("This tool will:")
    print("1. Extract video URLs from the website")
    print("2. Parse video metadata")
    print("3. Download thumbnails and videos (SSL SAFE)")
    print("4. Organize files in structured directories")
    print("=" * 60)
    
    # Run the processor
    results = asyncio.run(main())
    
    if results and results.get("success"):
        print("\n✅ Processing completed successfully!")
    else:
        print("\n❌ Processing failed or was incomplete.")
        sys.exit(1)