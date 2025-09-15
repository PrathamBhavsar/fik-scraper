import asyncio
import json
from pathlib import Path

# Import our simple modules
from parser import VideoParser
from downloader import VideoDownloader

async def main():
    """
    Complete video processing pipeline:
    1. Parse all videos from the website
    2. Download all videos, thumbnails, and metadata
    """

    print("🎬 Video Parser & Downloader")
    print("=" * 50)
    print("🌐 Website: https://rule34video.com")
    print("🎯 Target: ALL videos on homepage")
    print("📁 Output: downloads/ folder")
    print("=" * 50)

    # Step 1: Parse all videos
    print("\n🔍 STEP 1: PARSING VIDEOS")
    print("-" * 30)

    parser = VideoParser("https://rule34video.com/")

    # Get video URLs
    await parser.get_video_urls()

    if not parser.video_urls:
        print("❌ No videos found!")
        return

    # Parse all videos
    await parser.parse_all_videos()

    if not parser.parsed_videos:
        print("❌ No videos could be parsed!")
        return

    # Save parsed data
    parser.save_data("videos.json")

    # Step 2: Download all videos
    print("\n📥 STEP 2: DOWNLOADING VIDEOS")
    print("-" * 30)

    async with VideoDownloader(download_dir="downloads", max_concurrent=5) as downloader:
        results = await downloader.download_all_videos(parser.parsed_videos)

    # Final report
    print("\n" + "=" * 50)
    print("🎯 FINAL REPORT")
    print("=" * 50)

    total_videos = len(parser.parsed_videos)
    successful_downloads = sum(1 for r in results.values() if any(r.values()))

    print(f"📊 Videos found: {len(parser.video_urls)}")
    print(f"📝 Videos parsed: {len(parser.parsed_videos)}")
    print(f"✅ Videos downloaded: {successful_downloads}")
    print(f"📁 Files saved to: {Path('downloads').absolute()}")

    success_rate = (successful_downloads / total_videos) * 100 if total_videos > 0 else 0
    print(f"🎯 Success rate: {success_rate:.1f}%")

    if success_rate == 100:
        print("\n🎉 ALL VIDEOS DOWNLOADED SUCCESSFULLY!")
    else:
        print(f"\n⚠️ {total_videos - successful_downloads} videos had issues")

    print("=" * 50)

if __name__ == "__main__":
    """
    Run the complete video processing pipeline.

    Requirements:
    - parser.py (video parsing)
    - downloader.py (video downloading)
    - playwright (pip install playwright)
    - aiohttp (pip install aiohttp)
    - aiofiles (pip install aiofiles)

    Usage:
    python main.py

    This will:
    1. Parse ALL videos from rule34video.com homepage
    2. Download highest quality video files
    3. Download thumbnail images
    4. Save metadata as JSON files
    5. Organize everything in downloads/ folder
    """

    print("🚀 Starting complete video processing...")
    print("⚠️ This will download ALL videos from the homepage")
    print("📦 Make sure you have enough disk space")
    print("⏱️ This may take a while depending on video count and sizes")
    print()

    try:
        asyncio.run(main())
        print("\n✅ Processing completed!")
    except KeyboardInterrupt:
        print("\n⏹️ Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
