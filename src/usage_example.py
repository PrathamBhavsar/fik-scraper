"""
Simple Usage Example for IDM Video Downloader

This file shows how to use the IDM integration system with minimal setup.
"""

import asyncio
from idm_manger import VideoIDMProcessor, IDMManager


async def simple_example():
    """Simple example of using the IDM integration system."""

    # Configuration - CHANGE THESE VALUES
    BASE_URL = "https://rule34video.com"  # Your target website
    DOWNLOAD_DIR = "my_downloads"         # Where to save files

    print("🎬 Simple IDM Video Downloader")
    print("="*50)

    # Create the processor
    processor = VideoIDMProcessor(
        base_url=BASE_URL,
        download_dir=DOWNLOAD_DIR,
        use_idm_library=False  # Use command line method
    )

    # Process all videos
    results = await processor.process_all_videos()

    if results["success"]:
        print("\n✅ Success! Check IDM for downloads.")
        print(f"📊 Found {results['videos_parsed']} videos")
        print(f"📁 Saved to: {results['download_directory']}")
    else:
        print(f"\n❌ Error: {results['error']}")


async def manual_example():
    """Example of manually controlling the IDM manager."""

    # Sample video data (normally this comes from the parser)
    sample_videos = [
        {
            "video_id": "test123",
            "title": "Test Video",
            "thumbnail_src": "https://rule34video.com/contents/videos_screenshots/4029000/4029741/preview.jpg",
            "video_src": "https://rule34video.com/get_file/54/94394e085a92a7133a26ad559405eea59ad0ccde9c/4029000/4029741/4029741_1080p.mp4/?download_filename=rueka-inaba-hypnosis-pet-cafe_1080p.mp4&br=6453"
        }
    ]

    # Create IDM manager
    idm = IDMManager(
        base_download_dir="manual_downloads",
        use_idm_library=False
    )

    # Process videos
    results = idm.process_all_videos(sample_videos, start_queue=True)

    print(f"✅ Processed {results['successful_additions']} videos")
    print(f"📁 Directory: {results['download_directory']}")


if __name__ == "__main__":
    # Run the simple example
    asyncio.run(simple_example())

    # Uncomment to run manual example instead:
    # asyncio.run(manual_example())
