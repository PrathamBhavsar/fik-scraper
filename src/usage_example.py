"""
Fixed Usage Example for IDM Video Downloader

This file shows how to use the FIXED IDM integration system with proper
directory structure and enhanced error handling.

FIXES APPLIED:
- Enhanced IDM executable detection
- Proper Windows path formatting  
- Directory pre-creation
- Better error handling
- Optimized IDM command parameters
"""

import asyncio
from idm_manager import FixedVideoIDMProcessor, FixedIDMManager

async def fixed_example():
    """Fixed example of using the IDM integration system."""

    # Configuration - CHANGE THESE VALUES
    BASE_URL = "https://rule34video.com"  # Your target website
    DOWNLOAD_DIR = "my_downloads"         # Where to save files (will create video_id subfolders)
    
    print("🔧 FIXED IDM Video Downloader")
    print("="*50)
    print("🎯 Expected structure:")
    print("   my_downloads/")
    print("   ├── video_id_1/")
    print("   │   ├── video_id_1.mp4")
    print("   │   ├── video_id_1.jpg")  
    print("   │   └── video_id_1.json")
    print("   └── video_id_2/")
    print("       ├── video_id_2.mp4")
    print("       ├── video_id_2.jpg")
    print("       └── video_id_2.json")
    print("="*50)

    # Create the fixed processor
    processor = FixedVideoIDMProcessor(
        base_url=BASE_URL,
        download_dir=DOWNLOAD_DIR,
        idm_path=None  # Auto-detect IDM
    )

    # Process all videos
    results = await processor.process_all_videos()

    if results["success"]:
        print("\n✅ Success! Check IDM for downloads.")
        print(f"📊 Found {results['videos_parsed']} videos")
        print(f"📁 Files will be saved to: {results['download_directory']}")
        print("🎯 Each video gets its own folder with video_id as folder name")
    else:
        print(f"\n❌ Error: {results['error']}")


async def manual_fixed_example():
    """Example of manually controlling the FIXED IDM manager."""

    # Sample video data (normally this comes from the parser)
    sample_videos = [
        {
            "video_id": "test123",
            "title": "Test Video",
            "thumbnail_src": "https://rule34video.com/contents/videos_screenshots/4029000/4029741/preview.jpg",
            "video_src": "https://rule34video.com/get_file/54/94394e085a92a7133a26ad559405eea59ad0ccde9c/4029000/4029741/4029741_1080p.mp4/?download_filename=rueka-inaba-hypnosis-pet-cafe_1080p.mp4&br=6453"
        }
    ]

    print("🔧 Manual FIXED IDM Manager Test")
    print("="*40)
    
    # Create fixed IDM manager
    idm = FixedIDMManager(
        base_download_dir="manual_downloads",
    )

    # Process videos with the fixed system
    results = idm.process_all_videos(sample_videos, start_queue=True)

    print(f"\n📊 RESULTS:")
    print(f"✅ Processed videos: {results['successful_additions']}")
    print(f"❌ Failed videos: {results['failed_additions']}")
    print(f"📂 Directories created: {results['directories_created']}")
    print(f"📥 Queue items: {results['download_queue_size']}")
    print(f"🚀 Queue started: {results['queue_started']}")
    print(f"📁 Directory: {results['download_directory']}")


async def test_idm_detection():
    """Test IDM detection and validation."""
    
    print("🔍 Testing IDM Detection...")
    print("="*30)
    
    # Create manager to test detection
    idm = FixedIDMManager("test_downloads")
    
    print(f"IDM Path: {idm.idm_path}")
    print(f"Base Dir: {idm.base_download_dir}")
    
    # Get queue info
    queue_info = idm.get_queue_info()
    print(f"Queue Info: {queue_info}")


if __name__ == "__main__":
    print("🎬 FIXED IDM Integration System")
    print("=" * 50)
    print("🔧 Major fixes applied:")
    print("   - Better IDM executable detection")
    print("   - Proper Windows path handling") 
    print("   - Directory pre-creation")
    print("   - Enhanced error reporting")
    print("   - Optimized IDM commands")
    print("=" * 50)
    print()
    
    # Choose which example to run:
    
    print("1. Testing IDM detection...")
    asyncio.run(test_idm_detection())
    
    print("\n2. Running fixed example...")
    # Run the fixed example
    asyncio.run(fixed_example())

    # Uncomment to run manual example instead:
    # print("\n3. Running manual example...")
    # asyncio.run(manual_fixed_example())