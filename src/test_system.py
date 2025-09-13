"""
Simple test script for the FikFap system
Tests individual components
"""

import asyncio
import json
from pathlib import Path

async def test_config():
    """Test configuration loading"""
    print("🧪 Testing configuration...")

    try:
        with open('config.json', 'r') as f:
            config = json.load(f)

        required_keys = ['base_url', 'download_folder', 'm3u8_settings']
        for key in required_keys:
            if key not in config:
                print(f"❌ Missing config key: {key}")
                return False

        print("✅ Configuration test passed")
        return True
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

async def test_folder_creation():
    """Test folder structure creation"""
    print("🧪 Testing folder creation...")

    try:
        from m3u8_downloader import FolderManager

        # Test folder creation
        folder_manager = FolderManager("test_downloads")
        post_folder = folder_manager.create_post_folder("test_post")
        quality_folder = folder_manager.create_quality_folder(post_folder, "720p")

        # Check if folders exist
        if post_folder.exists() and quality_folder.exists():
            print("✅ Folder creation test passed")

            # Cleanup
            import shutil
            shutil.rmtree("test_downloads", ignore_errors=True)
            return True
        else:
            print("❌ Folders not created properly")
            return False

    except Exception as e:
        print(f"❌ Folder creation test failed: {e}")
        return False

async def test_playlist_parsing():
    """Test M3U8 playlist parsing"""
    print("🧪 Testing playlist parsing...")

    try:
        from m3u8_downloader import PlaylistParser

        # Sample M3U8 master playlist
        sample_master = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=1280x720,CODECS="avc1.64001f"
720p/video.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1920x1080,CODECS="avc1.640028"
1080p/video.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2000000,RESOLUTION=1280x720,CODECS="vp09.00.10.08"
vp9_720p/video.m3u8
"""

        parser = PlaylistParser()
        streams = parser.parse_master_playlist(sample_master)

        if len(streams) == 3:
            print(f"✅ Playlist parsing test passed - found {len(streams)} streams")
            return True
        else:
            print(f"❌ Expected 3 streams, found {len(streams)}")
            return False

    except Exception as e:
        print(f"❌ Playlist parsing test failed: {e}")
        return False

async def test_codec_filtering():
    """Test codec filtering"""
    print("🧪 Testing codec filtering...")

    try:
        from m3u8_downloader import CodecFilter

        # Sample streams
        streams = [
            {'codecs': 'avc1.64001f', 'resolution': '720p'},
            {'codecs': 'vp09.00.10.08', 'resolution': '720p'},
            {'codecs': 'avc1.640028', 'resolution': '1080p'}
        ]

        # Test VP9 exclusion
        filter_vp9 = CodecFilter(exclude_vp9=True)
        filtered = filter_vp9.filter_streams(streams)

        if len(filtered) == 2:  # Should exclude 1 VP9 stream
            print("✅ Codec filtering test passed")
            return True
        else:
            print(f"❌ Expected 2 streams after filtering, got {len(filtered)}")
            return False

    except Exception as e:
        print(f"❌ Codec filtering test failed: {e}")
        return False

async def run_all_tests():
    """Run all tests"""
    print("🧪 RUNNING SYSTEM TESTS")
    print("="*50)

    tests = [
        test_config,
        test_folder_creation, 
        test_playlist_parsing,
        test_codec_filtering
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if await test():
            passed += 1
        print()

    print("="*50)
    print(f"📊 RESULTS: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False

if __name__ == "__main__":
    asyncio.run(run_all_tests())