"""
IDM Integration Test Script

This script helps verify that the fixed IDM integration is working correctly
by testing individual components and providing detailed diagnostics.

Use this to troubleshoot any issues before running the full video processor.
"""

import os
import sys
import asyncio
import subprocess
from pathlib import Path
from idm_manager import FixedIDMManager, FixedVideoIDMProcessor

def test_idm_detection():
    """Test IDM executable detection and accessibility."""
    print("🔍 TESTING IDM DETECTION")
    print("=" * 40)
    
    try:
        # Create manager to test detection
        idm = FixedIDMManager("test_detection")
        
        print(f"✅ IDM Manager created successfully")
        print(f"📍 IDM Path: {idm.idm_path}")
        print(f"📁 Base Directory: {idm.base_download_dir}")
        
        # Test if IDM file exists
        if os.path.exists(idm.idm_path):
            print("✅ IDM executable file exists")
        else:
            print("❌ IDM executable file not found")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Error during IDM detection: {e}")
        return False

def test_directory_creation():
    """Test directory creation functionality."""
    print("\n📁 TESTING DIRECTORY CREATION") 
    print("=" * 40)
    
    try:
        idm = FixedIDMManager("test_directories")
        
        # Test creating video directories
        test_video_ids = ["test123", "test456", "test_with_special_chars<>|"]
        
        for video_id in test_video_ids:
            video_dir = idm.create_video_directory(video_id)
            
            if video_dir.exists():
                print(f"✅ Created: {video_dir}")
            else:
                print(f"❌ Failed to create: {video_dir}")
                return False
                
        print(f"📊 Directories created: {idm.stats['directories_created']}")
        return True
        
    except Exception as e:
        print(f"❌ Error during directory creation: {e}")
        return False

def test_idm_command():
    """Test IDM command execution with a dummy URL."""
    print("\n🚀 TESTING IDM COMMAND")
    print("=" * 40)
    print("⚠️ This will add a test entry to your IDM queue (safe to cancel)")
    
    try:
        idm = FixedIDMManager("test_command")
        
        # Create a test directory
        test_dir = idm.create_video_directory("idm_test")
        
        # Test with a dummy URL (this won't actually download anything harmful)
        test_url = "https://httpbin.org/robots.txt"  # Small test file
        test_filename = "test_file.txt"
        
        success = idm.add_to_idm_queue(test_url, test_dir, test_filename)
        
        if success:
            print("✅ IDM command executed successfully")
            print("💡 Check your IDM queue - you should see the test entry")
            print("💡 You can safely delete the test entry from IDM")
            return True
        else:
            print("❌ IDM command failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during IDM command test: {e}")
        return False

def test_sample_video_processing():
    """Test processing a sample video data structure."""
    print("\n🎬 TESTING SAMPLE VIDEO PROCESSING")
    print("=" * 40)
    
    try:
        idm = FixedIDMManager("test_video_processing")
        
        # Create sample video data
        sample_video = {
            "video_id": "sample_123",
            "title": "Sample Test Video",
            "thumbnail_src": "https://httpbin.org/image/jpeg",  # Test image
            "video_src": "https://httpbin.org/drip?duration=1&numbytes=1024"  # Test "video" 
        }
        
        print(f"📝 Processing sample video: {sample_video['title']}")
        
        # Process the video
        results = idm.add_video_to_idm_queue(sample_video)
        
        print(f"📊 Results:")
        print(f"   Metadata: {results['metadata']}")
        print(f"   Thumbnail: {results['thumbnail']}")
        print(f"   Video: {results['video']}")
        
        if any(results.values()):
            print("✅ Sample video processing successful")
            return True
        else:
            print("❌ Sample video processing failed")
            return False
            
    except Exception as e:
        print(f"❌ Error during sample video processing: {e}")
        return False

def cleanup_test_directories():
    """Clean up test directories created during testing."""
    print("\n🧹 CLEANING UP TEST DIRECTORIES")
    print("=" * 40)
    
    test_dirs = [
        "test_detection", 
        "test_directories", 
        "test_command", 
        "test_video_processing"
    ]
    
    for dir_name in test_dirs:
        test_path = Path(dir_name)
        if test_path.exists():
            try:
                import shutil
                shutil.rmtree(test_path)
                print(f"🗑️ Removed: {test_path}")
            except Exception as e:
                print(f"⚠️ Could not remove {test_path}: {e}")
    
    print("✅ Cleanup completed")

def main():
    """Run all tests in sequence."""
    print("🧪 FIXED IDM INTEGRATION - DIAGNOSTIC TESTS")
    print("=" * 60)
    print("This script will test the fixed IDM integration components")
    print("to help identify any issues before running the full system.")
    print("=" * 60)
    
    # Run tests
    tests = [
        ("IDM Detection", test_idm_detection),
        ("Directory Creation", test_directory_creation), 
        ("IDM Command", test_idm_command),
        ("Sample Video Processing", test_sample_video_processing)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🔬 Running {test_name} test...")
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Test {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Print summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    total = len(results)
    
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {test_name}")
        if success:
            passed += 1
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The fixed IDM integration should work correctly.")
        print("💡 You can now run your video downloader with confidence.")
    else:
        print("⚠️ Some tests failed. Please check the error messages above.")
        print("💡 Common issues and solutions:")
        print("   - IDM not installed: Install Internet Download Manager")
        print("   - Permission issues: Run as administrator")
        print("   - Path issues: Check Windows path formatting")
    
    # Cleanup
    cleanup_test_directories()
    
    return passed == total

if __name__ == "__main__":
    print("🔧 Starting diagnostic tests for FIXED IDM integration...")
    success = main()
    
    if success:
        print("\n✅ All diagnostic tests completed successfully!")
        print("🚀 Ready to run the full video downloader system.")
    else:
        print("\n❌ Some diagnostic tests failed.")
        print("🔧 Please fix the issues before running the full system.")
        
    input("\nPress Enter to exit...")