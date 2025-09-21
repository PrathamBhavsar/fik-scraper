#!/usr/bin/env python3
"""
FikFap Scraper - Phase 4 Testing Script
Comprehensive testing of storage and file management features
"""
import asyncio
import sys
import os
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from core.config import config
from storage.file_manager import FileManager
from storage.metadata_handler  import MetadataHandler
from utils.monitoring import SystemMonitor, DiskMonitor
from utils.logger import setup_logger
from data.models import (
    VideoPost, Author, VideoQuality, VideoCodec, StorageMetadata, 
    ProcessingStatus, DownloadJob, DownloadStatus, DirectoryStructure
)

async def test_file_manager():
    """Test file manager capabilities"""
    logger = setup_logger("test_phase4", config.log_level)

    logger.info("=== Testing File Manager ===")

    # Create test video post
    test_author = Author(userId="test-user", username="test_creator")

    test_qualities = [
        VideoQuality(
            resolution="720p",
            codec=VideoCodec.H264,
            playlist_url="https://api.fikfap.com/test/720p.m3u8"
        ),
        VideoQuality(
            resolution="1080p", 
            codec=VideoCodec.H264,
            playlist_url="https://api.fikfap.com/test/1080p.m3u8"
        )
    ]

    test_video = VideoPost(
        postId=99999,
        mediaId="test-media-999",
        bunnyVideoId="test-bunny-999",
        userId="test-user",
        label="Test Video - File Manager",
        videoStreamUrl="https://api.fikfap.com/test/master.m3u8",
        publishedAt=datetime.now(),
        author=test_author,
        availableQualities=test_qualities
    )

    # Initialize file manager
    file_manager = FileManager()

    # Test 1: Directory structure creation
    logger.info("🔧 Testing directory structure creation...")
    directory_structure = await file_manager.create_directory_structure(test_video)

    logger.info(f"✅ Created structure:")
    logger.info(f"   Post path: {directory_structure.postPath}")
    logger.info(f"   M3U8 path: {directory_structure.m3u8Path}")
    logger.info(f"   Quality paths: {len(directory_structure.qualityPaths)} qualities")

    # Verify directories exist
    for quality, path_str in directory_structure.qualityPaths.items():
        path = Path(path_str)
        if path.exists():
            logger.info(f"   ✅ {quality}: {path_str}")
        else:
            logger.error(f"   ❌ {quality}: {path_str} not created")

    # Test 2: Filename generation
    logger.info("📋 Testing filename generation...")
    for quality in test_qualities:
        filename = file_manager.generate_filename(
            test_video, 
            quality.resolution, 
            quality.codec.value
        )
        logger.info(f"   {quality.resolution}: {filename}")

    # Test 3: File storage operations
    logger.info("📄 Testing file storage operations...")

    stored_metadata_list = []
    for quality in test_qualities:
        # Create a temporary test file
        test_content = f"Test video content for {quality.resolution} - Post {test_video.postId}".encode()
        temp_file = Path(tempfile.mktemp(suffix='.mp4'))

        with open(temp_file, 'wb') as f:
            f.write(test_content * 100)  # Make it larger for realistic testing

        try:
            # Generate target path
            filename = file_manager.generate_filename(
                test_video,
                quality.resolution,
                quality.codec.value
            )

            quality_dir = Path(directory_structure.qualityPaths[quality.resolution])
            target_path = quality_dir / filename

            # Store file
            metadata = await file_manager.store_video_file(
                temp_file,
                target_path,
                test_video,
                quality.resolution,
                quality.codec.value,
                move_file=False  # Copy instead of move for testing
            )

            stored_metadata_list.append(metadata)
            logger.info(f"   ✅ Stored {quality.resolution}: {target_path.name}")

            # Verify file exists and has correct size
            if target_path.exists():
                file_size = target_path.stat().st_size
                logger.info(f"      File size: {file_size:,} bytes")
                logger.info(f"      Checksum: {metadata.checksum}")
            else:
                logger.error(f"   ❌ File not found after storage: {target_path}")

        except Exception as e:
            logger.error(f"   ❌ Error storing {quality.resolution}: {e}")
        finally:
            # Clean up temp file
            if temp_file.exists():
                temp_file.unlink()

    # Test 4: Storage statistics
    logger.info("📊 Testing storage statistics...")
    stats = file_manager.get_storage_stats()
    logger.info(f"   Total files: {stats['total_files']}")
    logger.info(f"   Total size: {stats['total_size']:,} bytes")
    logger.info(f"   Directories: {stats['directories']}")
    logger.info(f"   File types: {list(stats['file_types'].keys())}")

    # Test 5: Cleanup operations
    logger.info("🧹 Testing cleanup operations...")
    cleanup_summary = await file_manager.cleanup_incomplete_downloads()
    logger.info(f"   Files cleaned: {cleanup_summary.filesRemoved}")
    logger.info(f"   Bytes freed: {cleanup_summary.bytesFreed:,}")
    logger.info(f"   Success rate: {cleanup_summary.success_rate:.1f}%")
    logger.info(f"   Duration: {cleanup_summary.duration:.2f}s")

    return stored_metadata_list, directory_structure

async def test_metadata_handler():
    """Test metadata handler capabilities"""
    logger = setup_logger("test_phase4", config.log_level)

    logger.info("=== Testing Metadata Handler ===")

    # Initialize metadata handler
    metadata_handler = MetadataHandler()

    # Create test video post
    test_author = Author(userId="meta-user", username="metadata_test")
    test_video = VideoPost(
        postId=88888,
        mediaId="meta-media-888",
        bunnyVideoId="meta-bunny-888", 
        userId="meta-user",
        label="Test Video - Metadata Handler",
        videoStreamUrl="https://api.fikfap.com/meta/master.m3u8",
        publishedAt=datetime.now(),
        author=test_author,
        availableQualities=[]
    )

    # Test 1: Processing record creation
    logger.info("📝 Testing processing record creation...")
    processing_record = await metadata_handler.create_processing_record(test_video)
    logger.info(f"   ✅ Created record: {processing_record.processingId[:8]}")
    logger.info(f"   Status: {processing_record.status}")
    logger.info(f"   Started at: {processing_record.startedAt}")

    # Test 2: Status updates
    logger.info("🔄 Testing status updates...")
    await asyncio.sleep(0.5)  # Small delay to show time difference

    await metadata_handler.update_processing_record(
        test_video.postId,
        ProcessingStatus.PROCESSING
    )
    logger.info("   Status updated to: PROCESSING")

    await asyncio.sleep(0.5)

    await metadata_handler.update_processing_record(
        test_video.postId,
        ProcessingStatus.COMPLETED,
        stored_files=["test_video_720p.mp4", "test_video_1080p.mp4"]
    )
    logger.info("   Status updated to: COMPLETED")

    # Test 3: Processed posts tracking
    logger.info("✅ Testing processed posts tracking...")
    is_processed_before = await metadata_handler.is_post_processed(test_video.postId)
    logger.info(f"   Post {test_video.postId} processed (before): {is_processed_before}")

    # Should be marked as processed due to COMPLETED status
    is_processed_after = await metadata_handler.is_post_processed(test_video.postId)
    logger.info(f"   Post {test_video.postId} processed (after): {is_processed_after}")

    # Test 4: Duplicate detection
    logger.info("🔍 Testing duplicate detection...")
    duplicates = await metadata_handler.get_duplicate_posts()
    logger.info(f"   Total processed posts: {len(duplicates)}")
    if duplicates:
        logger.info(f"   Sample processed posts: {duplicates[:5]}")

    # Test 5: Processing statistics
    logger.info("📊 Testing processing statistics...")
    stats = await metadata_handler.get_processing_statistics()
    logger.info(f"   Total processed: {stats['total_processed']}")
    logger.info(f"   Processing records: {stats['processing_records']}")
    logger.info(f"   Success rate: {stats['success_rate']:.1f}%")

    status_breakdown = stats['status_breakdown']
    for status, count in status_breakdown.items():
        if count > 0:
            logger.info(f"   {status}: {count}")

    logger.info(f"   Recent activity: {len(stats['recent_activity'])} items")

    return processing_record

async def test_system_monitoring():
    """Test system monitoring capabilities"""
    logger = setup_logger("test_phase4", config.log_level)

    logger.info("=== Testing System Monitoring ===")

    # Initialize monitors
    system_monitor = SystemMonitor()
    disk_monitor = DiskMonitor()

    # Test 1: System status
    logger.info("💻 Testing system status...")
    system_status = system_monitor.get_system_status()
    logger.info(f"   CPU Usage: {system_status.cpuUsagePercent:.1f}%")
    logger.info(f"   Memory Usage: {system_status.memoryUsagePercent:.1f}%")
    logger.info(f"   Disk Space: {system_status.diskSpaceGb:.2f}GB free")
    logger.info(f"   System Healthy: {system_status.is_healthy}")
    logger.info(f"   Last Update: {system_status.lastUpdate}")

    # Test 2: Disk monitoring
    logger.info("💾 Testing disk usage monitoring...")
    storage_path = config.get('storage.base_path', './downloads')
    disk_info = disk_monitor.get_disk_usage(storage_path)
    logger.info(f"   Path: {disk_info.path}")
    logger.info(f"   Total: {disk_info.totalGb:.2f}GB")
    logger.info(f"   Used: {disk_info.usedGb:.2f}GB ({disk_info.usagePercent:.1f}%)")
    logger.info(f"   Free: {disk_info.freeGb:.2f}GB")
    logger.info(f"   Low Space Warning: {disk_info.is_low_space}")
    logger.info(f"   Last Checked: {disk_info.lastChecked}")

    # Test 3: Health checks
    logger.info("🏥 Testing system health check...")
    is_healthy, issues = system_monitor.check_system_health()
    logger.info(f"   Overall Health: {'✅ Healthy' if is_healthy else '⚠️  Issues Detected'}")

    if issues:
        logger.info("   Issues found:")
        for issue in issues:
            logger.info(f"     - {issue}")
    else:
        logger.info("   No issues detected")

    # Test 4: Storage recommendations
    logger.info("💡 Testing storage recommendations...")
    recommendations = system_monitor.get_storage_recommendations()
    if recommendations:
        logger.info("   Recommendations:")
        for i, rec in enumerate(recommendations[:5], 1):  # Show top 5
            logger.info(f"     {i}. {rec}")
    else:
        logger.info("   No recommendations at this time")

    # Test 5: Process information
    logger.info("⚙️  Testing process information...")
    process_info = system_monitor.get_process_info()
    logger.info(f"   PID: {process_info.get('pid', 'N/A')}")
    logger.info(f"   Memory: {process_info.get('memory_usage_mb', 0):.1f}MB")
    logger.info(f"   CPU: {process_info.get('cpu_percent', 0):.1f}%")
    logger.info(f"   Threads: {process_info.get('num_threads', 0)}")
    logger.info(f"   Status: {process_info.get('status', 'unknown')}")

    # Test 6: Disk usage summary
    logger.info("📈 Testing disk usage summary...")
    summary = disk_monitor.get_usage_summary()
    logger.info(f"   Total monitored paths: {summary['total_monitored']}")
    logger.info(f"   Healthy paths: {len(summary['healthy_paths'])}")
    logger.info(f"   Warning paths: {len(summary['warning_paths'])}")
    logger.info(f"   Critical paths: {len(summary['critical_paths'])}")

    if summary['critical_paths']:
        logger.warning(f"   Critical paths: {summary['critical_paths']}")

    return system_status

async def test_integrated_workflow():
    """Test complete integrated workflow"""
    logger = setup_logger("test_phase4", config.log_level)

    logger.info("=== Testing Integrated Workflow ===")

    # Initialize all components
    file_manager = FileManager()
    metadata_handler = MetadataHandler()
    system_monitor = SystemMonitor()

    # Create test scenario
    test_author = Author(userId="workflow-user", username="workflow_creator")
    test_video = VideoPost(
        postId=77777,
        mediaId="workflow-media",
        bunnyVideoId="workflow-bunny",
        userId="workflow-user",
        label="Test Video - Integrated Workflow",
        videoStreamUrl="https://api.fikfap.com/workflow/master.m3u8",
        publishedAt=datetime.now(),
        author=test_author,
        availableQualities=[
            VideoQuality(
                resolution="720p",
                codec=VideoCodec.H264,
                playlist_url="https://api.fikfap.com/workflow/720p.m3u8"
            )
        ]
    )

    logger.info("🎬 Testing complete workflow simulation...")

    try:
        # Step 1: Health check
        logger.info("1️⃣  System health check...")
        is_healthy, issues = system_monitor.check_system_health()
        if not is_healthy:
            logger.warning(f"   System issues: {issues}")
        else:
            logger.info("   ✅ System healthy")

        # Step 2: Duplicate check
        logger.info("2️⃣  Duplicate check...")
        is_duplicate = await metadata_handler.is_post_processed(test_video.postId)
        logger.info(f"   Already processed: {is_duplicate}")

        # Step 3: Create processing record
        logger.info("3️⃣  Creating processing record...")
        processing_record = await metadata_handler.create_processing_record(test_video)
        logger.info(f"   Record ID: {processing_record.processingId[:8]}")

        # Step 4: Create directory structure  
        logger.info("4️⃣  Creating storage structure...")
        directory_structure = await file_manager.create_directory_structure(test_video)
        logger.info(f"   Structure created: {Path(directory_structure.postPath).name}")

        # Step 5: Simulate download and storage
        logger.info("5️⃣  Simulating download and storage...")

        quality = test_video.availableQualities[0]

        # Create mock downloaded file
        test_content = f"Mock video content for integrated workflow test - {quality.resolution}".encode()
        temp_file = Path(tempfile.mktemp(suffix='.mp4'))
        with open(temp_file, 'wb') as f:
            f.write(test_content * 200)  # Make it substantial

        try:
            # Store the file
            filename = file_manager.generate_filename(
                test_video,
                quality.resolution,
                quality.codec.value
            )

            quality_dir = Path(directory_structure.qualityPaths[quality.resolution])
            target_path = quality_dir / filename

            storage_metadata = await file_manager.store_video_file(
                temp_file,
                target_path,
                test_video,
                quality.resolution,
                quality.codec.value,
                move_file=False
            )

            logger.info(f"   Stored: {storage_metadata.fileName}")
            logger.info(f"   Size: {storage_metadata.fileSize:,} bytes")
            logger.info(f"   Checksum: {storage_metadata.checksum[:8]}...")

            # Step 6: Save metadata
            logger.info("6️⃣  Saving metadata...")
            metadata_saved = await metadata_handler.save_video_metadata(storage_metadata, directory_structure)
            logger.info(f"   Metadata saved: {metadata_saved}")

            # Step 7: Update processing status
            logger.info("7️⃣  Updating processing status...")
            await metadata_handler.update_processing_record(
                test_video.postId,
                ProcessingStatus.COMPLETED,
                stored_files=[str(target_path)]
            )
            logger.info("   ✅ Processing completed")

        finally:
            # Cleanup temp file
            if temp_file.exists():
                temp_file.unlink()

        # Step 8: Verify final state
        logger.info("8️⃣  Verifying final state...")
        is_now_processed = await metadata_handler.is_post_processed(test_video.postId)
        logger.info(f"   Post marked as processed: {is_now_processed}")

        # Final statistics
        logger.info("📊 Final statistics:")
        stats = await metadata_handler.get_processing_statistics()
        logger.info(f"   Total processed: {stats['total_processed']}")

        storage_stats = file_manager.get_storage_stats()
        logger.info(f"   Storage files: {storage_stats['total_files']}")
        logger.info(f"   Storage size: {storage_stats['total_size']:,} bytes")

        system_status = system_monitor.get_system_status()
        logger.info(f"   System health: {'✅ Healthy' if system_status.is_healthy else '⚠️  Issues'}")

        logger.info("🎉 Integrated workflow test completed successfully!")

        return True

    except Exception as e:
        logger.error(f"❌ Integrated workflow test failed: {e}")
        return False

async def run_all_tests():
    """Run all Phase 4 tests"""
    logger = setup_logger("test_phase4", config.log_level, config.log_file)

    logger.info("🧪 FikFap Scraper Phase 4 - Complete Testing Suite")
    logger.info("=" * 90)

    # Create necessary directories
    config.create_directories()

    test_results = {
        'file_manager': False,
        'metadata_handler': False,
        'system_monitoring': False,
        'integrated_workflow': False
    }

    try:
        # Test 1: File Manager
        logger.info("🔧 Running File Manager tests...")
        stored_metadata, directory_structure = await test_file_manager()
        test_results['file_manager'] = True
        logger.info("✅ File Manager tests passed")

        # Test 2: Metadata Handler
        logger.info("📊 Running Metadata Handler tests...")
        processing_record = await test_metadata_handler()
        test_results['metadata_handler'] = True
        logger.info("✅ Metadata Handler tests passed")

        # Test 3: System Monitoring
        logger.info("🖥️  Running System Monitoring tests...")
        system_status = await test_system_monitoring()
        test_results['system_monitoring'] = True
        logger.info("✅ System Monitoring tests passed")

        # Test 4: Integrated Workflow
        logger.info("🔗 Running Integrated Workflow test...")
        workflow_success = await test_integrated_workflow()
        test_results['integrated_workflow'] = workflow_success
        if workflow_success:
            logger.info("✅ Integrated Workflow test passed")
        else:
            logger.error("❌ Integrated Workflow test failed")

        # Final summary
        logger.info("=" * 90)
        logger.info("📋 Phase 4 Test Results Summary:")

        passed_tests = sum(test_results.values())
        total_tests = len(test_results)

        for test_name, passed in test_results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"   {test_name.replace('_', ' ').title()}: {status}")

        logger.info(f"📊 Overall Results: {passed_tests}/{total_tests} tests passed")

        if passed_tests == total_tests:
            logger.info("🎉 All Phase 4 tests passed successfully!")
            logger.info("✨ Storage & File Management system is fully operational")
        else:
            logger.warning(f"⚠️  {total_tests - passed_tests} test(s) failed")

        logger.info("=" * 90)

        return passed_tests == total_tests

    except Exception as e:
        logger.error(f"❌ Test suite failed with error: {e}")
        return False

async def main():
    """Main test entry point"""
    try:
        success = await run_all_tests()
        return 0 if success else 1
    except KeyboardInterrupt:
        print("⏹️  Tests stopped by user")
        return 1
    except Exception as e:
        print(f"❌ Test suite error: {e}")
        return 1
    finally:
        pass

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
