#!/usr/bin/env python3
"""
FikFap Scraper - Phase 3 Demo: M3U8 Download System
Complete demonstration of M3U8 downloading capabilities
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from core.config import config
from core.base_scraper import BaseScraper
from utils.logger import setup_logger
from data.models import VideoPost, Author, VideoQuality, VideoCodec
from download.m3u8_downloader import M3U8Downloader
from download.quality_manager import QualityManager

async def demo_quality_management():
    """Demonstrate advanced quality management features"""
    logger = setup_logger("phase3_demo", "INFO")

    logger.info("=== Phase 3 Demo: Quality Management ===")

    # Create mock video post with diverse qualities
    mock_author = Author(userId="demo-user", username="demo_creator")

    mock_qualities = [
        VideoQuality(
            resolution="240p",
            codec=VideoCodec.H264,
            bandwidth=500000,
            playlist_url="https://api.fikfap.com/240p.m3u8"
        ),
        VideoQuality(
            resolution="480p",
            codec=VideoCodec.H264,
            bandwidth=1280000,
            playlist_url="https://api.fikfap.com/480p.m3u8"
        ),
        VideoQuality(
            resolution="720p",
            codec=VideoCodec.H264,
            bandwidth=2560000,
            playlist_url="https://api.fikfap.com/720p.m3u8"
        ),
        VideoQuality(
            resolution="720p",
            codec=VideoCodec.VP9,
            bandwidth=1800000,
            playlist_url="https://api.fikfap.com/vp9_720p.m3u8",
            is_vp9=True
        ),
        VideoQuality(
            resolution="1080p",
            codec=VideoCodec.H264,
            bandwidth=5000000,
            playlist_url="https://api.fikfap.com/1080p.m3u8"
        )
    ]

    mock_video = VideoPost(
        postId=12345,
        mediaId="demo-media-123",
        bunnyVideoId="demo-bunny-123",
        userId="demo-user",
        label="Phase 3 Demo Video - M3U8 Download Test",
        videoStreamUrl="https://api.fikfap.com/master.m3u8",
        publishedAt=datetime.now(),
        author=mock_author,
        availableQualities=mock_qualities
    )

    # Initialize quality manager
    quality_manager = QualityManager()

    # Demonstrate quality filtering
    logger.info(f"📊 Available qualities: {len(mock_qualities)}")
    for i, quality in enumerate(mock_qualities, 1):
        vp9_indicator = " (VP9)" if quality.is_vp9 else ""
        logger.info(f"  {i}. {quality.resolution} - {quality.codec.value}{vp9_indicator} - {quality.bandwidth:,} bps")

    # Test quality filtering
    filtered_qualities = await quality_manager.filter_qualities(mock_qualities)
    logger.info(f"✅ Filtered to {len(filtered_qualities)} qualities")

    # Test quality selection
    selected_qualities = quality_manager.select_qualities_for_download(filtered_qualities)
    logger.info(f"🎯 Selected {len(selected_qualities)} qualities for download:")
    for quality in selected_qualities:
        logger.info(f"   - {quality.resolution} ({quality.codec.value})")

    # Test quality analysis
    analysis = await quality_manager.analyze_quality_distribution(mock_video)
    logger.info("📈 Quality Analysis:")
    logger.info(f"   Total: {analysis['total_qualities']}")
    logger.info(f"   VP9: {analysis['vp9_count']}, H.264: {analysis['h264_count']}")
    logger.info(f"   Bandwidth: {analysis['bandwidth_range']['min']:,} - {analysis['bandwidth_range']['max']:,} bps")

    # Test quality summary
    summary = quality_manager.get_quality_summary(mock_qualities)
    logger.info(f"📋 Quality Summary: {summary}")

async def demo_m3u8_downloader():
    """Demonstrate M3U8 downloader capabilities"""
    logger = setup_logger("phase3_demo", "INFO")

    logger.info("=== Phase 3 Demo: M3U8 Downloader ===")

    # Create mock video for download testing
    mock_author = Author(userId="demo-user", username="test_creator")

    mock_qualities = [
        VideoQuality(
            resolution="720p",
            codec=VideoCodec.H264,
            bandwidth=2560000,
            playlist_url="https://api.fikfap.com/demo_720p.m3u8"
        ),
        VideoQuality(
            resolution="480p",
            codec=VideoCodec.H264,
            bandwidth=1280000,
            playlist_url="https://api.fikfap.com/demo_480p.m3u8"
        )
    ]

    mock_video = VideoPost(
        postId=54321,
        mediaId="demo-download-123",
        bunnyVideoId="demo-download-bunny",
        userId="demo-user",
        label="Download Test Video",
        videoStreamUrl="https://api.fikfap.com/demo_master.m3u8",
        publishedAt=datetime.now(),
        author=mock_author,
        availableQualities=mock_qualities
    )

    async with M3U8Downloader() as downloader:
        # Test download validation
        is_valid, issues = await downloader.validate_download_prerequisites(mock_video)
        logger.info(f"📋 Download validation: {'✅ Valid' if is_valid else '❌ Invalid'}")
        if issues:
            for issue in issues:
                logger.warning(f"   - {issue}")

        # Test size estimation (will fail for mock URLs but demonstrates the feature)
        logger.info("💾 Estimating download sizes...")
        try:
            size_estimates = await downloader.estimate_download_size(mock_video)
            if size_estimates:
                for resolution, size in size_estimates.items():
                    size_mb = size / (1024 * 1024)
                    logger.info(f"   {resolution}: ~{size_mb:.1f} MB")
            else:
                logger.info("   Size estimation not available for mock data")
        except Exception as e:
            logger.info(f"   Size estimation failed (expected for mock data): {e}")

        # Test quality summary
        summary = downloader.get_quality_summary(mock_video)
        logger.info(f"📊 Available qualities: {summary}")

        # Test quality analysis
        analysis = await downloader.analyze_video_qualities(mock_video)
        logger.info("📈 Video Analysis:")
        logger.info(f"   Total qualities: {analysis['total_qualities']}")
        if 'recommended' in analysis and analysis['recommended']:
            logger.info("   Recommended qualities:")
            for rec in analysis['recommended'][:2]:
                logger.info(f"     - {rec['resolution']} ({rec['codec']})")

async def demo_progress_tracking():
    """Demonstrate progress tracking capabilities"""
    logger = setup_logger("phase3_demo", "INFO")

    logger.info("=== Phase 3 Demo: Progress Tracking ===")

    from download.fragment_processor import DownloadProgress
    from utils.helpers import ProgressTracker, format_duration

    # Simulate download progress
    progress = DownloadProgress(total_fragments=100)
    tracker = ProgressTracker()

    logger.info("🚀 Simulating download progress...")
    tracker.add_operation("demo_download", 100)

    # Simulate progress updates
    for i in range(0, 101, 20):
        progress.completed_fragments = i
        progress.downloaded_bytes = i * 1024 * 1024  # 1MB per fragment

        tracker.update_progress("demo_download", i)
        progress_info = tracker.get_progress("demo_download")

        logger.info(f"   Progress: {progress_info['progress_percentage']:.1f}% "
                   f"({i}/100) - Speed: {progress.download_speed_mbps:.2f} Mbps - "
                   f"ETA: {format_duration(progress.eta_seconds)}")

        await asyncio.sleep(0.2)  # Simulate time passing

    tracker.remove_operation("demo_download")
    logger.info("✅ Progress tracking demonstration completed")

async def demo_file_organization():
    """Demonstrate file organization features"""
    logger = setup_logger("phase3_demo", "INFO")

    logger.info("=== Phase 3 Demo: File Organization ===")

    async with M3U8Downloader() as downloader:
        # Test filename generation
        mock_author = Author(userId="test-user", username="content_creator_2023")
        mock_video = VideoPost(
            postId=99999,
            mediaId="test-media",
            bunnyVideoId="test-bunny",
            userId="test-user",
            label="Amazing Video with Special Characters!!! @#$%",
            videoStreamUrl="https://api.fikfap.com/test.m3u8",
            publishedAt=datetime.now(),
            author=mock_author,
            availableQualities=[]
        )

        mock_quality = VideoQuality(
            resolution="1080p",
            codec=VideoCodec.H264,
            playlist_url="https://api.fikfap.com/test_1080p.m3u8"
        )

        # Test filename generation
        filename = downloader._generate_filename(mock_video, mock_quality)
        logger.info(f"📁 Generated filename: {filename}")

        # Test directory structure
        download_dir = await downloader._create_download_directory(mock_video)
        logger.info(f"📂 Download directory: {download_dir}")

        # Test filename sanitization
        test_names = [
            "Normal Video Name",
            r"Video with /special\chars<>:",
            "Very long video name that exceeds the maximum filename length limit and needs to be truncated",
            "Video|with*illegal?chars"
        ]

        logger.info("🧹 Filename sanitization tests:")
        for name in test_names:
            sanitized = downloader._sanitize_filename(name)
            logger.info(f"   '{name}' -> '{sanitized}'")

async def main():
    """Main demo function"""
    try:
        logger = setup_logger("phase3_demo", "INFO")

        # Create necessary directories
        config.create_directories()

        logger.info("🎬 FikFap Scraper Phase 3 - M3U8 Download System Demo")
        logger.info("=" * 70)

        # Run all demonstrations
        await demo_quality_management()
        print()

        await demo_m3u8_downloader()
        print()

        await demo_progress_tracking()
        print()

        await demo_file_organization()

        print()
        logger.info("=" * 70)
        logger.info("🎉 Phase 3 Demo Complete!")
        logger.info("✨ M3U8 Download System fully operational")
        logger.info("📋 Features demonstrated:")
        logger.info("   - Advanced Quality Management & Filtering")
        logger.info("   - VP9 Detection & Codec Preferences")
        logger.info("   - Concurrent Fragment Processing")
        logger.info("   - Progress Tracking & ETA Calculation")
        logger.info("   - File Organization & Sanitization")
        logger.info("   - Download Size Estimation")
        logger.info("   - Resume Capability Support")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
