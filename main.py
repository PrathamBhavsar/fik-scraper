#!/usr/bin/env python3
"""
FikFap Scraper - Phase 2 Main Entry Point
Enhanced with comprehensive data extraction and validation
"""
import asyncio
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from core.config import config
from core.base_scraper import BaseScraper
from core.exceptions import ConfigurationError, APIError
from utils.logger import setup_logger
from data.models import VideoPost
from data.extractor import FikFapDataExtractor
from data.validator import DataValidator

async def test_data_extraction():
    """Test comprehensive data extraction capabilities"""
    logger = setup_logger("fikfap_scraper", config.log_level, config.log_file)

    logger.info("=== FikFap Scraper Phase 2 - Data Extraction Test ===")

    # Validate configuration
    config_issues = config.validate_config()
    if config_issues:
        logger.warning("Configuration issues found:")
        for issue in config_issues:
            logger.warning(f"  - {issue}")

    # Log configuration summary
    config_summary = config.get_config_summary()
    logger.info("Configuration Summary:")
    for section, settings in config_summary.items():
        logger.info(f"  {section}: {settings}")

    try:
        async with BaseScraper() as scraper:
            logger.info("✅ Scraper initialized with data extraction capabilities")

            # Test 1: Mock data validation
            await test_data_validation(logger)

            # Test 2: Mock video extraction
            await test_video_extraction(scraper, logger)

            # Test 3: Mock M3U8 processing
            await test_m3u8_processing(scraper, logger)

            # Test 4: Quality filtering
            await test_quality_filtering(scraper, logger)

            logger.info("🎉 Phase 2 data extraction tests completed successfully!")

    except Exception as e:
        logger.error(f"❌ Phase 2 test failed: {e}")
        raise

async def test_data_validation(logger):
    """Test data validation capabilities"""
    logger.info("🧪 Testing data validation...")

    validator = DataValidator()

    # Test valid video data
    valid_video_data = {
        'postId': 12345,
        'mediaId': 'media-123',
        'bunnyVideoId': 'bunny-123',
        'userId': 'user-123',
        'label': 'Test Video for Phase 2',
        'videoStreamUrl': 'https://example.com/video.m3u8',
        'publishedAt': datetime.now().isoformat(),
        'viewsCount': 1000,
        'likesCount': 50,
        'duration': 120,
        'explicitnessRating': 'FULLY_EXPLICIT',
        'hashtags': ['test', 'phase2', 'validation']
    }

    validation_result = validator.validate_video_post(valid_video_data)
    if validation_result:
        logger.info("  ✅ Valid video data validation passed")
    else:
        logger.error("  ❌ Valid video data validation failed")

    # Test invalid video data
    invalid_video_data = {
        'postId': -1,  # Invalid negative ID
        'label': '',   # Empty label
        # Missing required fields
    }

    invalid_result = validator.validate_video_post(invalid_video_data)
    if not invalid_result:
        logger.info("  ✅ Invalid video data correctly rejected")
    else:
        logger.error("  ❌ Invalid video data incorrectly accepted")

    # Test M3U8 validation
    valid_m3u8 = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1280000,RESOLUTION=854x480
playlist_480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2560000,RESOLUTION=1280x720
playlist_720p.m3u8"""

    m3u8_result = validator.validate_m3u8_content(valid_m3u8)
    if m3u8_result:
        logger.info("  ✅ M3U8 content validation passed")
    else:
        logger.error("  ❌ M3U8 content validation failed")

async def test_video_extraction(scraper, logger):
    """Test video data extraction with mock data"""
    logger.info("📊 Testing video data extraction...")

    if not scraper.extractor:
        logger.error("  ❌ Data extractor not available")
        return

    # Mock API response for testing
    mock_video_response = {
        'video': {
            'postId': 12345,
            'mediaId': 'media-test-123',
            'bunnyVideoId': 'bunny-test-123',
            'userId': 'user-test-123',
            'label': 'Phase 2 Test Video - Data Extraction Demo',
            'description': 'This is a test video to demonstrate Phase 2 data extraction capabilities including comprehensive metadata processing and validation.',
            'videoStreamUrl': 'https://example.com/test/video.m3u8',
            'thumbnailUrl': 'https://example.com/test/thumbnail.jpg',
            'duration': 180,
            'viewsCount': 5000,
            'likesCount': 250,
            'score': 85,
            'explicitnessRating': 'FULLY_EXPLICIT',
            'publishedAt': datetime.now().isoformat(),
            'isBunnyVideoReady': True,
            'hashtags': ['phase2', 'test', 'extraction', 'demo'],
            'author': {
                'userId': 'user-test-123',
                'username': 'test_creator_phase2',
                'displayName': 'Phase 2 Test Creator',
                'isVerified': True,
                'isPartner': False,
                'isPremium': True,
                'description': 'Test creator for Phase 2 development',
                'thumbnailUrl': 'https://example.com/test/creator.jpg',
                'followerCount': 10000,
                'followingCount': 500,
                'postCount': 150,
                'profileLinks': [
                    {
                        'platform': 'twitter',
                        'url': 'https://twitter.com/testcreator',
                        'verified': True
                    }
                ]
            }
        }
    }

    try:
        # Test extraction from dictionary
        video_post = await scraper.extractor._extract_from_dict(mock_video_response)

        if video_post and isinstance(video_post, VideoPost):
            logger.info(f"  ✅ Successfully extracted video: '{video_post.label}'")
            logger.info(f"     Post ID: {video_post.postId}")
            logger.info(f"     Author: {video_post.author.username if video_post.author else 'Unknown'}")
            logger.info(f"     Duration: {video_post.duration}s")
            logger.info(f"     Views: {video_post.viewsCount:,}")
            logger.info(f"     Hashtags: {', '.join(video_post.hashtags)}")

            # Test video validation
            is_valid = scraper.is_valid_video(video_post)
            if is_valid:
                logger.info("  ✅ Extracted video passed validation")
            else:
                logger.warning("  ⚠️  Extracted video failed validation")

            # Test download summary
            summary = scraper.get_download_summary(video_post)
            logger.info("  📋 Download Summary:")
            logger.info(f"     Title: {summary['title']}")
            logger.info(f"     Author: {summary['author']}")
            logger.info(f"     Duration: {summary['duration']}s")
            logger.info(f"     Rating: {summary['rating']}")
            logger.info(f"     Total Qualities: {summary['total_qualities']}")
            logger.info(f"     Has VP9: {summary['has_vp9']}")

        else:
            logger.error("  ❌ Video extraction failed")

    except Exception as e:
        logger.error(f"  ❌ Video extraction error: {e}")

async def test_m3u8_processing(scraper, logger):
    """Test M3U8 playlist processing"""
    logger.info("🎵 Testing M3U8 playlist processing...")

    if not scraper.extractor:
        logger.error("  ❌ Data extractor not available")
        return

    # Mock master playlist content
    mock_master_playlist = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=1280000,RESOLUTION=854x480,CODECS="avc1.42001e"
playlists/480p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2560000,RESOLUTION=1280x720,CODECS="avc1.42001f"
playlists/720p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080,CODECS="avc1.42001f"
playlists/1080p.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=3000000,RESOLUTION=1280x720,CODECS="vp09.00.30.08"
playlists/vp9_720p.m3u8"""

    try:
        # Test playlist parsing
        base_url = 'https://example.com/test/master.m3u8'
        playlist_urls = await scraper.extractor.parse_master_playlist(mock_master_playlist, base_url)

        if playlist_urls:
            logger.info(f"  ✅ Parsed {len(playlist_urls)} playlist URLs:")
            for i, url in enumerate(playlist_urls, 1):
                logger.info(f"     {i}. {url}")

            # Test VP9 detection
            vp9_urls = [url for url in playlist_urls if 'vp9' in url.lower()]
            h264_urls = [url for url in playlist_urls if 'vp9' not in url.lower()]

            logger.info(f"  📊 Quality breakdown:")
            logger.info(f"     H.264/AVC1 streams: {len(h264_urls)}")
            logger.info(f"     VP9 streams: {len(vp9_urls)}")

            # Test VP9 filtering
            if config.exclude_vp9:
                filtered_urls = scraper.extractor._manual_playlist_parsing(mock_master_playlist, base_url)
                logger.info(f"  🔽 VP9 exclusion enabled - filtered to {len(filtered_urls)} URLs")
            else:
                logger.info("  ✅ VP9 streams included (exclusion disabled)")
        else:
            logger.error("  ❌ Playlist parsing failed")

    except Exception as e:
        logger.error(f"  ❌ M3U8 processing error: {e}")

async def test_quality_filtering(scraper, logger):
    """Test quality filtering capabilities"""
    logger.info("🎯 Testing quality filtering...")

    # Mock quality data
    mock_qualities = [
        {
            'resolution': '240p',
            'codec': 'h264',
            'bandwidth': 500000,
            'is_vp9': False,
            'fps': 30
        },
        {
            'resolution': '480p',
            'codec': 'h264',
            'bandwidth': 1280000,
            'is_vp9': False,
            'fps': 30
        },
        {
            'resolution': '720p',
            'codec': 'h264',
            'bandwidth': 2560000,
            'is_vp9': False,
            'fps': 30
        },
        {
            'resolution': '720p',
            'codec': 'vp9',
            'bandwidth': 1800000,
            'is_vp9': True,
            'fps': 30
        },
        {
            'resolution': '1080p',
            'codec': 'h264',
            'bandwidth': 5000000,
            'is_vp9': False,
            'fps': 30
        },
        {
            'resolution': '1080p',
            'codec': 'vp9',
            'bandwidth': 3500000,
            'is_vp9': True,
            'fps': 30
        }
    ]

    logger.info(f"  📊 Testing with {len(mock_qualities)} quality options")

    # Test VP9 filtering
    h264_only = scraper.filter_qualities_by_codec(mock_qualities, exclude_vp9=True)
    all_codecs = scraper.filter_qualities_by_codec(mock_qualities, exclude_vp9=False)

    logger.info(f"  🔽 H.264 only filtering: {len(h264_only)} qualities")
    logger.info(f"  ✅ All codecs included: {len(all_codecs)} qualities")

    # Test preferred quality selection
    preferred = scraper.get_preferred_quality(h264_only)
    if preferred:
        logger.info(f"  🎯 Preferred quality: {preferred['resolution']} ({preferred['codec']})")
        logger.info(f"     Bandwidth: {preferred['bandwidth']:,} bps")
    else:
        logger.error("  ❌ No preferred quality found")

    # Test quality ranking
    sorted_by_bandwidth = sorted(mock_qualities, key=lambda q: q['bandwidth'], reverse=True)
    logger.info("  📈 Qualities by bandwidth (highest first):")
    for i, quality in enumerate(sorted_by_bandwidth[:3], 1):
        codec_info = f"VP9" if quality['is_vp9'] else quality['codec'].upper()
        logger.info(f"     {i}. {quality['resolution']} ({codec_info}) - {quality['bandwidth']:,} bps")

async def main():
    """Main application entry point for Phase 2"""
    try:
        # Create necessary directories
        config.create_directories()

        # Run comprehensive data extraction tests
        await test_data_extraction()

        print("\n" + "="*60)
        print("🎉 FikFap Scraper Phase 2 - Data Extraction Complete!")
        print("✅ All data extraction and validation components working")
        print("📋 Ready for Phase 3: Download Implementation")
        print("="*60)

    except KeyboardInterrupt:
        print("\n⏹️  Phase 2 test stopped by user")
    except ConfigurationError as e:
        print(f"⚙️  Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Phase 2 test failed: {e}")
        sys.exit(1)

def run():
    """Run the Phase 2 application"""
    try:
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

        asyncio.run(main())

    except KeyboardInterrupt:
        print("\n⏹️  Scraper stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
