"""
IDM (Internet Download Manager) Integration Module

This module integrates with the existing video parser and provides automated IDM download management.
It creates directory structures and adds downloads to IDM queue with proper file paths.

Features:
- Creates organized directory structure for each video
- Adds downloads to IDM queue using command line interface  
- Manages batch downloads with proper queue control
- Starts IDM download queue automatically after adding all files
- Supports both direct IDM.exe calls and Python IDM library

Author: AI Assistant
Version: 1.0
"""

import os
import subprocess
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import asyncio
import shutil

class IDMManager:
    """
    Internet Download Manager automation class.

    Handles adding downloads to IDM queue and managing the download process
    with proper directory structure and batch processing.
    """

    def __init__(self, base_download_dir: str = "downloads", idm_path: str = None, use_idm_library: bool = False):
        """
        Initialize IDM Manager.

        Args:
            base_download_dir: Base directory for downloads
            idm_path: Path to IDM executable (auto-detected if None)
            use_idm_library: Whether to use Python IDM library instead of subprocess
        """
        self.base_download_dir = Path(base_download_dir)
        self.use_idm_library = use_idm_library
        self.idm_path = self._find_idm_executable(idm_path)
        self.download_queue = []
        self.stats = {
            'total_videos': 0,
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0
        }

        # Initialize IDM library if requested
        if use_idm_library:
            try:
                from idm import IDMan
                self.idm_library = IDMan()
                print("✅ IDM Python library initialized successfully")
            except ImportError:
                print("⚠️ IDM library not found. Install with: pip install idm")
                print("🔄 Falling back to subprocess method")
                self.use_idm_library = False

        # Create base download directory
        self.base_download_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Base download directory: {self.base_download_dir.absolute()}")
        print(f"🔧 IDM executable: {self.idm_path}")
        print(f"⚙️ Using IDM library: {self.use_idm_library}")

    def _find_idm_executable(self, idm_path: str = None) -> str:
        """
        Find IDM executable path.

        Args:
            idm_path: Custom IDM path

        Returns:
            Path to IDM executable
        """
        if idm_path and os.path.exists(idm_path):
            return idm_path

        # Common IDM installation paths
        common_paths = [
            r"C:\Program Files\Internet Download Manager\IDMan.exe",
            r"C:\Program Files (x86)\Internet Download Manager\IDMan.exe",
            r"C:\IDM\IDMan.exe",
            r"IDMan.exe"  # If in PATH
        ]

        for path in common_paths:
            if os.path.exists(path):
                return path

        # Try to find IDM in PATH
        try:
            result = subprocess.run(['where', 'IDMan'], capture_output=True, text=True, shell=True)
            if result.returncode == 0:
                return result.stdout.strip().split('\n')[0]
        except:
            pass

        print("⚠️ IDM executable not found. Please specify idm_path parameter.")
        return "IDMan.exe"  # Fallback - will work if IDM is in PATH

    def create_video_directory(self, video_id: str) -> Path:
        """
        Create organized directory structure for a video.

        Args:
            video_id: Unique video identifier

        Returns:
            Path to video directory
        """
        video_dir = self.base_download_dir / video_id
        video_dir.mkdir(parents=True, exist_ok=True)
        self.stats['directories_created'] += 1
        return video_dir

    def prepare_video_downloads(self, video_data: Dict) -> Dict[str, str]:
        """
        Prepare download information for a video.

        Args:
            video_data: Video metadata dictionary

        Returns:
            Dictionary with download information
        """
        video_id = video_data.get('video_id', 'unknown')
        video_dir = self.create_video_directory(video_id)

        downloads = {}

        # Prepare JSON metadata file
        json_path = video_dir / f"{video_id}.json"
        downloads['json'] = {
            'type': 'metadata',
            'path': json_path,
            'data': video_data
        }

        # Prepare thumbnail download
        thumbnail_url = video_data.get('thumbnail_src', '')
        if thumbnail_url:
            jpg_path = video_dir / f"{video_id}.jpg"
            downloads['thumbnail'] = {
                'type': 'thumbnail',
                'url': thumbnail_url,
                'path': jpg_path
            }

        # Prepare video download  
        video_url = video_data.get('video_src', '')
        if video_url:
            mp4_path = video_dir / f"{video_id}.mp4"
            downloads['video'] = {
                'type': 'video',
                'url': video_url,
                'path': mp4_path
            }

        return downloads

    def save_json_metadata(self, json_path: Path, video_data: Dict) -> bool:
        """
        Save video metadata as JSON file.

        Args:
            json_path: Path to save JSON file
            video_data: Video metadata

        Returns:
            Success status
        """
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(video_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Saved metadata: {json_path.name}")
            return True
        except Exception as e:
            print(f"❌ Error saving metadata {json_path.name}: {e}")
            return False

    def add_to_idm_queue_subprocess(self, url: str, local_path: Path, filename: str) -> bool:
        """
        Add download to IDM queue using subprocess (command line).

        Args:
            url: Download URL
            local_path: Local directory path
            filename: Local filename

        Returns:
            Success status
        """
        try:
            # Ensure directory exists
            local_path.mkdir(parents=True, exist_ok=True)

            # Build IDM command
            cmd = [
                self.idm_path,
                '/d', url,                           # Download URL
                '/p', str(local_path),               # Local path
                '/f', filename,                      # Filename
                '/a',                                # Add to queue without starting
                '/n'                                 # Silent mode
            ]

            print(f"🚀 Adding to IDM queue: {filename}")
            print(f"   URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f"   Path: {local_path}")

            # Execute IDM command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                print(f"✅ Successfully added to IDM queue: {filename}")
                return True
            else:
                print(f"❌ IDM command failed for {filename}")
                if result.stderr:
                    print(f"   Error: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print(f"⏰ Timeout adding {filename} to IDM queue")
            return False
        except Exception as e:
            print(f"❌ Error adding {filename} to IDM queue: {e}")
            return False

    def add_to_idm_queue_library(self, url: str, local_path: Path, filename: str) -> bool:
        """
        Add download to IDM queue using Python IDM library.

        Args:
            url: Download URL
            local_path: Local directory path  
            filename: Local filename

        Returns:
            Success status
        """
        try:
            # Ensure directory exists
            local_path.mkdir(parents=True, exist_ok=True)

            print(f"📚 Adding to IDM via library: {filename}")
            print(f"   URL: {url[:80]}{'...' if len(url) > 80 else ''}")
            print(f"   Path: {local_path}")

            # Add to IDM queue using library
            self.idm_library.download(
                url=url,
                path=str(local_path), 
                filename=filename,
                add_only=True  # Add to queue without starting
            )

            print(f"✅ Successfully added to IDM queue: {filename}")
            return True

        except Exception as e:
            print(f"❌ Error adding {filename} to IDM queue: {e}")
            return False

    def add_video_to_idm_queue(self, video_data: Dict) -> Dict[str, bool]:
        """
        Add all files for a video to IDM download queue.

        Args:
            video_data: Video metadata dictionary

        Returns:
            Dictionary with success status for each file type
        """
        video_id = video_data.get('video_id', 'unknown')
        print(f"\n🎬 Processing video: {video_data.get('title', 'Unknown')} (ID: {video_id})")

        # Prepare downloads
        downloads = self.prepare_video_downloads(video_data)
        results = {'metadata': False, 'thumbnail': False, 'video': False}

        # Save JSON metadata (always successful since it's local)
        if 'json' in downloads:
            json_info = downloads['json']
            results['metadata'] = self.save_json_metadata(json_info['path'], json_info['data'])

        # Add thumbnail to IDM queue
        if 'thumbnail' in downloads:
            thumb_info = downloads['thumbnail']
            if self.use_idm_library:
                success = self.add_to_idm_queue_library(
                    thumb_info['url'], 
                    thumb_info['path'].parent, 
                    thumb_info['path'].name
                )
            else:
                success = self.add_to_idm_queue_subprocess(
                    thumb_info['url'], 
                    thumb_info['path'].parent, 
                    thumb_info['path'].name
                )
            results['thumbnail'] = success

            if success:
                self.download_queue.append({
                    'type': 'thumbnail',
                    'video_id': video_id,
                    'url': thumb_info['url'],
                    'path': thumb_info['path']
                })

        # Add video to IDM queue
        if 'video' in downloads:
            video_info = downloads['video']
            if self.use_idm_library:
                success = self.add_to_idm_queue_library(
                    video_info['url'],
                    video_info['path'].parent,
                    video_info['path'].name
                )
            else:
                success = self.add_to_idm_queue_subprocess(
                    video_info['url'],
                    video_info['path'].parent, 
                    video_info['path'].name
                )
            results['video'] = success

            if success:
                self.download_queue.append({
                    'type': 'video', 
                    'video_id': video_id,
                    'url': video_info['url'],
                    'path': video_info['path']
                })

        # Update stats
        if any(results.values()):
            self.stats['successful_additions'] += 1
        else:
            self.stats['failed_additions'] += 1

        return results

    def start_idm_queue(self) -> bool:
        """
        Start IDM download queue.

        Returns:
            Success status
        """
        try:
            print("\n🚀 Starting IDM download queue...")

            if self.use_idm_library:
                print("📚 Using IDM library to start queue...")
                # Note: IDM library doesn't have a direct queue start method
                # Downloads should start automatically when added
                print("✅ IDM library downloads should start automatically")
                return True
            else:
                # Use subprocess to start IDM queue
                cmd = [self.idm_path, '/s']  # Start queue in scheduler

                print(f"🖥️ Executing: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)

                if result.returncode == 0:
                    print("✅ IDM queue started successfully!")
                    return True
                else:
                    print(f"❌ Failed to start IDM queue")
                    if result.stderr:
                        print(f"   Error: {result.stderr}")
                    return False

        except subprocess.TimeoutExpired:
            print("⏰ Timeout starting IDM queue")
            return False
        except Exception as e:
            print(f"❌ Error starting IDM queue: {e}")
            return False

    def process_all_videos(self, videos_data: List[Dict], start_queue: bool = True) -> Dict:
        """
        Process all videos - add to IDM queue and optionally start downloads.

        Args:
            videos_data: List of video metadata dictionaries
            start_queue: Whether to start IDM queue after adding all downloads

        Returns:
            Processing results dictionary
        """
        if not videos_data:
            print("❌ No video data provided")
            return {"success": False, "error": "No video data provided"}

        print(f"🎯 Processing {len(videos_data)} videos for IDM download...")
        print(f"📁 Download directory: {self.base_download_dir.absolute()}")
        print(f"⚙️ Method: {'IDM Library' if self.use_idm_library else 'Subprocess'}")
        print("="*80)

        # Reset stats
        self.stats = {
            'total_videos': len(videos_data),
            'successful_additions': 0,
            'failed_additions': 0,
            'directories_created': 0
        }

        # Process each video
        video_results = {}
        for i, video_data in enumerate(videos_data, 1):
            video_id = video_data.get('video_id', f'unknown_{i}')
            print(f"\n📋 Processing video {i}/{len(videos_data)}: {video_id}")

            try:
                results = self.add_video_to_idm_queue(video_data)
                video_results[video_id] = results

                # Show progress
                progress = (i / len(videos_data)) * 100
                print(f"📊 Progress: {i}/{len(videos_data)} videos ({progress:.1f}%)")

            except Exception as e:
                print(f"❌ Error processing video {video_id}: {e}")
                video_results[video_id] = {'metadata': False, 'thumbnail': False, 'video': False}
                self.stats['failed_additions'] += 1

        print("\n" + "="*80)
        print("📋 BATCH ADDITION COMPLETE!")
        self.print_stats()

        # Start IDM queue if requested
        queue_started = False
        if start_queue and len(self.download_queue) > 0:
            print("\n🚀 Starting IDM download queue...")
            queue_started = self.start_idm_queue()
            if queue_started:
                print("✅ All downloads added to IDM and queue started!")
            else:
                print("⚠️ Downloads added but failed to start queue automatically.")
                print("💡 Please start the queue manually in IDM.")
        elif len(self.download_queue) == 0:
            print("⚠️ No downloads were added to queue.")
        else:
            print("ℹ️ Downloads added to IDM queue but not started (start_queue=False)")
            print("💡 Use start_idm_queue() to start downloads later.")

        return {
            'success': True,
            'total_videos': self.stats['total_videos'],
            'successful_additions': self.stats['successful_additions'],
            'failed_additions': self.stats['failed_additions'],
            'directories_created': self.stats['directories_created'],
            'download_queue_size': len(self.download_queue),
            'queue_started': queue_started,
            'video_results': video_results,
            'download_directory': str(self.base_download_dir.absolute())
        }

    def print_stats(self):
        """Print processing statistics."""
        print(f"📊 STATISTICS:")
        print(f"   📁 Total videos: {self.stats['total_videos']}")
        print(f"   ✅ Successful additions: {self.stats['successful_additions']}")
        print(f"   ❌ Failed additions: {self.stats['failed_additions']}")
        print(f"   📂 Directories created: {self.stats['directories_created']}")
        print(f"   📥 Items in download queue: {len(self.download_queue)}")

        if self.stats['total_videos'] > 0:
            success_rate = (self.stats['successful_additions'] / self.stats['total_videos']) * 100
            print(f"   🎯 Success rate: {success_rate:.1f}%")

    def get_queue_info(self) -> Dict:
        """
        Get information about current download queue.

        Returns:
            Queue information dictionary
        """
        thumbnails = [item for item in self.download_queue if item['type'] == 'thumbnail']
        videos = [item for item in self.download_queue if item['type'] == 'video']

        return {
            'total_items': len(self.download_queue),
            'thumbnails': len(thumbnails),
            'videos': len(videos),
            'unique_videos': len(set(item['video_id'] for item in self.download_queue)),
            'queue_items': self.download_queue
        }

    def clear_queue(self):
        """Clear the download queue."""
        self.download_queue.clear()
        print("🧹 Download queue cleared")


# Integration class for complete workflow
class VideoIDMProcessor:
    """
    Complete video processing workflow that combines video parsing with IDM download management.
    """

    def __init__(self, base_url: str, download_dir: str = "downloads", idm_path: str = None, use_idm_library: bool = False):
        """
        Initialize complete video to IDM processor.

        Args:
            base_url: Base URL of video site
            download_dir: Directory for downloads
            idm_path: Path to IDM executable
            use_idm_library: Use Python IDM library instead of subprocess
        """
        self.base_url = base_url
        self.download_dir = download_dir

        # Import the existing parser
        try:
            from main import OptimizedVideoDataParser
            self.parser = OptimizedVideoDataParser(base_url)
            print("✅ Video parser initialized")
        except ImportError as e:
            print(f"❌ Could not import video parser: {e}")
            print("   Please ensure main.py is in the same directory")
            self.parser = None

        # Initialize IDM manager
        self.idm_manager = IDMManager(download_dir, idm_path, use_idm_library)
        print("✅ IDM manager initialized")

    async def process_all_videos(self) -> Dict:
        """
        Complete processing workflow: parse videos and add to IDM.

        Returns:
            Complete processing results
        """
        if not self.parser:
            return {"success": False, "error": "Video parser not available"}

        print(f"🎬 Starting complete video processing workflow")
        print(f"🌐 Source URL: {self.base_url}")
        print(f"📁 Download directory: {self.download_dir}")
        print("="*80)

        try:
            # Step 1: Extract video URLs
            print("🔍 Step 1: Extracting video URLs...")
            video_urls = await self.parser.extract_video_urls()

            if not video_urls:
                return {"success": False, "error": "No video URLs found"}

            print(f"✅ Found {len(video_urls)} video URLs")

            # Step 2: Parse video metadata
            print("\n📝 Step 2: Parsing video metadata...")
            videos_data = await self.parser.parse_all_videos()

            if not videos_data:
                return {"success": False, "error": "No video metadata could be parsed"}

            print(f"✅ Successfully parsed {len(videos_data)} videos")

            # Step 3: Add to IDM queue and start downloads
            print("\n📥 Step 3: Adding videos to IDM queue...")
            idm_results = self.idm_manager.process_all_videos(videos_data, start_queue=True)

            # Combine results
            return {
                "success": True,
                "urls_found": len(video_urls),
                "videos_parsed": len(videos_data),
                "idm_results": idm_results,
                "download_directory": self.download_dir
            }

        except Exception as e:
            print(f"❌ Error in processing workflow: {e}")
            return {"success": False, "error": str(e)}


# Example usage and main function
async def main():
    """
    Example usage of the IDM integration system.
    """
    # Configuration
    BASE_URL = "https://rule34video.com"  # Change to your target URL
    DOWNLOAD_DIR = "downloads"
    IDM_PATH = None  # Auto-detect IDM
    USE_IDM_LIBRARY = False  # Use subprocess method by default

    print("🎬 Video to IDM Processor")
    print("=" * 60)
    print("This tool will:")
    print("1. Extract video URLs from the website") 
    print("2. Parse video metadata")
    print("3. Create organized directory structure")
    print("4. Add downloads to IDM queue with proper paths")
    print("5. Start IDM download queue automatically")
    print("=" * 60)

    try:
        # Create processor
        processor = VideoIDMProcessor(
            base_url=BASE_URL,
            download_dir=DOWNLOAD_DIR, 
            idm_path=IDM_PATH,
            use_idm_library=USE_IDM_LIBRARY
        )

        # Process all videos
        results = await processor.process_all_videos()

        # Print final results
        print("\n" + "="*80)
        print("🎯 FINAL RESULTS")
        print("="*80)

        if results.get("success"):
            print("✅ Processing completed successfully!")
            print(f"🔍 URLs found: {results.get('urls_found', 0)}")
            print(f"📝 Videos parsed: {results.get('videos_parsed', 0)}")

            idm_results = results.get('idm_results', {})
            print(f"✅ Successful IDM additions: {idm_results.get('successful_additions', 0)}")
            print(f"❌ Failed IDM additions: {idm_results.get('failed_additions', 0)}")
            print(f"📂 Directories created: {idm_results.get('directories_created', 0)}")
            print(f"📥 Queue items: {idm_results.get('download_queue_size', 0)}")
            print(f"🚀 Queue started: {idm_results.get('queue_started', False)}")
            print(f"📁 Download directory: {results.get('download_directory', 'Unknown')}")
        else:
            print("❌ Processing failed!")
            print(f"Error: {results.get('error', 'Unknown error')}")

        return results

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return {"success": False, "error": str(e)}


if __name__ == "__main__":
    """
    Run the complete IDM integration system.

    Requirements:
    - Internet Download Manager installed on Windows
    - main.py (video parser) in same directory
    - Optional: pip install idm (for IDM library method)
    """

    print("🎬 Video to IDM Integration System")
    print("=" * 70)
    print("🎯 This will automatically:")
    print("   1. Parse videos from website")
    print("   2. Create organized directories")
    print("   3. Add all downloads to IDM queue")
    print("   4. Start IDM downloads automatically")
    print("=" * 70)

    # Run the complete workflow
    results = asyncio.run(main())

    if results and results.get("success"):
        print("\n✅ All done! Check IDM for download progress.")
    else:
        print("\n❌ Process completed with errors.")

