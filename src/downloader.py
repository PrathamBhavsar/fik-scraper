import asyncio
import aiohttp
import aiofiles
import ssl
import json
from pathlib import Path
from urllib.parse import urlparse

class VideoDownloader:
    def __init__(self, download_dir="downloads", max_concurrent=5):
        self.download_dir = Path(download_dir)
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None
        self.stats = {"completed": 0, "failed": 0}

    async def __aenter__(self):
        # SSL context that bypasses certificate verification
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(
            limit=50,
            ssl=ssl_context,
            force_close=True,
            enable_cleanup_closed=True
        )

        timeout = aiohttp.ClientTimeout(total=600, connect=60)

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def download_file(self, url, file_path, max_retries=3):
        """Download a file with retry logic"""
        if not url or not url.strip():
            return False

        async with self.semaphore:
            for attempt in range(max_retries + 1):
                try:
                    async with self.session.get(url, ssl=False, allow_redirects=True) as response:
                        if response.status == 200:
                            file_size = response.headers.get('content-length')

                            async with aiofiles.open(file_path, 'wb') as file:
                                async for chunk in response.content.iter_chunked(8192):
                                    await file.write(chunk)

                            size_mb = file_path.stat().st_size / (1024 * 1024)
                            print(f"   ✅ Downloaded {file_path.name} ({size_mb:.1f} MB)")
                            self.stats["completed"] += 1
                            return True

                        elif response.status == 404:
                            print(f"   ❌ File not found: {file_path.name}")
                            self.stats["failed"] += 1
                            return False
                        else:
                            if attempt == max_retries:
                                print(f"   ❌ HTTP {response.status}: {file_path.name}")
                                self.stats["failed"] += 1
                                return False

                except Exception as e:
                    if attempt == max_retries:
                        print(f"   ❌ Error downloading {file_path.name}: {e}")
                        self.stats["failed"] += 1
                        return False

                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

        return False

    async def save_metadata(self, video_data, file_path):
        """Save video metadata as JSON"""
        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(video_data, indent=2, ensure_ascii=False))
            return True
        except Exception as e:
            print(f"   ❌ Error saving metadata: {e}")
            return False

    async def download_video_files(self, video_data):
        """Download all files for one video"""
        video_id = video_data.get('video_id', 'unknown')
        title = video_data.get('title', 'Unknown')[:50]

        # Create video directory
        video_dir = self.download_dir / video_id
        video_dir.mkdir(parents=True, exist_ok=True)

        print(f"📥 Downloading: {title}... (ID: {video_id})")

        results = {"metadata": False, "thumbnail": False, "video": False}

        # Save metadata
        metadata_path = video_dir / f"{video_id}.json"
        results["metadata"] = await self.save_metadata(video_data, metadata_path)

        # Download tasks
        tasks = []

        # Thumbnail
        thumbnail_url = video_data.get('thumbnail_src', '')
        if thumbnail_url:
            thumbnail_path = video_dir / f"{video_id}.jpg"
            tasks.append(self.download_file(thumbnail_url, thumbnail_path))
        else:
            results["thumbnail"] = None

        # Video
        video_url = video_data.get('video_src', '')
        if video_url:
            video_path = video_dir / f"{video_id}.mp4"
            tasks.append(self.download_file(video_url, video_path))
        else:
            results["video"] = None

        # Execute downloads
        if tasks:
            task_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Update results
            task_index = 0
            if thumbnail_url:
                results["thumbnail"] = task_results[task_index] if not isinstance(task_results[task_index], Exception) else False
                task_index += 1
            if video_url:
                results["video"] = task_results[task_index] if not isinstance(task_results[task_index], Exception) else False

        # Summary
        success_count = sum(1 for v in results.values() if v is True)
        total_expected = sum(1 for k, v in results.items() if v is not None)

        if success_count == total_expected and total_expected > 0:
            print(f"   🎉 Complete ({success_count}/{total_expected} files)")
        else:
            print(f"   ⚠️ Partial ({success_count}/{total_expected} files)")

        return results

    async def download_all_videos(self, videos_data):
        """Download all videos with batch processing"""
        if not videos_data:
            print("❌ No videos to download")
            return {}

        total_videos = len(videos_data)
        print(f"🚀 Starting download of {total_videos} videos")
        print(f"📁 Download directory: {self.download_dir.absolute()}")
        print(f"⚡ Max concurrent: {self.max_concurrent}")

        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Process videos in batches to avoid overwhelming the server
        batch_size = min(self.max_concurrent, 8)
        results = {}

        for i in range(0, total_videos, batch_size):
            batch = videos_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_videos + batch_size - 1) // batch_size

            print(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} videos)")

            # Process batch
            batch_tasks = [self.download_video_files(video) for video in batch]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Store results
            for j, result in enumerate(batch_results):
                if not isinstance(result, Exception):
                    video_id = batch[j].get('video_id', f'unknown_{i+j}')
                    results[video_id] = result
                else:
                    print(f"   ❌ Batch error: {result}")

            # Progress
            completed_videos = len(results)
            progress = (completed_videos / total_videos) * 100
            print(f"📈 Progress: {completed_videos}/{total_videos} ({progress:.1f}%)")

            # Brief pause between batches
            if i + batch_size < total_videos:
                await asyncio.sleep(1)

        # Final summary
        successful = sum(1 for r in results.values() if any(r.values()))
        print(f"\n🎯 DOWNLOAD COMPLETE!")
        print(f"✅ Successful: {successful}/{total_videos}")
        print(f"📊 Files: {self.stats['completed']} downloaded, {self.stats['failed']} failed")
        print(f"📁 Location: {self.download_dir.absolute()}")

        return results

async def main():
    # Load video data
    try:
        with open('videos.json', 'r', encoding='utf-8') as f:
            videos_data = json.load(f)
    except FileNotFoundError:
        print("❌ videos.json not found. Run parser.py first.")
        return

    # Download all videos
    async with VideoDownloader(download_dir="downloads", max_concurrent=5) as downloader:
        await downloader.download_all_videos(videos_data)

if __name__ == "__main__":
    asyncio.run(main())
