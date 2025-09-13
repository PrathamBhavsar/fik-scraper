"""
Integrated FikFap Scraper + M3U8 Downloader
Complete pipeline from API scraping to video downloads
"""

from ast import Dict
import asyncio
import json
from datetime import datetime
from pathlib import Path

# Import our modules
from robust_api_scraper import IntegratedFikFapScraper
from m3u8_downloader import M3U8Downloader


class FikFapPipeline:
    """Complete pipeline: Scrape → Extract → Download"""

    def __init__(self, config_file: str = 'config.json'):
        with open(config_file, 'r') as f:
            self.config = json.load(f)

        self.base_url = self.config['base_url']
        self.results = {}

    async def run_full_pipeline(self, 
                               scrape_wait_time: int = 20,
                               download_immediately: bool = True,
                               min_score_filter: int = None) -> Dict:
        """Run the complete pipeline"""

        print("🚀 STARTING FULL FIKFAP PIPELINE")
        print("="*80)
        print(f"📍 Target URL: {self.base_url}")
        print(f"⏰ Scrape wait time: {scrape_wait_time} seconds")
        print(f"📥 Auto-download: {download_immediately}")
        print(f"🎯 Min score filter: {min_score_filter or 'None'}")

        try:
            # STAGE 1: Scrape and Extract
            print("\n" + "🔥"*60)
            print("🔥 STAGE 1: API SCRAPING & DATA EXTRACTION")
            print("🔥"*60)

            scraper = IntegratedFikFapScraper(self.base_url)
            scrape_results = await scraper.scrape_and_extract(
                wait_time=scrape_wait_time,
                extract_data=True
            )

            extracted_posts = scrape_results.get('extracted_posts', [])
            if not extracted_posts:
                print("❌ No posts extracted from scraping")
                return {'error': 'No posts extracted'}

            print(f"✅ Extracted {len(extracted_posts)} posts")

            # Apply filtering if specified
            if min_score_filter:
                filtered_posts = [p for p in extracted_posts if p.get('score', 0) >= min_score_filter]
                print(f"🔽 Filtered to {len(filtered_posts)} posts (score >= {min_score_filter})")
                extracted_posts = filtered_posts

            # Save pipeline extraction results
            pipeline_filename = f"pipeline_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            pipeline_data = {
                'pipeline_info': {
                    'timestamp': datetime.now().isoformat(),
                    'total_posts': len(extracted_posts),
                    'scrape_wait_time': scrape_wait_time,
                    'min_score_filter': min_score_filter,
                    'source': 'full_pipeline'
                },
                'posts': extracted_posts
            }

            with open(pipeline_filename, 'w', encoding='utf-8') as f:
                json.dump(pipeline_data, f, indent=2, ensure_ascii=False, default=str)

            print(f"💾 Saved pipeline posts to: {pipeline_filename}")

            self.results['scraping'] = {
                'posts_extracted': len(extracted_posts),
                'filename': pipeline_filename
            }

            # STAGE 2: Download Videos
            if download_immediately and extracted_posts:
                print("\n" + "🎬"*60)
                print("🎬 STAGE 2: M3U8 VIDEO DOWNLOADING")
                print("🎬"*60)

                # Initialize downloader
                downloader = M3U8Downloader()

                # Create temporary posts file for downloader
                temp_posts_file = "temp_pipeline_posts.json"
                with open(temp_posts_file, 'w', encoding='utf-8') as f:
                    json.dump(pipeline_data, f, indent=2, ensure_ascii=False, default=str)

                # Download videos
                download_results = await downloader.download_all_posts(temp_posts_file)

                # Clean up temp file
                Path(temp_posts_file).unlink(missing_ok=True)

                self.results['downloading'] = download_results

                print("\n🎉 FULL PIPELINE COMPLETED!")
                self._print_pipeline_summary()

            else:
                print("\n⏸️ PIPELINE PAUSED - Downloads skipped")
                print(f"💡 To download videos later, run:")
                print(f"   python m3u8_downloader.py")
                print(f"   (Make sure {pipeline_filename} is renamed to integrated_extracted_posts.json)")

            return self.results

        except Exception as e:
            print(f"❌ Pipeline error: {e}")
            return {'error': str(e)}

    async def run_scrape_only(self, wait_time: int = 20) -> Dict:
        """Run only the scraping stage"""
        print("🕷️ RUNNING SCRAPE-ONLY MODE")
        print("="*50)

        scraper = IntegratedFikFapScraper(self.base_url)
        results = await scraper.scrape_and_extract(
            wait_time=wait_time,
            extract_data=True
        )

        return results

    async def run_download_only(self, posts_file: str = 'integrated_extracted_posts.json') -> Dict:
        """Run only the download stage"""
        print("📥 RUNNING DOWNLOAD-ONLY MODE")
        print("="*50)

        if not Path(posts_file).exists():
            print(f"❌ Posts file not found: {posts_file}")
            return {'error': f'Posts file not found: {posts_file}'}

        downloader = M3U8Downloader()
        results = await downloader.download_all_posts(posts_file)

        return results

    def _print_pipeline_summary(self):
        """Print comprehensive pipeline results"""
        print("\n" + "="*80)
        print("📊 COMPLETE PIPELINE SUMMARY")
        print("="*80)

        # Scraping results
        if 'scraping' in self.results:
            scrape = self.results['scraping']
            print(f"🕷️ SCRAPING STAGE:")
            print(f"   Posts extracted: {scrape.get('posts_extracted', 0)}")
            print(f"   Data file: {scrape.get('filename', 'N/A')}")

        # Download results
        if 'downloading' in self.results:
            download = self.results['downloading']
            print(f"\n📥 DOWNLOAD STAGE:")
            print(f"   Total posts: {download.get('total_posts', 0)}")
            print(f"   ✅ Successful: {download.get('successful_downloads', 0)}")
            print(f"   ❌ Failed: {download.get('failed_downloads', 0)}")
            print(f"   ⏭️ Skipped: {download.get('skipped_posts', 0)}")

            if download.get('total_posts', 0) > 0:
                success_rate = (download.get('successful_downloads', 0) / download['total_posts']) * 100
                print(f"   📈 Success rate: {success_rate:.1f}%")

        print(f"\n📂 Check your downloads folder: {self.config.get('download_folder', 'downloads')}/")
        print("="*80)


# CLI Interface
async def main():
    """Main CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(description='FikFap Scraper + Downloader Pipeline')
    parser.add_argument('--mode', choices=['full', 'scrape', 'download'], default='full',
                       help='Pipeline mode (default: full)')
    parser.add_argument('--wait-time', type=int, default=20,
                       help='Wait time for scraping in seconds (default: 20)')
    parser.add_argument('--min-score', type=int, default=None,
                       help='Minimum score filter for posts')
    parser.add_argument('--posts-file', default='integrated_extracted_posts.json',
                       help='Posts file for download-only mode')
    parser.add_argument('--no-download', action='store_true',
                       help='Skip downloads in full mode')

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = FikFapPipeline()

    if args.mode == 'full':
        results = await pipeline.run_full_pipeline(
            scrape_wait_time=args.wait_time,
            download_immediately=not args.no_download,
            min_score_filter=args.min_score
        )
    elif args.mode == 'scrape':
        results = await pipeline.run_scrape_only(args.wait_time)
    elif args.mode == 'download':
        results = await pipeline.run_download_only(args.posts_file)

    return results

if __name__ == "__main__":
    # Usage examples:
    # python fikfap_pipeline.py                           # Full pipeline
    # python fikfap_pipeline.py --mode scrape             # Scrape only
    # python fikfap_pipeline.py --mode download           # Download only
    # python fikfap_pipeline.py --min-score 5000          # Filter high-quality posts
    # python fikfap_pipeline.py --wait-time 30            # Longer scrape time
    # python fikfap_pipeline.py --no-download             # Scrape without download

    asyncio.run(main())