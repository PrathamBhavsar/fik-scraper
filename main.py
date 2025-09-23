
"""
UPDATED main.py - FikFap Scraper with Video Downloader Integration

This integrates your existing scraping system with the new video downloader
to create the structured folder format you requested.
"""

import asyncio
import argparse
import signal
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Import the workflow components
from fikfap_workflow_integrator import FikFapWorkflowIntegrator, FikFapContinuousRunner
from video_downloader_organizer import VideoDownloaderOrganizer

# Import existing components
from core.config import Config, config
from core.exceptions import *
from utils.logger import setup_logger


class FikFapMainApplicationWithDownloader:
    """Enhanced main application with video downloader integration"""

    def __init__(self, config_path: Optional[str] = None, log_level: str = "INFO"):
        # Setup logging
        self.logger = setup_logger(self.__class__.__name__, level=log_level)

        # Load configuration
        self.config = Config()
        if config_path and Path(config_path).exists():
            self.config.load_from_file(config_path)

        # Components
        self.workflow_integrator: Optional[FikFapWorkflowIntegrator] = None
        self.continuous_runner: Optional[FikFapContinuousRunner] = None
        self.video_downloader: Optional[VideoDownloaderOrganizer] = None

        # State
        self.shutdown_requested = False

        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, requesting shutdown")
            self.shutdown_requested = True
            if self.continuous_runner:
                self.continuous_runner.request_stop()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def run_single_cycle_with_download(self, download_videos: bool = True) -> Dict[str, Any]:
        """Run a single scraping cycle and optionally download videos"""
        try:
            self.logger.info("Starting single cycle with video download integration")

            # Initialize workflow integrator
            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator

                # Run single scraping cycle
                scrape_result = await integrator.run_single_cycle()

                # Log scraping results
                if scrape_result.get("success", False):
                    self.logger.info(
                        f"Scraping completed successfully:\n"
                        f"  Posts scraped: {scrape_result.get('posts_scraped', 0)}\n"
                        f"  Posts processed: {scrape_result.get('posts_processed', 0)}\n"
                        f"  Posts failed: {scrape_result.get('posts_failed', 0)}\n"
                        f"  Duration: {scrape_result.get('cycle_duration', 0):.2f}s"
                    )

                    # Download videos if requested and scraping was successful
                    if download_videos and scrape_result.get("posts_scraped", 0) > 0:
                        self.logger.info("Starting video download process...")

                        # Initialize video downloader
                        async with VideoDownloaderOrganizer("./downloads") as downloader:
                            self.video_downloader = downloader

                            # Download videos from scraped posts
                            download_result = await downloader.process_all_posts("all_raw_posts.json")

                            # Combine results
                            combined_result = {
                                **scrape_result,
                                "download_enabled": True,
                                "download_results": download_result
                            }

                            self.logger.info(
                                f"Download completed:\n"
                                f"  Videos processed: {download_result.get('summary', {}).get('successful_count', 0)}\n"
                                f"  Total files downloaded: {download_result.get('summary', {}).get('total_files', 0)}\n"
                                f"  Success rate: {download_result.get('summary', {}).get('success_rate', '0%')}"
                            )

                            return combined_result
                    else:
                        return {**scrape_result, "download_enabled": False}
                else:
                    self.logger.error(f"Scraping failed: {scrape_result.get('error', 'Unknown error')}")
                    return scrape_result

        except Exception as e:
            self.logger.error(f"Single cycle with download failed: {e}")
            return {"success": False, "error": str(e)}

    async def run_continuous_loop_with_download(
        self, 
        interval: int = 300,
        download_videos: bool = True
    ) -> None:
        """Run continuous scraping loop with video downloads"""
        try:
            self.logger.info(f"Starting continuous execution with downloads (interval: {interval}s)")

            # Initialize workflow integrator
            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator

                # Initialize video downloader
                async with VideoDownloaderOrganizer("./downloads") as downloader:
                    self.video_downloader = downloader

                    # Create enhanced continuous runner
                    config_override = {"continuous.loop_interval": interval}
                    self.continuous_runner = EnhancedContinuousRunner(
                        integrator, downloader, config_override, download_videos
                    )

                    # Start continuous loop
                    await self.continuous_runner.run_continuous_loop()

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Continuous loop with downloads failed: {e}")
            raise

    async def download_existing_posts(self, posts_file: str = "all_raw_posts.json") -> Dict[str, Any]:
        """Download videos from an existing posts file"""
        try:
            self.logger.info(f"Downloading videos from existing posts file: {posts_file}")

            if not Path(posts_file).exists():
                raise FileNotFoundError(f"Posts file not found: {posts_file}")

            # Initialize video downloader
            async with VideoDownloaderOrganizer("./downloads") as downloader:
                self.video_downloader = downloader

                # Download videos
                download_result = await downloader.process_all_posts(posts_file)

                self.logger.info(
                    f"Download completed:\n"
                    f"  Videos processed: {download_result.get('summary', {}).get('successful_count', 0)}\n"
                    f"  Total files downloaded: {download_result.get('summary', {}).get('total_files', 0)}\n"
                    f"  Success rate: {download_result.get('summary', {}).get('success_rate', '0%')}"
                )

                return download_result

        except Exception as e:
            self.logger.error(f"Download existing posts failed: {e}")
            return {"success": False, "error": str(e)}

    # Keep all existing methods from the original main.py
    async def run_single_cycle(self) -> Dict[str, Any]:
        """Run a single scraping and processing cycle (original method)"""
        try:
            self.logger.info("Starting single cycle execution")

            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator
                result = await integrator.run_single_cycle()

                if result.get("success", False):
                    self.logger.info(
                        f"Single cycle completed successfully:\n"
                        f"  Posts scraped: {result.get('posts_scraped', 0)}\n"
                        f"  Posts processed: {result.get('posts_processed', 0)}\n"
                        f"  Posts failed: {result.get('posts_failed', 0)}\n"
                        f"  Duration: {result.get('cycle_duration', 0):.2f}s"
                    )
                else:
                    self.logger.error(f"Single cycle failed: {result.get('error', 'Unknown error')}")

                return result

        except Exception as e:
            self.logger.error(f"Single cycle execution failed: {e}")
            return {"success": False, "error": str(e)}

    async def run_continuous_loop(self, interval: int = 300) -> None:
        """Run continuous scraping and processing loop (original method)"""
        try:
            self.logger.info(f"Starting continuous execution (interval: {interval}s)")

            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator
                config_override = {"continuous.loop_interval": interval}
                self.continuous_runner = FikFapContinuousRunner(integrator, config_override)
                await self.continuous_runner.run_continuous_loop()

        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Continuous loop execution failed: {e}")
            raise

    async def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive system health check"""
        try:
            self.logger.info("Running system health check")

            health_results = {
                "config_status": {"healthy": True, "details": "Configuration loaded successfully"},
                "storage_status": {"healthy": False, "details": ""},
                "component_status": {"healthy": False, "details": ""},
                "overall_status": {"healthy": False}
            }

            # Check storage directories
            try:
                downloads_dir = Path(self.config.get('storage.downloads_dir', './downloads'))
                downloads_dir.mkdir(parents=True, exist_ok=True)

                test_file = downloads_dir / "health_check_test.txt"
                test_file.write_text("health check test")
                test_file.unlink()

                health_results["storage_status"] = {
                    "healthy": True,
                    "details": f"Storage directory accessible: {downloads_dir}"
                }

            except Exception as e:
                health_results["storage_status"] = {
                    "healthy": False,
                    "details": f"Storage directory error: {e}"
                }

            # Check component initialization
            try:
                async with FikFapWorkflowIntegrator() as integrator:
                    component_health = await integrator.run_health_check()
                    health_results["component_status"] = {
                        "healthy": component_health.get("overall_health", False),
                        "details": component_health
                    }

            except Exception as e:
                health_results["component_status"] = {
                    "healthy": False,
                    "details": f"Component initialization failed: {e}"
                }

            # Determine overall health
            health_results["overall_status"]["healthy"] = all(
                result["healthy"] for result in [
                    health_results["config_status"],
                    health_results["storage_status"],
                    health_results["component_status"]
                ]
            )

            # Log results
            if health_results["overall_status"]["healthy"]:
                self.logger.info("✓ System health check passed - all components healthy")
            else:
                self.logger.warning("✗ System health check failed - see details below")
                for check_name, result in health_results.items():
                    if not result.get("healthy", True):
                        self.logger.error(f"  {check_name}: {result.get('details', 'Unknown error')}")

            return health_results

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {"overall_status": {"healthy": False}, "error": str(e)}

    async def run_demo(self) -> None:
        """Run a demonstration of the complete workflow with downloads"""
        try:
            self.logger.info("🚀 Starting FikFap Integration Demo with Video Downloads")

            # Step 1: Health check
            self.logger.info("📊 Step 1: Running system health check...")
            health_results = await self.run_health_check()
            if not health_results.get("overall_status", {}).get("healthy", False):
                self.logger.error("❌ System health check failed - cannot continue demo")
                return
            self.logger.info("✅ Step 1 completed: System health check passed")

            # Step 2: Single cycle with download demonstration
            self.logger.info("🔄 Step 2: Running demonstration cycle with downloads...")
            cycle_result = await self.run_single_cycle_with_download(download_videos=True)

            if cycle_result.get("success", False):
                posts_scraped = cycle_result.get("posts_scraped", 0)
                posts_processed = cycle_result.get("posts_processed", 0)
                duration = cycle_result.get("cycle_duration", 0)

                self.logger.info(
                    f"✅ Step 2 completed: Demonstration cycle successful!\n"
                    f"  📥 Posts scraped from API: {posts_scraped}\n"
                    f"  ⚡ Posts processed: {posts_processed}\n"
                    f"  ⏱️ Duration: {duration:.2f} seconds"
                )

                # Show download results if available
                if cycle_result.get("download_enabled") and cycle_result.get("download_results"):
                    download_summary = cycle_result["download_results"].get("summary", {})
                    self.logger.info(
                        f"  📁 Videos downloaded: {download_summary.get('successful_count', 0)}\n"
                        f"  📊 Total files: {download_summary.get('total_files', 0)}\n"
                        f"  ✅ Success rate: {download_summary.get('success_rate', '0%')}"
                    )
            else:
                error = cycle_result.get("error", "Unknown error")
                self.logger.error(f"❌ Step 2 failed: {error}")
                return

            # Step 3: Demo completed
            self.logger.info("🎉 Step 3: Demo completed successfully!")
            self.logger.info(
                "🔧 The enhanced system is ready for production use:\n"
                "  • Use --single for scraping only\n"
                "  • Use --single-download for scraping + downloads\n"
                "  • Use --download-only to download from existing data\n"
                "  • Use --continuous-download for automated looping with downloads\n"
                "  • Use --health-check for system monitoring"
            )

        except Exception as e:
            self.logger.error(f"❌ Demo failed: {e}")


class EnhancedContinuousRunner:
    """Enhanced continuous runner with video download integration"""

    def __init__(self, integrator, downloader, config_override=None, download_enabled=True):
        self.integrator = integrator
        self.downloader = downloader
        self.config_override = config_override or {}
        self.download_enabled = download_enabled
        self.stop_requested = False
        self.logger = setup_logger(self.__class__.__name__)

        # Enhanced stats
        self.continuous_stats = {
            "total_cycles": 0,
            "successful_cycles": 0,
            "failed_cycles": 0,
            "consecutive_failures": 0,
            "start_time": None,
            "last_cycle_time": None,
            "total_posts_processed": 0,
            "total_videos_downloaded": 0,
            "total_files_created": 0
        }

    def request_stop(self):
        """Request stop of continuous loop"""
        self.stop_requested = True
        self.logger.info("Enhanced continuous runner stop requested")

    async def run_continuous_loop(self):
        """Run enhanced continuous loop with download integration"""
        try:
            self.continuous_stats["start_time"] = datetime.now()
            interval = self.config_override.get("continuous.loop_interval", 300)
            max_failures = self.config_override.get("continuous.max_consecutive_failures", 5)
            recovery_delay = self.config_override.get("continuous.recovery_delay", 60)

            self.logger.info(f"🔄 Starting enhanced continuous loop (interval: {interval}s, downloads: {self.download_enabled})")

            while not self.stop_requested:
                cycle_start = datetime.now()
                self.continuous_stats["total_cycles"] += 1

                try:
                    # Run scraping cycle
                    scrape_result = await self.integrator.run_single_cycle()

                    success = scrape_result.get("success", False)
                    posts_scraped = scrape_result.get("posts_scraped", 0)

                    # Download videos if enabled and scraping was successful
                    download_result = None
                    if self.download_enabled and success and posts_scraped > 0:
                        try:
                            download_result = await self.downloader.process_all_posts("all_raw_posts.json")
                            if download_result and download_result.get("summary"):
                                self.continuous_stats["total_videos_downloaded"] += download_result["summary"].get("successful_count", 0)
                                self.continuous_stats["total_files_created"] += download_result["summary"].get("total_files", 0)
                        except Exception as e:
                            self.logger.error(f"Download phase failed: {e}")
                            download_result = {"error": str(e)}

                    if success:
                        self.continuous_stats["successful_cycles"] += 1
                        self.continuous_stats["consecutive_failures"] = 0
                        self.continuous_stats["total_posts_processed"] += scrape_result.get("posts_processed", 0)

                        cycle_duration = (datetime.now() - cycle_start).total_seconds()
                        posts_processed = scrape_result.get("posts_processed", 0)

                        download_info = ""
                        if download_result and download_result.get("summary"):
                            videos_downloaded = download_result["summary"].get("successful_count", 0)
                            files_created = download_result["summary"].get("total_files", 0)
                            download_info = f", {videos_downloaded} videos downloaded ({files_created} files)"

                        self.logger.info(
                            f"✅ Cycle {self.continuous_stats['total_cycles']} completed: "
                            f"{posts_processed} posts processed{download_info} in {cycle_duration:.2f}s"
                        )
                    else:
                        self.continuous_stats["failed_cycles"] += 1
                        self.continuous_stats["consecutive_failures"] += 1
                        error = scrape_result.get("error", "Unknown error")
                        self.logger.error(f"❌ Cycle {self.continuous_stats['total_cycles']} failed: {error}")

                        if self.continuous_stats["consecutive_failures"] >= max_failures:
                            self.logger.error(f"💀 Max consecutive failures ({max_failures}) reached. Pausing...")
                            await asyncio.sleep(recovery_delay)
                            self.continuous_stats["consecutive_failures"] = 0

                except Exception as e:
                    self.continuous_stats["failed_cycles"] += 1
                    self.continuous_stats["consecutive_failures"] += 1
                    self.logger.error(f"💥 Cycle {self.continuous_stats['total_cycles']} crashed: {e}")

                    if self.continuous_stats["consecutive_failures"] >= max_failures:
                        self.logger.error(f"🚨 Recovery mode: sleeping {recovery_delay}s")
                        await asyncio.sleep(recovery_delay)
                        self.continuous_stats["consecutive_failures"] = 0

                self.continuous_stats["last_cycle_time"] = datetime.now()

                # Log periodic stats
                if self.continuous_stats["total_cycles"] % 10 == 0:
                    self._log_stats()

                # Wait for next cycle
                if not self.stop_requested:
                    await asyncio.sleep(interval)

            self.logger.info("🛑 Enhanced continuous loop stopped")

        except KeyboardInterrupt:
            self.logger.info("⌨️ Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"💀 Enhanced continuous loop fatal error: {e}")
            raise
        finally:
            self._log_final_stats()

    def _log_stats(self):
        """Log periodic statistics"""
        total = self.continuous_stats["total_cycles"]
        successful = self.continuous_stats["successful_cycles"]
        success_rate = (successful / total * 100) if total > 0 else 0
        runtime = (datetime.now() - self.continuous_stats["start_time"]).total_seconds()
        cycles_per_hour = (total / runtime) * 3600 if runtime > 0 else 0

        download_info = ""
        if self.download_enabled:
            download_info = f", {self.continuous_stats['total_videos_downloaded']} videos, {self.continuous_stats['total_files_created']} files"

        self.logger.info(
            f"📊 Enhanced Stats: {total} cycles, {success_rate:.1f}% success rate, "
            f"{cycles_per_hour:.1f} cycles/hour, {self.continuous_stats['total_posts_processed']} posts{download_info}"
        )

    def _log_final_stats(self):
        """Log final statistics"""
        self._log_stats()
        self.logger.info("📋 Final enhanced continuous execution statistics logged")


async def main():
    """Enhanced main entry point with download integration"""
    parser = argparse.ArgumentParser(
        description="FikFap Complete Scraping and Video Download System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --demo                      # Run demonstration with downloads
  %(prog)s --single                    # Run single scraping cycle only  
  %(prog)s --single-download           # Run single cycle with downloads
  %(prog)s --download-only             # Download from existing data
  %(prog)s --continuous-download       # Run continuous with downloads
  %(prog)s --continuous                # Run continuous scraping only
  %(prog)s --health-check              # Run system health check
"""
    )

    # Main operation modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--demo', action='store_true', help='Run demonstration workflow with downloads')
    group.add_argument('--single', action='store_true', help='Run single scraping cycle only')
    group.add_argument('--single-download', action='store_true', help='Run single cycle with video downloads')
    group.add_argument('--download-only', action='store_true', help='Download videos from existing posts file')
    group.add_argument('--continuous', action='store_true', help='Run continuous scraping loop')
    group.add_argument('--continuous-download', action='store_true', help='Run continuous loop with downloads')
    group.add_argument('--health-check', action='store_true', help='Run system health check')
    group.add_argument('--create-config', type=str, metavar='PATH', help='Create sample config file')

    # Configuration options
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--posts-file', type=str, default='all_raw_posts.json', help='Posts file for download-only mode')
    parser.add_argument('--interval', type=int, default=300, help='Loop interval in seconds (default: 300)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Logging level')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')

    args = parser.parse_args()

    # Handle config creation
    if args.create_config:
        from main import create_sample_config  # Import from original main
        create_sample_config(args.create_config)
        return

    # Setup logging level
    log_level = 'DEBUG' if args.verbose else args.log_level

    # Create enhanced main application
    app = FikFapMainApplicationWithDownloader(config_path=args.config, log_level=log_level)

    try:
        if args.demo:
            await app.run_demo()
        elif args.single:
            result = await app.run_single_cycle()
            if not result.get("success", False):
                sys.exit(1)
        elif args.single_download:
            result = await app.run_single_cycle_with_download()
            if not result.get("success", False):
                sys.exit(1)
        elif args.download_only:
            result = await app.download_existing_posts(args.posts_file)
            if result.get("error"):
                sys.exit(1)
        elif args.continuous:
            await app.run_continuous_loop(args.interval)
        elif args.continuous_download:
            await app.run_continuous_loop_with_download(args.interval)
        elif args.health_check:
            result = await app.run_health_check()
            if not result.get("overall_status", {}).get("healthy", False):
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n🛑 Graceful shutdown completed.")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


def run():
    """Synchronous wrapper for async main"""
    asyncio.run(main())


if __name__ == "__main__":
    run()
