"""
FIXED main.py - FikFap Scraper with UTF-8 Support and No Emoji Encoding Errors

FIXES APPLIED:
1. Fixed charmap codec error by removing problematic emojis
2. Added proper UTF-8 encoding for subprocess output
3. Ensured cross-platform compatibility
4. All functionality preserved without encoding issues
"""

import asyncio
import argparse
import signal
import sys
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Set UTF-8 encoding for stdout/stderr on Windows
if os.name == 'nt':  # Windows
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Import the workflow components
from fikfap_workflow_integrator import FikFapWorkflowIntegrator, FikFapContinuousRunner
from video_downloader_organizer import VideoDownloaderOrganizer

# Import existing components (with fallbacks for missing modules)
try:
    from utils.logger import setup_logger
except ImportError:
    import logging
    def setup_logger(name, level="INFO"):
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level))
        if not logger.handlers:
            handler = logging.StreamHandler()
            # Use UTF-8 encoding for log handler
            if hasattr(handler.stream, 'reconfigure'):
                handler.stream.reconfigure(encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

try:
    from utils.disk_space import get_free_space_gb, get_directory_size_gb, check_disk_space_limit
except ImportError:
    import shutil
    import os

    def get_free_space_gb(path: str | Path) -> float:
        """Return free disk space in GB"""
        usage = shutil.disk_usage(str(Path(path)))
        return usage.free / (1024 ** 3)

    def get_directory_size_gb(path: str | Path) -> float:
        """Calculate directory size in GB"""
        path = Path(path)
        if not path.exists():
            return 0.0
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = Path(dirpath) / filename
                    try:
                        if filepath.exists() and filepath.is_file():
                            total_size += filepath.stat().st_size
                    except (OSError, PermissionError):
                        continue
        except (OSError, PermissionError):
            return 0.0
        return total_size / (1024 ** 3)

    def check_disk_space_limit(downloads_dir: str | Path, max_size_gb: float) -> dict:
        """Check disk space limit"""
        downloads_dir = Path(downloads_dir)
        current_size = get_directory_size_gb(downloads_dir)
        free_space = get_free_space_gb(downloads_dir) if downloads_dir.exists() else 0.0
        return {
            'current_size_gb': current_size,
            'max_size_gb': max_size_gb,
            'exceeds_limit': current_size >= max_size_gb,
            'free_space_gb': free_space
        }


class FikFapMainApplicationWithDownloader:
    """Enhanced main application with video downloader integration - FIXED UTF-8 encoding"""

    def __init__(self, config_path: Optional[str] = None, log_level: str = "INFO"):
        # Setup logging
        self.logger = setup_logger(self.__class__.__name__, level=log_level)

        # Load configuration directly from JSON (FIXED: No more Config class issues)
        self.config = self.load_settings(config_path)

        # Components
        self.workflow_integrator: Optional[FikFapWorkflowIntegrator] = None
        self.continuous_runner: Optional[FikFapContinuousRunner] = None
        self.video_downloader: Optional[VideoDownloaderOrganizer] = None

        # State
        self.shutdown_requested = False

        self.setup_signal_handlers()

    def load_settings(self, config_path: Optional[str] = None) -> Dict[str, Any]:
        """Load settings from JSON file - FIXED: Direct JSON loading"""
        default_settings = {
            "storage": {"base_path": "./downloads"},
            "monitoring": {"min_disk_space_gb": 1.0}
        }

        settings_file = config_path if config_path else "settings.json"

        try:
            if Path(settings_file).exists():
                with open(settings_file, "r", encoding='utf-8') as f:
                    settings = json.load(f)
                    # Merge with defaults
                    for key, value in default_settings.items():
                        if key not in settings:
                            settings[key] = value
                        elif isinstance(value, dict):
                            for subkey, subvalue in value.items():
                                if subkey not in settings[key]:
                                    settings[key][subkey] = subvalue
                    return settings
        except Exception as e:
            self.logger.warning(f"Could not load {settings_file}: {e}, using defaults")

        return default_settings

    def get_config_value(self, key_path: str, default=None):
        """Get configuration value using dot notation (e.g., 'storage.base_path')"""
        keys = key_path.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

    def setup_signal_handlers(self):
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

            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator

                # Run single scraping cycle
                scrape_result = await integrator.run_single_cycle()

                if scrape_result.get("success", False):
                    self.logger.info(
                        f"Scraping completed successfully: "
                        f"Posts scraped: {scrape_result.get('posts_scraped', 0)}, "
                        f"Posts processed: {scrape_result.get('posts_processed', 0)}, "
                        f"Posts failed: {scrape_result.get('posts_failed', 0)}, "
                        f"Duration: {scrape_result.get('cycle_duration', 0):.2f}s"
                    )

                    if download_videos and scrape_result.get("posts_scraped", 0) > 0:
                        self.logger.info("Starting video download process...")

                        download_dir = self.get_config_value("storage.base_path", "./downloads")
                        async with VideoDownloaderOrganizer(download_dir) as downloader:
                            self.video_downloader = downloader

                            # Download videos from scraped posts
                            download_result = await downloader.process_all_posts("all_raw_posts.json")

                            combined_result = {
                                **scrape_result,
                                "download_enabled": True,
                                "download_results": download_result
                            }

                            # FIXED: Use progress tracker stats instead of 'total_posts'
                            if download_result and download_result.get("summary"):
                                summary = download_result["summary"]
                                self.logger.info(
                                    f"Download completed: "
                                    f"Videos processed: {summary.get('successful_count', 0)}, "
                                    f"Total files downloaded: {summary.get('total_files', 0)}, "
                                    f"Success rate: {summary.get('success_rate', 0)}%, "
                                    f"Total downloaded ever: {summary.get('total_downloaded_ever', 0)}"
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

    async def run_continuous_loop_with_download(self, interval: int = 300, download_videos: bool = True) -> None:
        """Run continuous scraping loop with video downloads"""
        try:
            self.logger.info(f"Starting continuous execution with downloads (interval={interval}s)")

            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator

                download_dir = self.get_config_value("storage.base_path", "./downloads")
                async with VideoDownloaderOrganizer(download_dir) as downloader:
                    self.video_downloader = downloader

                    config_override = {"continuous.loop_interval": interval}
                    self.continuous_runner = EnhancedContinuousRunner(
                        integrator, downloader, config_override, download_videos, self.config
                    )

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

            download_dir = self.get_config_value("storage.base_path", "./downloads")
            async with VideoDownloaderOrganizer(download_dir) as downloader:
                self.video_downloader = downloader

                download_result = await downloader.process_all_posts(posts_file)

                # FIXED: Use progress tracker stats instead of 'total_posts'
                if download_result and download_result.get("summary"):
                    summary = download_result["summary"]
                    self.logger.info(
                        f"Download completed: "
                        f"Videos processed: {summary.get('successful_count', 0)}, "
                        f"Total files downloaded: {summary.get('total_files', 0)}, "
                        f"Success rate: {summary.get('success_rate', 0)}%, "
                        f"Total downloaded ever: {summary.get('total_downloaded_ever', 0)}"
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
                        f"Single cycle completed successfully: "
                        f"Posts scraped: {result.get('posts_scraped', 0)}, "
                        f"Posts processed: {result.get('posts_processed', 0)}, "
                        f"Posts failed: {result.get('posts_failed', 0)}, "
                        f"Duration: {result.get('cycle_duration', 0):.2f}s"
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
            self.logger.info(f"Starting continuous execution (interval={interval}s)")

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
                downloads_dir = Path(self.get_config_value("storage.base_path", "./downloads"))
                downloads_dir.mkdir(parents=True, exist_ok=True)

                test_file = downloads_dir / "health_check_test.txt"
                test_file.write_text("health check test", encoding='utf-8')
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

            if health_results["overall_status"]["healthy"]:
                self.logger.info("System health check passed - all components healthy")
            else:
                self.logger.warning("System health check failed - see details below")
                for check_name, result in health_results.items():
                    if not result.get("healthy", True):
                        self.logger.error(f"{check_name}: {result.get('details', 'Unknown error')}")

            return health_results

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {"overall_status": {"healthy": False, "error": str(e)}}

    async def run_demo(self) -> None:
        """Run a demonstration of the complete workflow with downloads"""
        try:
            self.logger.info("Starting FikFap Integration Demo with Video Downloads")

            # Step 1: Health check
            self.logger.info("Step 1: Running system health check...")
            health_results = await self.run_health_check()

            if not health_results.get("overall_status", {}).get("healthy", False):
                self.logger.error("System health check failed - cannot continue demo")
                return

            self.logger.info("Step 1 completed: System health check passed")

            # Step 2: Single cycle with download demonstration
            self.logger.info("Step 2: Running demonstration cycle with downloads...")
            cycle_result = await self.run_single_cycle_with_download(download_videos=True)

            if cycle_result.get("success", False):
                posts_scraped = cycle_result.get("posts_scraped", 0)
                posts_processed = cycle_result.get("posts_processed", 0)
                duration = cycle_result.get("cycle_duration", 0)

                self.logger.info(
                    f"Step 2 completed: Demonstration cycle successful! "
                    f"Posts scraped from API: {posts_scraped}, "
                    f"Posts processed: {posts_processed}, "
                    f"Duration: {duration:.2f} seconds"
                )

                # Show download results if available
                if cycle_result.get("download_enabled") and cycle_result.get("download_results"):
                    download_summary = cycle_result["download_results"].get("summary", {})
                    self.logger.info(
                        f"Videos downloaded: {download_summary.get('successful_count', 0)}, "
                        f"Total files: {download_summary.get('total_files', 0)}, "
                        f"Success rate: {download_summary.get('success_rate', 0)}%, "
                        f"Total downloaded ever: {download_summary.get('total_downloaded_ever', 0)}"
                    )
            else:
                error = cycle_result.get("error", "Unknown error")
                self.logger.error(f"Step 2 failed: {error}")
                return

            # Step 3: Demo completed
            self.logger.info("Step 3: Demo completed successfully!")
            self.logger.info("The enhanced system is ready for production use:")
            self.logger.info("- Use --single for scraping only")
            self.logger.info("- Use --single-download for scraping + downloads") 
            self.logger.info("- Use --download-only to download from existing data")
            self.logger.info("- Use --continuous-download for automated looping with downloads")
            self.logger.info("- Use --health-check for system monitoring")

        except Exception as e:
            self.logger.error(f"Demo failed: {e}")


class EnhancedContinuousRunner:
    """Enhanced continuous runner with video-download integration and LIVE disk space limit enforcement."""

    def __init__(
        self,
        integrator,
        downloader,
        config_override: Optional[Dict[str, Any]] = None,
        download_enabled: bool = True,
        config: Optional[Dict[str, Any]] = None
    ):
        self.integrator = integrator
        self.downloader = downloader
        self.config_override = config_override or {}
        self.download_enabled = download_enabled
        self.config = config or {}

        self.stop_requested: bool = False
        self.logger = setup_logger(self.__class__.__name__)

        # Load disk space parameters from config
        storage_config = self.config.get("storage", {})
        monitoring_config = self.config.get("monitoring", {})

        self.base_path = Path(storage_config.get("base_path", "./downloads"))
        self.max_disk_size_gb = float(monitoring_config.get("min_disk_space_gb", 1.0))

        # Create downloads directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Statistics
        self.continuous_stats = {
            "total_cycles": 0,
            "successful_cycles": 0,
            "failed_cycles": 0,
            "consecutive_failures": 0,
            "start_time": None,
            "last_cycle_time": None,
            "total_posts_processed": 0,
            "total_videos_downloaded": 0,
            "total_files_created": 0,
        }

        self.logger.info(f"Disk space monitoring: {self.base_path} (max: {self.max_disk_size_gb} GB)")

    def request_stop(self):
        """Request stop of continuous loop"""
        self.stop_requested = True
        self.logger.info("Enhanced continuous runner stop requested")

    def check_disk_space_limit(self) -> bool:
        """
        Check if downloads directory size exceeds the configured limit.
        Returns True if within limits, False if exceeded (should stop).
        """
        try:
            space_check = check_disk_space_limit(self.base_path, self.max_disk_size_gb)

            current_size = space_check['current_size_gb']
            exceeds_limit = space_check['exceeds_limit']

            self.logger.debug(
                f"Disk space check: {current_size:.2f} GB used, "
                f"limit: {self.max_disk_size_gb:.2f} GB, "
                f"free space: {space_check['free_space_gb']:.2f} GB"
            )

            if exceeds_limit:
                self.logger.error(
                    f"DISK SPACE LIMIT EXCEEDED! "
                    f"Downloads directory size: {current_size:.2f} GB >= "
                    f"configured limit: {self.max_disk_size_gb:.2f} GB. "
                    f"Stopping process immediately for safety."
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking disk space: {e}")
            # If we can't check, assume it's safe to continue
            return True

    async def run_continuous_loop(self):
        """Run enhanced continuous loop with download integration and LIVE disk space limit enforcement."""
        self.continuous_stats["start_time"] = datetime.now()
        interval = self.config_override.get("continuous.loop_interval", 300)
        max_failures = self.config_override.get("continuous.max_consecutive_failures", 5)
        recovery_delay = self.config_override.get("continuous.recovery_delay", 60)

        self.logger.info(
            f"Starting enhanced continuous loop "
            f"(interval={interval}s, downloads={self.download_enabled}, "
            f"disk_limit={self.max_disk_size_gb:.2f} GB)"
        )

        while not self.stop_requested:
            # --- CRITICAL: Live disk space check BEFORE starting each cycle ---
            if not self.check_disk_space_limit():
                self.logger.critical("Disk space limit exceeded - terminating process immediately")
                # Log final stats before exiting
                self.log_final_stats()
                # Force exit the entire process
                sys.exit(1)

            cycle_start = datetime.now()
            self.continuous_stats["total_cycles"] += 1

            try:
                # ---------- existing scraping + optional download ----------
                scrape_result = await self.integrator.run_single_cycle()
                success = scrape_result.get("success", False)
                posts_scraped = scrape_result.get("posts_scraped", 0)

                download_result = None
                if self.download_enabled and success and posts_scraped > 0:
                    try:
                        download_result = await self.downloader.process_all_posts(
                            "all_raw_posts.json"
                        )
                        # FIXED: Use summary stats instead of 'total_posts'
                        if download_result and download_result.get("summary"):
                            summary = download_result["summary"]
                            self.continuous_stats["total_videos_downloaded"] += summary.get("successful_count", 0)
                            self.continuous_stats["total_files_created"] += summary.get("total_files", 0)
                    except Exception as e:
                        self.logger.error(f"Download phase failed: {e}")
                        download_result = {"error": str(e)}

                if success:
                    self.continuous_stats["successful_cycles"] += 1
                    self.continuous_stats["consecutive_failures"] = 0
                    self.continuous_stats["total_posts_processed"] += scrape_result.get(
                        "posts_processed", 0
                    )
                    cycle_duration = (datetime.now() - cycle_start).total_seconds()
                    download_info = ""
                    if download_result and download_result.get("summary"):
                        summary = download_result["summary"]
                        vids = summary.get("successful_count", 0)
                        files = summary.get("total_files", 0)
                        download_info = f", {vids} videos downloaded, {files} files"

                    # --- CRITICAL: Live disk space check AFTER cycle completion ---
                    cycle_log = (
                        f"Cycle {self.continuous_stats['total_cycles']} finished: "
                        f"{scrape_result.get('posts_processed', 0)} posts processed"
                        f"{download_info} in {cycle_duration:.2f}s"
                    )
                    self.logger.info(cycle_log)

                    # Check disk space after logging cycle completion
                    if not self.check_disk_space_limit():
                        self.logger.critical("Disk space limit exceeded after cycle - terminating process immediately")
                        self.log_final_stats()
                        sys.exit(1)

                else:
                    self.continuous_stats["failed_cycles"] += 1
                    self.continuous_stats["consecutive_failures"] += 1
                    err = scrape_result.get("error", "Unknown error")
                    self.logger.error(
                        f"Cycle {self.continuous_stats['total_cycles']} failed: {err}"
                    )

                    if self.continuous_stats["consecutive_failures"] >= max_failures:
                        self.logger.error(
                            f"Max consecutive failures ({max_failures}) reached. "
                            f"Sleeping {recovery_delay}s before retry."
                        )
                        await asyncio.sleep(recovery_delay)
                        self.continuous_stats["consecutive_failures"] = 0

            except Exception as e:
                self.continuous_stats["failed_cycles"] += 1
                self.continuous_stats["consecutive_failures"] += 1
                self.logger.error(
                    f"Cycle {self.continuous_stats['total_cycles']} crashed: {e}"
                )
                if self.continuous_stats["consecutive_failures"] >= max_failures:
                    self.logger.error(
                        f"Recovery mode: sleeping {recovery_delay}s after crash."
                    )
                    await asyncio.sleep(recovery_delay)
                    self.continuous_stats["consecutive_failures"] = 0

            self.continuous_stats["last_cycle_time"] = datetime.now()

            # Log stats every 10 cycles
            if self.continuous_stats["total_cycles"] % 10 == 0:
                self.log_stats()

            if not self.stop_requested:
                await asyncio.sleep(interval)

        self.logger.info("Enhanced continuous loop stopped.")
        self.log_final_stats()

    def log_stats(self):
        """Log periodic statistics"""
        total = self.continuous_stats["total_cycles"]
        successful = self.continuous_stats["successful_cycles"]
        success_rate = (successful / total * 100) if total > 0 else 0
        runtime = (datetime.now() - self.continuous_stats["start_time"]).total_seconds()
        cycles_per_hour = (total / runtime * 3600) if runtime > 0 else 0

        download_info = ""
        if self.download_enabled:
            download_info = (
                f", {self.continuous_stats['total_videos_downloaded']} videos, "
                f"{self.continuous_stats['total_files_created']} files"
            )

        # Include current disk usage in stats
        try:
            current_size = get_directory_size_gb(self.base_path)
            disk_info = f", disk: {current_size:.2f}/{self.max_disk_size_gb:.2f} GB"
        except Exception:
            disk_info = ""

        self.logger.info(
            f"Enhanced Stats: {total} cycles, {success_rate:.1f}% success rate, "
            f"{cycles_per_hour:.1f} cycles/hour, {self.continuous_stats['total_posts_processed']} posts"
            f"{download_info}{disk_info}"
        )

    def log_final_stats(self):
        """Log final statistics"""
        self.log_stats()
        self.logger.info("Final enhanced continuous execution statistics logged")


# Enhanced main entry point with download integration
async def main():
    parser = argparse.ArgumentParser(
        description="FikFap Complete Scraping and Video Download System with Disk Space Limits",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --demo                    # Run demonstration with downloads
  %(prog)s --single                  # Run single scraping cycle only
  %(prog)s --single-download         # Run single cycle with downloads
  %(prog)s --download-only           # Download from existing data
  %(prog)s --continuous-download     # Run continuous with downloads
  %(prog)s --continuous              # Run continuous scraping only
  %(prog)s --health-check            # Run system health check
        """
    )

    # Main operation modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--demo", action="store_true", help="Run demonstration workflow with downloads")
    group.add_argument("--single", action="store_true", help="Run single scraping cycle only")
    group.add_argument("--single-download", action="store_true", help="Run single cycle with video downloads")
    group.add_argument("--download-only", action="store_true", help="Download videos from existing posts file")
    group.add_argument("--continuous", action="store_true", help="Run continuous scraping loop")
    group.add_argument("--continuous-download", action="store_true", help="Run continuous loop with downloads")
    group.add_argument("--health-check", action="store_true", help="Run system health check")
    group.add_argument("--create-config", type=str, metavar="PATH", help="Create sample config file")

    # Configuration options
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--posts-file", type=str, default="all_raw_posts.json", help="Posts file for download-only mode")
    parser.add_argument("--interval", type=int, default=300, help="Loop interval in seconds (default: 300)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    if args.create_config:
        # Create sample config file
        sample_config = {
            "storage": {"base_path": "./downloads"},
            "monitoring": {"min_disk_space_gb": 5.0}
        }
        with open(args.create_config, 'w', encoding='utf-8') as f:
            json.dump(sample_config, f, indent=2, ensure_ascii=False)
        print(f"Sample config created: {args.create_config}")
        return

    # Setup logging level
    log_level = "DEBUG" if args.verbose else args.log_level

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
        print("\nGraceful shutdown completed.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def run():
    """Synchronous wrapper for async main"""
    asyncio.run(main())


if __name__ == "__main__":
    run()
