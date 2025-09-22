# fikfap_main.py
"""
FikFap Main Entry Point - Complete integration with your existing system.

This is the main entry point that provides:
1. Single cycle execution (scrape 5+9 posts and process them)
2. Continuous loop execution 
3. System health checks
4. Demo mode for testing

Usage:
    python fikfap_main.py --single                    # Run one cycle
    python fikfap_main.py --continuous                # Run continuously 
    python fikfap_main.py --continuous --interval 600 # Run every 10 minutes
    python fikfap_main.py --health-check              # Check system health
    python fikfap_main.py --demo                      # Run demo
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
from core.config import Config, config
from core.exceptions import *
from utils.logger import setup_logger


class FikFapMainApplication:
    """Main application class for FikFap scraper integration."""
    
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
        
        # State
        self.shutdown_requested = False
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, requesting shutdown")
            self.shutdown_requested = True
            
            if self.continuous_runner:
                self.continuous_runner.request_stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def run_single_cycle(self) -> Dict[str, Any]:
        """Run a single scraping and processing cycle."""
        try:
            self.logger.info("Starting single cycle execution")
            
            # Initialize workflow integrator
            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator
                
                # Run single cycle
                result = await integrator.run_single_cycle()
                
                # Log results
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
        """Run continuous scraping and processing loop."""
        try:
            self.logger.info(f"Starting continuous execution (interval: {interval}s)")
            
            # Initialize workflow integrator
            async with FikFapWorkflowIntegrator() as integrator:
                self.workflow_integrator = integrator
                
                # Create continuous runner
                config_override = {"continuous.loop_interval": interval}
                self.continuous_runner = FikFapContinuousRunner(integrator, config_override)
                
                # Start continuous loop
                await self.continuous_runner.run_continuous_loop()
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Continuous loop execution failed: {e}")
            raise
    
    async def run_health_check(self) -> Dict[str, Any]:
        """Run comprehensive system health check."""
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
                
                # Test write permissions
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
                
                # Log specific issues
                for check_name, result in health_results.items():
                    if not result.get("healthy", True):
                        self.logger.error(f"  {check_name}: {result.get('details', 'Unknown error')}")
            
            return health_results
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "overall_status": {"healthy": False},
                "error": str(e)
            }
    
    async def run_demo(self) -> None:
        """Run a demonstration of the complete workflow."""
        try:
            self.logger.info("🚀 Starting FikFap Integration Demo")
            
            # Step 1: Health check
            self.logger.info("📊 Step 1: Running system health check...")
            health_results = await self.run_health_check()
            
            if not health_results.get("overall_status", {}).get("healthy", False):
                self.logger.error("❌ System health check failed - cannot continue demo")
                return
            
            self.logger.info("✅ Step 1 completed: System health check passed")
            
            # Step 2: Single cycle demonstration
            self.logger.info("🔄 Step 2: Running demonstration cycle...")
            cycle_result = await self.run_single_cycle()
            
            if cycle_result.get("success", False):
                posts_scraped = cycle_result.get("posts_scraped", 0)
                posts_processed = cycle_result.get("posts_processed", 0)
                duration = cycle_result.get("cycle_duration", 0)
                
                self.logger.info(
                    f"✅ Step 2 completed: Demonstration cycle successful!\n"
                    f"  📥 Posts scraped from API: {posts_scraped}\n"
                    f"  ⚡ Posts processed: {posts_processed}\n"
                    f"  ⏱️  Duration: {duration:.2f} seconds"
                )
                
                # Show some processing details
                if cycle_result.get("processing_records"):
                    self.logger.info("📋 Processing Details:")
                    for record in cycle_result["processing_records"][:3]:  # Show first 3
                        post_id = record.get("post_id", "unknown")
                        status = record.get("status", "unknown")
                        self.logger.info(f"  Post {post_id}: {status}")
                    
                    if len(cycle_result["processing_records"]) > 3:
                        remaining = len(cycle_result["processing_records"]) - 3
                        self.logger.info(f"  ... and {remaining} more posts")
                
            else:
                error = cycle_result.get("error", "Unknown error")
                self.logger.error(f"❌ Step 2 failed: {error}")
                return
            
            # Step 3: Demo completed
            self.logger.info("🎉 Step 3: Demo completed successfully!")
            self.logger.info(
                "🔧 The system is ready for production use:\n"
                "  • Use --single for one-time runs\n"
                "  • Use --continuous for automated looping\n"
                "  • Use --health-check for system monitoring"
            )
            
        except Exception as e:
            self.logger.error(f"❌ Demo failed: {e}")


def create_sample_config(config_path: str):
    """Create a sample configuration file."""
    sample_config = {
        "scraper": {
            "headless": True,
            "slow_mo": 0,
            "timeout": 30000,
            "max_retries": 3
        },
        "processing": {
            "max_concurrent": 2,
            "quality_filter": None
        },
        "storage": {
            "downloads_dir": "./downloads",
            "create_subdirs": True
        },
        "continuous": {
            "loop_interval": 300,
            "max_consecutive_failures": 5,
            "recovery_delay": 60
        },
        "logging": {
            "level": "INFO",
            "file": "logs/fikfap_scraper.log"
        }
    }
    
    # Create config directory if needed
    config_file = Path(config_path)
    config_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write config
    with open(config_file, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"Sample configuration created: {config_path}")


async def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description="FikFap Complete Scraping and Processing System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --demo                          # Run demonstration
  %(prog)s --single                        # Run single cycle (scrape 5+9 posts)
  %(prog)s --continuous                    # Run continuously (5min intervals)
  %(prog)s --continuous --interval 600     # Run continuously (10min intervals)
  %(prog)s --health-check                  # Run system health check
  %(prog)s --create-config config.json     # Create sample configuration
        """
    )
    
    # Main operation modes
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--demo', action='store_true', help='Run demonstration workflow')
    group.add_argument('--single', action='store_true', help='Run single cycle (5+9 posts)')
    group.add_argument('--continuous', action='store_true', help='Run continuous processing loop')
    group.add_argument('--health-check', action='store_true', help='Run system health check')
    group.add_argument('--create-config', type=str, metavar='PATH', help='Create sample config file')
    
    # Configuration options
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--interval', type=int, default=300, help='Loop interval in seconds (default: 300)')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help='Logging level')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Handle config creation
    if args.create_config:
        create_sample_config(args.create_config)
        return
    
    # Setup logging level
    log_level = 'DEBUG' if args.verbose else args.log_level
    
    # Create main application
    app = FikFapMainApplication(config_path=args.config, log_level=log_level)
    
    try:
        if args.demo:
            await app.run_demo()
        elif args.single:
            result = await app.run_single_cycle()
            if not result.get("success", False):
                sys.exit(1)
        elif args.continuous:
            await app.run_continuous_loop(args.interval)
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
    """Synchronous wrapper for async main."""
    asyncio.run(main())


if __name__ == "__main__":
    run()