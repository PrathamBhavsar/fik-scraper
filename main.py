#!/usr/bin/env python3
"""
Phase 5: Main Entry Point with Complete Orchestration
FikFap Scraper - Production-Ready Main Application

Complete integration of all phases with robust orchestration,
error handling, and production-ready features.
"""
import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime
import argparse
import json
from typing import List, Dict, Any, Optional

# Add project root to Python path
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from orchestrator import FikFapScraperOrchestrator
from core.config import config
from core.exceptions import *
from utils.logger import setup_logger

class FikFapScraperApplication:
    """
    Main application class for the FikFap scraper
    
    Provides command-line interface and high-level operations
    """
    
    def __init__(self):
        """Initialize the application"""
        self.logger = setup_logger("fikfap_app", config.log_level, config.log_file)
        self.orchestrator: Optional[FikFapScraperOrchestrator] = None
    
    async def run_single_video(self, post_id: int, quality_filter: Optional[List[str]] = None) -> bool:
        """
        Process a single video
        
        Args:
            post_id: Post ID to process
            quality_filter: Optional quality filter
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info(f"Processing single video: {post_id}")
            
            async with FikFapScraperOrchestrator() as orchestrator:
                result = await orchestrator.process_video_workflow(
                    post_id=post_id,
                    quality_filter=quality_filter
                )
                
                if result['success']:
                    self.logger.info(f"✅ Successfully processed video {post_id}")
                    self.logger.info(f"   Duration: {result['duration']:.2f}s")
                    self.logger.info(f"   Files created: {result['stats']['files_created']}")
                    self.logger.info(f"   Total size: {result['stats']['total_size_bytes']:,} bytes")
                    return True
                else:
                    self.logger.error(f"❌ Failed to process video {post_id}: {result.get('error', 'Unknown error')}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"❌ Application error processing video {post_id}: {e}")
            return False
    
    async def run_batch_processing(
        self, 
        post_ids: List[int], 
        max_concurrent: Optional[int] = None,
        quality_filter: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Process multiple videos in batch
        
        Args:
            post_ids: List of post IDs to process
            max_concurrent: Maximum concurrent processing
            quality_filter: Optional quality filter
            
        Returns:
            Batch processing results
        """
        try:
            self.logger.info(f"Starting batch processing of {len(post_ids)} videos")
            
            async with FikFapScraperOrchestrator() as orchestrator:
                result = await orchestrator.process_multiple_videos(
                    post_ids=post_ids,
                    max_concurrent=max_concurrent,
                    quality_filter=quality_filter
                )
                
                summary = result['summary']
                self.logger.info("📊 Batch Processing Results:")
                self.logger.info(f"   Total: {summary['total']}")
                self.logger.info(f"   Successful: {summary['successful']}")
                self.logger.info(f"   Failed: {summary['failed']}")
                self.logger.info(f"   Skipped: {summary['skipped']}")
                self.logger.info(f"   Duration: {summary['duration']:.2f}s")
                self.logger.info(f"   Rate: {summary['videos_per_second']:.2f} videos/second")
                
                return result
                
        except Exception as e:
            self.logger.error(f"❌ Batch processing error: {e}")
            return {'success': False, 'error': str(e)}
    
    async def run_system_check(self) -> Dict[str, Any]:
        """
        Perform system health check and show status
        
        Returns:
            System status information
        """
        try:
            self.logger.info("🏥 Performing system health check...")
            
            # Initialize orchestrator for health checks
            orchestrator = FikFapScraperOrchestrator()
            
            # Perform startup health checks
            await orchestrator._perform_startup_health_checks()
            
            # Get system status
            status = orchestrator.get_system_status()
            
            self.logger.info("✅ System Health Report:")
            self.logger.info(f"   System Status: {'Healthy' if status['system'].get('is_healthy', True) else 'Issues Detected'}")
            self.logger.info(f"   Disk Space: {status['system'].get('diskSpaceGb', 0):.2f}GB free")
            self.logger.info(f"   Memory Usage: {status['system'].get('memoryUsagePercent', 0):.1f}%")
            self.logger.info(f"   CPU Usage: {status['system'].get('cpuUsagePercent', 0):.1f}%")
            
            components = status['components']
            self.logger.info("🔧 Components Status:")
            for component, available in components.items():
                status_icon = "✅" if available else "❌"
                self.logger.info(f"   {status_icon} {component}")
            
            return status
            
        except Exception as e:
            self.logger.error(f"❌ System check error: {e}")
            return {'error': str(e)}
    
    async def run_interactive_mode(self):
        """Run interactive mode for testing and development"""
        self.logger.info("🎮 Starting interactive mode...")
        
        print("FikFap Scraper - Interactive Mode")
        print("=" * 40)
        print("Commands:")
        print("  video <post_id>     - Process single video")
        print("  batch <ids...>      - Process multiple videos")
        print("  status             - Show system status") 
        print("  stats              - Show processing statistics")
        print("  help               - Show this help")
        print("  exit               - Exit interactive mode")
        print()
        
        try:
            async with FikFapScraperOrchestrator() as orchestrator:
                while True:
                    try:
                        command = input("fikfap> ").strip()
                        
                        if not command:
                            continue
                        
                        parts = command.split()
                        cmd = parts[0].lower()
                        
                        if cmd == 'exit':
                            break
                        elif cmd == 'help':
                            print("Available commands: video, batch, status, stats, help, exit")
                        elif cmd == 'video' and len(parts) >= 2:
                            try:
                                post_id = int(parts[1])
                                result = await orchestrator.process_video_workflow(post_id)
                                print(f"Result: {result['success']}")
                                if result['success']:
                                    print(f"Duration: {result['duration']:.2f}s")
                            except ValueError:
                                print("Error: Invalid post ID")
                        elif cmd == 'batch' and len(parts) >= 2:
                            try:
                                post_ids = [int(x) for x in parts[1:]]
                                result = await orchestrator.process_multiple_videos(post_ids)
                                print(f"Batch result: {result['summary']}")
                            except ValueError:
                                print("Error: Invalid post IDs")
                        elif cmd == 'status':
                            status = orchestrator.get_system_status()
                            print(json.dumps(status, indent=2, default=str))
                        elif cmd == 'stats':
                            stats = orchestrator.stats
                            print(f"Processing Statistics:")
                            print(f"  Videos Processed: {stats['videos_processed']}")
                            print(f"  Videos Failed: {stats['videos_failed']}")
                            print(f"  Videos Skipped: {stats['videos_skipped']}")
                            print(f"  Total Bytes: {stats['total_bytes_downloaded']:,}")
                        else:
                            print("Unknown command. Type 'help' for available commands.")
                    
                    except KeyboardInterrupt:
                        break
                    except Exception as e:
                        print(f"Error: {e}")
                        
        except Exception as e:
            self.logger.error(f"Interactive mode error: {e}")
        
        print("Exiting interactive mode...")
    
    def setup_argument_parser(self) -> argparse.ArgumentParser:
        """Set up command line argument parser"""
        parser = argparse.ArgumentParser(
            description="FikFap Scraper - Complete Video Processing System"
        )
        
        # Main command
        subparsers = parser.add_subparsers(dest='command', help='Available commands')
        
        # Single video processing
        video_parser = subparsers.add_parser('video', help='Process a single video')
        video_parser.add_argument('post_id', type=int, help='Post ID to process')
        video_parser.add_argument('--quality', nargs='+', help='Quality filter (e.g. 1080p 720p)')
        
        # Batch processing
        batch_parser = subparsers.add_parser('batch', help='Process multiple videos')
        batch_parser.add_argument('post_ids', type=int, nargs='+', help='Post IDs to process')
        batch_parser.add_argument('--max-concurrent', type=int, help='Maximum concurrent downloads')
        batch_parser.add_argument('--quality', nargs='+', help='Quality filter (e.g. 1080p 720p)')
        
        # System operations
        subparsers.add_parser('check', help='Perform system health check')
        subparsers.add_parser('interactive', help='Start interactive mode')
        
        # Configuration
        parser.add_argument('--config', help='Configuration file path')
        parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                          help='Logging level')
        
        return parser

async def main():
    """Main entry point"""
    app = FikFapScraperApplication()
    
    try:
        # Parse command line arguments
        parser = app.setup_argument_parser()
        args = parser.parse_args()
        
        # Apply configuration overrides
        config_override = {}
        if args.log_level:
            config_override['log_level'] = args.log_level
        
        # Execute command
        if args.command == 'video':
            success = await app.run_single_video(
                post_id=args.post_id,
                quality_filter=args.quality
            )
            return 0 if success else 1
            
        elif args.command == 'batch':
            result = await app.run_batch_processing(
                post_ids=args.post_ids,
                max_concurrent=args.max_concurrent,
                quality_filter=args.quality
            )
            return 0 if result.get('success', False) else 1
            
        elif args.command == 'check':
            status = await app.run_system_check()
            return 0 if 'error' not in status else 1
            
        elif args.command == 'interactive':
            await app.run_interactive_mode()
            return 0
            
        else:
            # No command specified, show help and run default demo
            parser.print_help()
            print("\nRunning default demo...")
            return await run_default_demo(app)
            
    except KeyboardInterrupt:
        app.logger.info("Application interrupted by user")
        return 1
    except Exception as e:
        app.logger.error(f"Application error: {e}")
        return 1

async def run_default_demo(app: FikFapScraperApplication) -> int:
    """Run default demo showing system capabilities"""
    logger = app.logger
    
    try:
        logger.info("🎬 FikFap Scraper - Phase 5: Complete System Integration")
        logger.info("=" * 80)
        
        # System check
        logger.info("Step 1: System Health Check")
        status = await app.run_system_check()
        if 'error' in status:
            logger.error("System check failed, aborting demo")
            return 1
        
        logger.info("Step 2: Single Video Processing Demo")
        demo_post_id = 12345
        success = await app.run_single_video(demo_post_id)
        
        if success:
            logger.info("Step 3: Batch Processing Demo")
            demo_post_ids = [12346, 12347, 12348]
            batch_result = await app.run_batch_processing(demo_post_ids, max_concurrent=2)
            
            if batch_result.get('success'):
                logger.info("🎉 Demo completed successfully!")
                logger.info("✅ All Phase 5 features demonstrated:")
                logger.info("   - Complete workflow orchestration")
                logger.info("   - Dependency injection and component integration")
                logger.info("   - Error handling and recovery")
                logger.info("   - System health monitoring")
                logger.info("   - Batch processing with concurrency control")
                logger.info("   - Graceful startup and shutdown")
                logger.info("=" * 80)
                return 0
        
        logger.error("Demo encountered errors")
        return 1
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        return 1

def run():
    """Synchronous entry point"""
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nApplication interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()