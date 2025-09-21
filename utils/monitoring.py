"""
System Monitoring for FikFap Scraper - Phase 4
System resource monitoring and disk space management
"""
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta

from core.config import config
from core.exceptions import *
from data.models import SystemStatus, DiskUsageInfo
from utils.logger import setup_logger
from utils.helpers import format_bytes

# Try to import psutil, fall back to basic monitoring if not available
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

class DiskMonitor:
    """
    Disk space monitoring with alerts and thresholds

    Features:
    - Real-time disk usage monitoring
    - Configurable thresholds and alerts
    - Low space warnings and errors
    """

    def __init__(self, monitored_paths: Optional[List[str]] = None):
        """Initialize disk monitor"""
        self.logger = setup_logger("disk_monitor", config.log_level)

        # Configuration
        self.monitored_paths = monitored_paths or [config.get('storage.base_path', './downloads')]
        self.min_disk_space_gb = config.get('monitoring.min_disk_space_gb', 5.0)
        self.alert_enabled = config.get('monitoring.alert_enabled', True)

        # State
        self.usage_history: Dict[str, List[DiskUsageInfo]] = {}
        self.alert_callbacks: List[Callable[[DiskUsageInfo], None]] = []

        self.logger.info(f"DiskMonitor initialized - Paths: {self.monitored_paths}")

    def add_alert_callback(self, callback: Callable[[DiskUsageInfo], None]):
        """Add alert callback for low disk space"""
        self.alert_callbacks.append(callback)

    def get_disk_usage(self, path: str) -> DiskUsageInfo:
        """Get current disk usage for a path"""
        try:
            path_obj = Path(path)
            if not path_obj.exists():
                path_obj.mkdir(parents=True, exist_ok=True)

            usage = shutil.disk_usage(path)

            total_gb = usage.total / (1024**3)
            used_gb = (usage.total - usage.free) / (1024**3)
            free_gb = usage.free / (1024**3)
            usage_percent = (used_gb / total_gb) * 100 if total_gb > 0 else 0

            disk_info = DiskUsageInfo(
                totalGb=total_gb,
                usedGb=used_gb,
                freeGb=free_gb,
                usagePercent=usage_percent,
                path=path,
                lastChecked=datetime.now()
            )

            # Update history
            if path not in self.usage_history:
                self.usage_history[path] = []

            self.usage_history[path].append(disk_info)

            # Limit history size (keep last 100 entries)
            if len(self.usage_history[path]) > 100:
                self.usage_history[path] = self.usage_history[path][-100:]

            # Check for alerts
            if self.alert_enabled and disk_info.is_low_space:
                self._trigger_low_space_alert(disk_info)

            return disk_info

        except Exception as e:
            self.logger.error(f"Error getting disk usage for {path}: {e}")
            raise Exception(f"Cannot get disk usage: {e}")

    def _trigger_low_space_alert(self, disk_info: DiskUsageInfo):
        """Trigger low space alert"""
        self.logger.warning(f"Low disk space alert: {disk_info.path} - "
                          f"{disk_info.freeGb:.2f}GB free ({disk_info.usagePercent:.1f}% used)")

        # Call registered callbacks
        for callback in self.alert_callbacks:
            try:
                callback(disk_info)
            except Exception as e:
                self.logger.error(f"Error in disk space alert callback: {e}")

    def check_available_space(self, path: str, required_gb: float) -> bool:
        """Check if sufficient disk space is available"""
        try:
            disk_info = self.get_disk_usage(path)
            return disk_info.freeGb >= required_gb
        except Exception as e:
            self.logger.error(f"Error checking available space: {e}")
            return False

    def get_usage_summary(self) -> Dict[str, Any]:
        """Get usage summary for all monitored paths"""
        summary = {
            'paths': {},
            'total_monitored': len(self.monitored_paths),
            'critical_paths': [],
            'warning_paths': [],
            'healthy_paths': [],
            'last_updated': datetime.now().isoformat()
        }

        try:
            for path in self.monitored_paths:
                disk_info = self.get_disk_usage(path)

                path_summary = {
                    'total_gb': disk_info.totalGb,
                    'used_gb': disk_info.usedGb,
                    'free_gb': disk_info.freeGb,
                    'usage_percent': disk_info.usagePercent,
                    'is_low_space': disk_info.is_low_space,
                    'last_checked': disk_info.lastChecked.isoformat()
                }

                summary['paths'][path] = path_summary

                # Categorize paths by health
                if disk_info.freeGb < 1.0:  # Critical: Less than 1GB free
                    summary['critical_paths'].append(path)
                elif disk_info.is_low_space:  # Warning: Above threshold
                    summary['warning_paths'].append(path)
                else:  # Healthy
                    summary['healthy_paths'].append(path)

            return summary

        except Exception as e:
            self.logger.error(f"Error getting usage summary: {e}")
            summary['error'] = str(e)
            return summary

class SystemMonitor:
    """
    Comprehensive system monitoring

    Features:
    - CPU, memory, and disk monitoring  
    - System health status
    - Performance metrics
    """

    def __init__(self):
        """Initialize system monitor"""
        self.logger = setup_logger("system_monitor", config.log_level)

        # Configuration
        self.memory_threshold = 90.0  # Default fallback
        self.cpu_threshold = 95.0     # Default fallback

        # Components
        self.disk_monitor = DiskMonitor()

        # State
        self.system_history: List[SystemStatus] = []
        self.alert_callbacks: List[Callable[[SystemStatus], None]] = []

        if not PSUTIL_AVAILABLE:
            self.logger.warning("psutil not available - using basic monitoring only")

        self.logger.info("SystemMonitor initialized")

    def get_system_status(self) -> SystemStatus:
        """Get current system status"""
        try:
            # Get memory and CPU usage
            if PSUTIL_AVAILABLE:
                memory = psutil.virtual_memory()
                memory_percent = memory.percent
                cpu_percent = psutil.cpu_percent(interval=0.1)  # Quick sample
            else:
                # Fallback - basic monitoring
                memory_percent = 0.0
                cpu_percent = 0.0
                self.logger.debug("Using basic monitoring - install psutil for detailed metrics")

            # Get disk space (using primary storage path)
            primary_path = config.get('storage.base_path', './downloads')
            disk_info = self.disk_monitor.get_disk_usage(primary_path)

            # Active downloads placeholder - would be provided by download manager
            active_downloads = 0

            status = SystemStatus(
                diskSpaceGb=disk_info.freeGb,
                memoryUsagePercent=memory_percent,
                cpuUsagePercent=cpu_percent,
                activeDownloads=active_downloads,
                lastUpdate=datetime.now()
            )

            # Add to history
            self.system_history.append(status)

            # Limit history size
            if len(self.system_history) > 100:
                self.system_history = self.system_history[-100:]

            # Check for alerts
            if not status.is_healthy:
                self._trigger_system_alert(status)

            return status

        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            raise Exception(f"Cannot get system status: {e}")

    def _trigger_system_alert(self, status: SystemStatus):
        """Trigger system health alert"""
        issues = []

        if status.diskSpaceGb <= 1.0:
            issues.append(f"Low disk space: {status.diskSpaceGb:.2f}GB")

        if status.memoryUsagePercent >= self.memory_threshold:
            issues.append(f"High memory usage: {status.memoryUsagePercent:.1f}%")

        if status.cpuUsagePercent >= self.cpu_threshold:
            issues.append(f"High CPU usage: {status.cpuUsagePercent:.1f}%")

        if issues:
            self.logger.warning(f"System health alert: {', '.join(issues)}")

        # Call registered callbacks
        for callback in self.alert_callbacks:
            try:
                callback(status)
            except Exception as e:
                self.logger.error(f"Error in system alert callback: {e}")

    def get_process_info(self) -> Dict[str, Any]:
        """Get current process information"""
        try:
            if PSUTIL_AVAILABLE:
                import os
                process = psutil.Process()

                return {
                    'pid': process.pid,
                    'memory_usage_mb': process.memory_info().rss / (1024**2),
                    'memory_percent': process.memory_percent(),
                    'cpu_percent': process.cpu_percent(),
                    'num_threads': process.num_threads(),
                    'create_time': datetime.fromtimestamp(process.create_time()).isoformat(),
                    'status': process.status()
                }
            else:
                import os
                return {
                    'pid': os.getpid(),
                    'memory_usage_mb': 0.0,
                    'memory_percent': 0.0,
                    'cpu_percent': 0.0,
                    'num_threads': 1,
                    'create_time': datetime.now().isoformat(),
                    'status': 'running'
                }

        except Exception as e:
            self.logger.error(f"Error getting process info: {e}")
            return {'error': str(e)}

    def add_alert_callback(self, callback: Callable[[SystemStatus], None]):
        """Add alert callback for system issues"""
        self.alert_callbacks.append(callback)

    def check_system_health(self) -> tuple[bool, List[str]]:
        """Check overall system health"""
        try:
            status = self.get_system_status()
            issues = []

            # Check disk space
            if status.diskSpaceGb < self.disk_monitor.min_disk_space_gb:
                issues.append(f"Low disk space: {status.diskSpaceGb:.2f}GB (minimum: {self.disk_monitor.min_disk_space_gb}GB)")

            # Check memory (only if psutil available)
            if PSUTIL_AVAILABLE and status.memoryUsagePercent > self.memory_threshold:
                issues.append(f"High memory usage: {status.memoryUsagePercent:.1f}% (threshold: {self.memory_threshold}%)")

            # Check CPU (only if psutil available)
            if PSUTIL_AVAILABLE and status.cpuUsagePercent > self.cpu_threshold:
                issues.append(f"High CPU usage: {status.cpuUsagePercent:.1f}% (threshold: {self.cpu_threshold}%)")

            is_healthy = len(issues) == 0
            return is_healthy, issues

        except Exception as e:
            self.logger.error(f"Error checking system health: {e}")
            return False, [f"Health check failed: {e}"]

    def get_storage_recommendations(self) -> List[str]:
        """Get storage optimization recommendations"""
        recommendations = []

        try:
            disk_summary = self.disk_monitor.get_usage_summary()

            for path, info in disk_summary['paths'].items():
                if info['is_low_space']:
                    recommendations.append(f"Consider cleaning up files in {path} - only {info['free_gb']:.2f}GB free")

                if info['usage_percent'] > 90:
                    recommendations.append(f"Path {path} is {info['usage_percent']:.1f}% full - consider expanding storage")

            if disk_summary['critical_paths']:
                recommendations.append("CRITICAL: Some storage paths have less than 1GB free space")

            # Add general recommendations
            if recommendations:
                recommendations.extend([
                    "Run cleanup operations to remove temporary files",
                    "Consider moving old downloads to external storage"
                ])

            if not PSUTIL_AVAILABLE:
                recommendations.append("Install psutil for detailed system monitoring: pip install psutil")

        except Exception as e:
            recommendations.append(f"Error generating recommendations: {e}")

        return recommendations
