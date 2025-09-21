"""
Configuration management for FikFap Scraper - Phase 2 Enhanced
Enhanced configuration with data validation settings
"""
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from .exceptions import ConfigurationError

class Config:
    """Enhanced configuration manager with validation settings"""

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self._settings = {}
        self._filters = {}
        self._validation_rules = {}
        self.load_configs()

    def load_configs(self):
        """Load all configuration files"""
        try:
            # Load main settings
            settings_file = self.config_dir / "settings.json"
            if settings_file.exists():
                with open(settings_file, 'r') as f:
                    self._settings = json.load(f)
            else:
                raise ConfigurationError(f"Settings file not found: {settings_file}")

            # Load filters
            filters_file = self.config_dir / "filters.json"
            if filters_file.exists():
                with open(filters_file, 'r') as f:
                    self._filters = json.load(f)
            else:
                raise ConfigurationError(f"Filters file not found: {filters_file}")

            # Set up validation rules from filters
            self._setup_validation_rules()

        except json.JSONDecodeError as e:
            raise ConfigurationError(f"Invalid JSON in config file: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error loading configuration: {e}")

    def _setup_validation_rules(self):
        """Setup validation rules from filter configuration"""
        self._validation_rules = {
            'min_duration': self._filters.get('content', {}).get('min_duration', 10),
            'max_duration': self._filters.get('content', {}).get('max_duration', 3600),
            'min_views': self._filters.get('content', {}).get('min_views', 100),
            'blocked_users': set(self._filters.get('content', {}).get('blocked_users', [])),
            'blocked_hashtags': set(self._filters.get('content', {}).get('blocked_hashtags', [])),
            'allowed_ratings': self._filters.get('content', {}).get('explicitness_ratings', ['FULLY_EXPLICIT']),
            'vp9_patterns': self._filters.get('codecs', {}).get('vp9_patterns', ['vp9_', 'vp09.']),
            'exclude_vp9': self._filters.get('codecs', {}).get('exclude_vp9', False),
            'preferred_codecs': self._filters.get('codecs', {}).get('preferred_codecs', ['avc1', 'h264']),
            'min_resolution': self._filters.get('quality', {}).get('min_resolution', '240p'),
            'max_resolution': self._filters.get('quality', {}).get('max_resolution', '1080p'),
            'exclude_resolutions': set(self._filters.get('quality', {}).get('exclude_resolutions', []))
        }

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by dotted key path"""
        keys = key.split('.')
        value = self._settings

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_filter(self, key: str, default: Any = None) -> Any:
        """Get filter configuration value"""
        keys = key.split('.')
        value = self._filters

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_validation_rule(self, rule_name: str, default: Any = None) -> Any:
        """Get validation rule by name"""
        return self._validation_rules.get(rule_name, default)

    # Core API settings
    @property
    def api_base_url(self) -> str:
        return self.get('api.base_url', 'https://api.fikfap.com')

    @property
    def api_timeout(self) -> int:
        return self.get('api.timeout', 30)

    @property
    def api_max_retries(self) -> int:
        return self.get('api.max_retries', 3)

    @property
    def api_rate_limit(self) -> float:
        return self.get('api.rate_limit', 1.0)

    # Download settings
    @property
    def download_path(self) -> str:
        return self.get('storage.base_path', './downloads')

    @property
    def concurrent_downloads(self) -> int:
        return self.get('download.concurrent_downloads', 5)

    @property
    def chunk_size(self) -> int:
        return self.get('download.chunk_size', 8192)

    @property
    def verify_ssl(self) -> bool:
        return self.get('download.verify_ssl', True)

    @property
    def user_agent(self) -> str:
        return self.get('download.user_agent', 'FikFap-Scraper/2.0')

    # Quality settings
    @property
    def exclude_vp9(self) -> bool:
        return self.get('quality.exclude_vp9', False)

    @property
    def preferred_qualities(self) -> List[str]:
        return self.get('quality.preferred_qualities', ['1080p', '720p', '480p', '360p', '240p'])

    @property
    def download_all_qualities(self) -> bool:
        return self.get('quality.download_all_qualities', True)

    # Content filtering
    @property
    def min_duration(self) -> int:
        return self.get_validation_rule('min_duration', 10)

    @property
    def max_duration(self) -> int:
        return self.get_validation_rule('max_duration', 3600)

    @property
    def min_views(self) -> int:
        return self.get_validation_rule('min_views', 100)

    @property
    def blocked_users(self) -> set:
        return self.get_validation_rule('blocked_users', set())

    @property
    def blocked_hashtags(self) -> set:
        return self.get_validation_rule('blocked_hashtags', set())

    @property
    def allowed_ratings(self) -> List[str]:
        return self.get_validation_rule('allowed_ratings', ['FULLY_EXPLICIT'])

    # Codec settings
    @property
    def vp9_patterns(self) -> List[str]:
        return self.get_validation_rule('vp9_patterns', ['vp9_', 'vp09.'])

    @property
    def preferred_codecs(self) -> List[str]:
        return self.get_validation_rule('preferred_codecs', ['avc1', 'h264'])

    # Logging settings
    @property
    def log_level(self) -> str:
        return self.get('logging.level', 'INFO')

    @property
    def log_file(self) -> Optional[str]:
        return self.get('logging.file')

    @property
    def log_max_size(self) -> str:
        return self.get('logging.max_size', '10MB')

    @property
    def log_backup_count(self) -> int:
        return self.get('logging.backup_count', 5)

    # Monitoring settings
    @property
    def check_disk_space(self) -> bool:
        return self.get('monitoring.check_disk_space', True)

    @property
    def min_disk_space_gb(self) -> float:
        return self.get('monitoring.min_disk_space_gb', 5.0)

    @property
    def alert_enabled(self) -> bool:
        return self.get('monitoring.alert_enabled', True)

    # FTP settings
    @property
    def ftp_enabled(self) -> bool:
        return self.get('ftp.enabled', False)

    @property
    def ftp_host(self) -> str:
        return self.get('ftp.host', '')

    @property
    def ftp_port(self) -> int:
        return self.get('ftp.port', 21)

    @property
    def ftp_username(self) -> str:
        return self.get('ftp.username', '')

    @property
    def ftp_password(self) -> str:
        return self.get('ftp.password', '')

    @property
    def ftp_remote_path(self) -> str:
        return self.get('ftp.remote_path', '/uploads')

    def create_directories(self):
        """Create necessary directories"""
        # Create downloads directory
        download_dir = Path(self.download_path)
        download_dir.mkdir(parents=True, exist_ok=True)

        # Create logs directory
        log_file = self.log_file
        if log_file:
            log_dir = Path(log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)

        # Create temp directory for processing
        temp_dir = download_dir / 'temp'
        temp_dir.mkdir(exist_ok=True)

    def validate_config(self) -> List[str]:
        """Validate configuration settings and return list of issues"""
        issues = []

        # Validate API settings
        if not self.api_base_url:
            issues.append("API base URL is required")

        if self.api_timeout <= 0:
            issues.append("API timeout must be positive")

        if self.api_max_retries < 0:
            issues.append("API max retries cannot be negative")

        # Validate download settings
        if self.concurrent_downloads <= 0:
            issues.append("Concurrent downloads must be positive")

        if self.chunk_size <= 0:
            issues.append("Chunk size must be positive")

        # Validate duration constraints
        if self.min_duration >= self.max_duration:
            issues.append("Min duration must be less than max duration")

        if self.min_views < 0:
            issues.append("Min views cannot be negative")

        # Validate FTP settings if enabled
        if self.ftp_enabled:
            if not self.ftp_host:
                issues.append("FTP host is required when FTP is enabled")
            if not self.ftp_username:
                issues.append("FTP username is required when FTP is enabled")

        return issues

    def update_setting(self, key: str, value: Any):
        """Update a setting value and save to file"""
        keys = key.split('.')
        config_dict = self._settings

        # Navigate to the correct nested location
        for k in keys[:-1]:
            if k not in config_dict:
                config_dict[k] = {}
            config_dict = config_dict[k]

        # Set the value
        config_dict[keys[-1]] = value

        # Save to file
        self.save_settings()

    def update_filter(self, key: str, value: Any):
        """Update a filter value and save to file"""
        keys = key.split('.')
        config_dict = self._filters

        # Navigate to the correct nested location
        for k in keys[:-1]:
            if k not in config_dict:
                config_dict[k] = {}
            config_dict = config_dict[k]

        # Set the value
        config_dict[keys[-1]] = value

        # Save to file and update validation rules
        self.save_filters()
        self._setup_validation_rules()

    def save_settings(self):
        """Save settings to file"""
        settings_file = self.config_dir / "settings.json"
        with open(settings_file, 'w') as f:
            json.dump(self._settings, f, indent=2)

    def save_filters(self):
        """Save filters to file"""
        filters_file = self.config_dir / "filters.json"
        with open(filters_file, 'w') as f:
            json.dump(self._filters, f, indent=2)

    def get_config_summary(self) -> Dict[str, Any]:
        """Get configuration summary for logging/debugging"""
        return {
            'api': {
                'base_url': self.api_base_url,
                'timeout': self.api_timeout,
                'max_retries': self.api_max_retries
            },
            'download': {
                'path': self.download_path,
                'concurrent': self.concurrent_downloads,
                'exclude_vp9': self.exclude_vp9
            },
            'validation': {
                'min_duration': self.min_duration,
                'max_duration': self.max_duration,
                'min_views': self.min_views,
                'blocked_users_count': len(self.blocked_users),
                'blocked_hashtags_count': len(self.blocked_hashtags)
            },
            'logging': {
                'level': self.log_level,
                'file_enabled': bool(self.log_file)
            },
            'ftp': {
                'enabled': self.ftp_enabled,
                'configured': bool(self.ftp_host and self.ftp_username)
            }
        }

# Global config instance
config = Config()
