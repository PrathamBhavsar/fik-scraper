"""
Custom exceptions for FikFap Scraper
"""

class FikFapScraperError(Exception):
    """Base exception for FikFap Scraper"""
    pass

class APIError(FikFapScraperError):
    """API related errors"""
    pass

class DownloadError(FikFapScraperError):
    """Download related errors"""
    pass

class ValidationError(FikFapScraperError):
    """Data validation errors"""
    pass

class StorageError(FikFapScraperError):
    """Storage related errors"""
    pass

class ConfigurationError(FikFapScraperError):
    """Configuration errors"""
    pass

class NetworkError(FikFapScraperError):
    """Network related errors"""
    pass

class QualityNotFoundError(DownloadError):
    """Requested quality not available"""
    pass

class PlaylistError(DownloadError):
    """M3U8 playlist processing errors"""
    pass

class FragmentError(DownloadError):
    """Video fragment processing errors"""
    pass
