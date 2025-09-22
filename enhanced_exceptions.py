"""
Enhanced exception classes for Phase 5 orchestration
"""
from typing import Optional, Any, Dict, List

class OrchestrationError(Exception):
    """Base exception for orchestration errors"""
    
    def __init__(self, message: str, component: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.component = component
        self.details = details or {}
        super().__init__(self.message)

class StartupError(OrchestrationError):
    """Exception raised during system startup"""
    pass

class ShutdownError(OrchestrationError):
    """Exception raised during system shutdown"""
    pass

class ProcessingError(OrchestrationError):
    """Exception raised during video processing workflow"""
    
    def __init__(self, message: str, post_id: Optional[int] = None, step: Optional[str] = None, **kwargs):
        self.post_id = post_id
        self.step = step
        super().__init__(message, **kwargs)

class ComponentError(OrchestrationError):
    """Exception raised by individual components"""
    pass

class ScrapingError(ProcessingError):
    """Exception raised during scraping operations"""
    pass

class ConfigurationError(OrchestrationError):
    """Exception raised for configuration issues"""
    pass

class ResourceError(OrchestrationError):
    """Exception raised for resource-related issues"""
    pass

class NetworkError(OrchestrationError):
    """Exception raised for network-related issues"""
    pass

class StorageError(OrchestrationError):
    """Exception raised for storage-related issues"""
    pass

class ExtractionError(ProcessingError):
    """Exception raised during data extraction"""
    pass

class DownloadError(ProcessingError):
    """Exception raised during download operations"""
    pass