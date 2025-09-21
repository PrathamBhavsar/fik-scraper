"""
Unicode-safe logging configuration for FikFap Scraper
Fixes emoji encoding issues on Windows and other platforms
"""
import logging
import sys
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler
import io

class UnicodeStreamHandler(logging.StreamHandler):
    """Custom stream handler that handles Unicode properly on Windows"""
    
    def __init__(self, stream=None):
        super().__init__(stream)
        
        # Force UTF-8 encoding for console output on Windows
        if sys.platform == "win32" and stream is None:
            try:
                # Set console to UTF-8 mode
                os.system("chcp 65001 >nul 2>&1")
                
                # Create a UTF-8 wrapper for stdout
                if hasattr(sys.stdout, 'buffer'):
                    self.stream = io.TextIOWrapper(
                        sys.stdout.buffer, 
                        encoding='utf-8', 
                        errors='replace',
                        newline='',
                        line_buffering=True
                    )
                else:
                    self.stream = sys.stdout
            except Exception:
                # Fallback to regular stdout
                self.stream = sys.stdout
    
    def emit(self, record):
        """Emit a record with proper Unicode handling"""
        try:
            msg = self.format(record)
            
            # Replace problematic Unicode characters if encoding fails
            if sys.platform == "win32":
                try:
                    # Test if the message can be encoded to current console encoding
                    msg.encode('cp1252')
                except UnicodeEncodeError:
                    # Replace emoji and special Unicode chars with safe alternatives
                    msg = self._replace_unicode_chars(msg)
            
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
            
        except Exception:
            self.handleError(record)
    
    def _replace_unicode_chars(self, text):
        """Replace common emoji and Unicode chars with ASCII alternatives"""
        replacements = {
            # Common emojis used in the project - exactly what's causing your error
            '🔽': '[DOWN]',  # This is the one causing your current error
            '🔼': '[UP]', 
            '⬇️': '[DOWN]', 
            '⬆️': '[UP]',
            '➡️': '[RIGHT]',
            '⬅️': '[LEFT]',
            '✅': '[OK]', 
            '❌': '[ERROR]', 
            '⚠️': '[WARNING]', 
            '🚀': '[LAUNCH]',
            '🎯': '[TARGET]', 
            '📊': '[CHART]', 
            '📈': '[UP]', 
            '📉': '[DOWN]',
            '💾': '[DISK]', 
            '🧠': '[MEMORY]', 
            '🔧': '[TOOL]', 
            '⚙️': '[SETTINGS]',
            '🏥': '[HEALTH]', 
            '💡': '[IDEA]', 
            '🎬': '[MOVIE]', 
            '📹': '[VIDEO]',
            '📁': '[FOLDER]', 
            '📄': '[FILE]', 
            '📋': '[LIST]', 
            '🔍': '[SEARCH]',
            '🧹': '[CLEAN]', 
            '🎉': '[SUCCESS]', 
            '✨': '[DONE]', 
            '🌟': '[STAR]',
            '🚨': '[ALERT]',
            '💻': '[COMPUTER]',
            '📱': '[MOBILE]',
            '🔊': '[AUDIO]',
            '📦': '[PACKAGE]',
            '🔐': '[SECURE]',
            '🔑': '[KEY]',
            '⭐': '[STAR]',
            '🌍': '[WORLD]',
            '🔗': '[LINK]',
            '⚡': '[FAST]',
            '🔥': '[HOT]',
            
            # Numbers
            '1️⃣': '1.', 
            '2️⃣': '2.', 
            '3️⃣': '3.', 
            '4️⃣': '4.', 
            '5️⃣': '5.',
            '6️⃣': '6.', 
            '7️⃣': '7.', 
            '8️⃣': '8.', 
            '9️⃣': '9.', 
            '🔟': '10.',
            
            # Process indicators
            '📥': '[DOWNLOAD]',
            '📤': '[UPLOAD]',
            '🔄': '[REFRESH]',
            '🔃': '[RELOAD]',
            '▶️': '[PLAY]',
            '⏸️': '[PAUSE]',
            '⏹️': '[STOP]',
            '⏭️': '[NEXT]',
            '⏮️': '[PREV]',
            '🔀': '[SHUFFLE]',
            '🔁': '[REPEAT]',
            '🔂': '[REPEAT-ONE]',
        }
        
        for emoji, replacement in replacements.items():
            text = text.replace(emoji, replacement)
        
        return text

def setup_logger(name: str = "fikfap_scraper", level: str = "INFO", log_file: str = None) -> logging.Logger:
    """
    Set up a Unicode-safe logger that works properly on Windows
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Set logging level
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    logger.setLevel(level_map.get(level.upper(), logging.INFO))
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Add console handler with Unicode support
    console_handler = UnicodeStreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        try:
            # Ensure log directory exists
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Create rotating file handler with UTF-8 encoding
            file_handler = RotatingFileHandler(
                log_file, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'  # Force UTF-8 for file output
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
        except Exception as e:
            # If file logging fails, just continue with console logging
            pass
    
    return logger

# For backward compatibility
def setup_unicode_safe_logging():
    """Set up Unicode-safe logging for the entire application"""
    root_logger = logging.getLogger()
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set up with Unicode-safe handler
    handler = UnicodeStreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

# Global logger instance with Unicode support
logger = setup_logger()