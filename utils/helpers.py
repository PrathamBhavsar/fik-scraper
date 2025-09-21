"""
Helper utilities for FikFap Scraper
"""
import re
import hashlib
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe filesystem usage"""
    # Remove or replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    # Remove multiple underscores
    filename = re.sub(r'_{2,}', '_', filename)
    # Trim and remove trailing periods/spaces
    filename = filename.strip('. ')
    # Limit length
    if len(filename) > 200:
        filename = filename[:200]
    return filename

def extract_video_id(url: str) -> Optional[str]:
    """Extract video ID from various URL formats"""
    patterns = [
        r'/video/(\d+)',
        r'postId[=:](\d+)',
        r'id[=:](\d+)',
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def get_file_hash(file_path: Path) -> str:
    """Calculate MD5 hash of a file"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception:
        return ""

def format_bytes(bytes_value: int) -> str:
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"

def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def parse_quality_from_path(path: str) -> Optional[str]:
    """Extract quality information from file path"""
    quality_patterns = [
        r'(\d+p)',
        r'vp9_(\d+p)',
        r'(\d+x\d+)',
    ]

    for pattern in quality_patterns:
        match = re.search(pattern, path, re.IGNORECASE)
        if match:
            return match.group(1)
    return None

def validate_post_data(data: Dict[str, Any]) -> bool:
    """Validate post data structure"""
    required_fields = ['postId', 'videoStreamUrl', 'publishedAt']
    return all(field in data for field in required_fields)

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    # Remove control characters
    text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', text)
    return text
