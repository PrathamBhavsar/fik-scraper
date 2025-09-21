"""
Enhanced helper utilities for FikFap Scraper - Phase 3
Additional utilities for download system
"""
import re
import hashlib
import asyncio
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
import time

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
        r'/post/(\d+)',
        r'p=(\d+)'
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

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f"{minutes:.0f}m {remaining_seconds:.0f}s"
    else:
        hours = seconds // 3600
        remaining_minutes = (seconds % 3600) // 60
        return f"{hours:.0f}h {remaining_minutes:.0f}m"

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

def extract_resolution_number(resolution: str) -> int:
    """Extract numeric value from resolution string (e.g., '720p' -> 720)"""
    match = re.search(r'(\d+)', resolution)
    return int(match.group(1)) if match else 0

def is_m3u8_url(url: str) -> bool:
    """Check if URL points to an M3U8 playlist"""
    return url.lower().endswith('.m3u8') or 'm3u8' in url.lower()

def parse_bandwidth_from_m3u8_line(line: str) -> Optional[int]:
    """Parse bandwidth from M3U8 stream info line"""
    match = re.search(r'BANDWIDTH=(\d+)', line)
    return int(match.group(1)) if match else None

def parse_resolution_from_m3u8_line(line: str) -> Optional[str]:
    """Parse resolution from M3U8 stream info line"""
    match = re.search(r'RESOLUTION=(\d+x\d+)', line)
    if match:
        width, height = match.group(1).split('x')
        return f"{height}p"  # Convert to standard format (e.g., "720p")
    return None

def parse_codecs_from_m3u8_line(line: str) -> Optional[str]:
    """Parse codecs from M3U8 stream info line"""
    match = re.search(r'CODECS="([^"]+)"', line)
    return match.group(1) if match else None

async def check_url_accessibility(url: str, session) -> tuple[bool, int]:
    """Check if URL is accessible and return status"""
    try:
        async with session.head(url, timeout=5) as response:
            return True, response.status
    except Exception:
        return False, 0

def calculate_download_eta(start_time: float, completed: int, total: int) -> float:
    """Calculate estimated time of arrival for download"""
    if completed <= 0:
        return 0.0

    elapsed = time.time() - start_time
    rate = completed / elapsed
    remaining = total - completed

    return remaining / rate if rate > 0 else 0.0

def generate_unique_filename(base_path: Path, desired_name: str) -> Path:
    """Generate unique filename if file already exists"""
    if not base_path.exists():
        return base_path / desired_name

    # If file doesn't exist, use desired name
    full_path = base_path / desired_name
    if not full_path.exists():
        return full_path

    # Generate unique name with counter
    stem = Path(desired_name).stem
    suffix = Path(desired_name).suffix
    counter = 1

    while True:
        new_name = f"{stem}_{counter}{suffix}"
        new_path = base_path / new_name
        if not new_path.exists():
            return new_path
        counter += 1

class RateLimiter:
    """Simple rate limiter for API requests"""

    def __init__(self, max_calls: int, time_window: float):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []

    async def acquire(self):
        """Acquire rate limit token"""
        now = time.time()

        # Remove old calls outside time window
        self.calls = [call_time for call_time in self.calls if now - call_time < self.time_window]

        # Check if we can make a call
        if len(self.calls) >= self.max_calls:
            # Wait until we can make a call
            sleep_time = self.time_window - (now - self.calls[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                return await self.acquire()  # Recursive call

        # Record this call
        self.calls.append(now)

class ProgressTracker:
    """Track progress of multiple operations"""

    def __init__(self):
        self.operations = {}

    def add_operation(self, operation_id: str, total: int):
        """Add new operation to track"""
        self.operations[operation_id] = {
            'total': total,
            'completed': 0,
            'start_time': time.time()
        }

    def update_progress(self, operation_id: str, completed: int):
        """Update progress for operation"""
        if operation_id in self.operations:
            self.operations[operation_id]['completed'] = completed

    def get_progress(self, operation_id: str) -> Dict[str, Any]:
        """Get progress information for operation"""
        if operation_id not in self.operations:
            return {}

        op = self.operations[operation_id]
        elapsed = time.time() - op['start_time']
        progress_pct = (op['completed'] / op['total']) * 100 if op['total'] > 0 else 0

        return {
            'total': op['total'],
            'completed': op['completed'],
            'progress_percentage': progress_pct,
            'elapsed_time': elapsed,
            'eta': calculate_download_eta(op['start_time'], op['completed'], op['total'])
        }

    def remove_operation(self, operation_id: str):
        """Remove completed operation"""
        self.operations.pop(operation_id, None)

def validate_m3u8_content(content: str) -> bool:
    """Validate M3U8 playlist content"""
    lines = content.strip().split('\n')

    # Must start with #EXTM3U
    if not lines or not lines[0].strip().startswith('#EXTM3U'):
        return False

    # Must contain at least one valid entry
    has_valid_entry = False
    for line in lines[1:]:
        line = line.strip()
        if line and not line.startswith('#'):
            has_valid_entry = True
            break

    return has_valid_entry

def detect_playlist_type(content: str) -> str:
    """Detect type of M3U8 playlist (master or media)"""
    if '#EXT-X-STREAM-INF' in content:
        return 'master'
    elif '#EXTINF' in content:
        return 'media'
    else:
        return 'unknown'

# Legacy compatibility
def parse_content_type(content_type: str) -> str:
    """Parse content type from HTTP header"""
    return content_type.split(';')[0].strip()

def is_video_content_type(content_type: str) -> bool:
    """Check if content type indicates video content"""
    video_types = ['video/', 'application/vnd.apple.mpegurl', 'application/x-mpegURL']
    return any(vtype in content_type for vtype in video_types)
