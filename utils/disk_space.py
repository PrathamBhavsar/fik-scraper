"""
Disk space utilities for monitoring download directory size and free space
"""
from pathlib import Path
import shutil
import os

def get_free_space_gb(path: str | Path) -> float:
    """
    Return the free disk space for *path* in **gigabytes**.

    Parameters
    ----------
    path : str | Path
        Directory whose filesystem should be inspected.

    Returns
    -------
    float
        Free space in GB.
    """
    usage = shutil.disk_usage(str(Path(path)))
    # Convert bytes → GB
    return usage.free / (1024 ** 3)


def get_directory_size_gb(path: str | Path) -> float:
    """
    Calculate the total size of all files in a directory recursively.

    Parameters
    ----------
    path : str | Path
        Directory to calculate size for.

    Returns
    -------
    float
        Total directory size in GB.
    """
    path = Path(path)
    if not path.exists():
        return 0.0

    total_size = 0

    try:
        # Walk through all files and subdirectories
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = Path(dirpath) / filename
                try:
                    if filepath.exists() and filepath.is_file():
                        total_size += filepath.stat().st_size
                except (OSError, PermissionError):
                    # Skip files we can't read
                    continue
    except (OSError, PermissionError):
        # If we can't read the directory at all
        return 0.0

    # Convert bytes to GB
    return total_size / (1024 ** 3)


def check_disk_space_limit(downloads_dir: str | Path, max_size_gb: float) -> dict:
    """
    Check if downloads directory size exceeds the maximum allowed size.

    Parameters
    ----------
    downloads_dir : str | Path
        Path to downloads directory to check.
    max_size_gb : float
        Maximum allowed size in GB.

    Returns
    -------
    dict
        Dictionary with check results:
        - 'current_size_gb': Current directory size in GB
        - 'max_size_gb': Maximum allowed size in GB  
        - 'exceeds_limit': Boolean indicating if limit is exceeded
        - 'free_space_gb': Free space on filesystem in GB
    """
    downloads_dir = Path(downloads_dir)

    current_size = get_directory_size_gb(downloads_dir)
    free_space = get_free_space_gb(downloads_dir) if downloads_dir.exists() else 0.0

    return {
        'current_size_gb': current_size,
        'max_size_gb': max_size_gb,
        'exceeds_limit': current_size >= max_size_gb,
        'free_space_gb': free_space
    }
