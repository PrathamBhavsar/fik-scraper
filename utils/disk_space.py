from pathlib import Path
import shutil

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
    # Convert bytes → GiB
    return usage.free / (1024 ** 3)
