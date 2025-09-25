import json
from pathlib import Path
from typing import List, Dict, Any

class ProgressTracker:
    """Simple progress tracker that only maintains downloaded video IDs and count"""

    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = Path(progress_file)
        self.ensure_progress_file()

    def ensure_progress_file(self):
        """Ensure progress.json exists with correct structure"""
        if not self.progress_file.exists():
            initial_data = {
                "downloaded_video_ids": [],
                "total_downloaded": 0
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(initial_data, f, indent=2)

    def load_progress(self) -> Dict[str, Any]:
        """Load progress from file"""
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Ensure correct structure
            if "downloaded_video_ids" not in data:
                data["downloaded_video_ids"] = []
            if "total_downloaded" not in data:
                data["total_downloaded"] = len(data.get("downloaded_video_ids", []))

            return data
        except Exception as e:
            print(f"Error loading progress: {e}")
            return {"downloaded_video_ids": [], "total_downloaded": 0}

    def save_progress(self, data: Dict[str, Any]):
        """Save progress to file"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving progress: {e}")

    def add_downloaded_video(self, video_id: str):
        """Add a successfully downloaded video ID to progress"""
        progress = self.load_progress()

        if video_id not in progress["downloaded_video_ids"]:
            progress["downloaded_video_ids"].append(video_id)
            progress["total_downloaded"] = len(progress["downloaded_video_ids"])
            self.save_progress(progress)

    def is_video_downloaded(self, video_id: str) -> bool:
        """Check if video is already downloaded"""
        progress = self.load_progress()
        return video_id in progress["downloaded_video_ids"]

    def get_stats(self) -> Dict[str, Any]:
        """Get download statistics"""
        progress = self.load_progress()
        return {
            "total_downloaded": progress["total_downloaded"],
            "downloaded_count": len(progress["downloaded_video_ids"])
        }
