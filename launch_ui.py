"""
FikFap Scraper Clean UI Launcher

Launches the clean UI that opens logs in separate terminal window.
This version removes log clutter from the UI and uses the terminal for logs.
"""

import sys
import os
from pathlib import Path

def main():
    """Launch the clean FikFap Scraper UI"""
    try:
        # Ensure we're in the right directory
        script_dir = Path(__file__).parent.absolute()
        os.chdir(script_dir)

        # Check for required files
        required_files = ["main.py", "settings.json", "clean_scraper_ui.py"]
        missing_files = []

        for file in required_files:
            if not Path(file).exists():
                missing_files.append(file)

        if missing_files:
            print("ERROR: Missing required files:")
            for file in missing_files:
                print(f"  - {file}")
            print("\nPlease ensure all scraper files are in the same directory.")
            input("Press Enter to exit...")
            return

        # Import and run the clean UI
        print("Starting Clean FikFap Scraper UI...")
        print("- Logs will open in separate terminal window")
        print("- VS Code terminal supports emojis and colors perfectly")
        print("- Clean, focused UI for control and monitoring")
        print("")

        from clean_scraper_ui import FikFapScraperUI

        app = FikFapScraperUI()
        app.run()

    except ImportError as e:
        print(f"Import error: {e}")
        print("\nPlease ensure all required Python packages are installed:")
        print("  - tkinter (usually included with Python)")
        print("  - Standard library modules")
        input("Press Enter to exit...")
    except Exception as e:
        print(f"Failed to start clean UI: {e}")
        import traceback
        traceback.print_exc()
        input("Press Enter to exit...")

if __name__ == "__main__":
    main()
