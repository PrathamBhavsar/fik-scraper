"""
FikFap Scraper UI - Clean Interface with Separate Log Terminal

This UI provides clean control interface and opens scraper logs in a separate terminal window.
Since VS Code terminal already supports emojis and UTF-8, we let it handle the log display.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import subprocess
import json
import os
import time
import queue
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import sys
import platform


class FikFapScraperUI:
    """Clean UI wrapper that opens scraper logs in separate terminal"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("FikFap Scraper Control Panel")
        self.root.geometry("500x400")
        self.root.resizable(True, False)

        # State variables
        self.scraper_process: Optional[subprocess.Popen] = None
        self.scraper_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.stop_reason = ""

        # Communication queue for thread-safe UI updates
        self.message_queue = queue.Queue()

        # Load settings
        self.settings = self.load_settings()
        self.download_dir = self.settings.get("storage", {}).get("base_path", "./downloads")
        self.max_size_gb = float(self.settings.get("monitoring", {}).get("min_disk_space_gb", 1.0))

        # Create UI
        self.create_widgets()
        self.update_ui_state()

        # Start message processing
        self.process_messages()

        # Handle window close
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def load_settings(self) -> Dict[str, Any]:
        """Load settings from settings.json"""
        try:
            if Path("settings.json").exists():
                with open("settings.json", "r", encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading settings: {e}")

        return {
            "storage": {"base_path": "./downloads"},
            "monitoring": {"min_disk_space_gb": 1.0}
        }

    def save_settings(self):
        """Save current settings to settings.json"""
        try:
            self.settings["storage"]["base_path"] = self.download_dir
            self.settings["monitoring"]["min_disk_space_gb"] = self.max_size_gb

            with open("settings.json", "w", encoding='utf-8') as f:
                json.dump(self.settings, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving settings: {e}")

    def create_widgets(self):
        """Create clean UI widgets without log output"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        # Title
        title_label = ttk.Label(main_frame, text="FikFap Scraper Control Panel", 
                               font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))

        # Control buttons frame
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=1, column=0, columnspan=3, pady=(0, 20), sticky=(tk.W, tk.E))
        button_frame.columnconfigure(0, weight=1)
        button_frame.columnconfigure(1, weight=1)

        # Start button
        self.start_button = ttk.Button(button_frame, text="▶ Start Scraper", 
                                      command=self.start_scraper, style="Success.TButton")
        self.start_button.grid(row=0, column=0, padx=(0, 10), sticky=(tk.W, tk.E))

        # Stop button
        self.stop_button = ttk.Button(button_frame, text="⏹ Stop Scraper", 
                                     command=self.stop_scraper, style="Danger.TButton")
        self.stop_button.grid(row=0, column=1, padx=(10, 0), sticky=(tk.W, tk.E))

        # Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="Status & Progress", padding="15")
        progress_frame.grid(row=2, column=0, columnspan=3, pady=(0, 15), sticky=(tk.W, tk.E))
        progress_frame.columnconfigure(0, weight=1)

        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, 
                                           mode='indeterminate')
        self.progress_bar.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 15))

        # Status text
        self.status_var = tk.StringVar(value="Idle - Ready to Start")
        self.status_display = ttk.Label(progress_frame, textvariable=self.status_var, 
                                       font=("Arial", 11, "bold"), foreground="blue")
        self.status_display.grid(row=1, column=0, pady=(0, 10))

        # Log info
        log_info_label = ttk.Label(progress_frame, 
                                  text="📝 Logs will open in separate terminal window", 
                                  font=("Arial", 9), foreground="gray")
        log_info_label.grid(row=2, column=0, pady=(0, 0))

        # Configuration section
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="15")
        config_frame.grid(row=3, column=0, columnspan=3, pady=(0, 15), sticky=(tk.W, tk.E))
        config_frame.columnconfigure(1, weight=1)

        # Download directory
        ttk.Label(config_frame, text="Download Directory:", font=("Arial", 9, "bold")).grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        dir_frame = ttk.Frame(config_frame)
        dir_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))
        dir_frame.columnconfigure(0, weight=1)

        self.dir_var = tk.StringVar(value=self.download_dir)
        self.dir_entry = ttk.Entry(dir_frame, textvariable=self.dir_var, width=50)
        self.dir_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        self.browse_button = ttk.Button(dir_frame, text="Browse...", command=self.browse_directory)
        self.browse_button.grid(row=0, column=1)

        # Max size setting
        ttk.Label(config_frame, text="Max Disk Size (GB):", font=("Arial", 9, "bold")).grid(row=2, column=0, sticky=tk.W, pady=(5, 5))

        size_frame = ttk.Frame(config_frame)
        size_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))

        self.size_var = tk.DoubleVar(value=self.max_size_gb)
        self.size_spinbox = ttk.Spinbox(size_frame, from_=0.1, to=1000.0, increment=0.1, 
                                       textvariable=self.size_var, width=15)
        self.size_spinbox.grid(row=0, column=0, sticky=tk.W)

        ttk.Button(size_frame, text="Apply Settings", command=self.apply_settings).grid(row=0, column=1, padx=(10, 0))

        # Statistics section
        stats_frame = ttk.LabelFrame(main_frame, text="Live Statistics", padding="15")
        stats_frame.grid(row=4, column=0, columnspan=3, pady=(0, 0), sticky=(tk.W, tk.E))
        stats_frame.columnconfigure(1, weight=1)

        # Create statistics grid
        stats_grid = ttk.Frame(stats_frame)
        stats_grid.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E))
        stats_grid.columnconfigure(1, weight=1)
        stats_grid.columnconfigure(3, weight=1)

        # Downloaded videos count
        ttk.Label(stats_grid, text="Videos Downloaded:", font=("Arial", 9)).grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        self.video_count_var = tk.StringVar(value="0")
        ttk.Label(stats_grid, textvariable=self.video_count_var, 
                 font=("Arial", 10, "bold"), foreground="green").grid(row=0, column=1, sticky=tk.W)

        # Current disk usage
        ttk.Label(stats_grid, text="Disk Usage:", font=("Arial", 9)).grid(row=0, column=2, sticky=tk.W, padx=(20, 10))
        self.disk_usage_var = tk.StringVar(value="0.0 GB")
        ttk.Label(stats_grid, textvariable=self.disk_usage_var, 
                 font=("Arial", 10, "bold"), foreground="orange").grid(row=0, column=3, sticky=tk.W)

        # Last update time
        ttk.Label(stats_grid, text="Last Update:", font=("Arial", 9)).grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=(10, 0))
        self.last_update_var = tk.StringVar(value="Never")
        ttk.Label(stats_grid, textvariable=self.last_update_var,
                 font=("Arial", 9)).grid(row=1, column=1, sticky=tk.W, pady=(10, 0))

        # Auto-refresh indicator
        ttk.Label(stats_grid, text="Auto-Refresh:", font=("Arial", 9)).grid(row=1, column=2, sticky=tk.W, padx=(20, 10), pady=(10, 0))
        ttk.Label(stats_grid, text="Every 5s", 
                 font=("Arial", 9), foreground="blue").grid(row=1, column=3, sticky=tk.W, pady=(10, 0))

        # Configure button styles
        style = ttk.Style()
        try:
            style.configure("Success.TButton", foreground="green")
            style.configure("Danger.TButton", foreground="red")
        except:
            pass

        # Initial statistics update
        self.update_statistics()

        # Schedule periodic statistics updates
        self.schedule_stats_update()

    def schedule_stats_update(self):
        """Schedule periodic statistics updates every 5 seconds"""
        self.update_statistics()
        self.root.after(5000, self.schedule_stats_update)  # Update every 5 seconds

    def browse_directory(self):
        """Open directory browser dialog"""
        if self.is_running:
            return

        directory = filedialog.askdirectory(initialdir=self.download_dir, 
                                          title="Select Download Directory")
        if directory:
            self.dir_var.set(directory)
            self.download_dir = directory

    def apply_settings(self):
        """Apply configuration changes"""
        if self.is_running:
            messagebox.showwarning("Warning", "Cannot change settings while scraper is running!")
            return

        try:
            new_dir = self.dir_var.get().strip()
            new_size = float(self.size_var.get())

            if not new_dir:
                messagebox.showerror("Error", "Download directory cannot be empty!")
                return

            if new_size <= 0:
                messagebox.showerror("Error", "Max size must be greater than 0!")
                return

            # Create directory if it doesn't exist
            Path(new_dir).mkdir(parents=True, exist_ok=True)

            # Update settings
            self.download_dir = new_dir
            self.max_size_gb = new_size
            self.save_settings()

            # Update statistics immediately
            self.update_statistics()

            messagebox.showinfo("Success", "Settings have been applied successfully!")

        except ValueError:
            messagebox.showerror("Error", "Invalid max size value!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to apply settings: {e}")

    def open_terminal_for_logs(self):
        """Open a new terminal window to display scraper logs"""
        try:
            system = platform.system().lower()

            if system == "windows":
                # Windows - open new command prompt
                cmd = f'start "FikFap Scraper Logs" cmd /k "python main.py --continuous-download --verbose --config settings.json"'
                subprocess.Popen(cmd, shell=True)

            elif system == "darwin":  # macOS
                # macOS - open new Terminal window
                applescript = """
                tell application "Terminal"
                    do script "cd '%s' && python main.py --continuous-download --verbose --config settings.json"
                    activate
                end tell
                """ % os.getcwd()
                subprocess.Popen(["osascript", "-e", applescript])

            else:  # Linux and others
                # Try common terminal emulators
                terminals = [
                    ["gnome-terminal", "--title=FikFap Scraper Logs", "--", "python", "main.py", "--continuous-download", "--verbose", "--config", "settings.json"],
                    ["konsole", "--title", "FikFap Scraper Logs", "-e", "python", "main.py", "--continuous-download", "--verbose", "--config", "settings.json"],
                    ["xfce4-terminal", "--title=FikFap Scraper Logs", "-e", "python main.py --continuous-download --verbose --config settings.json"],
                    ["xterm", "-title", "FikFap Scraper Logs", "-e", "python", "main.py", "--continuous-download", "--verbose", "--config", "settings.json"]
                ]

                for terminal_cmd in terminals:
                    try:
                        subprocess.Popen(terminal_cmd)
                        break
                    except FileNotFoundError:
                        continue
                else:
                    # Fallback: try to use x-terminal-emulator
                    subprocess.Popen([
                        "x-terminal-emulator", "-title", "FikFap Scraper Logs", "-e", 
                        "python", "main.py", "--continuous-download", "--verbose", "--config", "settings.json"
                    ])

        except Exception as e:
            messagebox.showerror("Error", f"Failed to open terminal window: {e}\n\nPlease run manually in terminal:\npython main.py --continuous-download --verbose")
            return False

        return True

    def start_scraper(self):
        """Start the scraper process in a separate terminal"""
        if self.is_running:
            return

        try:
            # Apply current settings before starting
            new_dir = self.dir_var.get().strip()
            new_size = float(self.size_var.get())

            if new_dir and new_size > 0:
                Path(new_dir).mkdir(parents=True, exist_ok=True)
                self.download_dir = new_dir
                self.max_size_gb = new_size
                self.save_settings()

            # Open logs in separate terminal window
            if not self.open_terminal_for_logs():
                return  # Failed to open terminal

            self.is_running = True
            self.stop_reason = ""
            self.update_ui_state()

            # Start progress bar animation
            self.progress_bar.start(10)

            # Start monitoring thread (lightweight - just checks if process is running)
            self.scraper_thread = threading.Thread(target=self.monitor_scraper, daemon=True)
            self.scraper_thread.start()

            self.status_var.set("🚀 Scraper Running - Check Terminal Window")

        except Exception as e:
            self.is_running = False
            self.update_ui_state()
            messagebox.showerror("Error", f"Failed to start scraper: {e}")

    def stop_scraper(self):
        """Stop the scraper process"""
        if not self.is_running:
            return

        try:
            # Since we opened in separate terminal, we can't directly control it
            # Show message to user
            result = messagebox.askquestion(
                "Stop Scraper", 
                "The scraper is running in a separate terminal window.\n\n"
                "To stop it, press Ctrl+C in the terminal window.\n\n"
                "Mark as stopped in this UI?",
                icon="question"
            )

            if result == 'yes':
                self.is_running = False
                self.stop_reason = "Stopped by user"
                self.update_ui_state()
                self.status_var.set("⏹ Stopped - Terminal may still be running")

        except Exception as e:
            messagebox.showerror("Error", f"Error stopping scraper: {e}")

    def monitor_scraper(self):
        """Lightweight monitoring of scraper status (runs in separate thread)"""
        try:
            # Since scraper runs in separate terminal, we can't directly monitor it
            # This is just a placeholder for UI state management

            # After some time, we can assume it's running if no errors reported
            time.sleep(3)
            if self.is_running:
                self.message_queue.put(("status", "✅ Running - Logs in Terminal"))

            # Keep thread alive while scraper is marked as running
            while self.is_running:
                time.sleep(5)
                if self.is_running:
                    # Update statistics periodically
                    self.message_queue.put(("update_stats", ""))

        except Exception as e:
            self.message_queue.put(("error", f"Monitor error: {e}"))

    def update_statistics(self):
        """Update statistics display"""
        try:
            # Count downloaded videos (number of directories in download folder)
            download_path = Path(self.download_dir)
            if download_path.exists():
                video_count = len([d for d in download_path.iterdir() if d.is_dir() and not d.name.startswith('.')])
                self.video_count_var.set(str(video_count))

                # Calculate disk usage
                total_size = 0
                for dirpath, dirnames, filenames in os.walk(download_path):
                    for filename in filenames:
                        filepath = Path(dirpath) / filename
                        try:
                            if filepath.exists():
                                total_size += filepath.stat().st_size
                        except (OSError, PermissionError):
                            continue

                size_gb = total_size / (1024 ** 3)
                usage_text = f"{size_gb:.2f} GB"
                if size_gb >= self.max_size_gb * 0.9:  # Warning at 90%
                    usage_text += f" ⚠️ ({self.max_size_gb:.1f} GB limit)"
                else:
                    usage_text += f" / {self.max_size_gb:.1f} GB"

                self.disk_usage_var.set(usage_text)
            else:
                self.video_count_var.set("0")
                self.disk_usage_var.set("0.0 GB")

            self.last_update_var.set(datetime.now().strftime("%H:%M:%S"))

        except Exception as e:
            print(f"Error updating statistics: {e}")

    def process_messages(self):
        """Process messages from the monitor thread"""
        try:
            while not self.message_queue.empty():
                try:
                    message_type, data = self.message_queue.get_nowait()

                    if message_type == "status":
                        self.status_var.set(data)
                    elif message_type == "update_stats":
                        self.update_statistics()
                    elif message_type == "error":
                        self.status_var.set("❌ Error - Check Terminal")
                        messagebox.showerror("Error", f"Scraper error: {data}")

                except queue.Empty:
                    break

        except Exception as e:
            print(f"Error processing messages: {e}")

        # Schedule next message processing
        self.root.after(100, self.process_messages)

    def update_ui_state(self):
        """Update UI element states based on scraper status"""
        if self.is_running:
            self.start_button.config(state="disabled")
            self.stop_button.config(state="normal")
            self.dir_entry.config(state="disabled")
            self.browse_button.config(state="disabled")
            self.size_spinbox.config(state="disabled")
            self.progress_bar.start(10)
        else:
            self.start_button.config(state="normal")
            self.stop_button.config(state="disabled")
            self.dir_entry.config(state="normal")
            self.browse_button.config(state="normal")
            self.size_spinbox.config(state="normal")
            self.progress_bar.stop()
            if not self.stop_reason:
                self.status_var.set("💤 Idle - Ready to Start")
            else:
                self.status_var.set(f"⏹ {self.stop_reason}")

    def on_closing(self):
        """Handle window close event"""
        if self.is_running:
            result = messagebox.askquestion(
                "Quit", 
                "Scraper may be running in terminal window.\n\n"
                "Close this UI? (Scraper will continue in terminal)",
                icon="question"
            )
            if result != 'yes':
                return

        self.root.destroy()

    def run(self):
        """Start the UI main loop"""
        try:
            self.root.mainloop()
        except Exception as e:
            print(f"UI error: {e}")


def main():
    """Main entry point for the clean UI"""
    try:
        # Check if main.py exists
        if not Path("main.py").exists():
            messagebox.showerror("Error", "main.py not found!\n\nPlease ensure the scraper files are in the same directory.")
            return

        print("Starting FikFap Scraper UI...")
        print("Logs will open in separate terminal window when you start the scraper.")

        # Create and run UI
        app = FikFapScraperUI()
        app.run()

    except Exception as e:
        print(f"Failed to start UI: {e}")
        if hasattr(e, '__traceback__'):
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
