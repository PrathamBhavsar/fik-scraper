"""
FikFap Scraper UI - Complete Control and Monitoring Interface

This UI provides real-time control and monitoring of the FikFap scraper process.
Features:
- Start/Stop controls with proper state management
- Real-time progress updates tied to disk space checks
- Download directory selection
- Live status display
- Downloaded videos count
- Cross-platform compatible (Windows/Linux)
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


class FikFapScraperUI:
    """Complete UI wrapper for FikFap scraper with real-time monitoring"""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("FikFap Scraper Control Panel")
        self.root.geometry("600x500")
        self.root.resizable(True, True)

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
                with open("settings.json", "r") as f:
                    return json.load(f)
        except Exception as e:
            print(f"Error loading settings: {e}")

        # Return default settings
        return {
            "storage": {"base_path": "./downloads"},
            "monitoring": {"min_disk_space_gb": 1.0}
        }

    def save_settings(self):
        """Save current settings to settings.json"""
        try:
            self.settings["storage"]["base_path"] = self.download_dir
            self.settings["monitoring"]["min_disk_space_gb"] = self.max_size_gb

            with open("settings.json", "w") as f:
                json.dump(self.settings, f, indent=2)
        except Exception as e:
            print(f"Error saving settings: {e}")

    def create_widgets(self):
        """Create all UI widgets"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
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
        self.start_button = ttk.Button(button_frame, text="Start Scraper", 
                                      command=self.start_scraper, style="Success.TButton")
        self.start_button.grid(row=0, column=0, padx=(0, 10), sticky=(tk.W, tk.E))

        # Stop button  
        self.stop_button = ttk.Button(button_frame, text="Stop Scraper", 
                                     command=self.stop_scraper, style="Danger.TButton")
        self.stop_button.grid(row=0, column=1, padx=(10, 0), sticky=(tk.W, tk.E))

        # Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="Progress", padding="10")
        progress_frame.grid(row=2, column=0, columnspan=3, pady=(0, 15), sticky=(tk.W, tk.E, tk.N, tk.S))
        progress_frame.columnconfigure(0, weight=1)

        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, variable=self.progress_var, 
                                           mode='indeterminate')
        self.progress_bar.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

        # Status text
        self.status_var = tk.StringVar(value="Idle")
        status_label = ttk.Label(progress_frame, text="Parser Status:")
        status_label.grid(row=1, column=0, sticky=tk.W)
        self.status_display = ttk.Label(progress_frame, textvariable=self.status_var, 
                                       font=("Arial", 10, "bold"))
        self.status_display.grid(row=2, column=0, sticky=tk.W, pady=(0, 10))

        # Configuration section
        config_frame = ttk.LabelFrame(main_frame, text="Configuration", padding="10")
        config_frame.grid(row=3, column=0, columnspan=3, pady=(0, 15), sticky=(tk.W, tk.E))
        config_frame.columnconfigure(1, weight=1)

        # Download directory
        ttk.Label(config_frame, text="Download Directory:").grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        dir_frame = ttk.Frame(config_frame)
        dir_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        dir_frame.columnconfigure(0, weight=1)

        self.dir_var = tk.StringVar(value=self.download_dir)
        self.dir_entry = ttk.Entry(dir_frame, textvariable=self.dir_var, width=50)
        self.dir_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        self.browse_button = ttk.Button(dir_frame, text="Browse...", command=self.browse_directory)
        self.browse_button.grid(row=0, column=1)

        # Max size setting
        ttk.Label(config_frame, text="Max Disk Size (GB):").grid(row=2, column=0, sticky=tk.W, pady=(5, 5))

        size_frame = ttk.Frame(config_frame)
        size_frame.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))

        self.size_var = tk.DoubleVar(value=self.max_size_gb)
        self.size_spinbox = ttk.Spinbox(size_frame, from_=0.1, to=1000.0, increment=0.1, 
                                       textvariable=self.size_var, width=10)
        self.size_spinbox.grid(row=0, column=0, sticky=tk.W)

        ttk.Button(size_frame, text="Apply Settings", command=self.apply_settings).grid(row=0, column=1, padx=(10, 0))

        # Statistics section
        stats_frame = ttk.LabelFrame(main_frame, text="Statistics", padding="10")
        stats_frame.grid(row=4, column=0, columnspan=3, pady=(0, 15), sticky=(tk.W, tk.E))
        stats_frame.columnconfigure(1, weight=1)

        # Downloaded videos count
        ttk.Label(stats_frame, text="Downloaded Videos:").grid(row=0, column=0, sticky=tk.W, pady=(0, 5))
        self.video_count_var = tk.StringVar(value="0")
        ttk.Label(stats_frame, textvariable=self.video_count_var, 
                 font=("Arial", 12, "bold")).grid(row=0, column=1, sticky=tk.W, pady=(0, 5))

        # Current disk usage
        ttk.Label(stats_frame, text="Current Disk Usage:").grid(row=1, column=0, sticky=tk.W, pady=(0, 5))
        self.disk_usage_var = tk.StringVar(value="0.0 GB")
        ttk.Label(stats_frame, textvariable=self.disk_usage_var, 
                 font=("Arial", 12, "bold")).grid(row=1, column=1, sticky=tk.W, pady=(0, 5))

        # Last update time
        ttk.Label(stats_frame, text="Last Update:").grid(row=2, column=0, sticky=tk.W)
        self.last_update_var = tk.StringVar(value="Never")
        ttk.Label(stats_frame, textvariable=self.last_update_var).grid(row=2, column=1, sticky=tk.W)

        # Log output
        log_frame = ttk.LabelFrame(main_frame, text="Log Output", padding="10")
        log_frame.grid(row=5, column=0, columnspan=3, pady=(0, 0), sticky=(tk.W, tk.E, tk.N, tk.S))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(5, weight=1)

        # Text widget with scrollbar
        text_frame = ttk.Frame(log_frame)
        text_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        text_frame.columnconfigure(0, weight=1)
        text_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(text_frame, height=10, width=70, wrap=tk.WORD)
        scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)

        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))

        # Configure button styles
        style = ttk.Style()
        style.configure("Success.TButton", foreground="green")
        style.configure("Danger.TButton", foreground="red")

        # Initial statistics update
        self.update_statistics()

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

            # Update statistics
            self.update_statistics()

            self.log_message("Settings updated successfully")
            messagebox.showinfo("Success", "Settings have been applied!")

        except ValueError:
            messagebox.showerror("Error", "Invalid max size value!")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to apply settings: {e}")

    def start_scraper(self):
        """Start the scraper process"""
        if self.is_running:
            return

        try:
            # Apply current settings before starting
            self.apply_settings()

            self.is_running = True
            self.stop_reason = ""
            self.update_ui_state()

            # Start progress bar animation
            self.progress_bar.start(10)

            # Start scraper in separate thread
            self.scraper_thread = threading.Thread(target=self.run_scraper, daemon=True)
            self.scraper_thread.start()

            self.log_message("Starting scraper...")

        except Exception as e:
            self.is_running = False
            self.update_ui_state()
            messagebox.showerror("Error", f"Failed to start scraper: {e}")

    def stop_scraper(self):
        """Stop the scraper process"""
        if not self.is_running:
            return

        try:
            self.log_message("Stopping scraper...")

            # Terminate scraper process
            if self.scraper_process and self.scraper_process.poll() is None:
                self.scraper_process.terminate()

                # Wait for process to terminate
                try:
                    self.scraper_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.scraper_process.kill()
                    self.log_message("Scraper process force-killed")

            self.is_running = False
            self.stop_reason = "Stopped by user"
            self.update_ui_state()

        except Exception as e:
            self.log_message(f"Error stopping scraper: {e}")
            self.is_running = False
            self.update_ui_state()

    def run_scraper(self):
        """Run the scraper process (runs in separate thread)"""
        try:
            # Build command to run the scraper
            cmd = [
                sys.executable, "main.py", 
                "--continuous-download",
                "--verbose",
                "--config", "settings.json"
            ]

            # Start the process
            self.scraper_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            self.message_queue.put(("status", "Running"))
            self.message_queue.put(("log", "Scraper process started"))

            # Read output lines
            while True:
                if self.scraper_process.poll() is not None:
                    break

                line = self.scraper_process.stdout.readline()
                if not line:
                    break

                line = line.strip()
                if line:
                    self.message_queue.put(("log", line))

                    # Check for specific status updates
                    if "finished:" in line.lower():
                        self.message_queue.put(("progress_update", line))
                    elif "disk space limit exceeded" in line.lower():
                        self.message_queue.put(("disk_limit", line))
                        break
                    elif "stopping loop" in line.lower():
                        self.message_queue.put(("stopping", line))
                        break

            # Process finished
            return_code = self.scraper_process.returncode
            if return_code != 0 and return_code is not None:
                self.message_queue.put(("error", f"Scraper exited with code {return_code}"))
            else:
                self.message_queue.put(("finished", "Scraper finished normally"))

        except Exception as e:
            self.message_queue.put(("error", f"Error running scraper: {e}"))
        finally:
            self.message_queue.put(("stopped", ""))

    def update_statistics(self):
        """Update statistics display"""
        try:
            # Count downloaded videos (number of directories in download folder)
            download_path = Path(self.download_dir)
            if download_path.exists():
                video_count = len([d for d in download_path.iterdir() if d.is_dir()])
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
                self.disk_usage_var.set(f"{size_gb:.2f} GB / {self.max_size_gb:.1f} GB")
            else:
                self.video_count_var.set("0")
                self.disk_usage_var.set("0.0 GB")

            self.last_update_var.set(datetime.now().strftime("%H:%M:%S"))

        except Exception as e:
            self.log_message(f"Error updating statistics: {e}")

    def process_messages(self):
        """Process messages from the scraper thread"""
        try:
            while not self.message_queue.empty():
                try:
                    message_type, data = self.message_queue.get_nowait()

                    if message_type == "log":
                        self.log_message(data)
                    elif message_type == "status":
                        self.status_var.set(data)
                    elif message_type == "progress_update":
                        # Update statistics when scraper reports cycle completion
                        self.update_statistics()
                        self.log_message(f"Progress: {data}")
                    elif message_type == "disk_limit":
                        self.status_var.set("Scraper stopped, max size reached!")
                        self.stop_reason = "Max disk size reached"
                        self.log_message("DISK LIMIT REACHED - Scraper stopped automatically")
                    elif message_type == "stopping":
                        self.status_var.set("Stopping...")
                    elif message_type == "finished":
                        self.status_var.set("Finished")
                        self.log_message(data)
                    elif message_type == "error":
                        self.status_var.set("Error")
                        self.log_message(f"ERROR: {data}")
                    elif message_type == "stopped":
                        self.is_running = False
                        self.progress_bar.stop()
                        self.update_ui_state()
                        if not self.stop_reason:
                            self.stop_reason = "Process ended"
                        final_status = f"Stopped ({self.stop_reason})"
                        self.status_var.set(final_status)
                        self.update_statistics()

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
            self.status_var.set("Starting...")
        else:
            self.start_button.config(state="normal")
            self.stop_button.config(state="disabled")
            self.dir_entry.config(state="normal")
            self.browse_button.config(state="normal")
            self.size_spinbox.config(state="normal")
            if not self.stop_reason:
                self.status_var.set("Idle")

    def log_message(self, message: str):
        """Add message to log output"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"

        self.log_text.insert(tk.END, formatted_message)
        self.log_text.see(tk.END)

        # Limit log size (keep last 1000 lines)
        lines = self.log_text.get("1.0", tk.END).count("\n")
        if lines > 1000:
            self.log_text.delete("1.0", "100.0")

    def on_closing(self):
        """Handle window close event"""
        if self.is_running:
            if messagebox.askokcancel("Quit", "Scraper is running. Stop and quit?"):
                self.stop_scraper()
                # Wait a moment for cleanup
                self.root.after(1000, self.root.destroy)
            return

        self.root.destroy()

    def run(self):
        """Start the UI main loop"""
        self.root.mainloop()


def main():
    """Main entry point for the UI"""
    try:
        # Check if main.py exists
        if not Path("main.py").exists():
            messagebox.showerror("Error", "main.py not found! Please ensure the scraper files are in the same directory.")
            return

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
