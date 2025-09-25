"""
Enhanced FikFap Scraper UI with Proper Subprocess Termination Detection

ENHANCEMENTS:
1. Monitors subprocess exit status in real-time
2. Automatically stops progress animation when subprocess terminates
3. Closes UI window when disk space limit is exceeded (exit code 1)
4. Shows termination reason in final status
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import subprocess
import time
import json
import os
import sys
from pathlib import Path
from datetime import datetime
import queue

class FikFapScraperUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("FikFap Scraper - Enhanced Process Monitor")
        self.root.geometry("800x600")
        self.root.configure(bg='#2b2b2b')

        # Process control
        self.process = None
        self.is_running = False
        self.progress_thread = None
        self.monitor_thread = None
        self.should_stop_progress = False
        
        # Progress tracking
        self.progress_steps = [
            "Initializing system components...",
            "Loading configuration settings...", 
            "Checking disk space and limits...",
            "Starting HTTP session...",
            "Fetching posts from API...",
            "Processing video metadata...",
            "Downloading video files...",
            "Organizing downloaded content...",
            "Validating file integrity...",
            "Updating progress statistics..."
        ]
        self.current_step = 0
        self.last_step_time = time.time()
        
        # Statistics
        self.stats = {
            "start_time": None,
            "cycles_completed": 0,
            "posts_processed": 0,
            "videos_downloaded": 0,
            "files_created": 0,
            "disk_usage_gb": 0.0,
            "disk_limit_gb": 2.0
        }
        
        # Queue for thread communication
        self.message_queue = queue.Queue()
        
        self.setup_ui()
        self.load_settings()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Start message processing
        self.process_message_queue()

    def setup_ui(self):
        """Setup the main UI components"""
        # Main container
        main_frame = tk.Frame(self.root, bg='#2b2b2b', padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title
        title_label = tk.Label(
            main_frame,
            text="FikFap Scraper - Process Monitor",
            font=("Arial", 18, "bold"),
            fg='#ffffff',
            bg='#2b2b2b'
        )
        title_label.pack(pady=(0, 20))

        # Settings frame
        settings_frame = tk.LabelFrame(
            main_frame,
            text="Settings",
            font=("Arial", 12, "bold"),
            fg='#ffffff',
            bg='#3d3d3d',
            relief=tk.RAISED,
            bd=2
        )
        settings_frame.pack(fill=tk.X, pady=(0, 20))

        # Disk limit setting
        disk_frame = tk.Frame(settings_frame, bg='#3d3d3d')
        disk_frame.pack(fill=tk.X, padx=10, pady=10)

        tk.Label(
            disk_frame,
            text="Disk Space Limit (GB):",
            font=("Arial", 10),
            fg='#ffffff',
            bg='#3d3d3d'
        ).pack(side=tk.LEFT)

        self.disk_limit_var = tk.StringVar(value="2.0")
        disk_entry = tk.Entry(
            disk_frame,
            textvariable=self.disk_limit_var,
            width=10,
            font=("Arial", 10),
            bg='#555555',
            fg='#ffffff',
            insertbackground='#ffffff'
        )
        disk_entry.pack(side=tk.LEFT, padx=(10, 0))

        # Control buttons frame
        control_frame = tk.Frame(main_frame, bg='#2b2b2b')
        control_frame.pack(fill=tk.X, pady=(0, 20))

        self.start_button = tk.Button(
            control_frame,
            text="Start Continuous Scraping",
            command=self.start_scraping,
            font=("Arial", 12, "bold"),
            bg='#4CAF50',
            fg='white',
            relief=tk.RAISED,
            bd=3,
            padx=20,
            pady=10
        )
        self.start_button.pack(side=tk.LEFT, padx=(0, 10))

        self.stop_button = tk.Button(
            control_frame,
            text="Stop Process",
            command=self.stop_scraping,
            font=("Arial", 12, "bold"),
            bg='#f44336',
            fg='white',
            relief=tk.RAISED,
            bd=3,
            padx=20,
            pady=10,
            state=tk.DISABLED
        )
        self.stop_button.pack(side=tk.LEFT)

        # Status frame
        status_frame = tk.LabelFrame(
            main_frame,
            text="Process Status",
            font=("Arial", 12, "bold"),
            fg='#ffffff',
            bg='#3d3d3d',
            relief=tk.RAISED,
            bd=2
        )
        status_frame.pack(fill=tk.X, pady=(0, 20))

        # Current operation
        self.status_label = tk.Label(
            status_frame,
            text="Ready to start...",
            font=("Arial", 11),
            fg='#00ff41',
            bg='#3d3d3d',
            anchor='w'
        )
        self.status_label.pack(fill=tk.X, padx=10, pady=(10, 5))

        # Progress bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            status_frame,
            variable=self.progress_var,
            mode='determinate',
            length=400
        )
        self.progress_bar.pack(fill=tk.X, padx=10, pady=5)

        # Statistics frame
        stats_frame = tk.LabelFrame(
            main_frame,
            text="Statistics",
            font=("Arial", 12, "bold"),
            fg='#ffffff',
            bg='#3d3d3d',
            relief=tk.RAISED,
            bd=2
        )
        stats_frame.pack(fill=tk.BOTH, expand=True)

        # Statistics display
        self.stats_text = scrolledtext.ScrolledText(
            stats_frame,
            font=("Consolas", 10),
            bg='#1e1e1e',
            fg='#00ff41',
            insertbackground='#ffffff',
            height=15,
            state=tk.DISABLED
        )
        self.stats_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    def load_settings(self):
        """Load settings from JSON file"""
        try:
            if Path("settings.json").exists():
                with open("settings.json", "r", encoding='utf-8') as f:
                    settings = json.load(f)
                    monitoring = settings.get("monitoring", {})
                    disk_limit = monitoring.get("min_disk_space_gb", 2.0)
                    self.disk_limit_var.set(str(disk_limit))
                    self.stats["disk_limit_gb"] = disk_limit
        except Exception as e:
            self.log_message(f"Error loading settings: {e}")

    def save_settings(self):
        """Save current settings to JSON file"""
        try:
            settings = {
                "storage": {"base_path": "./downloads"},
                "monitoring": {"min_disk_space_gb": float(self.disk_limit_var.get())}
            }
            with open("settings.json", "w", encoding='utf-8') as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
            self.stats["disk_limit_gb"] = float(self.disk_limit_var.get())
        except Exception as e:
            self.log_message(f"Error saving settings: {e}")

    def start_scraping(self):
        """Start the scraping process"""
        if self.is_running:
            return

        self.save_settings()
        
        # Reset state
        self.is_running = True
        self.should_stop_progress = False
        self.current_step = 0
        self.stats["start_time"] = datetime.now()
        
        # Update UI
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self.status_label.config(text="Starting continuous scraping process...", fg='#ffaa00')
        self.progress_var.set(0)
        
        # Clear previous stats
        self.stats_text.config(state=tk.NORMAL)
        self.stats_text.delete(1.0, tk.END)
        self.stats_text.config(state=tk.DISABLED)
        
        self.log_message("=== STARTING FIKFAP SCRAPER ===")
        self.log_message(f"Disk space limit: {self.stats['disk_limit_gb']} GB")
        self.log_message("Process will auto-terminate when limit is exceeded")
        self.log_message("")

        # Start subprocess
        try:
            cmd = [sys.executable, "main.py", "--continuous-download"]
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                bufsize=1,
                universal_newlines=True
            )
            
            # Start monitoring threads
            self.progress_thread = threading.Thread(target=self.update_progress, daemon=True)
            self.monitor_thread = threading.Thread(target=self.monitor_process, daemon=True)
            
            self.progress_thread.start()
            self.monitor_thread.start()
            
            self.log_message("Process started successfully")
            
        except Exception as e:
            self.log_message(f"ERROR: Failed to start process: {e}")
            self.reset_ui_state()

    def monitor_process(self):
        """Monitor the subprocess for termination and output"""
        if not self.process:
            return
            
        try:
            # Read output in real-time
            while self.process.poll() is None and self.is_running:
                try:
                    line = self.process.stdout.readline()
                    if line:
                        line = line.strip()
                        if line:
                            # Extract useful information from log lines
                            self.parse_log_line(line)
                except Exception:
                    break
                    
                time.sleep(0.1)
            
            # Process has terminated
            exit_code = self.process.poll()
            self.handle_process_termination(exit_code)
            
        except Exception as e:
            self.message_queue.put(("error", f"Process monitoring error: {e}"))

    def parse_log_line(self, line):
        """Parse log lines for statistics and important events"""
        try:
            # Look for cycle completion
            if "Cycle" in line and "finished:" in line:
                # Extract cycle stats
                if "posts processed" in line:
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit() and i > 0 and "posts" in parts[i+1]:
                            self.stats["posts_processed"] += int(part)
                            break
                
                if "videos downloaded" in line:
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit() and i > 0 and "videos" in parts[i+1]:
                            self.stats["videos_downloaded"] += int(part)
                            break
                
                if "files" in line:
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit() and i > 0 and "files" in parts[i+1]:
                            self.stats["files_created"] += int(part)
                            break
                
                self.stats["cycles_completed"] += 1
                self.message_queue.put(("cycle_complete", line))
            
            # Look for disk space warnings
            elif "DISK SPACE LIMIT EXCEEDED" in line:
                self.message_queue.put(("disk_limit_exceeded", line))
            
            # Look for errors
            elif "ERROR" in line or "CRITICAL" in line:
                self.message_queue.put(("error", line))
            
            # Other important messages
            elif any(keyword in line for keyword in ["Starting", "completed", "downloaded", "processed"]):
                self.message_queue.put(("info", line))
                
        except Exception:
            pass

    def handle_process_termination(self, exit_code):
        """Handle process termination based on exit code"""
        termination_reason = ""
        auto_close = False
        
        if exit_code == 0:
            termination_reason = "Process completed successfully"
            self.message_queue.put(("success", "Process completed successfully"))
        elif exit_code == 1:
            termination_reason = "Process terminated due to disk space limit exceeded"
            self.message_queue.put(("disk_limit_exceeded", "DISK SPACE LIMIT EXCEEDED - Process terminated"))
            auto_close = True
        else:
            termination_reason = f"Process terminated with error (exit code: {exit_code})"
            self.message_queue.put(("error", f"Process terminated with exit code: {exit_code}"))
        
        # Stop progress animation and update UI
        self.message_queue.put(("process_terminated", {
            "exit_code": exit_code,
            "reason": termination_reason,
            "auto_close": auto_close
        }))

    def update_progress(self):
        """Update progress animation"""
        while self.is_running and not self.should_stop_progress:
            try:
                # Cycle through progress steps
                if time.time() - self.last_step_time > 3:  # 3 seconds per step
                    self.current_step = (self.current_step + 1) % len(self.progress_steps)
                    self.last_step_time = time.time()
                
                # Update progress bar (0-100)
                progress = ((self.current_step + 1) / len(self.progress_steps)) * 100
                
                self.message_queue.put(("progress", {
                    "step": self.progress_steps[self.current_step],
                    "progress": progress
                }))
                
                time.sleep(0.5)
                
            except Exception:
                break

    def process_message_queue(self):
        """Process messages from background threads"""
        try:
            while True:
                try:
                    msg_type, data = self.message_queue.get_nowait()
                    
                    if msg_type == "progress":
                        if self.is_running:
                            self.status_label.config(text=data["step"], fg='#ffaa00')
                            self.progress_var.set(data["progress"])
                    
                    elif msg_type == "cycle_complete":
                        self.log_message(f"CYCLE: {data}")
                        self.update_stats_display()
                    
                    elif msg_type == "disk_limit_exceeded":
                        self.log_message(f"CRITICAL: {data}")
                        self.status_label.config(text="DISK SPACE LIMIT EXCEEDED!", fg='#ff0000')
                    
                    elif msg_type == "error":
                        self.log_message(f"ERROR: {data}")
                    
                    elif msg_type == "info":
                        self.log_message(f"INFO: {data}")
                    
                    elif msg_type == "success":
                        self.log_message(f"SUCCESS: {data}")
                        self.status_label.config(text="Process completed successfully", fg='#00ff41')
                    
                    elif msg_type == "process_terminated":
                        self.handle_ui_termination(data)
                    
                    self.message_queue.task_done()
                    
                except queue.Empty:
                    break
                    
        except Exception as e:
            print(f"Message queue error: {e}")
        
        # Schedule next check
        self.root.after(100, self.process_message_queue)

    def handle_ui_termination(self, termination_data):
        """Handle UI updates when process terminates"""
        exit_code = termination_data["exit_code"]
        reason = termination_data["reason"]
        auto_close = termination_data["auto_close"]
        
        # Stop progress animation
        self.should_stop_progress = True
        
        # Update UI state
        self.reset_ui_state()
        
        # Update status and log
        if exit_code == 1:
            self.status_label.config(text="TERMINATED: Disk space limit exceeded!", fg='#ff0000')
            self.log_message("=== PROCESS TERMINATED BY DISK SPACE LIMIT ===")
        else:
            self.status_label.config(text=reason, fg='#ff0000' if exit_code != 0 else '#00ff41')
        
        self.log_message(f"Process exit code: {exit_code}")
        self.log_message(f"Termination reason: {reason}")
        self.log_message("=== PROCESS MONITORING STOPPED ===")
        
        # Auto-close if disk limit exceeded
        if auto_close:
            self.log_message("Auto-closing UI in 5 seconds due to disk space limit...")
            self.root.after(5000, self.auto_close_application)

    def auto_close_application(self):
        """Automatically close the application"""
        try:
            self.root.quit()
            self.root.destroy()
        except Exception:
            pass

    def stop_scraping(self):
        """Stop the scraping process"""
        if not self.is_running:
            return

        self.log_message("Stopping scraping process...")
        self.should_stop_progress = True
        
        if self.process:
            try:
                self.process.terminate()
                time.sleep(2)
                if self.process.poll() is None:
                    self.process.kill()
            except Exception as e:
                self.log_message(f"Error stopping process: {e}")
        
        self.reset_ui_state()
        self.status_label.config(text="Process stopped by user", fg='#ffaa00')

    def reset_ui_state(self):
        """Reset UI to initial state"""
        self.is_running = False
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.progress_var.set(0)

    def update_stats_display(self):
        """Update the statistics display"""
        if not self.stats["start_time"]:
            return
            
        runtime = datetime.now() - self.stats["start_time"]
        runtime_str = str(runtime).split('.')[0]  # Remove microseconds
        
        stats_text = f"""
Runtime: {runtime_str}
Cycles Completed: {self.stats['cycles_completed']}
Posts Processed: {self.stats['posts_processed']}
Videos Downloaded: {self.stats['videos_downloaded']}
Files Created: {self.stats['files_created']}
Disk Limit: {self.stats['disk_limit_gb']} GB

Process Status: {"RUNNING" if self.is_running else "STOPPED"}
Auto-terminate when disk limit exceeded: YES
        """.strip()
        
        self.stats_text.config(state=tk.NORMAL)
        self.stats_text.delete(1.0, tk.END)
        self.stats_text.insert(tk.END, stats_text)
        self.stats_text.config(state=tk.DISABLED)

    def log_message(self, message):
        """Add a message to the log display"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        self.stats_text.config(state=tk.NORMAL)
        self.stats_text.insert(tk.END, log_entry)
        self.stats_text.see(tk.END)
        self.stats_text.config(state=tk.DISABLED)

    def on_closing(self):
        """Handle window closing event"""
        if self.is_running:
            if messagebox.askokcancel("Quit", "Scraping is running. Stop and quit?"):
                self.stop_scraping()
                self.root.after(1000, self.root.quit)
        else:
            self.root.quit()

    def run(self):
        """Start the UI main loop"""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            pass
        finally:
            if self.process:
                try:
                    self.process.terminate()
                except Exception:
                    pass


if __name__ == "__main__":
    app = FikFapScraperUI()
    app.run()