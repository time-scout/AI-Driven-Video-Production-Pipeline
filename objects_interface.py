# objects_interface.py

import tkinter as tk
from tkinter import ttk, filedialog
import json
import threading
import queue
import sys
from pathlib import Path
from MovingClipWorker import MovingClipWorker

class QueueLogger:
    """Object that mimics a file, redirecting 'write' to a queue."""
    def __init__(self, queue):
        self.queue = queue

    def write(self, message):
        self.queue.put(message)

    def flush(self):
        """Required method for compatibility with file-like objects."""
        pass

class ObjectsTab(ttk.Frame):
    def __init__(self, parent, main_app):
        super().__init__(parent)
        self.main_app = main_app
        self.config_key = "moving_clip" # Unique key for settings of this sub-tab

        # Restructuring main frame to accommodate log panel
        self.rowconfigure(0, weight=4) # Main part takes 80%
        self.rowconfigure(1, weight=1) # Log takes 20%
        self.columnconfigure(0, weight=1)

        self.objects_notebook = ttk.Notebook(self)
        self.objects_notebook.grid(row=0, column=0, sticky='nsew', padx=10, pady=(10, 5))

        self.create_moving_clip_tab()

        # Bind saving to changes in text fields
        self.source_video_path.trace_add("write", self._save_settings)
        self.background_video_path.trace_add("write", self._save_settings)
        self.output_dir_path.trace_add("write", self._save_settings)
        self.scale_var.trace_add("write", self._save_settings)
        self.crf_var.trace_add("write", self._save_settings)
        self.trajectory_var.trace_add("write", self._save_settings)
        self.travel_speed_var.trace_add("write", self._save_settings)

        # Load settings after creating widgets
        self._load_settings()

        # Adding logging panel
        log_frame = ttk.LabelFrame(self, text="Execution Log")
        log_frame.grid(row=1, column=0, sticky='nsew', padx=10, pady=(5, 10))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, height=8, state='disabled', wrap='word', bg='#f0f0f0')
        self.log_text.grid(row=0, column=0, sticky='nsew')

        # Add queue for safe communication with Worker thread
        self.log_queue = None
    def create_moving_clip_tab(self):
        """Creates and populates the interface for the 'Moving Clip' sub-tab."""
        moving_clip_frame = ttk.Frame(self.objects_notebook)
        self.objects_notebook.add(moving_clip_frame, text='Moving Clip')

        # --- Basic Grid Creation ---
        # GRID CONFIGURATION CHANGED FOR CORRECT FIELD WIDTH
        moving_clip_frame.columnconfigure(1, weight=1) # Input fields column
        moving_clip_frame.columnconfigure(2, weight=0) # Browse buttons column
        moving_clip_frame.columnconfigure(3, weight=1) # Empty space-filling column

        # --- File Selection Widgets ---
        ttk.Label(moving_clip_frame, text="Source video (interview):").grid(row=0, column=0, padx=5, pady=8, sticky='w')
        self.source_video_path = tk.StringVar()
        ttk.Entry(moving_clip_frame, textvariable=self.source_video_path).grid(row=0, column=1, padx=5, pady=8, sticky='ew')
        ttk.Button(moving_clip_frame, text="Browse...", command=self.select_source_video).grid(row=0, column=2, padx=5, pady=8)

        ttk.Label(moving_clip_frame, text="Background video (overlay):").grid(row=1, column=0, padx=5, pady=8, sticky='w')
        self.background_video_path = tk.StringVar()
        ttk.Entry(moving_clip_frame, textvariable=self.background_video_path).grid(row=1, column=1, padx=5, pady=8, sticky='ew')
        ttk.Button(moving_clip_frame, text="Browse...", command=self.select_background_video).grid(row=1, column=2, padx=5, pady=8)

        ttk.Label(moving_clip_frame, text="Output folder:").grid(row=2, column=0, padx=5, pady=8, sticky='w')
        self.output_dir_path = tk.StringVar()
        ttk.Entry(moving_clip_frame, textvariable=self.output_dir_path).grid(row=2, column=1, padx=5, pady=8, sticky='ew')
        ttk.Button(moving_clip_frame, text="Select folder...", command=self.select_output_dir).grid(row=2, column=2, padx=5, pady=8)

        # --- Object Settings Frame ---
        settings_frame = ttk.LabelFrame(moving_clip_frame, text="Object Settings")
        settings_frame.grid(row=3, column=0, columnspan=4, padx=5, pady=10, sticky='ew')
        # Grid configuration for uniform distribution
        for i in range(4):
            settings_frame.columnconfigure(i, weight=1)

        # --- Scale Setting ---
        ttk.Label(settings_frame, text="Clip scale (%):").grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.scale_var = tk.StringVar(value="70")
        ttk.Entry(settings_frame, textvariable=self.scale_var, width=10).grid(row=0, column=1, padx=5, pady=5, sticky='w')

        # --- CRF Setting ---
        ttk.Label(settings_frame, text="CRF (quality):").grid(row=0, column=2, padx=5, pady=5, sticky='w')
        self.crf_var = tk.StringVar(value="23")
        ttk.Entry(settings_frame, textvariable=self.crf_var, width=10).grid(row=0, column=3, padx=5, pady=5, sticky='w')

        # --- Trajectory Setting ---
        ttk.Label(settings_frame, text="Movement trajectory:").grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.trajectory_var = tk.StringVar()
        trajectory_options = ["Smooth movement right", "Wobble (sine wave)", "Static (center)"]
        ttk.Combobox(settings_frame, textvariable=self.trajectory_var, values=trajectory_options, state="readonly", width=25).grid(row=1, column=1, padx=5, pady=5, sticky='w')
        self.trajectory_var.set(trajectory_options[0])

        # --- Movement Speed Setting ---
        ttk.Label(settings_frame, text="Movement speed (multiplier):").grid(row=1, column=2, padx=5, pady=5, sticky='w')
        self.travel_speed_var = tk.StringVar(value="1.5")
        ttk.Entry(settings_frame, textvariable=self.travel_speed_var, width=10).grid(row=1, column=3, padx=5, pady=5, sticky='w')

        # --- Max Tilt Setting ---
        ttk.Label(settings_frame, text="Max tilt (degrees):").grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.max_tilt_var = tk.IntVar(value=5)
        ttk.Scale(settings_frame, from_=0, to=45, orient='horizontal', variable=self.max_tilt_var, command=lambda v: self._update_and_save(self.max_tilt_var, int(float(v)))).grid(row=2, column=1, padx=5, pady=5, sticky='ew')
        ttk.Label(settings_frame, textvariable=self.max_tilt_var).grid(row=2, column=2, padx=(0,5), pady=5, sticky='w')

        # --- Wobble Speed Setting ---
        ttk.Label(settings_frame, text="Wobble speed:").grid(row=3, column=0, padx=5, pady=5, sticky='w')
        self.wobble_speed_var = tk.DoubleVar(value=1.0)
        ttk.Scale(settings_frame, from_=0.1, to=5.0, orient='horizontal', variable=self.wobble_speed_var, command=lambda v: self._update_and_save(self.wobble_speed_var, round(float(v), 1))).grid(row=3, column=1, padx=5, pady=5, sticky='ew')
        ttk.Label(settings_frame, textvariable=self.wobble_speed_var).grid(row=3, column=2, padx=(0,5), pady=5, sticky='w')

        # --- Edge Feathering Setting ---
        ttk.Label(settings_frame, text="Edge feathering (%):").grid(row=4, column=0, padx=5, pady=5, sticky='w')
        self.feather_var = tk.IntVar(value=15)
        ttk.Scale(settings_frame, from_=0, to=50, orient='horizontal', variable=self.feather_var, command=lambda v: self._update_and_save(self.feather_var, int(float(v)))).grid(row=4, column=1, padx=5, pady=5, sticky='ew')
        ttk.Label(settings_frame, textvariable=self.feather_var).grid(row=4, column=2, padx=(0,5), pady=5, sticky='w')

        # --- Start Button ---
        ttk.Button(moving_clip_frame, text="Create Object", command=self.start_processing).grid(row=4, column=0, columnspan=4, padx=5, pady=20)

    # --- Button Methods (stubs to be refined) ---
    def select_source_video(self):
        path = filedialog.askopenfilename(title="Select source video")
        if path: self.source_video_path.set(path)

    def select_background_video(self):
        path = filedialog.askopenfilename(title="Select background video")
        if path: self.background_video_path.set(path)

    def select_output_dir(self):
        path = filedialog.askdirectory(title="Select folder to save result")
        if path: self.output_dir_path.set(path)
        

    def start_processing(self):
        source_path = self.source_video_path.get()
        background_path = self.background_video_path.get()
        output_dir = self.output_dir_path.get()

        if not all([source_path, background_path, output_dir]):
            self._write_to_log("Error: not all paths specified.")
            return

        # Generate output filename
        source_filename = Path(source_path).stem
        output_filename = f"{source_filename}_animated.mp4"
        output_path = str(Path(output_dir) / output_filename)

        # Collect options into dictionary
        options = {
            'source_path': source_path,
            'background_path': background_path,
            'output_path': output_path,
            'scale': self.scale_var.get(),
            'trajectory': self.trajectory_var.get(),
            'crf': self.crf_var.get(),
            'travel_speed': self.travel_speed_var.get(),
            'max_tilt': self.max_tilt_var.get(),
            'wobble_speed': self.wobble_speed_var.get(),
            'feather': self.feather_var.get()
        }

        self._clear_log()
        self.log_queue = queue.Queue()

        def run_worker():
            """Function to be executed in a separate thread."""
            try:
                worker = MovingClipWorker()
                # Call process_video, passing both options and log_queue
                worker.process_video(options, self.log_queue)
            except Exception as e:
                # Log any unexpected errors from the worker
                if self.log_queue:
                    self.log_queue.put(f"CRITICAL ERROR IN WORKER: {e}")

        self._write_to_log("Starting task in background mode...")
        thread = threading.Thread(target=run_worker)
        thread.start()
        # Start queue check loop
        self.after(100, self._check_log_queue)
    def _load_settings(self):
        # Get settings for the entire "Objects" tab
        objects_config = self.main_app.get_setting("objects_tab", {})
        # Get settings specifically for our sub-tab
        tab_settings = objects_config.get(self.config_key, {})

        self.source_video_path.set(tab_settings.get("source_video_path", ""))
        self.background_video_path.set(tab_settings.get("background_video_path", ""))
        self.output_dir_path.set(tab_settings.get("output_dir_path", ""))

        self.scale_var.set(tab_settings.get("scale", "70"))
        self.crf_var.set(tab_settings.get("crf", "23"))
        self.trajectory_var.set(tab_settings.get("trajectory", "Smooth movement right"))
        self.travel_speed_var.set(tab_settings.get("travel_speed", "1.5"))
        self.max_tilt_var.set(tab_settings.get("max_tilt", 5))
        self.wobble_speed_var.set(tab_settings.get("wobble_speed", 1.0))
        self.feather_var.set(tab_settings.get("feather", 15))

    def _update_and_save(self, var, value):
        var.set(value)
        self._save_settings()

    def _save_settings(self, *args):
        # Ensure main section exists
        if "objects_tab" not in self.main_app.settings:
            self.main_app.settings["objects_tab"] = {}

        # Collect current settings
        settings_to_save = {
            "source_video_path": self.source_video_path.get(),
            "background_video_path": self.background_video_path.get(),
            "output_dir_path": self.output_dir_path.get(),
            "scale": self.scale_var.get(),
            "trajectory": self.trajectory_var.get(),
            "crf": self.crf_var.get(),
            "travel_speed": self.travel_speed_var.get(),
            "max_tilt": self.max_tilt_var.get(),
            "wobble_speed": self.wobble_speed_var.get(),
            "feather": self.feather_var.get()
        }

        # Update config and call save method in main app
        self.main_app.settings["objects_tab"][self.config_key] = settings_to_save
        self.main_app._save_settings()

    def _write_to_log(self, message):
        """Safely adds a message to the log text field,
        handling carriage return for progress bar updates."""
        self.log_text.configure(state='normal')

        # If message contains carriage return - it's a progress bar update
        if '\r' in message:
            # Remove the last line in the log to replace it with a new one
            # 'end-2l' - start of penultimate line, 'end-1l' - end of penultimate line
            last_line_content = self.log_text.get("end-2l", "end-1l")
            if "Done" not in last_line_content: # Don't remove final "Done"
                 self.log_text.delete("end-2l", "end-1l")

            # Insert cleaned message
            clean_message = message.strip()
            self.log_text.insert(tk.END, clean_message + '\n')
        else:
            # For regular messages just add them
            self.log_text.insert(tk.END, message)

        self.log_text.see(tk.END) # Auto-scroll down
        self.log_text.configure(state='disabled')

    def _clear_log(self):
        """Clears the log before a new run."""
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')

    def _check_log_queue(self):
        """Checks the queue for new messages and outputs them."""
        try:
            while True:
                message = self.log_queue.get_nowait()
                self._write_to_log(message)
        except queue.Empty:
            pass
        finally:
            self.after(100, self._check_log_queue)