# project_create_and_voice_interface.py

import json
import re
import shutil
import subprocess
import sys
import threading
import time
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import filedialog, font, messagebox, scrolledtext, ttk
from collections import deque

import pandas as pd
from pydub import AudioSegment
from pydub.playback import play

# Import our backend
import voice_generator


class VoiceManagerDialog(tk.Toplevel):
    """Dialog window for adding/editing a voice."""

    def __init__(self, parent, title, voice_data=None):
        super().__init__(parent)
        self.transient(parent)
        self.title(title)
        self.parent = parent
        self.result = None
        self.new_sample_path = None

        voice_data = voice_data or {"actor_name": "", "template_uuid": "", "remark": "", "sample_file": ""}

        ttk.Label(self, text="Voice Name:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.name_entry = ttk.Entry(self, width=40)
        self.name_entry.grid(row=0, column=1, columnspan=2, padx=10, pady=5, sticky="we")
        self.name_entry.insert(0, voice_data["actor_name"])

        ttk.Label(self, text="Template UUID:").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.uuid_entry = ttk.Entry(self, width=40)
        self.uuid_entry.grid(row=1, column=1, columnspan=2, padx=10, pady=5, sticky="we")
        self.uuid_entry.insert(0, voice_data["template_uuid"])

        ttk.Label(self, text="Remark:").grid(row=2, column=0, padx=10, pady=5, sticky="nw")
        self.remark_text = tk.Text(self, width=40, height=4, wrap=tk.WORD)
        self.remark_text.grid(row=2, column=1, columnspan=2, padx=10, pady=5, sticky="we")
        self.remark_text.insert("1.0", voice_data["remark"])

        ttk.Label(self, text="Audio Sample:").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.sample_label = ttk.Label(self, text=voice_data.get("sample_file") or "not attached", width=25, anchor="w",
                                      wraplength=180)
        self.sample_label.grid(row=3, column=1, padx=10, pady=5, sticky="we")
        ttk.Button(self, text="Attach...", command=self._attach_sample).grid(row=3, column=2, padx=5, pady=5)

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=4, column=0, columnspan=3, pady=10)
        ttk.Button(btn_frame, text="Save", command=self._on_ok).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self.destroy).pack(side="left", padx=5)

        self.update_idletasks()
        x = self.parent.winfo_rootx() + (self.parent.winfo_width() / 2) - (self.winfo_width() / 2)
        y = self.parent.winfo_rooty() + (self.parent.winfo_height() / 2) - (self.winfo_height() / 2)
        self.geometry(f"+{int(x)}+{int(y)}")

        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window(self)

    def _attach_sample(self):
        filepath = filedialog.askopenfilename(
            title="Select audio sample",
            filetypes=[("Audio Files", "*.mp3 *.wav *.m4a"), ("All files", "*.*")],
            parent=self
        )
        if filepath:
            self.new_sample_path = Path(filepath)
            self.sample_label.config(text=self.new_sample_path.name)

    def _on_ok(self, event=None):
        name = self.name_entry.get().strip()
        uuid = self.uuid_entry.get().strip()
        if not name or not uuid:
            messagebox.showwarning("Input Error", "Voice name and UUID cannot be empty.", parent=self)
            return
        self.result = {"actor_name": name, "template_uuid": uuid, "remark": self.remark_text.get("1.0", tk.END).strip()}
        self.destroy()


class ProjectCreateAndVoiceTab(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.root = controller.root

        self.COMMON_ASSETS_PATH = self.controller.COMMON_ASSETS_PATH
        if self.controller.COMMON_ASSETS_PATH:
            self.VOICE_ASSETS_PATH = self.controller.COMMON_ASSETS_PATH / 'voice_samples'
            self.CONFIG_PATH = self.VOICE_ASSETS_PATH / 'voice_config.json'
            self.SOUND_SUCCESS = self.COMMON_ASSETS_PATH / 'sound_alerts' / 'voicing_complete.mp3'
            self.SOUND_ERROR = self.COMMON_ASSETS_PATH / 'sound_alerts' / 'voicing_error.mp3'
            self.SOUND_FRAGMENT_COMPLETE = self.COMMON_ASSETS_PATH / 'sound_alerts' / 'voicing_fragment_complete.mp3'
        else:
            self.VOICE_ASSETS_PATH = None
            self.CONFIG_PATH = None
            self.SOUND_SUCCESS = None
            self.SOUND_ERROR = None
            self.SOUND_FRAGMENT_COMPLETE = None

        if self.controller.DATABASE_PATH:
            self.DB_PATH = Path(self.controller.DATABASE_PATH) / 'main_database.xlsx'
            self.PROJECTS_ROOT_PATH = Path(self.controller.DATABASE_PATH).parent
        else:
            self.DB_PATH = None
            self.PROJECTS_ROOT_PATH = None

        self.api_key = ""
        self.voice_templates = []
        self.project_queue = deque()
        self.queue_processor_thread = None
        self.single_voicing_thread = None
        self.stop_queue_event = threading.Event()
        self.is_processing = False
        self.single_voicing_output_path = Path.home()

        self._ensure_config_exists()
        self._load_config()
        self._build_ui()
        self._update_api_key_label()
        self._populate_voices_table()

    def log_message(self, message: str, level: str = "INFO"):
        timestamp = time.strftime('%H:%M:%S')
        tag_map = {"INFO": "info", "SUCCESS": "success", "WARN": "warn", "ERROR": "error", "FATAL": "fatal",
                   "DEBUG": "debug"}
        tag = tag_map.get(level, "info")
        if tag != "debug":
            full_message = f"[{timestamp}] {message}\n"
            self.root.after(0, self._insert_log, full_message, tag)

    def _insert_log(self, msg, tag):
        self.log_widget.configure(state='normal')
        self.log_widget.insert(tk.END, msg, tag)
        self.log_widget.see(tk.END)
        self.log_widget.configure(state='disabled')

    def _build_ui(self):
        main_container = ttk.Frame(self)
        main_container.pack(fill='both', expand=True, padx=10, pady=5)
        main_container.columnconfigure(0, weight=3);
        main_container.columnconfigure(1, weight=2);
        main_container.rowconfigure(0, weight=1)

        left_panel = ttk.Frame(main_container)
        left_panel.grid(row=0, column=0, sticky='nsew', padx=(0, 5))
        left_panel.rowconfigure(0, weight=1);
        left_panel.columnconfigure(0, weight=1)

        notebook = ttk.Notebook(left_panel)
        notebook.grid(row=0, column=0, sticky='nsew')

        project_tab = ttk.Frame(notebook, padding=5);
        single_tab = ttk.Frame(notebook, padding=5)
        notebook.add(project_tab, text=' Project Voicing ');
        notebook.add(single_tab, text=' Single Voicing ')

        self._build_project_tab(project_tab);
        self._build_single_voicing_tab(single_tab)

        voices_frame = ttk.LabelFrame(main_container, text="Voice Manager", padding=10)
        voices_frame.grid(row=0, column=1, sticky='nsew', padx=(5, 0))
        voices_frame.rowconfigure(0, weight=1);
        voices_frame.columnconfigure(0, weight=1)

        cols = ("Voice Name", "Template UUID", "Remark")
        self.voices_tree = ttk.Treeview(voices_frame, columns=cols, show='headings', selectmode='browse')
        self.voices_tree.grid(row=0, column=0, sticky='nsew')

        voice_btns_frame = ttk.Frame(voices_frame)
        voice_btns_frame.grid(row=1, column=0, sticky='ew', pady=(10, 0))
        ttk.Button(voice_btns_frame, text="Add", command=self._add_voice).pack(side='left', padx=2)
        ttk.Button(voice_btns_frame, text="Edit", command=self._edit_voice).pack(side='left', padx=2)
        ttk.Button(voice_btns_frame, text="Delete", command=self._delete_voice).pack(side='left', padx=2)
        ttk.Button(voice_btns_frame, text="Listen", command=self._play_sample).pack(side='right', padx=2)

        log_frame = ttk.LabelFrame(self, text="Execution Log", padding=10, height=150)
        log_frame.pack(fill='x', pady=5, padx=10, expand=False);
        log_frame.pack_propagate(False)
        self.log_widget = scrolledtext.ScrolledText(log_frame, state='normal', wrap=tk.WORD, height=1)
        self.log_widget.pack(fill='both', expand=True)
        self.log_widget.tag_config("info", foreground="black");
        self.log_widget.tag_config("success", foreground="green");
        self.log_widget.tag_config("warn", foreground="orange");
        self.log_widget.tag_config("error", foreground="red");
        self.log_widget.tag_config("fatal", foreground="red", font=('TkDefaultFont', 10, 'bold'));
        self.log_widget.configure(state='disabled')

        bottom_frame = ttk.Frame(self);
        bottom_frame.pack(fill='x', pady=5, padx=10)
        api_frame = ttk.Frame(bottom_frame);
        api_frame.pack(side='left')
        ttk.Label(api_frame, text="API:").pack(side='left')
        self.api_key_label = ttk.Label(api_frame, text="");
        self.api_key_label.pack(side='left', padx=5)
        self.edit_api_btn = ttk.Button(api_frame, text="Change", command=self._toggle_api_edit);
        self.edit_api_btn.pack(side='left')
        self.api_key_entry = ttk.Entry(api_frame, width=40)
        self.save_api_btn = ttk.Button(api_frame, text="Save", command=self._save_new_api_key)

    def _build_project_tab(self, parent):
        parent.columnconfigure(0, weight=1);
        parent.rowconfigure(2, weight=1)
        input_frame = ttk.LabelFrame(parent, text="Parameters", padding=5)
        input_frame.grid(row=0, column=0, sticky='ew', pady=(0, 5));
        input_frame.columnconfigure(1, weight=1)
        ttk.Label(input_frame, text="Video ID (comma separated):").grid(row=0, column=0, pady=2, sticky='w')
        self.vid_entry = ttk.Entry(input_frame);
        self.vid_entry.grid(row=0, column=1, pady=2, sticky='ew')
        ttk.Label(input_frame, text="Common folder name (optional):").grid(row=1, column=0, pady=2,
                                                                                      sticky='w')
        self.proj_name_entry = ttk.Entry(input_frame);
        self.proj_name_entry.grid(row=1, column=1, pady=2, sticky='ew')
        add_remove_frame = ttk.Frame(input_frame)
        add_remove_frame.grid(row=2, column=0, columnspan=2, pady=5)
        self.add_to_queue_btn = ttk.Button(add_remove_frame, text="Add to Queue", command=self._add_to_queue);
        self.add_to_queue_btn.pack(side='left', padx=5)
        self.remove_from_queue_btn = ttk.Button(add_remove_frame, text="Remove Selected",
                                                command=self._remove_from_queue);
        self.remove_from_queue_btn.pack(side='left', padx=5)
        queue_list_frame = ttk.LabelFrame(parent, text="Projects in Queue", padding=5)
        queue_list_frame.grid(row=2, column=0, sticky='nsew', pady=5)
        queue_list_frame.rowconfigure(0, weight=1);
        queue_list_frame.columnconfigure(0, weight=1)
        self.queue_listbox = tk.Listbox(queue_list_frame, selectmode='extended');
        self.queue_listbox.grid(row=0, column=0, sticky='nsew')
        ysb = ttk.Scrollbar(queue_list_frame, orient='vertical', command=self.queue_listbox.yview);
        ysb.grid(row=0, column=1, sticky='ns')
        self.queue_listbox.configure(yscrollcommand=ysb.set)
        run_frame = ttk.Frame(parent);
        run_frame.grid(row=3, column=0, sticky='ew', pady=5)
        self.start_queue_btn = ttk.Button(run_frame, text="Start Queue", command=self._start_queue_processing);
        self.start_queue_btn.pack(side='left', padx=5)
        self.stop_queue_btn = ttk.Button(run_frame, text="Stop", command=self._stop_queue_processing,
                                         state='disabled');
        self.stop_queue_btn.pack(side='left', padx=5)

    def _build_single_voicing_tab(self, parent):
        parent.columnconfigure(0, weight=1);
        parent.rowconfigure(1, weight=1)
        settings_frame = ttk.LabelFrame(parent, text="Single Voicing Parameters", padding=5)
        settings_frame.grid(row=0, column=0, sticky='ew');
        settings_frame.columnconfigure(1, weight=1)
        ttk.Label(settings_frame, text="File Name/Prefix:").grid(row=0, column=0, padx=5, pady=2, sticky='w')
        self.single_name_entry = ttk.Entry(settings_frame);
        self.single_name_entry.grid(row=0, column=1, padx=5, pady=2, sticky='ew')
        ttk.Label(settings_frame, text="Save Folder:").grid(row=1, column=0, padx=5, pady=2, sticky='w')
        self.single_path_label = ttk.Label(settings_frame, text=str(self.single_voicing_output_path), relief='sunken',
                                           anchor='w')
        self.single_path_label.grid(row=1, column=1, padx=5, pady=2, sticky='ew')
        ttk.Button(settings_frame, text="...", width=3, command=self._select_single_output_path).grid(row=1, column=2,
                                                                                                      padx=2)
        text_frame = ttk.LabelFrame(parent, text="Text for Voicing", padding=5)
        text_frame.grid(row=1, column=0, pady=5, sticky='nsew');
        text_frame.rowconfigure(0, weight=1);
        text_frame.columnconfigure(0, weight=1)
        self.single_text_widget = scrolledtext.ScrolledText(text_frame, wrap=tk.WORD, height=10);
        self.single_text_widget.grid(row=0, column=0, sticky='nsew')
        btn_frame = ttk.Frame(parent);
        btn_frame.grid(row=2, column=0, pady=5, sticky='ew')
        ttk.Button(btn_frame, text="Select .txt files...", command=self._select_txt_files).pack(side='left', padx=5)
        self.single_start_btn = ttk.Button(btn_frame, text="Process", command=self._start_single_voicing);
        self.single_start_btn.pack(side='left', padx=5)
        self.single_stop_btn = ttk.Button(btn_frame, text="Stop", command=self._stop_queue_processing,
                                          state='disabled');
        self.single_stop_btn.pack(side='left', padx=5)

    def _select_single_output_path(self):
        dir_path = filedialog.askdirectory(title="Select folder to save audio files",
                                           initialdir=self.single_voicing_output_path)
        if dir_path:
            self.single_voicing_output_path = Path(dir_path)
            self.single_path_label.config(text=str(self.single_voicing_output_path))

    def _select_txt_files(self):
        filepaths = filedialog.askopenfilenames(title="Select one or more .txt files",
                                                filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
        if filepaths:
            self.single_text_widget.configure(state='normal', foreground='black')
            self.single_text_widget.delete('1.0', tk.END)
            self.single_text_widget.insert('1.0', f"Selected {len(filepaths)} files for voicing:\n" + "\n".join(
                Path(p).name for p in filepaths))
            self.single_text_widget.configure(state='disabled', foreground='gray')
            self._temp_selected_files = filepaths

    def _start_single_voicing(self):
        if self.is_processing: messagebox.showwarning("Busy", "Wait for project queue processing to complete.",
                                                      parent=self); return
        selected_voice_item = self.voices_tree.focus()
        if not selected_voice_item: messagebox.showwarning("Error", "First select a voice.", parent=self); return
        voice_template = self.voice_templates[int(selected_voice_item)]
        output_path = self.single_voicing_output_path
        prefix = self.single_name_entry.get().strip()
        tasks_to_process = []
        source_is_files = hasattr(self, '_temp_selected_files') and self._temp_selected_files
        if source_is_files:
            try:
                for filepath in self._temp_selected_files:
                    p = Path(filepath)
                    with open(p, 'r', encoding='utf-8') as f: text = f.read()
                    file_name = f"{prefix}_{p.stem}" if prefix else p.stem
                    tasks_to_process.append({"filename": file_name, "text": text})
            except Exception as e:
                messagebox.showerror("File Read Error", f"Failed to read file: {e}");
                return
            finally:
                if hasattr(self, '_temp_selected_files'): del self._temp_selected_files
                self.single_text_widget.config(state='normal', foreground='black');
                self.single_text_widget.delete('1.0', tk.END)
        else:
            text_content = self.single_text_widget.get("1.0", tk.END).strip()
            if not text_content: messagebox.showwarning("Error", "Enter text or select .txt files.",
                                                        parent=self); return
            if not prefix: messagebox.showwarning("Error", "Enter 'Output file name'.", parent=self); return
            tasks_to_process.append({"filename": prefix, "text": text_content})
        if not tasks_to_process: return
        self.is_processing = True
        self.stop_queue_event.clear()
        self._update_ui_for_processing(True)
        self.single_voicing_thread = threading.Thread(target=self._single_voicing_processor,
                                                      args=(tasks_to_process, voice_template, output_path), daemon=True)
        self.single_voicing_thread.start()

    def _single_voicing_processor(self, tasks, voice_template, output_path):
        self.log_message(f"--- Starting single voicing for {len(tasks)} tasks ---", "INFO")
        overall_success = True
        for i, task in enumerate(tasks):
            if self.stop_queue_event.is_set():
                self.log_message("Stopping single voicing.", "WARN")
                overall_success = False;
                break
            filename = task['filename']
            text = task['text']
            self.log_message(f"Processing ({i + 1}/{len(tasks)}): '{filename}'...", "INFO")
            result = voice_generator._process_and_voice_block(
                self.api_key,
                {'index': filename, 'text': text},
                voice_template['template_uuid'],
                output_path,
                self.log_message,
                self.stop_queue_event
            )
            status, _ = result
            if status == 'success':
                self.root.after(0, self._play_alert, self.SOUND_FRAGMENT_COMPLETE)
            else:
                overall_success = False
                self.log_message(f"Failed to process '{filename}'.", "ERROR")
        if overall_success:
            self.log_message("Single voicing completed successfully.", "SUCCESS")
            self.root.after(0, self._play_alert, self.SOUND_SUCCESS)
        elif not self.stop_queue_event.is_set():
            self.log_message("Single voicing completed with errors.", "ERROR")
            self.root.after(0, self._play_alert, self.SOUND_ERROR)
        self.root.after(0, self._on_queue_finished)

    def _add_to_queue(self):
        video_ids_str = self.vid_entry.get().strip()
        if not video_ids_str:
            messagebox.showwarning("Error", "Video ID field cannot be empty.", parent=self)
            return
        # CHANGED: Removed .upper() to preserve original case entered by user
        video_ids = [vid.strip() for vid in video_ids_str.split(',') if vid.strip()]
        if not video_ids:
            return
        selected_voice_item = self.voices_tree.focus()
        if not selected_voice_item:
            messagebox.showwarning("Error", "Select voice for projects.", parent=self)
            return
        proj_name_template = self.proj_name_entry.get().strip()
        voice_template = self.voice_templates[int(selected_voice_item)]

        self.add_to_queue_btn.config(state='disabled')
        try:
            for video_id in video_ids:
                status, found_folders = self._find_project_folders(video_id)

                if status == "DUPLICATE":
                    folder_names = "\n - ".join([f.name for f in found_folders])
                    messagebox.showerror("Error: Duplicates found",
                                         f"Multiple folders found for project {video_id}:\n - {folder_names}\n\nResolve duplicates and try again.")
                    self.log_message(f"Adding {video_id} canceled: duplicates found.", "FATAL")
                    return

                if status == "OK":
                    existing_folder_name = found_folders[0].name
                    should_proceed = messagebox.askyesno(
                        "Folder already exists",
                        f"A folder already exists for ID '{video_id}':\n'{existing_folder_name}'\n\nUse existing folder?"
                    )
                    if not should_proceed:
                        self.log_message(f"Adding project {video_id} canceled by user.", "WARN")
                        return

                task = {"video_id": video_id, "voice_template": voice_template,
                        "project_name_template": proj_name_template}
                self.project_queue.append(task)
                self.log_message(f"Project {video_id} added to queue.", "INFO")

            self._update_queue_listbox()
            self.vid_entry.delete(0, tk.END)
        finally:
            self.add_to_queue_btn.config(state='normal')

    def _remove_from_queue(self):
        selected_indices = self.queue_listbox.curselection()
        if not selected_indices: return
        for i in sorted(selected_indices, reverse=True):
            del self.project_queue[i]
        self._update_queue_listbox()
        self.log_message(f"Removed {len(selected_indices)} projects from queue.", "WARN")

    def _start_queue_processing(self):
        if self.is_processing: return
        if not self.project_queue:
            messagebox.showwarning("Attention", "Queue is empty.", parent=self)
            return
        if not self._validate_queue():
            return
        self.is_processing = True
        self.stop_queue_event.clear()
        self._update_ui_for_processing(True)
        self.queue_processor_thread = threading.Thread(target=self._queue_processor_thread_func, daemon=True)
        self.queue_processor_thread.start()

    def _validate_queue(self) -> bool:
        self.log_message("--- Starting preliminary Excel database check ---", "INFO")
        errors = []
        try:
            db_sheets = pd.ExcelFile(self.DB_PATH).sheet_names
            # CHANGED: Create list of sheets in lowercase for case-insensitive search
            db_sheets_lower = [s.lower() for s in db_sheets]
        except Exception as e:
            messagebox.showerror("Database Error", f"Could not read Excel file: {e}")
            return False
        for task in self.project_queue:
            video_id = task["video_id"]
            # CHANGED: Check sheet existence case-insensitively
            if video_id.lower() not in db_sheets_lower:
                errors.append(f"Project {video_id}: Sheet with this ID (case-insensitive) not found in database.")
        if errors:
            error_message = "CHECK FAILED:\n" + "\n".join(f"- {e}" for e in errors)
            self.log_message(error_message, "FATAL");
            messagebox.showerror("Queue errors", "Problems found in projects. Details in log.")
            return False
        self.log_message("Excel database check passed successfully.", "SUCCESS");
        return True

    def _queue_processor_thread_func(self):
        all_tasks = list(self.project_queue)
        self.project_queue.clear()

        def on_fragment_complete():
            self.root.after(0, self._play_alert, self.SOUND_FRAGMENT_COMPLETE)

        def on_project_complete():
            self.root.after(0, self._play_alert, self.SOUND_SUCCESS)

        self.log_message(f"Starting batch processing for {len(all_tasks)} projects...", "INFO")
        for task in all_tasks:
            if self.stop_queue_event.is_set(): break
            self.root.after(0, self._update_queue_listbox_and_highlight, task['video_id'])
            project_path = self._find_or_create_project_for_task(task)
            if not project_path:
                self.log_message(
                    f"Critical error: could not find or create folder for project {task['video_id']}. Skipping.",
                    "FATAL")
                continue
            texts = self._get_texts_from_excel(task['video_id'])
            if not texts:
                self.log_message(f"Could not get texts for project {task['video_id']}. Skipping.", "WARN")
                continue
            status = voice_generator.run_synthesis(
                self.api_key, texts, task['voice_template']['template_uuid'],
                project_path / 'voice', self.log_message,
                on_fragment_complete,
                self.stop_queue_event,
                on_project_complete
            )
            if status != 'success':
                self.log_message(f"Project '{task['video_id']}' failed.", "ERROR")
                self.root.after(0, self._play_alert, self.SOUND_ERROR)
        self.root.after(0, self._on_queue_finished)

    def _on_queue_finished(self):
        if self.stop_queue_event.is_set():
            self.log_message("Processing stopped by user.", "WARN")
        else:
            self.log_message("=== ALL PROCESSING COMPLETED ===", "SUCCESS")
        self.is_processing = False
        self._update_ui_for_processing(False)
        self.project_queue.clear()
        self._update_queue_listbox()

    def _stop_queue_processing(self):
        if not self.is_processing: return
        if messagebox.askyesno("Confirmation",
                               "Stop processing?\n\nCurrent task will be finished, then the process will stop."):
            self.stop_queue_event.set()
            self.log_message("Stop signal sent. Finishing current task...", "WARN")

    def _update_ui_for_processing(self, processing: bool):
        state = 'disabled' if processing else 'normal'
        self.add_to_queue_btn.config(state=state)
        self.start_queue_btn.config(state=state)
        self.stop_queue_btn.config(state='normal' if processing else 'disabled')
        self.single_start_btn.config(state=state)
        self.single_stop_btn.config(state='normal' if processing else 'disabled')

    def _update_queue_listbox(self):
        self.queue_listbox.delete(0, tk.END)
        for task in self.project_queue:
            self.queue_listbox.insert(tk.END, task['video_id'])

    def _update_queue_listbox_and_highlight(self, video_id_to_highlight):
        self.queue_listbox.delete(0, tk.END)
        all_ids = [video_id_to_highlight] + [t['video_id'] for t in self.project_queue]
        for i, vid in enumerate(all_ids):
            self.queue_listbox.insert(tk.END, vid)
            if i == 0: self.queue_listbox.itemconfig(0, {'bg': 'lightblue'})

    def _find_project_folders(self, video_id_to_find: str) -> tuple[str, list[Path]]:
        if not video_id_to_find:
            return "ERROR", []
        found_folders = []
        search_id_lower = video_id_to_find.lower()

        for item in self.PROJECTS_ROOT_PATH.glob(f"{video_id_to_find}*"):
            if not item.is_dir():
                continue
            match = re.match(r'^(vid\d+(v\d+)?)', item.name, re.IGNORECASE)
            if match:
                extracted_id = match.group(1).lower()
                if extracted_id == search_id_lower:
                    found_folders.append(item)

        if len(found_folders) == 0:
            return "NOT_FOUND", []
        if len(found_folders) == 1:
            return "OK", found_folders
        else:
            return "DUPLICATE", found_folders

    def _find_or_create_project_for_task(self, task: dict) -> Path | None:
        video_id = task['video_id']
        proj_name_suffix = task.get('project_name_template', '')
        status, found_folders = self._find_project_folders(video_id)
        
        project_path = None
        if status == "OK":
            project_path = found_folders[0]
        elif status == "DUPLICATE":
            self.log_message(f"Critical error: Duplicates for '{video_id}' were not resolved. Skipping.", "FATAL")
            return None
        else:
            if proj_name_suffix:
                new_project_name = f"{video_id}_{proj_name_suffix}"
            else:
                new_project_name = video_id
            project_path = self.PROJECTS_ROOT_PATH / new_project_name
            if project_path.exists():
                self.log_message(f"Folder '{new_project_name}' already exists. Using it.", "WARN")
        
        try:
            (project_path / 'voice').mkdir(parents=True, exist_ok=True)
            (project_path / 'preview').mkdir(exist_ok=True)
            return project_path
        except Exception as e:
            self.log_message(f"Failed to prepare folders for {project_path.name}: {e}", "ERROR")
            return None

    def _get_texts_from_excel(self, video_id: str) -> list[dict] | None:
        try:
            xls = pd.ExcelFile(self.DB_PATH)
            actual_sheet_name = None
            for sheet in xls.sheet_names:
                if sheet.lower() == video_id.lower():
                    actual_sheet_name = sheet
                    break

            if not actual_sheet_name:
                self.log_message(f"Excel error: Sheet for project '{video_id}' not found in database.", "FATAL")
                return None

            df = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
            df = df.fillna('')  # Replace NaN with empty strings for convenience

            col_id_idx, col_text_idx = 0, 1
            if col_id_idx >= len(df.columns) or col_text_idx >= len(df.columns):
                self.log_message(f"Insufficient columns in sheet {video_id}.", "FATAL")
                return None

            # 1. Search for starting point B01-01
            start_row = -1
            for i in range(len(df)):
                val = str(df.iloc[i, col_id_idx]).strip()
                if val == "B01-01":
                    start_row = i
                    break

            if start_row == -1:
                self.log_message(f"Integrity error: Starting block 'B01-01' not found in sheet {video_id}.", "FATAL")
                return None

            # 2. Collect blocks and check sequence
            extracted_data = []
            
            def parse_block_id(bid):
                # BXX-YY format
                m = re.match(r'B(\d+)-(\d+)', bid)
                if m:
                    return int(m.group(1)), int(m.group(2))
                return None

            current_row = start_row
            last_major, last_minor = 0, 0

            while current_row < len(df):
                bid = str(df.iloc[current_row, col_id_idx]).strip()
                if not bid:
                    break
                
                parsed = parse_block_id(bid)
                if not parsed:
                    self.log_message(f"Format error: Invalid block ID '{bid}' at line {current_row + 1}.", "FATAL")
                    return None
                
                major, minor = parsed
                
                # Check sequence
                if last_major != 0:
                    valid = False
                    # Either the same major number and minor +1
                    if major == last_major and minor == last_minor + 1:
                        valid = True
                    # Or major number +1 and minor 01
                    elif major == last_major + 1 and minor == 1:
                        valid = True
                    
                    if not valid:
                        self.log_message(f"Sequence error: {major:02d}-{minor:02d} follows {last_major:02d}-{last_minor:02d} (line {current_row + 1}).", "FATAL")
                        return None

                text = str(df.iloc[current_row, col_text_idx]).strip()
                extracted_data.append({"index": bid, "text": text})
                
                last_major, last_minor = major, minor
                current_row += 1

            if not extracted_data:
                return []

            # 3. Check for garbage after final (within 5 lines)
            check_idx = current_row
            while check_idx < len(df) and check_idx < current_row + 5:
                val = str(df.iloc[check_idx, col_id_idx]).strip()
                if val != "":
                    self.log_message(f"Integrity error: Unexpected text '{val}' found at line {check_idx + 1} after the last block in sheet {video_id}. Empty space expected.", "FATAL")
                    return None
                check_idx += 1
            
            # If we got here, it means either it's empty or the file ended — that's valid.
            return extracted_data

        except Exception as e:
            self.log_message(f"Excel error for project {video_id}: {e}", "ERROR");
            return None

    def _play_alert(self, sound_path: Path):
        if not sound_path.exists(): return
        try:
            audio = AudioSegment.from_file(sound_path)
            threading.Thread(target=play, args=(audio,), daemon=True).start()
        except Exception as e:
            self.log_message(f"Failed to play sound: {e}", "ERROR")

    def _ensure_config_exists(self):
        if not self.VOICE_ASSETS_PATH:
            return
        self.VOICE_ASSETS_PATH.mkdir(parents=True, exist_ok=True)
        if not self.CONFIG_PATH.exists():
            with open(self.CONFIG_PATH, 'w', encoding='utf-8') as f: json.dump({"api_key": "", "templates": []}, f,
                                                                               indent=4)

    def _load_config(self):
        if not self.CONFIG_PATH:
            self.api_key, self.voice_templates = "", []
            return
        try:
            with open(self.CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
            self.api_key, self.voice_templates = config.get("api_key", ""), config.get("templates", [])
        except (FileNotFoundError, json.JSONDecodeError) as e:
            messagebox.showerror("Configuration error", f"Failed to load {self.CONFIG_PATH.name}: {e}")

    def _save_config(self):
        try:
            with open(self.CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump({"api_key": self.api_key, "templates": self.voice_templates}, f, ensure_ascii=False, indent=4)
        except Exception as e:
            messagebox.showerror("Save error", f"Failed to save {self.CONFIG_PATH.name}: {e}")

    def _populate_voices_table(self):
        for item in self.voices_tree.get_children(): self.voices_tree.delete(item)
        if not self.voice_templates: return
        self.voices_tree.column("Voice Name", width=120, stretch=False)
        self.voices_tree.column("Template UUID", width=120, stretch=False)
        self.voices_tree.column("Remark", width=150)
        for i, t in enumerate(self.voice_templates): self.voices_tree.insert("", "end", iid=i,
                                                                             values=(t.get("actor_name", ""),
                                                                                     t.get("template_uuid", ""),
                                                                                     t.get("remark", "")))

    def _add_voice(self):
        dialog = VoiceManagerDialog(self, "Add new voice")
        if dialog.result:
            new_voice_data = dialog.result
            if dialog.new_sample_path:
                new_voice_data['sample_file'] = self._process_and_copy_sample(dialog.new_sample_path, new_voice_data)
            else:
                new_voice_data['sample_file'] = ''
            self.voice_templates.append(new_voice_data);
            self._save_config();
            self._populate_voices_table()

    def _edit_voice(self):
        selected_item = self.voices_tree.focus();
        if not selected_item: return
        item_index = int(selected_item)
        voice_data = self.voice_templates[item_index]
        dialog = VoiceManagerDialog(self, "Edit voice", voice_data)
        if dialog.result:
            updated_voice_data = dialog.result
            if dialog.new_sample_path:
                updated_voice_data['sample_file'] = self._process_and_copy_sample(dialog.new_sample_path,
                                                                                  updated_voice_data)
            else:
                updated_voice_data['sample_file'] = voice_data.get('sample_file', '')
            self.voice_templates[item_index] = updated_voice_data;
            self._save_config();
            self._populate_voices_table()

    def _delete_voice(self):
        selected_item = self.voices_tree.focus();
        if not selected_item: return
        item_index = int(selected_item)
        if messagebox.askyesno("Confirmation", f"Delete '{self.voice_templates[item_index].get('actor_name')}'?"):
            self.voice_templates.pop(item_index);
            self._save_config();
            self._populate_voices_table()

    def _play_sample(self):
        selected_item = self.voices_tree.focus();
        if not selected_item: return
        sample_file = self.voice_templates[int(selected_item)].get('sample_file')
        if not sample_file: self.log_message("Sample not attached.", "WARN"); return
        sample_path = self.VOICE_ASSETS_PATH / sample_file
        if sample_path.exists() and sample_path.is_file():
            self._play_alert(sample_path)
        else:
            self.log_message(f"Sample file not found: {sample_path}", "ERROR")

    def _process_and_copy_sample(self, source_path: Path, voice_data: dict) -> str:
        try:
            name_part = re.sub(r'[\W_]+', '_', voice_data['actor_name'])
            uuid_part = voice_data['template_uuid']
            dest_filename = f"{name_part}_{uuid_part}{source_path.suffix}"
            dest_path = self.VOICE_ASSETS_PATH / dest_filename
            shutil.copy(source_path, dest_path)
            return dest_filename
        except Exception as e:
            self.log_message(f"Error copying sample: {e}", "ERROR");
            return ""

    def _toggle_api_edit(self):
        if self.save_api_btn.winfo_ismapped():
            self.api_key_entry.pack_forget();
            self.save_api_btn.pack_forget()
        else:
            self.api_key_entry.pack(side='left', in_=self.edit_api_btn.master);
            self.api_key_entry.delete(0, tk.END);
            self.api_key_entry.insert(0, self.api_key);
            self.save_api_btn.pack(side='left', in_=self.edit_api_btn.master)

    def _save_new_api_key(self):
        new_key = self.api_key_entry.get().strip()
        if new_key: self.api_key = new_key; self._save_config(); self._update_api_key_label(); self._toggle_api_edit()

    def _update_api_key_label(self):
        self.api_key_label.config(text=f"...{self.api_key[-4:]}" if self.api_key else "not set")