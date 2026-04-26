# --- import_interface.py ---

import tkinter as tk
from tkinter import ttk
from pathlib import Path
import threading
import re
import traceback
import importlib


class ImportTab(ttk.Frame):
    def __init__(self, parent_notebook, controller):
        super().__init__(parent_notebook, padding="10")
        self.controller = controller

        # Create notebook for sub-tabs
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True)

        # Create two sub-tabs
        self.capture_tab = CaptureTab(self.notebook, controller)
        self.create_tab = CreateTab(self.notebook, controller)

        self.notebook.add(self.capture_tab, text="Capture")
        self.notebook.add(self.create_tab, text="Create")


class CaptureTab(ttk.Frame):
    """First subtab - data capture from YouTube"""
    def __init__(self, parent_notebook, controller):
        super().__init__(parent_notebook, padding="10")
        self.controller = controller
        self.project_id_var = tk.StringVar()
        self._create_widgets()

    def _create_widgets(self):
        top_frame = ttk.Frame(self);
        top_frame.pack(fill='x', expand=False, pady=(0, 10));
        top_frame.columnconfigure(0, weight=1)
        url_frame = ttk.LabelFrame(top_frame, text="Donor Video URLs (each on a new line)", padding="10");
        url_frame.grid(row=0, column=0, sticky='ew');
        url_frame.columnconfigure(0, weight=1)
        self.url_text = tk.Text(url_frame, height=10, wrap=tk.WORD, undo=True);
        self.url_text.grid(row=0, column=0, sticky='ew')
        scrollbar = ttk.Scrollbar(url_frame, orient='vertical', command=self.url_text.yview);
        scrollbar.grid(row=0, column=1, sticky='ns');
        self.url_text.config(yscrollcommand=scrollbar.set)

        settings_frame = ttk.Frame(top_frame, padding=(0, 10))
        settings_frame.grid(row=1, column=0, sticky='w', pady=(10, 0))
        ttk.Label(settings_frame, text="Project ID (4 digits):").pack(side='left', padx=(0, 5))
        project_id_entry = ttk.Entry(settings_frame, textvariable=self.project_id_var, width=10)
        project_id_entry.pack(side='left', padx=(0, 10))
        ttk.Label(settings_frame, text="(otherwise automatic)", foreground="gray").pack(side='left', padx=(0, 20))

        # Add radio buttons for subtitle language selection
        self.lang_var = tk.StringVar(value="en")
        ttk.Label(settings_frame, text="Language:").pack(side='left', padx=(0, 5))
        ttk.Radiobutton(settings_frame, text="EN", variable=self.lang_var, value="en").pack(side='left', padx=(0, 5))
        ttk.Radiobutton(settings_frame, text="RU", variable=self.lang_var, value="ru").pack(side='left', padx=(0, 5))

        button_frame = ttk.Frame(self)
        button_frame.pack(pady=5, expand=False, anchor='w', padx=10)

        self.extract_button = ttk.Button(button_frame, text="Extract Data", command=self._start_extraction_task);
        self.extract_button.pack(side='left')

        self.status_label = ttk.Label(self, text="", foreground="blue")
        self.status_label.pack(pady=(0, 5), expand=False, anchor='w', padx=10)

        log_frame = ttk.LabelFrame(self, text="Operations Log", padding="10");
        log_frame.pack(fill='both', expand=True, pady=(5, 0));
        log_frame.rowconfigure(0, weight=1);
        log_frame.columnconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, state='normal', wrap=tk.WORD, bg="#f0f0f0", borderwidth=0);
        self.log_text.pack(side='left', fill='both', expand=True)
        self.log_text.bind("<KeyPress>", self._handle_log_key_press)
        self.log_text.bind("<Control-c>", lambda e: None)
        self.log_text.bind("<Control-a>", lambda e: None)

        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview);
        log_scrollbar.pack(side='right', fill='y');
        self.log_text.config(yscrollcommand=log_scrollbar.set)

    def _start_extraction_task(self):
        urls = self._parse_urls()
        if not urls: self._log_message("Error: URL field is empty or contains no links.", is_error=True); return
        pid_input = self.project_id_var.get().strip()
        if pid_input and (not pid_input.isdigit() or len(pid_input) != 4):
            self._log_message("Error: Project ID must consist of 4 digits.", is_error=True);
            return
        work_root_str = self.controller.get_setting('work_root_path')
        if not work_root_str:
            self._log_message("Error: Project folder path is not specified in Settings.", is_error=True);
            return
        target_dir = Path(work_root_str) / "parsed_data"
        lang_input = self.lang_var.get()

        self._log_message("-" * 20)
        self._log_message(f"Starting task. Unique URLs found for processing: {len(urls)}. Language: {lang_input.upper()}")

        self._update_status_label("")
        self.extract_button.config(state='disabled')
        self.url_text.config(state='disabled')

        thread = threading.Thread(target=self._run_worker_in_thread, args=(urls, target_dir, pid_input or None, lang_input),
                                  daemon=True)
        thread.start()

    def _run_worker_in_thread(self, urls, target_dir, pid_input, lang_input):
        try:
            worker_module = importlib.import_module('import_extractor_worker_v2')
            process_import_task = worker_module.process_import_task
            process_import_task(url_list=urls, target_dir=target_dir, user_pid=pid_input, lang=lang_input,
                                log_callback=self._log_message,
                                status_callback=self._update_status_label)
        except (ImportError, AttributeError):
            self._log_message(f"CRITICAL ERROR: Failed to import worker v2.\n{traceback.format_exc()}",
                              is_error=True)
        except Exception:
            self._log_message(f"CRITICAL ERROR IN WORKER v2 THREAD:\n{traceback.format_exc()}", is_error=True)
        finally:
            self.after(0, lambda: self.extract_button.config(state='normal'))
            self.after(0, lambda: self.url_text.config(state='normal'))
            self.after(1500, lambda: self._update_status_label(""))

    def _log_message(self, message, is_error=False):
        self.after(0, self._update_log_widget, message, is_error)

    def _update_status_label(self, message: str):
        self.after(0, lambda: self.status_label.config(text=message))

    def _update_log_widget(self, message, is_error):
        tag = "error" if is_error else "info"
        self.log_text.tag_config("error", foreground="red");
        self.log_text.tag_config("info", foreground="black")
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.tag_add(tag, f"end-{len(message) + 2}c", "end-1c")
        self.log_text.see(tk.END)

    def _handle_log_key_press(self, event):
        if event.keysym in ('c', 'v', 'x', 'a') and (event.state & 0x4):
            return None
        if event.keysym in ('Delete', 'BackSpace', 'Return', 'Tab', 'space'):
            return "break"
        if event.keysym in ('Left', 'Right', 'Up', 'Down', 'Home', 'End', 'Prior', 'Next'):
            return None
        if event.state & 0x1:
            return None
        return None

    def _parse_urls(self) -> list[str]:
        raw_text = self.url_text.get('1.0', tk.END)
        processed_text = raw_text.replace("https://", " https://").replace("http://", " http://")
        potential_urls = re.findall(r'https?://\S+', processed_text)
        valid_urls = []
        for url in potential_urls:
            if 'youtube.com' in url or 'youtu.be' in url:
                clean_url_match = re.match(r'(https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[\w-]+)', url)
                if clean_url_match:
                    valid_urls.append(clean_url_match.group(1))
        return list(dict.fromkeys(valid_urls))


class CreateTab(ttk.Frame):
    """Second subtab - creating files from own text"""
    def __init__(self, parent_notebook, controller):
        super().__init__(parent_notebook, padding="10")
        self.controller = controller
        self.project_id_var = tk.StringVar()
        self._create_widgets()

    def _create_widgets(self):
        top_frame = ttk.Frame(self);
        top_frame.pack(fill='x', expand=False, pady=(0, 10));
        top_frame.columnconfigure(0, weight=1)

        # Text field for entering text material
        text_frame = ttk.LabelFrame(top_frame, text="Text Material", padding="10");
        text_frame.grid(row=0, column=0, sticky='ew');
        text_frame.columnconfigure(0, weight=1)
        self.text_input = tk.Text(text_frame, height=15, wrap=tk.WORD, undo=True);
        self.text_input.grid(row=0, column=0, sticky='ew')
        scrollbar = ttk.Scrollbar(text_frame, orient='vertical', command=self.text_input.yview);
        scrollbar.grid(row=0, column=1, sticky='ns');
        self.text_input.config(yscrollcommand=scrollbar.set)

        settings_frame = ttk.Frame(top_frame, padding=(0, 10));
        settings_frame.grid(row=1, column=0, sticky='w', pady=(10, 0))
        ttk.Label(settings_frame, text="Project ID (4 digits):").pack(side='left', padx=(0, 5))
        project_id_entry = ttk.Entry(settings_frame, textvariable=self.project_id_var, width=10);
        project_id_entry.pack(side='left', padx=(0, 10))
        ttk.Label(settings_frame, text="(otherwise automatic)", foreground="gray").pack(side='left')

        button_frame = ttk.Frame(self)
        button_frame.pack(pady=5, expand=False, anchor='w', padx=10)

        self.create_button = ttk.Button(button_frame, text="Create Files", command=self._start_create_task);
        self.create_button.pack(side='left')

        self.status_label = ttk.Label(self, text="", foreground="blue")
        self.status_label.pack(pady=(0, 5), expand=False, anchor='w', padx=10)

        log_frame = ttk.LabelFrame(self, text="Operation Log", padding="10");
        log_frame.pack(fill='both', expand=True, pady=(5, 0));
        log_frame.rowconfigure(0, weight=1);
        log_frame.columnconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, state='normal', wrap=tk.WORD, bg="#f0f0f0", borderwidth=0);
        self.log_text.pack(side='left', fill='both', expand=True)
        self.log_text.bind("<KeyPress>", self._handle_log_key_press)
        self.log_text.bind("<Control-c>", lambda e: None)
        self.log_text.bind("<Control-a>", lambda e: None)

        log_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview);
        log_scrollbar.pack(side='right', fill='y');
        self.log_text.config(yscrollcommand=log_scrollbar.set)

    def _start_create_task(self):
        text_content = self.text_input.get('1.0', tk.END).strip()
        if not text_content or len(text_content) < 50:
            self._log_message("Error: Text material field is empty or too short (minimum 50 characters).", is_error=True)
            return

        pid_input = self.project_id_var.get().strip()
        if pid_input and (not pid_input.isdigit() or len(pid_input) != 4):
            self._log_message("Error: Project ID must consist of 4 digits.", is_error=True)
            return

        work_root_str = self.controller.get_setting('work_root_path')
        if not work_root_str:
            self._log_message("Error: Project folder path is not specified in Settings.", is_error=True)
            return

        target_dir = Path(work_root_str) / "parsed_data"
        self._log_message("-" * 20)
        self._log_message(f"Starting file creation. Text contains {len(text_content)} characters.")

        self._update_status_label("")
        self.create_button.config(state='disabled')
        self.text_input.config(state='disabled')

        thread = threading.Thread(target=self._run_worker_in_thread, args=(text_content, target_dir, pid_input or None),
                                  daemon=True)
        thread.start()

    def _run_worker_in_thread(self, text_content, target_dir, pid_input):
        try:
            worker_module = importlib.import_module('import_text_creator_worker')
            process_text_creation = worker_module.process_text_creation
            process_text_creation(text_content=text_content, target_dir=target_dir, user_pid=pid_input,
                                  log_callback=self._log_message,
                                  status_callback=self._update_status_label)
        except (ImportError, AttributeError):
            self._log_message(f"CRITICAL ERROR: Failed to import text creation worker.\n{traceback.format_exc()}",
                              is_error=True)
        except Exception:
            self._log_message(f"CRITICAL ERROR IN WORKER THREAD:\n{traceback.format_exc()}", is_error=True)
        finally:
            self.after(0, lambda: self.create_button.config(state='normal'))
            self.after(0, lambda: self.text_input.config(state='normal'))
            self.after(1500, lambda: self._update_status_label(""))

    def _log_message(self, message, is_error=False):
        self.after(0, self._update_log_widget, message, is_error)

    def _update_status_label(self, message: str):
        self.after(0, lambda: self.status_label.config(text=message))

    def _update_log_widget(self, message, is_error):
        tag = "error" if is_error else "info"
        self.log_text.tag_config("error", foreground="red")
        self.log_text.tag_config("info", foreground="black")
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.tag_add(tag, f"end-{len(message) + 2}c", "end-1c")
        self.log_text.see(tk.END)

    def _handle_log_key_press(self, event):
        if event.keysym in ('c', 'v', 'x', 'a') and (event.state & 0x4):
            return None
        if event.keysym in ('Delete', 'BackSpace', 'Return', 'Tab', 'space'):
            return "break"
        if event.keysym in ('Left', 'Right', 'Up', 'Down', 'Home', 'End', 'Prior', 'Next'):
            return None
        if event.state & 0x1:
            return None
        return None
