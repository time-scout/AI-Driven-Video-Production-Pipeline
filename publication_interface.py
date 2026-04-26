# publication_interface.py (v5.3 - YouTube Error Resilience)

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import webbrowser
import threading
from datetime import datetime
from pathlib import Path
import requests
import json
import tomli
import shutil
import re
from collections import defaultdict
import traceback

import pandas as pd
from PIL import Image, ImageTk

import youtube_title_parser
import publication_ai_rewriter

# --- VERSION CONSTANTS ---
MAX_VERSIONS = 6


class PublicationTab(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.root = controller.root
        if self.controller.DATABASE_PATH:
            self.DB_PATH = self.controller.DATABASE_PATH / 'main_database.xlsx'
            self.PROJECTS_ROOT_PATH = self.controller.DATABASE_PATH.parent
        else:
            self.DB_PATH = None
            self.PROJECTS_ROOT_PATH = None
        self.SECRETS_PATH = self.controller.SECRETS_PATH
        self.video_scan_path = Path(self.controller.get_setting('publication_output_path')) if self.controller.get_setting('publication_output_path') else None
        self.video_data = {}
        self.base_project_id = None
        self.preview_photo_image_small = None
        self.preview_photo_image_large = None
        self.gemini_api_key = self.controller.get_setting('GOOGLE_API_KEY')
        self.version_widgets = {f'v{i}': {} for i in range(1, MAX_VERSIONS + 1)}
        self.version_final_previews = {}
        s = ttk.Style()
        self.theme_bg_color = s.lookup('TFrame', 'background')
        self._build_ui()


    def _build_ui(self):
        main_container = ttk.Frame(self, padding="10")
        main_container.pack(fill="both", expand=True)
        self._build_top_panel(main_container)
        self.notebook = ttk.Notebook(main_container)
        self.notebook.pack(fill='both', expand=True, pady=(10, 0))
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        title_tab = ttk.Frame(self.notebook, padding=10)
        desc_tab = ttk.Frame(self.notebook, padding=10)
        preview_tab = ttk.Frame(self.notebook, padding=10)
        video_tab = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(title_tab, text='  Title  ')
        self.notebook.add(desc_tab, text='  Description  ')
        self.notebook.add(preview_tab, text='  Preview  ')
        self.notebook.add(video_tab, text='  Video  ')
        self._build_title_tab(title_tab)
        self._build_description_tab(desc_tab)
        self._build_preview_tab(preview_tab)
        self._build_video_tab(video_tab)

    def _build_top_panel(self, parent):
        top_frame_container = ttk.Frame(parent);
        top_frame_container.pack(fill='x')
        header_frame = ttk.Frame(top_frame_container);
        header_frame.pack(fill='x', pady=(0, 2))
        ttk.Label(header_frame, text="Video Donor:").pack(side='left')
        self.url_label_header = tk.Label(header_frame, text="(not loaded)", fg="grey", cursor="hand2",
                                         bg=self.theme_bg_color, borderwidth=0)
        self.url_label_header.pack(side='left', padx=5)
        main_row_frame = ttk.Frame(top_frame_container);
        main_row_frame.pack(fill='x')
        ttk.Label(main_row_frame, text="Video ID:").pack(side='left')
        self.project_id_entry = ttk.Entry(main_row_frame, width=8);
        self.project_id_entry.pack(side='left', padx=(2, 5))
        self.project_id_entry.bind('<Return>', self._start_fetching_data)
        self.load_button = ttk.Button(main_row_frame, text="Load", command=self._start_fetching_data)
        self.load_button.pack(side='left', padx=(0, 10))
        ttk.Separator(main_row_frame, orient='vertical').pack(side='left', fill='y', padx=10, pady=0)
        ttk.Label(main_row_frame, text="Title:").pack(side='left')
        self.source_title_entry = ttk.Entry(main_row_frame, state='readonly');
        self.source_title_entry.pack(side='left', fill='x', expand=True, padx=(2, 10))
        ttk.Label(main_row_frame, text="Channel:").pack(side='left')
        self.channel_entry = ttk.Entry(main_row_frame, state='readonly', width=20);
        self.channel_entry.pack(side='left', padx=(2, 10))
        ttk.Label(main_row_frame, text="Stats:").pack(side='left')
        self.stats_entry = ttk.Entry(main_row_frame, state='readonly', width=18);
        self.stats_entry.pack(side='left', padx=2)

    def _create_preview_label(self, parent):
        return tk.Label(parent, bg=self.theme_bg_color, borderwidth=0)

    def _build_title_tab(self, parent):
        left_panel = ttk.Frame(parent);
        left_panel.pack(side='left', fill='y', padx=(0, 15), anchor='n')
        self.version_widgets['v1']['title_preview_label'] = self._create_preview_label(left_panel);
        self.version_widgets['v1']['title_preview_label'].pack()
        right_panel = ttk.Frame(parent);
        right_panel.pack(side='right', fill='both', expand=True)
        gen_frame = ttk.LabelFrame(right_panel, text="Variant Generation", padding=10);
        gen_frame.pack(fill='both', expand=True)
        self.rewrite_title_button = ttk.Button(gen_frame, text="Generate Titles",
                                               command=lambda: self._start_rewrite_task('title'));
        self.rewrite_title_button.pack(anchor='w', pady=(0, 5))
        ttk.Label(gen_frame, text="New title variants:").pack(anchor='w')
        self.title_options_text = scrolledtext.ScrolledText(gen_frame, height=10, wrap='word');
        self.title_options_text.pack(fill='both', expand=True, pady=2)
        approve_frame = ttk.LabelFrame(right_panel, text="Approve Titles for Versions", padding=10);
        approve_frame.pack(fill='x', pady=(10, 0))
        for i in range(1, MAX_VERSIONS + 1):
            version_id = f'v{i}';
            row = ttk.Frame(approve_frame);
            row.pack(fill='x', pady=2)
            label = ttk.Label(row, text=f"Version {i}:", width=15);
            label.pack(side='left');
            self.version_widgets[version_id]['title_label'] = label
            entry = ttk.Entry(row);
            entry.pack(side='left', fill='x', expand=True, padx=5);
            self.version_widgets[version_id]['title_entry'] = entry
            btn = ttk.Button(row, text="Approve", command=lambda v=version_id: self._approve_content('title', v));
            btn.pack(side='right');
            self.version_widgets[version_id]['title_approve_btn'] = btn

    def _build_description_tab(self, parent):
        parent.columnconfigure(2, weight=1)
        top_row = ttk.Frame(parent);
        top_row.grid(row=0, column=0, columnspan=3, sticky='ew', pady=(0, 10));
        top_row.columnconfigure(2, weight=1)
        self.rewrite_desc_button = ttk.Button(top_row, text="Rewrite Description",
                                              command=lambda: self._start_rewrite_task('description'));
        self.rewrite_desc_button.grid(row=0, column=0, sticky='nw', padx=(0, 20), pady=(5, 0))
        self.version_widgets['v1']['desc_preview_label'] = self._create_preview_label(top_row);
        self.version_widgets['v1']['desc_preview_label'].grid(row=0, column=1, sticky='nw', padx=(0, 20))
        desc_rewrite_frame = ttk.Frame(top_row);
        desc_rewrite_frame.grid(row=0, column=2, sticky='nsew')
        ttk.Label(desc_rewrite_frame, text="New description (rewrite result):").pack(anchor='w')
        self.new_desc_text = scrolledtext.ScrolledText(desc_rewrite_frame, height=10, wrap='word');
        self.new_desc_text.pack(fill='both', expand=True)
        approve_frame = ttk.LabelFrame(parent, text="Approve Descriptions for Versions", padding=10);
        approve_frame.grid(row=1, column=0, columnspan=3, sticky='ew', pady=(10, 0))
        for i in range(1, MAX_VERSIONS + 1):
            version_id = f'v{i}';
            row = ttk.Frame(approve_frame);
            row.pack(fill='x', pady=3)
            label = ttk.Label(row, text=f"Version {i}:", width=15);
            label.pack(side='left', anchor='n');
            self.version_widgets[version_id]['desc_label'] = label
            text_widget = scrolledtext.ScrolledText(row, height=4, wrap='word');
            text_widget.pack(side='left', fill='x', expand=True, padx=5);
            self.version_widgets[version_id]['desc_text'] = text_widget
            btn = ttk.Button(row, text="Approve",
                             command=lambda v=version_id: self._approve_content('description', v));
            btn.pack(side='right', anchor='n');
            self.version_widgets[version_id]['desc_approve_btn'] = btn

    def _build_preview_tab(self, parent):
        parent.rowconfigure(0, weight=1);
        parent.columnconfigure(0, weight=1)
        left_column = ttk.Frame(parent);
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 10));
        left_column.rowconfigure(2, weight=1)
        self.preview_image_label_large = self._create_preview_label(left_column);
        self.preview_image_label_large.grid(row=0, column=0, pady=(0, 10), sticky='nw')
        text_gen_frame = ttk.LabelFrame(left_column, text="Preview Text Generation", padding=10);
        text_gen_frame.grid(row=1, column=0, sticky='new', pady=(10, 0))
        ttk.Label(text_gen_frame, text="Text on source preview (enter manually):").pack(anchor='w')
        self.source_preview_text_entry = ttk.Entry(text_gen_frame);
        self.source_preview_text_entry.pack(fill='x', pady=(2, 5))
        self.rewrite_preview_text_button = ttk.Button(text_gen_frame, text="Rewrite Preview Text",
                                                      command=lambda: self._start_rewrite_task('preview_text'));
        self.rewrite_preview_text_button.pack(anchor='w')
        ttk.Label(text_gen_frame, text="Preview text variants:").pack(anchor='w', pady=(10, 0))
        self.preview_options_text = scrolledtext.ScrolledText(text_gen_frame, height=8, wrap='word');
        self.preview_options_text.pack(fill='both', expand=True, pady=2)
        approve_text_frame = ttk.LabelFrame(left_column, text="Approve Text for Versions", padding=10);
        approve_text_frame.grid(row=2, column=0, sticky='new', pady=(10, 0))
        for i in range(1, MAX_VERSIONS + 1):
            version_id = f'v{i}';
            row = ttk.Frame(approve_text_frame);
            row.pack(fill='x', pady=2)
            label = ttk.Label(row, text=f"Version {i}:", width=15);
            label.pack(side='left');
            self.version_widgets[version_id]['preview_text_label'] = label
            entry = ttk.Entry(row);
            entry.pack(side='left', fill='x', expand=True, padx=5);
            self.version_widgets[version_id]['preview_text_entry'] = entry
            btn = ttk.Button(row, text="Approve",
                             command=lambda v=version_id: self._approve_content('preview_text', v));
            btn.pack(side='right');
            self.version_widgets[version_id]['preview_text_approve_btn'] = btn
        right_column = ttk.Frame(parent);
        right_column.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        work_files_frame = ttk.Frame(right_column);
        work_files_frame.pack(fill='x', pady=(0, 20), anchor='n')
        ttk.Button(work_files_frame, text="Specify common working files", command=self._select_working_files).pack(
            anchor='w')
        final_preview_container = ttk.Frame(right_column);
        final_preview_container.pack(fill='both', expand=True)
        final_preview_container.columnconfigure(0, weight=1);
        final_preview_container.columnconfigure(1, weight=1)
        preview_col1 = ttk.Frame(final_preview_container);
        preview_col1.grid(row=0, column=0, sticky='new', padx=(0, 10))
        preview_col2 = ttk.Frame(final_preview_container);
        preview_col2.grid(row=0, column=1, sticky='new', padx=(10, 0))
        columns = [preview_col1, preview_col2]
        for i in range(1, MAX_VERSIONS + 1):
            col_index = 0 if i <= 3 else 1;
            parent_col = columns[col_index];
            version_id = f'v{i}'
            card = ttk.LabelFrame(parent_col, text=f"Version {i}", padding=10);
            card.pack(fill='x', expand=True, pady=(0, 10));
            card.columnconfigure(1, weight=1)
            left_sub_frame = ttk.Frame(card);
            left_sub_frame.grid(row=0, column=0, sticky='nw', padx=(0, 10))
            btn = ttk.Button(left_sub_frame, text=f"Specify preview",
                             command=lambda v=version_id: self._select_final_preview(v));
            btn.pack(anchor='w')
            path_label = ttk.Label(left_sub_frame, text="Path not specified", foreground="grey", wraplength=120,
                                   justify='left');
            path_label.pack(anchor='w', pady=(5, 0));
            self.version_widgets[version_id]['final_preview_path'] = path_label
            w, h = 240, 135;
            preview_placeholder = ttk.Frame(card, width=w, height=h);
            preview_placeholder.grid(row=0, column=1, sticky='e');
            preview_placeholder.pack_propagate(False)
            img_label = self._create_preview_label(preview_placeholder);
            img_label.pack(fill='both', expand=True);
            self.version_widgets[version_id]['final_preview_img'] = img_label

    def _build_video_tab(self, parent):
        container = ttk.Frame(parent);
        container.pack(fill='both', expand=True);
        container.rowconfigure(1, weight=1);
        container.columnconfigure(0, weight=1)
        top_frame = ttk.Frame(container);
        top_frame.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        ttk.Button(top_frame, text="Select video files manually...", command=self._mass_select_videos).pack(side='left')
        ttk.Button(top_frame, text="Remove selected from list", command=self._mass_remove_selected).pack(side='left',
                                                                                                           padx=10)
        self.video_scan_status_label = ttk.Label(top_frame, text="");
        self.video_scan_status_label.pack(side='left', padx=10)
        tree_frame = ttk.Frame(container);
        tree_frame.grid(row=1, column=0, sticky='nsew')
        columns = ('status', 'filename', 'project_id');
        self.mass_video_tree = ttk.Treeview(tree_frame, columns=columns, show='headings', selectmode='extended')
        self.mass_video_tree.pack(side='left', fill='both', expand=True)
        scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.mass_video_tree.yview);
        scrollbar.pack(side='right', fill='y')
        self.mass_video_tree.configure(yscrollcommand=scrollbar.set)
        self.mass_video_tree.heading('status', text='Status');
        self.mass_video_tree.column('status', width=200, anchor='w')
        self.mass_video_tree.heading('filename', text='File Name');
        self.mass_video_tree.column('filename', width=400)
        self.mass_video_tree.heading('project_id', text='Project ID (editable)');
        self.mass_video_tree.column('project_id', width=200)
        self.mass_video_tree.tag_configure('ok', foreground='green');
        self.mass_video_tree.tag_configure('attached', foreground='grey')
        self.mass_video_tree.tag_configure('occupied', foreground='orange');
        self.mass_video_tree.tag_configure('error', foreground='red')
        self.mass_video_tree.tag_configure('duplicate_parent', foreground='red', font=('TkDefaultFont', 11, 'bold'))
        self.mass_video_tree.bind('<Double-1>', self._mass_edit_cell)
        bottom_frame = ttk.Frame(container);
        bottom_frame.grid(row=2, column=0, sticky='ew', pady=(10, 0))
        self.mass_save_button = ttk.Button(bottom_frame, text="Save for selected",
                                           command=self._mass_save_bindings);
        self.mass_save_button.pack(side='right')

    # =================================================================
    # --- DATA LOGIC ---
    # =================================================================

    def _find_project_folder(self, project_id: str) -> (str, list[Path]):
        if not project_id:
            return 'ERROR', []

        found_folders = []
        # Convert searched ID to lowercase for case-insensitive comparison
        search_id_lower = project_id.lower()

        for item in self.PROJECTS_ROOT_PATH.iterdir():
            if not item.is_dir():
                continue

            item_name_lower = item.name.lower()

            # Extract "key identifier" from folder name.
            # Template searches for VIDXXX or VIDXXXvX at the start of the name.
            match = re.match(r'^(vid\d{3}(v\d)?)', item_name_lower)

            if match:
                # Extracted identifier from folder name (e.g., 'vid100', 'vid100v2')
                extracted_id = match.group(1)

                # Compare extracted identifier with the one we are looking for
                if extracted_id == search_id_lower:
                    found_folders.append(item)

        if len(found_folders) == 1:
            return 'OK', found_folders
        elif len(found_folders) == 0:
            return 'NOT_FOUND', []
        else:
            # If more than one folder found with the same key identifier, those are actual duplicates.
            return 'DUPLICATE', found_folders

    def _get_base_id(self, raw_id: str) -> str:
        match = re.match(r"^(VID\d+)", raw_id, re.IGNORECASE)
        return match.group(1).upper() if match else raw_id.upper()

    def _get_project_id_from_filename(self, filename: str) -> str:
        match = re.match(r"(VID\d+(v\d+)?)", filename, re.IGNORECASE)
        return match.group(1).upper() if match else ""

    def _get_project_code_for_version(self, version_id_str: str) -> str:
        if not self.base_project_id: return ""
        version_num = int(version_id_str.replace('v', ''))
        return f"{self.base_project_id}v{version_num}"

    def _get_or_create_project_folder(self, project_id: str) -> (Path | None):
        status, folder_paths = self._find_project_folder(project_id)
        if status == 'OK':
            return folder_paths[0]
        elif status == 'DUPLICATE':
            path_list_str = "\n - ".join([p.name for p in folder_paths])
            messagebox.showerror("Error: Duplicate folder",
                                 f"Multiple folders found for ID '{project_id}':\n - {path_list_str}\n\nResolve duplicates.")
            return None
        elif status == 'NOT_FOUND':
            try:
                new_folder_path = self.PROJECTS_ROOT_PATH / project_id
                new_folder_path.mkdir(exist_ok=True)
                (new_folder_path / "previews").mkdir(exist_ok=True)
                (new_folder_path / "voice").mkdir(exist_ok=True)
                return new_folder_path
            except Exception as e:
                messagebox.showerror("Folder creation error", f"Could not create folder for '{project_id}':\n{e}")
                return None
        return None

    def _clear_fields(self):
        def clear_entry(entry):
            if entry: entry.config(state='normal'); entry.delete(0, tk.END); entry.config(state='readonly')

        def clear_scrolled_text(widget):
            if widget: widget.config(state='normal'); widget.delete('1.0', tk.END)

        clear_entry(self.source_title_entry);
        clear_entry(self.channel_entry);
        clear_entry(self.stats_entry)
        self.url_label_header.config(text="(not loaded)", fg="grey");
        self.url_label_header.unbind("<Button-1>")
        clear_scrolled_text(self.title_options_text);
        clear_scrolled_text(self.new_desc_text)
        self.source_preview_text_entry.delete(0, tk.END);
        clear_scrolled_text(self.preview_options_text)
        if hasattr(self, 'mass_video_tree'): self.mass_video_tree.delete(*self.mass_video_tree.get_children())
        self.preview_photo_image_small = self.preview_photo_image_large = None
        for label in [self.version_widgets['v1'].get('title_preview_label'),
                      self.version_widgets['v1'].get('desc_preview_label'), self.preview_image_label_large]:
            if label: label.config(image='')
        for i in range(1, MAX_VERSIONS + 1): self._clear_ui_for_version(f'v{i}')
        self.version_final_previews.clear()

    def _start_fetching_data(self, event=None):
        raw_project_id = self.project_id_entry.get().strip()
        if not raw_project_id:
            messagebox.showwarning("Attention", "Project 'Video ID' field is not filled.")
            return
        self.base_project_id = self._get_base_id(raw_project_id)
        self.load_button.config(state='disabled')
        self.url_label_header.config(text="Loading...", fg="orange")
        self._clear_fields()
        for i in range(1, MAX_VERSIONS + 1):
            code = self._get_project_code_for_version(f'v{i}') or ''
            for key in ['title_label', 'desc_label', 'preview_text_label']:
                if key in self.version_widgets[f'v{i}']:
                    self.version_widgets[f'v{i}'][key].config(text=f"Version {i} ({code}):")
        thread = threading.Thread(target=self._fetch_data_thread, args=(self.base_project_id,), daemon=True)
        thread.start()

    def _fetch_data_thread(self, base_id):
        try:
            if not self.DB_PATH.exists():
                self.root.after(0, self._update_ui_with_error, f"Database file not found:\n{self.DB_PATH}")
                return

            xls = pd.ExcelFile(self.DB_PATH)

            # --- Reliable, case-insensitive search ---
            sheet_map = {name.lower(): name for name in xls.sheet_names}
            actual_sheet_name = sheet_map.get(base_id.lower())

            if not actual_sheet_name:
                self.root.after(0, self._update_ui_with_error, f"Sheet '{base_id}' not found in database.")
                return

            df = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
            if len(df) < 2 or len(df.columns) < 5:
                self.root.after(0, self._update_ui_with_error, f"Sheet '{actual_sheet_name}' structure is incorrect.")
                return

            video_url = df.iloc[1, 4]
            if pd.isna(video_url) or not str(video_url).strip().startswith('http'):
                self.root.after(0, self._update_ui_with_error,
                                f"URL not found in cell E2 on sheet '{actual_sheet_name}'.")
                return

        except Exception as e:
            self.root.after(0, self._update_ui_with_error, f"Error reading Excel: {e}");
            return

        # Get metadata. Function returns {'success': False, ...} on error
        metadata = youtube_title_parser.fetch_video_metadata(video_url)
        metadata['source_url'] = video_url  # Add URL in any case for display

        # Attempt to download thumbnail only if metadata was successfully retrieved
        if metadata.get('success'):
            image_data = None
            urls_to_try = [metadata.get('thumbnail_maxres_url'), metadata.get('thumbnail_hq_url')]
            for url in urls_to_try:
                if url:
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        image_data = response.content
                        metadata['image_data'] = image_data
                        break
                    except requests.RequestException:
                        continue

        # Pass data to the main thread to update UI
        self.root.after(0, self._update_ui_with_data_and_versions, metadata)

    # =========================================================================
    # --- MODIFIED FUNCTION ---
    # =========================================================================
    def _update_ui_with_data_and_versions(self, data: dict):
        """
        Updates UI. Now this function is not interrupted if YouTube data
        is unavailable, but instead displays local data.
        """
        self.load_button.config(state='normal')
        self.video_data = data

        def set_entry_text(entry, text):
            entry.config(state='normal');
            entry.delete(0, tk.END);
            entry.insert(0, text or "");
            entry.config(state='readonly')

        youtube_data_is_valid = data.get('success', False)
        url = data.get('source_url', '')

        # --- SCENARIO 1: YouTube data successfully retrieved ---
        if youtube_data_is_valid:
            set_entry_text(self.source_title_entry, data.get('title'))
            set_entry_text(self.channel_entry, data.get('channel'))
            set_entry_text(self.stats_entry, self._format_statistics(data))

            self.new_desc_text.config(state='normal');
            self.new_desc_text.delete('1.0', tk.END);
            self.new_desc_text.insert('1.0', data.get('description', ''))

            self.url_label_header.config(text=url, fg="blue")
            self.url_label_header.unbind("<Button-1>")
            self.url_label_header.bind("<Button-1>", lambda e, u=url: webbrowser.open(u))

            if data.get('image_data'):
                try:
                    main_project_folder = self._get_or_create_project_folder(self._get_project_code_for_version('v1'))
                    if main_project_folder:
                        image_save_path = main_project_folder / "previews" / f"Original_Preview_{self.base_project_id}.jpg"
                        with open(image_save_path, 'wb') as f: f.write(data['image_data'])

                        img = Image.open(image_save_path)
                        img_small = img.resize((160, 90), Image.Resampling.LANCZOS)
                        self.preview_photo_image_small = ImageTk.PhotoImage(img_small)
                        self.version_widgets['v1']['title_preview_label'].config(image=self.preview_photo_image_small)
                        self.version_widgets['v1']['desc_preview_label'].config(image=self.preview_photo_image_small)

                        img_large = img.resize((480, 270), Image.Resampling.LANCZOS)
                        self.preview_photo_image_large = ImageTk.PhotoImage(img_large)
                        self.preview_image_label_large.config(image=self.preview_photo_image_large)
                except Exception as e:
                    messagebox.showerror("Preview error", f"Failed to save or display preview: {e}")

        # --- SCENARIO 2: Error fetching data from YouTube ---
        else:
            error_message = data.get('error', 'Unknown error.')
            set_entry_text(self.source_title_entry, "⚠️ YouTube data unavailable")
            set_entry_text(self.channel_entry, "")
            set_entry_text(self.stats_entry, "")

            self.new_desc_text.config(state='normal');
            self.new_desc_text.delete('1.0', tk.END);

            self.url_label_header.config(text=f"Error: {error_message}", fg="red")
            self.url_label_header.unbind("<Button-1>")

        # --- THIS PART IS ALWAYS EXECUTED (both on success and error) ---
        self._load_all_versions_data()
        self._find_and_assign_previews_auto()

    def _load_all_versions_data(self):
        if not self.base_project_id: return
        for i in range(1, MAX_VERSIONS + 1):
            version_id_str = f'v{i}'
            project_id = self._get_project_code_for_version(version_id_str)
            status, folder_paths = self._find_project_folder(project_id)
            if status == 'OK' and folder_paths:
                project_folder = folder_paths[0]
                json_path = project_folder / "publication_content.json"
                if json_path.exists():
                    try:
                        with open(json_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        self._populate_ui_for_version(version_id_str, data)
                    except (json.JSONDecodeError, FileNotFoundError) as e:
                        print(f"JSON read error for {project_id}: {e}");
                        self._clear_ui_for_version(version_id_str)
                else:
                    self._clear_ui_for_version(version_id_str)
            else:
                self._clear_ui_for_version(version_id_str)

    def _clear_ui_for_version(self, version_id: str):
        widgets = self.version_widgets.get(version_id, {})
        if 'title_entry' in widgets: widgets['title_entry'].delete(0, tk.END)
        if 'desc_text' in widgets: widgets['desc_text'].delete('1.0', tk.END)
        if 'preview_text_entry' in widgets: widgets['preview_text_entry'].delete(0, tk.END)
        if 'final_preview_img' in widgets: widgets['final_preview_img'].config(image='')
        if 'final_preview_path' in widgets: widgets['final_preview_path'].config(text="Path not specified",
                                                                                 foreground="grey")

    def _populate_ui_for_version(self, version_id: str, data: dict):
        widgets = self.version_widgets.get(version_id, {})
        if 'title_entry' in widgets: widgets['title_entry'].delete(0, tk.END); widgets['title_entry'].insert(0,
                                                                                                             data.get(
                                                                                                                 'approved_title',
                                                                                                                 ''))
        if 'desc_text' in widgets: widgets['desc_text'].delete('1.0', tk.END); widgets['desc_text'].insert('1.0',
                                                                                                           data.get(
                                                                                                               'approved_description',
                                                                                                               ''))
        if 'preview_text_entry' in widgets: widgets['preview_text_entry'].delete(0, tk.END); widgets[
            'preview_text_entry'].insert(0, data.get('approved_preview_text', ''))
        path_str = data.get('final_preview_path', '')
        if path_str and Path(path_str).exists(): self._update_final_preview_display(version_id, Path(path_str))

    def _on_tab_changed(self, event):
        try:
            selected_tab_text = self.notebook.tab(self.notebook.select(), "text").strip()
            if selected_tab_text == 'Video': self._start_auto_scan_videos()
        except tk.TclError:
            pass

    def _start_auto_scan_videos(self, manual_files_list=None):
        self.video_scan_status_label.config(text="Scanning...", foreground="blue")
        thread = threading.Thread(target=self._auto_scan_videos_thread, args=(manual_files_list,), daemon=True);
        thread.start()

    def _auto_scan_videos_thread(self, manual_files_list=None):
        try:
            self.root.after(0, self.video_scan_status_label.config,
                            {"text": "Scanning video files...", "foreground": "blue"})
            if manual_files_list:
                video_files = manual_files_list
            else:
                if not self.video_scan_path.exists(): self.root.after(0, self._update_scan_error,
                                                                          "Scanning folder not found!"); return
                video_files = sorted([p for p in self.video_scan_path.glob('*.mp4')],
                                      key=lambda p: p.stat().st_mtime, reverse=True)
            self.root.after(0, self.video_scan_status_label.config,
                            {"text": "Finding existing bindings...", "foreground": "blue"})
            attached_video_paths = {}
            for project_folder in self.PROJECTS_ROOT_PATH.glob('VID*'):
                if project_folder.is_dir():
                    json_path = project_folder / "publication_content.json"
                    if json_path.exists():
                        try:
                            with open(json_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            video_path = data.get('final_video_path')
                            if video_path:
                                id_from_folder = self._get_project_id_from_filename(project_folder.name)
                                if id_from_folder: attached_video_paths[id_from_folder] = Path(video_path).resolve()
                        except (json.JSONDecodeError, FileNotFoundError):
                            continue
            self.root.after(0, self._populate_video_tab, video_files, attached_video_paths)
        except Exception:
            error_info = traceback.format_exc();
            self.root.after(0, self._update_scan_error, f"Error in scanning thread:\n{error_info}")

    def _update_scan_error(self, message):
        self.video_scan_status_label.config(text="Error!", foreground="red")
        messagebox.showerror("Scanning error", message)

    def _populate_video_tab(self, video_files, attached_video_paths):
        try:
            self.mass_video_tree.delete(*self.mass_video_tree.get_children())
            files_by_id = defaultdict(list)
            for path in video_files:
                project_id = self._get_project_id_from_filename(path.name)
                if project_id: files_by_id[project_id].append(path)
            for project_id, paths in files_by_id.items():
                if len(paths) > 1:
                    parent_id = f"DUPLICATE_CHOICE_{project_id}"
                    status_text = f"❌ Duplicate in selection ({len(paths)} files)";
                    self.mass_video_tree.insert('', 'end', iid=parent_id, values=(status_text, "", project_id),
                                                tags=('duplicate_parent',))
                    for path in paths: self.mass_video_tree.insert(parent_id, 'end', iid=str(path),
                                                                   values=("", path.name, project_id), tags=('error',))
                else:
                    path = paths[0]
                    status_code, folder_paths = self._find_project_folder(project_id)
                    status_text, tag = self._get_status_display(status_code, path, project_id, attached_video_paths)
                    if status_code == 'DUPLICATE':
                        parent_id = f"DUPLICATE_FOLDER_{project_id}"
                        status_text = f"❌ Duplicate folder for ID: {project_id}";
                        self.mass_video_tree.insert('', 'end', iid=parent_id, values=(status_text, "", project_id),
                                                    tags=('duplicate_parent',))
                        self.mass_video_tree.insert(parent_id, 'end', iid=str(path),
                                                    values=("(your file)", path.name, project_id), tags=('error',))
                        for folder in folder_paths: self.mass_video_tree.insert(parent_id, 'end', iid=str(folder),
                                                                                values=("(folder found)", folder.name,
                                                                                        ""), tags=('error',))
                    else:
                        self.mass_video_tree.insert('', 'end', iid=str(path),
                                                    values=(status_text, path.name, project_id), tags=(tag.lower(),))
            self.video_scan_status_label.config(text=f"Found {len(video_files)} video files.", foreground="black")
            self._check_save_possibility()
        except Exception:
            error_info = traceback.format_exc();
            self._update_scan_error(f"Error drawing table:\n{error_info}")

    def _get_status_display(self, status_code: str, video_path: Path, project_id: str, attached_paths: dict) -> (str,
                                                                                                                 str):
        resolved_path = video_path.resolve()
        for pid, path in attached_paths.items():
            if path == resolved_path: return "✅ Attached", "ATTACHED"
        if project_id in attached_paths: return f"⚠️ Occupied by another file", "OCCUPIED"
        if status_code == 'OK' or status_code == 'NOT_FOUND':
            return "🟡 OK (ready to bind)", "OK"
        else:
            return f"❌ Folder error", "ERROR"

    def _mass_select_videos(self):
        file_paths_str = filedialog.askopenfilenames(
            title="Select video files for mass binding",
            initialdir=str(self.video_scan_path) if self.video_scan_path.exists() else str(Path.home()),
            filetypes=[("Video files", "*.mp4")])
        if not file_paths_str: return
        file_paths = [Path(p) for p in file_paths_str]
        self._start_auto_scan_videos(manual_files_list=file_paths)

    def _mass_remove_selected(self):
        selected_items = self.mass_video_tree.selection()
        if not selected_items: return
        for item_id in selected_items:
            if self.mass_video_tree.exists(item_id): self.mass_video_tree.delete(item_id)
        self._check_save_possibility()

    def _check_save_possibility(self):
        can_save = True
        for item_id in self.mass_video_tree.get_children():
            tags = self.mass_video_tree.item(item_id, 'tags')
            if 'error' in tags or 'duplicate_parent' in tags: can_save = False; break
        self.mass_save_button.config(state='normal' if can_save else 'disabled')

    def _mass_edit_cell(self, event):
        item_id = self.mass_video_tree.identify_row(event.y);
        column = self.mass_video_tree.identify_column(event.x)
        if not item_id or column != '#3' or "DUPLICATE" in item_id: return
        x, y, width, height = self.mass_video_tree.bbox(item_id, column)
        value = self.mass_video_tree.item(item_id, 'values')[2]
        entry = ttk.Entry(self.mass_video_tree);
        entry.place(x=x, y=y, width=width, height=height)
        entry.insert(0, value);
        entry.focus_force()

        def on_focus_out(e):
            new_value = entry.get().upper();
            entry.destroy()
            if new_value and new_value != value:
                self._start_auto_scan_videos(
                    manual_files_list=[Path(iid) for iid in self.mass_video_tree.get_children() if
                                       "DUPLICATE" not in iid])

        entry.bind('<Return>', on_focus_out);
        entry.bind('<FocusOut>', on_focus_out);
        entry.bind('<Escape>', lambda e: entry.destroy())

    def _mass_save_bindings(self):
        selected_items = self.mass_video_tree.selection()
        if not selected_items: messagebox.showinfo("Information", "No files selected for saving."); return
        items_to_save = []
        for iid in selected_items:
            if "DUPLICATE" in iid: continue
            tags = self.mass_video_tree.item(iid, 'tags')
            if 'error' not in tags: items_to_save.append(iid)
        if not items_to_save: messagebox.showwarning("Attention",
                                                     "No files available for saving among selected.\nResolve errors (❌) or select files with status (🟡) or (✅)."); return
        total = len(items_to_save);
        success_count = 0
        for iid in items_to_save:
            values = self.mass_video_tree.item(iid, 'values')
            path_str = iid;
            project_id = values[2]
            if self._save_path_to_json(project_id, 'final_video_path', Path(path_str)):
                success_count += 1
                self.mass_video_tree.item(iid, values=("✅ Attached", values[1], values[2]), tags=('attached',))
            else:
                messagebox.showerror("Save error",
                                     f"Could not save binding for project '{project_id}'. Process stopped.");
                break
        if success_count > 0: messagebox.showinfo("Success",
                                                  f"Successfully saved/updated bindings: {success_count} of {total}.")
        self.mass_video_tree.selection_remove(self.mass_video_tree.selection())

    def _save_path_to_json(self, project_id: str, key: str, path: Path):
        project_folder = self._get_or_create_project_folder(project_id)
        if not project_folder: return False
        json_path = project_folder / "publication_content.json"
        content_data = {}
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    content_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                pass
        content_data[key] = str(path.resolve())
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(content_data, f, ensure_ascii=False, indent=4)
            return True
        except Exception as e:
            print(f"JSON save error for {project_id}: {e}");
            return False

    def _approve_content(self, content_type: str, version_id: str):
        project_id = self._get_project_code_for_version(version_id);
        project_folder = self._get_or_create_project_folder(project_id)
        if not project_folder: return
        json_path = project_folder / "publication_content.json";
        content_data = {};
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    content_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                pass
        value = '';
        widgets = self.version_widgets.get(version_id, {})
        key_map = {'title': ('approved_title', widgets.get('title_entry')),
                   'preview_text': ('approved_preview_text', widgets.get('preview_text_entry')),
                   'description': ('approved_description', widgets.get('desc_text'))}
        if content_type not in key_map or not key_map[content_type][1]: return
        json_key, widget = key_map[content_type]
        if isinstance(widget, tk.Entry):
            value = widget.get()
        elif isinstance(widget, scrolledtext.ScrolledText):
            value = widget.get('1.0', 'end-1c')
        content_data[json_key] = value.strip()
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(content_data, f, ensure_ascii=False, indent=4)
            messagebox.showinfo("Success", f"Data for '{project_folder.name}' saved in:\n{json_path.name}")
        except Exception as e:
            messagebox.showerror("Save error", f"Failed to save JSON file: {e}")

    def _find_and_assign_previews_auto(self):
        if not self.base_project_id: return
        for i in range(1, MAX_VERSIONS + 1):
            version_id_str = f'v{i}';
            project_id = self._get_project_code_for_version(version_id_str)
            status, folder_paths = self._find_project_folder(project_id)
            if status != 'OK': continue
            project_folder = folder_paths[0]
            if not (project_folder / "previews").exists(): continue
            search_str1 = project_id.lower();
            search_str2 = "id" + self.base_project_id[3:].lower()
            found_files = [p for ext in ["*.jpg", "*.jpeg", "*.png"] for p in (project_folder / "previews").glob(ext) if
                           search_str1 in p.name.lower() or search_str2 in p.name.lower()]
            if len(found_files) == 1:
                final_path = found_files[0]
                self._save_path_to_json(project_id, 'final_preview_path', final_path)
                self._update_final_preview_display(version_id_str, final_path)

    def _select_working_files(self):
        main_project_folder = self._get_or_create_project_folder(self._get_project_code_for_version('v1'))
        if not main_project_folder: return
        file_paths = filedialog.askopenfilenames(title="Select working files")
        if not file_paths: return
        destination_folder = main_project_folder / "previews"
        try:
            for file_path in file_paths: shutil.copy(file_path, destination_folder)
            messagebox.showinfo("Success",
                                f"Copied {len(file_paths)} files to 'previews' folder of the main project.")
        except Exception as e:
            messagebox.showerror("Copy error", f"Failed to copy files: {e}")

    def _select_final_preview(self, version_id: str):
        project_id = self._get_project_code_for_version(version_id)
        if not project_id: return
        file_path_str = filedialog.askopenfilename(title=f"Select preview for {project_id}",
                                                   filetypes=[("Image files", "*.jpg *.jpeg *.png")])
        if not file_path_str: return
        source_path = Path(file_path_str)
        project_folder = self._get_or_create_project_folder(project_id)
        if not project_folder: return
        destination_folder = project_folder / "previews"
        final_path = destination_folder / source_path.name
        try:
            if source_path.resolve() != final_path.resolve(): shutil.copy(source_path, destination_folder)
            if self._save_path_to_json(project_id, 'final_preview_path', final_path):
                self._update_final_preview_display(version_id, final_path)
                messagebox.showinfo("Success", f"Preview for '{project_folder.name}' set.")
            else:
                messagebox.showerror("Error", "Failed to save preview path to JSON.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to process preview file: {e}")

    def _update_final_preview_display(self, version_id: str, file_path: Path):
        widgets = self.version_widgets[version_id]
        try:
            relative_path = file_path.relative_to(self.PROJECTS_ROOT_PATH.parent)
        except ValueError:
            relative_path = file_path
        try:
            img = Image.open(file_path);
            w, h = 240, 135;
            img.thumbnail((w, h), Image.Resampling.LANCZOS)
            self.version_final_previews[version_id] = ImageTk.PhotoImage(img)
            widgets['final_preview_img'].config(image=self.version_final_previews[version_id])
            widgets['final_preview_path'].config(text=str(relative_path), foreground="black")
        except Exception as e:
            print(f"Error displaying preview for {version_id}: {e}");
            widgets['final_preview_img'].config(image='');
            widgets['final_preview_path'].config(text="Loading error", foreground="red")

    def _format_statistics(self, data: dict) -> str:
        view_count = data.get('view_count');
        upload_date_str = data.get('upload_date')
        if view_count is not None:
            if view_count >= 1_000_000:
                views_str = f"{view_count / 1_000_000:.1f}M views"
            elif view_count >= 1_000:
                views_str = f"{view_count / 1_000:.1f}k views"
            else:
                views_str = f"{view_count} views"
        else:
            views_str = "N/A views"
        age_str = ""
        if upload_date_str:
            try:
                upload_date = datetime.strptime(upload_date_str, '%Y-%m-%d')
                days_ago = (datetime.now() - upload_date).days
                if (years_ago := days_ago / 365.25) >= 3:
                    age_str = f"{int(years_ago)} years"
                elif days_ago > 180:
                    age_str = f"{int(days_ago / 30.44)} months"
                else:
                    age_str = f"{days_ago} days"
            except (ValueError, TypeError):
                age_str = ""
        return f"{views_str} / {age_str}" if age_str else views_str

    def _update_ui_with_error(self, error_message):
        messagebox.showerror("Error", error_message);
        self.load_button.config(state='normal');
        self.url_label_header.config(text="Error", fg="red")

    def _start_rewrite_task(self, task_type: str):
        if not self.gemini_api_key: messagebox.showerror("Error",
                                                         f"Failed to load GOOGLE_API_KEY from file:\n{self.SECRETS_PATH}\n\nCheck that the file exists and the key is specified in it."); return
        source_text = ""
        if task_type == 'title':
            source_text = self.source_title_entry.get();
            if "⚠️" in source_text:  # Check that we are not trying to rewrite an error message
                messagebox.showwarning("Attention",
                                       "Cannot generate titles because source title from YouTube is unavailable.")
                return
            self.rewrite_title_button.config(state='disabled')
        elif task_type == 'preview_text':
            source_text = self.source_preview_text_entry.get();
            self.rewrite_preview_text_button.config(state='disabled')
        elif task_type == 'description':
            if not self.video_data: messagebox.showwarning("Attention", "First load video data."); return
            source_text = self.new_desc_text.get('1.0', 'end-1c');
            if not source_text.strip():  # Check that description is not empty
                messagebox.showwarning("Attention",
                                       "Cannot rewrite because source description from YouTube is unavailable.")
                return
            self.rewrite_desc_button.config(state='disabled')
        if not source_text:
            messagebox.showwarning("Attention", "No source text for rewrite.")
            if task_type == 'title': self.rewrite_title_button.config(state='normal')
            if task_type == 'preview_text': self.rewrite_preview_text_button.config(state='normal')
            if task_type == 'description': self.rewrite_desc_button.config(state='normal')
            return
        thread = threading.Thread(target=self._rewrite_thread, args=(self.gemini_api_key, task_type, source_text),
                                  daemon=True);
        thread.start()

    def _rewrite_thread(self, api_key, task_type, source_text):
        result = None
        if task_type == 'title':
            result = publication_ai_rewriter.rewrite_title(api_key, source_text)
        elif task_type == 'preview_text':
            result = publication_ai_rewriter.rewrite_preview_text(api_key, source_text)
        elif task_type == 'description':
            result = publication_ai_rewriter.rewrite_description(api_key, source_text)
        self.root.after(0, self._update_ui_with_rewrite, task_type, result)

    def _update_ui_with_rewrite(self, task_type, result):
        if task_type == 'title': self.rewrite_title_button.config(state='normal')
        if task_type == 'preview_text': self.rewrite_preview_text_button.config(state='normal')
        if task_type == 'description': self.rewrite_desc_button.config(state='normal')
        is_error = False
        if isinstance(result, list) and result and isinstance(result[0], str) and result[0].startswith("API_ERROR:"):
            is_error, error_msg = True, result[0]
        elif isinstance(result, str) and result.startswith("API_ERROR:"):
            is_error, error_msg = True, result
        if is_error: messagebox.showerror("API Error", error_msg); return
        if task_type == 'title':
            self.title_options_text.config(state='normal')
            self.title_options_text.delete('1.0', tk.END);
            self.title_options_text.insert('1.0', "\n".join(result))
        elif task_type == 'preview_text':
            self.preview_options_text.config(state='normal')
            self.preview_options_text.delete('1.0', tk.END);
            self.preview_options_text.insert('1.0', "\n".join(result))
        elif task_type == 'description':
            self.new_desc_text.config(state='normal')
            self.new_desc_text.delete('1.0', tk.END);
            self.new_desc_text.insert('1.0', result)