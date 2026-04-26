# publication_planner_interface.py (v22.0 - Final Version)

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import sys
import re
from datetime import datetime, timedelta
import json
from pathlib import Path
import threading
import pandas as pd

import publication_planner_logic
import youtube_publications_scanner

try:
    from AppKit import NSWorkspace
    from Foundation import NSURL

    pyobjc_installed = True
except ImportError:
    pyobjc_installed = False


class ChannelSettingsWindow(tk.Toplevel):
    def __init__(self, parent, channel_manager):
        super().__init__(parent)
        self.channel_manager = channel_manager
        self.transient(parent)
        self.title("Channel Settings")
        self.geometry("1000x500")
        self.channel_entries_data = []
        self._create_widgets()
        self._populate_data()
        self._center_window()
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.grab_set()
        self.wait_window(self)

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding=10);
        main_frame.pack(fill='both', expand=True)
        header_frame = ttk.Frame(main_frame);
        header_frame.pack(fill='x', pady=(0, 5))
        ttk.Label(header_frame, text="#", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=0, padx=5, sticky='w')
        ttk.Label(header_frame, text="Channel Name", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=1,
                                                                                                 padx=5, sticky='w')
        ttk.Label(header_frame, text="Channel URL", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=2, padx=5,
                                                                                            sticky='w')
        ttk.Label(header_frame, text="Proxy", font=('TkDefaultFont', 10, 'bold')).grid(row=0, column=3, padx=5,
                                                                                        sticky='w')
        header_frame.grid_columnconfigure(1, weight=2);
        header_frame.grid_columnconfigure(2, weight=3);
        header_frame.grid_columnconfigure(3, weight=3)
        self.canvas = tk.Canvas(main_frame, borderwidth=0, highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)
        self.scrollable_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True);
        scrollbar.pack(side="right", fill="y")
        button_frame = ttk.Frame(self, padding=(10, 5, 10, 10));
        button_frame.pack(fill='x', side='bottom')
        ttk.Button(button_frame, text="Cancel", command=self.destroy).pack(side='right', padx=5)
        ttk.Button(button_frame, text="Save", command=self._on_save).pack(side='right')

    def _populate_data(self):
        for widget in self.scrollable_frame.winfo_children(): widget.destroy()
        self.channel_entries_data = []
        channels = self.channel_manager.get_channels_data()
        for ch in channels: self.channel_entries_data.append(
            {'id': ch['id'], 'name_var': tk.StringVar(value=ch['name']), 'url_var': tk.StringVar(value=ch['url']),
             'proxy_var': tk.StringVar(value=ch['proxy']), 'channel_id': ch.get('channel_id', '')})
        self.scrollable_frame.grid_columnconfigure(1, weight=2);
        self.scrollable_frame.grid_columnconfigure(2, weight=3);
        self.scrollable_frame.grid_columnconfigure(3, weight=2);
        self.scrollable_frame.grid_columnconfigure(4, weight=1)
        for i, entry_data in enumerate(self.channel_entries_data):
            id_label = ttk.Label(self.scrollable_frame, text=str(entry_data['id']), width=4);
            id_label.grid(row=i, column=0, padx=5, pady=5, sticky='w')
            name_entry = ttk.Entry(self.scrollable_frame, textvariable=entry_data['name_var']);
            name_entry.grid(row=i, column=1, padx=5, pady=5, sticky='ew')
            url_entry = ttk.Entry(self.scrollable_frame, textvariable=entry_data['url_var']);
            url_entry.grid(row=i, column=2, padx=5, pady=5, sticky='ew')
            proxy_entry = ttk.Entry(self.scrollable_frame, textvariable=entry_data['proxy_var']);
            proxy_entry.grid(row=i, column=3, padx=5, pady=5, sticky='ew')
            proxy_check_frame = ttk.Frame(self.scrollable_frame);
            proxy_check_frame.grid(row=i, column=4, padx=5, pady=5, sticky='ew')
            status_label = ttk.Label(proxy_check_frame, text="", width=12);
            status_label.pack(side='left', padx=5)
            check_button = ttk.Button(proxy_check_frame, text="Check", command=lambda p=entry_data['proxy_var'],
                                                                                          s=status_label: self._check_proxy_thread(
                p, s));
            check_button.pack(side='left')
            buttons_frame = ttk.Frame(self.scrollable_frame);
            buttons_frame.grid(row=i, column=5, padx=5, pady=5)
            up_button = ttk.Button(buttons_frame, text="↑", width=3, command=lambda idx=i: self._move_channel(idx, -1));
            up_button.pack(side='left')
            down_button = ttk.Button(buttons_frame, text="↓", width=3,
                                     command=lambda idx=i: self._move_channel(idx, 1));
            down_button.pack(side='left')
            if i == 0: up_button.config(state='disabled')
            if i == len(self.channel_entries_data) - 1: down_button.config(state='disabled')
            self._check_proxy_thread(entry_data['proxy_var'], status_label, silent=True)

    def _move_channel(self, index, direction):
        if not (0 <= index + direction < len(self.channel_entries_data)): return
        self.channel_entries_data.insert(index + direction, self.channel_entries_data.pop(index))
        for i, entry in enumerate(self.channel_entries_data): entry['id'] = i + 1
        self._populate_data()

    def _on_save(self):
        new_data = []
        for entry in self.channel_entries_data:
            url = entry['url_var'].get()
            channel_id = entry['channel_id']
            if url:
                match = re.search(r'(UC[\w-]{22})', url)
                if match: channel_id = match.group(1)
            new_data.append(
                {"id": entry['id'], "name": entry['name_var'].get(), "url": url, "proxy": entry['proxy_var'].get(),
                 "channel_id": channel_id})
        self.channel_manager.update_channels_data(new_data)
        self.destroy()

    def _center_window(self):
        self.update_idletasks()
        parent_x = self.master.winfo_rootx();
        parent_y = self.master.winfo_rooty()
        parent_width = self.master.winfo_width();
        parent_height = self.master.winfo_height()
        win_width = self.winfo_width();
        win_height = self.winfo_height()
        x = parent_x + (parent_width - win_width) // 2
        y = parent_y + (parent_height - win_height) // 2
        self.geometry(f'+{x}+{y}')

    def _check_proxy_thread(self, proxy_var, status_label, silent=False):
        proxy_str = proxy_var.get()
        if not proxy_str: status_label.config(text="No proxy", foreground='orange'); return
        if not silent: status_label.config(text="Checking...", foreground='blue')
        threading.Thread(target=self._run_proxy_check, args=(proxy_str, status_label), daemon=True).start()

    def _run_proxy_check(self, proxy_str, status_label):
        success, _ = youtube_publications_scanner.check_proxy(proxy_str)
        self.after(0, lambda: status_label.config(text='OK' if success else 'Error',
                                                  foreground='green' if success else 'red'))


class PublicationPlannerTab(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller;
        self.root = controller.root

        try:
            self.DATABASE_PATH = self.controller.DATABASE_PATH
            self.PROJECTS_ROOT_PATH = self.DATABASE_PATH.parent / 'CELEBRITIES_CENTRALIZED'
            self.channels_config_path = self.DATABASE_PATH / 'youtube_channels_config.json'
            db_excel_path = self.DATABASE_PATH / 'main_database.xlsx'
            self.channel_manager = publication_planner_logic.ChannelConfigManager(self.channels_config_path)
            self.scanned_data_manager = publication_planner_logic.ScannedDataManager(db_excel_path)
            self.history_visualizer = publication_planner_logic.PublicationHistory(self.scanned_data_manager,
                                                                                   self.channel_manager)
            self.planner = publication_planner_logic.PublicationPlanner(self.history_visualizer)
            self.scanned_data_cache = pd.DataFrame()
        except Exception as e:
            messagebox.showerror("Critical initialization error", f"Could not initialize logic modules:\n{e}")
            ttk.Label(self, text=f"Module loading error: {e}", wraplength=500).pack(pady=50);
            return

        self.topmost_var = tk.BooleanVar()
        topmost_check = ttk.Checkbutton(self, text="Stay on top", variable=self.topmost_var,
                                        command=self._toggle_topmost)
        topmost_check.place(relx=1.0, rely=0, anchor='ne', x=-10, y=0)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(pady=(25, 10), padx=10, fill="both", expand=True)

        self.scanner_tab = UpdateInfoTab(self.notebook, self)
        self.history_tab = HistorySubTab(self.notebook, self)
        self.planner_tab = PlannerSubTab(self.notebook, self)
        self.publisher_tab = PublisherSubTab(self.notebook, self)

        self.notebook.add(self.scanner_tab, text='Information Update')
        self.notebook.add(self.history_tab, text='Publication History')
        self.notebook.add(self.planner_tab, text='Planner')
        self.notebook.add(self.publisher_tab, text='Publication')

        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.root.after(100, self.full_data_reload)  # Start with a short delay

    def _toggle_topmost(self):
        self.controller.root.attributes("-topmost", self.topmost_var.get())

    def _on_tab_changed(self, event=None):
        try:
            selected_tab = self.notebook.nametowidget(self.notebook.select())
            if hasattr(selected_tab, 'render_content'):
                selected_tab.render_content()
        except tk.TclError:
            pass

    def full_data_reload(self, on_complete=None):
        try:
            self.scanned_data_manager.ensure_sheet_exists()
            self.scanned_data_cache = self.scanned_data_manager.get_scanned_data()
            self.channel_manager.load_config()
            self.history_visualizer.generate_history_view()
        except Exception as e:
            self.scanned_data_cache = pd.DataFrame()
            self.history_visualizer.history = []
            self.scanner_tab.log_message(f"Error reading data: {e}")

        self.update_all_tabs()
        if on_complete: on_complete()

    def update_all_tabs(self):
        self.scanner_tab.update_channel_list()
        self.planner_tab.update_channel_list()
        self._on_tab_changed()

    def _on_closing(self):
        if hasattr(self, 'planner_tab'): self.planner_tab.save_settings()
        self.root.destroy()

    def transfer_plan_to_publisher(self, plan_data):
        self.publisher_tab.populate_queue(plan_data)
        self.notebook.select(self.publisher_tab)


class UpdateInfoTab(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent);
        self.controller = controller;
        self.root = controller.root;
        self.selected_item_data = {};
        self._create_widgets()

    def _create_widgets(self):
        main_frame = ttk.PanedWindow(self, orient=tk.VERTICAL);
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        top_panel = ttk.Frame(main_frame);
        main_frame.add(top_panel, weight=0)
        controls_frame = ttk.LabelFrame(top_panel, text="Scan Management");
        controls_frame.pack(side='left', fill='both', expand=True, padx=(0, 5))
        channel_select_frame = ttk.Frame(controls_frame);
        channel_select_frame.pack(fill='x', padx=5, pady=5)
        ttk.Label(channel_select_frame, text="Channel:").pack(side='left')
        self.channel_var = tk.StringVar();
        self.channel_var.trace_add('write', lambda *_: self.render_content())
        self.channel_combobox = ttk.Combobox(channel_select_frame, textvariable=self.channel_var, state='readonly',
                                             width=45);
        self.channel_combobox.pack(side='left', padx=5)
        settings_frame = ttk.Frame(controls_frame);
        settings_frame.pack(fill='x', padx=5, pady=5)
        ttk.Label(settings_frame, text="Scan from date:").pack(side='left')
        self.start_date_entry = ttk.Entry(settings_frame);
        self.start_date_entry.insert(0, (datetime.now() - timedelta(days=90)).strftime('%d.%m.%Y'));
        self.start_date_entry.pack(side='left', padx=5)
        self.scan_btn = ttk.Button(settings_frame, text="Start Scan", command=self._start_scan);
        self.scan_btn.pack(side='left', padx=10)
        settings_btn_frame = ttk.LabelFrame(top_panel, text="Settings");
        settings_btn_frame.pack(side='right', fill='y', padx=(5, 0))
        ttk.Button(settings_btn_frame, text="Channel Settings", command=self._open_channel_settings).pack(expand=True,
                                                                                                           padx=20,
                                                                                                           pady=10)
        middle_panel = ttk.Frame(main_frame);
        main_frame.add(middle_panel, weight=1)
        results_frame = ttk.LabelFrame(middle_panel, text="Found Publications (Posting_Scanned DB)");
        results_frame.pack(fill='both', expand=True, side='left', padx=(0, 5))
        cols = ('time', 'title', 'filename');
        self.results_tree = ttk.Treeview(results_frame, columns=cols, show='headings')
        ysb = ttk.Scrollbar(results_frame, orient='vertical', command=self.results_tree.yview);
        self.results_tree.configure(yscrollcommand=ysb.set);
        self.results_tree.pack(side='left', fill='both', expand=True);
        ysb.pack(side='right', fill='y')
        self.results_tree.heading('time', text="Date and Time");
        self.results_tree.heading('title', text="Title");
        self.results_tree.heading('filename', text="File Name")
        self.results_tree.column('time', width=150, anchor='center');
        self.results_tree.column('title', width=400);
        self.results_tree.column('filename', width=200)
        self.results_tree.bind("<<TreeviewSelect>>", self._on_result_select_in_tree)
        edit_frame = ttk.LabelFrame(middle_panel, text="File Name Editor");
        edit_frame.pack(fill='y', side='right', padx=(5, 0), ipadx=5)
        ttk.Label(edit_frame, text="File Name:").pack(pady=5, padx=5, anchor='w')
        self.filename_entry = ttk.Entry(edit_frame, width=30);
        self.filename_entry.pack(pady=5, padx=5, fill='x')
        self.save_filename_btn = ttk.Button(edit_frame, text="Save Name", command=self._save_filename,
                                            state='disabled');
        self.save_filename_btn.pack(pady=10, padx=5)
        log_frame = ttk.LabelFrame(main_frame, text="Log");
        main_frame.add(log_frame, weight=0)
        self.log_widget = scrolledtext.ScrolledText(log_frame, height=6, state='disabled', wrap=tk.WORD);
        self.log_widget.pack(fill='both', expand=True, padx=5, pady=5)

    def update_channel_list(self):
        channel_names = self.controller.channel_manager.get_channel_names()
        current_selection = self.channel_combobox.get()
        self.channel_combobox['values'] = ["All channels"] + channel_names
        if current_selection in self.channel_combobox['values']:
            self.channel_combobox.set(current_selection)
        else:
            self.channel_combobox.current(0)

    def render_content(self):
        self.results_tree.delete(*self.results_tree.get_children())
        df = self.controller.scanned_data_cache
        channel_name = self.channel_var.get()
        if not df.empty and channel_name != "All channels": df = df[df['ChannelName'] == channel_name]
        if not df.empty:
            df = df.sort_values(by='PublicationTimestamp', ascending=False)
            for _, row in df.iterrows():
                fname = row['OriginalFilename'] if pd.notna(row['OriginalFilename']) and row[
                    'OriginalFilename'] else "File name not specified"
                self.results_tree.insert("", 'end', iid=row['VideoURL'],
                                         values=(row['PublicationTimestamp'].strftime('%d.%m.%y %H:%M'),
                                                 row['VideoTitle'], fname), tags=(row['ChannelName'],))
        self._on_result_select_in_tree()

    def _open_channel_settings(self):
        ChannelSettingsWindow(self.root, self.controller.channel_manager)
        self.controller.full_data_reload()

    def log_message(self, msg):
        timestamp = datetime.now().strftime('%H:%M:%S');
        full_msg = f"[{timestamp}] {msg}\n";
        print(full_msg.strip())
        self.root.after(0, lambda: (self.log_widget.config(state='normal'), self.log_widget.insert('end', full_msg),
                                    self.log_widget.see('end'), self.log_widget.config(state='disabled')))

    def _start_scan(self):
        self.controller.scanned_data_manager.ensure_sheet_exists()
        channel_name = self.channel_var.get();
        start_date_str = self.start_date_entry.get()
        if not channel_name: messagebox.showerror("Error", "Channels not configured.", parent=self); return
        try:
            start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
            if (datetime.now() - start_date).days > 180: messagebox.showinfo("Warning",
                                                                             "YouTube RSS feed only provides ~15 recent videos.",
                                                                             parent=self)
        except ValueError:
            messagebox.showerror("Error", "Invalid date format.", parent=self); return
        self.scan_btn.config(state='disabled');
        self.log_message(f"Starting scan for '{channel_name}'...")
        threading.Thread(target=self._scan_thread, args=(channel_name, start_date), daemon=True).start()

    def _scan_thread(self, channel_name, start_date):
        try:
            config_all = self.controller.channel_manager.get_channels_data()
            channels_to_scan = config_all if channel_name == "All channels" else [ch for ch in config_all if
                                                                                ch['name'] == channel_name]
            all_found_videos = []
            for ch_data in channels_to_scan:
                if not ch_data.get('url') or not ch_data.get('channel_id'): self.log_message(
                    f"URL or ChannelID for '{ch_data['name']}' not found. Skipping."); continue
                videos = youtube_publications_scanner.fetch_channel_videos(ch_data['channel_id'], start_date,
                                                                           ch_data.get('proxy'))
                self.log_message(f"Found {len(videos)} videos on YouTube for '{ch_data['name']}'.")
                if videos:
                    for v in videos: v['channel_name'] = ch_data['name']; v['channel_id'] = ch_data['channel_id']
                    all_found_videos.extend(videos)
            if all_found_videos: self.controller.scanned_data_manager.add_scanned_videos(all_found_videos)
            self.log_message(f"Scan completed.")
        except Exception as e:
            self.log_message(f"Critical error in scanning thread: {e}")
        finally:
            self.root.after(0, self.controller.full_data_reload, lambda: self.scan_btn.config(state='normal'))

    def _on_result_select_in_tree(self, event=None):
        selected = self.results_tree.selection()
        if not selected: self.save_filename_btn.config(state='disabled'); self.filename_entry.delete(0,
                                                                                                     tk.END); self.selected_item_data = {}; return
        item_id = selected[0];
        item_values = self.results_tree.item(item_id, 'values');
        item_tags = self.results_tree.item(item_id, 'tags')
        self.selected_item_data = {'url': item_id, 'filename': item_values[2],
                                   'channel_name': item_tags[0] if item_tags else "", 'datetime_str': item_values[0]}
        self.filename_entry.delete(0, tk.END)
        if "not specified" not in self.selected_item_data['filename']: self.filename_entry.insert(0,
                                                                                               self.selected_item_data[
                                                                                                   'filename'])
        self.save_filename_btn.config(state='normal')

    def _save_filename(self):
        if not self.selected_item_data: return
        new_fname = self.filename_entry.get().strip();
        old_fname = self.selected_item_data['filename']
        if not new_fname: messagebox.showwarning("Warning", "File name cannot be empty.", parent=self); return
        if "not specified" not in old_fname and new_fname != old_fname:
            channel_info = self.selected_item_data['channel_name'];
            date_info = self.selected_item_data['datetime_str']
            msg = f"A file name is already set for publication on channel '{channel_info}' from {date_info}:\n\n'{old_fname}'\n\nReplace it with:\n\n'{new_fname}'?"
            if not messagebox.askyesno("Confirm overwrite", msg, parent=self): return
        self.save_filename_btn.config(state='disabled')
        on_complete = lambda: self.controller.full_data_reload(lambda: self.save_filename_btn.config(state='normal'))
        threading.Thread(target=lambda: (
            self.controller.scanned_data_manager.update_filename(self.selected_item_data['url'], new_fname),
            self.root.after(0, on_complete)), daemon=True).start()


class HistorySubTab(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent);
        self.controller = controller;
        self.root = controller.root
        self.tree = None
        self._create_widgets()

    def _create_widgets(self):
        self.tree_frame = ttk.Frame(self);
        self.tree_frame.pack(fill='both', expand=True, padx=10, pady=10)

    def render_content(self):
        if self.tree: self.tree.destroy(); self.ysb.destroy()
        history_data = self.controller.history_visualizer.history
        channel_names = self.controller.channel_manager.get_channel_names()
        columns = ("date",) + tuple(name.replace(" ", "_") for name in channel_names)
        self.tree = ttk.Treeview(self.tree_frame, columns=columns, show="headings")
        self.ysb = ttk.Scrollbar(self.tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=self.ysb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        self.ysb.pack(side="right", fill="y")
        self.tree_frame.update_idletasks()
        available_width = self.tree_frame.winfo_width()
        date_col_width = 100
        channel_col_width = max(80,
                                (available_width - date_col_width - 20) // len(channel_names)) if channel_names else 80
        self.tree.heading("date", text="Date");
        self.tree.column("date", width=date_col_width, anchor="center", stretch=tk.NO)
        for i, name in enumerate(channel_names):
            self.tree.heading(columns[i + 1], text=name);
            self.tree.column(columns[i + 1], width=channel_col_width, anchor="center")
        if not history_data:
            self.tree.insert("", "end", values=("No data to display.",) + ("",) * len(channel_names))
            return
        for row_data in history_data:
            row_values = [row_data['date'].strftime('%d.%b.%y')]
            for name in channel_names:
                cell_content = row_data.get(name)
                display_value = ""
                if cell_content is True:
                    display_value = "✓"
                elif isinstance(cell_content, str) and cell_content:
                    s_name = publication_planner_logic.parse_base_id(cell_content)
                    v_match = re.search(r'(v\d+)', cell_content, re.IGNORECASE)
                    if v_match: s_name += v_match.group(1).lower()
                    display_value = s_name
                row_values.append(display_value)
            self.tree.insert("", "end", values=tuple(row_values))


class PlannerSubTab(ttk.Frame):
    @classmethod
    def from_backup(cls, parent, controller):
        return cls(parent, controller)

    def __init__(self, parent, controller):
        super().__init__(parent);
        self.controller = controller;
        self.planner = controller.planner;
        self._create_widgets()

    def _create_widgets(self):
        main_frame = ttk.Frame(self);
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        input_frame = ttk.LabelFrame(main_frame, text="1. Settings and Data Input");
        input_frame.pack(fill="x", pady=5)
        settings_subframe = ttk.Frame(input_frame);
        settings_subframe.pack(fill="x", padx=10, pady=5)
        ttk.Label(settings_subframe, text="Minimum gap (days):").pack(side="left", padx=5)
        self.min_gap_spinbox = ttk.Spinbox(settings_subframe, from_=1, to=30, width=5);
        self.min_gap_spinbox.set(3);
        self.min_gap_spinbox.pack(side="left", padx=5)
        ttk.Label(settings_subframe, text="Ideal gap (days):").pack(side="left", padx=20)
        self.ideal_gap_spinbox = ttk.Spinbox(settings_subframe, from_=1, to=90, width=5);
        self.ideal_gap_spinbox.set(7);
        self.ideal_gap_spinbox.pack(side="left", padx=5)
        files_subframe = ttk.Frame(input_frame);
        files_subframe.pack(fill="both", expand=True, padx=10, pady=10)
        ttk.Label(files_subframe, text="Target channel:").pack(pady=2, anchor="w")
        self.channel_combobox = ttk.Combobox(files_subframe, state="readonly", width=40);
        self.channel_combobox.pack(pady=2, anchor="w")
        ttk.Label(files_subframe, text="List of new video files:").pack(pady=2, anchor="w")
        self.files_text = scrolledtext.ScrolledText(files_subframe, height=8, width=80);
        self.files_text.pack(fill="x", expand=True, pady=2)
        analyze_button = ttk.Button(input_frame, text="Create Plan", command=self._on_analyze);
        analyze_button.pack(pady=10)
        plan_frame = ttk.LabelFrame(main_frame, text="2. Publication Plan");
        plan_frame.pack(fill="both", expand=True, pady=10)
        columns = ("file", "recommendation", "manual_date");
        self.plan_tree = ttk.Treeview(plan_frame, columns=columns, show="headings");
        self.plan_tree.pack(fill="both", expand=True, padx=5, pady=5)
        self.plan_tree.heading("file", text="Your file");
        self.plan_tree.column("file", width=200);
        self.plan_tree.heading("recommendation", text="Analyzer recommendation");
        self.plan_tree.column("recommendation", width=500);
        self.plan_tree.heading("manual_date", text="Assigned date");
        self.plan_tree.column("manual_date", width=200, anchor="center")
        self.plan_tree.bind('<Double-1>', self._edit_cell)
        bottom_frame = ttk.Frame(main_frame);
        bottom_frame.pack(fill='x', pady=5)
        save_plan_button = ttk.Button(bottom_frame, text="Save schedule and proceed to publication",
                                      command=self._on_save_plan);
        save_plan_button.pack(side='right')
        self.status_label = ttk.Label(bottom_frame, text="");
        self.status_label.pack(side='left')

    def update_channel_list(self):
        self.channel_combobox['values'] = self.controller.channel_manager.get_channel_names()
        if self.channel_combobox['values']: self.channel_combobox.current(0)

    def render_content(self):
        pass

    def save_settings(self):
        pass

    def _on_analyze(self):
        min_gap = int(self.min_gap_spinbox.get());
        ideal_gap = int(self.ideal_gap_spinbox.get())
        raw_text = self.files_text.get('1.0', tk.END).strip()
        if not raw_text: messagebox.showwarning("Warning", "Enter video file names.", parent=self); return
        filenames = re.split(r'[\s,]+', raw_text);
        filenames = [f for f in filenames if f]
        results = self.controller.planner.analyze_files(filenames, min_gap, ideal_gap)
        results.sort(key=lambda x: x.get('min_date', datetime.now().date()))
        self.plan_tree.delete(*self.plan_tree.get_children())
        for res in results: self.plan_tree.insert("", "end", values=(res['filename'], res['details'],
                                                                     res.get('ideal_date',
                                                                             datetime.now().date()).strftime(
                                                                         '%d.%m.%Y')))

    def _edit_cell(self, event):
        pass

    def _on_save_plan(self):
        plan_data = [{"file": self.plan_tree.item(i, "values")[0], "date": self.plan_tree.item(i, "values")[2],
                      "channel": self.channel_combobox.get()} for i in self.plan_tree.get_children()]
        if plan_data:
            self.controller.transfer_plan_to_publisher(plan_data)
        else:
            messagebox.showwarning("Warning", "No data to save.", parent=self)


class PublisherSubTab(ttk.Frame):
    @classmethod
    def from_backup(cls, parent, controller): return cls(parent, controller)

    def __init__(self, parent, controller):
        super().__init__(parent);
        self.controller = controller;
        self.root_window = controller.root
        self.PROJECTS_ROOT_PATH = self.controller.DATABASE_PATH.parent / 'CELEBRITIES_CENTRALIZED';
        self.current_file_data = {};
        self._create_widgets()

    def _create_widgets(self):
        main_pane = ttk.PanedWindow(self, orient=tk.HORIZONTAL);
        main_pane.pack(fill="both", expand=True, padx=10, pady=10)
        left_panel = ttk.Frame(main_pane);
        main_pane.add(left_panel, weight=1)
        list_frame = ttk.LabelFrame(left_panel, text="Publication Queue");
        list_frame.pack(fill='both', expand=True, padx=5, pady=5)
        columns = ("date", "file");
        self.publish_queue_tree = ttk.Treeview(list_frame, columns=columns, show="headings");
        self.publish_queue_tree.pack(fill="both", expand=True)
        self.publish_queue_tree.heading("date", text="Date");
        self.publish_queue_tree.column("date", width=100, anchor="center");
        self.publish_queue_tree.heading("file", text="File");
        self.publish_queue_tree.column("file", width=200)
        self.publish_queue_tree.bind("<<TreeviewSelect>>", self._on_file_select)
        ttk.Button(left_panel, text="Refresh Finder", command=self._on_refresh_finder).pack(pady=5)
        self.details_frame = ttk.LabelFrame(main_pane, text="Publication Data:");
        main_pane.add(self.details_frame, weight=3)
        fields_container = ttk.Frame(self.details_frame, padding=10);
        fields_container.pack(fill="both", expand=True)
        video_btn = ttk.Button(fields_container, text="Open video file in Finder", command=self._on_open_video);
        video_btn.grid(row=0, column=0, columnspan=2, sticky="w", pady=(5, 10))
        ttk.Label(fields_container, text="Video Title:").grid(row=1, column=0, sticky="w", pady=2);
        self.title_entry = ttk.Entry(fields_container, width=80);
        self.title_entry.grid(row=2, column=0, columnspan=2, sticky="we", pady=2)
        ttk.Label(fields_container, text="Description:").grid(row=3, column=0, sticky="w", pady=2);
        self.desc_text = scrolledtext.ScrolledText(fields_container, height=4, width=80);
        self.desc_text.grid(row=4, column=0, columnspan=2, sticky="nsew", pady=2)
        thumb_btn = ttk.Button(fields_container, text="Open preview in Finder", command=self._on_open_thumb);
        thumb_btn.grid(row=5, column=0, columnspan=2, sticky="w", pady=(10, 5))
        fields_container.rowconfigure(4, weight=1);
        fields_container.columnconfigure(1, weight=1)
        commit_frame = ttk.Frame(self.details_frame, padding=10);
        commit_frame.pack(fill="x", side="bottom")
        ttk.Label(commit_frame, text="YouTube Link:").pack(side="left", padx=5);
        self.youtube_link_entry = ttk.Entry(commit_frame, width=40);
        self.youtube_link_entry.pack(side="left", padx=5)
        ttk.Label(commit_frame, text="Date (DD.MM.YYYY):").pack(side="left", padx=5);
        self.actual_date_entry = ttk.Entry(commit_frame);
        self.actual_date_entry.pack(side="left", padx=5)
        save_button = ttk.Button(commit_frame, text="Save to Database", command=self._on_save_to_db);
        save_button.pack(side="right", padx=10, pady=5)

    def render_content(self): pass

    def populate_queue(self, data):
        self.publish_queue_tree.delete(*self.publish_queue_tree.get_children())
        sorted_data = sorted(data, key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'))
        for item in sorted_data: self.publish_queue_tree.insert("", "end", values=(item["date"], item["file"]),
                                                                iid=item["file"])

    def _on_file_select(self, event): pass

    def _on_open_video(self): pass

    def _on_open_thumb(self): pass

    def _on_refresh_finder(self): pass

    def _on_save_to_db(self): pass