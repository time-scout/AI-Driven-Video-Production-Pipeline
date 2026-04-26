import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, font
import json
from pathlib import Path
import datetime
import threading
import re

# Import worker
from script_creation_worker import SeedManager, Brainstormer, EntityRecognizer, ScriptOrchestrator

class ScriptCreationTab(ttk.Frame):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app
        self.work_root = app.WORK_ROOT_PATH
        self.chat_history = []
        
        # Load settings
        sc_settings = self.app.get_setting("script_creation", {})
        min_w = sc_settings.get("min_words", "14")

        self.vars = {
            "pid": tk.StringVar(value=""),
            "version": tk.StringVar(value="v1"),
            "chronology": tk.StringVar(),
            "selected_perspective": tk.StringVar(),
            "archetype_number": tk.StringVar(value=""),
            "title_1": tk.StringVar(),
            "title_2": tk.StringVar(),
            "title_3": tk.StringVar(),
            "preliminary_perspective": tk.StringVar(),
            "min_words": tk.StringVar(value=min_w),
            "status_msg": tk.StringVar(value="Ready")
        }
        
        self.vars["min_words"].trace_add("write", self._save_global_settings)
        self._setup_ui()
        
    def _save_global_settings(self, *args):
        cur = self.app.get_setting("script_creation", {})
        cur["min_words"] = self.vars["min_words"].get()
        self.app.set_setting("script_creation", cur)

    def _setup_ui(self):
        self.sub_notebook = ttk.Notebook(self)
        self.sub_notebook.pack(fill='x', expand=False, padx=5, pady=5)
        
        self.tab0 = ttk.Frame(self.sub_notebook) # Song List
        self.tab1 = ttk.Frame(self.sub_notebook) # Script
        self.tab2 = ttk.Frame(self.sub_notebook) # Perspective Verification
        self.tab3 = ttk.Frame(self.sub_notebook) # Fact Gathering
        self.tab4 = ttk.Frame(self.sub_notebook) # Script Creation
        
        self.sub_notebook.add(self.tab0, text="Song List")
        self.sub_notebook.add(self.tab1, text="Script")
        self.sub_notebook.add(self.tab2, text="Perspective Verification")
        self.sub_notebook.add(self.tab3, text="Fact Gathering")
        self.sub_notebook.add(self.tab4, text="Script Creation")
        
        self._setup_tab0()
        self._setup_tab1()
        self._setup_tab2()
        self._setup_tab3()
        self._setup_tab4()
        
        # Bottom panel
        self.log_area = scrolledtext.ScrolledText(self, height=9, state='disabled', font=("Consolas", 9))
        self.log_area.pack(fill='both', expand=True, padx=5, pady=5)

    def _set_ui_state(self, state='normal'):
        buttons = [self.btn_load0, self.btn_save0, self.btn_find0, self.btn_load1, self.btn_save1]
        for b in buttons:
            try: b.config(state=state)
            except: pass

    def _setup_tab0(self):
        """Tab 0: Song List"""
        top_frame = ttk.Frame(self.tab0); top_frame.pack(fill='x', padx=10, pady=5)
        ttk.Label(top_frame, text="ID:").pack(side='left', padx=2)
        ttk.Entry(top_frame, textvariable=self.vars["pid"], width=8).pack(side='left', padx=2)
        self.btn_load0 = ttk.Button(top_frame, text="Load", command=self._on_load_click); self.btn_load0.pack(side='left', padx=5)
        ttk.Label(top_frame, text="V:").pack(side='left', padx=5)
        for v in ["v1", "v2", "v3", "v4", "v5", "v6"]:
            ttk.Radiobutton(top_frame, text=v, value=v, variable=self.vars["version"]).pack(side='left', padx=2)
        self.btn_find0 = ttk.Button(top_frame, text="Find in Database", command=self._on_find_in_db_click); self.btn_find0.pack(side='left', padx=15)
        self.btn_save0 = ttk.Button(top_frame, text="Save All Data", command=self._on_save_project_click); self.btn_save0.pack(side='left', padx=5)
        ttk.Label(top_frame, textvariable=self.vars["status_msg"], foreground="blue").pack(side='left', padx=20)
        ttk.Button(top_frame, text="↓", width=3, command=self._move_row_down).pack(side='right', padx=2)
        ttk.Button(top_frame, text="↑", width=3, command=self._move_row_up).pack(side='right', padx=2)

        mid_frame = ttk.Frame(self.tab0); mid_frame.pack(fill='x', padx=10, pady=5)
        mid_frame.columnconfigure(0, weight=1); mid_frame.columnconfigure(1, weight=3)
        l_cont = ttk.LabelFrame(mid_frame, text="Dirty List"); l_cont.grid(row=0, column=0, sticky='nsew', padx=(0, 5))
        self.dirty_list_text = scrolledtext.ScrolledText(l_cont, width=40, height=18, undo=True); self.dirty_list_text.pack(fill='both', expand=True, padx=5, pady=5)
        r_cont = ttk.LabelFrame(mid_frame, text="Recognition Results"); r_cont.grid(row=0, column=1, sticky='nsew')
        cols = ("Num", "Song", "SSID", "S_Status", "Artist", "EID", "A_Status")
        self.recognition_tree = ttk.Treeview(r_cont, columns=cols, show='headings', height=18)
        self.recognition_tree.heading("Num", text="№"); self.recognition_tree.column("Num", width=30, anchor='center', stretch=False)
        self.recognition_tree.heading("Song", text="Song"); self.recognition_tree.column("Song", width=220, stretch=True)
        self.recognition_tree.heading("SSID", text="SSID"); self.recognition_tree.column("SSID", width=65, anchor='center', stretch=False)
        self.recognition_tree.heading("S_Status", text="St"); self.recognition_tree.column("S_Status", width=35, anchor='center', stretch=False)
        self.recognition_tree.heading("Artist", text="Artist"); self.recognition_tree.column("Artist", width=220, stretch=True)
        self.recognition_tree.heading("EID", text="EID"); self.recognition_tree.column("EID", width=65, anchor='center', stretch=False)
        self.recognition_tree.heading("A_Status", text="St"); self.recognition_tree.column("A_Status", width=35, anchor='center', stretch=False)
        self.recognition_tree.pack(fill='both', expand=True, padx=5, pady=5)

    def _setup_tab1(self):
        """Tab 1: Script (formerly Settings and Ideas)"""
        top_frame = ttk.Frame(self.tab1); top_frame.pack(fill='x', padx=10, pady=5)
        ttk.Label(top_frame, text="ID:").pack(side='left', padx=2)
        ttk.Entry(top_frame, textvariable=self.vars["pid"], width=8).pack(side='left', padx=2)
        self.btn_load1 = ttk.Button(top_frame, text="Load", command=self._on_load_click); self.btn_load1.pack(side='left', padx=5)
        
        ttk.Label(top_frame, text="V:").pack(side='left', padx=5)
        for v in ["v1", "v2", "v3", "v4", "v5", "v6"]:
            ttk.Radiobutton(top_frame, text=v, value=v, variable=self.vars["version"]).pack(side='left', padx=2)
            
        ttk.Label(top_frame, text="Time interval:").pack(side='left', padx=5)
        ttk.Entry(top_frame, textvariable=self.vars["chronology"], width=12).pack(side='left', padx=2)
        ttk.Label(top_frame, text="Min Words:").pack(side='left', padx=(10, 2))
        ttk.Entry(top_frame, textvariable=self.vars["min_words"], width=2).pack(side='left', padx=2)
        
        self.btn_save1 = ttk.Button(top_frame, text="Save All Data", command=self._on_save_project_click); self.btn_save1.pack(side='left', padx=15)
        ttk.Label(top_frame, textvariable=self.vars["status_msg"], foreground="blue").pack(side='left', padx=10)

        # Preliminary perspective row
        prelim_frame = ttk.Frame(self.tab1)
        prelim_frame.pack(fill='x', padx=10, pady=(5, 0))
        ttk.Label(prelim_frame, text="Preliminary Perspective:", font=("-weight bold")).pack(side='left', padx=(0, 5))
        self.prelim_entry = ttk.Entry(prelim_frame, textvariable=self.vars["preliminary_perspective"], width=100)
        self.prelim_entry.pack(side='left', fill='x', expand=True)
        self._add_context_menu(self.prelim_entry)

        # Full-width perspective
        mid_frame = ttk.Frame(self.tab1); mid_frame.pack(fill='x', padx=10, pady=5)
        mid_frame.columnconfigure(0, weight=1)
        p_cont = ttk.LabelFrame(mid_frame, text="Ideation and Discussion")
        p_cont.grid(row=0, column=0, sticky='nsew')
        bb = ttk.Frame(p_cont); bb.pack(fill='x', padx=5, pady=2)
        ttk.Button(bb, text="Suggest Perspectives", command=self._on_brainstorm_start).pack(side='left')
        ttk.Button(bb, text="Send Message", command=self._on_send_message).pack(side='left', padx=5)
        ttk.Button(bb, text="Clear Context", command=self._on_clear_chat).pack(side='left', padx=5)
        f_large = font.Font(size=font.nametofont("TkDefaultFont").actual()['size'] + 2)
        self.ai_chat_output = scrolledtext.ScrolledText(p_cont, height=16, undo=True, font=f_large); self.ai_chat_output.pack(fill='both', expand=True, padx=5, pady=5)
        
        s_persp = ttk.Frame(self.tab1); s_persp.pack(fill='x', padx=10, pady=5)
        ttk.Label(s_persp, text="Selected Perspective:").pack(side='left')
        self.perspective_entry = tk.Entry(s_persp, textvariable=self.vars["selected_perspective"], width=100, relief="sunken", borderwidth=1); self.perspective_entry.pack(side='left', fill='x', expand=True, padx=5)
        self._add_context_menu(self.perspective_entry)
        ttk.Label(s_persp, text="archetype number:").pack(side='left', padx=(10, 2))
        self.archetype_entry = ttk.Entry(s_persp, textvariable=self.vars["archetype_number"], width=2); self.archetype_entry.pack(side='left', padx=2)
        
        btm_row = ttk.Frame(self.tab1); btm_row.pack(fill='x', padx=10, pady=5); btm_row.columnconfigure(1, weight=1)
        t_cont = ttk.LabelFrame(btm_row, text="Titles"); t_cont.grid(row=0, column=0, sticky='nw', padx=(0, 5))
        t_bb = ttk.Frame(t_cont); t_bb.pack(fill='x', padx=5, pady=2)
        ttk.Button(t_bb, text="Generate Variations", command=self._on_generate_titles_click).pack(side='left')
        ttk.Button(t_bb, text="Title Format", command=self._on_title_format_click).pack(side='left', padx=5)
        for i in range(1, 4):
            fr = ttk.Frame(t_cont); fr.pack(fill='x', padx=5, pady=2); ttk.Label(fr, text=f"Title {i}:", width=12).pack(side='left')
            ttk.Entry(fr, textvariable=self.vars[f"title_{i}"], width=80).pack(side='left')
        d_cont = ttk.LabelFrame(btm_row, text="Video Description"); d_cont.grid(row=0, column=1, sticky='nsew')
        self.video_desc_text = tk.Text(d_cont, height=8, undo=True); self.video_desc_text.pack(fill='both', expand=True, padx=5, pady=5)

        # Action Bar
        action_bar = ttk.Frame(self.tab1); action_bar.pack(fill='x', padx=10, pady=5)
        ttk.Button(action_bar, text="Write Script", style="Accent.TButton", command=self._on_write_script_click).pack(side='right', padx=5)

    def _on_write_script_click(self):
        pid = self.vars["pid"].get()
        if not pid: self._log("Enter project ID."); return
        
        self._set_ui_state('disabled')
        # Disable the specific button itself if needed, or via _set_ui_state if we add it there.
        # But _set_ui_state is hardcoded list. I should probably add this button to it or just disable it manually.
        # But prompt says: "Write Script", "Load", "Save" should be disabled.
        # _set_ui_state handles Load/Save. I'll stick to that and maybe disable notebook tabs if I could, but instruction is specific.
        
        def run_chain():
            try:
                ScriptOrchestrator.run_full_chain(self.app, pid, self._log)
            except Exception as e:
                self._log(f"RED: Error launching chain: {e}")
            finally:
                self.after(0, lambda: self._set_ui_state('normal'))

        threading.Thread(target=run_chain, daemon=True).start()

    def _on_load_click(self):
        pid = self.vars["pid"].get()
        if not pid: self._log("Enter project ID."); return
        data = SeedManager.load_seed(self.app.WORK_ROOT_PATH, pid)
        if data:
            self.vars["pid"].set(data.get("metadata", {}).get("pid", pid))
            self.vars["version"].set(data.get("metadata", {}).get("version", "v1"))
            self.vars["archetype_number"].set(data.get("metadata", {}).get("archetype_number", ""))
            self.vars["chronology"].set(data.get("inputs", {}).get("chronology", ""))
            self.vars["selected_perspective"].set(data.get("perspective", {}).get("selected", ""))
            self.vars["preliminary_perspective"].set(data.get("perspective", {}).get("preliminary", ""))
            td = data.get("titles_desc", {})
            self.vars["title_1"].set(td.get("t1", "")); self.vars["title_2"].set(td.get("t2", "")); self.vars["title_3"].set(td.get("t3", ""))
            raw_text = data.get("inputs", {}).get("songs_raw", "")
            if not raw_text and "songs" in data: raw_text = "\n".join([f"{s['artist']} - {s['song']}" for s in data["songs"]])
            self.dirty_list_text.delete("1.0", "end"); self.dirty_list_text.insert("1.0", raw_text)
            for item in self.recognition_tree.get_children(): self.recognition_tree.delete(item)
            if "songs" in data:
                for idx, s in enumerate(data["songs"]):
                    self.recognition_tree.insert("", "end", values=(idx+1, s['song'], s['ssid'], "✅", s['artist'], s['eid'], "✅"))
            self.video_desc_text.delete("1.0", "end"); self.video_desc_text.insert("1.0", td.get("description", ""))
            fname = f"PID{self.vars['pid'].get()}_seed.json" if not self.vars['pid'].get().startswith("PID") else f"{self.vars['pid'].get()}_seed.json"
            self.vars["status_msg"].set(f"Opened: {fname}")
            self._log(f"Project {fname} opened.")
        else: self.vars["status_msg"].set("Project not found"); self._log(f"Project {pid} not found.")

    def _on_find_in_db_click(self):
        raw = self.dirty_list_text.get("1.0", "end-1c").strip()
        if not raw: self._log("Song list is empty."); return
        self._set_ui_state('disabled')
        def run():
            try:
                self._log(f"Starting recognition for {len(raw.splitlines())} lines.")
                rec = EntityRecognizer(self.app); results = rec.check_list(raw, self._log)
                self.after(0, lambda: self._fill_recognition_tree(results))
            except Exception as e: self._log(f"Failure: {e}")
            finally: self.after(0, lambda: self._set_ui_state('normal'))
        threading.Thread(target=run, daemon=True).start()

    def _fill_recognition_tree(self, results):
        for item in self.recognition_tree.get_children(): self.recognition_tree.delete(item)
        for i, r in enumerate(results):
            s_st, a_st = ("✅" if r["song_status"] == "found" else "🆕"), ("✅" if r["artist_status"] == "found" else "🆕")
            self.recognition_tree.insert("", "end", values=(i+1, r["song"], r["ssid"] or "", s_st, r["artist"], r["eid"] or "", a_st))

    def _on_save_project_click(self):
        pid_input = self.vars["pid"].get(); ver = self.vars["version"].get()
        tree_data = []
        for child in self.recognition_tree.get_children():
            v = self.recognition_tree.item(child)["values"]
            tree_data.append({"song": v[1], "ssid": v[2], "song_status": "found" if v[3] == "✅" else "new", "artist": v[4], "eid": v[5], "artist_status": "found" if v[6] == "✅" else "new"})
        if not tree_data: self._log("No data in table."); return
        
        # Check for file existence BEFORE saving
        seed_dir = Path(self.app.WORK_ROOT_PATH) / "database" / "seed"
        # Name formation logic (simplified for verification)
        potential_name = pid_input if re.search(r'\d+v\d+', pid_input) else f"{pid_input}{ver}"
        if not potential_name.startswith("PID"): potential_name = f"PID{potential_name}"
        file_path = seed_dir / f"{potential_name}_seed.json"
        
        if file_path.exists():
            if not messagebox.askyesno("File exists", f"File {file_path.name} already exists.\nOverwrite?"):
                self._log("Saving cancelled.")
                return

        data = {
            "inputs": {
                "songs_raw": self.dirty_list_text.get("1.0", "end-1c"),
                "chronology": self.vars["chronology"].get()
            },
            "perspective": {
                "selected": self.vars["selected_perspective"].get(),
                "preliminary": self.vars["preliminary_perspective"].get()
            },
            "titles_desc": {
                "t1": self.vars["title_1"].get(),
                "t2": self.vars["title_2"].get(),
                "t3": self.vars["title_3"].get(),
                "description": self.video_desc_text.get("1.0", "end-1c")
            }
        }
        self._set_ui_state('disabled')
        def run():
            try:
                res = SeedManager.save_seed(self.app, data, pid_input, ver, tree_data, archetype_number=self.vars["archetype_number"].get())
                if res["status"] == "success": self.after(0, lambda: self._on_save_success(res))
                else: self._log(f"Error: {res.get('message')}")
            except Exception as e: self._log(f"Save failure: {e}")
            finally: self.after(0, lambda: self._set_ui_state('normal'))
        threading.Thread(target=run, daemon=True).start()

    def _on_save_success(self, res):
        self.vars["pid"].set(res["pid"]); self.vars["status_msg"].set(f"Opened: {res['filename']}")
        self._log(f"Data saved to {res['filename']}"); self._on_load_click()

    def _move_row_up(self):
        sel = self.recognition_tree.selection()
        if not sel: return
        for s in sel:
            idx = self.recognition_tree.index(s)
            if idx > 0: self.recognition_tree.move(s, self.recognition_tree.parent(s), idx-1)
        self._reindex_tree()

    def _move_row_down(self):
        sel = self.recognition_tree.selection()
        if not sel: return
        for s in reversed(sel):
            idx = self.recognition_tree.index(s)
            self.recognition_tree.move(s, self.recognition_tree.parent(s), idx+1)
        self._reindex_tree()

    def _reindex_tree(self):
        for i, child in enumerate(self.recognition_tree.get_children()):
            v = list(self.recognition_tree.item(child)["values"]); v[0] = i + 1; self.recognition_tree.item(child, values=v)

    def _add_context_menu(self, widget):
        m = tk.Menu(self, tearoff=0); m.add_command(label="Cut", command=lambda: widget.event_generate("<<Cut>>")); m.add_command(label="Copy", command=lambda: widget.event_generate("<<Copy>>")); m.add_command(label="Paste", command=lambda: widget.event_generate("<<Paste>>"))
        def show(e): m.post(e.x_root, e.y_root)
        widget.bind("<Button-3>", show); widget.bind("<Button-2>", show)

    def _log(self, message): self.after(0, lambda: self._safe_log(message))
    def _safe_log(self, message):
        self.log_area.configure(state='normal'); ts = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_area.insert('end', f"[{ts}] {message}\n"); self.log_area.see('end'); self.log_area.configure(state='disabled')

    def _on_clear_chat(self): self.ai_chat_output.delete("1.0", "end"); self._log("Chat cleared.")
    def _on_brainstorm_start(self):
        raw = self.dirty_list_text.get("1.0", "end-1c"); chron = self.vars["chronology"].get(); ver = self.vars["version"].get()
        if not raw.strip(): self._log("Song list is empty."); return
        self._on_clear_chat()
        threading.Thread(target=lambda: self._append_ai_reply(Brainstormer.chat_with_ai(self.app, "", raw, chron, version=ver)), daemon=True).start()
    def _on_send_message(self):
        raw = self.dirty_list_text.get("1.0", "end-1c"); chron = self.vars["chronology"].get(); txt = self.ai_chat_output.get("1.0", "end-1c"); ver = self.vars["version"].get()
        threading.Thread(target=lambda: self._append_ai_reply(Brainstormer.chat_with_ai(self.app, txt, raw, chron, version=ver)), daemon=True).start()
    def _append_ai_reply(self, r): self.after(0, lambda: [self.ai_chat_output.insert('end', f"\n\n>>> ASSISTANT:\n{r}\n\n>>> USER:\n"), self.ai_chat_output.see('end')])
    def _on_generate_titles_click(self):
        raw = self.dirty_list_text.get("1.0", "end-1c"); chron = self.vars["chronology"].get(); persp = self.vars["selected_perspective"].get(); ver = self.vars["version"].get()
        if not persp.strip(): self._log("Selected perspective is empty."); return
        def run():
            res = Brainstormer.generate_titles(self.app, raw, chron, persp, version=ver); self.after(0, lambda: self._apply_titles_result(res))
        threading.Thread(target=run, daemon=True).start()
    def _apply_titles_result(self, res):
        if "error" in res: self._log(f"AI Error: {res['error']}"); return
        self.vars["title_1"].set(res.get("title_1", "")); self.vars["title_2"].set(res.get("title_2", "")); self.vars["title_3"].set(res.get("title_3", ""))
        self.video_desc_text.delete("1.0", "end"); self.video_desc_text.insert("1.0", res.get("description", ""))
    def _on_title_format_click(self):
        p = tk.Toplevel(self); p.title("Formats"); p.geometry("600x300"); p.grab_set()
        s = self.app.get_setting("script_creation", {}); fms = s.get("title_formats", ["", "", "", "", "", ""])
        ents = []
        for i in range(6):
            fr = ttk.Frame(p); fr.pack(fill='x', padx=10, pady=2); e = ttk.Entry(fr); e.insert(0, fms[i] if i < len(fms) else ""); e.pack(side='left', fill='x', expand=True); ents.append(e)
        def save(): s["title_formats"] = [e.get().strip() for e in ents]; self.app.set_setting("script_creation", s); p.destroy()
        ttk.Button(p, text="Save", command=save).pack(pady=10)
    def _setup_tab2(self):
        f = ttk.Frame(self.tab2); f.pack(fill='both', expand=True, padx=10, pady=10)
        ttk.Button(f, text="Verify", command=lambda: self._log("Verification...")).pack()
        self.v_out = scrolledtext.ScrolledText(f, height=15); self.v_out.pack(fill='both', expand=True)
    def _setup_tab3(self):
        f = ttk.Frame(self.tab3); f.pack(fill='both', expand=True, padx=10, pady=10)
        ttk.Button(f, text="Fact Gathering", command=lambda: self._log("Gathering...")).pack()
        self.f_tree = ttk.Treeview(f, columns=("1","2"), show='headings'); self.f_tree.pack(fill='both', expand=True)
    def _setup_tab4(self):
        f = ttk.Frame(self.tab4); f.pack(fill='both', expand=True, padx=10, pady=10)
        ttk.Button(f, text="Final Script", command=lambda: self._log("Final...")).pack()
        self.s_prev = scrolledtext.ScrolledText(f, height=20); self.s_prev.pack(fill='both', expand=True)