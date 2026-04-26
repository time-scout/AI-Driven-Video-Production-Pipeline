# --- app_settings_interface.py - Enhanced Version ---

import tkinter as tk
from tkinter import ttk, filedialog, simpledialog, messagebox
import json
import requests


class AppSettingsTab(ttk.Frame):
    def __init__(self, parent_notebook, app):
        super().__init__(parent_notebook)
        self.app = app

        # Variables for paths
        self.projects_path_var = tk.StringVar()
        self.materials_path_var = tk.StringVar()

        # Variables for API keys
        self.api_key_var = tk.StringVar()
        self.hf_token_var = tk.StringVar()

        # AI Manager reference (will be set after initialization)
        self.ai_manager = None

        self._create_widgets()
        self.update_fields_from_settings()

    def _create_widgets(self):
        # Create main Notebook for subtabs
        notebook = ttk.Notebook(self)
        notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # Create frames for each subtab
        paths_tab = ttk.Frame(notebook)
        api_tab = ttk.Frame(notebook)
        ai_tab = ttk.Frame(notebook)

        notebook.add(paths_tab, text='Paths')
        notebook.add(api_tab, text='API Keys')
        notebook.add(ai_tab, text='AI Networks')

        # Populate each subtab
        self._create_paths_sub_tab(paths_tab)
        self._create_api_sub_tab(api_tab)
        self._create_ai_sub_tab(ai_tab)

    def _create_paths_sub_tab(self, parent):
        """Creates subtab with path settings"""
        settings_frame = ttk.Frame(parent, padding="15")
        settings_frame.pack(fill='x', expand=True)
        settings_frame.columnconfigure(1, weight=1)

        # --- Paths ---
        ttk.Label(settings_frame, text="Project Root Folder:").grid(row=0, column=0, padx=(0, 10), pady=10,
                                                                         sticky='w')
        projects_path_entry = ttk.Entry(settings_frame, textvariable=self.projects_path_var)
        projects_path_entry.grid(row=0, column=1, pady=10, sticky='ew')
        projects_path_entry.bind("<FocusOut>", lambda e: self.app.set_setting('work_root_path',
                                                                                      self.projects_path_var.get()))
        ttk.Button(settings_frame, text="Select...", command=self._select_projects_path).grid(row=0, column=2,
                                                                                                padx=(10, 0))

        ttk.Label(settings_frame, text="Materials Root Folder:").grid(row=1, column=0, pady=10, sticky='w')
        materials_path_entry = ttk.Entry(settings_frame, textvariable=self.materials_path_var)
        materials_path_entry.grid(row=1, column=1, pady=10, sticky='ew')
        materials_path_entry.bind("<FocusOut>", lambda e: self.app.set_setting('media_archive_path',
                                                                                       self.materials_path_var.get()))
        ttk.Button(settings_frame, text="Select...", command=self._select_materials_path).grid(row=1, column=2,
                                                                                                 padx=(10, 0))

    def _create_api_sub_tab(self, parent):
        """Creates subtab with API keys (dynamic display for all providers)"""
        self.api_key_frame = ttk.Frame(parent, padding="15")
        self.api_key_frame.pack(fill='x', expand=True)
        self.api_key_frame.columnconfigure(1, weight=1)

        # Refresh button in top center
        button_frame = ttk.Frame(self.api_key_frame)
        button_frame.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        ttk.Button(button_frame, text="Refresh Provider List", command=self._refresh_api_keys).pack()

        # Create scrollable frame for API keys
        canvas = tk.Canvas(self.api_key_frame, height=400)
        scrollbar = ttk.Scrollbar(self.api_key_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.grid(row=1, column=0, columnspan=3, sticky='nsew')
        scrollbar.grid(row=1, column=3, sticky='ns')

        # Grid weight configuration
        self.api_key_frame.rowconfigure(1, weight=1)
        self.api_key_frame.columnconfigure(1, weight=1)

        # Header
        ttk.Label(scrollable_frame, text="API Keys for Providers", font=("-size 12 -weight bold")).grid(row=0, column=0, columnspan=3, pady=(0, 20), sticky='w')
        scrollable_frame.columnconfigure(1, weight=1)

        # Dictionary for storing API key widgets
        self.api_key_widgets = {}

        # Add standard providers
        self._add_standard_api_keys(scrollable_frame)

        # Save reference to scrollable_frame
        self.scrollable_frame = scrollable_frame

        # Initialize keys on startup via after, to allow GUI to build
        self.after(100, self._refresh_api_keys)

    def _create_ai_sub_tab(self, parent):
        """Creates subtab with AI settings"""
        main_frame = ttk.Frame(parent, padding=10)
        main_frame.pack(fill='both', expand=True)

        ttk.Label(main_frame, text="AI Management", font=("-size 16 -weight bold")).pack(anchor='w', pady=(0, 20))

        # Providers section
        providers_frame = ttk.LabelFrame(main_frame, text="AI Providers", padding=10)
        providers_frame.pack(fill='x', pady=(0, 10))

        providers_tree_frame = ttk.Frame(providers_frame)
        providers_tree_frame.pack(fill='x', pady=(0, 5))

        self.providers_tree = ttk.Treeview(providers_tree_frame, columns=('name', 'url'), show='headings', height=6)
        self.providers_tree.heading('name', text='Name')
        self.providers_tree.heading('url', text='URL')
        self.providers_tree.column('name', width=150)
        self.providers_tree.column('url', width=300)
        self.providers_tree.pack(side='left', fill='both', expand=True)

        providers_scrollbar = ttk.Scrollbar(providers_tree_frame, orient="vertical", command=self.providers_tree.yview)
        self.providers_tree.configure(yscrollcommand=providers_scrollbar.set)
        providers_scrollbar.pack(side='right', fill='y')

        # Bind selection event for filtering
        self.providers_tree.bind('<<TreeviewSelect>>', self._on_provider_selected)

        provider_buttons = ttk.Frame(providers_frame)
        provider_buttons.pack(fill='x')
        ttk.Button(provider_buttons, text="Add", command=self.add_provider).pack(side='left', padx=(0, 5))
        ttk.Button(provider_buttons, text="Edit", command=self.edit_provider).pack(side='left', padx=(0, 5))
        ttk.Button(provider_buttons, text="Delete", command=self.delete_provider).pack(side='left')

        # Models section
        models_frame = ttk.LabelFrame(main_frame, text="AI Models", padding=10)
        models_frame.pack(fill='x', pady=(0, 10))

        models_tree_frame = ttk.Frame(models_frame)
        models_tree_frame.pack(fill='x', pady=(0, 5))

        self.models_tree = ttk.Treeview(models_tree_frame, columns=('id', 'string', 'name', 'provider'), show='headings', height=6)
        self.models_tree.heading('id', text='ID')
        self.models_tree.heading('string', text='API String')
        self.models_tree.heading('name', text='Name')
        self.models_tree.heading('provider', text='Provider')
        self.models_tree.column('id', width=100)
        self.models_tree.column('string', width=200)
        self.models_tree.column('name', width=150)
        self.models_tree.column('provider', width=100)
        self.models_tree.pack(side='left', fill='both', expand=True)

        models_scrollbar = ttk.Scrollbar(models_tree_frame, orient="vertical", command=self.models_tree.yview)
        self.models_tree.configure(yscrollcommand=models_scrollbar.set)
        models_scrollbar.pack(side='right', fill='y')

        model_buttons = ttk.Frame(models_frame)
        model_buttons.pack(fill='x')
        self.add_model_button = ttk.Button(model_buttons, text="Add", command=self.add_model, state='disabled')
        self.add_model_button.pack(side='left', padx=(0, 5))
        ttk.Button(model_buttons, text="Edit", command=self.edit_model).pack(side='left', padx=(0, 5))
        ttk.Button(model_buttons, text="Delete", command=self.delete_model).pack(side='left')

        # Task assignments section
        assignments_frame = ttk.LabelFrame(main_frame, text="Task Assignment", padding=10)
        assignments_frame.pack(fill='x', pady=(0, 10))

        self.assignments_frame = ttk.Frame(assignments_frame)
        self.assignments_frame.pack(fill='x')

        # Load initial configuration
        self.load_ai_config()

    def _add_standard_api_keys(self, parent):
        """Adds standard API keys"""
        # --- Google AI API Key ---
        row = 1
        ttk.Label(parent, text="Google AI API Key:").grid(row=row, column=0, pady=10, sticky='w')
        self.api_key_label = ttk.Label(parent, text="", style="secondary.TLabel")
        self.api_key_label.grid(row=row, column=1, pady=10, sticky='w')
        api_key_frame = ttk.Frame(parent)
        api_key_frame.grid(row=row, column=2, padx=(10, 0))
        self.api_key_button = ttk.Button(api_key_frame, text="Set Key...", command=self._set_api_key)
        self.api_key_button.pack(side='left', padx=(0, 5))
        self.api_key_test_button = ttk.Button(api_key_frame, text="Test", command=self._test_api_key)
        self.api_key_test_button.pack(side='left')

        # --- Hugging Face Token ---
        row += 1
        ttk.Label(parent, text="Hugging Face Token:").grid(row=row, column=0, pady=10, sticky='w')
        self.hf_token_label = ttk.Label(parent, text="", style="secondary.TLabel")
        self.hf_token_label.grid(row=row, column=1, pady=10, sticky='w')
        hf_token_frame = ttk.Frame(parent)
        hf_token_frame.grid(row=row, column=2, padx=(10, 0))
        self.hf_token_button = ttk.Button(hf_token_frame, text="Set Token...", command=self._set_hf_token)
        self.hf_token_button.pack(side='left', padx=(0, 5))
        self.hf_token_test_button = ttk.Button(hf_token_frame, text="Test", command=self._test_hf_token)
        self.hf_token_test_button.pack(side='left')

        # --- OpenRouter API Key ---
        row += 1
        ttk.Label(parent, text="OpenRouter API Key:").grid(row=row, column=0, pady=10, sticky='w')
        self.openrouter_key_label = ttk.Label(parent, text="", style="secondary.TLabel")
        self.openrouter_key_label.grid(row=row, column=1, pady=10, sticky='w')
        openrouter_frame = ttk.Frame(parent)
        openrouter_frame.grid(row=row, column=2, padx=(10, 0))
        self.openrouter_key_button = ttk.Button(openrouter_frame, text="Set Key...", command=self._set_openrouter_key)
        self.openrouter_key_button.pack(side='left', padx=(0, 5))
        self.openrouter_key_test_button = ttk.Button(openrouter_frame, text="Test", command=self._test_openrouter_key)
        self.openrouter_key_test_button.pack(side='left')

        if not self.app.TOML_AVAILABLE:
            self.api_key_button.config(state='disabled')
            self.api_key_test_button.config(state='disabled')
            self.api_key_label.config(text="Install 'tomli' and 'tomli-w' to use")
            self.hf_token_button.config(state='disabled')
            self.hf_token_test_button.config(state='disabled')
            self.openrouter_key_button.config(state='disabled')
            self.openrouter_key_test_button.config(state='disabled')

    def _refresh_api_keys(self):
        """Refreshes API key list based on providers from AI config"""
        # Get provider list from AI config
        ai_config = self.app.settings.get('ai_settings', {})
        providers = ai_config.get('providers', {})

        # Standard providers already in UI
        standard_providers = {"OpenRouter", "Google", "Hugging Face"}

        # Debugging information
        print(f"DEBUG: Found providers: {list(providers.keys())}")
        print(f"DEBUG: Existing api_key_widgets: {list(self.api_key_widgets.keys())}")

        # Add API keys for new providers
        for provider_name, provider_data in providers.items():
            if provider_name not in standard_providers and provider_name not in self.api_key_widgets:
                print(f"DEBUG: Adding provider {provider_name}")
                self._add_provider_api_key(provider_name)

        # Update all key values
        self._update_all_api_key_labels()

    def _add_provider_api_key(self, provider_name):
        """Adds widgets for a new provider's API key"""
        parent = self.scrollable_frame

        # Skip header and count rows with keys
        # Header in row 0, keys start from 1
        # 3 standard providers = rows 1-3
        row = 4  # Start after standard providers

        # Find an empty row by checking existing widgets
        while True:
            # Check if a widget exists at position (row, 0)
            occupied = False
            for widget in parent.grid_slaves():
                info = widget.grid_info()
                if info and info['row'] == row and info['column'] == 0:
                    occupied = True
                    break

            if not occupied:
                break
            row += 1

        # Create widgets for this provider
        ttk.Label(parent, text=f"API Key {provider_name}:").grid(row=row, column=0, pady=10, sticky='w')

        key_label = ttk.Label(parent, text="", style="secondary.TLabel")
        key_label.grid(row=row, column=1, pady=10, sticky='w')

        key_frame = ttk.Frame(parent)
        key_frame.grid(row=row, column=2, padx=(10, 0))

        set_button = ttk.Button(key_frame, text="Set Key...",
                               command=lambda p=provider_name: self._set_provider_api_key(p))
        set_button.pack(side='left', padx=(0, 5))

        test_button = ttk.Button(key_frame, text="Test",
                                command=lambda p=provider_name: self._test_provider_api_key(p))
        test_button.pack(side='left')

        if not self.app.TOML_AVAILABLE:
            set_button.config(state='disabled')
            test_button.config(state='disabled')
            key_label.config(text="Install 'tomli' and 'tomli-w' to use")

        # Save widgets to dictionary
        self.api_key_widgets[provider_name] = {
            'label': key_label,
            'set_button': set_button,
            'test_button': test_button
        }

    def _get_api_key_name_for_provider(self, provider_name: str) -> str:
        """Get API key name for provider"""
        # Mapping providers to key names in secrets.toml
        key_mapping = {
            "OpenRouter": "OPENROUTER_API_KEY",
            "Google": "GOOGLE_API_KEY",
            "Google Account_1": "GOOGLE_API_KEY",
            "Google Account_2": "GOOGLE_API_KEY",
            "Googler": "GOOGLER_API_KEY",
            "Hugging Face": "HF_TOKEN",
            "Z AI": "Z_AI_API_KEY",
        }
        return key_mapping.get(provider_name, f"{provider_name.upper().replace(' ', '_')}_API_KEY")

    def _set_provider_api_key(self, provider_name):
        """Sets API key for provider"""
        api_key_name = self._get_api_key_name_for_provider(provider_name)
        current_key = self.app.get_setting(api_key_name, '')

        print(f"DEBUG: Setting API key for {provider_name} as {api_key_name}")

        new_key = simpledialog.askstring(
            f"API Key {provider_name}",
            f"Enter API key for {provider_name}:",
            initialvalue=current_key
        )

        if new_key is None:
            return

        if new_key == "" and current_key != "":
            if not messagebox.askyesno("Confirmation", "Are you sure you want to delete the key?"):
                return

        self.app.set_setting(api_key_name, new_key)
        print(f"DEBUG: Saved key {api_key_name}: {'*' * len(new_key) if new_key else 'None'}")
        self._update_all_api_key_labels()

    def _test_provider_api_key(self, provider_name):
        """Tests API key for provider"""
        api_key_name = self._get_api_key_name_for_provider(provider_name)
        api_key = self.app.get_setting(api_key_name, '')

        if not api_key:
            messagebox.showwarning("Warning", f"API key for {provider_name} is not set")
            return

        # Get provider data from config
        ai_config = self.app.settings.get('ai_settings', {})
        providers = ai_config.get('providers', {})
        provider_data = providers.get(provider_name, {})
        base_url = provider_data.get('base_url', '')

        if not base_url:
            messagebox.showwarning("Warning", f"base_url not specified for provider {provider_name}")
            return

        # Simple check - try to get list of models
        try:
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            # Try standard endpoint for OpenAI-compatible API
            test_url = f"{base_url.rstrip('/')}/models"
            response = requests.get(test_url, headers=headers, timeout=10)

            if response.status_code == 200:
                messagebox.showinfo("Success", f"API key for {provider_name} is working correctly!")
            else:
                messagebox.showerror("Error", f"API key is not working. Code: {response.status_code}")
        except Exception as e:
            messagebox.showerror("Error", f"Error checking API key: {str(e)}")

    def _update_all_api_key_labels(self):
        """Updates all API key labels"""
        if not self.app.TOML_AVAILABLE: return

        # Update standard keys
        self._update_api_key_label()
        self._update_hf_token_label()
        self._update_openrouter_key_label()

        # Update keys of new providers
        for provider_name, widgets in self.api_key_widgets.items():
            api_key_name = self._get_api_key_name_for_provider(provider_name)
            key = self.app.get_setting(api_key_name, '')
            display_text = "Key not set" if not key else f"{key[:20]}..." if len(key) > 20 else key
            widgets['label'].config(text=display_text)

    # === Existing methods for paths and API keys ===
    def update_fields_from_settings(self):
        self.projects_path_var.set(self.app.get_setting('work_root_path', ''))
        self.materials_path_var.set(self.app.get_setting('media_archive_path', ''))
        self._update_all_api_key_labels()

    def _select_projects_path(self):
        if path := filedialog.askdirectory(title="Select project root folder"):
            self.projects_path_var.set(path)
            self.app.set_setting('work_root_path', path)

    def _select_materials_path(self):
        if path := filedialog.askdirectory(title="Select materials root folder"):
            self.materials_path_var.set(path)
            self.app.set_setting('media_archive_path', path)

    def _set_api_key(self):
        current_key = self.app.get_setting('GOOGLE_API_KEY', '')
        new_key = simpledialog.askstring(
            "API Key", "Enter your Google AI API key:",
            initialvalue=current_key
        )

        if new_key is None:
            return

        if new_key == "" and current_key != "":
            if not messagebox.askyesno("Confirmation", "Are you sure you want to delete the key?"):
                return

        self.app.set_setting('GOOGLE_API_KEY', new_key)
        self._update_api_key_label()

    def _set_hf_token(self):
        current_token = self.app.get_setting('HF_TOKEN', '')
        new_token = simpledialog.askstring(
            "Hugging Face Token", "Enter your HF Token:",
            initialvalue=current_token
        )

        if new_token is None:
            return

        if new_token == "" and current_token != "":
            if not messagebox.askyesno("Confirmation", "Are you sure you want to delete the token?"):
                return

        self.app.set_setting('HF_TOKEN', new_token)
        self._update_hf_token_label()

    def _update_api_key_label(self):
        if not self.app.TOML_AVAILABLE: return
        key = self.app.get_setting('GOOGLE_API_KEY', '')
        display_text = "Key not set" if not key else key
        self.api_key_label.config(text=display_text)

    def _update_hf_token_label(self):
        if not self.app.TOML_AVAILABLE: return
        token = self.app.get_setting('HF_TOKEN', '')
        display_text = "Token not set" if not token else token
        self.hf_token_label.config(text=display_text)

    def _test_api_key(self):
        """Tests Google AI API key"""
        api_key = self.app.get_setting('GOOGLE_API_KEY', '')
        if not api_key:
            messagebox.showwarning("Warning", "API key is not set")
            return

        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-pro')
            response = model.generate_content("Test")
            if response.text:
                messagebox.showinfo("Success", "API key is working correctly!")
            else:
                messagebox.showerror("Error", "API key is not working")
        except Exception as e:
            messagebox.showerror("Error", f"Error checking API key: {str(e)}")

    def _test_hf_token(self):
        """Tests Hugging Face token"""
        hf_token = self.app.get_setting('HF_TOKEN', '')
        if not hf_token:
            messagebox.showwarning("Warning", "HF token is not set")
            return

        try:
            from huggingface_hub import HfApi, whoami
            api = HfApi(token=hf_token)
            user_info = whoami(api)
            messagebox.showinfo("Success", f"Token works! User: {user_info.get('name', 'N/A')}")
        except ImportError:
            messagebox.showwarning("Warning", "Install 'huggingface_hub' to test the token")
        except Exception as e:
            messagebox.showerror("Error", f"Error checking token: {str(e)}")

    def _set_openrouter_key(self):
        current_key = self.app.get_setting('OPENROUTER_API_KEY', '')
        new_key = simpledialog.askstring(
            "OpenRouter API Key", "Enter your OpenRouter API key:",
            initialvalue=current_key
        )

        if new_key is None:
            return

        if new_key == "" and current_key != "":
            if not messagebox.askyesno("Confirmation", "Are you sure you want to delete the key?"):
                return

        self.app.set_setting('OPENROUTER_API_KEY', new_key)
        self._update_openrouter_key_label()

    def _update_openrouter_key_label(self):
        if not self.app.TOML_AVAILABLE: return
        key = self.app.get_setting('OPENROUTER_API_KEY', '')
        display_text = "Key not set" if not key else key
        self.openrouter_key_label.config(text=display_text)

    def _test_openrouter_key(self):
        """Tests OpenRouter API key"""
        api_key = self.app.get_setting('OPENROUTER_API_KEY', '')
        if not api_key:
            messagebox.showwarning("Warning", "OpenRouter API key is not set")
            return

        try:
            import requests
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            response = requests.get("https://openrouter.ai/api/v1/models", headers=headers, timeout=10)
            if response.status_code == 200:
                models = response.json()
                messagebox.showinfo("Success", f"API key works! {len(models.get('data', []))} models available")
            else:
                messagebox.showerror("Error", f"API key is not working. Code: {response.status_code}")
        except Exception as e:
            messagebox.showerror("Error", f"Error checking API key: {str(e)}")

    # === AI Management methods from donor project ===
    def load_ai_config(self):
        """Loads AI configuration from settings"""
        # Get entire config from main controller
        config = self.app.settings
        # Work only with our section
        self.ai_config = config.get('ai_settings', {"providers": {}, "models": [], "task_assignments": {}})

        # Load providers from self.ai_config
        self.providers_tree.delete(*self.providers_tree.get_children())
        providers = self.ai_config.get('providers', {})
        for name, data in providers.items():
            self.providers_tree.insert('', 'end', values=(name, data.get('base_url', '')))

        # Load models - don't show any initially (Provider-Container concept)
        self.models_tree.delete(*self.models_tree.get_children())

        self.load_task_assignments({})

    def _on_provider_selected(self, event):
        """Handle provider selection - Provider-Container concept"""
        selected_items = self.providers_tree.selection()

        if not selected_items:
            # No provider selected - empty models table and disable add button
            self.models_tree.delete(*self.models_tree.get_children())
            self.add_model_button.config(state='disabled')
            # Clear task assignments when no provider selected
            self.load_task_assignments({})
            return

        # Provider selected - switch to provider container context
        provider_name = self.providers_tree.item(selected_items[0], 'values')[0]

        # Clear models table and repopulate only with selected provider's models from self.ai_config
        self.models_tree.delete(*self.models_tree.get_children())

        models = self.ai_config.get('models', [])

        for model in models:
            if model.get('provider') == provider_name:
                self.models_tree.insert('', 'end', values=(
                    model.get('id', ''),
                    model.get('model_string', ''),
                    model.get('model_name', ''),
                    model.get('provider', '')
                ))

        # Enable add model button
        self.add_model_button.config(state='normal')

        # Load task assignments for current provider's models from self.ai_config
        task_assignments = self.ai_config.get('task_assignments', {})
        self.load_task_assignments(task_assignments, provider_name)

        """Loads task assignments into UI"""
        # Clear existing assignments
        for widget in self.assignments_frame.winfo_children():
            if not isinstance(widget, ttk.Button):  # Keep the "Save" button
                widget.destroy()

        # Get available models - only from currently displayed provider
        model_options = []
        model_id_map = {}  # Map display names back to IDs

        for item in self.models_tree.get_children():
            values = self.models_tree.item(item, 'values')
            model_id = values[0]
            model_name = values[2]
            model_provider = values[3]

            # Format: [Provider Name] Human Readable Model Name
            display_name = f"[{model_provider}] {model_name}"
            model_options.append(display_name)
            model_id_map[display_name] = model_id

        # Create assignment widgets
        ttk.Label(self.assignments_frame, text="Task Category").grid(row=0, column=0, sticky='w', padx=(0, 10))
        ttk.Label(self.assignments_frame, text="Assigned Model").grid(row=0, column=1, sticky='w')

        self.assignment_vars = {}
        self.model_id_map = model_id_map  # Store for later use
        row = 1
        task_categories = [
            'text_processing',
            'material_processing',
            'version_creation',
            'idea_generation',
            'fact_research',
            'script_writing'
        ]
        for task in task_categories:  # Basic task categories
            ttk.Label(self.assignments_frame, text=task).grid(row=row, column=0, sticky='w', pady=2)
            # Convert stored model ID back to display name
            current_model_id = assignments.get(task, '')
            current_display = ''
            for display, mid in model_id_map.items():
                if mid == current_model_id:
                    current_display = display
                    break

            var = tk.StringVar(value=current_display)
            self.assignment_vars[task] = var
            combo = ttk.Combobox(self.assignments_frame, textvariable=var, values=model_options, state='readonly', width=50)
            combo.grid(row=row, column=1, sticky='ew', pady=2)
            row += 1

        # Add single centered save button
        save_button_frame = ttk.Frame(self.assignments_frame)
        save_button_frame.grid(row=row, column=0, columnspan=2, pady=10)
        ttk.Button(save_button_frame, text="Save Assignments", command=self._save_task_assignments).pack()

        """Adds a new provider"""
        dialog = ProviderDialog(self, "Add Provider")
        if dialog.result:
            name, url = dialog.result
            self.ai_config['providers'][name] = {'name': name, 'base_url': url}
            # Pass new provider name for selection
            self._commit_ai_config(select_provider_name=name)

        """Edits selected provider"""
        selected = self.providers_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Select a provider to edit")
            return
        item = selected[0]
        values = self.providers_tree.item(item, 'values')
        old_name = values[0]
        dialog = ProviderDialog(self, "Edit Provider", values)
        if dialog.result:
            name, url = dialog.result
            if name != old_name:
                del self.ai_config['providers'][old_name]
            self.ai_config['providers'][name] = {'name': name, 'base_url': url}
            # Pass provider name for selection
            self._commit_ai_config(select_provider_name=name)

        """Deletes selected provider"""
        selected = self.providers_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Select a provider to delete")
            return
        if messagebox.askyesno("Confirmation", "Delete selected provider?"):
            provider_name = self.providers_tree.item(selected[0], 'values')[0]
            if provider_name in self.ai_config['providers']:
                del self.ai_config['providers'][provider_name]
            self.ai_config['models'] = [m for m in self.ai_config['models'] if m.get('provider') != provider_name]
            self._commit_ai_config()

        """Adds new model"""
        # Get currently selected provider
        selected_provider_items = self.providers_tree.selection()
        if not selected_provider_items:
            messagebox.showwarning("Warning", "Select a provider before adding a model")
            return

        provider_name = self.providers_tree.item(selected_provider_items[0], 'values')[0]

        # Create dialog with pre-filled provider
        dialog = ModelDialog(self, "Add Model", provider_name=provider_name)
        if dialog.result:
            id_val, string, name, provider = dialog.result
            self.ai_config['models'].append({
                'id': id_val,
                'model_string': string,
                'model_name': name,
                'provider': provider
            })
            self._commit_ai_config()

        """Edits selected model"""
        selected = self.models_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Select a model to edit")
            return
        item = selected[0]
        values = self.models_tree.item(item, 'values')
        dialog = ModelDialog(self, "Edit Model", values)
        if dialog.result:
            id_val, string, name, provider = dialog.result
            # Check if provider changed (shouldn't happen with read-only field, but safety check)
            if provider != values[3]:
                messagebox.showerror("Error", "Cannot change model provider")
                return
            for model in self.ai_config['models']:
                if model['id'] == values[0]:  # Use old ID for searching
                    model.update({
                        'id': id_val, 'model_string': string, 'model_name': name, 'provider': provider
                    })
                    break
            self._commit_ai_config()

        """Deletes selected model"""
        selected = self.models_tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Select a model to delete")
            return
        if messagebox.askyesno("Confirmation", "Delete selected model?"):
            model_id = self.models_tree.item(selected[0], 'values')[0]
            self.ai_config['models'] = [m for m in self.ai_config['models'] if m['id'] != model_id]
            self._commit_ai_config()

    def _save_task_assignments(self):
        """Saves task assignments"""
        # --- START OF CHANGES: Update assignments from UI before saving ---

        # 1. Create dictionary for updated assignments
        updated_assignments = {}
        # Check that interface variables exist
        if hasattr(self, 'assignment_vars'):
            for task, var in self.assignment_vars.items():
                # 2. Get the name selected in the dropdown (e.g., "[Google] Gemini")
                display_name = var.get()
                if display_name and hasattr(self, 'model_id_map'):
                    # 3. Convert this name back to model ID (e.g., "google-gemini-2.0-flash-lite")
                    model_id = self.model_id_map.get(display_name)
                    if model_id:
                        updated_assignments[task] = model_id

        # 4. Update our main settings object in memory
        self.ai_config['task_assignments'] = updated_assignments

        # --- END OF CHANGES ---

        # Save changes via centralized method
        self._commit_ai_config()

        messagebox.showinfo("Success", "Task assignments saved")

    def _commit_ai_config(self, select_provider_name=None):
        """
        Saves the current state of self.ai_config, reloads the config
        from the controller, and updates the UI, preserving or setting the user's selection.
        """
        # 1. Determine which provider should be selected after reload.
        # Priority goes to the explicitly passed name (when adding/editing).
        provider_to_select = select_provider_name

        # If no name was passed, try to preserve current selection.
        if not provider_to_select:
            if selected_items := self.providers_tree.selection():
                provider_to_select = self.providers_tree.item(selected_items[0], 'values')[0]

        # 2. Save changes via controller.
        self.app.update_ai_settings_and_save(self.ai_config)

        # 3. Completely reload the configuration.
        self.load_ai_config()

        # 4. Update API key list (for new/deleted providers)
        if hasattr(self, 'api_key_widgets'):
            self._refresh_api_keys()

        # 5. Restore or set selection.
        if provider_to_select:
            for item_id in self.providers_tree.get_children():
                if self.providers_tree.item(item_id, 'values')[0] == provider_to_select:
                    self.providers_tree.selection_set(item_id)
                    self.providers_tree.focus(item_id)
                    self._on_provider_selected(None)
                    break


class ProviderDialog:
    """Dialog for adding/editing a provider"""
    def __init__(self, parent, title, values=None):
        self.result = None
        dialog = tk.Toplevel(parent)
        dialog.title(title)
        dialog.geometry("400x150")
        dialog.transient(parent)
        dialog.grab_set()

        ttk.Label(dialog, text="Provider Name:").grid(row=0, column=0, sticky='w', padx=10, pady=5)
        self.name_var = tk.StringVar(value=values[0] if values else "")
        ttk.Entry(dialog, textvariable=self.name_var).grid(row=0, column=1, sticky='ew', padx=10, pady=5)

        ttk.Label(dialog, text="Base URL:").grid(row=1, column=0, sticky='w', padx=10, pady=5)
        self.url_var = tk.StringVar(value=values[1] if values else "")
        ttk.Entry(dialog, textvariable=self.url_var).grid(row=1, column=1, sticky='ew', padx=10, pady=5)

        def save():
            if self.name_var.get() and self.url_var.get():
                self.result = (self.name_var.get(), self.url_var.get())
                dialog.destroy()
            else:
                messagebox.showwarning("Warning", "Fill in all fields")

        ttk.Button(dialog, text="Save", command=save).grid(row=2, column=0, columnspan=2, pady=10)
        dialog.columnconfigure(1, weight=1)
        dialog.wait_window(dialog)


class ModelDialog:
    """Dialog for adding/editing a model"""
    def __init__(self, parent, title, values=None, provider_name=None):
        self.result = None
        self.dialog = tk.Toplevel(parent)
        self.dialog.title(title)
        self.dialog.geometry("500x200")
        self.dialog.transient(parent)
        self.dialog.grab_set()

        # Store variables as instance attributes to prevent garbage collection
        self.id_var = tk.StringVar(value=values[0] if values else "")
        self.string_var = tk.StringVar(value=values[1] if values else "")
        self.name_var = tk.StringVar(value=values[2] if values else "")

        # For adding new model, use provided provider_name; for editing, use values[3]
        if provider_name is not None:
            # Adding new model - provider is pre-filled and read-only
            self.provider_var = tk.StringVar(value=provider_name)
            self.provider_readonly = True
        else:
            # Editing existing model - provider is read-only
            self.provider_var = tk.StringVar(value=values[3] if values else "")
            self.provider_readonly = True

        ttk.Label(self.dialog, text="Model ID:").grid(row=0, column=0, sticky='w', padx=10, pady=5)
        self.id_entry = ttk.Entry(self.dialog, textvariable=self.id_var)
        self.id_entry.grid(row=0, column=1, sticky='ew', padx=10, pady=5)

        ttk.Label(self.dialog, text="API String:").grid(row=1, column=0, sticky='w', padx=10, pady=5)
        self.string_entry = ttk.Entry(self.dialog, textvariable=self.string_var)
        self.string_entry.grid(row=1, column=1, sticky='ew', padx=10, pady=5)

        ttk.Label(self.dialog, text="Model Name:").grid(row=2, column=0, sticky='w', padx=10, pady=5)
        self.name_entry = ttk.Entry(self.dialog, textvariable=self.name_var)
        self.name_entry.grid(row=2, column=1, sticky='ew', padx=10, pady=5)

        ttk.Label(self.dialog, text="Provider:").grid(row=3, column=0, sticky='w', padx=10, pady=5)
        if self.provider_readonly:
            self.provider_entry = ttk.Entry(self.dialog, textvariable=self.provider_var, state='readonly')
        else:
            self.provider_entry = ttk.Entry(self.dialog, textvariable=self.provider_var)
        self.provider_entry.grid(row=3, column=1, sticky='ew', padx=10, pady=5)

        ttk.Button(self.dialog, text="Save", command=self.save).grid(row=4, column=0, columnspan=2, pady=10)
        self.dialog.columnconfigure(1, weight=1)

        # Set focus to first entry
        self.id_entry.focus_set()

        # Bind Enter key to save
        self.dialog.bind('<Return>', lambda e: self.save())
        self.dialog.wait_window(self.dialog)

    def save(self):
        if all([self.id_var.get(), self.string_var.get(), self.name_var.get(), self.provider_var.get()]):
            self.result = (self.id_var.get(), self.string_var.get(), self.name_var.get(), self.provider_var.get())
            self.dialog.destroy()
        else:
            messagebox.showwarning("Warning", "Fill in all fields")