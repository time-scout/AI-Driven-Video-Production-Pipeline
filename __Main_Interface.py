
# __Main_Interface.py (Final Reference Version)

import tkinter as tk
from tkinter import ttk
import datetime
from pathlib import Path
import json
import traceback
import sys
import threading

# Tab Imports
from process_visuals_interface import ProcessingTab
from project_create_and_voice_interface import ProjectCreateAndVoiceTab
from montage_interface_2 import MontageTab2
from publication_interface import PublicationTab
from version_creation_interface import VersionCreationTab
from publication_planner_interface import PublicationPlannerTab
from objects_interface import ObjectsTab
from import_interface import ImportTab
from text_processing_interface import TextProcessingTab
from app_settings_interface import AppSettingsTab
from reproduction_interface import ReproductionTab
from script_creation_interface import ScriptCreationTab

# Import AI Manager
from AI_Manager import AIManager


class GenFeatApp:
    def __init__(self, root):
        self.root = root
        self.root.title("GEN-FEAT")
        self.root.geometry("1400x900")
        self.root.minsize(800, 700)

        # --- Stage 1: Basic Variable Initialization ---
        self.TOML_AVAILABLE = self._check_toml_available()
        self.DATABASE_DIR_NAME = "database"
        self.SECRETS_DIR_NAME = ".centralized_montage"
        self.CONFIG_FILE_NAME = "config.json"
        self.POINTER_FILE_NAME = "work_root_pointer.json"
        self.SECRET_KEYS = ['GOOGLE_API_KEY', 'HF_TOKEN', 'GOOGLER_API_KEY', 'OPENROUTER_API_KEY']

        # Add AI Manager
        self.ai_manager = None
        self.settings = {}
        self.secrets = {}

        # --- Stage 2: Loading Settings ---
        self.settings = self._load_settings()

        # --- Stage 3: AI Manager Initialization ---
        self._initialize_ai_manager()

        # --- Stage 4: Path Resolution ---
        # First determine the path to secrets. It does NOT depend on settings.
        self.SECRETS_PATH = Path(__file__).parent / self.SECRETS_DIR_NAME / 'secrets.toml'

        # Now determine paths that depend on settings.
        work_root_str = self.get_setting('work_root_path', "")
        if work_root_str and Path(work_root_str).is_dir():
            work_root_path = Path(work_root_str)
            self.WORK_ROOT_PATH = work_root_path
            self.DATABASE_PATH = work_root_path / self.DATABASE_DIR_NAME
            self.CONFIG_FILE_PATH = self.DATABASE_PATH / self.CONFIG_FILE_NAME
            self.COMMON_ASSETS_PATH = work_root_path / 'common_assets'
            self.PROMPTS_PATH = self.COMMON_ASSETS_PATH / 'prompts'
            self.SOUND_ALERTS_PATH = self.COMMON_ASSETS_PATH / 'sound_alerts'
        else:
            self.WORK_ROOT_PATH, self.DATABASE_PATH, self.CONFIG_FILE_PATH, self.COMMON_ASSETS_PATH, \
            self.PROMPTS_PATH, self.SOUND_ALERTS_PATH = [None] * 6

        # --- Stage 5: Loading Secrets ---
        self.secrets = self._load_secrets()
        
        # --- Stage 6: Resource Initialization ---
        if self.DATABASE_PATH:
            self.CACHE_FILE_PATH = self.DATABASE_PATH / 'archive_cache.json'
            self.LOG_FILES = {
                'photo_dl': self.DATABASE_PATH / 'log_photo_download.txt',
                'video_dl': self.DATABASE_PATH / 'log_video_download.txt',
                'zoom': self.DATABASE_PATH / 'log_zoom_creation.txt',
                'slicer': self.DATABASE_PATH / 'log_video_slicing.txt'
            }
        else:
            self.CACHE_FILE_PATH, self.LOG_FILES = None, {}
            
        self._initialize_log_files()

        self.entity_details_cache, self.entity_stats_cache = {}, {}

        self._center_window()

        # --- UI Construction ---
        main_frame = ttk.Frame(root)
        main_frame.pack(padx=10, pady=10, fill='both', expand=True)
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill='both', expand=True, side="top")

        self._create_tabs()

    def _check_toml_available(self):
        try:
            import tomli, tomli_w
            return True
        except ImportError:
            return False

    def _center_window(self):
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
        
    def _load_settings(self):
        default_settings = {
            'work_root_path': "",
            'media_archive_path': "",
            'ai_settings': {
                "providers": {},
                "models": [],
                "task_assignments": {}
            }
        }
        pointer_path = Path(__file__).parent / self.POINTER_FILE_NAME

        work_root_from_pointer = ""
        if pointer_path.exists():
            try:
                with open(pointer_path, 'r', encoding='utf-8') as f:
                    pointer_data = json.load(f)
                    work_root_from_pointer = pointer_data.get('work_root_path', "")
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Could not read pointer file at {pointer_path}")

        if not work_root_from_pointer or not Path(work_root_from_pointer).is_dir():
            return default_settings

        main_config_path = Path(work_root_from_pointer) / self.DATABASE_DIR_NAME / self.CONFIG_FILE_NAME

        if not main_config_path.exists():
            loaded_settings = default_settings.copy()
            loaded_settings['work_root_path'] = work_root_from_pointer
            return loaded_settings

        try:
            with open(main_config_path, 'r', encoding='utf-8') as f:
                loaded_settings = json.load(f)
            for key, value in default_settings.items():
                loaded_settings.setdefault(key, value)
            loaded_settings['work_root_path'] = work_root_from_pointer
            return loaded_settings
        except (json.JSONDecodeError, IOError):
            print(f"Warning: Could not read main config file at {main_config_path}")
            final_settings = default_settings.copy()
            final_settings['work_root_path'] = work_root_from_pointer
            return final_settings

    def _save_settings(self):
        # Dynamically determine the path for saving
        work_root_str = self.settings.get('work_root_path', "")
        if not work_root_str or not Path(work_root_str).is_dir():
            # Cannot save if root path is not set or incorrect
            return

        config_to_save_path = Path(work_root_str) / self.DATABASE_DIR_NAME / self.CONFIG_FILE_NAME

        try:
            config_to_save_path.parent.mkdir(parents=True, exist_ok=True)
            
            settings_to_save = self.settings.copy()
            if 'work_root_path' in settings_to_save:
                del settings_to_save['work_root_path'] # Do not store work_root_path in main config

            with open(config_to_save_path, 'w', encoding='utf-8') as f:
                json.dump(settings_to_save, f, indent=4, ensure_ascii=False)
        except IOError as e:
            print(f"Error saving configuration file: {e}")

    def _save_pointer(self):
        pointer_path = Path(__file__).parent / self.POINTER_FILE_NAME
        work_root_to_save = self.settings.get('work_root_path', "")
        try:
            with open(pointer_path, 'w', encoding='utf-8') as f:
                json.dump({'work_root_path': work_root_to_save}, f, indent=4)
        except IOError as e:
            print(f"Error saving pointer file: {e}")

    def _load_secrets(self):
        if not self.SECRETS_PATH or not self.SECRETS_PATH.exists():
            return {}
        try:
            import tomli
            with open(self.SECRETS_PATH, 'rb') as f:
                return tomli.load(f)
        except Exception as e:
            print(f"Error loading secrets: {e}")
            return {}

    def _save_secrets(self):
        if not self.TOML_AVAILABLE or not self.SECRETS_PATH:
            return
        try:
            self.SECRETS_PATH.parent.mkdir(parents=True, exist_ok=True)
            import tomli_w
            with open(self.SECRETS_PATH, 'wb') as f:
                tomli_w.dump(self.secrets, f)
        except IOError as e:
            print(f"Error saving secrets: {e}")

    def get_setting(self, key: str, default=None):
        # Check if the key is a secret
        is_secret = (key in self.SECRET_KEYS or
                    key.endswith('_API_KEY') or
                    key.endswith('_TOKEN'))

        if is_secret:
            return self.secrets.get(key, default)
        return self.settings.get(key, default)

    def set_setting(self, key: str, value):
        # Check if the key is a secret
        is_secret = (key in self.SECRET_KEYS or
                    key.endswith('_API_KEY') or
                    key.endswith('_TOKEN'))

        if is_secret:
            self.secrets[key] = value
            self._save_secrets()
            return

        self.settings[key] = value

        if key == 'work_root_path':
            self._save_pointer()
            print("INFO: Work root path changed. Please restart the application to apply changes.")
        else:
            self._save_settings()

    def _initialize_ai_manager(self):
        """AI Manager Initialization"""
        try:
            # Get configuration paths
            config_path = self._get_config_path()
            secrets_path = self._get_secrets_path()

            if config_path and secrets_path:
                self.ai_manager = AIManager(config_path, secrets_path)
                print("AI Manager initialized successfully")
            else:
                print("AI Manager not initialized: missing config or secrets path")
                self.ai_manager = None
        except Exception as e:
            print(f"Error initializing AI Manager: {e}")
            self.ai_manager = None

    def _get_config_path(self):
        """Get config.json path"""
        work_root_str = self.get_setting('work_root_path', "")
        if work_root_str and Path(work_root_str).is_dir():
            work_root_path = Path(work_root_str)
            return work_root_path / self.DATABASE_DIR_NAME / self.CONFIG_FILE_NAME
        return None

    def _get_secrets_path(self):
        """Get secrets.toml path"""
        return Path(__file__).parent / self.SECRETS_DIR_NAME / 'secrets.toml'

    def update_ai_settings_and_save(self, new_ai_settings: dict):
        """Safely updates ai_settings and saves the entire config."""
        # Create a copy to avoid modification during iteration
        self.settings['ai_settings'] = new_ai_settings.copy()
        self._save_settings()

        # Reload AI Manager if initialized
        if hasattr(self, 'ai_manager') and self.ai_manager:
            try:
                config_path = self._get_config_path()
                secrets_path = self._get_secrets_path()
                if config_path and secrets_path:
                    self.ai_manager = AIManager(config_path, secrets_path)
            except Exception as e:
                print(f"Error reloading AI Manager: {e}")

    def _create_tabs(self):
        tabs_to_create = [
            (AppSettingsTab, "Settings", "settings_tab"),
            (ReproductionTab, "Reproduction", "reproduction_tab"),
            (ScriptCreationTab, "Script Creation", "script_creation_tab"),
            (ProcessingTab, "Material Processing", "processing_tab"),
            (ProjectCreateAndVoiceTab, "Voiceover", "synthesis_tab"),
            (ObjectsTab, "Objects", "objects_tab"),
            (MontageTab2, "Montage", "montage_tab"),
            (PublicationTab, "Pre-Publication", "publication_tab"),
            (PublicationPlannerTab, "Publisher", "planner_tab")
        ]

        for tab_class, tab_text, attr_name in tabs_to_create:
            try:
                # Pass AI Manager to settings if AppSettingsTab
                if tab_class == AppSettingsTab:
                    tab_instance = tab_class(self.notebook, self)
                    # Set AI Manager reference
                    if hasattr(self, 'ai_manager') and self.ai_manager:
                        tab_instance.ai_manager = self.ai_manager
                else:
                    tab_instance = tab_class(self.notebook, self)

                setattr(self, attr_name, tab_instance)
                self.notebook.add(tab_instance, text=tab_text)
            except Exception:
                print(f"CRITICAL: Failed to initialize tab '{tab_text}'.")
                traceback.print_exc()

    # --- Helper Methods ---
    def _initialize_log_files(self):
        if not self.DATABASE_PATH: return
        self.DATABASE_PATH.mkdir(parents=True, exist_ok=True)
        for log_file_path in self.LOG_FILES.values():
            try:
                with open(log_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"--- Session Log from {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")
            except IOError as e:
                print(f"Error initializing log file {log_file_path}: {e}")
    
    def write_to_log_file(self, log_key, message):
        if log_key not in self.LOG_FILES: return
        log_file_path = self.LOG_FILES[log_key]
        try:
            with open(log_file_path, 'a', encoding='utf-8') as f:
                clean_message = ''.join(c for c in message if c.isprintable() or c in '\n\r\t')
                f.write(f"{datetime.datetime.now().strftime('%H:%M:%S')} | {clean_message.strip()}\n")
        except IOError as e:
            print(f"Error writing to log file {log_file_path}: {e}")

    def get_full_entity_data(self, eid):
        details = self.entity_details_cache.get(eid, {})
        stats = self.entity_stats_cache.get(eid, {})
        regular_clips = stats.get('clips_total', 0) - stats.get('clips_highlights', 0)
        regular_zooms = stats.get('zooms_total', 0) - stats.get('zooms_highlights', 0)
        file_stats = {
            'h_clip': stats.get('clips_highlights', 0),
            'clip': regular_clips,
            'h_zoom': stats.get('zooms_highlights', 0),
            'zoom': regular_zooms,
            'raw_video': stats.get('raw_videos_total', 0),
            'unchecked_photos': stats.get('unchecked_photos_total', 0),
            'photos': stats.get('photos_total', 0)
        }
        return {'name': details.get('name', '(name not found)'), 'eid': eid,
                'role': details.get('role', '(role not found)'),
                'projects': details.get('projects', []), 'stats': file_stats}


def handle_thread_exception(args):
    """Global handler for catching and outputting errors from background threads."""
    print("="*80, file=sys.stderr)
    print("!!!!!!   CRITICAL ERROR IN BACKGROUND THREAD   !!!!!!", file=sys.stderr)
    print("="*80, file=sys.stderr)
    traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback, file=sys.stderr)
    print("="*80, file=sys.stderr)


if __name__ == '__main__':
    try:
        threading.excepthook = handle_thread_exception
        root = tk.Tk()
        app = GenFeatApp(root)
        root.mainloop()
    except Exception as e:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!!!!!   CRITICAL ERROR CAUGHT   !!!!!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        traceback.print_exc(file=sys.stdout)
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        input("--- Press ENTER to complete ---")