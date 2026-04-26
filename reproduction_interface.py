# reproduction_interface.py

import tkinter as tk
from tkinter import ttk

from import_interface import ImportTab
from text_processing_interface import TextProcessingTab
from version_creation_interface import VersionCreationTab


class ReproductionTab(ttk.Frame):
    """'Reproduction' tab with three sub-tabs:
    - Import
    - Text Processing
    - Create Version
    """

    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

        # Create notebook for sub-tabs
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill='both', expand=True)

        # Create three sub-tabs
        self.import_tab = ImportTab(self.notebook, controller)
        self.text_processing_tab = TextProcessingTab(self.notebook, controller)
        self.version_creation_tab = VersionCreationTab(self.notebook, controller)

        # Add sub-tabs to notebook
        self.notebook.add(self.import_tab, text="Import")
        self.notebook.add(self.text_processing_tab, text="Text Processing")
        self.notebook.add(self.version_creation_tab, text="Create Version")
