![Python](https://img.shields.io/badge/python-3.10%2B-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Build](https://github.com/time-scout/AI-Driven-Video-Production-Pipeline/actions/workflows/python-tests.yml/badge.svg)

# AI-Driven Video Production Pipeline

A desktop application for automated, high-volume production of research-backed video content for social media platforms (YouTube Shorts, Reels, TikTok).

Built in Python with Tkinter. Runs on macOS (optimized for Apple Silicon M1/M2/M3 with hardware-accelerated video rendering via `h264_videotoolbox`).

---

## What it does

This pipeline automates the full lifecycle of content production — from a raw list of ideas to a finished, publish-ready video file with AI voiceover, synchronized visuals, and generated metadata.

**Input:** A list of items (e.g., "Top 13 songs from 1994"), a narrative archetype, and a stylistic direction.

**Output:** A finished HD video file + a complete publication package (title, description, tags).

---

## Core Modules

| Module | Responsibility |
|---|---|
| `AI_Manager.py` | Central dispatcher — routes tasks to configured AI providers (Google, OpenRouter, etc.) |
| `script_creation_worker.py` | Research agent, entity recognizer, brainstormer, script synthesizer |
| `montage_processor_2.py` | Core video assembly engine (1600+ lines, multi-threaded, ffmpeg-based) |
| `interview_semantic_slicer.py` | AI-powered interview slicer using Whisper + WhisperX + Pyannote |
| `video_song_sandwich_worker.py` | Composite video builder — merges audio source with looped/trimmed video |
| `version_creation_worker.py` | Creates multiple video versions with different narrations or visual sequences |
| `text_splitter_worker.py` | Splits long scripts into timed, voiceover-ready fragments |
| `entity_manager_v2.py` | Filesystem-level entity (artist/song) registry with EID/SSID system |
| `download_orchestrator_v2.py` | Multi-threaded asset downloader via yt-dlp |
| `publication_planner_logic.py` | Publication queue manager and metadata generator |

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> Key dependencies: `ffmpeg-python`, `yt-dlp`, `openai-whisper`, `whisperx`, `pyannote.audio`, `google-generativeai`, `openpyxl`, `pydub`, `fuzzywuzzy`, `tomli`, `tomli-w`

### 3. Configure secrets

Copy the secrets template and fill in your API keys:

```bash
cp .centralized_montage/secrets.toml.example .centralized_montage/secrets.toml
```

Then edit `secrets.toml`:

```toml
GOOGLE_API_KEY = "your-google-api-key"
HF_TOKEN = "your-huggingface-token"
OPENROUTER_API_KEY = "your-openrouter-api-key"
```

### 4. Set your Work Root

On first launch, go to the **Settings** tab and specify your `Work Root` directory — the folder where your `database/`, `common_assets/`, and media archive live.

### 5. Run

```bash
python __Main_Interface.py
```

---

## Architecture

```
__Main_Interface.py          ← Main window, tab orchestration
├── AI_Manager               ← Provider-agnostic AI router
├── ScriptCreationTab        ← Research → Brainstorm → Write → Export
├── ProcessingTab            ← Download → Slice → Zoom → Parse
├── ProjectCreateAndVoiceTab ← Voiceover synthesis
├── MontageTab2              ← Assembly queue + rendering
├── PublicationTab           ← Pre-publication review
└── PublicationPlannerTab    ← Scheduling & metadata generation
```

AI provider routing is fully configurable via `database/config.json` — swap models per task category without touching code.

---

## Security

- All API keys are stored in `.centralized_montage/secrets.toml` — excluded from git via `.gitignore`
- `work_root_pointer.json` (machine-local path) is also excluded
- No credentials, usernames, or personal paths are hardcoded in the source

---

## License

MIT License. See [LICENSE](LICENSE).
