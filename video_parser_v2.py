# video_parser_v2.py

import subprocess
from pathlib import Path
from typing import Callable, List, Dict, Any
import re
import json
import datetime
import shutil


def get_video_id(url: str, progress_callback: Callable[[str], None]) -> str:
    """Extracts video ID from URL using yt-dlp."""
    try:
        command = ["yt-dlp", "--get-id", "--no-playlist", url]
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
        )
        return result.stdout.strip()
    except FileNotFoundError:
        progress_callback(
            "❌ CRITICAL ERROR: `yt-dlp` not found. Ensure it is installed and available in system PATH.")
        return ""
    except subprocess.CalledProcessError as e:
        progress_callback(f"⚠️ Failed to get ID for URL {url}: {e.stderr.strip()}")
        return ""


def download_video(video_url: str, output_path: Path, progress_callback: Callable[[str], None]) -> bool:
    """
    Downloads a single video by URL using yt-dlp, with a timeout.
    After downloading, creates a "Golden Master" using ffmpeg.
    Returns True on success, False on error.
    """
    # Download to temporary file (use temp_ prefix instead of .tmp suffix)
    # This avoids the issue where yt-dlp creates filename.mp4.tmp.mp4
    temp_output_path = output_path.parent / f"temp_{output_path.name}"

    # --- CHANGE: Added --no-playlist ---
    command = [
        "yt-dlp",
        "--quiet",
        "--no-warnings",
        "--no-playlist",  # Ignore playlists, download single video only
        "-f", "bv[height<=1080]+ba/b[height<=1080]",
        "--merge-output-format", "mp4",
        "-o", str(temp_output_path),
        video_url
    ]

    try:
        # --- CHANGE: Added timeout (30 minutes) ---
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=1800,  # 30 minutes for download
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
        )
        progress_callback(f"✅ Successfully downloaded: {output_path.name}")

        # Creating "Golden Master" using ffmpeg
        progress_callback(f"  -> Converting to Golden Master...")
        ffmpeg_command = [
            "ffmpeg",
            "-y",  # Overwrite if exists
            "-i", str(temp_output_path),  # Input file
            # Video parameters
            "-c:v", "h264_videotoolbox",
            "-b:v", "3000k",
            "-pix_fmt", "yuv420p",
            "-r", "25",
            # Geometry
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1",
            # Audio parameters
            "-c:a", "aac",
            "-ar", "48000",
            "-ac", "2",
            # Volume (loudnorm)
            "-af", "loudnorm=i=-16:tp=-1.5",
            # Compatibility
            "-movflags", "+faststart",
            str(output_path)  # Output file
        ]

        subprocess.run(
            ffmpeg_command,
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8',
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
        )

        # Remove temporary file after successful conversion
        temp_output_path.unlink()
        progress_callback(f"  -> Golden Master created: {output_path.name}")
        return True
    except subprocess.TimeoutExpired:
        progress_callback(f"❌ Error: Downloading {video_url} took more than 30 minutes and was aborted.")
        if temp_output_path.exists():
            temp_output_path.unlink()
        return False
    except FileNotFoundError:
        progress_callback("❌ CRITICAL ERROR: `yt-dlp` or `ffmpeg` not found.")
        if temp_output_path.exists():
            temp_output_path.unlink()
        return False
    except subprocess.CalledProcessError as e:
        error_message = e.stderr.strip() if e.stderr else str(e)
        progress_callback(f"❌ Error during download/conversion of {video_url}. Reason: {error_message}")
        if temp_output_path.exists():
            temp_output_path.unlink()
        return False
    except Exception as e:
        progress_callback(f"❌ Unknown error while downloading {video_url}: {e}")
        if temp_output_path.exists():
            temp_output_path.unlink()
        return False


def _fetch_single_video_metadata_sync(url: str, progress_callback: Callable[[str], None], timeout_seconds: int = 60) -> Dict[str, Any]:
    """
    Synchronously retrieves metadata for a single video with a timeout.
    """
    # --- START OF DIAGNOSTIC CODE ---
    progress_callback(f"DEBUG: Searching for yt-dlp. Path found: {shutil.which('yt-dlp')}")
    # --- END OF DIAGNOSTIC CODE ---
    command = ["yt-dlp", "-j", "--no-warnings", "--no-playlist", url]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding='utf-8',
            timeout=timeout_seconds,
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
        )

        try:
            metadata = json.loads(result.stdout)
            return {
                'url': url,
                'title': metadata.get('title', 'No Title'),
                'duration': metadata.get('duration', 0),
                'error': None
            }
        except json.JSONDecodeError:
            return {'url': url, 'title': 'N/A', 'duration': 0, 'error': 'Error parsing JSON response from yt-dlp'}

    except subprocess.TimeoutExpired as e:
        error_msg = f"Timeout ({timeout_seconds} sec) during request to yt-dlp. Process hung."
        if e.stdout: error_msg += f"\nStdout: {e.stdout}"
        if e.stderr: error_msg += f"\nStderr: {e.stderr}"
        return {'url': url, 'title': 'N/A', 'duration': 0, 'error': error_msg}

    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.strip()
        if "Video unavailable" in error_msg:
            error_clean = "Video unavailable"
        elif "Private video" in error_msg:
            error_clean = "Private video"
        else:
            error_clean = error_msg.splitlines()[-1] if error_msg.splitlines() else "Unknown yt-dlp error"
        return {'url': url, 'title': 'N/A', 'duration': 0, 'error': error_clean}

    except FileNotFoundError:
        return {'url': url, 'title': 'N/A', 'duration': 0, 'error': "'yt-dlp' command not found"}

    except Exception as e:
        return {'url': url, 'title': 'N/A', 'duration': 0, 'error': f"Unknown error in subprocess: {e}"}


def get_videos_metadata(urls: List[str], progress_callback: Callable[[str], None]) -> List[Dict[str, Any]]:
    """
    Retrieves metadata for a list of URLs synchronously, one by one.
    """
    results = []
    for i, url in enumerate(urls):
        # Pass progress_callback only for the first call to avoid cluttering the log
        cb = progress_callback if i == 0 else lambda msg: None
        metadata = _fetch_single_video_metadata_sync(url, progress_callback=cb)
        results.append(metadata)
    return results


def format_duration(seconds: int) -> str:
    """Converts seconds to human-readable format HH:MM:SS."""
    if seconds is None:
        return "N/A"
    return str(datetime.timedelta(seconds=int(seconds)))