# --- import_text_creator_worker.py ---
# Worker for creating files from user's own text

import json
import re
from datetime import datetime
from pathlib import Path


def _get_next_pid(target_dir: Path) -> str:
    """Generates the next available PID."""
    target_dir.mkdir(parents=True, exist_ok=True)
    pids = [int(m.group(1)) for f in target_dir.glob('PID????_*.json') if (m := re.search(r'PID(\d{4})', f.name))]
    return f"{(max(pids) if pids else 0) + 1:04d}"


def _calculate_text_statistics(text: str) -> dict:
    """
    Calculates text statistics.

    Uses an average speed of 150 words/min for reading time estimation.
    """
    char_count = len(text)
    word_count = len(text.split())

    # Reading time estimation (150 words per minute - average speed)
    estimated_minutes = word_count / 150 if word_count > 0 else 0

    # Calculate performance statistics
    words_per_minute = round(word_count / estimated_minutes, 2) if estimated_minutes > 0 else 0
    chars_per_minute = round(char_count / estimated_minutes, 2) if estimated_minutes > 0 else 0
    minutes_per_1k_chars = round(estimated_minutes / (char_count / 1000), 2) if char_count > 0 else 0
    minutes_per_1k_words = round(estimated_minutes / (word_count / 1000), 2) if word_count > 0 else 0

    # Format duration as HH:MM:SS
    total_seconds = int(estimated_minutes * 60)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"

    return {
        "char_count_with_spaces": char_count,
        "word_count": word_count,
        "duration_str": duration_str,
        "estimated_minutes": round(estimated_minutes, 2),
        "words_per_minute": words_per_minute,
        "chars_per_minute": chars_per_minute,
        "minutes_per_1k_chars": minutes_per_1k_chars,
        "minutes_per_1k_words": minutes_per_1k_words
    }


def _extract_title_from_text(text: str) -> str:
    """
    Extracts title from text.
    Searches for the first non-empty line and uses it as a title.
    """
    lines = text.strip().split('\n')
    for line in lines:
        title_candidate = line.strip()
        if len(title_candidate) > 5 and len(title_candidate) < 100:
            # Truncate overly long titles
            return title_candidate[:100]
    return "Custom Text"


def _generate_json_data(pid: str, text: str, stats: dict) -> dict:
    """Generates JSON structure for custom text."""
    title = _extract_title_from_text(text)

    # Take first line as description (up to 500 characters)
    lines = text.strip().split('\n')
    description_lines = []
    for line in lines[:5]:  # First 5 lines
        cleaned = line.strip()
        if cleaned and len(cleaned) > 10:
            description_lines.append(cleaned)
    description = ' '.join(description_lines)[:500]

    return {
        "PID": pid,
        "url": "own text",
        "title": title,
        "description": description,
        "channel": "",
        "channel_id": "",
        "video_id": "",
        "upload_date": datetime.now().strftime('%Y-%m-%d'),
        "duration": stats["duration_str"],
        "view_count": 0,
        "thumbnail_url": "",
        "thumbnail_width": 0,
        "thumbnail_height": 0,
        "char_count_with_spaces": stats["char_count_with_spaces"],
        "word_count": stats["word_count"],
        "words_per_minute": stats["words_per_minute"],
        "chars_per_minute": stats["chars_per_minute"],
        "minutes_per_1k_chars": stats["minutes_per_1k_chars"],
        "minutes_per_1k_words": stats["minutes_per_1k_words"],
        "source_type": "user_provided_text",
        "estimated_reading_time_minutes": stats["estimated_minutes"]
    }


def process_text_creation(text_content: str, target_dir: Path, user_pid: str | None,
                          log_callback: callable, status_callback: callable):
    """
    Main function for creating files from user text.

    Args:
        text_content: User provided text
        target_dir: Directory for saving (parsed_data)
        user_pid: Project ID from user (optional)
        log_callback: Callback for logging
        status_callback: Callback for status updates
    """
    try:
        status_callback("Generating PID...")
        pid = user_pid if user_pid else _get_next_pid(target_dir)

        log_callback(f"Using PID: {pid}")

        status_callback("Calculating statistics...")
        stats = _calculate_text_statistics(text_content)

        log_callback(f"-> Statistics:")
        log_callback(f"   Characters: {stats['char_count_with_spaces']}")
        log_callback(f"   Words: {stats['word_count']}")
        log_callback(f"   Estimated reading time: {stats['duration_str']} ({stats['estimated_minutes']} min)")

        status_callback("Preparing JSON...")
        json_data = _generate_json_data(pid, text_content, stats)

        status_callback("Saving files...")
        txt_filename = f"PID{pid}_transcript.txt"
        json_filename = f"PID{pid}_transcript.json"
        txt_filepath = target_dir / txt_filename
        json_filepath = target_dir / json_filename

        # Save .txt file
        txt_filepath.write_text(text_content, encoding='utf-8')
        log_callback(f"-> Transcript saved: {txt_filepath.name}")

        # Save .json file
        json_filepath.write_text(json.dumps(json_data, ensure_ascii=False, indent=4), encoding='utf-8')
        log_callback(f"-> JSON saved: {json_filepath.name}")

        log_callback(f"SUCCESS: PID {pid}: All files created.")
        status_callback("Ready!")

    except Exception as e:
        error_msg = f"ERROR creating files: {str(e)}"
        log_callback(error_msg, is_error=True)
        status_callback("Error")
        import traceback
        log_callback(f"Traceback: {traceback.format_exc()}", is_error=True)
