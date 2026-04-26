"""
Composite Video Worker (Sandwich Worker)

Module for creating music videos where the audio track is taken from one source,
and the video track from another (or several), with automatic timing adjustment,
looping, and format standardization.
"""

import subprocess
import pathlib
import json
import shutil
import tempfile
import math
import os
import re
from typing import Dict, Callable, Optional, List, Tuple

# Imports for working with entity_manager
import entity_manager_v2

# ============================================================================
# Global video settings (macOS M1 Hardware Acceleration)
# ============================================================================
VIDEO_STANDARD = {
    'vcodec': 'h264_videotoolbox',
    'pix_fmt': 'yuv420p',
    'width': 1920,
    'height': 1080,
    'framerate': 25,
    'bitrate': '3000k'
}


def get_video_duration(file_path: pathlib.Path) -> float:
    """
    Get exact duration of video/audio file using ffprobe.

    Args:
        file_path: Path to file.

    Returns:
        Duration in seconds (float).
    """
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'json',
        str(file_path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
        data = json.loads(result.stdout)
        return float(data['format']['duration'])
    raise ValueError(f"Failed to get duration for file {file_path}")


def download_youtube_video(url: str, output_path: pathlib.Path, download_audio: bool = True,
                          progress_callback: Optional[Callable[[str], None]] = None,
                          stop_event: Optional[any] = None) -> Optional[pathlib.Path]:
    """
    Download video from YouTube using yt-dlp.

    Args:
        url: Video URL.
        output_path: Folder for saving.
        download_audio: If True - download audio only, else - video only.
        progress_callback: Function for logging.
        stop_event: Event for interruption.

    Returns:
        Path to downloaded file or None on error.
    """
    if progress_callback:
        progress_callback(f"📥 Downloading from YouTube: {url} (audio={download_audio})...")

    output_path.mkdir(parents=True, exist_ok=True)

    # Name template format
    template = str(output_path / '%(id)s.%(ext)s')

    if download_audio:
        # Download best audio
        format_spec = 'bestaudio/best'
        postprocessors = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'm4a',
        }]
        extra_args = ['--extract-audio', '--audio-format', 'm4a', '--audio-quality', '0']
    else:
        # Download best video without audio
        format_spec = 'bestvideo/best'
        postprocessors = []
        extra_args = []

    cmd = [
        'yt-dlp',
        '--no-playlist',
        '-f', format_spec,
        '-o', template,
        '--newline',
        '--no-warnings',
        '--socket-timeout', '30', # Connection timeout 30 seconds
    ] + extra_args + [url]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800,
                              check=False)

        if stop_event and stop_event.is_set():
            return None

        if result.returncode != 0:
            if progress_callback:
                progress_callback(f"❌ yt-dlp error for {url}: {result.stderr[:200]}")
            return None

        # Find downloaded file
        if download_audio:
            # Look for .m4a or .mp3 files
            extensions = ['.m4a', '.mp3', '.aac', '.opus']
        else:
            # Look for video files
            extensions = ['.mp4', '.mkv', '.webm', '.mov']

        for ext in extensions:
            matching_files = list(output_path.glob(f'*{ext}'))
            if matching_files:
                return matching_files[0]

        # If not found by extension, take last modified file
        files = list(output_path.iterdir())
        if files:
            files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return files[0]

        if progress_callback:
            progress_callback(f"❌ Downloaded file not found for {url}")
        return None

    except subprocess.TimeoutExpired:
        if progress_callback:
            progress_callback(f"❌ Download timeout for {url}")
        return None
    except Exception as e:
        if progress_callback:
            progress_callback(f"❌ Exception while downloading {url}: {e}")
        return None


def calculate_edit_list(audio_duration: float, video_durations: List[Tuple[pathlib.Path, float]],
                       progress_callback: Optional[Callable[[str], None]] = None) -> List[Dict]:
    """
    Calculate edit list for video assembly.

    Args:
        audio_duration: Audio duration (target).
        video_durations: List of tuples (video_path, duration).
        progress_callback: Function for logging.

    Returns:
        List of dictionaries with parameters for each fragment:
        [{'file': path, 'start': float, 'duration': float}, ...]
    """
    if not video_durations:
        raise ValueError("No video for assembly")

    edit_list = []
    total_video_duration = sum(vd[1] for vd in video_durations)
    num_videos = len(video_durations)

    # Scenario 1: One video, and it is LONGER than audio
    if num_videos == 1 and video_durations[0][1] >= audio_duration:
        video_path, v_duration = video_durations[0]
        start_time = (v_duration - audio_duration) / 2
        cut_duration = audio_duration

        if progress_callback:
            progress_callback(f"📐 Mode: One piece from middle (start={start_time:.2f}s, duration={cut_duration:.2f}s)")

        edit_list.append({
            'file': video_path,
            'start': start_time,
            'duration': cut_duration
        })
        return edit_list

    # Scenario 2: Video shorter than audio OR multiple videos (Loop Mode)
    if progress_callback:
        progress_callback(f"📐 Mode: Sandwich (Loop Mode) - {num_videos} video(s)")

    video_index = 0
    accumulated_duration = 0.0
    loop_count = 0

    while accumulated_duration < audio_duration:
        video_path, v_duration = video_durations[video_index]
        fragment_index = len(edit_list)

        # Clipping rules:
        # Start Time: for first fragment = 0, for others = 25
        if fragment_index == 0:
            start_time = 0.0
        else:
            start_time = 25.0

        # End Time: cut off 7 seconds from the end
        max_end_time = v_duration - 7

        if start_time >= max_end_time:
            # Video is too short for these rules, take what's available
            start_time = 0.0
            max_end_time = v_duration

        available_duration = max_end_time - start_time

        # How much more is needed to reach audio duration
        remaining_needed = audio_duration - accumulated_duration

        # Duration of this piece
        if available_duration <= remaining_needed:
            # Take all available piece
            cut_duration = available_duration
        else:
            # Trim to needed length
            cut_duration = remaining_needed

        edit_list.append({
            'file': video_path,
            'start': start_time,
            'duration': cut_duration
        })

        accumulated_duration += cut_duration
        video_index = (video_index + 1) % num_videos

        if video_index == 0:
            loop_count += 1

        if loop_count > 100:  # Protection against infinite loop
            raise ValueError("Too many iterations when calculating the edit list")

    if progress_callback:
        progress_callback(f"📐 Calculated {len(edit_list)} fragments, total duration={accumulated_duration:.2f}s")

    return edit_list


def normalize_video_fragment(input_path: pathlib.Path, output_path: pathlib.Path,
                            progress_callback: Optional[Callable[[str], None]] = None) -> bool:
    """
    Normalize video fragment to standard format (1920x1080, 25fps, SAR=1).
    Uses macOS M1 hardware acceleration (h264_videotoolbox).

    Args:
        input_path: Path to source file.
        output_path: Path for normalized file.
        progress_callback: Function for logging.

    Returns:
        True on success, False on error.
    """
    # Filter: scaling, padding, SAR fix, FPS fix
    vf_filter = (
        f"fps={VIDEO_STANDARD['framerate']},"
        f"scale={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:force_original_aspect_ratio=decrease,"
        f"pad={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:(ow-iw)/2:(oh-ih)/2,"
        "setsar=1"
    )

    cmd = [
        'ffmpeg', '-y',
        '-i', str(input_path),
        '-vf', vf_filter,
        '-an',  # No audio
        '-c:v', VIDEO_STANDARD['vcodec'],
        '-b:v', '6000k',  # Increased bitrate for intermediate files
        '-pix_fmt', VIDEO_STANDARD['pix_fmt'],
        '-r', str(VIDEO_STANDARD['framerate']),
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        if progress_callback:
            progress_callback(f"❌ Normalization error for {input_path.name}: {result.stderr[:100]}")
        return False

    return True


def create_concat_video(edit_list: List[Dict], output_path: pathlib.Path,
                       temp_dir: pathlib.Path, progress_callback: Optional[Callable[[str], None]] = None,
                       stop_event: Optional[any] = None) -> bool:
    """
    Create concatenated video from fragments.
    Combines cutting and normalization into one command for optimization.

    Args:
        edit_list: List of dictionaries with fragment parameters.
        output_path: Path for output file.
        temp_dir: Temporary folder.
        progress_callback: Function for logging.
        stop_event: Event for interruption.

    Returns:
        True on success, False on error.
    """
    if progress_callback:
        progress_callback(f"🎬 Cutting and normalizing {len(edit_list)} fragments...")

    normalized_files = []
    concat_dir = temp_dir / 'concat_parts'
    concat_dir.mkdir(exist_ok=True)

    # Normalization filter (same for all fragments)
    vf_filter = (
        f"fps={VIDEO_STANDARD['framerate']},"
        f"scale={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:force_original_aspect_ratio=decrease,"
        f"pad={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:(ow-iw)/2:(oh-ih)/2,"
        "setsar=1"
    )

    for i, fragment in enumerate(edit_list):
        if stop_event and stop_event.is_set():
            return False

        # Combine cutting and normalization into ONE command
        normalized_file = concat_dir / f'norm_{i:03d}.mp4'

        cmd = [
            'ffmpeg', '-y',
            '-ss', str(fragment['start']),
            '-i', str(fragment['file']),
            '-t', str(fragment['duration']),
            '-vf', vf_filter,
            '-an',  # No audio
            '-c:v', VIDEO_STANDARD['vcodec'],
            '-b:v', '6000k',  # Increased bitrate for intermediate files
            '-pix_fmt', VIDEO_STANDARD['pix_fmt'],
            '-r', str(VIDEO_STANDARD['framerate']),
            str(normalized_file)
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

        if result.returncode != 0:
            if progress_callback:
                progress_callback(f"❌ Error processing fragment {i}: {result.stderr[:100]}")
            return False

        normalized_files.append(normalized_file)

    if progress_callback:
        progress_callback(f"🎬 Stitching {len(normalized_files)} fragments...")

    # Create file for concat demuxer
    concat_list_file = temp_dir / 'concat_list.txt'
    with open(concat_list_file, 'w') as f:
        for norm_file in normalized_files:
            f.write(f"file '{norm_file.absolute()}'\n")

    # Join via concat demuxer
    cmd = [
        'ffmpeg', '-y',
        '-f', 'concat',
        '-safe', '0',
        '-i', str(concat_list_file),
        '-c', 'copy',
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

    if result.returncode != 0:
        if progress_callback:
            progress_callback(f"❌ Stitching error: {result.stderr[:100]}")
        return False

    return True


def create_final_master(video_path: pathlib.Path, audio_path: pathlib.Path,
                       output_path: pathlib.Path, progress_callback: Optional[Callable[[str], None]] = None) -> bool:
    """
    Create final "Golden Master" with audio and all filters.
    Uses macOS M1 hardware acceleration (h264_videotoolbox).

    Args:
        video_path: Path to video file.
        audio_path: Path to audio file.
        output_path: Path for output file.
        progress_callback: Function for logging.

    Returns:
        True on success, False on error.
    """
    if progress_callback:
        progress_callback("🎨 Creating final master file...")

    # Volume filter (applied after merging all video tracks)
    audio_filter = "loudnorm=i=-16:tp=-1.5"

    cmd = [
        'ffmpeg', '-y',
        '-i', str(video_path),
        '-i', str(audio_path),
        '-c:v', VIDEO_STANDARD['vcodec'],
        '-b:v', VIDEO_STANDARD['bitrate'],
        '-pix_fmt', VIDEO_STANDARD['pix_fmt'],
        '-r', str(VIDEO_STANDARD['framerate']),
        '-c:a', 'aac',
        '-ar', '48000',
        '-ac', '2',
        '-af', audio_filter,
        '-shortest',  # Cut to the shorter track
        '-movflags', '+faststart',
        str(output_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)

    if result.returncode != 0:
        if progress_callback:
            progress_callback(f"❌ Final render error: {result.stderr[:100]}")
        return False

    if progress_callback:
        progress_callback("✅ Final file created successfully!")

    return True


def parse_urls_from_text(text: str) -> List[str]:
    """
    Extract URLs from text, ignoring spaces and line breaks.

    Args:
        text: Source text.

    Returns:
        List of unique valid URLs.
    """
    # Regex for YouTube URL
    youtube_pattern = r'https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[\w-]+(?:\S*)?'

    urls = re.findall(youtube_pattern, text, re.IGNORECASE)

    # Cleaning URL from trailing garbage
    cleaned_urls = []
    for url in urls:
        # Find end of URL (first space or end of line in original)
        match = re.search(r'https?://[^\s<>"]+', url)
        if match:
            clean_url = match.group(0)
            # Remove extra characters at the end
            clean_url = re.sub(r'[^\w\-/?=&]+$', '', clean_url)
            if clean_url not in cleaned_urls:
                cleaned_urls.append(clean_url)

    return cleaned_urls


def extract_video_id(url: str) -> str:
    """
    Extract video ID from YouTube URL.

    Args:
        url: YouTube URL.

    Returns:
        Video ID or empty string.
    """
    patterns = [
        r'(?:v=|/)([0-9A-Za-z_-]{11}).*',
        r'youtu\.be/([0-9A-Za-z_-]{11})'
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return ''


def process_sandwich_task(task: Dict, base_archive_path: pathlib.Path,
                         progress_callback: Callable[[str], None],
                         stop_event: Optional[any] = None) -> Optional[pathlib.Path]:
    """
    Main function for processing composite video creation task.

    Args:
        task: Dictionary with task parameters.
        base_archive_path: Path to archive root.
        progress_callback: Function to send text logs.
        stop_event: For interruption.

    Returns:
        Path to created file or None on error.
    """
    try:
        # Stage A: Preparing folders and names
        eid = task.get('eid')
        name = task.get('name', '')
        role = task.get('role', '')
        ssid = task.get('ssid')
        song_name = task.get('song_name', '')
        audio_url = task.get('audio_url', '')
        video_urls = task.get('video_urls', [])

        if not audio_url:
            progress_callback("❌ Audio source URL not specified!")
            return None

        if not video_urls:
            progress_callback("❌ Video source URLs not specified!")
            return None

        progress_callback("🎬 Starting 'Composite Video' task...")
        progress_callback(f"   Audio: {audio_url}")
        progress_callback(f"   Video: {len(video_urls)} pcs.")

        # Get path to entity
        entity_path, _ = entity_manager_v2.get_or_create_entity_path(
            base_archive_path, eid, name, role, ssid=ssid, song_name=song_name,
            log_callback=progress_callback
        )

        if not entity_path:
            progress_callback(f"❌ Failed to get entity path for EID={eid}")
            return None

        # Folder for raw_videos
        raw_videos_folder = entity_path / 'raw_videos'
        raw_videos_folder.mkdir(parents=True, exist_ok=True)

        # Get folder name for filename generation
            # This is a song
            folder_name = pathlib.Path(entity_path).name  # Song folder name
        else:
            folder_name = name.replace(' ', '_')[:30]

        # Get next free index
        next_index = entity_manager_v2.get_next_media_index(
            raw_videos_folder, 'raw_video', r'raw_video_(\d+)_'
        )

        # Get audio video ID for naming
        audio_id = extract_video_id(audio_url) or 'UNKNOWN'

        # Output filename
        output_filename = f"raw_video_{next_index:03d}_{folder_name}--EID{eid}_{audio_id}_SANDWICH.mp4"
        output_path = raw_videos_folder / output_filename

        progress_callback(f"📁 Target file: {output_filename}")

        # Stage B: Downloading
        progress_callback("=" * 50)
        progress_callback("📥 Stage B: Downloading sources")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = pathlib.Path(temp_dir)

            if stop_event and stop_event.is_set():
                progress_callback("⏹️ Task interrupted by user")
                return None

            # Downloading audio
            progress_callback("📥 Downloading audio...")
            audio_temp_folder = temp_path / 'audio_source'
            audio_file = download_youtube_video(
                audio_url, audio_temp_folder, download_audio=True,
                progress_callback=progress_callback, stop_event=stop_event
            )

            if not audio_file:
                progress_callback("❌ Failed to download audio!")
                return None

            audio_duration = get_video_duration(audio_file)
            progress_callback(f"✅ Audio downloaded: {audio_file.name}, duration={audio_duration:.2f}s")

            if stop_event and stop_event.is_set():
                progress_callback("⏹️ Task interrupted by user")
                return None

            # Downloading video
            progress_callback("📥 Downloading video...")
            video_temp_folder = temp_path / 'video_sources'
            video_files = []

            for i, video_url in enumerate(video_urls):
                if stop_event and stop_event.is_set():
                    progress_callback("⏹️ Task interrupted by user")
                    return None

                video_file = download_youtube_video(
                    video_url, video_temp_folder / f'source_{i}', download_audio=False,
                    progress_callback=progress_callback, stop_event=stop_event
                )

                if video_file:
                    try:
                        v_duration = get_video_duration(video_file)
                        video_files.append((video_file, v_duration))
                        progress_callback(f"✅ Video {i+1} downloaded: {video_file.name}, duration={v_duration:.2f}s")
                    except ValueError as e:
                        progress_callback(f"⚠️ Skipping video {i+1} due to error: {e}")
                        if video_file.exists():
                            video_file.unlink()
                else:
                    progress_callback(f"⚠️ Failed to download video {i+1}: {video_url}")

            if not video_files:
                progress_callback("❌ Failed to download any videos!")
                return None

            # Stage C: Calculate edit list
            progress_callback("=" * 50)
            progress_callback("📐 Stage C: Calculate edit list")

            edit_list = calculate_edit_list(
                audio_duration, video_files, progress_callback
            )

            # Stage D: Assembly
            progress_callback("=" * 50)
            progress_callback("🎬 Stage D: Assembly final video")

            if stop_event and stop_event.is_set():
                progress_callback("⏹️ Task interrupted by user")
                return None

            # 1. Create concatenated video
            concat_video_path = temp_path / 'concatenated.mp4'

            if not create_concat_video(
                edit_list, concat_video_path, temp_path, progress_callback, stop_event
            ):
                progress_callback("❌ Error creating concatenated video!")
                return None

            if stop_event and stop_event.is_set():
                progress_callback("⏹️ Task interrupted by user")
                return None

            # 2. Final render with audio
            if not create_final_master(
                concat_video_path, audio_file, output_path, progress_callback
            ):
                progress_callback("❌ Error during final render!")
                return None

            # Check if file was created
            if output_path.exists() and output_path.stat().st_size > 1000:
                progress_callback("=" * 50)
                progress_callback(f"🎉 DONE! File created: {output_path.name}")
                progress_callback(f"    Size: {output_path.stat().st_size / (1024*1024):.2f} MB")
                return output_path
            else:
                progress_callback("❌ File was not created or has too small size!")
                return None

    except Exception as e:
        progress_callback(f"❌ Critical error during processing: {e}")
        import traceback
        progress_callback(f"📋 Traceback: {traceback.format_exc()}")
        return None
