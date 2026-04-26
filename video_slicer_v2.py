# video_slicer_v2.py

import subprocess
from pathlib import Path
import random
import json
from typing import Callable, List
import re
import os

import entity_manager_v2 as em


def get_video_duration(video_path: Path, progress_callback: Callable[[str], None]) -> float:
    if not video_path.exists():
        return 0.0
    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', str(video_path)]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        data = json.loads(result.stdout)
        return float(data['format']['duration'])
    except Exception as e:
        progress_callback(f"⚠️ Could not get duration for {video_path.name}: {e}")
        return 0.0


def analyze_media_file(video_path: Path, progress_callback: Callable[[str], None]) -> dict:
    """
    Analyzes media file using ffprobe and returns audio information.
    Returns dictionary with keys: 'audio_codec', 'sample_rate', 'channels'
    """
    if not video_path.exists():
        return {'audio_codec': None, 'sample_rate': None, 'channels': None}

    command = ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams', '-select_streams', 'a:0', str(video_path)]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8')
        data = json.loads(result.stdout)

        if 'streams' in data and len(data['streams']) > 0:
            stream = data['streams'][0]
            return {
                'audio_codec': stream.get('codec_name'),
                'sample_rate': int(stream.get('sample_rate', 0)),
                'channels': int(stream.get('channels', 0))
            }
        else:
            return {'audio_codec': None, 'sample_rate': None, 'channels': None}
    except Exception as e:
        progress_callback(f"⚠️ Could not analyze audio for {video_path.name}: {e}")
        return {'audio_codec': None, 'sample_rate': None, 'channels': None}


def _generate_non_overlapping_starts(total_duration: float, num_cuts: int, cut_duration: int) -> List[int]:
    if total_duration < num_cuts * cut_duration:
        return []
    possible_starts = list(range(int(total_duration - cut_duration)))
    selected_starts = []
    for _ in range(num_cuts):
        if not possible_starts: break
        start_time = random.choice(possible_starts)
        selected_starts.append(start_time)
        for i in range(start_time - cut_duration, start_time + cut_duration):
            if i in possible_starts:
                possible_starts.remove(i)
    return sorted(selected_starts)


def slice_clips_only(
        source_video_path: Path, entity_path: Path, eid: str,
        num_cuts: int, cut_duration: int, progress_callback: Callable[[str], None]
) -> List[Path]:
    """
    Slices ONE video into clips WITHOUT master file conversion and WITHOUT deleting source.
    """
    progress_callback(f"▶️ Simple slicing: {source_video_path.name}")
    output_path = entity_path / "selected_clips"
    output_path.mkdir(exist_ok=True)
    created_files = []

    total_duration = get_video_duration(source_video_path, progress_callback)
    if total_duration < cut_duration:
        progress_callback(f"INFO: Skipping {source_video_path.name}: duration < cut duration.")
        return created_files

    start_times = _generate_non_overlapping_starts(total_duration, num_cuts, cut_duration)
    if not start_times:
        progress_callback(f"INFO: Skipping {source_video_path.name}: could not generate fragments.")
        return created_files

    entity_folder_name = entity_path.name
    source_name_stem = source_video_path.stem
    source_index_match = re.search(r'raw_video_(\d+)_', source_name_stem)
    source_index = int(source_index_match.group(1)) if source_index_match else 0

    for i, start_time in enumerate(start_times):
        slice_index = i + 1
        new_filename = (f"clip_s{source_index:03d}_{slice_index:03d}_{entity_folder_name}_{source_name_stem}.mp4")
        output_cut_path = output_path / new_filename
        slice_command = [
            'ffmpeg', '-y', '-ss', str(start_time), '-i', str(source_video_path),
            '-t', str(cut_duration),
            '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1',
            '-c:v', 'libx264', '-preset', 'medium', '-crf', '27',
            '-pix_fmt', 'yuv420p', '-r', '25', '-an',
            '-movflags', '+faststart',
            str(output_cut_path)
        ]
        try:
            subprocess.run(slice_command, check=True, capture_output=True, text=True, encoding='utf-8')
            progress_callback(f"   ✅ Created slice: {new_filename}")
            created_files.append(output_cut_path)
        except subprocess.CalledProcessError as e:
            progress_callback(f"   ❌ ERROR during slicing -> {new_filename}:\n{e.stderr.strip()}")
            continue

    progress_callback(f"✅ Simple slicing {source_video_path.name} complete. Created {len(created_files)}/{len(start_times)} clips.")
    return created_files


def convert_master_file_only(
        source_video_path: Path, progress_callback: Callable[[str], None]
) -> bool:
    """
    Converts ONE video to P-Pro format and deletes source.
    """
    progress_callback(f"▶️ P-Pro conversion: {source_video_path.name}")

    # Audio track analysis
    audio_info = analyze_media_file(source_video_path, progress_callback)

    # Determine audio parameters
    if (audio_info['audio_codec'] == 'aac' and
        audio_info['sample_rate'] == 44100 and
        audio_info['channels'] == 2):
        audio_params = ['-c:a', 'copy']
        progress_callback(f"   -> Audio is already in standard, copying without changes")
    else:
        audio_params = ['-c:a', 'aac', '-b:a', '192k', '-ar', '44100', '-ac', '2']
        progress_callback(f"   -> Audio requires re-encoding to standard")

    # Formation of output filename
    final_output_name = f"P-Pro_{source_video_path.name}"
    final_output_path = source_video_path.parent / final_output_name

    # FFmpeg command for conversion
    convert_command = [
        'ffmpeg', '-y', '-i', str(source_video_path),
        '-c:v', 'libx264', '-preset', 'medium', '-crf', '27',
        '-pix_fmt', 'yuv420p', '-r', '25',
        '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1'
    ] + audio_params + [
        '-movflags', '+faststart',
        str(final_output_path)
    ]

    try:
        subprocess.run(convert_command, check=True, capture_output=True, text=True, encoding='utf-8')
        progress_callback(f"✅ Master file {final_output_name} successfully created")

        # Delete source file only after successful master file creation
        source_video_path.unlink()
        progress_callback(f"   -> Source file {source_video_path.name} deleted")

    except subprocess.CalledProcessError as e:
        progress_callback(f"❌ ERROR during master file conversion: {e.stderr.strip()}")
        # Do not delete source file on error
        return False

    return True


def slice_and_convert_full_cycle(
        source_video_path: Path, entity_path: Path, eid: str,
        num_cuts: int, cut_duration: int, progress_callback: Callable[[str], None]
) -> List[Path]:
    """
    Old function for backward compatibility or manual tasks.
    Just calls both new functions sequentially.
    """
    created_files = slice_clips_only(source_video_path, entity_path, eid, num_cuts, cut_duration, progress_callback)
    convert_master_file_only(source_video_path, progress_callback)
    return created_files


# For backward compatibility
def slice_single_video(*args, **kwargs):
    """
    Deprecated function. Use slice_and_convert_full_cycle instead.
    """
    return slice_and_convert_full_cycle(*args, **kwargs)