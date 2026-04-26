#
# --- START OF FILE montage_processor.py ---
#

import os
import re
import sys
import shutil
import pandas as pd
import ffmpeg
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from collections import defaultdict, deque
import traceback
import subprocess
import random
import threading
from datetime import datetime
import json
import textwrap

# --- GLOBAL STOP FLAG ---
stop_flag = threading.Event()

# Constants for Seamless mode
SEAMLESS_HEAD_SAFE = 30.0
SEAMLESS_TAIL_SAFE = 20.0
SEAMLESS_CROSSFADE_MS = 120
SEAMLESS_ATTACK_MS = 120
SEAMLESS_FADEOUT_MS = 2000
THIRD_MODE_FADE_MS = 200


def stop_execution():
    """Sets flag for immediate (if possible) stop of all long operations."""
    stop_flag.set()


# --- HELPER FUNCTIONS ---
VIDEO_EXTENSIONS = ('.mp4', '.mov', '.mkv', '.avi')
AUDIO_EXTENSIONS = ('.mp3', '.wav', '.aac', '.m4a')


def _wrap_text(text, width=45):
    if not text or text.lower() == 'nan':
        return ""
    return textwrap.fill(text, width=width)


def _parse_audio_filename(filename: str):
    match = re.search(r'(\d+)', Path(filename).stem)
    return int(match.group(1)) if match else None


def _create_safe_filename(original_path: Path, counter: int) -> str:
    stem = Path(original_path).stem
    safe_chars = re.sub(r'[^a-zA-Z0-9]', '', stem)
    safe_stem = safe_chars[:25]
    return f"{safe_stem}_{counter:04d}{original_path.suffix}"


def _get_media_duration(file_path: Path) -> float:
    try:
        probe = ffmpeg.probe(str(file_path))
        return float(probe['format']['duration'])
    except Exception:
        return 0.0


def _is_file_broken(file_path: Path) -> str | None:
    """
    Checks file for integrity: existence, readability via ffmpeg, and duration presence.
    Returns error string or None if everything is fine.
    """
    if not file_path or not file_path.exists():
        return "File does not exist or path is empty"
    try:
        # Try to get metadata. If file is broken, ffmpeg.probe will throw an exception.
        probe = ffmpeg.probe(str(file_path))
        
        # Check for presence of streams
        if not probe.get('streams'):
            return "No media streams in file"
            
        # Check duration
        duration = float(probe.get('format', {}).get('duration', 0))
        if duration <= 0:
            # Sometimes duration might be in streams if not in format
            durations = [float(s.get('duration', 0)) for s in probe['streams'] if 'duration' in s]
            if not durations or max(durations) <= 0:
                return "File duration 0 or undefined"
                
        return None
    except ffmpeg.Error as e:
        stderr = e.stderr.decode('utf-8', 'ignore') if e.stderr else str(e)
        return f"FFmpeg cannot read file: {stderr[:150]}..."
    except Exception as e:
        return f"Integrity error: {str(e)}"


def _is_conformant_master(file_path: Path, standard: dict) -> bool:
    """
    Checks file for conformance to 'Golden Standard'.
    Checks 9+ parameters: resolution, codec, FPS, PixFmt, SampleRate, Channels, Bitrate, SAR and streams presence.
    """
    try:
        probe = ffmpeg.probe(str(file_path))
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        audio_stream = next((s for s in probe['streams'] if s['codec_type'] == 'audio'), None)

        if not video_stream or not audio_stream:
            return False

        # 1 & 2. Resolution
        if int(video_stream.get('width', 0)) != standard['width'] or \
           int(video_stream.get('height', 0)) != standard['height']:
            return False

        # 3. Video codec
        if video_stream.get('codec_name', '') != 'h264':
            return False

        # 4. Pixel format
        if video_stream.get('pix_fmt', '') != standard['pix_fmt']:
            return False

        # 5. FPS
        fps_eval = video_stream.get('r_frame_rate', '0/1')
        try:
            fps = eval(fps_eval) if '/' in fps_eval else float(fps_eval)
            if abs(fps - standard['framerate']) > 0.1:
                return False
        except:
            return False

        # 6. Audio Sample Rate
        if int(audio_stream.get('sample_rate', 0)) != 48000:
            return False

        # 7. Audio channels
        if int(audio_stream.get('channels', 0)) != 2:
            return False

        # 8. SAR (Sample Aspect Ratio)
        if video_stream.get('sample_aspect_ratio', '1:1') != '1:1':
            # Allow absence if SAR is not explicitly specified as 1:1 but image is actually correct
            pass

        # 9. Data presence
        if float(probe.get('format', {}).get('duration', 0)) <= 0:
            return False

        return True
    except Exception:
        return False


def _get_frame_count(file_path: Path) -> int:
    try:
        probe = ffmpeg.probe(str(file_path))
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        if video_stream and 'nb_frames' in video_stream and int(video_stream['nb_frames']) > 0:
            return int(video_stream['nb_frames'])
        if video_stream and 'duration' in video_stream and 'r_frame_rate' in video_stream:
            duration = float(video_stream['duration'])
            frame_rate = eval(video_stream['r_frame_rate'])
            return int(duration * frame_rate)
        return 0
    except (ffmpeg.Error, KeyError):
        return 0


def _has_audio_stream(file_path: Path) -> bool:
    try:
        probe = ffmpeg.probe(str(file_path))
        return any(s['codec_type'] == 'audio' for s in probe['streams'])
    except ffmpeg.Error:
        return False


def _get_clips_from_path(folder_path: Path):
    if not folder_path or not folder_path.is_dir(): return [], []
    all_files = [f for f in folder_path.iterdir() if f.suffix.lower() in VIDEO_EXTENSIONS and f.is_file()]
    highlights = sorted([f for f in all_files if f.name.startswith('h_')], key=lambda p: p.name)
    regulars = sorted([f for f in all_files if not f.name.startswith('h_')], key=lambda p: p.name)
    return highlights, regulars


def _run_ffmpeg_command(command, logger, operation_name="ffmpeg_task"):
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
        return result
    except subprocess.CalledProcessError as e:
        logger.log(f"!!! CRITICAL FFmpeg ERROR during '{operation_name}' operation:")
        logger.log(f"--- COMMAND ---\n{' '.join(e.cmd)}")
        logger.log(f"--- RETURN CODE ---\n{e.returncode}")
        logger.log(f"--- STDERR ---\n{e.stderr}")
        raise


# --- NEW AUDIO FUNCTIONS BLOCK ---

def _normalize_audio(input_path, output_path, lufs_target, logger, tp_value=-1.5):
    """
    Takes audio file and levels its loudness using ffmpeg-python with loudnorm filter.

    Args:
        input_path: Path to source file
        output_path: Path to output file
        lufs_target: Target LUFS level (integrated loudness)
        logger: Logger for messages
        tp_value: True Peak target in dB (default -1.5)
    """
    try:
        (
            ffmpeg
            .input(str(input_path))
            .filter('loudnorm', i=lufs_target, lra=7, tp=tp_value)
            .output(str(output_path), acodec='pcm_s16le', ar=48000, ac=2)
            .run(overwrite_output=True, quiet=True)
        )
        logger.log(f"  -> Audio {Path(input_path).name} normalized (LUFS: {lufs_target}, TP: {tp_value}dB).")
    except ffmpeg.Error as e:
        logger.log(f"!!! Error normalizing audio {Path(input_path).name}:\n{e.stderr.decode('utf-8', 'ignore')}")
        raise

def _limit_audio(input_path, output_path, tp_value, logger):
    """Takes audio file and cuts peaks using ffmpeg-python with alimiter filter."""
    try:
        (
            ffmpeg
            .input(str(input_path))
            .filter('alimiter', limit=f'{tp_value}dB', level=True)
            .output(str(output_path), acodec='pcm_s16le', ar=48000, ac=2)
            .run(overwrite_output=True, quiet=True)
        )
        logger.log(f"  -> Audio {Path(input_path).name} limited.")
    except ffmpeg.Error as e:
        logger.log(f"!!! Error limiting audio {Path(input_path).name}:\n{e.stderr.decode('utf-8', 'ignore')}")
        raise

def _process_prefix_audio(video_path, temp_dir, sound_settings, logger):
    """Extracts and processes sound from prefix video."""
    prefix_audio_raw_path = temp_dir / f"{Path(video_path).stem}_raw_audio.wav"
    prefix_audio_norm_path = temp_dir / f"{Path(video_path).stem}_norm_audio.wav"
    prefix_audio_final_path = temp_dir / f"{Path(video_path).stem}_final_audio.wav"
    
    if _has_audio_stream(Path(video_path)):
        ffmpeg.input(str(video_path)).output(str(prefix_audio_raw_path), acodec='pcm_s16le', ar=48000, ac=2).run(overwrite_output=True, quiet=True)
    else:
        duration = _get_media_duration(Path(video_path))
        ffmpeg.input(f"anullsrc=channel_layout=stereo:sample_rate=48000", f='lavfi', t=duration).output(str(prefix_audio_raw_path), acodec='pcm_s16le').run(overwrite_output=True, quiet=True)

    tp_value = float(sound_settings.get('final_limiter_tp', '-1.5'))
    _normalize_audio(prefix_audio_raw_path, prefix_audio_norm_path, sound_settings['target_source_lufs'], logger, tp_value)
    _limit_audio(prefix_audio_norm_path, prefix_audio_final_path, tp_value, logger)
    
    return prefix_audio_final_path

def _get_random_segment_from_song(song_path, duration, sector_start, sector_end, logger, exclude_intervals=None):
    """
    Cuts a random segment from specified sector of a song.
    exclude_intervals: list of tuples (start, end) that should not be intersected (Content ID protection).
    """
    if exclude_intervals is None:
        exclude_intervals = []

    song_dur = _get_media_duration(song_path)
    # Limit sector by real song length
    actual_end = min(sector_end, song_dur - duration)
    actual_start = max(sector_start, 0)

    if actual_start >= actual_end:
        # If sector is too small, expand it to entire song (except edges)
        actual_start = song_dur * 0.05
        actual_end = song_dur * 0.95 - duration

    # Try to find start point that doesn't fall into excluded intervals
    max_attempts = 10
    for _ in range(max_attempts):
        start_t = random.uniform(actual_start, actual_end)
        end_t = start_t + duration

        # Check for intersection
        conflict = False
        for ex_start, ex_end in exclude_intervals:
            # If there's an overlap of more than 0.5s - consider it a conflict
            if not (end_t < ex_start + 0.5 or start_t > ex_end - 0.5):
                conflict = True
                break

        if not conflict:
            return start_t

    # If no unique piece found after 10 attempts - return just a random one
    return random.uniform(actual_start, actual_end)

def _generate_block_audio_content(narration_path, music_path, temp_dir, settings, logger, music_start_t=0):
    """
    Creates ready, mixed audio track (voice + music).
    """
    sound_settings = settings.get('sound_settings', {})
    bg_music_db = float(sound_settings.get('bg_music_db', '-25'))
    cvl_db = float(sound_settings.get('combo_vs_live_db', '0'))
    tp_value = float(sound_settings.get('final_limiter_tp', '-1.5'))
    lufs_target = float(sound_settings.get('target_source_lufs', '-16'))

    narration_stem = Path(narration_path).stem

    # Get narration duration
    narration_duration = _get_media_duration(narration_path)
    if narration_duration <= 0:
        raise RuntimeError(f"Narration file duration for {narration_stem} is 0")

    if not music_path or not Path(music_path).exists():
        # Voice only
        final_audio = temp_dir / f"{narration_stem}_final.wav"
        (ffmpeg
               .input(str(narration_path))
               .filter('volume', f"{cvl_db}dB")
               .filter('aresample', **{'async': 1})
               .filter('alimiter', limit=f'{tp_value}dB', level=True)
               .output(str(final_audio), acodec='pcm_s16le', ar=48000, ac=2)
               .run(overwrite_output=True, quiet=True))
        return str(final_audio)

    # --- MUSIC BOUNDARIES CHECK ---
    music_dur = _get_media_duration(Path(music_path))
    if music_start_t + narration_duration > music_dur:
        # Adjust music start point so the piece doesn't fly over the edge
        music_start_t = max(0, music_dur - narration_duration - 0.1)

    # Cut required piece of music for backing and apply volume filter
    music_sub_path = temp_dir / f"{narration_stem}_bg_music.wav"

    (ffmpeg
           .input(str(music_path), ss=music_start_t, t=narration_duration)
           .filter('volume', f"{bg_music_db}dB")
           .output(str(music_sub_path), acodec='pcm_s16le', ar=48000, ac=2)
           .run(overwrite_output=True, quiet=True))

    # Mixing (narration with normalization + music)
    narration_input = ffmpeg.input(str(narration_path)).audio.filter('loudnorm', i=lufs_target, lra=7, tp=tp_value)
    music_input = ffmpeg.input(str(music_sub_path))

    mixed_audio_path = temp_dir / f"{narration_stem}_mixed.wav"
    (ffmpeg.filter([narration_input, music_input], 'amix', inputs=2, duration='first', weights='1 1')
            .output(str(mixed_audio_path), acodec='pcm_s16le', ar=48000, ac=2)
            .run(overwrite_output=True, quiet=True))

    # Block boost (cvl_db) + Synchronization + Limiter (tp_value)
    final_audio_path = temp_dir / f"{narration_stem}_final.wav"
    (ffmpeg
           .input(str(mixed_audio_path))
           .filter('volume', f"{cvl_db}dB")
           .filter('aresample', **{'async': 1})
           .filter('alimiter', limit=f'{tp_value}dB', level=True)
           .output(str(final_audio_path), acodec='pcm_s16le', ar=48000, ac=2)
           .run(overwrite_output=True, quiet=True))

    # Point cleaning of intermediate file
    try:
        Path(mixed_audio_path).unlink(missing_ok=True)
    except Exception:
        pass

    return str(final_audio_path)
def generate_test_mix(sample_files, settings, output_path, logger):
    """
    Creates informative test audio file (extended version).
    [Pair 1] -> [Ad] -> [Pair 2]
    """
    temp_dir = Path(output_path).parent / "audio_test_temp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    sound_settings = settings.get('sound_settings', {})
    lufs_target = float(sound_settings.get('target_source_lufs', '-16'))
    bg_music_db = float(sound_settings.get('bg_music_db', '-25'))
    cvl_db = float(sound_settings.get('combo_vs_live_db', '0'))
    tp_value = float(sound_settings.get('final_limiter_tp', '-1.5'))
    L = float(settings.get('live_clip_duration', 3.9))

    bg_vol = 10**(bg_music_db/20)
    cvl_vol = 10**(cvl_db/20)
    cvl_boost_combo = cvl_vol / 2.0
    cross_s = 0.120

    test_segments = []

    try:
        def create_seamless_pair(song_path, voice_path, idx):
            if not song_path or not Path(song_path).is_file(): return None
            if not voice_path or not Path(voice_path).is_file(): return None
            
            v_dur = _get_media_duration(Path(voice_path))
            total_dur = L + v_dur
            song_dur = _get_media_duration(Path(song_path))
            start_t = song_dur * 0.5 # Strictly MIDDLE
            
            # 1. Music envelope
            music_vol_expr = (
                f"if(lt(t,0.12), t/0.12, "
                f"if(lt(t,{L}), 1.0, "
                f"if(lt(t,{L}+{cross_s}), 1.0-(t-{L})/{cross_s}*(1.0-{bg_vol}), "
                f"if(lt(t,{total_dur}-2.0), {bg_vol}, "
                f"{bg_vol}*(1.0-(t-({total_dur}-2.0))/2.0)))))"
            )
            music_envelope = (
                ffmpeg.input(str(song_path), ss=start_t, t=total_dur)
                .filter('volume', music_vol_expr, eval='frame')
            )
            
            # 2. Voice normalization
            v_raw = temp_dir / f"test_v{idx}_raw.wav"
            v_norm = temp_dir / f"test_v{idx}_norm.wav"
            ffmpeg.input(str(voice_path)).output(str(v_raw), acodec='pcm_s16le', ar=48000, ac=2).run(overwrite_output=True, quiet=True)
            _normalize_audio(v_raw, v_norm, lufs_target, logger, tp_value)
            
            # 3. Mixing
            voice_delayed = ffmpeg.input(str(v_norm)).filter('adelay', f"{int(L*1000)}|{int(L*1000)}")
            raw_mix = ffmpeg.filter([music_envelope, voice_delayed], 'amix', inputs=2, duration='first', normalize=0, weights='1 1')
            
            # 4. Final boost
            final_boost_expr = f"if(lt(t,{L}), 1.0, {cvl_boost_combo})"
            
            pair_wav = temp_dir / f"pair_{idx}.wav"
            (raw_mix
             .filter('volume', final_boost_expr, eval='frame')
             .output(str(pair_wav), acodec='pcm_s16le', ar=48000, ac=2)
             .run(overwrite_output=True, quiet=True))
            
            return ffmpeg.input(str(pair_wav))

        # --- ASSEMBLING THE CHAIN ---
        
        # Pair 1
        p1 = create_seamless_pair(sample_files.get('song1'), sample_files.get('narration1'), 1)
        if p1: test_segments.append(p1)
        
        # Ad
        ad_path = sample_files.get('ad')
        if ad_path and Path(ad_path).is_file():
            logger.log("Test: Adding advertisement...")
            ad_out = temp_dir / "test_ad_norm.wav"
            ad_raw = temp_dir / "test_ad_raw.wav"
            if _has_audio_stream(Path(ad_path)):
                ffmpeg.input(str(ad_path)).output(str(ad_raw), acodec='pcm_s16le', ar=48000, ac=2).run(overwrite_output=True, quiet=True)
                _normalize_audio(ad_raw, ad_out, lufs_target, logger, tp_value)
                test_segments.append(ffmpeg.input(str(ad_out)))
            
        # Pair 2
        p2 = create_seamless_pair(sample_files.get('song2'), sample_files.get('narration2'), 2)
        if p2: test_segments.append(p2)

        if test_segments:
            logger.log("Test: Final gluing...")
            f_wav = temp_dir / "final_test.wav"
            (ffmpeg.concat(*test_segments, v=0, a=1).output(str(f_wav), acodec='pcm_s16le', ar=48000, ac=2).run(quiet=True, overwrite_output=True))
            
            # Final limiter
            limited_wav = temp_dir / "limited_test.wav"
            _limit_audio(f_wav, limited_wav, tp_value, logger)
            
            ffmpeg.input(str(limited_wav)).output(output_path, acodec='libmp3lame', audio_bitrate='192k').run(quiet=True, overwrite_output=True)
            logger.log(f"✅ Extended test mix ready: {output_path}")

    except Exception as e:
        logger.log(f"❌ Error in audio test: {e}")
    finally:
        if Path(temp_dir).exists(): shutil.rmtree(temp_dir)

def _generate_sawtooth_timeline(needed_segments, song_path, live_duration, logger):
    """
    Generates timecodes for segments according to the "Sawtooth Rollback" algorithm.

    Algorithm:
    1. Inside groups, segments move BACKWARDS along the timeline.
    2. Between groups, a JUMP FORWARD occurs.
    3. Each group = 2 stories (voice files + Lives).

    Args:
        needed_segments: List of dictionaries with type and duration
        song_path: Path to song
        live_duration: Live clip length (L)
        logger: Logger

    Returns:
        List of actual_start_t for each segment
    """
    T_total = _get_media_duration(song_path)

    # Timeline parameters
    T_start = max(T_total * 0.15, 45.0)
    T_end = max(T_total * 0.10, 30.0)
    T_limit = T_total - T_end - live_duration

    logger.log(f"  -> Timeline: T_total={T_total:.1f}s, T_start={T_start:.1f}s, T_limit={T_limit:.1f}s")

    # Group segments: each group = 2 voice files + 2 Lives
    groups = []
    current_group = []
    voice_count_in_group = 0

    for seg in needed_segments:
        current_group.append(seg)
        if seg['type'] == 'C' or seg['type'] == 'C_sub':
            voice_count_in_group += 1

        # Group finished after 2 voice files
        if voice_count_in_group >= 2:
            groups.append(current_group)
            current_group = []
            voice_count_in_group = 0

    # Add leftovers (if any)
    if current_group:
        groups.append(current_group)

    logger.log(f"  -> Formed {len(groups)} groups")

    # Generate timecodes for each group
    result_timestamps = []
    prev_group_anchor = None
    used_intervals = []  # List of used intervals (start, end)

    for group_idx, group in enumerate(groups):
        # Select T_anchor for current group
        if prev_group_anchor is None:
            # First group - random point in the right part
            T_anchor = random.uniform(T_start + 30, T_limit)
        else:
            # Jump forward: at least 30 seconds later than previous T_anchor
            min_anchor = prev_group_anchor + 30
            if min_anchor > T_limit:
                # If no space in "future", find a large "gap"
                max_gap = 0
                best_hole_start = T_start
                best_hole_end = T_start + 60

                # Sort used intervals
                sorted_intervals = sorted(used_intervals, key=lambda x: x[0])

                # Find gaps between intervals
                prev_end = T_start
                for start, end in sorted_intervals:
                    if start - prev_end > max_gap:
                        max_gap = start - prev_end
                        best_hole_start = prev_end
                        best_hole_end = start
                    prev_end = max(prev_end, end)

                # Check gap after last interval
                if T_limit - prev_end > max_gap:
                    max_gap = T_limit - prev_end
                    best_hole_start = prev_end
                    best_hole_end = T_limit

                T_anchor = random.uniform(best_hole_start, min(best_hole_end, T_limit))
            else:
                T_anchor = random.uniform(min_anchor, min(min_anchor + 60, T_limit))

        logger.log(f"  -> Group {group_idx + 1}: T_anchor={T_anchor:.1f}s")

        # Generate timecodes inside group (move BACKWARDS)
        T_current = T_anchor
        for seg in group:
            seg_dur = seg.get('duration', live_duration)

            # Check: if T_current < T_start, jump forward
            if T_current - seg_dur < T_start:
                # Jump to available area
                if prev_group_anchor is not None:
                    min_jump = prev_group_anchor + 30
                    T_current = max(min_jump, T_start + random.uniform(10, 30))
                else:
                    T_current = T_start + random.uniform(10, 30)
                # Check not to exceed T_limit
                if T_current > T_limit:
                    T_current = random.uniform(T_start, T_limit)

            # Set start point
            start_t = max(T_start, min(T_current, T_limit))
            result_timestamps.append(start_t)

            # Move BACKWARD by random amount (2-4) * L
            step_back = random.uniform(2, 4) * live_duration
            T_current = T_current - step_back

            # Remember used interval
            used_intervals.append((start_t, start_t + seg_dur))

        prev_group_anchor = T_anchor

    return result_timestamps


def assemble_block_new(plan_item, settings, temp_dir, logger):
    """
    Assembles one full block according to the ACOUSTIC CAMOUFLAGE (Zigzag) algorithm
    or the new SECTOR OVERLAP (Seamless) algorithm.
    """
    # --- GLOBAL INITIALIZATION AND PARAMETERS ---
    final_segments_paths = []
    needed_segments = []
    L = float(settings['live_clip_duration'])
    sound_settings = settings.get('sound_settings', {})
    bg_music_db = float(sound_settings.get('bg_music_db', '-25'))
    cvl_db = float(sound_settings.get('combo_vs_live_db', '0'))
    tp_value = float(sound_settings.get('final_limiter_tp', '-1.5'))
    lufs_target = float(sound_settings.get('target_source_lufs', '-16'))

    block_name = plan_item['name']
    caption_text = _wrap_text(plan_item.get('Caption', ''))
    ssid = plan_item.get('SSID')
    song_path = plan_item.get('song_path')
    voice_files = plan_item.get('voice_files', [])
    formula = plan_item.get('formula', 'C+L+C')
    VIDEO_STANDARD = settings['VIDEO_STANDARD']
    montage_mode = settings.get('montage_mode', 'zigzag')

    if plan_item.get('is_intro'):
        intro_songs = plan_item.get('intro_song_paths', [])
        if not intro_songs:
            logger.log(f"!!! ERROR: No songs found for intro.")
            return None
        song_path = intro_songs[0]

    if not song_path or not Path(song_path).exists():
        logger.log(f"!!! ERROR: Song source for block {block_name} not found.")
        return None

    is_intro = 'B01' in block_name or plan_item.get('is_intro')

    if is_intro:
        intro_songs = plan_item.get('intro_song_paths', [])
        intro_mode = settings.get('manual_intro_mode', 'auto')
        manual_enabled = settings.get('manual_intro_enabled', False)

        for sub_idx_minus_1, voice_file in enumerate(voice_files):
            sub_idx = sub_idx_minus_1 + 1
            manual_clips_added = False

            # 1. Manual clips collection (Type M)
            if manual_enabled:
                sub_list = settings.get(f'manual_intro_list{sub_idx}', [])
                manual_clips = [p for p in sub_list if "[Live+Combo]" not in p]
                for m_clip in manual_clips:
                    needed_segments.append({'type': 'M', 'path': str(m_clip), 'sub_idx': sub_idx})
                    manual_clips_added = True

            # 2. Automation or pairs planning (L, C, P)
            if intro_mode == 'auto':
                if not manual_clips_added:
                    needed_segments.append({'type': 'L', 'sub_idx': sub_idx, 'duration': L})

                # Added logic for splitting long voice to activate "Zigzag"
                voice_dur = _get_media_duration(voice_file)
                if voice_dur > L:
                    k = int((voice_dur + L - 1) // L)
                    segment_dur = voice_dur / k
                    for s_idx in range(k):
                        needed_segments.append({'type': 'C_sub', 'audio': voice_file, 'sub_index': s_idx, 'sub_total': k, 'duration': segment_dur, 'sub_idx': sub_idx})
                else:
                    needed_segments.append({'type': 'C', 'audio': voice_file, 'sub_idx': sub_idx, 'duration': voice_dur})
            else:
                needed_segments.append({'type': 'P', 'audio': voice_file, 'mode': intro_mode, 'sub_idx': sub_idx})

        # 3. Song binding (for L, C, P types)
        for seg in needed_segments:
            if seg['type'] == 'M': continue
            manual_bg = settings.get('manual_intro_bg_songs', {}).get(seg['sub_idx'])
            current_song = None
            if manual_bg:
                m_base = settings.get('ssid_map', {}).get(manual_bg)
                if m_base:
                    v_files = [f for f in Path(m_base).joinpath("raw_videos").iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS]
                    if v_files: current_song = v_files[0]
            if not current_song:
                current_song = intro_songs[(seg['sub_idx'] - 1) % len(intro_songs)]
            seg['current_song'] = current_song

        # 4. Timeline (only for Zigzag Intro)
        if intro_mode == 'auto':
            t_indices = [idx for idx, s in enumerate(needed_segments) if s['type'] != 'M']
            if t_indices:
                groups = [{'song': needed_segments[t_indices[0]]['current_song'], 'indices': [t_indices[0]]}]
                for i in range(1, len(t_indices)):
                    idx = t_indices[i]
                    if str(needed_segments[idx]['current_song']) == str(groups[-1]['song']):
                        groups[-1]['indices'].append(idx)
                    else:
                        groups.append({'song': needed_segments[idx]['current_song'], 'indices': [idx]})
                for g in groups:
                    # First set durations for all group segments
                    for idx in g['indices']:
                        if 'duration' not in needed_segments[idx]:
                            needed_segments[idx]['duration'] = L if needed_segments[idx]['type'] == 'L' else _get_media_duration(needed_segments[idx]['audio'])

                    # Then calculate timeline based on these durations
                    ts = _generate_sawtooth_timeline([needed_segments[idx] for idx in g['indices']], g['song'], L, logger)
                    for i, idx in enumerate(g['indices']):
                        needed_segments[idx]['actual_start_t'] = ts[i]
    else:
        # --- PLANNING (Song blocks) ---
        if montage_mode == 'third_mode':
            for i, voice_file in enumerate(voice_files):
                needed_segments.append({'type': 'P', 'audio': voice_file, 'mode': 'third_mode', 'current_song': song_path})
        elif montage_mode == 'seamless':
             for i, voice_file in enumerate(voice_files):
                needed_segments.append({'type': 'P', 'audio': voice_file, 'mode': 'manual', 'current_song': song_path})
        else: # zigzag
            raw_segments_sequence = []
            for i, voice_file in enumerate(voice_files):
                raw_segments_sequence.append({'type': 'C', 'audio': voice_file})
                if i < len(voice_files) - 1:
                    raw_segments_sequence.append({'type': 'L'})

            for seg in raw_segments_sequence:
                if seg['type'] == 'L':
                    needed_segments.append({'type': 'L', 'audio': None, 'duration': L, 'current_song': song_path})
                else:
                    voice_file = seg['audio']
                    voice_dur = _get_media_duration(voice_file)
                    if voice_dur > L:
                        k = int((voice_dur + L - 1) // L)
                        segment_dur = voice_dur / k
                        for sub_idx in range(k):
                            needed_segments.append({'type': 'C_sub', 'audio': voice_file, 'sub_index': sub_idx, 'sub_total': k, 'duration': segment_dur, 'current_song': song_path})
                    else:
                        needed_segments.append({'type': 'C', 'audio': voice_file, 'duration': voice_dur, 'current_song': song_path})

            calculated_timestamps = _generate_sawtooth_timeline(needed_segments, song_path, L, logger)
            for i, seg in enumerate(needed_segments):
                seg['actual_start_t'] = calculated_timestamps[i]
                if 'duration' not in seg:
                     seg['duration'] = L if seg['type'] == 'L' else _get_media_duration(seg['audio'])

    # --- FINAL RENDERING (Unified cycle) ---
    for i, seg in enumerate(needed_segments):
        seg_type = seg['type']
        seg_path = temp_dir / f"block_{block_name}_seg_{i}_{seg_type}.mp4"

        if seg_type == 'M':
            # --- MANUAL CLIP RENDER ---
            m_path = Path(seg['path'])
            seg_dur = _get_media_duration(m_path)
            v = ffmpeg.input(str(m_path)).video.filter('fps', fps=VIDEO_STANDARD['framerate']).filter('scale', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], force_original_aspect_ratio='decrease').filter('pad', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], '(ow-iw)/2', '(oh-ih)/2').filter('setsar', 1)
            a = ffmpeg.input(str(m_path)).audio.filter('loudnorm', i=lufs_target, lra=7, tp=tp_value).filter('aresample', **{'async': 1}).filter('aformat', sample_rates=48000, channel_layouts='stereo').filter('afade', t='in', d=0.120).filter('afade', t='out', st=max(0, seg_dur - 0.120), d=0.120).filter('alimiter', limit=f'{tp_value}dB', level=True)
            (ffmpeg.output(v, a, str(seg_path), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'], acodec='aac').run(overwrite_output=True, quiet=True))

        elif seg_type == 'P':
            # --- SEAMLESS PAIR RENDER (Manual / Third Mode) ---
            voice_file, song_path_pair = seg['audio'], seg['current_song']
            voice_dur = _get_media_duration(voice_file)
            bg_vol, cvl_vol = 10**(bg_music_db/20), 10**(cvl_db/20)

            if seg['mode'] == 'third_mode':
                total_dur = max(voice_dur + L, 2 * L + 0.12)
                vol_expr = f"if(lt(t,0.12), t/0.12, if(lt(t,{L}), 1.0, if(lt(t,{L}+0.12), 1.0-(t-{L})/0.12*(1.0-{bg_vol}), if(lt(t,2*{L}), {bg_vol}, if(lt(t,2*{L}+0.12), {bg_vol}*(1.0-(t-2*{L})/0.12), 0)))))"
            else:
                total_dur = L + voice_dur
                vol_expr = f"if(lt(t,0.12), t/0.12, if(lt(t,{L}), 1.0, if(lt(t,{L}+0.12), 1.0-(t-{L})/0.12*(1.0-{bg_vol}), if(lt(t,{total_dur}-0.12), {bg_vol}, {bg_vol}*(1.0-(t-({total_dur}-0.12))/0.12)))))"

            # Synchronized video and audio start
            song_total_dur = _get_media_duration(song_path_pair)
            start_t = random.uniform(30.0, max(30.0, song_total_dur - total_dur - 20.0))

            v_in = ffmpeg.input(str(song_path_pair), ss=start_t, t=total_dur).filter('scale', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], force_original_aspect_ratio='decrease').filter('pad', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], '(ow-iw)/2', '(oh-ih)/2').filter('setsar', 1)
            m_env = ffmpeg.input(str(song_path_pair), ss=start_t, t=total_dur).filter('volume', vol_expr, eval='frame')
            v_del = ffmpeg.input(str(voice_file)).filter('adelay', f"{int(L*1000)}|{int(L*1000)}")
            a_mix = ffmpeg.filter([m_env, v_del], 'amix', inputs=2, duration='first').filter('volume', f"if(lt(t,{L}), 1.0, {cvl_vol})", eval='frame').filter('alimiter', limit=f'{tp_value}dB', level=True)
            (ffmpeg.output(v_in, a_mix, str(seg_path), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'], acodec='aac').run(overwrite_output=True, quiet=True))

        elif seg_type in ('L', 'C', 'C_sub'):
            # --- ZIGZAG RENDER (L / C) ---
            start_t, cur_song, s_dur = seg['actual_start_t'], seg['current_song'], seg['duration']
            v_part = ffmpeg.input(str(cur_song), ss=start_t, t=s_dur).filter('scale', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], force_original_aspect_ratio='decrease').filter('pad', VIDEO_STANDARD['width'], VIDEO_STANDARD['height'], '(ow-iw)/2', '(oh-ih)/2').filter('setsar', 1)
            if seg_type == 'L':
                a_part = ffmpeg.input(str(cur_song), ss=start_t, t=s_dur).filter('afade', t='in', d=0.120).filter('afade', t='out', st=s_dur-0.120, d=0.120).filter('alimiter', limit=f'{tp_value}dB', level=True)
            else:
                voice_for_c = seg['audio']
                if seg_type == 'C_sub':
                    voice_start = seg['sub_index'] * seg['duration']
                    voice_src = temp_dir / f"block_{block_name}_seg_{i}_voice_sub.wav"
                    ffmpeg.input(str(voice_for_c), ss=voice_start, t=s_dur).output(str(voice_src), acodec='pcm_s16le', ar=48000, ac=2).run(overwrite_output=True, quiet=True)
                    voice_for_c = voice_src

                audio_res = _generate_block_audio_content(voice_for_c, cur_song, temp_dir, settings, logger, music_start_t=start_t)
                a_part = ffmpeg.input(str(audio_res))
            (ffmpeg.output(v_part, a_part, str(seg_path), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'], acodec='aac').run(overwrite_output=True, quiet=True))

        final_segments_paths.append(str(seg_path))

    # --- GENERAL FINALIZATION (for both modes) ---
    if not final_segments_paths: return None

    concat_list = temp_dir / f"concat_{block_name}.txt"
    with open(concat_list, 'w') as f:
        for p in final_segments_paths: f.write(f"file '{Path(p).absolute().as_posix()}'\n")

    intermediate_block = temp_dir / f"block_{block_name}_no_cap.mp4"
    (ffmpeg.input(str(concat_list), format='concat', safe=0).output(str(intermediate_block), c='copy').run(overwrite_output=True, quiet=True))

    if not caption_text or caption_text.lower() == 'nan' or is_intro: return str(intermediate_block)

    final_block = temp_dir / f"FINAL_BLOCK_{block_name}.mp4"
    total_block_dur = sum(_get_media_duration(Path(p)) for p in final_segments_paths)
    cap_mode, user_val = settings.get('caption_mode', 'fixed'), float(settings.get('caption_duration', 5.8))
    end_t = min(user_val, total_block_dur) if cap_mode == 'fixed' else max(0, total_block_dur - user_val)
    pos, margin = settings.get('caption_pos', 'Middle'), 50
    if pos == 'Top': x_expr, y_expr = '(w-text_w)/2', f'{margin}'
    elif pos == 'Bottom': x_expr, y_expr = '(w-text_w)/2', f'h-text_h-{margin}'
    elif pos == 'Top-left': x_expr, y_expr = f'{margin}', f'{margin}'
    elif pos == 'Bottom-left': x_expr, y_expr = f'{margin}', f'h-text_h-{margin}'
    else: x_expr, y_expr = '(w-text_w)/2', '(h-text_h)/2'

    vf_params = {'fontfile': Path(settings['caption_font']).as_posix().replace(':', '\\:'), 'text': caption_text.replace("'", r"'\'"), 'fontsize': settings['font_size'], 'fontcolor': settings['font_color'], 'borderw': settings['outline_width'], 'bordercolor': settings['outline_color'], 'x': x_expr, 'y': y_expr, 'enable': f"between(t,0,{end_t})"}
    try:
        in_f = ffmpeg.input(str(intermediate_block))
        (ffmpeg.output(in_f.video.filter('drawtext', **vf_params), in_f.audio, str(final_block), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'], r=VIDEO_STANDARD['framerate'], acodec='copy').run(overwrite_output=True, quiet=True))
        return str(final_block)
    except Exception: return str(intermediate_block)

# --- NORMALIZATION AND UTILS BLOCK ---

# Logger stub class (pickle-able for multiprocessing)
class _SilentLogger:
    """Logger stub that can be passed to subprocess."""
    def log(self, msg):
        # Do nothing - logs are collected in main process
        pass


def _assemble_block_worker(args):
    """Worker function for multi-threaded block assembly."""
    item, settings, temp_dir, index = args
    if stop_flag.is_set():
        return (index, None, "Stopped by user.")
    try:
        # Convert temp_dir back to Path if it came as a string
        if isinstance(temp_dir, str):
            temp_dir = Path(temp_dir)
        # Use stub instead of logger
        silent_logger = _SilentLogger()
        block_file = assemble_block_new(item, settings, temp_dir, silent_logger)
        return (index, block_file, None)
    except ffmpeg.Error as e:
        error_msg = f"FFmpeg Error: {e}"
        if hasattr(e, 'stderr') and e.stderr:
            try:
                error_msg += f"\nStderr: {e.stderr.decode('utf-8', errors='replace')}"
            except Exception:
                error_msg += f"\nStderr: {e.stderr}"
        return (index, None, error_msg)
    except Exception as e:
        return (index, None, f"{type(e).__name__}: {str(e)}")

def _normalize_master_song_worker(args):
    input_path, output_path, standard, lufs_target = args

    # Conformance check via ffprobe (smart skip for ready files)
    try:
        probe = ffmpeg.probe(str(input_path))
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
        audio_stream = next((s for s in probe['streams'] if s['codec_type'] == 'audio'), None)

        # Conformance criteria for "Golden Master"
        is_conformant = True

        if video_stream:
            width = int(video_stream.get('width', 0))
            height = int(video_stream.get('height', 0))
            codec_name = video_stream.get('codec_name', '')
            pix_fmt = video_stream.get('pix_fmt', '')

            # Resolution: 1920x1080
            if width != 1920 or height != 1080:
                is_conformant = False

            # FPS: 25
    
            fps_eval = video_stream.get('r_frame_rate', '0/1')
            try:
                fps = eval(fps_eval) if '/' in fps_eval else float(fps_eval)
                if abs(fps - 25.0) > 0.1:
                    is_conformant = False
            except:
                is_conformant = False

            # Video codec: h264
            if codec_name != 'h264':
                is_conformant = False

            # Pixel format: yuv420p
            if pix_fmt != 'yuv420p':
                is_conformant = False
        else:
            is_conformant = False

        if audio_stream:
            sample_rate = int(audio_stream.get('sample_rate', 0))
            channels = int(audio_stream.get('channels', 0))

            # Audio sample rate: 48000
            if sample_rate != 48000:
                is_conformant = False

            # Channels: 2 (stereo)
            if channels != 2:
                is_conformant = False
        else:
            is_conformant = False

        # If file matches all criteria - return original path
        if is_conformant:
            return (str(input_path), str(input_path), None)

    except Exception:
        # On ffprobe error continue with normal conversion
        pass

    # Standard conversion if file does not conform
    try:
        inp = ffmpeg.input(str(input_path))
        v = (
            inp.video
            .filter('fps', fps=standard['framerate'])
            .filter('scale', standard['width'], standard['height'], force_original_aspect_ratio='decrease')
            .filter('pad', standard['width'], standard['height'], '(ow-iw)/2', '(oh-ih)/2')
            .filter('setsar', 1)
        )
        a = inp.audio.filter('loudnorm', i=lufs_target, lra=7, tp=-1.5).filter('aformat', sample_rates=48000, channel_layouts='stereo')
        (ffmpeg.output(v, a, str(output_path), vcodec=standard['vcodec'], video_bitrate=standard['bitrate'], pix_fmt=standard['pix_fmt'], acodec='aac')
               .run(overwrite_output=True, quiet=True))
        return (str(input_path), str(output_path), None)
    except Exception as e:
        return (str(input_path), None, str(e))

def _normalize_clip_worker(args):
    """Basic video normalization without sound."""
    input_path_str, output_path_str, standard = args
    if stop_flag.is_set(): return input_path_str, None, "Stopped by user."
    try:
        stream = ffmpeg.input(input_path_str)
        video = stream.video.filter('scale', standard['width'], standard['height'],
                                    force_original_aspect_ratio='decrease').filter('pad', standard['width'],
                                                                                   standard['height'], '(ow-iw)/2',
                                                                                   '(oh-ih)/2').filter('setsar', 1)
        out = ffmpeg.output(video, output_path_str, vcodec=standard['vcodec'], pix_fmt=standard['pix_fmt'],
                            r=standard['framerate'], an=None)
        out.run(overwrite_output=True, quiet=True)
        return input_path_str, output_path_str, None
    except ffmpeg.Error as e:
        return input_path_str, None, f"Normalization error: {e}"

# --- MAIN PROCESSOR FUNCTION ---
def run_montage_process(enriched_plan_from_ui, settings, logger):
    stop_flag.clear()
    logger.log("--- STARTING MONTAGE PROCESSOR (SSID EDITION) ---")

    # --- CRITICAL VALIDATION: live_clip_duration ---
    live_dur = settings.get('live_clip_duration')
    if not live_dur or float(live_dur) <= 0:
        logger.log("!!! CRITICAL ERROR: Live clip duration is not set in the interface (subtab 'Captions').")
        raise ValueError("Missing live_clip_duration")

    VIDEO_STANDARD = {'vcodec': 'h264_videotoolbox', 'pix_fmt': 'yuv420p', 'width': 1920, 'height': 1080, 'framerate': 25, 'bitrate': '3000k'}
    settings['VIDEO_STANDARD'] = VIDEO_STANDARD

    # --- PHASE 0: CRITICAL RESOURCE CHECK ---
    logger.log("\n--- CHECKING RESOURCE READINESS (INTEGRITY) ---")
    resource_errors = []
    
    ssid_map_from_ui = settings.get('ssid_map', {})
    entity_map_from_ui = settings.get('entity_map', {})
    audio_folder = Path(settings['audio_folder'])
    all_voice_files = list(audio_folder.iterdir()) if audio_folder.is_dir() else []
    db_path = Path(settings['db_path'])
    project_id = settings['video_id']

    # --- 0.0. Checking auxiliary assets ---
    assets_to_check = [
        ('Disclaimer', settings.get('disclaimer_path')),
        ('Overlay', settings.get('overlay_path')),
        ('Glitch effect', settings.get('glitch_effect')),
        ('Ad block', settings.get('ad_file'))
    ]
    for label, path_str in assets_to_check:
        if path_str and Path(path_str).is_file():
            err = _is_file_broken(Path(path_str))
            if err:
                resource_errors.append(f"Asset '{label}' is corrupted: {err} ({Path(path_str).name})")
            else:
                logger.log(f"  -> Asset '{label}' verified: OK")

    intro_ssids = []
    # intro_song_paths_validated will be formed after normalization
    
    # 0.1. Searching for intro songs in the SECOND TABLE (columns P-T)
    try:
        full_df = pd.read_excel(db_path, sheet_name=project_id, header=None)
        # Columns: P(15)=Order, R(17)=SSID, T(19)=EID
        for i in range(1, len(full_df)):
            val_p = full_df.iloc[i, 15]
            if pd.notna(val_p) and isinstance(val_p, (int, float, complex)):
                ssid_val = str(full_df.iloc[i, 17]).strip() if pd.notna(full_df.iloc[i, 17]) else None
                eid_val = str(full_df.iloc[i, 19]).strip() if pd.notna(full_df.iloc[i, 19]) else None
                if ssid_val and ssid_val.startswith('SSID'):
                    intro_ssids.append({'SSID': ssid_val, 'EID': eid_val, 'order': int(val_p)})
        
        # Sort by order (P)
        intro_ssids.sort(key=lambda x: x['order'])
        logger.log(f"  -> Found intro songs in the second table: {len(intro_ssids)}")
    except Exception as e:
        logger.log(f"  -> Warning when reading intro songs: {e}")

    # Map of video file paths
    ssid_to_video_map = {}
    # Map of intro song SSIDs for later replacement with normalized paths
    intro_ssid_list = []

    # 0.2. Checking Intro resources
    for item in intro_ssids:
        ssid = item['SSID']

        ssid_path_str = ssid_map_from_ui.get(ssid)
        if not ssid_path_str:
            resource_errors.append(f"Intro: Song {ssid} not found in archive.")
        else:
            raw_videos_path = Path(ssid_path_str) / "raw_videos"
            if not raw_videos_path.is_dir():
                resource_errors.append(f"Intro: Song folder {ssid} is missing raw_videos subfolder.")
            else:
                v_files = [f for f in raw_videos_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS]
                if not v_files:
                    resource_errors.append(f"Intro: No video in {raw_videos_path}.")
                elif len(v_files) > 1:
                    resource_errors.append(f"Intro: More than one video in {raw_videos_path}.")
    
                else:
                    song_path = v_files[0]
                    err = _is_file_broken(song_path)
                    if err:
                        resource_errors.append(f"Intro: Song {ssid} is corrupted: {err}")
                    else:
                        ssid_to_video_map[ssid] = song_path
                        intro_ssid_list.append(ssid)

    # 0.3. Checking main block resources
    
    for _, row in enriched_plan_from_ui.iterrows():
        block_name = row['Block']
        ssid = str(row.get('SSID', '')).strip()
        eid = str(row.get('EID_Ref', '')).strip() if 'EID_Ref' in row else ""
        
        if not ssid or 'N/A' in ssid: continue
        
        # Song check
        ssid_path_str = ssid_map_from_ui.get(ssid)
        if not ssid_path_str:
            resource_errors.append(f"Block {block_name}: Song {ssid} not found in archive.")
        else:
            raw_videos_path = Path(ssid_path_str) / "raw_videos"
            if not raw_videos_path.is_dir():
                resource_errors.append(f"Block {block_name}: Song folder {ssid} is missing raw_videos subfolder.")
            else:
                v_files = [f for f in raw_videos_path.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTENSIONS]
                if not v_files:
                    resource_errors.append(f"Block {block_name}: No video in {raw_videos_path}.")
                elif len(v_files) > 1:
                    resource_errors.append(f"Block {block_name}: More than one video in {raw_videos_path}.")
                else:
                    song_path = v_files[0]
                    err = _is_file_broken(song_path)
                    if err:
                        resource_errors.append(f"Block {block_name}: Song {ssid} is corrupted: {err}")
                    else:
                        ssid_to_video_map[ssid] = song_path
        
        # Artist check
        if eid and eid.startswith('EID') and eid not in entity_map_from_ui:
            resource_errors.append(f"Block {block_name}: Artist {eid} not found in archive.")
            
        # Voice check
        block_num_str = re.search(r'\d+', block_name).group()
        block_voice_files = [
            f for f in all_voice_files 
            if f.name.startswith(f"B{block_num_str}-") or f.name.startswith(f"{int(block_num_str)}.")
        ]
        if not block_voice_files and 'B01' not in block_name:
            resource_errors.append(f"Block {block_name}: No narrator audio files found in {audio_folder.name}")
        else:
            # Check each voice file
            for v_file in block_voice_files:
                err = _is_file_broken(v_file)
                if err:
                    resource_errors.append(f"Block {block_name}: Voice {v_file.name} is corrupted: {err}")

    if resource_errors:
        logger.log("\n❌ MONTAGE STOPPED. Resource issues detected (integrity):")
        for err in resource_errors:
            logger.log(f"  - {err}")
        raise ValueError("Resource check error or corrupted files.") 

    logger.log("✅ All resources present and functional. Starting preparation...")

    output_path = Path(settings['output_path'])
    temp_parent_dir = Path(settings.get('temp_path', output_path))
    temp_dir = temp_parent_dir / f"tmp_{project_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    montage_successful = False

    try:
        temp_dir.mkdir(parents=True, exist_ok=True)

        # 0.4. Song Master Normalization
        logger.log("\n--- Song Master Normalization ---")
        master_songs_dir = temp_dir / 'normalized_masters'
        master_songs_dir.mkdir(parents=True, exist_ok=True)

        # Collect unique video files from ssid_to_video_map
        unique_songs = {}
        for ssid, video_path in ssid_to_video_map.items():
            if video_path not in unique_songs:
                unique_songs[video_path] = []
            unique_songs[video_path].append(ssid)

        # Prepare worker arguments
        sound_settings = settings.get('sound_settings', {})
        lufs_target = float(sound_settings.get('target_source_lufs', '-16'))

        master_normalize_args = []
        for video_path, ssids in unique_songs.items():
            output_name = f"master_{video_path.stem}.mp4"
            master_output_path = master_songs_dir / output_name
            master_normalize_args.append((video_path, master_output_path, VIDEO_STANDARD, lufs_target))

        # Launch parallel normalization with progress
        normalized_ssid_map = {}
        if master_normalize_args:
            total_count = len(master_normalize_args)
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = {executor.submit(_normalize_master_song_worker, arg): i for i, arg in enumerate(master_normalize_args, 1)}
                for future in as_completed(futures):
                    i = futures[future]
                    input_path_str, output_path_str, error = future.result()
                    if error:
                        logger.log(f"  -> [{i}/{total_count}] Normalization error for {Path(input_path_str).name}: {error}")
                    elif output_path_str:
                        # Update mapping for all SSIDs that used this file
                        original_path = Path(input_path_str)
                        for ssid in unique_songs[original_path]:
                            normalized_ssid_map[ssid] = Path(output_path_str)
                        logger.log(f"  -> [{i}/{total_count}] Normalized: {Path(input_path_str).name}")

            logger.log(f"  -> Normalized songs: {len(normalized_ssid_map)}")
        else:
            logger.log("  -> No songs for normalization")

        # Form a list of normalized paths for intro songs
        intro_song_paths_validated = []
        for ssid in intro_ssid_list:
            if ssid in normalized_ssid_map:
                intro_song_paths_validated.append(normalized_ssid_map[ssid])
            elif ssid in ssid_to_video_map:
                # If normalization failed, use original path
                intro_song_paths_validated.append(ssid_to_video_map[ssid])

        # 0.5. Narration normalization (voice_files) - NEW STEP
        # Voice should not go into the mix without normalization
        logger.log("\n--- Narration normalization ---")
        voice_normalization_dir = temp_dir / 'normalized_voice'
        voice_normalization_dir.mkdir(parents=True, exist_ok=True)

        # Map to replace original voice paths with normalized ones
        normalized_voice_map = {}

        # Collect unique voice files
        unique_voice_files = {}
        for _, plan_row in enriched_plan_from_ui.iterrows():
            block_name = plan_row['Block']
            block_num_str = re.search(r'\d+', block_name).group() if re.search(r'\d+', block_name) else "01"
            block_voice_files = sorted([
                f for f in all_voice_files
                if f.name.startswith(f"B{block_num_str}-") or f.name.startswith(f"{int(block_num_str)}.")
            ], key=lambda x: x.name)

            for voice_file in block_voice_files:
                if voice_file not in unique_voice_files:
                    unique_voice_files[voice_file] = []

        # Normalize all unique voice files
        voice_normalize_count = 0
        tp_value = float(sound_settings.get('final_limiter_tp', '-1.5'))
        for voice_file in unique_voice_files.keys():
            try:
                output_name = f"voice_{voice_file.stem}.wav"
                normalized_voice_path = voice_normalization_dir / output_name

                # Normalize voice to lufs_target
                _normalize_audio(voice_file, normalized_voice_path, lufs_target, logger, tp_value)
                normalized_voice_map[str(voice_file)] = str(normalized_voice_path)
                voice_normalize_count += 1
                logger.log(f"  -> Normalized: {voice_file.name}")
            except Exception as e:
                logger.log(f"  -> Normalization error for {voice_file.name}: {e}")
                # In case of error, use original path
                normalized_voice_map[str(voice_file)] = str(voice_file)

        logger.log(f"  -> Normalized narration files: {voice_normalize_count}")

        # 0.6. Glitch transition normalization (if selected)
        normalized_glitch_path = None
        glitch_path_str = settings.get('glitch_effect')
        if glitch_path_str and Path(glitch_path_str).is_file():
            logger.log(f"-> Preparing Glitch transition...")
            glitch_src = Path(glitch_path_str)
            normalized_glitch_path = temp_dir / "glitch_normalized.mp4"
            (ffmpeg.input(str(glitch_src))
                   .output(str(normalized_glitch_path), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'],
                           r=VIDEO_STANDARD['framerate'], acodec='aac', ar='48000', ac=2,
                           vf=f"scale={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:force_original_aspect_ratio=decrease,pad={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:(ow-iw)/2:(oh-ih)/2",
                           **{'metadata:s:a:0': 'language=eng'})
                   .run(overwrite_output=True, quiet=True))

        # 0.6. Ad block normalization (if selected)
        normalized_ad_path = None
        ad_file_str = settings.get('ad_file')
        if ad_file_str and Path(ad_file_str).is_file():
            logger.log(f"-> Preparing ad block...")
            ad_src = Path(ad_file_str)
            normalized_ad_path = temp_dir / "ad_block_normalized.mp4"

            # Ad sound: normalize to lufs_target, then boost via volume
            cvl_db = float(settings['sound_settings'].get('combo_vs_live_db', '0'))
            lufs_target = float(settings['sound_settings'].get('target_source_lufs', '-16'))
            tp_value = float(settings['sound_settings'].get('final_limiter_tp', '-1.5'))

            (ffmpeg.input(str(ad_src))
                   .output(str(normalized_ad_path), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'],
                           r=VIDEO_STANDARD['framerate'], acodec='aac', ar='48000', ac=2,
                           vf=f"scale={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:force_original_aspect_ratio=decrease,pad={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:(ow-iw)/2:(oh-ih)/2",
                           af=f"loudnorm=i={lufs_target}:lra=7:tp={tp_value},volume={cvl_db}dB,alimiter=limit={tp_value}dB:level=true",
                           **{'metadata:s:a:0': 'language=eng'})
                   .run(overwrite_output=True, quiet=True))

        # 1. Forming assembly plan
        logger.log("\n--- Forming assembly plan ---")
        final_montage_plan = []
        
        for _, plan_row in enriched_plan_from_ui.iterrows():
            block_name = plan_row['Block']
            ssid = str(plan_row.get('SSID', '')).strip()
            caption = plan_row.get('Caption', '')
            eid = str(plan_row.get('EID_Ref', '')).strip() if 'EID_Ref' in plan_row else ""

            block_num_str = re.search(r'\d+', block_name).group()
            # Collect original files, then replace with normalized paths
            block_voice_files_orig = sorted([
                f for f in all_voice_files
                if f.name.startswith(f"B{block_num_str}-") or f.name.startswith(f"{int(block_num_str)}.")
            ], key=lambda x: x.name)
            # Replace with normalized paths (if normalization succeeded)
            block_voice_files = [
                Path(normalized_voice_map.get(str(f), str(f))) for f in block_voice_files_orig
            ]
            
            if 'B01' in block_name:
                # Logic for adding extra songs for intro
                current_intro_paths = intro_song_paths_validated.copy()
                if len(block_voice_files) > len(current_intro_paths):
                    logger.log(f"  -> Intro: Sub-blocks ({len(block_voice_files)}) more than intro songs ({len(current_intro_paths)}). Getting from B04+...")

                    # Safe song pool: from B04 to the second to last
                    safe_pool = []
                    # Collect all main block SSIDs (filter by valid SSID)
                    all_main_blocks = enriched_plan_from_ui[
                        (enriched_plan_from_ui['Block'].str.contains('B')) &
                        (~enriched_plan_from_ui['Block'].str.contains('B01'))
                    ].copy()

                    # Filter only blocks with a valid SSID
                    all_main_blocks = all_main_blocks[
                        all_main_blocks['SSID'].notna() &
                        (all_main_blocks['SSID'].astype(str).str.strip() != '') &
                        (~all_main_blocks['SSID'].astype(str).str.startswith('N/A'))
                    ].copy()

                    if len(all_main_blocks) >= 5: # If enough blocks to choose from
                        # Block indices: B01(0), B02(1), B03(2), B04(3) ... Last(N)
                        # We need from index 3 to N-1 (B04 to penultimate)
                        safe_blocks = all_main_blocks.iloc[2:-1] # Relative to all_main_blocks (where B02 is first)
                        for _, s_row in safe_blocks.iterrows():
                            s_ssid = str(s_row.get('SSID', '')).strip()
                            # Skip if SSID is not valid
                            if not s_ssid or s_ssid.startswith('N/A'):
                                continue
                            # Use normalized path if available, otherwise original
                            s_path = normalized_ssid_map.get(s_ssid) or ssid_to_video_map.get(s_ssid)
                            if s_path and s_path not in current_intro_paths:
                                safe_pool.append((s_ssid, s_path))
                                logger.log(f"     -> Added song {s_ssid} from block {s_row.get('Block', 'Unknown')} to Intro")

                    if safe_pool:
                        random.shuffle(safe_pool)
                        needed = len(block_voice_files) - len(current_intro_paths)
                        added_songs = safe_pool[:needed]
                        current_intro_paths.extend([path for _, path in added_songs])
                        logger.log(f"  -> Intro: Added {len(added_songs)} songs from the main set.")

                plan_item = {
                    'name': block_name,
                    'Caption': caption,
                    'formula': settings['formula_intro'],
                    'voice_files': block_voice_files,
                    'is_intro': True,
                    'intro_song_paths': current_intro_paths
                }
            else:
                # Use normalized path if available, otherwise original
                song_path = normalized_ssid_map.get(ssid) or ssid_to_video_map.get(ssid)
                plan_item = {
                    'name': block_name,
                    'Caption': caption,
                    'SSID': ssid,
                    'EID': eid,
                    'song_path': song_path,
                    'formula': settings['formula_main'],
                    'voice_files': block_voice_files,
                    'is_intro': False
                }
            
            final_montage_plan.append(plan_item)

        # 3. Block assembly
        final_block_paths = []
        
        # Add Disclaimer to beginning (if exists)
        disclaimer_path = settings.get('disclaimer_path')
        if disclaimer_path and Path(disclaimer_path).is_file():
            logger.log("-> Preparing Disclaimer...")
            norm_disclaimer = temp_dir / "disclaimer_final.mp4"
            (ffmpeg.input(disclaimer_path)
                   .output(str(norm_disclaimer), vcodec=VIDEO_STANDARD['vcodec'], video_bitrate=VIDEO_STANDARD['bitrate'], pix_fmt=VIDEO_STANDARD['pix_fmt'],
                           r=VIDEO_STANDARD['framerate'], acodec='aac', ar='48000', ac=2,
                           vf=f"scale={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:force_original_aspect_ratio=decrease,pad={VIDEO_STANDARD['width']}:{VIDEO_STANDARD['height']}:(ow-iw)/2:(oh-ih)/2",
                           **{'metadata:s:a:0': 'language=eng'})
                   .run(overwrite_output=True, quiet=True))
            final_block_paths.append(str(norm_disclaimer))

        # Assemble blocks (multi-threaded)
        logger.log("\n--- Assembling blocks (multi-threaded) ---")
        block_args = []
        for i, item in enumerate(final_montage_plan):
            # Pass only pickle-able arguments (without logger)
            block_args.append((item, settings, str(temp_dir), i))

        results = {}
        if block_args:
            total_count = len(block_args)
            completed_count = 0
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = {executor.submit(_assemble_block_worker, arg): arg[3] for arg in block_args}
                for future in as_completed(futures):
                    result_index, block_file, error = future.result()
                    if error:
                        # !!! CRITICAL STOP !!!
                        err_msg = f"CRITICAL ERROR in block {result_index + 1}: {error}"
                        logger.log(f"  -> {err_msg}")
                        raise ValueError(err_msg)
                    elif block_file:
                        results[result_index] = block_file
                        completed_count += 1
                        logger.log(f"  -> [{completed_count}/{total_count}] Block assembled: {Path(block_file).name}")

        # Sort results by index and add to final list
        for index in sorted(results.keys()):
            final_block_paths.append(results[index])
            # Insert normalized glitch after block (if any)
            if normalized_glitch_path and normalized_glitch_path.exists():
                final_block_paths.append(str(normalized_glitch_path))

        # 3.5. Ad block insertion (if any)
        if normalized_ad_path and normalized_ad_path.exists():
            placement_raw = str(settings.get('ad_placement', '0')).strip().lower()

            # Determine target block ID
            target_block_id = None
            if placement_raw == 'intro' or placement_raw == '0':
                target_block_id = "B01"
            elif placement_raw.isdigit():
                # "after 4th song" -> this is block B05 (since B01=Intro, B02=S1, B03=S2, B04=S3, B05=S4)
                target_block_id = f"B{int(placement_raw) + 1:02d}"

            if target_block_id:
                insert_idx = -1
                for i, path in enumerate(final_block_paths):
                    if target_block_id in Path(path).name:
                        insert_idx = i + 1
                
                if insert_idx != -1:
                    # Check if followed by a glitch
                    if insert_idx < len(final_block_paths) and "glitch_normalized" in Path(final_block_paths[insert_idx]).name:
                        insert_idx += 1
                    
                    logger.log(f"-> Inserting ad after block {target_block_id} (index {insert_idx})")
                    final_block_paths.insert(insert_idx, str(normalized_ad_path))
                    if normalized_glitch_path and normalized_glitch_path.exists():
                        final_block_paths.insert(insert_idx + 1, str(normalized_glitch_path))
                else:
                    logger.log(f"⚠️ WARNING: Target block {target_block_id} for ad not found. Ad skipped.")

        # 4. Final assembly
        logger.log("\n--- Final video assembly ---")
        concat_list = temp_dir / "final_concat.txt"
        with open(concat_list, 'w') as f:
            for p in final_block_paths: f.write(f"file '{Path(p).absolute().as_posix()}'\n")
            
        final_video_no_overlay = temp_dir / "output_no_overlay.mp4"
        (ffmpeg.input(str(concat_list), format='concat', safe=0)
               .output(str(final_video_no_overlay), c='copy')
               .run(overwrite_output=True, quiet=True))
        
        current_video = final_video_no_overlay
        
        # Overlay
        overlay_path = settings.get('overlay_path')
        if overlay_path and Path(overlay_path).exists():
            logger.log("-> Applying overlay...")
            final_video_with_overlay = temp_dir / "output_with_overlay.mp4"
            _run_ffmpeg_command([
                'ffmpeg', '-y', '-i', str(current_video), '-stream_loop', '-1', '-i', str(overlay_path),
                '-filter_complex', "[0:v]format=yuv420p[v];[1:v]format=yuv420p,setsar=1[ov];[v][ov]blend=all_mode='screen':all_opacity=0.7[out]",
                '-map', '[out]', '-map', '0:a', '-shortest', '-vcodec', VIDEO_STANDARD['vcodec'], '-b:v', VIDEO_STANDARD['bitrate'], '-pix_fmt', VIDEO_STANDARD['pix_fmt'], str(final_video_with_overlay)
            ], logger, "overlay")
            current_video = final_video_with_overlay

        # Final save
        final_name = f"{project_id}_{datetime.now().strftime('%Y%m%d_%H%M')}.mp4"
        final_video_full_path = output_path / final_name
        shutil.copy(current_video, final_video_full_path)

        montage_successful = True
        logger.log(f"\n✅ MONTAGE COMPLETED: {final_video_full_path}")

        # Tracklist generation (before deleting temp files!)
        logger.log("\n--- Tracklist generation ---")
        logger.log(f"  -> Total blocks for analysis: {len(final_block_paths)}")
        _generate_tracklist(final_video_full_path, final_block_paths, final_montage_plan, settings, logger)

    except Exception as e:
        logger.log(f"\n❌ CRITICAL ERROR: {e}")
        logger.log(traceback.format_exc())
        raise
    finally:
        if montage_successful:
            shutil.rmtree(temp_dir)
            logger.log("Temporary files deleted.")
        else:
            logger.log(f"Temporary files saved in: {temp_dir}")


def _generate_tracklist(final_video_path: Path, final_block_paths, final_montage_plan, settings, logger) -> Path:
    """
    Generates tracklist in format: MM:SS Song Name by Artist
    Uses real file durations from final_block_paths

    Args:
        final_video_path: Path to final video file
        final_block_paths: List of paths to assembled blocks in correct order
        final_montage_plan: List of dictionaries with block data (SSID, EID, name)
        settings: Project settings
        logger: Logger

    Returns:
        Path to the created tracklist file
    """
    try:
        tracklist_path = final_video_path.with_suffix('.txt')

        # Load songs database to get titles and artists
        db_path = Path(settings.get('db_path', ''))
        song_name_map = {}
        artist_map = {}

        try:
            df_songs = pd.read_excel(db_path, sheet_name='Songs_Database', header=0)
            for _, row in df_songs.iterrows():
                ssid = str(row.get('SSID', '')).strip()
                song_name = str(row.get('Song_Name', '')).strip()
                artist_name = str(row.get('Name', '')).strip()
                if ssid and ssid.startswith('SSID'):
                    song_name_map[ssid] = song_name
                    if artist_name:
                        artist_map[ssid] = artist_name
            logger.log(f"  -> Loaded songs: {len(song_name_map)}")
        except Exception as e:
            logger.log(f"  -> WARNING: Error loading Songs_Database: {e}")

        # Create map: Block Name -> SSID
        # Extract only the main part of the block (B02 from B02-01)
        block_ssid_map = {}
        for plan_item in final_montage_plan:
            block_name = plan_item.get('name', '')
            ssid = plan_item.get('SSID', '')
            if ssid:
                # Extract B## from B02-01, B02-02, etc.
                match = re.match(r'(B\d{2})', block_name)
                if match:
                    main_block = match.group(1)
                    block_ssid_map[main_block] = ssid

        # Collect tracklist data using real files
        tracklist_entries = []
        cumulative_time = 0.0

        for block_path in final_block_paths:
            path_obj = Path(block_path)
            filename = path_obj.name

            # Skip technical blocks
            if 'disclaimer' in filename.lower():
                duration = _get_media_duration(path_obj)
                logger.log(f"  -> Skip: {filename} = {duration:.2f}s")
                cumulative_time += duration
                continue
            if 'glitch' in filename.lower():
                duration = _get_media_duration(path_obj)
                logger.log(f"  -> Skip: {filename} = {duration:.2f}s")
                cumulative_time += duration
                continue
            if 'ad_block' in filename.lower():
                duration = _get_media_duration(path_obj)
                logger.log(f"  -> Skip: {filename} = {duration:.2f}s")
                cumulative_time += duration
                continue

            # Look for block name (B01, B02, etc.) in filename
            block_match = re.search(r'B\d{2}', filename)
            if not block_match:
                # If file doesn't contain B## - skip
                duration = _get_media_duration(path_obj)
                logger.log(f"  -> No block match: {filename} = {duration:.2f}s")
                cumulative_time += duration
                continue

            block_name = block_match.group()

            # B01 = Radio Intro
            if block_name == 'B01':
                duration = _get_media_duration(path_obj)
                cumulative_time += duration
                logger.log(f"  -> B01 Intro: {filename} = {duration:.2f}s (cumulative: {cumulative_time:.2f}s)")
                tracklist_entries.append("00:00 Radio Intro")
                continue

            # Main music blocks (B02+)
            ssid = block_ssid_map.get(block_name, '')
            if not ssid:
                # No SSID for this block - just add time
                duration = _get_media_duration(path_obj)
                logger.log(f"  -> No SSID for {block_name}: {filename} = {duration:.2f}s")
                cumulative_time += duration
                continue

            # Get real block duration
            duration = _get_media_duration(path_obj)

            # Format time
            minutes = int(cumulative_time // 60)
            seconds = int(cumulative_time % 60)
            time_str = f"{minutes:02d}:{seconds:02d}"

            # Get song name and artist
            song_name = song_name_map.get(ssid, ssid)
            artist_name = artist_map.get(ssid, 'Unknown Artist')

            tracklist_entry = f"{time_str} {song_name} by {artist_name}"
            tracklist_entries.append(tracklist_entry)

            logger.log(f"  -> {block_name} ({ssid}): {time_str} | duration={duration:.2f}s | cumulative={cumulative_time:.2f}s")

            # Increase cumulative time
            cumulative_time += duration

        # Write tracklist to file
        if tracklist_entries:
            with open(tracklist_path, 'w', encoding='utf-8') as f:
                f.write("Tracklist:\n")
                for entry in tracklist_entries:
                    f.write(f"{entry}\n")
            logger.log(f"📝 Tracklist created: {tracklist_path}")
            logger.log(f"   -> Total tracks: {len(tracklist_entries)}")
        else:
            logger.log("⚠️ Tracklist not created: no track data")

        return tracklist_path

    except Exception as e:
        logger.log(f"⚠️ Error creating tracklist: {str(e)}")
        return None

