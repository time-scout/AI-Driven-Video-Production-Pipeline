# interview_semantic_slicer.py

import os
import sys
import json
import toml
import time
import logging
import traceback
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Callable
import subprocess
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import warnings
from pydub import AudioSegment
from pydub.silence import detect_leading_silence

# AI/ML libraries
try:
    import torch
    import whisper
    import whisperx
    from pyannote.audio import Pipeline
    from whisperx.diarize import DiarizationPipeline
    import google.generativeai as genai
    from mutagen.wave import WAVE
except ImportError as e:
    print(f"❌ Missing required library: {e}")
    print("Please install required packages:")
    print("pip install torch openai-whisper whisperx pyannote.audio google-generativeai mutagen toml")
    sys.exit(1)
# <<< START OF NEW BLOCK: CHILD PROCESS MANAGER >>>
import atexit
import weakref

_child_processes = weakref.WeakSet()

def _cleanup_child_processes():
    """Forcefully terminates all tracked child processes."""
    print("Final cleanup: Terminating any lingering child processes...")
    for proc in list(_child_processes):
        if proc and proc.poll() is None: # If process exists and is running
            try:
                print(f"  - Terminating lingering process PID: {proc.pid}")
                proc.terminate()
                proc.wait(timeout=2)
            except Exception as e:
                try:
                    print(f"  - Force killing process PID: {proc.pid} due to error: {e}")
                    proc.kill()
                except Exception as final_e:
                    print(f"  - Failed to kill process PID: {proc.pid}: {final_e}")

atexit.register(_cleanup_child_processes)

def _run_subprocess(command: List[str], **kwargs) -> subprocess.CompletedProcess:
    """A wrapper for subprocess.run that tracks the created process."""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
    _child_processes.add(process)
    try:
        stdout, stderr = process.communicate()
        retcode = process.poll()
        if retcode:
            raise subprocess.CalledProcessError(retcode, command, output=stdout, stderr=stderr)
        return subprocess.CompletedProcess(command, retcode, stdout, stderr)
    finally:
        _child_processes.discard(process)
# <<< END OF NEW BLOCK >>>

# Setup logging filters
warnings.filterwarnings("ignore", category=UserWarning, module='torchaudio')


def _log(progress_callback: Callable[[str], None], message: str):
    """Helper function for unified logging"""
    # The interface adds timestamps itself, we pass the "clean" message
    progress_callback(f"[Semantic Slicer]: {message}")


def extract_audio(video_path: Path, entity_path: Path, progress_callback: Callable[[str], None]) -> Path:
    """Extract audio from video file"""
    _log(progress_callback, f"🎵 Extracting audio from: {video_path.name}")

    # Create interview_fragments directory if it doesn't exist
    fragments_path = entity_path / "interview_fragments"
    fragments_path.mkdir(parents=True, exist_ok=True)

    transcriptions_path = fragments_path / "transcriptions"
    transcriptions_path.mkdir(parents=True, exist_ok=True)
    audio_path = transcriptions_path / f"{video_path.stem}_audio.wav"

    command = [
        'ffmpeg', '-y', '-i', str(video_path),
        '-vn',  # No video
        '-acodec', 'pcm_s16le',  # PCM 16-bit
        '-ar', '16000',  # 16kHz sample rate
        '-ac', '1',  # Mono
        str(audio_path)
    ]

    try:
        result = _run_subprocess(command)
        _log(progress_callback, f"✅ Audio extracted: {audio_path}")
        return audio_path
    except subprocess.CalledProcessError as e:
        _log(progress_callback, f"❌ Audio extraction failed: {e.stderr}")
        raise


def run_transcription_and_diarization(task: Dict, audio_path: Path, whisper_model: 'whisperx.asr.ASR', diarize_model: 'DiarizationPipeline', progress_callback: Callable[[str], None]) -> List[Dict]:
    """Runs transcription and diarization, returns a list of word segments with re-labeled speakers."""
    settings = task.get('settings', {})
    _log(progress_callback, "Starting transcription and diarization...")



    try:
        asr_device = "cpu"
        diar_device = "mps" if torch.backends.mps.is_available() else "cpu"
        _log(progress_callback, f"Using ASR device: '{asr_device}' | Diarization device: '{diar_device}'")


        # 2. Transcribe and align
        audio = whisperx.load_audio(str(audio_path))
        language = settings.get('language', 'en')
        result = whisper_model.transcribe(audio, batch_size=16, language=language)

        # KEY FIX: We must use the language returned by whisper,
        # but if it's undefined, use the default.
        # This ensures the align_model always gets the correct language.
        language_code_for_align = language
        model_a, metadata = whisperx.load_align_model(language_code=language_code_for_align, device=asr_device)
        aligned_result = whisperx.align(result["segments"], model_a, metadata, audio, device=asr_device, return_char_alignments=False)

        # 3. Speaker diarization
        _log(progress_callback, "Starting speaker diarization...")
        min_speakers = settings.get('min_speakers', 2)
        max_speakers = settings.get('max_speakers', 5)
        diarize_segments = diarize_model(audio, min_speakers=min_speakers, max_speakers=max_speakers)
        _log(progress_callback, "Diarization completed.")

        # 4. Combine results
        final_result = whisperx.assign_word_speakers(diarize_segments, aligned_result)

        # 5. Format and re-label speakers
        _log(progress_callback, "Formatting results and re-labeling speakers...")
        words = []
        word_counter = 0
        if "segments" in final_result:
            for segment in final_result["segments"]:
                for word_info in segment.get('words', []):
                    words.append({
                        'index': word_counter,
                        'word': word_info.get('word', '').strip(),
                        'start': word_info.get('start'),
                        'end': word_info.get('end'),
                        'speaker': word_info.get('speaker', 'UNKNOWN')
                    })
                    word_counter += 1

        unique_speakers = sorted(list(set(w['speaker'] for w in words if w['speaker'] != 'UNKNOWN')))
        speaker_map = {speaker_id: f"SPEAKER_{i+1}" for i, speaker_id in enumerate(unique_speakers)}

        for word in words:
            if word['speaker'] in speaker_map:
                word['speaker'] = speaker_map[word['speaker']]

        _log(progress_callback, f"Speakers re-labeled to: {list(speaker_map.values())}")

        # Save transcription
        entity_path = task.get('entity_path')
        transcriptions_path = entity_path / "interview_fragments" / "transcriptions"
        transcriptions_path.mkdir(parents=True, exist_ok=True)

        transcription_file = transcriptions_path / "transcription.json"
        with open(transcription_file, 'w', encoding='utf-8') as f:
            json.dump(words, f, indent=2, ensure_ascii=False)

        _log(progress_callback, f"✅ Transcription and diarization complete. Found {len(words)} words.")
        return words

    except Exception as e:
        _log(progress_callback, f"❌ Combined transcription and diarization failed.")
        _log(progress_callback, f"Full traceback:\n{traceback.format_exc()}")
        raise




def get_llm_response(task: Dict, text_input: str, prompt_filename: str, progress_callback: Callable[[str], None]) -> Dict:
    """Generic function to get a response from the LLM based on a prompt file."""
    try:
        # Resolve prompt path from task config (work_root) — no hardcoded paths
        work_root = task.get('work_root', '')
        prompts_dir = Path(work_root) / 'common_assets' / 'prompts' if work_root else Path('.')
        prompt_path = prompts_dir / prompt_filename
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()
    except FileNotFoundError:
        _log(progress_callback, f"Prompt file not found: {prompt_path}")
        return {}

    prompt = prompt_template.format(text_input=text_input)

    try:
        api_key = task.get('api_keys', {}).get('google_api_key')
        if not api_key:
            _log(progress_callback, "❌ Google API key not found in task")
            return {}

        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('models/gemini-flash-latest')
        response = model.generate_content(prompt)
        response_text = response.text.strip()

        if response_text.startswith('```json'):
            response_text = response_text[7:-3].strip()

        _log(progress_callback, f"LLM response received from {prompt_filename}")
        result = json.loads(response_text)
        return result
    except Exception as e:
        import traceback
        error_details = f"CRITICAL ERROR during Google AI call: {e}\n{traceback.format_exc()}"
        _log(progress_callback, error_details)
        return {}


def validate_and_log_quotes(task: Dict, llm_quotes: List[Dict], transcription_map: Dict, quote_type: str, progress_callback: Callable[[str], None]) -> List[Dict]:
    """Validates quotes by duration for all types and logs the process."""
    settings = task.get('settings', {})
    _log(progress_callback, f"--- Validating {len(llm_quotes)} {quote_type.upper()} quotes ---")
    validated_quotes = []
    word_map = {word['index']: word for word in transcription_map}

    for i, quote in enumerate(llm_quotes):
        final_indices = []
        quote_text = ""

        # Handle different quote types
        if quote_type == 'short':
            if 'start_index' in quote and 'end_index' in quote:
                start_idx = quote['start_index']
                end_idx = quote['end_index']
                final_indices = [[start_idx, end_idx]]
                quote_text = ' '.join(word_map[idx]['word'] for idx in range(start_idx, end_idx + 1) if idx in word_map)
            else:
                _log(progress_callback, f"  [SHORT {i+1:02d}] REJECTED. Malformed quote object: {quote}")
                continue

        elif quote_type in ['edit1', 'edit2', 'edit3']:
            if 'final_indices' in quote and isinstance(quote['final_indices'], list) and quote['final_indices']:
                final_indices = quote['final_indices']
                quote_text_parts = [' '.join(word_map[idx]['word'] for idx in range(seg[0], seg[1] + 1) if idx in word_map) for seg in final_indices]
                quote_text = ' ... '.join(quote_text_parts)
            else:
                _log(progress_callback, f"  [{quote_type.upper()} {i+1:02d}] REJECTED. Malformed edited quote object: {quote}")
                continue
        else:
            continue

        # Calculate total duration as sum of segment durations
        total_duration = 0.0
        all_indices = [idx for segment in final_indices for idx in segment]
        if not all_indices:
            _log(progress_callback, f"  [{quote_type.upper()} {i+1:02d}] REJECTED. No indices found.")
            continue

        for segment in final_indices:
            start_idx, end_idx = segment
            if start_idx not in word_map or end_idx not in word_map:
                _log(progress_callback, f"  [{quote_type.upper()} {i+1:02d}] REJECTED. Invalid segment indices: {segment}")
                total_duration = 0.0
                break
            segment_start = word_map[start_idx]['start']
            segment_end = word_map[end_idx]['end']
            total_duration += segment_end - segment_start

        if total_duration == 0.0:
            continue

        duration = total_duration

        # Check duration against settings
        min_duration = float(settings.get('min_clip_duration', 6.0))
        max_duration = float(settings.get('max_clip_duration', 12.0))

        if min_duration <= duration <= max_duration:
            _log(progress_callback, f"  [{quote_type.upper()} {i+1:02d}] ACCEPTED. Duration: {duration:.2f}s.")
            validated_quotes.append(quote)
        else:
            _log(progress_callback, f"  [{quote_type.upper()} {i+1:02d}] REJECTED. Duration: {duration:.2f}s.")

    # Save validated quotes
    entity_path = task.get('entity_path')
    llm_path = entity_path / "interview_fragments" / "transcriptions" / "llm_analysis"
    llm_path.mkdir(parents=True, exist_ok=True)

    validated_file = llm_path / f"validated_{quote_type}_quotes.json"
    with open(validated_file, 'w', encoding='utf-8') as f:
        json.dump(validated_quotes, f, indent=2, ensure_ascii=False)
    _log(progress_callback, f"Validation complete for {quote_type.upper()}. Results saved to {validated_file}")

    return validated_quotes


def get_main_speaker_for_quote(final_indices: List[List[int]], word_map: Dict) -> str:
    """Determines the dominant speaker for a given quote based on final_indices."""
    speaker_counts = {}
    # Removed the line that was causing the error. Now function directly uses passed word_map.

    for segment in final_indices:
        for idx in range(segment[0], segment[1] + 1):
            if idx in word_map:
                speaker = word_map[idx].get('speaker', 'UNKNOWN')
                speaker_counts[speaker] = speaker_counts.get(speaker, 0) + 1

    if not speaker_counts:
        return "UNKNOWN"

    main_speaker = max(speaker_counts, key=speaker_counts.get)
    return main_speaker


def cut_final_clips(task: Dict, video_path: Path, quotes: List[Dict], words: List[Dict], audio_path: Path, quote_type: str, progress_callback: Callable[[str], None]) -> List[Path]:
    """Cut final video clips based on LLM quotes with support for surgical editing"""
    settings = task.get('settings', {})
    entity_path = task.get('entity_path')
    fragments_path = entity_path / "interview_fragments"

    _log(progress_callback, f"🎬 Starting final video cutting for {quote_type.upper()} quotes...")
    audio_segment = AudioSegment.from_wav(audio_path)
    created_clips = []
    word_map = {word['index']: word for word in words}

    for i, quote in enumerate(tqdm(quotes, desc=f"🎬 Cutting {quote_type.upper()} clips")):
        try:
            # Determine final_indices for all types
            if quote_type == 'short':
                start_idx, end_idx = quote['start_index'], quote['end_index']
                final_indices = [[start_idx, end_idx]]
            elif quote_type in ['edit1', 'edit2', 'edit3']:
                final_indices = quote.get('final_indices', [])
                if not final_indices: continue
            else: continue

            # Determine filename
            main_speaker = get_main_speaker_for_quote(final_indices, word_map)
            speaker_num = main_speaker.split('_')[-1]

            if quote_type == 'short': marker = "_WHOLE"
            elif quote_type == 'edit1': marker = "_EDIT-1"
            elif quote_type == 'edit2': marker = "_EDIT-2"
            elif quote_type == 'edit3': marker = "_EDIT-3"
            else: marker = "_UNKNOWN"

            clip_filename = f"SPK-{speaker_num}{marker}_quote_{i+1:03d}.mp4"
            clip_path = fragments_path / clip_filename

            # Cutting logic
            if len(final_indices) == 1:  # This is a "whole" quote
                start_idx, end_idx = final_indices[0]
                if start_idx not in word_map or end_idx not in word_map: continue

                clip_start, clip_end = word_map[start_idx]['start'], word_map[end_idx]['end']

                # Apply silence detection for better cut points
                search_start_ms = int(clip_start * 1000)
                cut_start_ms = max(0, search_start_ms - 300)

                leading_chunk = audio_segment[cut_start_ms:search_start_ms]
                if len(leading_chunk) > 0:
                    silence_before = detect_leading_silence(leading_chunk.reverse(), silence_threshold=-50, chunk_size=10)
                    if silence_before:
                        clip_start = (search_start_ms - silence_before) / 1000.0

                search_end_ms = int(clip_end * 1000)
                cut_end_ms = search_end_ms + 300
                trailing_chunk = audio_segment[search_end_ms:cut_end_ms]
                if len(trailing_chunk) > 0:
                    silence_after = detect_leading_silence(trailing_chunk, silence_threshold=-50, chunk_size=10)
                    if silence_after:
                        clip_end = (search_end_ms + silence_after) / 1000.0

                duration = clip_end - clip_start
                min_duration = float(settings.get('min_clip_duration', 6.0))
                max_duration = float(settings.get('max_clip_duration', 12.0))
                if not (min_duration <= duration <= max_duration): continue

                video_crf = settings.get('video_crf', 23)
                command = [
                    'ffmpeg', '-y', '-ss', str(clip_start), '-i', str(video_path),
                    '-t', str(duration),
                    '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1',
                    '-c:v', 'libx264', '-preset', 'medium', '-crf', str(video_crf),
                    '-pix_fmt', 'yuv420p', '-r', '25', '-c:a', 'aac', '-b:a', '192k',
                    '-movflags', '+faststart', str(clip_path)
                ]
                _run_subprocess(command)

            else:  # This is a "surgical" quote
                _create_surgical_cut_video(task, video_path, final_indices, word_map, clip_path, progress_callback)

            # Calculate timing info for .txt file
            if len(final_indices) == 1:  # Whole quote - use existing clip_start, clip_end, duration
                txt_clip_start, txt_clip_end, txt_duration = clip_start, clip_end, duration
            else:  # Surgical quote - calculate from final_indices
                all_segment_starts = []
                all_segment_ends = []
                total_duration = 0.0

                for segment in final_indices:
                    start_idx, end_idx = segment
                    if start_idx in word_map and end_idx in word_map:
                        seg_start = word_map[start_idx]['start']
                        seg_end = word_map[end_idx]['end']
                        all_segment_starts.append(seg_start)
                        all_segment_ends.append(seg_end)
                        total_duration += seg_end - seg_start

                txt_clip_start = min(all_segment_starts) if all_segment_starts else 0.0
                txt_clip_end = max(all_segment_ends) if all_segment_ends else 0.0
                txt_duration = total_duration

            # Create .txt file (common for all)
            txt_filename = f"SPK-{speaker_num}{marker}_quote_{i+1:03d}.txt"
            transcriptions_path = fragments_path / "transcriptions"
            transcriptions_path.mkdir(parents=True, exist_ok=True)
            txt_path = transcriptions_path / txt_filename

            # Extract quote text based on final_indices
            quote_text_parts = []
            for segment in final_indices:
                segment_text = ' '.join(
                    word_map[idx]['word']
                    for idx in range(segment[0], segment[1] + 1)
                    if idx in word_map
                )
                quote_text_parts.append(segment_text)

            quote_text = ' ... '.join(quote_text_parts)

            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(f"Quote {i+1} ({quote_type.upper()})\n")
                f.write(f"Time: {txt_clip_start:.2f}s - {txt_clip_end:.2f}s ({txt_duration:.2f}s)\n")
                f.write(f"Speaker: {main_speaker}\n")
                f.write(f"Segments: {final_indices}\n\n")
                f.write(quote_text)

            created_clips.append(clip_path)
            _log(progress_callback, f"✅ Created {quote_type.upper()} clip: {clip_filename}")

        except Exception as e:
            _log(progress_callback, f"❌ Error processing {quote_type.upper()} quote {i+1}: {e}")

    _log(progress_callback, f"✅ {quote_type.upper()} video cutting completed: {len(created_clips)} clips created")
    return created_clips


def _create_surgical_cut_video(task: Dict, video_path: Path, final_indices: List[List[int]], word_map: Dict, output_path: Path, progress_callback: Callable[[str], None]):
    """Creates a video by surgically cutting and concatenating multiple segments using smart transcoding."""
    settings = task.get('settings', {})
    entity_path = task.get('entity_path')
    fragments_path = entity_path / "interview_fragments"

    _log(progress_callback, f"  Performing surgical cut for {output_path.name}...")
    temp_files = []
    concat_list_path = fragments_path / "concat_list.txt"

    try:
        # Step 1: Cut and re-transcode each segment individually
        for j, segment_indices in enumerate(final_indices):
            temp_output_path = fragments_path / f"temp_{j}.mp4"
            temp_files.append(temp_output_path)

            start_time = word_map[segment_indices[0]]['start']
            end_time = word_map[segment_indices[1]]['end']
            duration = end_time - start_time

            # <<< START OF PROTECTIVE BLOCK >>>
            if duration <= 0:
                _log(progress_callback, f"   ⚠️ Skipped defective segment with negative/zero duration: {duration:.2f}s")
                continue # Skip this segment without crashing
            # <<< END OF PROTECTIVE BLOCK >>>

            video_crf = settings.get('video_crf', 23)
            command = [
                'ffmpeg', '-y',
                '-ss', str(start_time),
                '-i', str(video_path),
                '-t', str(duration),
                '-vf', 'scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1',
                '-c:v', 'libx264',
                '-preset', 'medium',
                '-crf', str(video_crf),
                '-pix_fmt', 'yuv420p',
                '-r', '25',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-movflags', '+faststart',
                str(temp_output_path)
            ]
            _run_subprocess(command)

        # Step 2: Fast concatenation without re-transcoding
        with open(concat_list_path, 'w') as f:
            for temp_file in temp_files:
                f.write(f"file '{temp_file.name}'\n")

        # Step 3: Fast concatenation
        command = [
            'ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', str(concat_list_path),
            '-c', 'copy', str(output_path)
        ]
        _run_subprocess(command)
        _log(progress_callback, f"    Successfully concatenated {len(temp_files)} segments.")

    finally:
        # Cleanup
        for temp_file in temp_files:
            if temp_file.exists():
                temp_file.unlink()
        if concat_list_path.exists():
            concat_list_path.unlink()


def process_semantic_slicing_task(task: Dict, progress_callback: Callable[[str], None], stop_event: threading.Event):
    """Main control function for semantic slicing task."""
    start_time = time.time()

    name = task.get('name', 'Unknown')
    entity_path = task.get('entity_path')
    interview_files = task.get('interview_files', [])
    settings = task.get('settings', {})
    api_keys = task.get('api_keys', {})
    api_key = api_keys.get('google_api_key')

    _log(progress_callback, f"▶️ Starting semantic slicing for '{name}'. Files: {len(interview_files)}")

    # Task validity checks
    if not interview_files:
        _log(progress_callback, f"❌ No interview files found to process for '{name}'")
        return

    if not entity_path or not entity_path.exists():
        _log(progress_callback, f"❌ Entity path not found: {entity_path}")
        return

    # Set up API
    if not api_key:
        _log(progress_callback, "❌ Google API key not found. Processing impossible.")
        return
    try:
        genai.configure(api_key=api_key)
        _log(progress_callback, "✅ Google API key successfully configured.")
    except Exception as e:
        _log(progress_callback, f"❌ Google API configuration error: {e}")
        return

    total_video_duration = 0
    all_clips_created = []

    # One-time AI models loading
    try:
        asr_device = "cpu"
        diar_device = "mps" if torch.backends.mps.is_available() else "cpu"
        _log(progress_callback, f"Loading AI models on devices - ASR: '{asr_device}' | Diarization: '{diar_device}'")

        # Load Whisper model
        whisper_model_name = settings.get('whisper_model', 'base')
        _log(progress_callback, f"Loading Whisper model '{whisper_model_name}'...")
        whisper_model = whisperx.load_model(whisper_model_name, asr_device, compute_type="int8")
        _log(progress_callback, "Whisper model loaded successfully.")

        # Load Diarization model
        hf_token = api_keys.get('hf_token')
        if not hf_token:
            _log(progress_callback, "❌ Hugging Face token not found. Diarization impossible.")
            diarize_model = None
        else:
            diarize_model = DiarizationPipeline(use_auth_token=hf_token, device=diar_device)
            _log(progress_callback, "Diarization model loaded successfully.")

    except Exception as e:
        _log(progress_callback, f"❌ Failed to load AI models: {e}")
        _log(progress_callback, f"Full traceback:\n{traceback.format_exc()}")
        return
    try:
        for video_path in interview_files:
            if stop_event.is_set(): break
            _log(progress_callback, f"--- Processing file: {video_path.name} ---")

            try:
                # 1. Audio extraction
                audio_path = extract_audio(video_path, entity_path, progress_callback)

                # Get real duration for report
                try:
                    from mutagen.wave import WAVE
                    total_video_duration += WAVE(str(audio_path)).info.length
                except:
                    _log(progress_callback, "⚠️ Failed to determine audio duration")

                # 2. Transcription and diarization
                if diarize_model is None:
                    _log(progress_callback, "Skipping transcription because diarization model was not loaded.")
                    continue

                full_transcription = run_transcription_and_diarization(task, audio_path, whisper_model, diarize_model, progress_callback)
                if not full_transcription:
                    _log(progress_callback, f"❌ Transcription error for {video_path.name}")
                    continue

                text_parts = [f"[{w['index']}][{w['speaker']}]{w['word']}" for w in full_transcription]
                text_for_llm = ' '.join(text_parts)

                # 3. LLM Analysis (depending on settings)
                slicing_type = settings.get('slicing_type', 'full')
                all_quotes = []

                # Pass 1: Whole quotes (always executed)
                _log(progress_callback, "🎯 LLM Request: Searching for whole quotes...")
                short_quotes_raw = get_llm_response(task, text_for_llm, "Prompt_v2_Short.txt", progress_callback)
                validated_short = validate_and_log_quotes(task, short_quotes_raw.get('quotes', []), full_transcription, 'short', progress_callback)
                all_quotes.extend(validated_short)

                if slicing_type == 'full':
                    # Passes 2, 3, 4: Edited quotes
                    _log(progress_callback, "✂️ LLM Request: Searching for quotes with 1 edit...")
                    edit1_raw = get_llm_response(task, text_for_llm, "Prompt_v2_Edit1.txt", progress_callback)
                    validated_edit1 = validate_and_log_quotes(task, edit1_raw.get('edited_quotes', []), full_transcription, 'edit1', progress_callback)
                    all_quotes.extend(validated_edit1)

                    _log(progress_callback, "✂️✂️ LLM Request: Searching for quotes with 2 edits...")
                    edit2_raw = get_llm_response(task, text_for_llm, "Prompt_v2_Edit2.txt", progress_callback)
                    validated_edit2 = validate_and_log_quotes(task, edit2_raw.get('edited_quotes', []), full_transcription, 'edit2', progress_callback)
                    all_quotes.extend(validated_edit2)

                    _log(progress_callback, "✂️✂️✂️ LLM Request: Searching for quotes with 2-3 edits...")
                    edit3_raw = get_llm_response(task, text_for_llm, "Prompt_v2_Edit3.txt", progress_callback)
                    validated_edit3 = validate_and_log_quotes(task, edit3_raw.get('edited_quotes', []), full_transcription, 'edit3', progress_callback)
                    all_quotes.extend(validated_edit3)

                # 4. Final video cutting
                if not all_quotes:
                    _log(progress_callback, f"⚠️ No suitable quotes found for '{video_path.name}'.")
                    continue

                _log(progress_callback, f"🎬 Starting cutting {len(all_quotes)} clips for '{video_path.name}'...")

                # Cut by quote types
                whole_clips = cut_final_clips(task, video_path, validated_short, full_transcription, audio_path, 'short', progress_callback) if validated_short else []
                edit1_clips = cut_final_clips(task, video_path, validated_edit1, full_transcription, audio_path, 'edit1', progress_callback) if validated_edit1 else []
                edit2_clips = cut_final_clips(task, video_path, validated_edit2, full_transcription, audio_path, 'edit2', progress_callback) if validated_edit2 else []
                edit3_clips = cut_final_clips(task, video_path, validated_edit3, full_transcription, audio_path, 'edit3', progress_callback) if validated_edit3 else []

                file_clips = whole_clips + edit1_clips + edit2_clips + edit3_clips
                all_clips_created.extend(file_clips)

                _log(progress_callback, f"✅ Processed file {video_path.name}: {len(file_clips)} clips created")

            except Exception as e:
                _log(progress_callback, f"❌ Critical error processing {video_path.name}: {e}")

    finally:
        # AI Resource Cleanup
        _log(progress_callback, "Cleaning up AI resources...")
        try:
            # Models are now always declared, can delete without check
            del whisper_model
            del diarize_model
            import gc
            gc.collect()
            if torch.backends.mps.is_available():
                torch.mps.empty_cache()
            elif torch.cuda.is_available():
                torch.cuda.empty_cache()
            _log(progress_callback, "✅ AI resources cleanup completed.")
        except Exception as e:
            _log(progress_callback, f"⚠️ Error during cleanup: {e}")

    if not stop_event.is_set():
        # Final report
        end_time = time.time()
        total_time = end_time - start_time

        _log(progress_callback, f"🎉 Semantic slicing for '{name}' completed!")
        _log(progress_callback, f"📊 Total clips created: {len(all_clips_created)}")
        _log(progress_callback, f"⏱️ Total processing time: {total_time:.1f} sec")

        if total_video_duration > 0:
            speedup = total_video_duration / total_time
            _log(progress_callback, f"⚡ Processing speed: {speedup:.1f}x real-time")
        else:
            _log(progress_callback, "⚠️ Failed to calculate processing speed")

    # <<< START OF INSERTED BLOCK >>>
    # This part now only executes after all thread work is done.
    if not stop_event.is_set():
         _log(progress_callback, "All worker tasks completed. Forcing clean process exit.")
         os._exit(0)
    # <<< END OF INSERTED BLOCK >>>


def _process_single_interview_file(video_file: Path, fragments_path: Path, transcriptions_path: Path,
                                  settings: Dict, progress_callback: Callable[[str], None],
                                  stop_event: threading.Event):
    """
    Processing of a single interview file.
    This function contains stubs for future implementation.
    """
    file_name = video_file.name
    base_name = video_file.stem

    # Simulated processing stages
    steps = [
        "Extracting audio from video",
        "Transcription using Whisper",
        "Diarization (speaker separation)",
        "Semantic text analysis",
        "Fragment boundary detection",
        "Creating clips",
        "Saving results"
    ]

    for step in steps:
        if stop_event.is_set():
            return

        # Simulated processing time
        time.sleep(0.1)  # 100ms simulated delay

        progress_callback(f"[Semantic Slicer]:   • {step}...")

        # Simulated creation of result files
        if step == "Creating clips":
            # Create several fragments (simulated)
            for j in range(3):  # Create 3 fragments
                fragment_name = f"{base_name}_fragment_{j+1:03d}.mp4"
                fragment_path = fragments_path / fragment_name

                # Simulated file creation (ffmpeg would be here in reality)
                try:
                    # Create empty file for simulation
                    fragment_path.touch()
                    progress_callback(f"[Semantic Slicer]:     ✓ Created fragment: {fragment_name}")
                except Exception as e:
                    progress_callback(f"[Semantic Slicer]:     ❌ Error creating fragment: {e}")

        elif step == "Transcription using Whisper":
            # Simulated transcription file creation
            transcription_file = transcriptions_path / f"{base_name}_transcription.json"

            # Simulated transcription data
            mock_transcription = {
                "file": str(video_file),
                "language": settings.get('language', 'en'),
                "duration": 120.5,  # seconds
                "segments": [
                    {
                        "start": 0.0,
                        "end": 30.0,
                        "text": "Mock transcription text for testing purposes...",
                        "speaker": "Speaker 1"
                    }
                ]
            }

            try:
                with open(transcription_file, 'w', encoding='utf-8') as f:
                    json.dump(mock_transcription, f, indent=2, ensure_ascii=False)
                progress_callback(f"[Semantic Slicer]:     ✓ Saved transcription: {transcription_file.name}")
            except Exception as e:
                progress_callback(f"[Semantic Slicer]:     ❌ Error saving transcription: {e}")

    progress_callback(f"[Semantic Slicer]:   ✅ File {file_name} processed")


def _setup_whisper_model(model_name: str, progress_callback: Callable[[str], None]):
    """
    Stub for Whisper model setup.
    In the future, model initialization will be here.
    """
    progress_callback(f"[Semantic Slicer]: Setting up Whisper model: {model_name}")
    # Simulated model loading
    time.sleep(0.5)
    progress_callback(f"[Semantic Slicer]: ✅ Model {model_name} ready")


def _setup_diarization_pipeline(progress_callback: Callable[[str], None]):
    """
    Stub for diarization pipeline setup.
    In the future, pyannote.audio initialization will be here.
    """
    progress_callback(f"[Semantic Slicer]: Setting up diarization pipeline")
    # Simulated model loading
    time.sleep(0.5)
    progress_callback(f"[Semantic Slicer]: ✅ Diarization pipeline ready")


def _setup_google_api(api_key: str, progress_callback: Callable[[str], None]):
    """
    Stub for Google API setup.
    In the future, google.generativeai setup will be here.
    """
    if api_key:
        progress_callback(f"[Semantic Slicer]: Setting up Google API")
        # Simulated API setup
        time.sleep(0.2)
        progress_callback(f"[Semantic Slicer]: ✅ Google API configured")
    else:
        progress_callback(f"[Semantic Slicer]: ⚠️ Google API key not provided")


# Performance measurement functions (for future optimization)
def _log_performance_metrics(operation: str, start_time: float, end_time: float,
                           progress_callback: Callable[[str], None]):
    """
    Logging of performance metrics.
    """
    duration = end_time - start_time
    progress_callback(f"[Semantic Slicer]: 📊 {operation}: {duration:.2f} sec")


def _estimate_processing_time(file_size_mb: float, settings: Dict) -> float:
    """
    Estimation of file processing time based on size and settings.
    """
    # Simulated time calculation
    base_time = file_size_mb * 0.1  # 100ms per MB
    model_multiplier = {"tiny": 0.5, "base": 1.0, "small": 2.0, "medium": 4.0, "large": 8.0}
    model_factor = model_multiplier.get(settings.get('whisper_model', 'base'), 1.0)

    return base_time * model_factor


# Exporting main functions for use in other modules
__all__ = [
    'process_semantic_slicing_task',
    '_process_single_interview_file',
    '_setup_whisper_model',
    '_setup_diarization_pipeline',
    '_setup_google_api'
]
# <<< START OF NEW BLOCK: SUBPROCESS ENTRY POINT >>>
if __name__ == "__main__":
    # Read JSON string from stdin
    input_json = sys.stdin.read()

    # Deserialize JSON to task dictionary
    try:
        task_data = json.loads(input_json)
    except json.JSONDecodeError:
        print("FATAL: Failed to decode JSON from stdin.")
        sys.exit(1)

    # Restore Path objects which became strings after JSON serialization
    if 'entity_path' in task_data:
        task_data['entity_path'] = Path(task_data['entity_path'])
    if 'interview_files' in task_data:
        task_data['interview_files'] = [Path(p) for p in task_data['interview_files']]

    # Create simple progress_callback that just prints to stdout
    # The interface will read these messages
    def stdout_progress_callback(message):
        print(message, flush=True)

    # Create dummy stop_event as we are no longer managed from a thread
    stop_event = threading.Event()

    # Run main function
    try:
        process_semantic_slicing_task(task_data, stdout_progress_callback, stop_event)
    except Exception as e:
        # Log any unexpected error
        stdout_progress_callback(f"Critical error at worker top level: {e}\n{traceback.format_exc()}")
        sys.exit(1) # Exit with error code
# <<< END OF NEW BLOCK >>>