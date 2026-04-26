# voice_generator.py

import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from pydub import AudioSegment

# --- Configuration ---
BASE_URL = "https://voiceapi.csv666.ru"
POLLING_INTERVAL_SECONDS = 6
MAX_POLLING_ATTEMPTS = 40  # 40 * 6 = 240 seconds per chunk
TTS_CHAR_LIMIT = 5000
MAX_WORKERS = 5  # Number of simultaneous threads
MAX_RETRIES = 3  # Number of retries for temporary errors


def _get_balance(api_key: str, log_callback: callable):
    """Helper function to check and log balance."""
    headers = {"X-API-Key": api_key}
    balance_url = f"{BASE_URL}/balance"
    try:
        balance_response = requests.get(balance_url, headers=headers, timeout=10)
        log_callback(f"[RAW] Server response (balance): {balance_response.status_code} {balance_response.text}", "DEBUG")
        if balance_response.status_code == 200:
            balance_text = balance_response.json().get('balance_text', 'unknown')
            log_callback(f"Current balance: {balance_text} characters.", "INFO")
    except requests.RequestException:
        pass  # Not critical if balance check fails


def _split_text(text: str) -> list[str]:
    """
    Splits text into parts not exceeding TTS_CHAR_LIMIT.
    """
    if len(text) <= TTS_CHAR_LIMIT:
        return [text]
    chunks = []
    current_chunk_start = 0
    while current_chunk_start < len(text):
        end_pos = min(current_chunk_start + TTS_CHAR_LIMIT, len(text))
        if end_pos == len(text):
            chunks.append(text[current_chunk_start:])
            break
        sentence_break = text.rfind('.', current_chunk_start, end_pos)
        if sentence_break != -1:
            end_pos = sentence_break + 1
        else:
            space_break = text.rfind(' ', current_chunk_start, end_pos)
            if space_break != -1:
                end_pos = space_break + 1
        chunks.append(text[current_chunk_start:end_pos])
        current_chunk_start = end_pos
    return chunks


def _process_and_voice_block(api_key: str, block_data: dict, template_uuid: str, output_path: Path,
                             log_callback: callable, stop_event) -> tuple[str, int]:
    """
    Fully processes ONE logical block, including chunks and merging.
    Executed in a single thread.
    """
    block_index = block_data['index']
    original_text = block_data['text']
    log_callback(f"--- Block {block_index}: Taken into work by thread ---", "INFO")

    text_chunks = _split_text(original_text)
    if len(text_chunks) > 1:
        log_callback(f"Block {block_index}: Text split into {len(text_chunks)} parts.", "INFO")

    audio_segments = []
    for i, chunk in enumerate(text_chunks):
        if stop_event.is_set(): return 'stopped', block_index

        part_log_prefix = f"Block {block_index}, part {i + 1}/{len(text_chunks)}"

        for attempt in range(MAX_RETRIES):
            if stop_event.is_set(): return 'stopped', block_index
            try:
                # 1. Task creation
                task_creation_url = f"{BASE_URL}/tasks"
                headers_post = {"Content-Type": "application/json", "X-API-Key": api_key}
                data_post = {"text": chunk, "template_uuid": template_uuid}

                response_post = requests.post(task_creation_url, json=data_post, headers=headers_post, timeout=15)
                log_callback(
                    f"[RAW] {part_log_prefix}: Creation response: {response_post.status_code} {response_post.text}",
                    "DEBUG")
                response_post.raise_for_status()

                task_info = response_post.json()
                task_id = task_info.get("task_id")
                log_callback(f"{part_log_prefix}: Task created. ID: {task_id}", "INFO")

                # 2. Result retrieval
                task_result_url = f"{BASE_URL}/tasks/{task_id}/result"
                headers_get = {"X-API-Key": api_key}

                for poll_attempt in range(MAX_POLLING_ATTEMPTS):
                    if stop_event.is_set(): return 'stopped', block_index

                    time.sleep(POLLING_INTERVAL_SECONDS)
                    response_get = requests.get(task_result_url, headers=headers_get, timeout=20)

                    if response_get.status_code == 200:
                        log_callback(f"{part_log_prefix}: Audio fragment successfully received.", "SUCCESS")
                        temp_file_path = output_path / f"temp_{block_index}_{i}.mp3"
                        with open(temp_file_path, 'wb') as f:
                            f.write(response_get.content)
                        audio_segments.append(AudioSegment.from_mp3(temp_file_path))
                        os.remove(temp_file_path)
                        break  # Success, exit polling loop

                    elif response_get.status_code == 202:
                        log_callback(
                            f"{part_log_prefix}: Waiting... (attempt {poll_attempt + 1}/{MAX_POLLING_ATTEMPTS})",
                            "INFO")

                    else:
                        response_get.raise_for_status()
                else:
                    log_callback(f"{part_log_prefix}: Polling timeout exceeded.", "FATAL")
                    return 'error', block_index

                break  # Success, exit retries loop

            except requests.HTTPError as e:
                if 400 <= e.response.status_code < 500:
                    log_callback(f"{part_log_prefix}: FATAL API ERROR {e.response.status_code}. Stopping.",
                                 "FATAL")
                    return 'error', block_index
                log_callback(
                    f"{part_log_prefix}: Temporary server error ({e.response.status_code}). Attempt {attempt + 1}/{MAX_RETRIES}",
                    "WARN")
            except requests.RequestException as e:
                log_callback(f"{part_log_prefix}: Temporary network error: {e}. Attempt {attempt + 1}/{MAX_RETRIES}",
                             "WARN")

            time.sleep(5)  # Pause before retry
        else:
            log_callback(f"{part_log_prefix}: Failed to process chunk after {MAX_RETRIES} attempts.", "FATAL")
            return 'error', block_index

    # If all block parts processed successfully
    if len(audio_segments) == len(text_chunks):
        final_audio = sum(audio_segments) if len(audio_segments) > 1 else audio_segments[0]
        if len(audio_segments) > 1:
            log_callback(f"Block {block_index}: All {len(audio_segments)} parts merged.", "SUCCESS")

        final_filename = output_path / f"{block_index}.mp3"
        final_audio.export(final_filename, format="mp3")
        log_callback(f"Block {block_index}: File {final_filename.name} successfully saved.", "SUCCESS")
        return 'success', block_index
    else:
        log_callback(f"Block {block_index}: Failed to retrieve all audio parts. Processing failed.", "FATAL")
        return 'error', block_index


# ### CHANGE 1: Added new argument `project_complete_callback` ###
def run_synthesis(api_key: str, texts_to_process: list[dict], template_uuid: str,
                  output_path: Path, log_callback: callable, fragment_complete_callback: callable,
                  stop_event, project_complete_callback: callable = None) -> str:
    """
    Conductor: starts block processing in parallel threads.
    """
    log_callback("=== Starting voiceover process ===", "INFO")

    output_path.mkdir(parents=True, exist_ok=True)

    existing_files = {p.stem for p in output_path.glob('*.mp3')}
    tasks_to_run = [item for item in texts_to_process if str(item['index']) not in existing_files]

    if not tasks_to_run:
        log_callback("All required audio files are already in place.", "SUCCESS")
        # ### CHANGE 2: Call new callback even if nothing was done, but result is success ###
        if project_complete_callback:
            project_complete_callback()
        return 'success' # Return success, since everything is in place
    else:
        log_callback(
            f"Total blocks in task: {len(texts_to_process)}. Found ready: {len(existing_files)}. To be voiced: {len(tasks_to_run)}.",
            "INFO")
        _get_balance(api_key, log_callback)

    error_occurred = False
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_and_voice_block, api_key, task, template_uuid, output_path, log_callback,
                                   stop_event): task for task in tasks_to_run}

        try:
            for future in as_completed(futures):
                if stop_event.is_set():
                    error_occurred = True
                    break
                try:
                    status, block_index = future.result()
                    if status == 'success':
                        if fragment_complete_callback:
                            fragment_complete_callback()
                    elif status == 'error':
                        log_callback(f"Critical error during processing of block {block_index}. Stopping all tasks.",
                                     "FATAL")
                        error_occurred = True
                        break
                except Exception as exc:
                    task_data = futures[future]
                    log_callback(f"Block {task_data['index']} generated exception: {exc}", "FATAL")
                    error_occurred = True
                    break
        finally:
            if error_occurred or stop_event.is_set():
                log_callback("Error or stop detected, canceling remaining tasks...", "WARN")
                for f in futures: f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)

    if error_occurred: return 'error'
    if stop_event.is_set(): return 'stopped'

    # Final completeness check
    log_callback("Processing complete. Final completeness check...", "INFO")
    required_indices = {str(item['index']) for item in texts_to_process}
    final_files_indices = {p.stem for p in output_path.glob('*.mp3')}

    if required_indices.issubset(final_files_indices):
        log_callback("Check passed. All files are in place and match the task.", "SUCCESS")
        _get_balance(api_key, log_callback)
        # ### CHANGE 3: Call new callback here after successful check ###
        if project_complete_callback:
            project_complete_callback()
        return 'success'
    else:
        missing = sorted(list(required_indices - final_files_indices))
        log_callback(f"CHECK FAILED! Missing files for blocks: {missing}", "FATAL")
        return 'error'