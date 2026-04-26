# download_orchestrator_v2.py (FIXED)

import shutil
import tempfile
import re
from pathlib import Path
from typing import Dict, Callable, List
import threading

import entity_manager_v2 as em
import image_parser_v2 as ip
import video_parser_v2 as vp


def process_single_photo_task(
        task: Dict,
        base_archive_path: Path,
        progress_callback: Callable[[str], None],
        stop_event: threading.Event,
        overwrite_decision: str = 'ask'
) -> bool:  # --- CHANGE: Returns bool to indicate success ---
    eid = task.get('eid')
    name = task.get('name')
    role = task.get('role')
    count = task.get('count')
    manual_query = task.get('manual_query')

    if not all([eid, name, role]):
        progress_callback(f"❌ Task error: EID, name, or role missing. Skipping.")
        return False

    progress_callback(f"▶️ Starting task for: {name} ({eid})")

    try:
        entity_path, msg = em.get_or_create_entity_path(
            base_path=base_archive_path, 
            eid=eid, 
            name=name, 
            role=role, 
            log_callback=progress_callback
        )
        if not entity_path:
            progress_callback(f"❌ Structure error for {name}: {msg}. Task canceled.")
            return False

        target_path = entity_path / "unchecked_photos"
        if overwrite_decision == 'overwrite':
            if target_path.exists():
                progress_callback(f"⚠️ Clearing folder {target_path} according to selection...")
                shutil.rmtree(target_path)
        target_path.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=f"{eid}_photos_") as temp_dir_str:
            temp_path = Path(temp_dir_str)
            search_query = manual_query if manual_query else f"{name} {role}"
            progress_callback(f"Using search query: '{search_query}'")

            links = ip.get_combined_image_links(search_query, count, progress_callback)
            if stop_event.is_set(): return False
            if not links:
                progress_callback(f"No links found for '{search_query}'.")
                return False

            downloaded_files = ip.download_images_parallel(links[:count], temp_path, progress_callback, stop_event)
            if stop_event.is_set(): return False
            if not downloaded_files:
                progress_callback(f"Could not download any images for '{search_query}'.")
                return False

            processed_files = ip.post_process_images(temp_path, progress_callback)
            if stop_event.is_set(): return False
            if not processed_files:
                progress_callback(f"Could not process any images for '{search_query}'.")
                return False

            progress_callback(f"Renaming and moving {len(processed_files)} files...")
            folder_name_for_filename = entity_path.name

            for processed_file_path in processed_files:
                if stop_event.is_set(): return False
                next_index = em.get_next_media_index(target_path, "image",
                                                     f"image_(\\d+)_{re.escape(folder_name_for_filename)}--{eid}\\.jpg")
                new_filename = f"image_{next_index:03d}_{folder_name_for_filename}--{eid}.jpg"
                final_path = target_path / new_filename
                shutil.move(processed_file_path, final_path)

            if not stop_event.is_set():
                progress_callback(f"✅ Task for {name} completed. Saved {len(processed_files)} photos.")
            return True

    except Exception as e:
        progress_callback(f"❌ Critical error during photo processing for '{name}': {e}")
    return False


def process_single_video_task(
        task: Dict,
        base_archive_path: Path,
        progress_callback: Callable[[str], None],
        stop_event: threading.Event,
        overwrite_decision: str = 'ask'
) -> List[Path]:
    eid = task.get('eid');
    name = task.get('name');
    role = task.get('role');
    regular_links = task.get('regular_links', [])
    interview_links = task.get('interview_links', [])

    # [(link, is_interview), ...]
    all_links = [(link, True) for link in interview_links] + [(link, False) for link in regular_links]
    successfully_downloaded = []
    if not all([eid, name, role, all_links]):
        progress_callback(f"❌ Video task error: EID, name, role, or links missing. Skipping.")
        return successfully_downloaded
    progress_callback(f"▶️ Started processing {len(all_links)} videos for '{name}'")
    try:
        ssid = task.get('ssid')
        song_name = task.get('song_name')
        entity_path, msg = em.get_or_create_entity_path(
            base_path=base_archive_path, 
            eid=eid, 
            name=name, 
            role=role, 
            ssid=ssid,
            song_name=song_name,
            log_callback=progress_callback
        )
        if not entity_path:
            progress_callback(f"Structure error for {name}: {msg}. Skipping video.")
            return successfully_downloaded
        target_path = entity_path / "raw_videos"
        if overwrite_decision == 'overwrite':
            if target_path.exists():
                progress_callback(f"⚠️ Clearing folder {target_path} according to selection...")
                shutil.rmtree(target_path)
        target_path.mkdir(parents=True, exist_ok=True)
        interview_path = entity_path / "interview_fragments"
        interview_path.mkdir(parents=True, exist_ok=True)
        for i, (link, is_interview) in enumerate(all_links):
            if stop_event.is_set(): break
            progress_callback(f"({i + 1}/{len(all_links)}) Downloading video for '{name}': {link}")
            video_id = vp.get_video_id(link, progress_callback)
            if not video_id:
                progress_callback(f"   -> Could not retrieve ID for {link}. Skipping.");
                continue
            if overwrite_decision != 'overwrite':
                if any(video_id in f.name for f in target_path.glob('*.mp4')):
                    progress_callback(f"   -> Video {video_id} already downloaded. Skipping.");
                    continue
            folder_name_for_filename = entity_path.name
            # Form filename prefix depending on video type
            filename_prefix = "--INTERVIEW--" if is_interview else ""
            next_index = em.get_next_media_index(target_path, "raw_video",
                                                  f"raw_video_(\\d+)_{re.escape(folder_name_for_filename)}--{eid}_.*\\.mp4")
            new_filename = f"{filename_prefix}raw_video_{next_index:03d}_{folder_name_for_filename}--{eid}_{video_id}.mp4"
            final_path = target_path / new_filename
            if vp.download_video(link, final_path, progress_callback):
                successfully_downloaded.append(final_path)
        if not stop_event.is_set():
            progress_callback(f"✅ Video download completed for: {name}")
    except Exception as e:
        progress_callback(f"❌ Critical error in video worker for '{name}': {e}")
    return successfully_downloaded