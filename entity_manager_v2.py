# entity_manager_v2.py

import json
import os
import re
from pathlib import Path
from typing import Dict, Tuple, Optional, Callable
import datetime
import threading

EID_PATH_CACHE: Dict[str, Optional[Path]] = {}
JSON_LOCK = threading.Lock()


def log_placeholder(message):
    print(f"[ENTITY_MANAGER_V2 LOG]: {message}")


def _sanitize_name(text: str) -> str:
    text = text.lower().replace(' ', '_')
    return re.sub(r'[^a-z0-9_-]', '', text)


def find_path_by_eid(base_path: Path, eid: str, log_callback: Callable = log_placeholder) -> Optional[Path]:


    if eid in EID_PATH_CACHE:


        return EID_PATH_CACHE[eid]


    


    if not base_path.exists():


        return None





    log_callback(f"Searching for folder by EID {eid} in archive...")


    try:


        # Search for artist folder at the top level of the archive


        for item in base_path.iterdir():


            if item.is_dir() and not item.name.startswith('.'):


                if item.name.endswith(f"_{eid}"):


                    EID_PATH_CACHE[eid] = item


                    return item


    except Exception as e:


        log_callback(f"Error searching for folder: {e}")





    EID_PATH_CACHE[eid] = None


    return None








def get_or_create_entity_path(base_path: Path, eid: str, name: str, role: str,


                              ssid: str = None, song_name: str = None,


                              log_callback: Callable = log_placeholder) -> Tuple[Optional[Path], str]:


    try:


        # 1. First find or create ARTIST folder


        artist_path = find_path_by_eid(base_path, eid, log_callback)


        


        if not artist_path:


            folder_name = f"{_sanitize_name(name)}_{eid}"


            artist_path = base_path / folder_name


            artist_path.mkdir(parents=True, exist_ok=True)


            log_callback(f"Created artist folder: {artist_path.name}")


        


        # Ensure base artist folders exist


        (artist_path / "photos").mkdir(exist_ok=True)


        (artist_path / "unchecked_photos").mkdir(exist_ok=True)


        (artist_path / "interview_fragments").mkdir(exist_ok=True)





        # 2. If SSID is not specified, return artist path (for photos/interviews)


        if not ssid:


            return artist_path, "Artist path is ready."





        # 3. If SSID is specified, find or create SONG folder inside artist folder


        song_folder_name = f"{ssid}_{_sanitize_name(song_name)}_by_{_sanitize_name(name)}_{eid}"


        song_path = artist_path / song_folder_name


        


        if not song_path.exists():


            song_path.mkdir(parents=True, exist_ok=True)


            log_callback(f"Created song folder: {song_path.name}")


        


        # Ensure video folder exists inside song


        (song_path / "raw_videos").mkdir(exist_ok=True)


        


        return song_path, "Song path is ready."





    except Exception as e:


        log_callback(f"Critical error in get_or_create_entity_path for EID {eid}/SSID {ssid}: {e}")


        return None, f"Error: {e}"








def get_next_media_index(media_folder: Path, prefix: str, pattern_str: str) -> int:


    media_folder.mkdir(parents=True, exist_ok=True)


    max_index = 0


    # Updated pattern to account for prefixes like --INTERVIEW--


    pattern = re.compile(rf".*{re.escape(prefix)}_(\d+)_.*")


    for filename in os.listdir(media_folder):


        match = pattern.match(filename)


        if match:


            max_index = max(max_index, int(match.group(1)))


    return max_index + 1








def update_readiness_stats(entity_path: Path, new_stats: Dict, log_callback: Callable = log_placeholder):


    """


    Empty function to maintain backward compatibility.


    No longer using JSON to store statistics.


    """


    pass

