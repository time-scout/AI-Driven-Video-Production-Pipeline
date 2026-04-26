# image_parser_v2.py (FIXED)

from pathlib import Path
import random
import requests
from concurrent.futures import ThreadPoolExecutor
import re
import urllib.parse
from typing import Callable
import threading

from PIL import Image, UnidentifiedImageError
from duckduckgo_search import DDGS

FIXED_HEIGHT = 1080


def get_links_from_google_simple(keyword: str, log_callback: Callable):
    log_callback(f"Google: quick search for '{keyword}'...")
    try:
        url = f"https://www.google.com/search?q={urllib.parse.quote(keyword)}&tbm=isch"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        pattern = r'\["(https://[^"]*\.(?:jpg|jpeg|webp|png|gif))"'
        links = re.findall(pattern, response.text)
        unique_links = list(set(links))
        log_callback(f"Google: found {len(unique_links)} links.")
        return unique_links
    except requests.RequestException as e:
        log_callback(f"⚠️ Google: request error: {e}")
        return []


def get_links_from_ddg_lib(keyword: str, num_images: int, log_callback: Callable):
    log_callback(f"DDG: searching for {num_images} images...")
    all_links = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
    try:
        with DDGS(headers=headers, timeout=20) as ddgs:
            search_results = ddgs.images(keyword, safesearch="off", size="Large", max_results=num_images)
            for r in search_results:
                all_links.append(r['image'])
    except Exception as e:
        log_callback(f"❌ DDG: error: {e}")
    log_callback(f"DDG: found {len(all_links)} links.")
    return all_links


def get_combined_image_links(keyword: str, target_count: int, log_callback: Callable):
    google_links = get_links_from_google_simple(keyword, log_callback)
    ddg_links = get_links_from_ddg_lib(keyword, target_count, log_callback)
    all_links = set(google_links)
    all_links.update(ddg_links)
    final_list = list(all_links)
    log_callback(f"Total: found {len(final_list)} unique links.")
    return final_list


def _download_single_image(link: str, download_path: Path):
    try:
        response = requests.get(link, stream=True, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        if int(response.headers.get('content-length', 0)) < 20 * 1024: return None
        filename = ''.join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=12))
        ext = Path(urllib.parse.urlparse(link).path).suffix or '.jpg'
        if ext.lower() not in ['.jpg', '.jpeg', '.png', '.webp', '.gif']: ext = '.jpg'
        filepath = download_path / (filename + ext)
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(1024 * 8): f.write(chunk)
        return filepath
    except Exception:
        return None


def download_images_parallel(links: list, download_path: Path, log_callback: Callable, stop_event: threading.Event):
    log_callback(f"Starting download of {len(links)} images...")
    successful_downloads = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(_download_single_image, link, download_path): link for link in links}
        for future in futures:
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break
            result = future.result()
            if result: successful_downloads.append(result)
    log_callback(f"Successfully downloaded {len(successful_downloads)} images.")
    return successful_downloads


def post_process_images(source_path: Path, log_callback: Callable) -> list[Path]:
    log_callback("Starting image post-processing...")
    processed_files = []
    # --- MODIFICATION 1: Create a copy of the list to safely iterate and delete ---
    initial_files = list(source_path.iterdir())
    for old_filepath in initial_files:
        if not old_filepath.is_file(): continue  # Skip folders if any
        try:
            with Image.open(old_filepath) as img:
                img_rgb = img.convert('RGB')
                if img_rgb.height == 0: continue
                h_percent = FIXED_HEIGHT / float(img_rgb.height)
                w_size = int(float(img_rgb.width) * h_percent)
                img_resized = img_rgb.resize((w_size, FIXED_HEIGHT), Image.Resampling.LANCZOS)

                # --- MODIFICATION 2: Always create a new file with a new name to avoid conflicts ---
                new_filename = f"processed_{old_filepath.stem}.jpg"
                processed_filepath = source_path / new_filename

                img_resized.save(processed_filepath, "jpeg", quality=90)
                processed_files.append(processed_filepath)
        except (UnidentifiedImageError, OSError):
            pass  # Ignore broken files
        finally:
            # --- MODIFICATION 3: Always delete the old file after successful processing ---
            if old_filepath.exists():
                try:
                    old_filepath.unlink()
                except OSError as e:
                    log_callback(f"Failed to delete temporary file {old_filepath.name}: {e}")

    log_callback(f"Processed {len(processed_files)} images.")
    return processed_files  # Return list of paths to new, processed files