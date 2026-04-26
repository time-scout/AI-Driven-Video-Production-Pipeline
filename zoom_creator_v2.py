# zoom_creator_v2.py

from pathlib import Path
from PIL import Image, ImageFilter
import cv2
import numpy as np
import subprocess
import re
import shutil
from typing import Callable, List, Optional

VIDEO_WIDTH, VIDEO_HEIGHT = 1920, 1080
PAN_BASE_SCALE = 1.2
FPS = 25


def get_xmp_rating(image_path: Path) -> int:
    try:
        with Image.open(image_path) as img:
            xmp_data = img.info.get('xmp')
            if xmp_data and isinstance(xmp_data, bytes):
                xmp_str = xmp_data.decode('utf-8', errors='ignore')
                match = re.search(r'xmp:Rating="(\d)"', xmp_str)
                if match:
                    return int(match.group(1))
    except Exception:
        pass
    return 0


def _create_master_video(image_path: Path, output_filepath: Path, settings: dict) -> bool:
    video_writer = None
    try:
        img = Image.open(image_path).convert("RGB")
        orig_w, orig_h = img.size
        new_h, new_w = VIDEO_HEIGHT, int(orig_w * (VIDEO_HEIGHT / orig_h))
        img_resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
        blur_radius = settings.get('blur_radius', 100)
        bg = img.resize((VIDEO_WIDTH, int(orig_h * (VIDEO_WIDTH / orig_w))), Image.Resampling.LANCZOS).filter(
            ImageFilter.GaussianBlur(radius=blur_radius))
        top = (bg.height - VIDEO_HEIGHT) / 2
        bg_cropped = bg.crop((0, top, VIDEO_WIDTH, top + VIDEO_HEIGHT))
        duration = settings.get('duration', 6)
        total_frames = max(1, int(duration * FPS))
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        video_writer = cv2.VideoWriter(str(output_filepath), fourcc, float(FPS), (VIDEO_WIDTH, VIDEO_HEIGHT))
        effect_type = settings.get('effect', 'Zoom In')
        speed_multiplier = settings.get('effect_speed', 1.0)
        for i in range(total_frames):
            progress = i / (total_frames - 1) if total_frames > 1 else 0
            current_w, current_h = float(new_w), float(new_h)
            current_x, current_y = (new_w - current_w) / 2.0, (new_h - current_h) / 2.0
            if "Zoom" in effect_type:
                magnitude = 0.2 * speed_multiplier
                scale = 1.0 + magnitude * progress if effect_type == "Zoom In" else 1.0 + magnitude * (1.0 - progress)
                current_w, current_h = new_w / scale, new_h / scale
                current_x, current_y = (new_w - current_w) / 2.0, (new_h - current_h) / 2.0
            elif "Pan" in effect_type:
                scale = PAN_BASE_SCALE
                current_w, current_h = new_w / scale, new_h / scale
                max_pan_x, max_pan_y = new_w - current_w, new_h - current_h
                pan_progress = min(progress * speed_multiplier, 1.0)
                current_x, current_y = max_pan_x / 2, max_pan_y / 2
                if effect_type == "Pan Right":
                    current_x = 0 + (max_pan_x * pan_progress)
                elif effect_type == "Pan Left":
                    current_x = max_pan_x - (max_pan_x * pan_progress)
                elif effect_type == "Pan Down":
                    current_y = 0 + (max_pan_y * pan_progress)
                elif effect_type == "Pan Up":
                    current_y = max_pan_y - (max_pan_y * pan_progress)
            crop_box = (current_x, current_y, current_x + current_w, current_y + current_h)
            fg_image = img_resized.crop(crop_box).resize((new_w, new_h), Image.Resampling.LANCZOS)
            frame = bg_cropped.copy()
            frame.paste(fg_image, ((VIDEO_WIDTH - new_w) // 2, (VIDEO_HEIGHT - new_h) // 2))
            cv_frame = cv2.cvtColor(np.array(frame), cv2.COLOR_RGB2BGR)
            video_writer.write(cv_frame)
        return True
    except Exception as e:
        print(f"Error creating master video for {image_path.name}: {e}")
        return False
    finally:
        if video_writer is not None and video_writer.isOpened():
            video_writer.release()


def _normalize_master_video(master_path: Path, final_path: Path, progress_callback: Callable) -> bool:
    try:
        command = [
            'ffmpeg', '-y', '-i', str(master_path), '-c:v', 'libx264',
            '-preset', 'medium', '-crf', '27', '-pix_fmt', 'yuv420p',
            '-r', str(FPS), '-an', '-movflags', '+faststart', str(final_path)
        ]
        subprocess.run(command, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError as e:
        progress_callback(f"❌ ERROR normalizing {master_path.name}: {e.stderr.decode('utf-8', 'ignore')}")
        return False


def create_zoom_from_photo(
        photo_path: Path,
        entity_path: Path,
        settings: dict,
        index: int,
        eid: str,
        progress_callback: Callable
) -> Optional[Path]:
    """
    Creates master, normalizes, names and returns path to file.
    """
    zooms_output_path = entity_path / "zoomed_videos"
    zooms_output_path.mkdir(exist_ok=True)
    temp_master_dir = zooms_output_path / "temp_masters"
    temp_master_dir.mkdir(exist_ok=True)
    progress_callback(f"▶️ Processing photo: {photo_path.name}")
    rating = get_xmp_rating(photo_path)
    prefix = "h_" if rating > 0 else ""
    entity_folder_name = entity_path.name
    output_filename = f"{prefix}zoom_{index:03d}_{entity_folder_name}--{eid}.mp4"
    final_output_path = zooms_output_path / output_filename
    temp_master_path = temp_master_dir / f"master_{output_filename}"
    if not _create_master_video(photo_path, temp_master_path, settings):
        progress_callback(f"❌ Could not create master video for {photo_path.name}")
        if temp_master_path.exists(): temp_master_path.unlink()
        return None
    normalized_successfully = _normalize_master_video(temp_master_path, final_output_path, progress_callback)
    if temp_master_path.exists():
        temp_master_path.unlink()
    if normalized_successfully:
        progress_callback(f"   ✅ Created Z-Clip: {final_output_path.name}")
        return final_output_path
    return None


def cleanup_temp_folder(entity_path: Path):
    temp_master_dir = entity_path / "zoomed_videos" / "temp_masters"
    if temp_master_dir.exists():
        shutil.rmtree(temp_master_dir)