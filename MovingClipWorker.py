# MovingClipWorker.py
from moviepy.editor import *
import numpy as np

class MovingClipWorker:
    def process_video(self, options, log_queue):
        log_queue.put("Worker started with options: " + str(options))

        # 1. Load clips
        source_clip = VideoFileClip(options['source_path'])
        bg_clip = VideoFileClip(options['background_path'])

        # 2. Background processing: resize to 1920x1080
        if bg_clip.size != [1920, 1080]:
            log_queue.put(f"Background has size {bg_clip.size}, resizing to 1920x1080.")
            bg_clip = bg_clip.resize(height=1080)
            if bg_clip.size[0] < 1920:
                bg_clip = bg_clip.fx(vfx.crop, width=1920, x_center=bg_clip.w/2)

        # Set background duration equal to source clip duration
        bg_clip = bg_clip.subclip(0, source_clip.duration)

        # 3. Source clip processing
        scale_percent = int(options['scale']) / 100.0
        processed_clip = source_clip.resize(width=1920 * scale_percent)

        # --- Creating gradient mask for soft edges using NumPy ---
        feather_percent = int(options['feather'])
        if feather_percent > 0:
            log_queue.put(f"Creating soft edges with intensity {feather_percent}%...")
            w, h = processed_clip.w, processed_clip.h

            # Blur size in pixels
            feather_size = int(min(w, h) * (feather_percent / 100.0) / 2)

            # Create 1D gradients from 0 to 1
            ramp_x = np.linspace(0, 1, feather_size)
            ramp_y = np.linspace(0, 1, feather_size)

            # Create 2D masks for horizontal and vertical gradients
            mask_x = np.ones(w)
            mask_x[:feather_size] = ramp_x
            mask_x[-feather_size:] = ramp_x[::-1] # Inverted gradient

            mask_y = np.ones(h)
            mask_y[:feather_size] = ramp_y
            mask_y[-feather_size:] = ramp_y[::-1]

            # Combine 1D masks into final 2D mask
            # Use np.minimum for correct corner handling
            final_mask_array = np.minimum.outer(mask_y, mask_x)

            # Create mask from NumPy array using ImageClip (syntax for moviepy 1.0.3)
            # Array must be in 0-1 range, and ismask=True ensures correct handling.
            mask_clip = ImageClip(final_mask_array, ismask=True)
            processed_clip = processed_clip.set_mask(mask_clip)

        # rotate will be applied dynamically in the next step

        # 4. Motion animation
        screen_w, screen_h = 1920, 1080
        clip_w, clip_h = processed_clip.w, processed_clip.h

        # --- Function for tilt rocking ---
        max_tilt = int(options['max_tilt'])
        wobble_speed = float(options['wobble_speed'])
        def rotation_function(t):
            # Use sine for smooth rocking from -max_tilt to +max_tilt
            return max_tilt * np.sin(t * wobble_speed)

        # --- Function for trajectory movement ---
        travel_speed = float(options['travel_speed'])
        def position_function(t):
            center_x = (screen_w - clip_w) / 2
            center_y = (screen_h - clip_h) / 2
            trajectory = options['trajectory']

            if trajectory == "Smooth movement right":
                # Movement depends on time and total duration
                progress = t / source_clip.duration
                x = (screen_w - clip_w) * progress
                # Light vertical rocking
                y = center_y + 30 * np.sin(t * travel_speed)
                return (x, y)
            elif trajectory == "Rocking (sinusoidal)":
                x = center_x + 80 * np.cos(t * (travel_speed / 2)) # Divide to slow down movement
                y = center_y + 40 * np.sin(t * travel_speed)
                return (x, y)
            else: # Static (center)
                return ('center', 'center')

        # Apply all transformations
        animated_clip = (processed_clip
                         .set_position(position_function)
                         # Set dynamic rotation. resample='bicubic' smoothes edges.
                         .rotate(rotation_function, resample='bicubic'))

        # 5. Assembly and export
        final_clip = CompositeVideoClip([bg_clip, animated_clip], size=(1920, 1080))

        log_queue.put("Video rendering starting...")
        final_clip.write_videofile(
            options['output_path'],
            fps=25,
            codec='libx264',
            audio_codec='aac',
            audio_fps=44100,
            preset='medium',
            threads=4,
            ffmpeg_params=[
                '-pix_fmt', 'yuv420p',
                '-b:a', '192k',
                '-crf', str(options['crf']) # Use CRF from settings
            ]
        )
        log_queue.put(f"Video successfully saved to {options['output_path']}")