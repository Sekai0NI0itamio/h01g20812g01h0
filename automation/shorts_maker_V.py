import os # for file operations
import time # for timing events and creating filenames like timestamps
import random # for randomizing elements
import textwrap # for wrapping text into lines but most cases being handled by textclip class in moviepy
import requests # for making HTTP requests
import numpy as np # for numerical operations here used for rounding off
import logging # for logging events
from PIL import Image, ImageFilter, ImageDraw, ImageFont# for image processing
from moviepy  import ( # for video editing
    VideoFileClip, VideoClip, TextClip, CompositeVideoClip,ImageClip,
    AudioFileClip, concatenate_videoclips, ColorClip, CompositeAudioClip, concatenate_audioclips
)
from moviepy.video.fx import *
# from moviepy.config import change_settings
# change_settings({"IMAGEMAGICK_BINARY": "magick"}) # for windows users
from dotenv import load_dotenv
import shutil # for file operations like moving and deleting files
import tempfile # for creating temporary files
from datetime import datetime # for more detailed time tracking
import concurrent.futures
from functools import wraps
import traceback  # Import traceback at the module level
from helper.minor_helper import measure_time, cleanup_temp_directories
from helper.fetch import fetch_videos_parallel
from helper.image import create_image_clips_parallel, fetch_image_from_duckduckgo
from helper.blur import custom_blur, custom_edge_blur
from helper.text import TextHelper
from helper.process import process_background_clips_parallel
from helper.audio import AudioHelper
from helper.shorts_assets import (
    add_dynamic_auto_captions_to_video,
    add_anime_greenscreen_overlay_to_video,
    add_background_music_to_video,
    build_brainrot_overlay_clip,
    pick_random_brainrot_video,
    pick_random_brainrot_start_time,
)
from automation.parallel_tasks import ParallelTaskExecutor
from automation.renderer import render_video
import multiprocessing

# Configure logging for easier debugging
# Do NOT initialize basicConfig here - this will be handled by main.py
logger = logging.getLogger(__name__)

load_dotenv()  # Load environment variables from .env file

# Get temp directory from environment variable or use default
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp"))
# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

class YTShortsCreator_V:
    def __init__(self, fps=30):
        """
        Initialize the YouTube Shorts creator with necessary settings

        Args:
            fps (int): Frames per second for the output video
        """
        # Setup directories
        self.temp_dir = os.path.join(TEMP_DIR, f"shorts_v_{int(time.time())}")
        os.makedirs(self.temp_dir, exist_ok=True)

        # Initialize TextHelper
        self.text_helper = TextHelper()

        # Initialize AudioHelper
        self.audio_helper = AudioHelper(self.temp_dir)

        # Video settings
        self.resolution = (1080, 1920)  # Portrait mode for shorts (width, height)
        self.fps = fps
        self.audio_sync_offset = 0.0  # Remove audio delay to improve sync

    @measure_time
    def create_youtube_short(self, title, script_sections, background_query="abstract background",
                            output_filename=None, add_captions=False, style="video", voice_style=None, max_duration=25,
                            background_queries=None, blur_background=False, edge_blur=False, add_watermark_text=None,
                            existing_audio_data=None):
        """
        Create a YouTube Short with the given script sections.

        Args:
            title (str): Title of the video
            script_sections (list): List of dict with text, duration, and voice_style
            background_query (str): Search term for background video
            output_filename (str): Output filename, if None one will be generated
            add_captions (bool): If True, add captions to the video
            style (str): Style of the video
            voice_style (str): Voice style from Azure TTS (excited, cheerful, etc)
            max_duration (int): Maximum video duration in seconds
            background_queries (list): Optional list of section-specific background queries
            blur_background (bool): Whether to apply blur effect to background videos
            edge_blur (bool): Whether to apply edge blur to background videos
            add_watermark_text (str): Text to use as watermark (None for no watermark)
            existing_audio_data (list): Optional pre-generated audio data (from shorts_maker_I fallback)

        Returns:
            str: Output file path
        """
        try:
            if not output_filename:
                date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_filename = os.path.join(self.temp_dir, f"short_{date_str}.mp4")

            # Get total duration from script sections
            total_raw_duration = sum(section.get('duration', 5) for section in script_sections)
            duration_scaling_factor = min(1.0, max_duration / total_raw_duration) if total_raw_duration > max_duration else 1.0

            # Scale durations if needed to fit max time
            if duration_scaling_factor < 1.0:
                logger.info(f"Scaling durations by factor {duration_scaling_factor:.2f} to fit max duration of {max_duration}s")
                for section in script_sections:
                    section['duration'] = section['duration'] * duration_scaling_factor

            # Add unique IDs to sections if they don't have them
            for i, section in enumerate(script_sections):
                if 'id' not in section:
                    section['id'] = f"section_{i}_{int(time.time())}"

            # 1. Prepare background queries
            if not background_queries:
                background_queries = [background_query] * len(script_sections)
            elif len(background_queries) != len(script_sections):
                # Pad or truncate the queries list
                if len(background_queries) < len(script_sections):
                    background_queries.extend([background_query] * (len(script_sections) - len(background_queries)))
                else:
                    background_queries = background_queries[:len(script_sections)]

            # Create a parallel task executor to run major steps concurrently
            parallel_executor = ParallelTaskExecutor()

            # 2, 3: Run background fetching and (if needed) audio generation in parallel
            logger.info("Starting parallel execution of major steps")

            # Define task functions
            def fetch_videos_task():
                logger.info("Fetching background videos in parallel")
                return fetch_videos_parallel(
                    queries=background_queries,
                    count_per_query=4,
                    min_duration=int(max(section.get('duration', 5) for section in script_sections)) + 2
                )

            def generate_audio_task():
                # Skip audio generation if we already have audio data
                if existing_audio_data:
                    logger.info("Using existing audio data (from fallback)")
                    return existing_audio_data
                
                logger.info("Generating audio clips in parallel")
                return self.audio_helper.process_audio_for_script(
                    script_sections=script_sections,
                    voice_style=voice_style
                )

            # Add tasks to executor
            parallel_executor.add_task("fetch_videos", fetch_videos_task)
            parallel_executor.add_task("generate_audio", generate_audio_task)

            # Execute all tasks in parallel and wait for results
            results = parallel_executor.execute()

            # Extract results
            videos_by_query = results.get("fetch_videos", {})
            audio_data = results.get("generate_audio", [])

            if not audio_data:
                logger.warning("No audio clips were generated; using silent placeholders for each section.")
                audio_data = [None] * len(script_sections)
            elif len(audio_data) != len(script_sections):
                logger.warning(
                    "Audio clip count mismatch (%s/%s); padding with silent placeholders.",
                    len(audio_data),
                    len(script_sections),
                )
                if len(audio_data) < len(script_sections):
                    audio_data.extend([None] * (len(script_sections) - len(audio_data)))
                else:
                    audio_data = audio_data[:len(script_sections)]

            # Check if we have necessary components before continuing
            if not videos_by_query:
                logger.error("No background videos fetched")
                return None

            # First, check audio durations to use them as source of truth
            audio_durations = {}
            inter_section_gap = float(os.getenv("SHORTS_AUDIO_GAP_SECONDS", "0.7"))
            for i, audio_section in enumerate(audio_data):
                expected_duration = script_sections[i].get('duration', 5)
                if audio_section and 'path' in audio_section:
                    try:
                        audio_clip = AudioFileClip(audio_section['path'])
                        spoken_duration = audio_clip.duration
                        audio_clip.close()

                        # Apply explicit pause between adjacent spoken lines.
                        actual_duration = spoken_duration + (inter_section_gap if i < len(script_sections) - 1 else 0.0)
                        
                        # Store actual audio duration for this section
                        audio_durations[i] = actual_duration
                        
                        # Update the script section duration to match audio
                        script_sections[i]['duration'] = actual_duration
                        
                        # Log mismatches for debugging
                        if abs(actual_duration - expected_duration) > 0.5:
                            logger.info(
                                "Section %s spoken audio %.2fs (+%.2fs gap) => %.2fs used instead of script duration %.2fs",
                                i,
                                spoken_duration,
                                inter_section_gap if i < len(script_sections) - 1 else 0.0,
                                actual_duration,
                                expected_duration,
                            )
                    except Exception as e:
                        logger.error(f"Error checking audio duration for section {i}: {e}")
                        audio_durations[i] = expected_duration
                else:
                    audio_durations[i] = expected_duration
            
            # Captions generation disabled by user request — create empty placeholders
            logger.info("Captions generation disabled; skipping text clip creation")
            text_clips = [None] * len(script_sections)
            
            # Final verification of clip durations against audio durations
            for i, clip in enumerate(text_clips):
                if clip:
                    target_duration = audio_durations.get(i, script_sections[i].get('duration', 5))
                    if abs(clip.duration - target_duration) > 0.5:
                        logger.warning(f"Text clip {i} duration mismatch: {clip.duration:.2f}s vs audio {target_duration:.2f}s - fixing...")
                        text_clips[i] = clip.with_duration(target_duration)
            
            # 5. Process background videos
            logger.info("Processing background videos")

            # Prefer unique stock videos across sections; if uniqueness is impossible, fallback to image search.
            used_video_paths = set()
            video_info = []
            video_section_indices = []
            image_fallback_indices = []
            image_fallback_queries = []

            for i, section in enumerate(script_sections):
                query = background_queries[i]
                target_duration = section.get('duration', 5)
                candidates = videos_by_query.get(query, [])

                chosen_video = None
                for candidate in candidates:
                    if candidate and candidate not in used_video_paths:
                        chosen_video = candidate
                        break

                if chosen_video:
                    used_video_paths.add(chosen_video)
                    video_info.append({
                        'path': chosen_video,
                        'target_duration': target_duration,
                        'section_idx': i,
                        'query': query
                    })
                    video_section_indices.append(i)
                else:
                    image_fallback_indices.append(i)
                    image_fallback_queries.append(query)

            processed_video_clips = process_background_clips_parallel(
                video_info=video_info,
                blur_background=blur_background,
                edge_blur=edge_blur,
            ) if video_info else []

            background_clips = [None] * len(script_sections)
            for idx, clip in zip(video_section_indices, processed_video_clips):
                background_clips[idx] = clip

            if image_fallback_indices:
                logger.warning(
                    "Using DDG->Brave->Ecosia image fallback for %s sections where unique videos were unavailable",
                    len(image_fallback_indices),
                )
                used_image_paths = set()
                image_paths = []
                image_durations = []

                for fallback_idx, query in zip(image_fallback_indices, image_fallback_queries):
                    target_duration = script_sections[fallback_idx].get('duration', 5)
                    image_path = None

                    # Try query variants to avoid duplicate image assets.
                    query_variants = [query]
                    for token in str(query).split():
                        cleaned = token.strip(",.:'\"()[]{}")
                        if len(cleaned) >= 3 and cleaned not in query_variants:
                            query_variants.append(cleaned)

                    for variant in query_variants[:8]:
                        candidate_path = fetch_image_from_duckduckgo(variant)
                        if candidate_path and candidate_path not in used_image_paths:
                            image_path = candidate_path
                            used_image_paths.add(candidate_path)
                            break

                    image_paths.append(image_path)
                    image_durations.append(target_duration)

                image_clips = create_image_clips_parallel(
                    image_paths=image_paths,
                    durations=image_durations,
                    with_zoom=True,
                )

                for idx, clip in zip(image_fallback_indices, image_clips):
                    if clip:
                        if blur_background:
                            clip = custom_blur(clip, intensity=2)
                        elif edge_blur:
                            clip = custom_edge_blur(clip, edge_size=80)
                        background_clips[idx] = clip

            # Make sure we have all the components
            missing_background = [i for i, clip in enumerate(background_clips) if clip is None]
            if (
                not background_clips
                or len(background_clips) != len(script_sections)
                or missing_background
                or not text_clips
                or len(text_clips) != len(script_sections)
            ):
                logger.error(
                    "A component is missing. backgrounds=%s/%s missing_background=%s audio=%s/%s text=%s/%s",
                    len([clip for clip in background_clips if clip is not None]),
                    len(script_sections),
                    missing_background,
                    len(audio_data) if audio_data else 0,
                    len(script_sections),
                    len([clip for clip in text_clips if clip is not None]),
                    len(script_sections),
                )
                return None

            # Create section clips (background + audio + text)
            section_clips = []
            section_info = {}  # For better logging in parallel renderer
            brainrot_video_path = pick_random_brainrot_video()
            brainrot_elapsed = pick_random_brainrot_start_time(brainrot_video_path, min_remaining_seconds=60.0)
            brainrot_height_ratio = float(os.getenv("SHORTS_BRAINROT_HEIGHT_RATIO", "0.6667"))

            # Now process each section using audio duration as the source of truth
            for i, (bg_clip, audio, text_clip) in enumerate(zip(background_clips, audio_data, text_clips)):
                # Use the audio duration as the source of truth for section duration
                section_duration = audio_durations.get(i, script_sections[i].get('duration', 5))
                
                # Adjust background to match audio duration
                bg_clip = bg_clip.with_duration(section_duration)
                visual_layers = [bg_clip]

                if brainrot_video_path:
                    try:
                        brainrot_clip = build_brainrot_overlay_clip(
                            brainrot_video_path,
                            start_time=brainrot_elapsed,
                            duration=section_duration,
                            canvas_size=self.resolution,
                            top_height_ratio=brainrot_height_ratio,
                        )
                        if brainrot_clip:
                            visual_layers.append(brainrot_clip)
                    except Exception as e:
                        logger.error(f"Error building brainrot overlay for section {i}: {e}")
                brainrot_elapsed += section_duration

                # Add timed meme image overlays for this section (if planned).
                section_meta = script_sections[i] if i < len(script_sections) else {}
                meme_items = section_meta.get('meme_overlays', []) or []
                for meme in meme_items:
                    try:
                        meme_path = meme.get('image_path')
                        if not meme_path or not os.path.exists(meme_path):
                            continue

                        meme_offset = max(0.0, float(meme.get('offset_seconds', 0.0) or 0.0))
                        meme_duration = max(2.0, min(5.0, float(meme.get('duration_seconds', 3.0) or 3.0)))
                        if meme_offset >= section_duration:
                            continue
                        meme_duration = min(meme_duration, section_duration - meme_offset)

                        meme_clip = ImageClip(meme_path).resized(width=int(self.resolution[0] * 0.66))
                        meme_clip = meme_clip.with_start(meme_offset).with_duration(meme_duration)
                        meme_clip = meme_clip.with_position(("center", int(self.resolution[1] * 0.34)))
                        visual_layers.append(meme_clip)
                    except Exception as meme_err:
                        logger.warning(f"Failed to add meme overlay for section {i}: {meme_err}")

                visual_base = CompositeVideoClip(visual_layers, size=self.resolution).with_duration(section_duration)
                
                # Add text clip after the visual stack so subtitles stay on top
                if text_clip:
                    # Make text clip match audio duration
                    text_clip = text_clip.with_duration(section_duration)
                    composite = CompositeVideoClip([visual_base, text_clip], size=self.resolution).with_duration(section_duration)
                else:
                    composite = visual_base
                
                # Add audio without duration adjustment
                if audio and 'path' in audio:
                    try:
                        # Load audio without modifying its duration
                        audio_clip = AudioFileClip(audio['path'])

                        # Optional per-line sound effect from SoundEffects folder.
                        sfx_clip = None
                        section_meta = script_sections[i] if i < len(script_sections) else {}
                        sfx_file = section_meta.get('sfx_file')
                        sfx_offset = float(section_meta.get('sfx_offset', 0.0) or 0.0)
                        if sfx_file:
                            try:
                                sfx_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'SoundEffects', sfx_file)
                                if os.path.exists(sfx_path):
                                    sfx_clip_raw = AudioFileClip(sfx_path).with_volume_scaled(
                                        float(os.getenv("SHORTS_SFX_VOLUME", "0.45"))
                                    )
                                    max_sfx_dur = max(0.05, section_duration - max(0.0, sfx_offset))
                                    sfx_clip = sfx_clip_raw.subclipped(0, min(sfx_clip_raw.duration, max_sfx_dur)).with_start(max(0.0, sfx_offset))
                            except Exception as sfx_err:
                                logger.warning(f"Failed to apply SFX for section {i}: {sfx_err}")

                        if sfx_clip:
                            mixed_audio = CompositeAudioClip([audio_clip, sfx_clip]).with_duration(section_duration)
                            composite = composite.with_audio(mixed_audio)
                        else:
                            # Apply narration as is when no effect selected
                            composite = composite.with_audio(audio_clip)
                    except Exception as e:
                        logger.error(f"Error adding audio to section {i}: {e}")
                        # Create silent audio as fallback
                        try:
                            silent_audio = AudioFileClip.__new__(AudioFileClip)
                            silent_audio.duration = section_duration
                            composite = composite.with_audio(silent_audio)
                        except:
                            logger.error(f"Could not create silent audio for section {i}")
                else:
                    # No audio provided, create silent audio
                    logger.warning(f"No audio data for section {i}, creating silent audio")
                    try:
                        silent_audio = AudioFileClip.__new__(AudioFileClip)
                        silent_audio.duration = section_duration
                        composite = composite.with_audio(silent_audio)
                    except Exception as e:
                        logger.error(f"Could not create silent audio: {e}")

                # Add debugging info to the clip
                section_text = script_sections[i].get('text', '')[:30] + '...' if len(script_sections[i].get('text', '')) > 30 else script_sections[i].get('text', '')
                composite._debug_info = f"Section {i}: {section_text}"
                composite._section_idx = i

                # Store section information
                section_info[i] = {
                    'section_idx': i,
                    'section_text': section_text,
                    'duration': section_duration
                }

                section_clips.append(composite)

            # Use our unified renderer
            logger.info("Rendering final video using optimized renderer")

            # Ensure rendering temp directory exists
            render_temp_dir = os.path.join(self.temp_dir, "render")
            os.makedirs(render_temp_dir, exist_ok=True)

            # Use the unified rendering interface
            output_path = render_video(
                clips=section_clips,
                output_file=output_filename,
                fps=self.fps,
                temp_dir=render_temp_dir,
                preset="ultrafast",
                parallel=True,
                memory_per_worker_gb=1.0,
                options={
                    'clean_temp': True,
                    'section_info': section_info
                }
            )

            # Validate output path exists
            if not output_path or not os.path.exists(output_path):
                logger.error(f"Failed to render final video - output file not found: {output_path}")
                if output_filename and os.path.exists(output_filename):
                    # Use original output filename if render_video fails
                    logger.info(f"Using fallback output file: {output_filename}")
                    output_path = output_filename
                else:
                    logger.error("No valid output file available")
                    return None

            logger.info(f"Successfully rendered video to {output_path}")

            # Add background music as a post-process
            if output_path and os.path.exists(output_path):
                output_path = add_background_music_to_video(
                    output_path,
                    fps=self.fps,
                    preset="ultrafast",
                )

            # Add a random green-screen anime girl overlay as a final post-process.
            if output_path and os.path.exists(output_path):
                output_path = add_anime_greenscreen_overlay_to_video(
                    output_path,
                    preset="ultrafast",
                )

            # Add dynamic auto-captions after final composition and overlays.
            if output_path and os.path.exists(output_path) and os.getenv("AUTO_CAPTIONS_ENABLED", "true").lower() == "true":
                output_path = add_dynamic_auto_captions_to_video(
                    output_path,
                    script_sections=script_sections,
                    font_size=50,
                    position_ratio=0.5,
                    preset="ultrafast",
                )

            # Add watermark if requested
            if add_watermark_text and output_path and os.path.exists(output_path):
                logger.info("Adding watermark to final video")
                try:
                    # Load the rendered video
                    final_video = VideoFileClip(output_path)

                    # Add watermark
                    final_with_watermark = self.text_helper.add_watermark(final_video, watermark_text=add_watermark_text)

                    # Determine watermarked output filename
                    watermarked_output = output_path.replace('.mp4', '_watermarked.mp4')

                    # Write the watermarked video
                    final_with_watermark.write_videofile(
                        watermarked_output,
                        fps=self.fps,
                        codec="libx264",
                        audio_codec="aac",
                        preset="ultrafast"
                    )

                    # Replace original with watermarked version
                    os.replace(watermarked_output, output_path)

                    # Clean up
                    final_video.close()
                    final_with_watermark.close()
                except Exception as watermark_error:
                    logger.error(f"Error adding watermark: {watermark_error}")
                    logger.error(f"Detailed watermark error: {traceback.format_exc()}")

            return output_path

        except Exception as e:
            logger.error(f"Error creating video: {e}")
            logger.error(f"Detailed error trace: {traceback.format_exc()}")
            # If we encounter an error, try to clean up temp files
            cleanup_temp_directories(specific_dir=self.temp_dir, force_all=True)
            return None

    def cleanup(self):
        """Clean up temporary files"""
        try:
            cleanup_temp_directories(specific_dir=self.temp_dir, force_all=True)
            logger.info(f"Cleaned up temporary files in {self.temp_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up temporary files: {e}")
