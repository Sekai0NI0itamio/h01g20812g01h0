import os
import logging
import tempfile
import uuid
import shutil
import subprocess
import traceback
import re
from typing import List, Tuple, Dict, Any, Optional

# MoviePy imports
from moviepy import VideoFileClip, concatenate_videoclips
from moviepy.video.fx.FadeIn import FadeIn
from moviepy.video.fx.FadeOut import FadeOut

logger = logging.getLogger(__name__)


def _path_has_audio_stream(filepath: str) -> bool:
    """Best-effort audio detection that does not rely only on MoviePy state."""
    if not filepath or not os.path.exists(filepath):
        return False

    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "a",
                "-show_entries",
                "stream=index",
                "-of",
                "csv=p=0",
                filepath,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout.strip():
            return True
    except Exception as exc:
        logger.debug("ffprobe audio detection failed for %s: %s", filepath, exc)

    try:
        result = subprocess.run(
            ["ffmpeg", "-i", filepath],
            capture_output=True,
            text=True,
            check=False,
        )
        return "Audio:" in (result.stderr or "")
    except Exception as exc:
        logger.debug("ffmpeg audio detection fallback failed for %s: %s", filepath, exc)

    return False


def _clip_has_audio(clip: Any) -> bool:
    if getattr(clip, "_has_audio_stream", None) is not None:
        return bool(getattr(clip, "_has_audio_stream"))
    return getattr(clip, "audio", None) is not None

def extract_section_index(filepath: str) -> Optional[int]:
    """
    Extract section index from a filepath.
    
    Args:
        filepath: Path to the rendered clip file
        
    Returns:
        int: Extracted section index or None if couldn't extract
    """
    try:
        # Try the new naming pattern first (clip_idx123_...)
        match = re.search(r'clip_idx(\d+)_', os.path.basename(filepath))
        if match:
            return int(match.group(1))
        
        # Try the pattern with prerender prefix (prerender_5_...)
        match = re.search(r'prerender_(\d+)_', os.path.basename(filepath))
        if match:
            return int(match.group(1))
            
        # Try to extract from simple clip pattern (clip_005_...)
        match = re.search(r'clip_(\d+)_', os.path.basename(filepath))
        if match:
            return int(match.group(1))
            
        # If all else fails
        logger.warning(f"Could not extract index from rendered path: {filepath}")
        return None
    except Exception as e:
        logger.warning(f"Error extracting index from {filepath}: {e}")
        return None

def concatenate_with_crossfade(
    clip_paths: List[Tuple[int, str]],
    output_file: str,
    crossfade_duration: float = 1.0,
    preset: str = "ultrafast"
) -> str:
    """
    Concatenate clips with crossfade transitions, trying multiple methods with fallback.
    
    Args:
        clip_paths: List of (index, file path) tuples
        output_file: Path to output file
        crossfade_duration: Duration of crossfade in seconds
        preset: FFmpeg preset for encoding
        
    Returns:
        Path to the output file
    """
    if not clip_paths:
        raise ValueError("No clips provided for concatenation")
        
    # Sort by index
    sorted_paths = sorted(clip_paths, key=lambda x: x[0])
    
    # Log order
    logger.info("Concatenating clips in the following order:")
    for idx, path in sorted_paths:
        logger.info(f"  {idx}: {os.path.basename(path)}")
        
    # Handle single clip case
    if len(sorted_paths) == 1:
        _, path = sorted_paths[0]
        shutil.copy(path, output_file)
        logger.info(f"Only one clip provided, copied to {output_file}")
        return output_file
        
    # Validate clips
    valid_paths = _validate_clip_files(sorted_paths)
    if not valid_paths:
        raise ValueError("No valid clips found for concatenation")
    
    # For larger clip counts, prefer FFmpeg concat first to avoid opening too many files in MoviePy.
    moviepy_max_clips = int(os.getenv("SHORTS_MOVIEPY_CONCAT_MAX_CLIPS", "12"))
    if len(valid_paths) > moviepy_max_clips:
        logger.info(
            "Clip count (%s) exceeds MoviePy concat threshold (%s). Using FFmpeg concat first.",
            len(valid_paths),
            moviepy_max_clips,
        )
        success, result = _try_ffmpeg_concatenation(valid_paths, output_file)
        if success:
            return result

    # Try MoviePy with crossfades first for smaller batches
    success, result = _try_moviepy_concatenation(valid_paths, output_file, crossfade_duration, preset)
    if success:
        return result
    
    # Fall back to FFmpeg direct concatenation
    success, result = _try_ffmpeg_concatenation(valid_paths, output_file)
    if success:
        return result
    
    # Emergency fallback
    _, first_path = valid_paths[0]
    shutil.copy(first_path, output_file)
    logger.warning(f"All concatenation methods failed, copied first clip to {output_file}")
    return output_file

def _validate_clip_files(clip_paths: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    """Validate clip files and return only valid ones with correct indices."""
    valid_paths = []
    for idx, path in clip_paths:
        if not os.path.exists(path):
            logger.error(f"Clip file not found: {path}")
            continue
            
        if os.path.getsize(path) == 0:
            logger.error(f"Clip file is empty: {path}")
            continue
            
        # Check if we need to fix the index from the filename
        extracted_idx = extract_section_index(path)
        if extracted_idx is not None and extracted_idx != idx:
            logger.info(f"Correcting index from {idx} to {extracted_idx} for file: {os.path.basename(path)}")
            idx = extracted_idx
            
        valid_paths.append((idx, path))
    
    # Make sure paths are sorted by their index
    valid_paths.sort(key=lambda x: x[0])
    return valid_paths

def _try_moviepy_concatenation(
    clip_paths: List[Tuple[int, str]], 
    output_file: str, 
    crossfade_duration: float,
    preset: str
) -> Tuple[bool, Optional[str]]:
    """Try concatenation with MoviePy using different methods."""
    clips = []
    try:
        clip_indices = []
        
        # Load all clips
        for idx, path in clip_paths:
            try:
                # Skip invalid files
                if not os.path.exists(path) or os.path.getsize(path) == 0:
                    continue

                has_audio_stream = _path_has_audio_stream(path)
                clip = VideoFileClip(path)
                
                # Skip clips with invalid duration
                if clip.duration <= 0:
                    clip.close()
                    continue
                    
                clip._section_idx = idx
                clip._has_audio_stream = has_audio_stream or getattr(clip, "audio", None) is not None
                logger.info(
                    "Loaded clip %s: %s, duration=%.2fs, has_audio=%s",
                    idx,
                    os.path.basename(path),
                    clip.duration,
                    clip._has_audio_stream,
                )
                clips.append(clip)
                clip_indices.append(idx)
            except Exception as e:
                logger.error(f"Failed to load clip {idx}: {e}")
        
        # Ensure we have clips
        if not clips:
            return False, None
            
        # Single clip case
        if len(clips) == 1:
            try:
                _write_final_clip(clips[0], output_file, preset)
                clips[0].close()
                return True, output_file
            except Exception as e:
                logger.error(f"Failed to write single clip: {e}")
                _close_all_clips(clips)
                return False, None
        
        # Verify ordering
        if clip_indices != sorted(clip_indices):
            logger.warning(f"Reordering clips: {clip_indices} to {sorted(clip_indices)}")
            clips_with_idx = [(getattr(clip, '_section_idx', i), clip) for i, clip in enumerate(clips)]
            clips_with_idx.sort(key=lambda x: x[0])
            clips = [clip for _, clip in clips_with_idx]

        clips_have_audio = any(_clip_has_audio(clip) for clip in clips)

        # Try method 1: Direct crossfade
        if crossfade_duration > 0:
            if clips_have_audio:
                logger.info(
                    "Clips contain audio; skipping overlap crossfade so total narration length is preserved."
                )
                try:
                    success, result = _try_manual_fades(clips, output_file, crossfade_duration, preset)
                    if success:
                        return success, result
                except Exception as e:
                    logger.warning(f"Manual fades failed: {e}, trying simple concatenation")
            else:
                try:
                    success, result = _try_direct_crossfade(clips, output_file, crossfade_duration, preset)
                    if success:
                        return success, result
                except Exception as e:
                    logger.warning(f"Direct crossfade failed: {e}, trying method 2")

                # Try method 2: Manual fades
                try:
                    success, result = _try_manual_fades(clips, output_file, crossfade_duration, preset)
                    if success:
                        return success, result
                except Exception as e:
                    logger.warning(f"Manual fades failed: {e}, trying simple concatenation")
        
        # Try method 3: Simple concatenation
        try:
            success, result = _try_simple_concatenation(clips, output_file, preset)
            if success:
                return success, result
            return False, None
        except Exception as e:
            logger.error(f"All MoviePy methods failed: {e}")
            return False, None
    
    except Exception as e:
        logger.error(f"MoviePy concatenation failed: {e}")
        logger.error(traceback.format_exc())
        return False, None
    finally:
        # Always close opened file handles, even on non-exception failed paths.
        _close_all_clips(clips)

def _try_direct_crossfade(
    clips: List[Any], 
    output_file: str, 
    crossfade_duration: float,
    preset: str
) -> Tuple[bool, Optional[str]]:
    """Try concatenation with direct crossfade method."""
    try:
        logger.info("Using direct crossfade method")
        # Use a smaller crossfade duration to avoid audio issues
        actual_crossfade = min(crossfade_duration, 0.5)
        
        final_clip = concatenate_videoclips(
            clips,
            method="compose",
            padding=-actual_crossfade
        )
        
        _write_final_clip(final_clip, output_file, preset)
        
        # Clean up
        for clip in clips:
            clip.close()
        final_clip.close()
        
        logger.info(f"Successfully created video with crossfades: {output_file}")
        return True, output_file
    
    except Exception as e:
        logger.error(f"Direct crossfade failed: {e}")
        # Clean up any clips
        try:
            final_clip.close()
        except:
            pass
        return False, None

def _try_manual_fades(
    clips: List[Any], 
    output_file: str, 
    crossfade_duration: float,
    preset: str
) -> Tuple[bool, Optional[str]]:
    """Try concatenation with manual fade in/out method."""
    try:
        logger.info("Using manual fade in/out method")
        clips_with_fades = []
        
        for i, clip in enumerate(clips):
            modified_clip = clip.copy()
            
            # Add fade in for all except the first clip
            if i > 0:
                modified_clip = modified_clip.with_effects([FadeIn(crossfade_duration / 2)])
                
            # Add fade out for all except the last clip
            if i < len(clips) - 1:
                modified_clip = modified_clip.with_effects([FadeOut(crossfade_duration / 2)])
                
            clips_with_fades.append(modified_clip)
        
        # Concatenate
        final_clip = concatenate_videoclips(
            clips_with_fades,
            method="compose"
        )
        
        _write_final_clip(final_clip, output_file, preset)
        
        # Clean up
        for clip in clips:
            clip.close()
        for clip in clips_with_fades:
            clip.close()
        final_clip.close()
        
        logger.info(f"Successfully created video with manual fades: {output_file}")
        return True, output_file
    
    except Exception as e:
        logger.error(f"Manual fades failed: {e}")
        # Clean up
        try:
            for clip in clips_with_fades:
                clip.close()
            final_clip.close()
        except:
            pass
        return False, None

def _try_simple_concatenation(
    clips: List[Any], 
    output_file: str,
    preset: str
) -> Tuple[bool, Optional[str]]:
    """Try simple concatenation without transitions."""
    try:
        logger.info("Using simple concatenation without transitions")
        final_clip = concatenate_videoclips(clips)
        
        _write_final_clip(final_clip, output_file, preset)
        
        # Clean up
        for clip in clips:
            clip.close()
        final_clip.close()
        
        logger.info(f"Successfully created video with simple concatenation: {output_file}")
        return True, output_file
    
    except Exception as e:
        logger.error(f"Simple concatenation failed: {e}")
        try:
            final_clip.close()
        except:
            pass
        return False, None

def _write_final_clip(clip: Any, output_file: str, preset: str) -> None:
    """Write the final clip to file with optimized settings."""
    clip.write_videofile(
        output_file,
        fps=30,
        codec="libx264",
        audio_codec="aac",
        preset=preset,
        threads=4,
        ffmpeg_params=[
            '-movflags', '+faststart', 
            '-max_muxing_queue_size', '9999'
        ]
    )

def _try_ffmpeg_concatenation(
    clip_paths: List[Tuple[int, str]],
    output_file: str
) -> Tuple[bool, Optional[str]]:
    """Try concatenation using direct FFmpeg command."""
    try:
        logger.info("Using FFmpeg direct concatenation")
        
        # Create temporary concat list
        temp_dir = os.path.dirname(clip_paths[0][1])
        concat_list_path = os.path.join(temp_dir, f"concat_list_{uuid.uuid4().hex[:8]}.txt")
        
        # Write concat list
        with open(concat_list_path, "w") as f:
            for _, path in clip_paths:
                f.write(f"file '{os.path.abspath(path)}'\n")
        
        # Run FFmpeg
        ffmpeg_cmd = [
            "ffmpeg", "-y", 
            "-f", "concat", 
            "-safe", "0", 
            "-i", concat_list_path,
            "-c", "copy",
            "-movflags", "+faststart",
            output_file
        ]
        
        subprocess.run(ffmpeg_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Clean up
        if os.path.exists(concat_list_path):
            os.remove(concat_list_path)
        
        logger.info(f"Successfully concatenated clips with FFmpeg: {output_file}")
        return True, output_file
    
    except Exception as e:
        logger.error(f"FFmpeg concatenation failed: {e}")
        return False, None

def _close_all_clips(clips: List[Any]) -> None:
    """Safely close all clips."""
    for clip in clips:
        try:
            clip.close()
        except:
            pass 
