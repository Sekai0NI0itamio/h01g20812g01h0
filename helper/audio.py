import os
import time
import logging
import concurrent.futures
from typing import List, Dict, Any, Optional
from moviepy  import AudioFileClip, concatenate_audioclips
from helper.minor_helper import measure_time
from dotenv import load_dotenv
import subprocess
import tempfile
import re

# Set up logging
logger = logging.getLogger(__name__)

load_dotenv()

# Get temp directory from environment variable or use default
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp"))
# Create audio subdirectory
audio_temp_dir = os.path.join(TEMP_DIR, "audio_clips")
os.makedirs(audio_temp_dir, exist_ok=True)  # Create temp directory if it doesn't exist

class AudioHelper:
    def __init__(self, temp_dir=None):
        """
        Initialize audio helper with necessary settings

        Args:
            temp_dir (str): Directory to save temporary audio files
        """
        self.temp_dir = temp_dir or audio_temp_dir
        os.makedirs(self.temp_dir, exist_ok=True)

        self.freevoicereader_tts = None
        self.max_tts_gap_seconds = float(os.getenv("TTS_MAX_GAP_SECONDS", "0.7"))
        self.tts_max_retries = max(1, int(os.getenv("TTS_GENERATION_RETRIES", "3")))
        self.tts_retry_backoff_seconds = max(0.2, float(os.getenv("TTS_RETRY_BACKOFF_SECONDS", "1.0")))
        self.tts_regen_passes = max(1, int(os.getenv("TTS_REGENERATION_PASSES", "2")))
        self.tts_max_workers = max(1, int(os.getenv("SHORTS_TTS_MAX_WORKERS", "4")))
        self.tts_speed_factor = max(0.8, min(2.0, float(os.getenv("TTS_SPEED_FACTOR", "1.2"))))
        use_freevoicereader = os.getenv("USE_FREEVOICEREADER_TTS", "true").lower()
        logger.info("USE_FREEVOICEREADER_TTS=%s", use_freevoicereader)

        if use_freevoicereader == "true":
            try:
                from automation.voiceover_freevoicereader import FreeVoiceReaderVoiceover

                self.freevoicereader_tts = FreeVoiceReaderVoiceover(output_dir=self.temp_dir)
                logger.info("FreeVoiceReader TTS initialized successfully")
            except Exception as e:
                logger.exception("Failed to initialize FreeVoiceReader TTS")

    @measure_time
    def create_tts_audio(self, text, filename=None, voice_style="none", min_duration=None):
        """
        Create TTS audio file with robust error handling

        Args:
            text (str): Text to convert to speech
            filename (str): Output filename
            voice_style (str): Style of voice ('excited', 'calm', etc.)

        Returns:
            str: Path to the audio file or None if all methods fail
        """
        if not filename:
            filename = os.path.join(self.temp_dir, f"tts_{int(time.time())}.wav")

        # Enforce male narration globally for TTS generation.
        voice_style = "male"

        # Clean symbols that tend to be read literally by TTS engines.
        text = self._sanitize_tts_text(text)

        # Make sure text is not empty and has minimum length
        if not text or len(text.strip()) == 0:
            text = "No text provided"
        elif len(text.strip()) < 5:
            # For very short texts like "Check it out!", expand it slightly to ensure TTS works well
            text = text.strip() + "."  # Add period if missing

        if self.freevoicereader_tts:
            last_error = None
            for attempt in range(1, self.tts_max_retries + 1):
                try:
                    out = self.freevoicereader_tts.generate_speech(
                        text,
                        output_filename=filename,
                        voice_style=voice_style,
                    )
                    if not self._is_valid_audio_file(out):
                        raise RuntimeError(f"Generated audio file is missing or empty: {out}")

                    try:
                        sped = self._speedup_audio(out, speed=self.tts_speed_factor)
                        normalized = self._normalize_tts_pacing(sped)
                        final_path = self._ensure_min_duration(normalized, min_duration)
                        if self._is_valid_audio_file(final_path):
                            return final_path
                        raise RuntimeError(f"Post-processed audio file is missing or empty: {final_path}")
                    except Exception:
                        if self._is_valid_audio_file(out):
                            return out
                        raise
                except Exception as e:
                    last_error = e
                    logger.error("FreeVoiceReader TTS failed (attempt %s/%s): %s", attempt, self.tts_max_retries, e)
                    if attempt < self.tts_max_retries:
                        time.sleep(self.tts_retry_backoff_seconds * attempt)

            if last_error:
                logger.error("FreeVoiceReader TTS exhausted retries: %s", last_error)
            return None

        logger.error("FreeVoiceReader TTS is disabled or unavailable (USE_FREEVOICEREADER_TTS=%s, initialized=%s)", os.getenv("USE_FREEVOICEREADER_TTS", "true"), bool(self.freevoicereader_tts))
        return None

    def _sanitize_tts_text(self, text):
        """
        Keep only letters/numbers/whitespace and the punctuation marks: " . , '
        This prevents the TTS engine from reading symbols aloud.
        """
        if text is None:
            return ""

        # Normalize to string and remove disallowed symbols.
        cleaned = str(text)
        cleaned = re.sub(r"[^A-Za-z0-9\s\"\.,']+", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    @measure_time
    def generate_audio_clips_parallel(self, script_sections, voice_style=None, max_workers=None):
        """
        Generate audio clips for all script sections in parallel

        Args:
            script_sections (list): List of script sections with 'text' key
            voice_style (str): Voice style to use
            max_workers (int): Maximum number of concurrent workers

        Returns:
            list: List of audio file paths
        """
        start_time = time.time()
        logger.info(f"Generating {len(script_sections)} audio clips in parallel")

        def process_section(index, section):
            section_voice = section.get('voice_style', voice_style)
            text = section.get('text', '')
            section_id = section.get('id') or f"section_{index}_{int(time.time())}"
            filename = os.path.join(self.temp_dir, f"audio_{section_id}.wav")
            target_duration = float(section.get('duration', 0.0) or 0.0)

            return self.create_tts_audio(text, filename, section_voice, min_duration=target_duration)

        workers = max_workers or min(len(script_sections), self.tts_max_workers)
        audio_files = [None] * len(script_sections)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_section, i, section): i
                      for i, section in enumerate(script_sections)}

            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    audio_path = future.result()
                    if self._is_valid_audio_file(audio_path):
                        audio_files[idx] = audio_path
                    else:
                        logger.warning("Audio clip missing or invalid for section %s", idx)
                except Exception as e:
                    logger.error(f"Error generating audio for section {idx}: {e}")

        # Recovery pass for missing clips (sequential-ish to reduce upstream timeout/rate pressure)
        for regen_pass in range(1, self.tts_regen_passes + 1):
            missing_indices = [i for i, path in enumerate(audio_files) if not self._is_valid_audio_file(path)]
            if not missing_indices:
                break

            logger.warning(
                "Regenerating %s missing audio clips (pass %s/%s)",
                len(missing_indices),
                regen_pass,
                self.tts_regen_passes,
            )

            for idx in missing_indices:
                section = script_sections[idx]
                section_voice = section.get('voice_style', voice_style)
                text = section.get('text', '')
                section_id = section.get('id') or f"section_{idx}_{int(time.time())}"
                retry_filename = os.path.join(self.temp_dir, f"audio_{section_id}_regen{regen_pass}.wav")
                target_duration = float(section.get('duration', 0.0) or 0.0)
                regenerated = self.create_tts_audio(
                    text,
                    retry_filename,
                    section_voice,
                    min_duration=target_duration,
                )
                if self._is_valid_audio_file(regenerated):
                    audio_files[idx] = regenerated

        total_time = time.time() - start_time
        valid_count = sum(1 for p in audio_files if self._is_valid_audio_file(p))
        logger.info(
            "Generated %s/%s valid audio clips in %.2f seconds",
            valid_count,
            len(script_sections),
            total_time,
        )

        return audio_files

    @measure_time
    def combine_audio_clips(self, audio_files, output_filename=None):
        """
        Combine multiple audio clips into a single file

        Args:
            audio_files (list): List of audio file paths
            output_filename (str): Output file path

        Returns:
            str: Path to combined audio file
        """
        if not audio_files:
            logger.warning("No audio files to combine")
            return None

        if not output_filename:
            output_filename = os.path.join(self.temp_dir, f"combined_audio_{int(time.time())}.mp3")

        try:
            clips = [AudioFileClip(f) for f in audio_files]
            combined = concatenate_audioclips(clips)
            combined.write_audiofile(output_filename, logger=None)

            # Close all clips to release resources
            for clip in clips:
                clip.close()
            combined.close()

            return output_filename
        except Exception as e:
            logger.error(f"Error combining audio clips: {e}")
            return None

    @measure_time
    def process_audio_for_script(self, script_sections, voice_style=None, max_workers=None):
        """
        Process audio for all script sections and return audio files with durations

        Args:
            script_sections (list): List of script sections
            voice_style (str): Voice style to use
            max_workers (int): Maximum number of concurrent workers

        Returns:
            list: List of dicts with audio file paths and duration info
        """
        # Generate audio for all sections in parallel
        audio_files = self.generate_audio_clips_parallel(
            script_sections, voice_style, max_workers
        )

        # Get durations for each audio file
        audio_data = []
        for i, audio_file in enumerate(audio_files):
            if self._is_valid_audio_file(audio_file):
                try:
                    clip = AudioFileClip(audio_file)
                    duration = clip.duration
                    clip.close()

                    audio_data.append({
                        'path': audio_file,
                        'duration': duration,
                        'section_idx': i
                    })
                except Exception as e:
                    logger.error(f"Error getting audio duration for {audio_file}: {e}")
                    audio_data.append(None)
            else:
                audio_data.append(None)

        return audio_data

    def ensure_audio_data_complete(self, script_sections, audio_data, voice_style=None):
        """
        Validate audio_data alignment and regenerate missing/invalid section audio files.
        Returns a list aligned to script_sections where each entry is either audio metadata dict or None.
        """
        normalized = list(audio_data or [])
        if len(normalized) < len(script_sections):
            normalized.extend([None] * (len(script_sections) - len(normalized)))
        elif len(normalized) > len(script_sections):
            normalized = normalized[:len(script_sections)]

        for idx, section in enumerate(script_sections):
            item = normalized[idx]
            path = item.get("path") if isinstance(item, dict) else None
            if self._is_valid_audio_file(path):
                continue

            section_voice = section.get('voice_style', voice_style)
            text = section.get('text', '')
            section_id = section.get('id') or f"section_{idx}_{int(time.time())}"
            regen_filename = os.path.join(self.temp_dir, f"audio_{section_id}_ensure.wav")
            target_duration = float(section.get('duration', 0.0) or 0.0)
            regenerated = self.create_tts_audio(
                text,
                regen_filename,
                section_voice,
                min_duration=target_duration,
            )

            if self._is_valid_audio_file(regenerated):
                duration = target_duration
                try:
                    clip = AudioFileClip(regenerated)
                    duration = float(clip.duration or target_duration)
                    clip.close()
                except Exception:
                    pass

                normalized[idx] = {
                    'path': regenerated,
                    'duration': duration,
                    'section_idx': idx,
                }
                logger.info("Recovered missing audio for section %s", idx)
            else:
                normalized[idx] = None
                logger.warning("Could not recover audio for section %s", idx)

        return normalized

    def slice_audio_by_sections(self, audio_path, script_sections, output_prefix="master_section"):
        """
        Slice a master narration audio file into per-section clips using transcript timestamps.
        """
        if not self._is_valid_audio_file(audio_path):
            logger.warning("Master audio is missing or invalid: %s", audio_path)
            return []

        audio_data = []
        final_section_tail_pad = max(0.0, float(os.getenv("SHORTS_FINAL_SECTION_AUDIO_TAIL_PAD", "0.42")))
        section_tail_pad = max(0.0, float(os.getenv("SHORTS_SECTION_AUDIO_TAIL_PAD", "0.03")))
        last_index = max(0, len(script_sections or []) - 1)
        for idx, section in enumerate(script_sections or []):
            start_time = max(0.0, float(section.get("start_time", 0.0) or 0.0))
            transcript_end_time = max(start_time, float(section.get("end_time", start_time) or start_time))
            tail_pad = final_section_tail_pad if idx == last_index else section_tail_pad
            end_time = max(start_time, transcript_end_time + tail_pad)
            clip_duration = max(0.12, end_time - start_time)
            output_path = os.path.join(self.temp_dir, f"{output_prefix}_{idx:02d}.wav")

            try:
                cmd = [
                    "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-i", audio_path,
                    "-ss", f"{start_time:.3f}",
                    "-t", f"{clip_duration:.3f}",
                    "-acodec", "pcm_s16le",
                    output_path,
                ]
                subprocess.run(cmd, check=True)
                if not self._is_valid_audio_file(output_path):
                    raise RuntimeError(f"Sliced audio clip is missing or empty: {output_path}")

                audio_data.append(
                    {
                        "path": output_path,
                        "duration": clip_duration,
                        "section_idx": idx,
                        "start_time": start_time,
                        "end_time": end_time,
                        "transcript_end_time": transcript_end_time,
                        "preserve_timing": True,
                        "source": "master_paragraph",
                    }
                )
            except Exception as exc:
                logger.error("Failed to slice audio for section %s: %s", idx, exc)
                audio_data.append(None)

        return audio_data

    def _is_valid_audio_file(self, path):
        if not path:
            return False
        try:
            return os.path.exists(path) and os.path.getsize(path) > 128
        except Exception:
            return False

    def _speedup_audio(self, input_path, speed=1.3):
        """
        Speed up an audio file using ffmpeg and replace it atomically.
        Returns the path to the sped-up file (same path as input_path).
        """
        try:
            base, ext = os.path.splitext(input_path)
            # Create temp output in same directory to avoid cross-device moves
            dirn = os.path.dirname(input_path) or '.'
            fd, tmp_out = tempfile.mkstemp(suffix=ext, dir=dirn)
            os.close(fd)

            cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
                '-i', input_path,
                '-filter:a', f"atempo={float(speed):.2f}",
                tmp_out
            ]
            subprocess.run(cmd, check=True)

            # Replace original file with sped version
            os.replace(tmp_out, input_path)
            logger.info(f"Sped up audio {input_path} by {speed}x")
            return input_path
        except Exception as e:
            logger.error(f"Failed to speed up audio {input_path}: {e}")
            # Cleanup temp file if exists
            try:
                if 'tmp_out' in locals() and os.path.exists(tmp_out):
                    os.remove(tmp_out)
            except Exception:
                pass
            raise

    def _normalize_tts_pacing(self, input_path):
        """
        Trim excessive silence so pauses between consecutive TTS clips stay short.
        Keeps at most self.max_tts_gap_seconds trailing silence.
        """
        try:
            _, ext = os.path.splitext(input_path)
            dirn = os.path.dirname(input_path) or '.'
            fd, tmp_out = tempfile.mkstemp(suffix=ext, dir=dirn)
            os.close(fd)

            keep_tail = max(0.05, min(self.max_tts_gap_seconds, 0.25))
            # Trim trailing silence only (via reverse-pass) to avoid clipping spoken attack words.
            filter_chain = (
                f"areverse,silenceremove=start_periods=1:start_silence={keep_tail:.2f}:"
                "start_threshold=-55dB,areverse"
            )

            cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
                '-i', input_path,
                '-af', filter_chain,
                tmp_out
            ]
            subprocess.run(cmd, check=True)
            os.replace(tmp_out, input_path)
            logger.info("Normalized TTS pacing for %s (max gap %.2fs)", input_path, keep_tail)
            return input_path
        except Exception as e:
            logger.warning("Failed to normalize TTS pacing for %s: %s", input_path, e)
            try:
                if 'tmp_out' in locals() and os.path.exists(tmp_out):
                    os.remove(tmp_out)
            except Exception:
                pass
            return input_path

    def _ensure_min_duration(self, input_path, min_duration):
        """Pad with silence when needed so a section's sped-up clip never undershoots its target duration."""
        try:
            min_duration = float(min_duration or 0.0)
            if min_duration <= 0.0:
                return input_path

            clip = AudioFileClip(input_path)
            current_duration = float(clip.duration or 0.0)
            clip.close()

            if current_duration >= min_duration:
                return input_path

            _, ext = os.path.splitext(input_path)
            dirn = os.path.dirname(input_path) or '.'
            fd, tmp_out = tempfile.mkstemp(suffix=ext, dir=dirn)
            os.close(fd)

            cmd = [
                'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
                '-i', input_path,
                '-af', f"apad,atrim=0:{min_duration:.3f}",
                tmp_out,
            ]
            subprocess.run(cmd, check=True)
            os.replace(tmp_out, input_path)
            logger.info(
                "Padded audio %s from %.2fs to minimum %.2fs",
                input_path,
                current_duration,
                min_duration,
            )
            return input_path
        except Exception as e:
            logger.warning("Failed to enforce min duration for %s: %s", input_path, e)
            try:
                if 'tmp_out' in locals() and os.path.exists(tmp_out):
                    os.remove(tmp_out)
            except Exception:
                pass
            return input_path
