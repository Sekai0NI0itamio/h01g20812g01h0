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
        self.max_tts_gap_seconds = float(os.getenv("TTS_MAX_GAP_SECONDS", "0.4"))
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
    def create_tts_audio(self, text, filename=None, voice_style="none"):
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
            try:
                out = self.freevoicereader_tts.generate_speech(
                    text,
                    output_filename=filename,
                    voice_style=voice_style,
                )
                try:
                    sped = self._speedup_audio(out, speed=1.3)
                    return self._normalize_tts_pacing(sped)
                except Exception:
                    return out
            except Exception as e:
                logger.error(f"FreeVoiceReader TTS failed: {e}")
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

        def process_section(section):
            section_voice = section.get('voice_style', voice_style)
            text = section.get('text', '')
            section_id = section.get('id', int(time.time()))
            filename = os.path.join(self.temp_dir, f"audio_{section_id}.wav")

            return self.create_tts_audio(text, filename, section_voice)

        workers = max_workers or min(len(script_sections), os.cpu_count() * 2)
        audio_files = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(process_section, section): i
                      for i, section in enumerate(script_sections)}

            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    audio_path = future.result()
                    if audio_path:
                        # Store the result in the correct order
                        while len(audio_files) <= idx:
                            audio_files.append(None)
                        audio_files[idx] = audio_path
                except Exception as e:
                    logger.error(f"Error generating audio for section {idx}: {e}")

        total_time = time.time() - start_time
        logger.info(f"Generated {len(audio_files)} audio clips in {total_time:.2f} seconds")

        # Filter out None values
        audio_files = [f for f in audio_files if f]

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
            if audio_file:
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

        return audio_data

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

            keep_tail = max(0.05, min(self.max_tts_gap_seconds, 0.4))
            filter_chain = (
                "silenceremove="
                f"start_periods=1:start_silence=0.05:start_threshold=-40dB:"
                f"stop_periods=1:stop_silence={keep_tail:.2f}:stop_threshold=-40dB"
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
