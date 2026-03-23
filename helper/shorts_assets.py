import logging
import os
import random
import subprocess
import json
import re
import tempfile
import math

from moviepy import (
    AudioFileClip,
    CompositeAudioClip,
    VideoFileClip,
    concatenate_audioclips,
    concatenate_videoclips,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DEFAULT_MUSIC_DIR = os.getenv("SHORTS_MUSIC_DIR", os.path.join(PROJECT_ROOT, "Music"))
DEFAULT_BRAINROT_DIR = os.getenv(
    "SHORTS_BRAINROT_DIR",
    os.path.join(PROJECT_ROOT, "AttentionGrabBrainRotVideos"),
)
DEFAULT_GREENSCREEN_DIR = os.getenv(
    "SHORTS_GREENSCREEN_DIR",
    os.path.join(PROJECT_ROOT, "GreenScreenAnimeGirls"),
)

AUDIO_EXTENSIONS = (".mp3", ".wav", ".m4a", ".aac", ".ogg", ".flac")
VIDEO_EXTENSIONS = (".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv")
HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


def get_default_font_path():
    candidates = [
        os.path.join(PROJECT_ROOT, "packages", "fonts", "default_font.ttf"),
        os.path.join(PROJECT_ROOT, "helper", "fonts", "default_font.ttf"),
    ]

    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate

    return None


def _list_media_files(directory, extensions):
    if not directory or not os.path.isdir(directory):
        return []

    files = []
    for entry in os.listdir(directory):
        path = os.path.join(directory, entry)
        if os.path.isfile(path) and entry.lower().endswith(extensions):
            files.append(path)
    return sorted(files)


def pick_random_background_music(music_dir=None):
    music_files = _list_media_files(music_dir or DEFAULT_MUSIC_DIR, AUDIO_EXTENSIONS)
    if not music_files:
        logger.warning("No background music files found in %s", music_dir or DEFAULT_MUSIC_DIR)
        return None

    selected = random.choice(music_files)
    logger.info("Selected background music: %s", os.path.basename(selected))
    return selected


def pick_random_brainrot_video(brainrot_dir=None):
    video_files = _list_media_files(brainrot_dir or DEFAULT_BRAINROT_DIR, VIDEO_EXTENSIONS)
    if not video_files:
        logger.warning("No brainrot overlay videos found in %s", brainrot_dir or DEFAULT_BRAINROT_DIR)
        return None

    selected = random.choice(video_files)
    logger.info("Selected brainrot overlay video: %s", os.path.basename(selected))
    return selected


def pick_random_greenscreen_video(greenscreen_dir=None):
    video_files = _list_media_files(greenscreen_dir or DEFAULT_GREENSCREEN_DIR, VIDEO_EXTENSIONS)
    if not video_files:
        logger.warning("No green-screen anime videos found in %s", greenscreen_dir or DEFAULT_GREENSCREEN_DIR)
        return None

    # SystemRandom avoids any accidental deterministic seeding from elsewhere.
    selected = random.SystemRandom().choice(video_files)
    logger.info(
        "Selected green-screen anime video (%s candidates): %s",
        len(video_files),
        os.path.basename(selected),
    )
    return selected


def pick_random_brainrot_start_time(video_path, min_remaining_seconds=60.0):
    """
    Pick a random start time while guaranteeing at least min_remaining_seconds remain.
    If the source video is too short, returns 0.0.
    """
    if not video_path or not os.path.exists(video_path):
        return 0.0

    source_video = None
    try:
        source_video = VideoFileClip(video_path).without_audio()
        duration = float(source_video.duration or 0.0)
        max_start = duration - float(min_remaining_seconds)
        if max_start <= 0:
            return 0.0
        return random.uniform(0.0, max_start)
    except Exception as exc:
        logger.warning("Failed to compute random brainrot start time for %s: %s", video_path, exc)
        return 0.0
    finally:
        try:
            if source_video:
                source_video.close()
        except Exception:
            pass


def _build_looped_audio_clip(audio_path, duration):
    if not audio_path or duration <= 0:
        return None

    source_audio = AudioFileClip(audio_path)
    if source_audio.duration <= 0:
        source_audio.close()
        return None

    remaining = duration
    audio_segments = []

    if source_audio.duration > duration:
        max_start = max(0, source_audio.duration - duration)
        start_time = random.uniform(0, max_start) if max_start > 0 else 0
        clip = source_audio.subclipped(start_time, start_time + duration).with_duration(duration)
        clip._source_audio = source_audio
        return clip

    while remaining > 0.01:
        segment_duration = min(remaining, source_audio.duration)
        audio_segments.append(source_audio.subclipped(0, segment_duration))
        remaining -= segment_duration

    looped_audio = (
        audio_segments[0]
        if len(audio_segments) == 1
        else concatenate_audioclips(audio_segments)
    ).with_duration(duration)
    looped_audio._source_audio = source_audio
    return looped_audio


def add_background_music_to_video(video_path, music_dir=None, selected_music_path=None, volume_scale=None, fps=30, preset="ultrafast"):
    if not video_path or not os.path.exists(video_path):
        return video_path

    music_path = selected_music_path or pick_random_background_music(music_dir)
    if not music_path:
        return video_path

    volume_scale = (
        float(os.getenv("SHORTS_MUSIC_VOLUME", "0.08"))
        if volume_scale is None
        else volume_scale
    )

    # Apply a small global boost to background music per user request (+5%)
    try:
        volume_scale = float(volume_scale) * 1.05
    except Exception:
        # If casting fails, leave as-is
        pass

    video_clip = None
    music_clip = None
    mixed_audio = None
    final_video = None
    temp_output = video_path.replace(".mp4", "_with_music.mp4")

    try:
        video_clip = VideoFileClip(video_path)
        music_clip = _build_looped_audio_clip(music_path, video_clip.duration)
        if not music_clip:
            return video_path

        music_clip = music_clip.with_volume_scaled(volume_scale)
        audio_layers = []
        if video_clip.audio:
            audio_layers.append(video_clip.audio)
        audio_layers.append(music_clip)

        mixed_audio = CompositeAudioClip(audio_layers)
        final_video = video_clip.with_audio(mixed_audio)
        final_video.write_videofile(
            temp_output,
            fps=fps,
            codec="libx264",
            audio_codec="aac",
            preset=preset,
            logger=None,
        )
        os.replace(temp_output, video_path)
        logger.info("Added background music to video using %s", os.path.basename(music_path))
        return video_path
    except Exception as exc:
        logger.error("Failed to add background music: %s", exc)
        if os.path.exists(temp_output):
            os.remove(temp_output)
        return video_path
    finally:
        for clip in (final_video, mixed_audio, music_clip, video_clip):
            try:
                if clip:
                    clip.close()
            except Exception:
                pass


def add_anime_greenscreen_overlay_to_video(
    video_path,
    greenscreen_dir=None,
    selected_overlay_path=None,
    scale_factor=0.6929,
    chroma_similarity=None,
    chroma_blend=None,
    preset="ultrafast",
):
    """
    Overlay a random green-screen anime clip centered in the 4th quadrant (bottom-right).
    Default scaling is 20% larger than previous baseline.
    """
    if not video_path or not os.path.exists(video_path):
        return video_path

    overlay_video_path = selected_overlay_path or pick_random_greenscreen_video(greenscreen_dir)
    if not overlay_video_path:
        return video_path

    chroma_similarity = (
        float(os.getenv("SHORTS_GREENSCREEN_CHROMA_SIMILARITY", "0.24"))
        if chroma_similarity is None
        else float(chroma_similarity)
    )
    chroma_blend = (
        float(os.getenv("SHORTS_GREENSCREEN_CHROMA_BLEND", "0.10"))
        if chroma_blend is None
        else float(chroma_blend)
    )

    temp_output = video_path.replace('.mp4', '_with_anime_overlay.mp4')

    filter_complex = (
        f"[1:v][0:v]scale2ref=w=main_w*{float(scale_factor):.4f}:h=main_h*{float(scale_factor):.4f}[anime_s][base];"
        f"[anime_s]chromakey=0x00FF00:{chroma_similarity:.4f}:{chroma_blend:.4f}[anime];"
        f"[base][anime]overlay="
        f"x='max(0,min(main_w-overlay_w,main_w*0.75-overlay_w/2))':"
        f"y='max(0,min(main_h-overlay_h,main_h*0.75-overlay_h/2))':"
        f"shortest=1[outv]"
    )

    cmd = [
        'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y',
        '-i', video_path,
        '-stream_loop', '-1', '-i', overlay_video_path,
        '-filter_complex', filter_complex,
        '-map', '[outv]',
        '-map', '0:a?',
        '-c:v', 'libx264',
        '-preset', preset,
        '-crf', '20',
        '-c:a', 'aac',
        '-shortest',
        temp_output,
    ]

    try:
        subprocess.run(cmd, check=True)
        os.replace(temp_output, video_path)
        logger.info(
            "Added green-screen anime overlay using %s",
            os.path.basename(overlay_video_path),
        )
        return video_path
    except Exception as exc:
        logger.error("Failed to add green-screen anime overlay: %s", exc)
        try:
            if os.path.exists(temp_output):
                os.remove(temp_output)
        except Exception:
            pass
        return video_path


def _chunk_caption_words(words, fast_mode=False):
    """Build 2-3 word chunks for readable fast captions."""
    if fast_mode:
        # Fewer, larger chunks reduce render overhead significantly.
        chunks = []
        i = 0
        while i < len(words):
            remaining = len(words) - i
            take = 5 if remaining > 6 else remaining
            chunks.append(words[i:i + take])
            i += take
        return chunks

    chunks = []
    i = 0
    use_three = True
    while i < len(words):
        remaining = len(words) - i
        if remaining <= 3:
            take = remaining
        else:
            take = 3 if use_three else 2
        if take == 1 and chunks:
            chunks[-1].append(words[i])
            break
        chunks.append(words[i:i + take])
        i += take
        use_three = not use_three
    return chunks


def _rebalance_caption_timeline(timeline, max_chunks):
    """Compress a caption timeline without dropping the back half of the narration."""
    if not timeline:
        return []
    if max_chunks is None or max_chunks <= 0 or len(timeline) <= max_chunks:
        return timeline

    merged = []
    cursor = 0
    total = len(timeline)
    while cursor < total and len(merged) < max_chunks:
        remaining = total - cursor
        remaining_groups = max(1, max_chunks - len(merged))
        take = int(math.ceil(remaining / remaining_groups))
        group = timeline[cursor:cursor + take]
        cursor += take
        if not group:
            continue

        start = float(group[0].get("start", 0.0) or 0.0)
        end = start
        text_parts = []
        words = []
        for item in group:
            duration = float(item.get("duration", 0.0) or 0.0)
            end = max(end, float(item.get("start", start) or start) + duration)
            text = str(item.get("text", "") or "").strip()
            if text:
                text_parts.append(text)
            words.extend(item.get("words", []) or [])

        merged.append(
            {
                "start": start,
                "duration": max(0.12, end - start),
                "text": " ".join(text_parts).strip(),
                "words": words,
            }
        )

    return merged


def _build_caption_timeline(script_sections):
    fast_mode = os.getenv("AUTO_CAPTIONS_FAST_MODE", "true").lower() == "true"
    max_chunks = max(1, int(os.getenv("AUTO_CAPTIONS_MAX_CHUNKS", "48")))
    timeline = []
    cursor = 0.0
    for section in script_sections or []:
        text = str(section.get("text", "") or "").strip()
        duration = float(section.get("duration", 0.0) or 0.0)
        if not text or duration <= 0.05:
            cursor += max(0.0, duration)
            continue

        words = [w for w in re.findall(r"[A-Za-z0-9']+", text) if w]
        if len(words) < 2:
            cursor += duration
            continue

        chunks = _chunk_caption_words(words, fast_mode=fast_mode)
        part_count = max(1, len(chunks))
        part_duration = duration / part_count
        section_cursor = cursor
        for chunk in chunks:
            chunk_duration = max(0.12, part_duration)
            timeline.append(
                {
                    "start": section_cursor,
                    "duration": chunk_duration,
                    "text": " ".join(chunk),
                    "words": [w.lower() for w in chunk],
                }
            )
            section_cursor += chunk_duration
        cursor += duration

    return _rebalance_caption_timeline(timeline, max_chunks)


def _chunk_words_with_timestamps(words, fast_mode=False, max_chunks=48):
    """Build caption chunks from timestamped words while preserving exact audio timing."""
    if not words:
        return []

    chunk_target = 5 if fast_mode else 3
    timeline = []
    i = 0
    while i < len(words):
        group = words[i:i + chunk_target]
        i += chunk_target
        if not group:
            continue

        start = float(group[0].get("start", 0.0) or 0.0)
        end = float(group[-1].get("end", start + 0.15) or (start + 0.15))
        text = " ".join([str(w.get("word", "")).strip() for w in group]).strip()
        if not text:
            continue

        timeline.append(
            {
                "start": start,
                "duration": max(0.12, end - start),
                "text": text,
                "words": [str(w.get("word", "")).strip().lower() for w in group if str(w.get("word", "")).strip()],
            }
        )

    return _rebalance_caption_timeline(timeline, max_chunks)


def transcribe_audio_to_word_timestamps(audio_path):
    """
    Transcribe an audio file with faster-whisper and return timestamped words.
    Returns an empty list if ASR is unavailable or transcription fails.
    """
    if not audio_path or not os.path.exists(audio_path):
        return []

    try:
        from faster_whisper import WhisperModel
    except Exception as exc:
        logger.info("faster-whisper unavailable for audio transcription: %s", exc)
        return []

    model_size = os.getenv("AUTO_CAPTIONS_WHISPER_MODEL", "tiny")
    device = os.getenv("AUTO_CAPTIONS_WHISPER_DEVICE", "cpu")
    compute_type = os.getenv("AUTO_CAPTIONS_WHISPER_COMPUTE_TYPE", "int8")

    try:
        model = WhisperModel(model_size, device=device, compute_type=compute_type)
        segments, _ = model.transcribe(
            audio_path,
            word_timestamps=True,
            vad_filter=True,
            beam_size=1,
        )
    except Exception as exc:
        logger.warning("Audio transcription failed for %s: %s", audio_path, exc)
        return []

    words = []
    for seg in segments:
        for word in (getattr(seg, "words", None) or []):
            raw_word = str(getattr(word, "word", "") or "").strip()
            if not raw_word:
                continue
            start = float(getattr(word, "start", 0.0) or 0.0)
            end = float(getattr(word, "end", start) or start)
            words.append(
                {
                    "start": start,
                    "end": max(start, end),
                    "word": raw_word,
                }
            )

    if words:
        logger.info("Transcribed %s words from %s", len(words), os.path.basename(audio_path))
    return words


def build_transcript_text(words):
    return " ".join(str(item.get("word", "")).strip() for item in words if str(item.get("word", "")).strip()).strip()


def build_script_sections_from_word_timestamps(
    words,
    *,
    min_chunks=8,
    max_chunks=16,
    min_words_per_chunk=4,
    max_words_per_chunk=9,
):
    """
    Turn Whisper word timestamps into timed script sections for image/video planning.
    """
    if not words:
        return []

    cleaned_words = []
    for item in words:
        token = str(item.get("word", "") or "").strip()
        if not token:
            continue
        start = float(item.get("start", 0.0) or 0.0)
        end = float(item.get("end", start) or start)
        cleaned_words.append(
            {
                "start": start,
                "end": max(start, end),
                "word": token,
            }
        )

    if not cleaned_words:
        return []

    total_words = len(cleaned_words)
    desired_chunks = max(min_chunks, min(max_chunks, int(round(total_words / 6.0)) or 1))
    chunk_size = int(math.ceil(total_words / desired_chunks))
    chunk_size = max(min_words_per_chunk, min(max_words_per_chunk, chunk_size))

    grouped_words = []
    cursor = 0
    while cursor < total_words:
        remaining = total_words - cursor
        remaining_groups = max(1, desired_chunks - len(grouped_words))
        take = int(math.ceil(remaining / remaining_groups))
        take = max(1, min(chunk_size, take, max_words_per_chunk))
        group = cleaned_words[cursor:cursor + take]

        # Avoid a tiny trailing fragment by merging it into the current section.
        trailing = total_words - (cursor + take)
        if 0 < trailing < min_words_per_chunk and (cursor + take) < total_words:
            group = cleaned_words[cursor:cursor + take + trailing]
            take += trailing

        grouped_words.append(group)
        cursor += take

    sections = []
    for idx, group in enumerate(grouped_words):
        if not group:
            continue

        start = float(group[0].get("start", 0.0) or 0.0)
        end = float(group[-1].get("end", start) or start)
        text = " ".join(str(word.get("word", "")).strip() for word in group).strip()
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue

        sections.append(
            {
                "id": f"transcript_section_{idx}",
                "text": text,
                "duration": max(0.12, end - start),
                "voice_style": "male",
                "speaker": "boy",
                "start_time": start,
                "end_time": end,
                "word_timestamps": group,
            }
        )

    if sections:
        logger.info("Built %s timed script sections from Whisper transcript", len(sections))
    return sections


def _build_caption_timeline_from_section_words(script_sections):
    fast_mode = os.getenv("AUTO_CAPTIONS_FAST_MODE", "true").lower() == "true"
    max_chunks = max(1, int(os.getenv("AUTO_CAPTIONS_MAX_CHUNKS", "48")))
    words = []
    for section in script_sections or []:
        for item in section.get("word_timestamps", []) or []:
            token = str(item.get("word", "") or "").strip()
            if not token:
                continue
            words.append(
                {
                    "start": float(item.get("start", 0.0) or 0.0),
                    "end": float(item.get("end", 0.0) or 0.0),
                    "word": token,
                }
            )

    if not words:
        return []

    timeline = _chunk_words_with_timestamps(words, fast_mode=fast_mode, max_chunks=max_chunks)
    if timeline:
        logger.info("Built caption timeline from precomputed transcript words (%s chunks)", len(timeline))
    return timeline


def _build_caption_timeline_from_audio(video_path):
    """
    Build caption timeline from real ASR timestamps using faster-whisper (free/open source).
    Falls back to empty list if dependency/model fails.
    """
    if not video_path or not os.path.exists(video_path):
        return []

    fd, wav_path = tempfile.mkstemp(suffix=".wav")
    os.close(fd)

    try:
        # Extract mono 16k wav for faster transcription.
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            "-i", video_path,
            "-vn",
            "-ac", "1",
            "-ar", "16000",
            wav_path,
        ]
        subprocess.run(cmd, check=True)

        fast_mode = os.getenv("AUTO_CAPTIONS_FAST_MODE", "true").lower() == "true"
        max_chunks = max(1, int(os.getenv("AUTO_CAPTIONS_MAX_CHUNKS", "48")))
        words = transcribe_audio_to_word_timestamps(wav_path)
        timeline = _chunk_words_with_timestamps(words, fast_mode=fast_mode, max_chunks=max_chunks)
        if timeline:
            logger.info("Built caption timeline from audio timestamps (%s chunks)", len(timeline))
        return timeline
    except Exception as exc:
        logger.warning("Audio-timed caption generation failed: %s", exc)
        return []
    finally:
        try:
            if os.path.exists(wav_path):
                os.remove(wav_path)
        except Exception:
            pass


def _get_gpt_caption_colors(script_sections):
    """Ask GPT for optional highlight colors for selected words."""
    try:
        from automation.scitely_client import create_chat_completion
        from automation.content_generator import _parse_json_response, _extract_completion_content
    except Exception as exc:
        logger.info("Caption color planner unavailable: %s", exc)
        return {}

    joined = " ".join(str(s.get("text", "") or "") for s in script_sections or [])
    words = [w.lower() for w in re.findall(r"[A-Za-z]{4,}", joined)]
    dedup = []
    for w in words:
        if w not in dedup:
            dedup.append(w)
    candidates = dedup[:60]
    if not candidates:
        return {}

    prompt = (
        "Pick up to 18 emotionally important words from the provided word list and assign each "
        "a hex color (#RRGGBB) for video caption highlight effects. Return JSON only in shape "
        "{\"colors\": {\"word\": \"#RRGGBB\"}}.\n"
        f"Words: {', '.join(candidates)}"
    )

    try:
        response = create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=260,
            temperature=0.7,
        )
        content = _extract_completion_content(response)
        parsed = _parse_json_response(content)
        raw = parsed.get("colors", {}) if isinstance(parsed, dict) else {}
        result = {}
        for word, hex_color in raw.items():
            key = str(word or "").strip().lower()
            val = str(hex_color or "").strip()
            if key and HEX_COLOR_RE.match(val):
                result[key] = val
        return result
    except Exception as exc:
        logger.warning("GPT caption color planning failed: %s", exc)
        return {}


def _extract_preplanned_caption_colors(script_sections):
    color_map = {}
    for section in script_sections or []:
        raw_map = section.get("caption_color_map")
        if not isinstance(raw_map, dict):
            continue
        for word, hex_color in raw_map.items():
            key = str(word or "").strip().lower()
            value = str(hex_color or "").strip()
            if key and HEX_COLOR_RE.match(value):
                color_map[key] = value
    return color_map


def add_dynamic_auto_captions_to_video(
    video_path,
    script_sections,
    font_size=55,
    position_ratio=0.5,
    preset="ultrafast",
):
    """
    Add dynamic middle-screen captions after final composition.
    Captions are timed from spoken section durations and displayed in 2-3 word chunks.
    """
    if not video_path or not os.path.exists(video_path):
        return video_path
    if not script_sections:
        return video_path

    try:
        from moviepy import CompositeVideoClip, TextClip, VideoFileClip
    except Exception as exc:
        logger.error("Dynamic caption dependencies unavailable: %s", exc)
        return video_path

    # Prefer precomputed transcript timestamps, then real audio-based timestamps, then duration estimation.
    timeline = _build_caption_timeline_from_section_words(script_sections)
    use_audio_timestamps = os.getenv("AUTO_CAPTIONS_USE_AUDIO_TIMESTAMPS", "true").lower() == "true"
    if not timeline and use_audio_timestamps:
        timeline = _build_caption_timeline_from_audio(video_path)
    if not timeline:
        timeline = _build_caption_timeline(script_sections)
    if not timeline:
        logger.warning("No caption timeline generated; skipping auto captions")
        return video_path

    fast_mode = os.getenv("AUTO_CAPTIONS_FAST_MODE", "true").lower() == "true"
    color_map = _extract_preplanned_caption_colors(script_sections)
    if not color_map and not fast_mode:
        color_map = _get_gpt_caption_colors(script_sections)
    palette = ["#FFD54F", "#4FC3F7", "#FF8A80", "#A5D6A7", "#CE93D8", "#FFB74D"]

    base_clip = None
    composited = None
    temp_output = video_path.replace(".mp4", "_with_captions.mp4")
    try:
        base_clip = VideoFileClip(video_path)
        center_y = int(base_clip.h * float(position_ratio))
        font_path = get_default_font_path() or ""

        caption_layers = [base_clip]
        for idx, item in enumerate(timeline):
            text = item["text"]
            start = float(item["start"])
            duration = float(item["duration"])
            words = item.get("words", [])

            highlight = None
            for w in words:
                if w in color_map:
                    highlight = color_map[w]
                    break
            if not highlight:
                highlight = palette[idx % len(palette)]

            if fast_mode:
                # Single-layer captions for faster composition.
                core = TextClip(
                    text=text,
                    font=font_path,
                    font_size=font_size,
                    color="#FFFFFF",
                    stroke_color=highlight,
                    stroke_width=3,
                    method="caption",
                    size=(int(base_clip.w * 0.9), None),
                ).with_start(start).with_duration(duration).with_position(("center", center_y))
                caption_layers.append(core)
            else:
                # Outer glow layer
                glow = TextClip(
                    text=text,
                    font=font_path,
                    font_size=font_size,
                    color=highlight,
                    stroke_color=highlight,
                    stroke_width=8,
                    method="caption",
                    size=(int(base_clip.w * 0.9), None),
                ).with_start(start).with_duration(duration).with_position(("center", center_y)).with_opacity(0.22)

                # Main bold/fat readable layer
                core = TextClip(
                    text=text,
                    font=font_path,
                    font_size=font_size,
                    color="#FFFFFF",
                    stroke_color=highlight,
                    stroke_width=4,
                    method="caption",
                    size=(int(base_clip.w * 0.9), None),
                ).with_start(start).with_duration(duration).with_position(("center", center_y))

                # Quick shimmer pass to simulate a shine sweep
                shine_dur = max(0.12, min(0.28, duration * 0.35))
                shine = TextClip(
                    text=text,
                    font=font_path,
                    font_size=font_size,
                    color="#FFFFFF",
                    stroke_color="#FFFFFF",
                    stroke_width=2,
                    method="caption",
                    size=(int(base_clip.w * 0.9), None),
                ).with_start(start + min(0.08, duration * 0.2)).with_duration(shine_dur).with_position(("center", center_y - 1)).with_opacity(0.55)

                caption_layers.extend([glow, core, shine])

        composited = CompositeVideoClip(caption_layers, size=(base_clip.w, base_clip.h)).with_duration(base_clip.duration)
        if base_clip.audio:
            composited = composited.with_audio(base_clip.audio)

        composited.write_videofile(
            temp_output,
            fps=max(24, int(base_clip.fps or 30)),
            codec="libx264",
            audio_codec="aac",
            preset=preset,
            logger=None,
        )

        os.replace(temp_output, video_path)
        logger.info("Added dynamic auto captions (%s chunks)", len(timeline))
        return video_path
    except Exception as exc:
        logger.error("Failed to add dynamic captions: %s", exc)
        try:
            if os.path.exists(temp_output):
                os.remove(temp_output)
        except Exception:
            pass
        return video_path
    finally:
        try:
            if composited:
                composited.close()
        except Exception:
            pass
        try:
            if base_clip:
                base_clip.close()
        except Exception:
            pass


def build_brainrot_overlay_clip(
    video_path,
    start_time,
    duration,
    canvas_size=(1080, 1920),
    top_height_ratio=0.5,
):
    if not video_path or duration <= 0:
        return None

    source_video = VideoFileClip(video_path).without_audio()
    if source_video.duration <= 0:
        source_video.close()
        return None

    cursor = start_time % source_video.duration
    remaining = duration
    video_segments = []

    while remaining > 0.01:
        available = source_video.duration - cursor
        segment_duration = min(remaining, available)
        video_segments.append(source_video.subclipped(cursor, cursor + segment_duration))
        remaining -= segment_duration
        cursor = 0

    overlay_clip = (
        video_segments[0]
        if len(video_segments) == 1
        else concatenate_videoclips(video_segments, method="compose")
    ).with_duration(duration)

    target_width = canvas_size[0]
    target_height = int(canvas_size[1] * top_height_ratio)

    if overlay_clip.w / overlay_clip.h < target_width / target_height:
        overlay_clip = overlay_clip.resized(width=target_width)
    else:
        overlay_clip = overlay_clip.resized(height=target_height)

    crop_x = max(0, int((overlay_clip.w - target_width) / 2))
    crop_y = max(0, int((overlay_clip.h - target_height) / 2))
    overlay_clip = overlay_clip.cropped(
        x1=crop_x,
        y1=crop_y,
        x2=crop_x + target_width,
        y2=crop_y + target_height,
    )
    overlay_clip = overlay_clip.with_position(("center", 0)).with_duration(duration)
    overlay_clip._source_video = source_video
    return overlay_clip
