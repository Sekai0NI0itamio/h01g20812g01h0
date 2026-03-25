import argparse
import concurrent.futures
import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

from helper.runtime import require_actions_runtime


logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOUND_EFFECTS_DIR = PROJECT_ROOT / "SoundEffects"
DEFAULT_BUNDLES_DIR = PROJECT_ROOT / "workflow_bundles"
DEFAULT_ARTIFACTS_DIR = PROJECT_ROOT / "workflow_artifacts"


def _configure_logging():
    if logging.getLogger().handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


def _slugify(value: str, fallback: str = "short") -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "")).strip("_")
    return text[:80] or fallback


def _clean_topic(text: str) -> str:
    if not text:
        return ""
    candidate = text.strip().splitlines()[0].strip()
    candidate = re.sub(r"^[\-\*\d\.)\s]+", "", candidate)
    candidate = candidate.strip("\"'")
    candidate = re.sub(r"\s+", " ", candidate).strip()
    return candidate[:120].rstrip()


def _coerce_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _normalize_main_video_mode(value: str | None = None) -> str:
    normalized = str(value or os.getenv("SHORTS_MAIN_VIDEO_MODE", "yes-main")).strip().lower()
    normalized = normalized.replace("_", "-").replace(" ", "-")
    if normalized in {"no-main", "nomain", "attention-only", "no"}:
        return "no-main"
    return "yes-main"


def _auto_story_enabled() -> bool:
    return os.getenv("SHORTS_AUTO_STORY_ENABLED", "true").strip().lower() == "true"


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _bundle_file(bundle_dir: Path, name: str) -> Path:
    return bundle_dir / name


def _save_json(path: Path, payload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _load_json(path: Path, default=None):
    if not path.exists():
        if default is not None:
            return default
        raise FileNotFoundError(f"Missing JSON file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _relpath(path: Path, root: Path) -> str:
    return os.path.relpath(str(path), str(root))


def _stage_bundle_dir(bundle_dir: str | Path) -> Path:
    return _ensure_dir(Path(bundle_dir).resolve())


def _content_path(bundle_dir: Path) -> Path:
    return _bundle_file(bundle_dir, "content_package.json")


def _load_content(bundle_dir: Path) -> dict:
    return _load_json(_content_path(bundle_dir))


def _load_script_text(bundle_dir: Path, *names: str) -> str:
    for name in names:
        candidate = _bundle_file(bundle_dir, name)
        if candidate.exists():
            text = candidate.read_text(encoding="utf-8").strip()
            if text:
                return text
    return ""


def _sound_effect_files() -> list[str]:
    if not SOUND_EFFECTS_DIR.is_dir():
        return []
    return sorted([item.name for item in SOUND_EFFECTS_DIR.iterdir() if item.is_file() and item.suffix.lower() == ".mp3"])


def _matrix_payload(count: int, story_text: str = "") -> list[dict]:
    count = max(1, int(count))
    if story_text.strip():
        return [{"short_index": 1}]
    return [{"short_index": idx} for idx in range(1, count + 1)]


def prepare_matrix_stage(count: int, story_text: str = "") -> list[dict]:
    payload = _matrix_payload(count=count, story_text=story_text)
    logger.info("Prepared matrix for %s shorts", len(payload))
    return payload


def _resolve_requested_topic(short_index: int, topic_direction: str = "", story_text: str = "") -> str:
    cleaned_direction = _clean_topic(topic_direction)
    if story_text.strip():
        return cleaned_direction or "User Provided Story"
    if _auto_story_enabled():
        if cleaned_direction:
            return f"{cleaned_direction} #{short_index}"
        return f"Auto Story #{short_index}"
    return cleaned_direction or f"Auto Topic #{short_index}"


def prepare_content_stage(bundle_dir: Path, short_index: int, topic_direction: str = "", story_text: str = "") -> Path:
    from automation.content_generator import generate_comprehensive_content

    require_actions_runtime("prepare-content")
    bundle_dir = _stage_bundle_dir(bundle_dir)

    requested_topic = _resolve_requested_topic(short_index, topic_direction=topic_direction, story_text=story_text)
    logger.info("[%s] Preparing content with topic bias: %s", short_index, requested_topic or "(auto)")

    content_package = generate_comprehensive_content(
        requested_topic,
        source_story_text=story_text.strip() or None,
        paragraph_only=True,
        max_tokens=None,
    )

    paragraph = " ".join(str(content_package.get("paragraph") or content_package.get("script") or "").split()).strip()
    if not paragraph:
        raise RuntimeError("Content package did not return a paragraph narration body")

    title = str(content_package.get("title") or requested_topic or f"Short {short_index}").strip()
    content_payload = dict(content_package)
    content_payload.update(
        {
            "short_index": short_index,
            "requested_topic": requested_topic,
            "effective_topic": str(content_package.get("effective_topic") or requested_topic).strip(),
            "paragraph": paragraph,
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
    )

    raw_paragraph = " ".join(str(content_payload.get("raw_paragraph") or paragraph).split()).strip()
    direct_paragraph = " ".join(str(content_payload.get("direct_paragraph") or paragraph).split()).strip()
    final_paragraph = " ".join(str(content_payload.get("final_paragraph") or paragraph).split()).strip() or paragraph
    content_payload["raw_paragraph"] = raw_paragraph
    content_payload["direct_paragraph"] = direct_paragraph
    content_payload["final_paragraph"] = final_paragraph
    content_payload["paragraph"] = final_paragraph
    content_payload["script"] = final_paragraph
    content_payload["script_files"] = {
        "raw": "script_raw.txt",
        "direct": "script_direct.txt",
        "final": "script_final.txt",
        "canonical": "script.txt",
    }

    _save_json(_content_path(bundle_dir), content_payload)
    _bundle_file(bundle_dir, "script_raw.txt").write_text(raw_paragraph, encoding="utf-8")
    _bundle_file(bundle_dir, "script_direct.txt").write_text(direct_paragraph, encoding="utf-8")
    _bundle_file(bundle_dir, "script_final.txt").write_text(final_paragraph, encoding="utf-8")
    _bundle_file(bundle_dir, "script.txt").write_text(final_paragraph, encoding="utf-8")
    logger.info("[%s] Saved long-form content package: %s", short_index, title)
    return bundle_dir


def _fallback_sections_from_paragraph(paragraph: str, audio_duration: float) -> list[dict]:
    words = [token for token in str(paragraph or "").split() if token.strip()]
    if not words:
        return []

    chunk_size = max(10, min(24, int(round(len(words) / max(1, min(24, len(words) // 14 or 1))))))
    sections = []
    cursor = 0
    current_start = 0.0
    while cursor < len(words):
        chunk = words[cursor:cursor + chunk_size]
        cursor += chunk_size
        section_duration = max(0.8, audio_duration * (len(chunk) / max(1, len(words))))
        sections.append(
            {
                "id": f"fallback_section_{len(sections)}",
                "text": " ".join(chunk),
                "duration": section_duration,
                "voice_style": "male",
                "speaker": "boy",
                "start_time": current_start,
                "end_time": current_start + section_duration,
                "word_timestamps": [],
            }
        )
        current_start += section_duration
    return sections


def _build_transcript_sections(words: list[dict], paragraph: str, audio_duration: float) -> list[dict]:
    from helper.shorts_assets import build_script_sections_from_word_timestamps

    if words:
        approx_sections = max(12, min(72, int(round(len(words) / 16.0)) or 1))
        min_chunks = max(10, min(36, approx_sections))
        max_chunks = max(min_chunks, min(90, approx_sections + 12))
        sections = build_script_sections_from_word_timestamps(
            words,
            min_chunks=min_chunks,
            max_chunks=max_chunks,
            min_words_per_chunk=8,
            max_words_per_chunk=18,
        )
        if sections:
            return sections
    return _fallback_sections_from_paragraph(paragraph, audio_duration)


def _segment_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", str(text or "")))


def _build_sections_from_narration_segments(
    words: list[dict],
    segments: list[dict],
    segment_timeline: list[dict],
    paragraph: str,
    audio_duration: float,
) -> list[dict]:
    if not segments or not segment_timeline:
        return _build_transcript_sections(words, paragraph, audio_duration)

    if not words:
        fallback_sections = []
        for idx, item in enumerate(segment_timeline):
            text = " ".join(str(item.get("text") or "").split()).strip()
            if not text:
                continue
            start_time = float(item.get("start_time", 0.0) or 0.0)
            end_time = max(start_time, float(item.get("end_time", start_time) or start_time))
            fallback_sections.append(
                {
                    "id": f"segment_section_{idx}",
                    "text": text,
                    "duration": max(0.12, end_time - start_time),
                    "voice_style": "male",
                    "speaker": "boy",
                    "start_time": start_time,
                    "end_time": end_time,
                    "word_timestamps": [],
                    "pause_after_seconds": float(item.get("pause_after_seconds", 0.0) or 0.0),
                }
            )
        return fallback_sections or _fallback_sections_from_paragraph(paragraph, audio_duration)

    normalized_words = []
    for word in words:
        token = str(word.get("word", "") or "").strip()
        if not token:
            continue
        start = float(word.get("start", 0.0) or 0.0)
        end = max(start, float(word.get("end", start) or start))
        normalized_words.append({"word": token, "start": start, "end": end})

    if not normalized_words:
        return _fallback_sections_from_paragraph(paragraph, audio_duration)

    target_counts = [max(1, _segment_word_count(item.get("text", ""))) for item in segments]
    sections = []
    cursor = 0
    remaining_targets = sum(target_counts)

    for idx, item in enumerate(segment_timeline):
        if cursor >= len(normalized_words):
            break

        remaining_words = len(normalized_words) - cursor
        if idx == len(segment_timeline) - 1:
            take = remaining_words
        else:
            target = target_counts[idx] if idx < len(target_counts) else max(1, _segment_word_count(item.get("text", "")))
            proportional = int(round(remaining_words * (target / max(1, remaining_targets))))
            min_remaining = max(1, len(segment_timeline) - idx - 1)
            take = max(1, min(remaining_words - min_remaining, proportional if proportional > 0 else target))

        group = normalized_words[cursor:cursor + take]
        cursor += take
        remaining_targets -= target_counts[idx] if idx < len(target_counts) else 0
        if not group:
            continue

        start_time = float(group[0].get("start", 0.0) or 0.0)
        end_time = max(start_time, float(group[-1].get("end", start_time) or start_time))
        text = " ".join(str(word.get("word", "")).strip() for word in group).strip()
        if not text:
            text = " ".join(str(item.get("text") or "").split()).strip()
        sections.append(
            {
                "id": f"segment_section_{idx}",
                "text": text,
                "duration": max(0.12, end_time - start_time),
                "voice_style": "male",
                "speaker": "boy",
                "start_time": start_time,
                "end_time": end_time,
                "word_timestamps": group,
                "pause_after_seconds": float(item.get("pause_after_seconds", 0.0) or 0.0),
            }
        )

    if sections:
        logger.info("Built %s timed script sections from narration segments", len(sections))
        return sections
    return _build_transcript_sections(words, paragraph, audio_duration)


def generate_audio_stage(bundle_dir: Path, short_index: int) -> Path:
    from automation.content_generator import segment_paragraph_for_tts
    from helper.audio import AudioHelper
    from helper.shorts_assets import build_transcript_text, transcribe_audio_to_word_timestamps

    require_actions_runtime("generate-audio")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    content = _load_content(bundle_dir)
    paragraph = _load_script_text(bundle_dir, "script_final.txt", "script.txt")
    if not paragraph:
        paragraph = " ".join(str(content.get("paragraph") or content.get("script") or "").split()).strip()
    if not paragraph:
        raise RuntimeError("Missing paragraph text for audio generation")

    audio_helper = AudioHelper(str(bundle_dir / "audio"))
    narration_segments = segment_paragraph_for_tts(paragraph)
    if not narration_segments:
        narration_segments = [{"index": 0, "text": paragraph, "pause_after_seconds": 0.0}]

    segment_sections = []
    for idx, segment in enumerate(narration_segments):
        segment_sections.append(
            {
                "id": f"narration_segment_{idx:03d}",
                "text": str(segment.get("text") or "").strip(),
                "voice_style": "male",
                "pause_after_seconds": float(segment.get("pause_after_seconds", 0.0) or 0.0),
            }
        )

    segment_audio_data = audio_helper.process_audio_for_script(segment_sections, voice_style="male")
    segment_audio_data = audio_helper.ensure_audio_data_complete(segment_sections, segment_audio_data, voice_style="male")
    segment_audio_files = [item.get("path") if isinstance(item, dict) else None for item in segment_audio_data]

    narration_path = _bundle_file(bundle_dir, "master_narration.wav")
    pause_after_seconds = [float(segment.get("pause_after_seconds", 0.0) or 0.0) for segment in narration_segments]
    created_audio = audio_helper.combine_audio_clips_with_pauses(
        segment_audio_files,
        pause_after_seconds=pause_after_seconds,
        output_filename=str(narration_path),
    )
    if not created_audio or not os.path.exists(created_audio):
        logger.warning("[%s] Segmented TTS combine failed; falling back to single-pass narration", short_index)
        created_audio = audio_helper.create_tts_audio(paragraph, filename=str(narration_path))
        narration_segments = [{"index": 0, "text": paragraph, "pause_after_seconds": 0.0}]
        segment_audio_data = []
    if not created_audio or not os.path.exists(created_audio):
        raise RuntimeError("Failed to generate master narration audio")

    words = transcribe_audio_to_word_timestamps(created_audio)
    transcript_text = build_transcript_text(words) if words else paragraph
    try:
        from moviepy import AudioFileClip
        audio_duration_clip = AudioFileClip(created_audio)
        audio_duration = float(audio_duration_clip.duration or 0.0)
        audio_duration_clip.close()
    except Exception:
        audio_duration = 0.0

    segment_timeline = []
    cursor = 0.0
    for idx, segment in enumerate(narration_segments):
        item = segment_audio_data[idx] if idx < len(segment_audio_data) else None
        duration = 0.0
        audio_path = None
        if isinstance(item, dict):
            duration = float(item.get("duration", 0.0) or 0.0)
            audio_path = item.get("path")
        if duration <= 0.0 and idx == 0 and len(narration_segments) == 1:
            duration = audio_duration

        start_time = cursor
        end_time = start_time + max(0.0, duration)
        pause_after = float(segment.get("pause_after_seconds", 0.0) or 0.0)
        segment_timeline.append(
            {
                "index": idx,
                "text": str(segment.get("text") or "").strip(),
                "pause_after_seconds": pause_after,
                "audio_duration_seconds": duration,
                "start_time": start_time,
                "end_time": end_time,
                "audio_path": _relpath(Path(audio_path), bundle_dir) if audio_path else "",
            }
        )
        cursor = end_time + pause_after

    sections = _build_sections_from_narration_segments(
        words,
        narration_segments,
        segment_timeline,
        paragraph,
        audio_duration,
    )
    if not sections:
        raise RuntimeError("Failed to derive transcript sections from narration audio")

    _save_json(_bundle_file(bundle_dir, "narration_segments.json"), segment_timeline)
    _save_json(_bundle_file(bundle_dir, "transcript_words.json"), words)
    _save_json(_bundle_file(bundle_dir, "transcript_sections.json"), sections)
    _save_json(
        _bundle_file(bundle_dir, "narration_metadata.json"),
        {
            "short_index": short_index,
            "master_narration_path": "master_narration.wav",
            "transcript_text": transcript_text,
            "audio_duration_seconds": audio_duration,
            "segment_count": len(segment_timeline),
            "section_count": len(sections),
        },
    )
    logger.info(
        "[%s] Generated segmented master narration (%s segments) and %s transcript sections",
        short_index,
        len(segment_timeline),
        len(sections),
    )
    return bundle_dir


def plan_memes_stage(bundle_dir: Path, short_index: int) -> Path:
    from automation.content_generator import generate_paired_meme_plan
    from helper.image import fetch_best_image_for_prompt

    require_actions_runtime("plan-memes")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    content = _load_content(bundle_dir)
    script_sections = _load_json(_bundle_file(bundle_dir, "transcript_sections.json"), default=[])
    if not script_sections:
        _save_json(_bundle_file(bundle_dir, "meme_events.json"), [])
        return bundle_dir

    sound_effect_files = _sound_effect_files()
    if not sound_effect_files:
        logger.warning("[%s] No sound effects available; meme planning will be empty", short_index)
        _save_json(_bundle_file(bundle_dir, "meme_events.json"), [])
        return bundle_dir

    planned_events = generate_paired_meme_plan(
        script_sections=script_sections,
        sound_effect_files=sound_effect_files,
        topic=str(content.get("title") or content.get("effective_topic") or content.get("requested_topic") or ""),
        min_events=5,
        max_events=10,
    )

    assets_dir = _ensure_dir(bundle_dir / "meme_assets")
    materialized_events = []
    for idx, event in enumerate(planned_events):
        section_index = int(event.get("section_index", -1))
        if section_index < 0 or section_index >= len(script_sections):
            continue

        section = script_sections[section_index]
        query = str(event.get("query", "") or "").strip()
        if not query:
            continue

        image_path = fetch_best_image_for_prompt(
            query,
            style="meme reaction image",
            allow_ai_fallback=False,
            allow_generated_fallback=False,
        )
        if not image_path:
            fallback_query = " ".join(str(section.get("text", "")).split()[:6]).strip()
            if fallback_query and fallback_query != query:
                image_path = fetch_best_image_for_prompt(
                    fallback_query,
                    style="meme reaction image",
                    allow_ai_fallback=False,
                    allow_generated_fallback=False,
                )
        if not image_path:
            logger.warning("[%s] Meme image search failed for query '%s'", short_index, query)
            continue

        src = Path(image_path)
        dest = assets_dir / f"meme_{idx:03d}{src.suffix.lower() or '.jpg'}"
        shutil.copy2(src, dest)

        start_time = float(section.get("start_time", 0.0) or 0.0) + float(event.get("offset_seconds", 0.0) or 0.0)
        materialized_events.append(
            {
                "section_index": section_index,
                "query": query,
                "sound_effect_file": str(event.get("sound_effect_file") or "").strip(),
                "offset_seconds": float(event.get("offset_seconds", 0.0) or 0.0),
                "duration_seconds": float(event.get("duration_seconds", 2.0) or 2.0),
                "start_time": start_time,
                "image_path": _relpath(dest, bundle_dir),
            }
        )

    _save_json(_bundle_file(bundle_dir, "meme_events.json"), materialized_events)
    logger.info("[%s] Planned %s paired meme events", short_index, len(materialized_events))
    return bundle_dir


def _normalize_query_map(raw_queries: dict, sections: list[dict], fallback_query: str) -> list[str]:
    queries = []
    for idx, section in enumerate(sections):
        query = raw_queries.get(idx) if isinstance(raw_queries, dict) else None
        if query is None and isinstance(raw_queries, dict):
            query = raw_queries.get(str(idx))
        query = str(query or "").strip()
        if not query:
            query = fallback_query or "cinematic office scene"
        queries.append(query)
    return queries


def fetch_visual_assets_stage(bundle_dir: Path, short_index: int) -> Path:
    from automation.content_generator import generate_batch_video_queries
    from helper.fetch import fetch_videos_parallel
    from helper.image import fetch_best_image_for_prompt

    require_actions_runtime("fetch-visual-assets")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    content = _load_content(bundle_dir)
    sections = _load_json(_bundle_file(bundle_dir, "transcript_sections.json"), default=[])
    if not sections:
        raise RuntimeError("Transcript sections are required before fetching visuals")

    main_video_mode = _normalize_main_video_mode()
    if main_video_mode == "no-main":
        payload = {
            "main_video_mode": main_video_mode,
            "sections": [],
            "skipped_main_visual_fetch": True,
        }
        _save_json(_bundle_file(bundle_dir, "visual_manifest.json"), payload)
        logger.info("[%s] Skipped main visual fetch because main video mode is No Main", short_index)
        return bundle_dir

    texts = [str(section.get("text", "") or "").strip() for section in sections]
    overall_topic = str(content.get("effective_topic") or content.get("title") or content.get("requested_topic") or "")
    raw_queries = generate_batch_video_queries(texts, overall_topic=overall_topic)
    default_query = overall_topic or "cinematic office scene"
    queries = _normalize_query_map(raw_queries, sections, default_query)

    min_duration = max(5, int(max(float(section.get("duration", 1.0) or 1.0) for section in sections)) + 2)
    videos_by_query = fetch_videos_parallel(queries, count_per_query=3, min_duration=min_duration)

    assets_dir = _ensure_dir(bundle_dir / "visual_assets")
    manifest = []
    used_video_paths = set()
    last_visual_path = None
    last_visual_kind = None

    for idx, (section, query) in enumerate(zip(sections, queries)):
        duration = float(section.get("duration", 1.0) or 1.0)
        section_start = float(section.get("start_time", 0.0) or 0.0)
        candidates = videos_by_query.get(query, []) or []

        chosen_video = None
        for candidate in candidates:
            if candidate and candidate not in used_video_paths:
                chosen_video = candidate
                break

        asset_kind = None
        asset_path = None
        if chosen_video:
            used_video_paths.add(chosen_video)
            src = Path(chosen_video)
            asset_path = assets_dir / f"section_{idx:03d}{src.suffix.lower() or '.mp4'}"
            shutil.copy2(src, asset_path)
            asset_kind = "video"
        else:
            image_path = fetch_best_image_for_prompt(
                query,
                style="photorealistic",
                allow_ai_fallback=False,
                allow_generated_fallback=False,
            )
            if image_path:
                src = Path(image_path)
                asset_path = assets_dir / f"section_{idx:03d}{src.suffix.lower() or '.jpg'}"
                shutil.copy2(src, asset_path)
                asset_kind = "image"
            elif last_visual_path and last_visual_path.exists():
                asset_kind = last_visual_kind or "image"
                asset_path = assets_dir / f"section_{idx:03d}{last_visual_path.suffix.lower()}"
                shutil.copy2(last_visual_path, asset_path)
                logger.warning("[%s] Reused previous visual for section %s due to missing asset", short_index, idx)

        if not asset_path or not asset_kind:
            raise RuntimeError(f"Could not find or build a visual asset for section {idx} ({query})")

        last_visual_path = asset_path
        last_visual_kind = asset_kind
        manifest.append(
            {
                "section_index": idx,
                "query": query,
                "asset_kind": asset_kind,
                "path": _relpath(asset_path, bundle_dir),
                "duration": duration,
                "start_time": section_start,
                "end_time": float(section.get("end_time", section_start + duration) or (section_start + duration)),
                "text": texts[idx],
            }
        )

    _save_json(
        _bundle_file(bundle_dir, "visual_manifest.json"),
        {
            "main_video_mode": main_video_mode,
            "sections": manifest,
        },
    )
    logger.info("[%s] Prepared %s timed visual assets", short_index, len(manifest))
    return bundle_dir


def _build_visual_clips(bundle_dir: Path, sections: list[dict], manifest: list[dict]):
    from helper.image import create_image_clips_parallel
    from helper.process import process_background_clips_parallel

    visual_clips = [None] * len(sections)
    video_info = []
    video_indices = []
    image_indices = []
    image_paths = []
    image_durations = []

    for item in manifest:
        idx = int(item.get("section_index", -1))
        if idx < 0 or idx >= len(sections):
            continue
        path = bundle_dir / str(item.get("path", ""))
        if not path.exists():
            continue
        duration = float(item.get("duration", sections[idx].get("duration", 1.0)) or 1.0)
        if item.get("asset_kind") == "video":
            video_info.append({"path": str(path), "target_duration": duration, "section_idx": idx, "query": item.get("query")})
            video_indices.append(idx)
        else:
            image_indices.append(idx)
            image_paths.append(str(path))
            image_durations.append(duration)

    if video_info:
        processed_video_clips = process_background_clips_parallel(video_info=video_info, blur_background=False, edge_blur=False)
        for idx, clip in zip(video_indices, processed_video_clips):
            visual_clips[idx] = clip

    if image_paths:
        processed_image_clips = create_image_clips_parallel(image_paths=image_paths, durations=image_durations, texts=None, with_zoom=True)
        for idx, clip in zip(image_indices, processed_image_clips):
            visual_clips[idx] = clip

    for idx, clip in enumerate(visual_clips):
        if clip is None:
            raise RuntimeError(f"Visual clip {idx} could not be built")
        clip._section_idx = idx
        clip._debug_info = f"Section {idx}: {str(sections[idx].get('text', '') or '')[:48]}"

    return visual_clips


def _compose_attention_driven_section_clips(sections: list[dict], main_visual_clips=None):
    from moviepy import ColorClip, CompositeVideoClip
    from helper.shorts_assets import (
        build_brainrot_overlay_clip,
        pick_random_brainrot_start_time,
        pick_random_brainrot_video,
    )

    mode = _normalize_main_video_mode()
    attention_video_path = pick_random_brainrot_video()
    canvas_size = (1080, 1920)
    top_height_ratio = 1.0 if mode == "no-main" else max(
        0.15,
        min(0.5, float(os.getenv("SHORTS_BRAINROT_YES_MAIN_HEIGHT_RATIO", "0.3333"))),
    )

    if not attention_video_path:
        if mode == "no-main":
            raise RuntimeError("No attention-grab videos are available for No Main mode")
        logger.warning("No attention-grab video available; proceeding without top attention overlay")
        return main_visual_clips or []

    total_duration = sum(max(0.12, float(section.get("duration", 0.12) or 0.12)) for section in sections)
    attention_elapsed = pick_random_brainrot_start_time(
        attention_video_path,
        min_remaining_seconds=max(12.0, min(60.0, total_duration)),
    )
    composed_clips = []

    for idx, section in enumerate(sections):
        section_duration = max(0.12, float(section.get("duration", 0.12) or 0.12))
        attention_clip = build_brainrot_overlay_clip(
            attention_video_path,
            start_time=attention_elapsed,
            duration=section_duration,
            canvas_size=canvas_size,
            top_height_ratio=top_height_ratio,
        )
        attention_elapsed += section_duration

        if mode == "no-main":
            if attention_clip is None:
                raise RuntimeError(f"Failed to build attention-grab clip for section {idx}")
            composed = attention_clip.with_duration(section_duration)
        else:
            main_clip = main_visual_clips[idx] if main_visual_clips else None
            if main_clip is None:
                main_clip = ColorClip(size=canvas_size, color=(0, 0, 0)).with_duration(section_duration)
            else:
                main_clip = main_clip.with_duration(section_duration)

            layers = [main_clip]
            if attention_clip:
                layers.append(attention_clip)
            composed = CompositeVideoClip(layers, size=canvas_size).with_duration(section_duration)

        composed._section_idx = idx
        composed._debug_info = f"Section {idx}: {str(section.get('text', '') or '')[:48]}"
        composed_clips.append(composed)

    logger.info(
        "Built %s section clips using main video mode '%s'%s",
        len(composed_clips),
        mode,
        f" with attention overlay ratio {top_height_ratio:.2f}" if mode != "no-main" else " using attention-only background",
    )
    return composed_clips


def render_base_stage(bundle_dir: Path, short_index: int) -> Path:
    from automation.renderer import render_video
    from helper.shorts_assets import add_narration_and_background_music_to_video

    require_actions_runtime("render-base")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    sections = _load_json(_bundle_file(bundle_dir, "transcript_sections.json"), default=[])
    manifest_payload = _load_json(_bundle_file(bundle_dir, "visual_manifest.json"), default={})
    narration_path = _bundle_file(bundle_dir, "master_narration.wav")

    if isinstance(manifest_payload, list):
        main_video_mode = _normalize_main_video_mode()
        manifest = manifest_payload
    else:
        main_video_mode = _normalize_main_video_mode(manifest_payload.get("main_video_mode"))
        manifest = list(manifest_payload.get("sections", []) or [])

    if not sections:
        raise RuntimeError("render-base requires transcript sections")
    if main_video_mode != "no-main" and not manifest:
        raise RuntimeError("render-base requires a visual manifest when main video mode is Yes Main")

    render_dir = _ensure_dir(bundle_dir / "render")
    main_visual_clips = None
    if main_video_mode != "no-main":
        main_visual_clips = _build_visual_clips(bundle_dir, sections, manifest)

    visual_clips = _compose_attention_driven_section_clips(sections, main_visual_clips=main_visual_clips)
    base_visual_path = _bundle_file(bundle_dir, "base_visuals.mp4")

    render_video(
        clips=visual_clips,
        output_file=str(base_visual_path),
        fps=30,
        temp_dir=str(render_dir),
        preset="ultrafast",
        parallel=True,
        memory_per_worker_gb=1.0,
        crossfade_duration=0.0,
        options={
            "clean_temp": True,
            "crossfade_duration": 0.0,
        },
    )

    base_video_path = _bundle_file(bundle_dir, "base_video.mp4")
    shutil.copy2(base_visual_path, base_video_path)
    add_narration_and_background_music_to_video(str(base_video_path), str(narration_path), preset="ultrafast")
    logger.info("[%s] Rendered base video with hard cuts and master narration", short_index)
    return bundle_dir


def generate_thumbnail_stage(bundle_dir: Path, short_index: int) -> Path:
    from automation.thumbnail import ThumbnailGenerator

    require_actions_runtime("generate-thumbnail")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    content = _load_content(bundle_dir)

    thumbnail_generator = ThumbnailGenerator(output_dir=str(bundle_dir / "thumbnail_work"))
    output_path = _bundle_file(bundle_dir, "thumbnail.jpg")

    thumbnail_path = thumbnail_generator.generate_thumbnail(
        title=str(content.get("title") or content.get("requested_topic") or f"Short {short_index}"),
        script_sections=[],
        prompt=content.get("thumbnail_hf_prompt"),
        style="photorealistic",
        output_path=str(output_path),
    )

    if not thumbnail_path:
        query = str(content.get("thumbnail_unsplash_query") or content.get("effective_topic") or content.get("requested_topic") or "").strip()
        downloaded = thumbnail_generator.fetch_image_unsplash(query)
        if downloaded:
            thumbnail_path = thumbnail_generator.create_thumbnail_image_only(
                image_path=downloaded,
                output_path=str(output_path),
                anime_image_path=thumbnail_generator.fetch_anime_character_image(),
            )

    thumbnail_generator.cleanup()
    if not thumbnail_path or not output_path.exists():
        raise RuntimeError("Failed to generate thumbnail")
    logger.info("[%s] Generated thumbnail", short_index)
    return bundle_dir


def finalize_video_stage(bundle_dir: Path, short_index: int) -> Path:
    from helper.shorts_assets import (
        add_anime_greenscreen_overlay_to_video,
        add_dynamic_auto_captions_to_video,
        add_paired_meme_overlays_to_video,
    )

    require_actions_runtime("finalize-video")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    base_video_path = _bundle_file(bundle_dir, "base_video.mp4")
    final_video_path = _bundle_file(bundle_dir, "final_video.mp4")
    sections = _load_json(_bundle_file(bundle_dir, "transcript_sections.json"), default=[])
    meme_events = _load_json(_bundle_file(bundle_dir, "meme_events.json"), default=[])

    if not base_video_path.exists():
        raise FileNotFoundError(f"Missing base video: {base_video_path}")

    shutil.copy2(base_video_path, final_video_path)
    add_anime_greenscreen_overlay_to_video(str(final_video_path), preset="ultrafast")

    resolved_events = []
    for event in meme_events:
        event_copy = dict(event)
        image_relpath = event_copy.get("image_path")
        if image_relpath:
            event_copy["image_path"] = str((bundle_dir / str(image_relpath)).resolve())
        resolved_events.append(event_copy)
    add_paired_meme_overlays_to_video(str(final_video_path), resolved_events, preset="ultrafast")
    add_dynamic_auto_captions_to_video(
        str(final_video_path),
        script_sections=sections,
        font_size=int(os.getenv("AUTO_CAPTIONS_FONT_SIZE", "72")),
        position_ratio=0.5,
        preset="ultrafast",
    )

    if not final_video_path.exists():
        raise RuntimeError("Final video was not produced")
    logger.info("[%s] Finalized video with anime, memes, and captions", short_index)
    return bundle_dir


def collect_artifacts_stage(bundle_dir: Path, short_index: int, artifacts_dir: Path) -> Path:
    require_actions_runtime("collect-artifacts")
    bundle_dir = _stage_bundle_dir(bundle_dir)
    artifacts_dir = _ensure_dir(Path(artifacts_dir).resolve())
    content = _load_content(bundle_dir)
    final_video_path = _bundle_file(bundle_dir, "final_video.mp4")
    thumbnail_path = _bundle_file(bundle_dir, "thumbnail.jpg")
    visual_manifest_payload = _load_json(_bundle_file(bundle_dir, "visual_manifest.json"), default={})
    if isinstance(visual_manifest_payload, dict):
        main_video_mode = _normalize_main_video_mode(visual_manifest_payload.get("main_video_mode"))
    else:
        main_video_mode = _normalize_main_video_mode()

    if not final_video_path.exists():
        raise FileNotFoundError(f"Missing final video: {final_video_path}")
    if not thumbnail_path.exists():
        raise FileNotFoundError(f"Missing thumbnail: {thumbnail_path}")

    item_dir = _ensure_dir(artifacts_dir / f"{short_index:03d}_{_slugify(content.get('title') or content.get('requested_topic') or f'short_{short_index}')}")
    shutil.copy2(final_video_path, item_dir / "video.mp4")
    shutil.copy2(thumbnail_path, item_dir / "thumbnail.jpg")

    script_path = _bundle_file(bundle_dir, "script.txt")
    if script_path.exists():
        shutil.copy2(script_path, item_dir / "script.txt")
    for extra_script in ("script_raw.txt", "script_direct.txt", "script_final.txt", "narration_segments.json"):
        extra_path = _bundle_file(bundle_dir, extra_script)
        if extra_path.exists():
            shutil.copy2(extra_path, item_dir / extra_script)
    for extra_bundle_file in (
        "content_package.json",
        "master_narration.wav",
        "transcript_words.json",
        "transcript_sections.json",
        "meme_events.json",
        "visual_manifest.json",
        "base_video.mp4",
        "narration_metadata.json",
    ):
        extra_path = _bundle_file(bundle_dir, extra_bundle_file)
        if extra_path.exists():
            shutil.copy2(extra_path, item_dir / extra_bundle_file)

    metadata = {
        "index": short_index,
        "topic": str(content.get("effective_topic") or content.get("requested_topic") or ""),
        "requested_topic_bias": str(content.get("requested_topic") or ""),
        "title": str(content.get("title") or "").strip(),
        "description": str(content.get("description") or "").strip(),
        "generated_at": str(content.get("generated_at") or ""),
        "main_video_mode": main_video_mode,
        "video_source": str(final_video_path),
        "thumbnail_source": str(thumbnail_path),
    }
    metadata.update(content)
    _save_json(item_dir / "metadata.json", metadata)
    shutil.copy2(_content_path(bundle_dir), item_dir / "content_meta.json")
    logger.info("[%s] Collected workflow artifacts into %s", short_index, item_dir)
    return item_dir


def run_single_short_pipeline(bundle_dir: Path, short_index: int, topic_direction: str = "", story_text: str = "", artifacts_dir: Path | None = None) -> Path:
    bundle_dir = _stage_bundle_dir(bundle_dir)
    artifacts_dir = Path(artifacts_dir).resolve() if artifacts_dir else DEFAULT_ARTIFACTS_DIR.resolve()

    prepare_content_stage(bundle_dir, short_index, topic_direction=topic_direction, story_text=story_text)
    generate_audio_stage(bundle_dir, short_index)
    stage_jobs = [
        ("plan-memes", plan_memes_stage),
        ("fetch-visual-assets", fetch_visual_assets_stage),
        ("generate-thumbnail", generate_thumbnail_stage),
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(stage_jobs)) as executor:
        future_map = {
            executor.submit(stage_fn, bundle_dir, short_index): stage_name
            for stage_name, stage_fn in stage_jobs
        }
        for future in concurrent.futures.as_completed(future_map):
            stage_name = future_map[future]
            future.result()
            logger.info("[%s] Completed parallel stage: %s", short_index, stage_name)

    render_base_stage(bundle_dir, short_index)
    finalize_video_stage(bundle_dir, short_index)
    return collect_artifacts_stage(bundle_dir, short_index, artifacts_dir)


def run_batch_pipeline(count: int, topic_direction: str = "", story_text: str = "", artifacts_dir: str | Path = DEFAULT_ARTIFACTS_DIR) -> Path:
    require_actions_runtime("run-batch")
    payload = _matrix_payload(count, story_text=story_text)
    artifacts_root = Path(artifacts_dir).resolve()
    if artifacts_root.exists():
        shutil.rmtree(artifacts_root)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    bundle_root = DEFAULT_BUNDLES_DIR.resolve()
    if bundle_root.exists():
        shutil.rmtree(bundle_root)
    bundle_root.mkdir(parents=True, exist_ok=True)

    for item in payload:
        short_index = int(item["short_index"])
        bundle_dir = bundle_root / f"short_{short_index:03d}"
        run_single_short_pipeline(
            bundle_dir=bundle_dir,
            short_index=short_index,
            topic_direction=topic_direction,
            story_text=story_text,
            artifacts_dir=artifacts_root,
        )

    logger.info("Completed Actions pipeline batch. Artifacts directory: %s", artifacts_root)
    return artifacts_root


def run_actions_job(
    count: int,
    topic_direction: str = "",
    story_text: str = "",
    artifacts_dir: str | Path = DEFAULT_ARTIFACTS_DIR,
    upload_to_youtube: bool = False,
    youtube_privacy: str = "public",
) -> Path:
    artifacts_root = run_batch_pipeline(
        count=count,
        topic_direction=topic_direction,
        story_text=story_text,
        artifacts_dir=artifacts_dir,
    )

    if upload_to_youtube:
        from automation.upload_artifacts_to_youtube import upload_artifacts

        uploaded = upload_artifacts(Path(artifacts_root), privacy=youtube_privacy)
        if uploaded <= 0:
            raise RuntimeError("YouTube upload was requested but no videos were uploaded")

    return artifacts_root


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage-aware Actions pipeline for long-story YouTube video generation")
    subparsers = parser.add_subparsers(dest="command", required=True)

    matrix_parser = subparsers.add_parser("prepare-matrix", help="Emit the short-index matrix JSON for GitHub Actions")
    matrix_parser.add_argument("--count", type=int, required=True)
    matrix_parser.add_argument("--story-text", default="")

    for command in [
        "prepare-content",
        "generate-audio",
        "plan-memes",
        "fetch-visual-assets",
        "render-base",
        "generate-thumbnail",
        "finalize-video",
    ]:
        stage_parser = subparsers.add_parser(command)
        stage_parser.add_argument("--short-index", type=int, required=True)
        stage_parser.add_argument("--bundle-dir", required=True)
        if command == "prepare-content":
            stage_parser.add_argument("--topic-direction", default="")
            stage_parser.add_argument("--story-text", default="")

    collect_parser = subparsers.add_parser("collect-artifacts")
    collect_parser.add_argument("--short-index", type=int, required=True)
    collect_parser.add_argument("--bundle-dir", required=True)
    collect_parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))

    run_parser = subparsers.add_parser("run-batch", help="Run the full staged pipeline sequentially for development or compatibility")
    run_parser.add_argument("--count", type=int, default=1)
    run_parser.add_argument("--topic-direction", default="")
    run_parser.add_argument("--story-text", default="")
    run_parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))

    actions_job_parser = subparsers.add_parser("run-actions-job", help="Run the full Actions pipeline in one installed environment")
    actions_job_parser.add_argument("--count", type=int, default=1)
    actions_job_parser.add_argument("--topic-direction", default="")
    actions_job_parser.add_argument("--story-text", default="")
    actions_job_parser.add_argument("--artifacts-dir", default=str(DEFAULT_ARTIFACTS_DIR))
    actions_job_parser.add_argument("--upload-to-youtube", default="false")
    actions_job_parser.add_argument("--youtube-privacy", default="public", choices=["public", "private", "unlisted"])

    return parser


def main(argv=None) -> int:
    _configure_logging()
    args = _build_parser().parse_args(argv)

    if args.command == "prepare-matrix":
        print(json.dumps({"include": prepare_matrix_stage(args.count, story_text=args.story_text)}, ensure_ascii=False))
        return 0

    if args.command == "prepare-content":
        prepare_content_stage(Path(args.bundle_dir), args.short_index, topic_direction=args.topic_direction, story_text=args.story_text)
        return 0
    if args.command == "generate-audio":
        generate_audio_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "plan-memes":
        plan_memes_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "fetch-visual-assets":
        fetch_visual_assets_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "render-base":
        render_base_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "generate-thumbnail":
        generate_thumbnail_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "finalize-video":
        finalize_video_stage(Path(args.bundle_dir), args.short_index)
        return 0
    if args.command == "collect-artifacts":
        collect_artifacts_stage(Path(args.bundle_dir), args.short_index, Path(args.artifacts_dir))
        return 0
    if args.command == "run-batch":
        run_batch_pipeline(
            count=args.count,
            topic_direction=args.topic_direction,
            story_text=args.story_text,
            artifacts_dir=args.artifacts_dir,
        )
        return 0
    if args.command == "run-actions-job":
        run_actions_job(
            count=args.count,
            topic_direction=args.topic_direction,
            story_text=args.story_text,
            artifacts_dir=args.artifacts_dir,
            upload_to_youtube=_coerce_bool(args.upload_to_youtube),
            youtube_privacy=args.youtube_privacy,
        )
        return 0

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
