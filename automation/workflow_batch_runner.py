import argparse
import json
import logging
import os
import re
import shutil
import subprocess
from pathlib import Path

from automation.actions_pipeline import run_batch_pipeline
from helper.runtime import coerce_creator_mode, is_github_actions_runtime, require_actions_runtime

logger = logging.getLogger(__name__)


def _coerce_bool(value: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value}")


def _auto_story_enabled() -> bool:
    return os.getenv("SHORTS_AUTO_STORY_ENABLED", "true").strip().lower() == "true"


def _extract_completion_content(response: dict) -> str:
    if not isinstance(response, dict):
        return ""

    choices = response.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else None
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
        text = first.get("text") if isinstance(first, dict) else None
        if isinstance(text, str) and text.strip():
            return text.strip()

    for key in ("content", "output_text", "text", "response"):
        value = response.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def _clean_topic(text: str) -> str:
    if not text:
        return ""

    candidate = text.strip().splitlines()[0].strip()
    candidate = re.sub(r"^[\-\*\d\.)\s]+", "", candidate)
    candidate = candidate.strip('"\'')
    candidate = re.sub(r"\s+", " ", candidate).strip()

    if len(candidate) > 100:
        candidate = candidate[:100].rstrip()

    return candidate


def generate_auto_topic(direction: str, index: int, used_topics: set[str]) -> str:
    from automation.scitely_client import (
        ScitelyAPIError,
        create_chat_completion,
        get_default_chat_provider,
        get_preferred_chat_model,
    )

    direction_text = direction.strip() if direction else ""
    if direction_text:
        user_direction = f"General direction: {direction_text}"
    else:
        user_direction = "General direction: any engaging modern AI/tech or culturally relevant topic."

    used_list = "\n".join(f"- {topic}" for topic in sorted(used_topics)) or "- (none yet)"

    prompt = (
        "Generate exactly one YouTube Shorts topic title.\\n"
        "Requirements:\\n"
        "- Return only plain text, no quotes, no numbering.\\n"
        "- 4 to 12 words.\\n"
        "- Must be specific, curiosity-driven, and suitable for a 20-30 second short.\\n"
        f"- This is item #{index}.\\n"
        f"- {user_direction}\\n"
        "Avoid repeats or very similar phrasing to these already used topics:\\n"
        f"{used_list}"
    )

    provider = get_default_chat_provider()
    for attempt in range(3):
        try:
            response = create_chat_completion(
                messages=[{"role": "user", "content": prompt}],
                model=get_preferred_chat_model(provider),
                max_tokens=64,
                temperature=0.95,
                provider=provider,
            )
            topic = _clean_topic(_extract_completion_content(response))
            if topic and topic not in used_topics:
                return topic
            logger.warning("Topic generation returned empty or duplicate result for item %s on attempt %s/3", index, attempt + 1)
        except (ScitelyAPIError, ValueError, RuntimeError) as exc:
            logger.warning("Topic generation failed for item %s on attempt %s/3: %s", index, attempt + 1, exc)

    fallback_base = direction_text or "Artificial Intelligence"
    fallback_topic = f"{fallback_base} trend #{index}"
    dedupe_counter = 2
    while fallback_topic in used_topics:
        fallback_topic = f"{fallback_base} trend #{index}.{dedupe_counter}"
        dedupe_counter += 1
    return fallback_topic


def derive_script_path(video_path: Path) -> Path:
    name = video_path.name
    if name.startswith("yt_shorts_") and name.lower().endswith(".mp4"):
        suffix = name[len("yt_shorts_") : -4]
        return video_path.with_name(f"script_{suffix}.txt")
    return video_path.with_suffix(".txt")


def derive_meta_path(video_path: Path) -> Path:
    name = video_path.name
    if name.startswith("yt_shorts_") and name.lower().endswith(".mp4"):
        suffix = name[len("yt_shorts_") : -4]
        return video_path.with_name(f"meta_{suffix}.json")
    return video_path.with_suffix(".json")


def ensure_thumbnail(video_path: Path, thumbnail_path: str | None, out_dir: Path) -> Path:
    if thumbnail_path and Path(thumbnail_path).exists():
        return Path(thumbnail_path)

    generated = out_dir / "thumbnail_fallback.jpg"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(video_path),
        "-vf",
        "thumbnail,scale=1080:1920:force_original_aspect_ratio=increase,crop=1080:1920",
        "-frames:v",
        "1",
        str(generated),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return generated


def find_latest_generated_video(output_dir: Path) -> Path | None:
    candidates = sorted(output_dir.glob("yt_shorts_*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def copy_artifacts(index: int, topic: str, video_path: Path, script_path: Path, thumbnail_path: Path, artifacts_root: Path, meta_path: Path | None = None) -> None:
    generated_meta = {}
    resolved_topic = str(topic or "").strip()
    if meta_path and meta_path.exists():
        try:
            candidate_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(candidate_meta, dict):
                generated_meta = candidate_meta
                resolved_topic = str(
                    candidate_meta.get("effective_topic")
                    or candidate_meta.get("source_story_title")
                    or candidate_meta.get("title")
                    or resolved_topic
                ).strip()
        except Exception as exc:
            logger.warning("Failed to pre-read generated metadata from %s: %s", meta_path, exc)

    safe_topic = re.sub(r"[^A-Za-z0-9._-]+", "_", resolved_topic).strip("_")[:50] or f"topic_{index:03d}"
    item_dir = artifacts_root / f"{index:03d}_{safe_topic}"
    item_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(video_path, item_dir / "video.mp4")
    shutil.copy2(script_path, item_dir / "script.txt")
    shutil.copy2(thumbnail_path, item_dir / "thumbnail.jpg")

    metadata = {
        "index": index,
        "topic": resolved_topic or topic,
        "requested_topic_bias": topic,
        "video_source": str(video_path),
        "script_source": str(script_path),
        "thumbnail_source": str(thumbnail_path),
    }

    if generated_meta:
        metadata.update(generated_meta)
    elif meta_path and meta_path.exists():
        try:
            generated_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(generated_meta, dict):
                metadata.update(generated_meta)
        except Exception as exc:
            logger.warning("Failed to merge generated metadata from %s: %s", meta_path, exc)
    if meta_path and meta_path.exists():
        try:
            shutil.copy2(meta_path, item_dir / "content_meta.json")
        except Exception as exc:
            logger.warning("Failed to copy generated metadata from %s: %s", meta_path, exc)

    (item_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sequential batch runner for GitHub Actions")
    parser.add_argument("--count", type=int, default=10, help="Number of shorts to generate")
    parser.add_argument("--topic-direction", default="", help="Optional topic or story-direction bias for synthetic story generation")
    parser.add_argument("--upload-to-youtube", default="false", help="true/false")
    parser.add_argument("--creator", choices=["auto", "video", "image"], default=coerce_creator_mode(None))
    parser.add_argument("--artifacts-dir", default="workflow_artifacts", help="Folder to collect per-video artifacts")
    parser.add_argument("--story-text", default="", help="Optional full story text pasted by the user")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    require_actions_runtime("workflow batch runner")
    from automation.scitely_client import select_working_provider_for_run

    if args.count < 1:
        raise ValueError("--count must be at least 1")

    upload_to_youtube = _coerce_bool(args.upload_to_youtube)
    manual_story_text = (args.story_text or os.getenv("SHORTS_SOURCE_STORY", "")).strip()
    resolved_creator_mode = coerce_creator_mode(args.creator)

    logger.info("Starting batch run for %s shorts", args.count)
    logger.info("Topic direction: %s", args.topic_direction or "(auto)")
    logger.info("Creator mode: %s", "video")
    logger.info("YouTube upload: %s", upload_to_youtube)
    logger.info("Auto story mode: %s", _auto_story_enabled())
    if is_github_actions_runtime():
        logger.info("GitHub Actions runtime policy is active")
    if manual_story_text:
        logger.info("Manual story mode enabled from workflow input")

    selected_provider = select_working_provider_for_run()
    logger.info("AI provider locked for this run: %s", selected_provider)

    artifacts_root = run_batch_pipeline(
        count=args.count,
        topic_direction=args.topic_direction,
        story_text=manual_story_text,
        artifacts_dir=args.artifacts_dir,
    )

    logger.info("Batch run completed. Artifacts directory: %s", artifacts_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
