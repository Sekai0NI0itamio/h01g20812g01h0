import argparse
import json
import logging
from pathlib import Path

from automation.youtube_upload import get_authenticated_service, upload_video


logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Upload generated workflow artifacts to YouTube")
    parser.add_argument("--artifacts-dir", default="workflow_artifacts", help="Directory created by workflow_batch_runner")
    parser.add_argument("--privacy", default="public", choices=["public", "private", "unlisted"])
    return parser.parse_args()


def _build_title_description(item_dir: Path):
    metadata_path = item_dir / "metadata.json"
    topic = item_dir.name
    title = ""
    description = ""
    if metadata_path.exists():
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            topic = str(payload.get("topic") or topic)
            title = str(payload.get("title") or "").strip()
            description = str(payload.get("description") or "").strip()
        except Exception:
            pass

    if not title:
        title = str(topic).strip()[:100] or "Generated YouTube Short"
    if not description:
        description = f"{topic}\n\n#shorts #storytime #ai"
    return title, description


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    args = parse_args()

    artifacts_root = Path(args.artifacts_dir)
    if not artifacts_root.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {artifacts_root}")

    item_dirs = sorted([p for p in artifacts_root.iterdir() if p.is_dir()])
    if not item_dirs:
        logger.warning("No artifact item directories found in %s", artifacts_root)
        return 0

    youtube = get_authenticated_service()

    uploaded = 0
    for item in item_dirs:
        video_path = item / "video.mp4"
        thumbnail_path = item / "thumbnail.jpg"
        if not video_path.exists():
            logger.warning("Skipping %s: missing video.mp4", item)
            continue

        title, description = _build_title_description(item)
        try:
            upload_video(
                youtube=youtube,
                file_path=str(video_path),
                title=title,
                description=description,
                tags=["shorts", "storytime", "ai"],
                thumbnail_path=str(thumbnail_path) if thumbnail_path.exists() else None,
                privacy=args.privacy,
            )
            uploaded += 1
            logger.info("Uploaded %s", item.name)
        except Exception as exc:
            logger.error("Failed to upload %s: %s", item.name, exc)

    logger.info("Upload complete: %s/%s uploaded", uploaded, len(item_dirs))
    return 0 if uploaded > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
