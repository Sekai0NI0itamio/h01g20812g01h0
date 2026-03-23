import os # for environment variables and file paths
import sys # for stdout encoding
import argparse
import concurrent.futures
import logging # for logging events
import logging.handlers # Import handlers
import json
from pathlib import Path # for file paths and directory creation
from dotenv import load_dotenv # for loading environment variables
import datetime # for timestamp
from automation.content_generator import (
    generate_batch_video_queries,
    generate_batch_image_prompts,
    generate_comprehensive_content,
    generate_sound_effect_plan,
    generate_meme_insertion_plan,
)
from helper.audio import AudioHelper
from automation.shorts_maker_V import YTShortsCreator_V
from automation.shorts_maker_I import YTShortsCreator_I
from automation.thumbnail import ThumbnailGenerator
from helper.minor_helper import ensure_output_directory, parse_script_to_cards, cleanup_temp_directories
from helper.c05_key_provider import configure_provider_keys_from_c05
from helper.image import fetch_image_from_duckduckgo

load_dotenv()
YOUTUBE_TOPIC = os.getenv("YOUTUBE_TOPIC", "Artificial Intelligence")

# Configure logging with daily rotation
LOG_DIR = 'logs'  # Define log directory
LOG_FILENAME = os.path.join(LOG_DIR, 'youtube_shorts_daily.log') # Create full path
LOG_LEVEL = logging.INFO

# Add a debug flag to enable more verbose logging when needed
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
if DEBUG_MODE:
    LOG_LEVEL = logging.DEBUG
    print("DEBUG MODE ENABLED: More verbose logging activated")

# Ensure log directory exists
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

# First, disable any existing loggers to avoid duplicate outputs
logging.getLogger().handlers = []

# Configure a single root logger
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
# Suppress MoviePy logs to avoid excessive output
logging.getLogger('moviepy').setLevel(logging.ERROR)
logging.getLogger('imageio').setLevel(logging.ERROR)
logging.getLogger('imageio_ffmpeg').setLevel(logging.ERROR)
# Also suppress PIL warnings (which are common with MoviePy 2.1.2)
logging.getLogger('PIL').setLevel(logging.ERROR)

# Define log format - simpler format without emojis to avoid encoding issues
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')

# Add file handler with rotation
file_handler = logging.handlers.TimedRotatingFileHandler(
    LOG_FILENAME, when='midnight', interval=1, backupCount=7,
    encoding='utf-8'  # Force UTF-8 encoding for log files
)
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

# Add console handler
console_handler = logging.StreamHandler(sys.stdout)  # Use explicit stdout with proper encoding
console_handler.setFormatter(formatter)
root_logger.addHandler(console_handler)

# Use the root logger for this module
logger = logging.getLogger(__name__)


def _chunk_text_for_script_beats(text, min_lines=8, max_lines=16):
    """Create readable line beats from a paragraph when the model returns too few script lines."""
    raw_words = [w for w in str(text or "").replace("\n", " ").split(" ") if w.strip()]
    if not raw_words:
        return ""

    target_lines = max(min_lines, min(max_lines, max(1, len(raw_words) // 8)))
    words_per_line = max(4, min(12, max(1, len(raw_words) // target_lines)))

    lines = []
    for i in range(0, len(raw_words), words_per_line):
        lines.append(" ".join(raw_words[i:i + words_per_line]).strip())

    if len(lines) < min_lines and len(raw_words) >= min_lines * 4:
        # Retry with shorter chunks to increase parallelizable sections.
        words_per_line = max(4, words_per_line - 2)
        lines = []
        for i in range(0, len(raw_words), words_per_line):
            lines.append(" ".join(raw_words[i:i + words_per_line]).strip())

    return "\n".join([ln for ln in lines if ln]).strip()


def _generate_thumbnail_job(output_dir, safe_title, timestamp, title, script_cards, thumbnail_image_prompt, thumbnail_unsplash_query, style):
    """Background thumbnail generation task so render path is not blocked."""
    try:
        thumbnail_dir = os.path.join(output_dir, "thumbnails")
        os.makedirs(thumbnail_dir, exist_ok=True)
        thumbnail_generator = ThumbnailGenerator(output_dir=thumbnail_dir)

        safe_title_thumbnail = safe_title[:20]
        thumbnail_output_path = os.path.join(
            thumbnail_dir,
            f"thumbnail_{safe_title_thumbnail}_{timestamp}.jpg"
        )

        thumbnail_path = thumbnail_generator.generate_thumbnail(
            title=title,
            script_sections=script_cards,
            prompt=thumbnail_image_prompt,
            style=style,
            output_path=thumbnail_output_path
        )

        if not thumbnail_path:
            logger.info(f"Attempting with Unsplash query: {thumbnail_unsplash_query}")
            downloaded = thumbnail_generator.fetch_image_unsplash(thumbnail_unsplash_query)
            if downloaded:
                thumbnail_path = thumbnail_generator.create_thumbnail(
                    title=title,
                    image_path=downloaded,
                    output_path=thumbnail_output_path
                )

        thumbnail_generator.cleanup()
        return thumbnail_path
    except Exception as thumbnail_error:
        logger.error(f"Failed to generate thumbnail: {thumbnail_error}")
        return None

def get_creator_for_day():
    """Alternate between video and image creators based on day"""
    today = datetime.datetime.now()
    day_of_year = today.timetuple().tm_yday  # 1-366
    use_images = day_of_year % 2 == 0  # Even days use images, odd days use videos

    if use_images:
        logger.info(f"Day {day_of_year}: Using image-based creator (YTShortsCreator_I)")
        return YTShortsCreator_I()
    else:
        logger.info(f"Day {day_of_year}: Using video-based creator (YTShortsCreator_V)")
        return YTShortsCreator_V()

def resolve_topic(topic):
    if topic and topic.strip():
        return topic.strip()

    if YOUTUBE_TOPIC and YOUTUBE_TOPIC.strip():
        return YOUTUBE_TOPIC.strip()

    return "Artificial Intelligence"


def should_auto_upload(auto_upload=None):
    if auto_upload is not None:
        return auto_upload

    return os.getenv("ENABLE_YOUTUBE_UPLOAD", "false").lower() == "true"


def generate_youtube_short(topic, style="photorealistic", max_duration=25, creator_type=None, auto_upload=None, source_story_text=None):
    """
    Generate a YouTube Short.

    Args:
        topic (str): Topic for the YouTube Short
        style (str): Style for the content ("photorealistic", "digital art", etc.)
        max_duration (int): Maximum video duration in seconds
        creator_type: Optional creator instance to use (if None, will create a new one)

    Returns:
        tuple: (video_path, thumbnail_path)
    """
    try:
        output_dir = ensure_output_directory()

        # Generate unique filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        topic = resolve_topic(topic)
        logger.info(f"Generating comprehensive content for : {topic}")

        # Generate all content in a single API call
        content_package = generate_comprehensive_content(
            topic,
            max_tokens=1200,
            source_story_text=source_story_text,
        )

        # Extract content elements
        script = content_package["script"]
        paragraph = content_package.get("paragraph", "")
        title = content_package["title"].strip()
        description = content_package["description"]
        thumbnail_image_prompt = content_package["thumbnail_hf_prompt"]
        thumbnail_unsplash_query = content_package["thumbnail_unsplash_query"]

        logger.info("Content package generated successfully:")
        logger.info(f"Title: {title}")
        logger.info(f"Description length: {len(description)} characters")
        logger.info("Raw script generated successfully")

        # Create output filename using the title instead of raw topic
        safe_title = title.replace(' ', '_').replace(':', '').replace('?', '').replace('!', '')[:30]
        output_filename = f"yt_shorts_{safe_title}_{timestamp}.mp4"
        output_path = os.path.join(output_dir, output_filename)
        script_output_filename = f"script_{safe_title}_{timestamp}.txt"
        script_output_path = os.path.join(output_dir, script_output_filename)

        try:
            with open(script_output_path, "w", encoding="utf-8") as script_file:
                script_file.write(script)
            logger.info(f"Saved script to: {script_output_path}")
        except Exception as script_write_error:
            logger.warning(f"Failed to save script file: {script_write_error}")

        # Persist generated package metadata for downstream artifact upload jobs.
        try:
            metadata_output_filename = f"meta_{safe_title}_{timestamp}.json"
            metadata_output_path = os.path.join(output_dir, metadata_output_filename)
            with open(metadata_output_path, "w", encoding="utf-8") as meta_file:
                json.dump(
                    {
                        "topic": topic,
                        "title": title,
                        "description": description,
                        "thumbnail_hf_prompt": thumbnail_image_prompt,
                        "thumbnail_unsplash_query": thumbnail_unsplash_query,
                    },
                    meta_file,
                    ensure_ascii=False,
                    indent=2,
                )
            logger.info(f"Saved content metadata to: {metadata_output_path}")
        except Exception as metadata_write_error:
            logger.warning(f"Failed to save content metadata file: {metadata_write_error}")

        # Optional paragraph narration audio. Disabled by default to avoid duplicate TTS work.
        generate_paragraph_audio = os.getenv("ENABLE_PARAGRAPH_NARRATION_AUDIO", "false").lower() == "true"
        if paragraph and generate_paragraph_audio:
            try:
                audio_helper = AudioHelper()
                para_audio_path = os.path.join(output_dir, f"narration_paragraph_{safe_title}_{timestamp}.wav")
                created = audio_helper.create_tts_audio(paragraph, filename=para_audio_path)
                if created:
                    logger.info(f"Generated paragraph narration audio: {created}")
                else:
                    logger.warning("Failed to generate paragraph narration audio")
            except Exception as e:
                logger.exception("Error generating paragraph narration audio: %s", e)
        elif paragraph:
            logger.info("Skipping paragraph narration audio generation (ENABLE_PARAGRAPH_NARRATION_AUDIO=false)")

        # If script came back too short, derive beat lines from paragraph for better section parallelism.
        raw_lines = [ln.strip() for ln in str(script or "").splitlines() if ln.strip()]
        if len(raw_lines) < 4 and paragraph:
            rebuilt_script = _chunk_text_for_script_beats(paragraph, min_lines=8, max_lines=16)
            if rebuilt_script:
                script = rebuilt_script
                logger.info("Rebuilt short script into %s beat lines from paragraph", len([ln for ln in script.splitlines() if ln.strip()]))

        # Parse script into cards as before
        script_cards = parse_script_to_cards(script)
        logger.info(f"Script parsed into {len(script_cards)} sections")
        for i, card in enumerate(script_cards):
            logger.info(f"Section {i+1}: {card['text'][:30]}... (duration: {card['duration']}s)")

        # Log all sections to confirm proper order
        logger.info("=== FINAL SCRIPT CARDS ORDER ===")
        for i, card in enumerate(script_cards):
            logger.info(f"Section {i}: '{card['text'][:30]}...' (duration: {card['duration']}s)")
        logger.info("=== END SCRIPT CARDS ORDER ===")

        # Optional sound effects planning (6-10, spaced by at least one line)
        sound_effects_dir = os.path.join(os.path.dirname(__file__), "SoundEffects")
        sound_effect_files = []
        if os.path.isdir(sound_effects_dir):
            sound_effect_files = sorted(
                [f for f in os.listdir(sound_effects_dir) if f.lower().endswith(".mp3")]
            )

        # Build SFX and meme plans concurrently (both are independent AI planning calls)
        sfx_plan = []
        meme_plan = []
        if script_cards:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as planner_pool:
                sfx_future = None
                if sound_effect_files:
                    sfx_future = planner_pool.submit(
                        generate_sound_effect_plan,
                        script_lines=[card.get("text", "") for card in script_cards],
                        sound_effect_files=sound_effect_files,
                        topic=topic,
                    )

                meme_future = planner_pool.submit(
                    generate_meme_insertion_plan,
                    script_lines=[card.get("text", "") for card in script_cards],
                    topic=topic,
                    min_insertions=5,
                    max_insertions=11,
                )

                if sfx_future:
                    try:
                        sfx_plan = sfx_future.result()
                    except Exception as plan_err:
                        logger.warning("SFX plan generation failed: %s", plan_err)
                try:
                    meme_plan = meme_future.result()
                except Exception as plan_err:
                    logger.warning("Meme plan generation failed: %s", plan_err)

        if sound_effect_files and script_cards and sfx_plan:
            for item in sfx_plan:
                idx = item.get("line_index")
                if isinstance(idx, int) and 0 <= idx < len(script_cards):
                    script_cards[idx]["sfx_file"] = item.get("effect_file")
                    script_cards[idx]["sfx_offset"] = float(item.get("offset_seconds", 0.0))

            logger.info(
                "Sound effect plan applied: %s",
                [
                    f"line {e['line_index']} -> {e['effect_file']} @ {e['offset_seconds']:.2f}s"
                    for e in sfx_plan
                ],
            )
        else:
            logger.info("No SoundEffects/*.mp3 files found or no script cards; skipping sound effect planning")

        # Meme insertion planning: apply pre-generated plan and fetch images.
        if script_cards and meme_plan:
            for entry in meme_plan:
                idx = entry.get("line_index")
                if not isinstance(idx, int) or idx < 0 or idx >= len(script_cards):
                    continue

                meme_query = entry.get("query", "")
                meme_img_path = fetch_image_from_duckduckgo(meme_query)
                if not meme_img_path:
                    continue

                meme_item = {
                    "image_path": meme_img_path,
                    "offset_seconds": float(entry.get("offset_seconds", 0.0) or 0.0),
                    "duration_seconds": float(entry.get("duration_seconds", 1.0) or 1.0),
                    "query": meme_query,
                }

                script_cards[idx].setdefault("meme_overlays", []).append(meme_item)

            logger.info(
                "Meme insertion plan applied: %s",
                [
                    f"line {e['line_index']} -> '{e['query']}' @ {float(e.get('offset_seconds', 0.0)):.2f}s for {float(e.get('duration_seconds', 1.0)):.2f}s"
                    for e in meme_plan
                ],
            )

        # Kick off thumbnail generation now so it runs in parallel with video rendering.
        thumbnail_future = None
        thumbnail_path = None
        thumbnail_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        thumbnail_future = thumbnail_pool.submit(
            _generate_thumbnail_job,
            output_dir,
            safe_title,
            timestamp,
            title,
            script_cards,
            thumbnail_image_prompt,
            thumbnail_unsplash_query,
            style,
        )

        if creator_type is None:
            creator_type = get_creator_for_day()

        # Generate section-specific queries based on creator type
        card_texts = [card['text'] for card in script_cards]

        # We still need to generate section-specific queries for each section
        if isinstance(creator_type, YTShortsCreator_V):
            logger.info("Generating video search queries for each section using AI...")
            batch_query_results = generate_batch_video_queries(card_texts, overall_topic=topic)
        else:
            logger.info("Generating image search prompts for each section using AI...")
            batch_query_results = generate_batch_image_prompts(card_texts, overall_topic=topic)

        # Extract queries in order, using a fallback if needed
        default_query = f"abstract {topic}"

        section_queries = []
        for i in range(len(script_cards)):
            query = batch_query_results.get(i, default_query) # Get query by index, fallback to default
            if not query: # Ensure query is not empty string
                 query = default_query
                 logger.warning(f"Query for section {i} was empty, using fallback: '{default_query}'")
            section_queries.append(query)

        # Log all section queries at once to avoid duplication
        logger.info(f"Section queries: {', '.join([f'{i+1}: {q}' for i, q in enumerate(section_queries)])}")

        # Generate a fallback query for the whole script if needed
        fallback_query = section_queries[0] if section_queries else default_query

        # Video Creation - only log style for image-based creators
        if isinstance(creator_type, YTShortsCreator_I):
            logger.info(f"Creating YouTube Short with style: {style}")
        else:
            logger.info(f"Creating YouTube Short")

        video_path = creator_type.create_youtube_short(
            title=title,  # Use the generated title
            script_sections=script_cards,
            background_query=fallback_query,
            output_filename=output_path,
            style=style,
            voice_style="none",
            max_duration=max_duration,
            background_queries=section_queries,
            blur_background=False,
            edge_blur=False
        )

        # Resolve thumbnail result from concurrent generation task.
        try:
            if thumbnail_future:
                thumbnail_path = thumbnail_future.result()
            logger.info(f"Thumbnail generated at: {thumbnail_path}")
        except Exception as thumbnail_error:
            logger.error(f"Failed to resolve thumbnail generation task: {thumbnail_error}")
        finally:
            if thumbnail_pool:
                thumbnail_pool.shutdown(wait=False)

        # Optional: YouTube Upload
        if should_auto_upload(auto_upload):
            logger.info("Uploading to YouTube")
            try:
                from automation.youtube_upload import upload_video, get_authenticated_service

                youtube = get_authenticated_service()

                upload_video(
                    youtube,
                    video_path,
                    title,
                    description,  # Use the generated description
                    ["shorts", "ai", "technology"],  # Still include default tags
                    thumbnail_path=thumbnail_path
                )
            except Exception as upload_error:
                logger.error(f"YouTube upload failed, continuing with artifact output only: {upload_error}")

        return video_path, thumbnail_path

    except Exception as e:
        logger.error(f"Error generating YouTube Short: {e}")
        raise

def main(creator_type=None, topic=None, auto_upload=None):

    try:
        # Only get creator for day if no creator_type is provided
        if creator_type is None:
            creator_type = get_creator_for_day()

        # Set style based on creator type
        style = "photorealistic"
        # Only log style for image-based creators
        if isinstance(creator_type, YTShortsCreator_I):
            logger.info(f"Using style: {style}")

        try:
            # Set max_duration to 25 seconds as requested
            max_duration = 25  # Full duration for shorts

            result = generate_youtube_short(
                topic,
                style=style,
                max_duration=max_duration,
                creator_type=creator_type,
                auto_upload=auto_upload,
            )

            # Unpack the result (video_path, thumbnail_path)
            if isinstance(result, tuple) and len(result) == 2:
                video_path, thumbnail_path = result
                if not video_path:
                    raise RuntimeError("Video generation did not produce an output file.")
                logger.info(f"Process completed successfully!")
                logger.info(f"Video path: {video_path}")
                if thumbnail_path:
                    logger.info(f"Thumbnail path: {thumbnail_path}")
            else:
                # For backward compatibility
                video_path = result
                if not video_path:
                    raise RuntimeError("Video generation did not produce an output file.")
                logger.info(f"Process completed successfully! Video path: {video_path}")

            return video_path

        except Exception as e:
            logger.error(f"Error generating YouTube Short: {str(e)}")
            import traceback
            logger.error(f"Detailed error: {traceback.format_exc()}")
            raise
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        import traceback
        logger.error(f"Detailed error trace: {traceback.format_exc()}")
        return None
    finally:
        # Always run cleanup at the end regardless of success or failure
        try:
            logger.info("Running global cleanup of temporary files")
            cleanup_temp_directories(max_age_hours=24)
        except Exception as cleanup_error:
            logger.error(f"Error during final cleanup: {cleanup_error}")

def build_arg_parser():
    parser = argparse.ArgumentParser(description="Generate YouTube Shorts locally.")
    parser.add_argument(
        "creator",
        nargs="?",
        choices=["auto", "video", "image"],
        default="auto",
        help="Rendering mode. Default: auto.",
    )
    parser.add_argument(
        "--topic",
        help="Topic to generate. Defaults to YOUTUBE_TOPIC, then latest news if empty.",
    )
    parser.add_argument(
        "--run-mode",
        choices=["create-only", "auto-upload"],
        help="Choose whether to only create files or create and upload.",
    )
    parser.add_argument(
        "--use-c05-keys",
        dest="use_c05_keys",
        action="store_true",
        help="Fetch app provider keys from the local C05 provider.",
    )
    parser.add_argument(
        "--no-use-c05-keys",
        dest="use_c05_keys",
        action="store_false",
        help="Do not fetch provider keys from C05; use current environment values.",
    )
    parser.set_defaults(use_c05_keys=None)
    return parser


def prompt_for_run_mode():
    if not sys.stdin.isatty():
        return "create-only"

    while True:
        print("Select run mode:")
        print("1. create video only")
        print("2. create video and auto upload")
        choice = input("Enter 1 or 2 [1]: ").strip() or "1"
        if choice == "1":
            return "create-only"
        if choice == "2":
            return "auto-upload"
        print("Invalid selection.")


def creator_from_choice(choice):
    if choice == "video":
        logger.info("Manually selected video-based creator (YTShortsCreator_V)")
        return YTShortsCreator_V()
    if choice == "image":
        logger.info("Manually selected image-based creator (YTShortsCreator_I)")
        return YTShortsCreator_I()
    return None


def should_use_c05_keys(value):
    if value is not None:
        return value
    return os.getenv("USE_C05_LOCAL_KEYS", "true").lower() == "true"


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    run_mode = args.run_mode or prompt_for_run_mode()
    auto_upload = run_mode == "auto-upload"
    use_c05_keys = should_use_c05_keys(args.use_c05_keys)

    logger.info("Selected run mode: %s", run_mode)
    if auto_upload:
        logger.info("Auto-upload is enabled for this run.")
    else:
        logger.info("Create-only mode selected. YouTube authentication will be skipped.")

    if use_c05_keys:
        logger.info("Using C05 local provider for app API keys.")
        configure_provider_keys_from_c05()
    else:
        logger.info("Using provider keys from the current environment.")

    creator_type = creator_from_choice(args.creator)

    try:
        main(
            creator_type=creator_type,
            topic=args.topic,
            auto_upload=auto_upload,
        )
    finally:
        # One final cleanup to ensure everything is removed
        logger.info("Performing final cleanup of all temporary files")
        cleanup_temp_directories(max_age_hours=24, force_all=True)
