import logging
import os
import random
import time
import re
import json
import math
from pathlib import Path

from automation.scitely_client import (
    ScitelyAPIError,
    create_chat_completion,
    disable_scitely,
    get_default_chat_provider,
    get_preferred_chat_model,
    is_g4f_available,
    has_any_chat_provider,
)

# Configure logging - don't use basicConfig since main.py handles this
logger = logging.getLogger(__name__)

SCRIPT_TEMPLATE_PATH = Path(__file__).resolve().parent / "prompts" / "ai_shorts_script_template.txt"
DEFAULT_STORY_CHARACTER_FILE = Path(__file__).resolve().parent.parent / "story_character_involvement_pool.txt"
DEFAULT_STORY_THEME_FILE = Path(__file__).resolve().parent.parent / "story_theme_pool.txt"
DEFAULT_STORY_IDEA_FILE = Path(__file__).resolve().parent.parent / "story_idea_pool.txt"
DEFAULT_STORY_PERSPECTIVE_FILE = Path(__file__).resolve().parent.parent / "story_perspective_pool.txt"
DEFAULT_STORY_SOURCE_LINK_FILE = Path(__file__).resolve().parent.parent / "story_prompt_source_links.txt"
AUTO_STORY_ALLOWED_FLAIRS = ("Story-related",)
STORY_COMPONENT_ORDER = (
    "character_involvement",
    "theme",
    "story_idea",
    "perspective",
)
STORY_POOL_SPECS = {
    "character_involvement": {
        "env": "SHORTS_STORY_CHARACTER_FILE",
        "path": DEFAULT_STORY_CHARACTER_FILE,
        "min_count": 100,
    },
    "theme": {
        "env": "SHORTS_STORY_THEME_FILE",
        "path": DEFAULT_STORY_THEME_FILE,
        "min_count": 50,
    },
    "story_idea": {
        "env": "SHORTS_STORY_IDEA_FILE",
        "path": DEFAULT_STORY_IDEA_FILE,
        "min_count": 200,
    },
    "perspective": {
        "env": "SHORTS_STORY_PERSPECTIVE_FILE",
        "path": DEFAULT_STORY_PERSPECTIVE_FILE,
        "min_count": 100,
    },
}
AUTO_STORY_SYSTEM_PROMPT = (
    "You write original first-person personal stories for short-form video narration. "
    "The story should feel lived-in, messy, specific, and emotionally believable. "
    "Use concrete details like stores, receipts, texts, family habits, awkward silences, cars, kitchens, schools, "
    "and little social cues. Keep the voice natural and human, not polished literary fiction. "
    "Do not mention AI, prompts, Reddit, subreddits, or the writing process. "
    "Do not use bullet points or headings inside the story body. No sexual content. No graphic violence. No gore. "
    "Keep the tension interpersonal, emotional, awkward, romantic, family-based, or social."
)
AUTO_STORY_DISALLOWED_PATTERNS = (
    re.compile(r"\b(sex|sexual|orgasm|masturbat|naked|nude|penis|vagina|cum|boob|boobs|thong|vibrator|horny)\b", re.IGNORECASE),
    re.compile(r"\b(murder|kill(?:ed|ing)?|stab(?:bed|bing)?|gore|corpse|dismember|gun|shoot(?:ing)?|knife attack|strangle(?:d|ing)?|assault(?:ed|ing)?)\b", re.IGNORECASE),
)
_USED_AUTO_STORY_SIGNATURES = set()

REDDIT_REWRITE_SYSTEM_PROMPT = (
    "You are a skilled narrative writer. I will give you a raw personal story or source story. "
    "Your task is to rewrite it as a compelling first-person storytelling piece. Preserve all the key "
    "events and details, but enhance the emotional texture, inner thoughts, and sensory moments to make "
    "the reader feel like they are inside the narrator's head. Use a natural, conversational tone that "
    "matches the original voice. Keep the pacing tight-show, don't just tell. If the original has dialogue, "
    "keep it but make it feel vivid. The goal is to turn a raw anecdote into a short, engaging narrative "
    "that captures the emotional arc (confusion, embarrassment, realization, etc.) as it unfolded in real time. "
    "Use plain everyday words only and avoid literary or poetic descriptors. "
    "Minor grammar imperfections are acceptable if the flow feels natural. "
    "Keep it as one paragraph, but use short clear sentences with periods so it does not turn into one giant run-on. "
    "Do not compress the source into a teaser. Preserve nearly all meaningful beats and keep similar narrative density."
)

REDDIT_REWRITE_USER_TEMPLATE = (
    "Here is the story:\n\n{story}\n\n"
    "Rewrite this as a single, focused paragraph in first-person perspective.\n"
    "Keep all key events and details, enhance emotional texture and sensory moments, and use a natural, conversational tone.\n"
    "Use simple non-literary wording and keep it fluid even if grammar is not perfect.\n"
    "Keep it as one paragraph, but break the narration into short, clear sentences with periods.\n"
    "Avoid giant run-on sentences.\n"
    "Do not drastically shorten the story or reduce it to a summary.\n"
    "End with a call for comments, like: Comment what you think about this down in the comments.\n"
    "Return exactly one paragraph (no lists or line breaks)."
)


def _is_auto_story_enabled():
    return os.getenv("SHORTS_AUTO_STORY_ENABLED", "true").strip().lower() == "true"


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _get_structured_story_min_words():
    return max(300, _env_int("SHORTS_STRUCTURED_STORY_MIN_WORDS", 650))


def _get_synthetic_story_max_tokens():
    return max(3500, _env_int("SHORTS_SYNTHETIC_STORY_MAX_TOKENS", 7000))


def _get_content_package_max_tokens():
    return max(1500, _env_int("SHORTS_CONTENT_PACKAGE_MAX_TOKENS", 5000))


def _get_direct_rewrite_max_tokens():
    return max(1200, _env_int("SHORTS_DIRECT_REWRITE_MAX_TOKENS", 2600))


def _get_shorten_rewrite_max_tokens():
    return max(1000, _env_int("SHORTS_SHORTEN_REWRITE_MAX_TOKENS", 2200))


def _get_metadata_refresh_max_tokens():
    return max(800, _env_int("SHORTS_METADATA_REFRESH_MAX_TOKENS", 1800))


def _get_segmentation_max_tokens():
    return max(600, _env_int("SHORTS_TTS_SEGMENTATION_MAX_TOKENS", 1800))


def _get_default_meme_event_count():
    configured = _env_int("SHORTS_MEME_EVENT_TARGET_COUNT", 7)
    return max(5, min(10, configured))


def _resolve_story_component_path(env_name, default_path):
    raw_value = str(os.getenv(env_name, str(default_path))).strip()
    resolved = Path(raw_value)
    if not resolved.is_absolute():
        resolved = Path(__file__).resolve().parent.parent / resolved
    return resolved


def _load_story_line_pool(path):
    try:
        raw_text = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Failed to load story component file from %s: %s", path, exc)
        return []

    seen = set()
    lines = []
    for raw_line in raw_text.splitlines():
        line = re.sub(r"\s+", " ", str(raw_line or "").strip())
        if not line or line.startswith("#"):
            continue
        normalized = line.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        lines.append(line)
    return lines


def _load_story_ingredient_pools():
    pools = {"_source_files": {}}
    for key, spec in STORY_POOL_SPECS.items():
        path = _resolve_story_component_path(spec["env"], spec["path"])
        lines = _load_story_line_pool(path)
        if len(lines) < spec["min_count"]:
            logger.warning(
                "Story ingredient file %s only has %s usable entries; expected at least %s.",
                path,
                len(lines),
                spec["min_count"],
            )
            return None
        pools[key] = lines
        pools["_source_files"][key] = str(path)
    return pools


def _sample_story_ingredient_bundle(pools):
    selected = None
    for attempt in range(48):
        candidate = {key: random.choice(pools[key]) for key in STORY_COMPONENT_ORDER}
        signature = tuple(candidate[key].strip().lower() for key in STORY_COMPONENT_ORDER)
        if signature not in _USED_AUTO_STORY_SIGNATURES or attempt == 47:
            _USED_AUTO_STORY_SIGNATURES.add(signature)
            selected = candidate
            break
    return selected


def _sanitize_auto_story_title(title):
    cleaned = " ".join(str(title or "").replace("\n", " ").split()).strip().strip("\"'")
    if not cleaned:
        return ""
    return cleaned[:140].rstrip(" .,!?:;-")


def _story_contains_disallowed_content(text):
    return any(pattern.search(str(text or "")) for pattern in AUTO_STORY_DISALLOWED_PATTERNS)


def _build_story_generation_context(selected_bundle):
    return {
        "character_involvement": selected_bundle["character_involvement"],
        "theme": selected_bundle["theme"],
        "story_idea": selected_bundle["story_idea"],
        "perspective": selected_bundle["perspective"],
    }


def _validate_auto_story_payload(payload, selected_bundle):
    if not isinstance(payload, dict):
        raise ValueError("Auto story payload was not a JSON object.")

    required_fields = {"source_title", "story_body"}
    missing_fields = sorted(required_fields - set(payload.keys()))
    if missing_fields:
        raise ValueError(f"Auto story payload missing fields: {missing_fields}")

    title = _sanitize_auto_story_title(payload.get("source_title"))
    body = str(payload.get("story_body", "")).strip()
    if not title:
        raise ValueError("Auto story title was empty.")

    if not body:
        raise ValueError("Auto story body was empty.")
    if re.search(r"^\s*(?:[-*]|\d+\.)", body, re.MULTILINE):
        raise ValueError("Auto story body contained list formatting.")
    if not re.search(r"\b(i|me|my|i'm|i’d|i've)\b", body.lower()):
        raise ValueError("Auto story body did not read like first person narration.")
    if _story_contains_disallowed_content(body) or _story_contains_disallowed_content(title):
        raise ValueError("Auto story violated the non-sexual/non-graphic/non-violent content guardrails.")

    paragraphs = [chunk.strip() for chunk in re.split(r"\n\s*\n", body) if chunk.strip()]
    word_count = len(re.findall(r"\b[\w'-]+\b", body))
    minimum_word_count = _get_structured_story_min_words()
    if word_count < minimum_word_count:
        raise ValueError(f"Structured auto story word count {word_count} was below minimum {minimum_word_count}.")
    if not 4 <= len(paragraphs) <= 14:
        raise ValueError(f"Structured auto story used {len(paragraphs)} paragraphs instead of 4-14.")

    generation_context = _build_story_generation_context(selected_bundle)

    return {
        "source_title": title,
        "source_flair": "Story-related",
        "story_mode": "structured_first_person",
        "seed_terms_used": [generation_context[key] for key in STORY_COMPONENT_ORDER],
        "character_involvement": generation_context["character_involvement"],
        "theme": generation_context["theme"],
        "story_idea": generation_context["story_idea"],
        "perspective": generation_context["perspective"],
        "generation_context": generation_context,
        "story_body": body,
    }


def _build_auto_story_user_prompt(topic, selected_bundle):
    topic_bias = str(topic or "").strip() or "(none)"
    min_words = _get_structured_story_min_words()

    return f"""
Write one original first-person source story for a short-form drama/confession video.

Optional topic or direction bias:
{topic_bias}

Selected story ingredients:
- character involvement: {selected_bundle["character_involvement"]}
- theme: {selected_bundle["theme"]}
- story idea: {selected_bundle["story_idea"]}
- perspective: {selected_bundle["perspective"]}

Global rules:
- The narrator must clearly be the selected perspective.
- First-person only.
- Immediate hook in sentence one.
- The selected character involvement must matter to the plot.
- The selected theme must emotionally drive the story.
- The selected story idea must be the main engine of the plot.
- Concrete place, time, object, and dialogue details.
- Natural human imperfections allowed.
- No AI language, no meta commentary, no moral lesson.
- Do not mention Reddit, subreddits, or posts.
- No bullet lists or headings inside story_body.
- No sexual content.
- No graphic violence, combat, gore, or explicit injury.
- Use ordinary anchors like grocery stores, school hallways, apartments, family dinners, work break rooms, text messages, receipts, birthdays, pharmacies, and cars when relevant.
- At least {min_words} words.
- No hard maximum length. Preserve the full incident if it keeps escalating naturally.
- 4 to 14 short paragraphs.
- End on a strong realization, social sting, emotional turn, or unresolved family tension.

Return exactly one valid JSON object with this exact shape:
{{
  "source_title": "...",
  "source_flair": "Story-related",
  "story_mode": "structured_first_person",
  "character_involvement": "{selected_bundle["character_involvement"]}",
  "theme": "{selected_bundle["theme"]}",
  "story_idea": "{selected_bundle["story_idea"]}",
  "perspective": "{selected_bundle["perspective"]}",
  "story_body": "..."
}}

Additional formatting rules:
- source_title must feel like a raw curiosity-heavy personal-story title.
- Echo the exact same selected values for character_involvement, theme, story_idea, and perspective.
- story_body must be only the story prose with normal paragraph breaks.
"""


def _generate_synthetic_structured_story(topic, model, retries):
    if not _is_auto_story_enabled():
        logger.info("Auto story synthesis disabled by environment.")
        return None

    ingredient_pools = _load_story_ingredient_pools()
    if not ingredient_pools:
        return None

    selected_bundle = _sample_story_ingredient_bundle(ingredient_pools)
    if not selected_bundle:
        logger.warning("Could not sample enough unique story ingredients for auto story generation.")
        return None

    user_prompt = _build_auto_story_user_prompt(topic, selected_bundle)
    messages = [
        {"role": "system", "content": AUTO_STORY_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    for attempt in range(retries):
        try:
            response_text = _create_text_completion(
                messages=messages,
                model=model,
                max_tokens=_get_synthetic_story_max_tokens(),
                temperature=0.95,
            )
            payload = _parse_json_response(response_text)
            normalized = _validate_auto_story_payload(payload, selected_bundle)
            normalized["seed_source_file"] = ingredient_pools.get("_source_files")
            normalized["source_links_file"] = str(DEFAULT_STORY_SOURCE_LINK_FILE)
            logger.info(
                "Generated structured first-person story: perspective=%s theme=%s title=%s",
                normalized["perspective"],
                normalized["theme"],
                normalized["source_title"],
            )
            return normalized
        except Exception as exc:
            logger.warning(
                "Structured story generation failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return None


def _split_run_on_paragraph(text, max_words_per_sentence=16):
    """Break a long run-on paragraph into shorter sentence-like clauses while keeping one paragraph."""
    clauses = re.split(r"\s+(?=(?:and|but|then|because|when|while|after|before|finally|so)\b)", text)
    rebuilt_sentences = []
    current_parts = []
    current_words = 0

    for clause in clauses:
        clean_clause = str(clause or "").strip(" ,")
        if not clean_clause:
            continue

        clause_words = len(clean_clause.split())
        if current_parts and current_words + clause_words > max_words_per_sentence:
            rebuilt_sentences.append(" ".join(current_parts).strip(" ,"))
            current_parts = [clean_clause]
            current_words = clause_words
        else:
            current_parts.append(clean_clause)
            current_words += clause_words

    if current_parts:
        rebuilt_sentences.append(" ".join(current_parts).strip(" ,"))

    normalized_sentences = []
    for sentence in rebuilt_sentences:
        trimmed = sentence.strip(" .!?")
        if not trimmed:
            continue
        normalized_sentences.append(trimmed[0].upper() + trimmed[1:] if len(trimmed) > 1 else trimmed.upper())

    return ". ".join(normalized_sentences).strip()


def _normalize_paragraph_narration_style(paragraph_text):
    """Normalize paragraph style for narration constraints requested by user."""
    text = str(paragraph_text or "").strip().replace("\n", " ")
    if not text:
        return text

    text = re.sub(r"\s*[,;:]+\s*", ". ", text)
    text = re.sub(r"([.!?])(?=[A-Za-z])", r"\1 ", text)
    text = re.sub(r"\s+", " ", text).strip()

    segments = []
    raw_segments = [segment.strip(" ,") for segment in re.split(r"[.!?]+", text) if segment.strip(" ,")]
    for segment in raw_segments:
        if len(segment.split()) > 24:
            split_segment = _split_run_on_paragraph(segment)
            if split_segment:
                segments.extend([part.strip() for part in split_segment.split(".") if part.strip()])
        else:
            segments.append(segment)

    if not segments and text:
        segments = [text]

    text = ". ".join(
        part[0].upper() + part[1:] if len(part) > 1 else part.upper()
        for part in segments
        if part
    ).strip()

    cta = "Comment what you think about this down in the comments"
    lowered = text.lower()
    if "comment" not in lowered or "comments" not in lowered:
        if text and text[-1] not in ".!?":
            text += "."
        text += f" {cta}."

    return text


def _clean_plain_text_response(text):
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^```(?:json|text)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _derive_script_beats_from_paragraph(paragraph_text, min_lines=8, max_lines=16):
    text = " ".join(str(paragraph_text or "").split()).strip()
    if not text:
        return ""

    sentence_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]
    beat_lines = []
    for part in sentence_parts:
        cleaned = part.strip(" ")
        if not cleaned:
            continue
        words = cleaned.split()
        if len(words) <= 12:
            beat_lines.append(cleaned)
            continue

        cursor = 0
        while cursor < len(words):
            remaining = len(words) - cursor
            take = max(5, min(12, int(math.ceil(remaining / max(1, math.ceil(remaining / 10))))))
            chunk = " ".join(words[cursor:cursor + take]).strip()
            if chunk:
                beat_lines.append(chunk)
            cursor += take

    if len(beat_lines) < min_lines:
        words = text.split()
        beat_lines = []
        cursor = 0
        chunk_size = max(5, min(11, int(math.ceil(len(words) / max(1, min_lines)))))
        while cursor < len(words):
            beat_lines.append(" ".join(words[cursor:cursor + chunk_size]).strip())
            cursor += chunk_size

    if len(beat_lines) > max_lines:
        merged = []
        cursor = 0
        remaining = len(beat_lines)
        remaining_groups = max_lines
        while cursor < len(beat_lines):
            take = int(math.ceil(remaining / max(1, remaining_groups)))
            merged.append(" ".join(beat_lines[cursor:cursor + take]).strip())
            cursor += take
            remaining = len(beat_lines) - cursor
            remaining_groups -= 1
        beat_lines = merged

    return "\n".join(line for line in beat_lines if line).strip()


def _rewrite_paragraph_direct_speech(paragraph_text, model):
    messages = [
        {
            "role": "system",
            "content": (
                "You rewrite narration so it sounds like a real person talking directly. "
                "Remove poetic, literary, overwritten, and overly descriptive wording. "
                "Keep the same events, emotional beats, and order. "
                "Use plain spoken language, short clear sentences, and keep it as one paragraph. "
                "Return only the rewritten paragraph."
            ),
        },
        {
            "role": "user",
            "content": (
                "This script sounds too poetic and uses too many literary or descriptive words. "
                "Rewrite it in direct plain spoken text and show the full script again.\n\n"
                f"{paragraph_text}"
            ),
        },
    ]
    rewritten = _create_text_completion(
        messages=messages,
        model=model,
        max_tokens=_get_direct_rewrite_max_tokens(),
        temperature=0.45,
    )
    return _normalize_paragraph_narration_style(_clean_plain_text_response(rewritten))


def _shorten_paragraph_for_one_minute(paragraph_text, model):
    messages = [
        {
            "role": "system",
            "content": (
                "You are editing spoken narration for short-form video. "
                "Keep it direct and natural. Keep the same meaning and story arc, but tighten it so it reads aloud in about one minute. "
                "Do not turn it into bullet points. Keep it as one paragraph. Return only the rewritten paragraph."
            ),
        },
        {
            "role": "user",
            "content": (
                "Shorten this so it is around 1 minute when read out loud. "
                "Keep the full flow understandable, but make it tighter and more direct.\n\n"
                f"{paragraph_text}"
            ),
        },
    ]
    rewritten = _create_text_completion(
        messages=messages,
        model=model,
        max_tokens=_get_shorten_rewrite_max_tokens(),
        temperature=0.4,
    )
    return _normalize_paragraph_narration_style(_clean_plain_text_response(rewritten))


def _refresh_metadata_from_final_paragraph(paragraph_text, topic, model, retries=3):
    prompt = f"""
You are refreshing metadata for a narrated short-form video based on the final spoken paragraph.

Topic context: {topic}

Final narration paragraph:
{paragraph_text}

Return ONE valid JSON object with these exact fields:
1) title
2) description
3) thumbnail_hf_prompt
4) thumbnail_unsplash_query

Rules:
- title should be 40-60 characters and clickable.
- description should be 100-200 characters and include 3-4 hashtags.
- thumbnail_hf_prompt should be 20-30 words, image-only, with concrete scene details and no text/logos.
- thumbnail_unsplash_query should be 2-4 words.
- Base everything on the final paragraph, not a longer draft.
"""

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=_get_metadata_refresh_max_tokens(),
                temperature=0.5,
            )
            parsed = _parse_json_response(response_content)
            required = ["title", "description", "thumbnail_hf_prompt", "thumbnail_unsplash_query"]
            missing = [field for field in required if not str(parsed.get(field, "") or "").strip()]
            if missing:
                raise ValueError(f"Metadata refresh missing fields: {missing}")
            return {field: str(parsed.get(field, "") or "").strip() for field in required}
        except Exception as exc:
            logger.warning(
                "Metadata refresh failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return {}


def _postprocess_content_package(content_package, topic, model, retries=3, paragraph_only=False):
    package = dict(content_package or {})
    raw_paragraph = _normalize_paragraph_narration_style(
        package.get("paragraph") or package.get("script") or ""
    )
    if not raw_paragraph:
        return package

    direct_paragraph = raw_paragraph
    final_paragraph = raw_paragraph

    if _has_chat_ai_provider():
        try:
            direct_candidate = _rewrite_paragraph_direct_speech(raw_paragraph, model)
            if direct_candidate:
                direct_paragraph = direct_candidate
        except Exception as exc:
            logger.warning("Direct-speech rewrite failed; keeping raw paragraph: %s", exc)

        try:
            shortened_candidate = _shorten_paragraph_for_one_minute(direct_paragraph, model)
            if shortened_candidate:
                final_paragraph = shortened_candidate
        except Exception as exc:
            logger.warning("One-minute shorten rewrite failed; keeping direct paragraph: %s", exc)

        refreshed = _refresh_metadata_from_final_paragraph(final_paragraph, topic=topic, model=model, retries=retries)
        if refreshed:
            package.update(refreshed)

    package["raw_paragraph"] = raw_paragraph
    package["direct_paragraph"] = direct_paragraph
    package["final_paragraph"] = final_paragraph
    package["paragraph"] = final_paragraph
    if paragraph_only:
        package["script"] = final_paragraph
    else:
        package["script"] = _derive_script_beats_from_paragraph(final_paragraph)
    return package


def _load_script_template():
    try:
        return SCRIPT_TEMPLATE_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("Failed to load AI shorts script template from %s: %s", SCRIPT_TEMPLATE_PATH, exc)
        return ""


def _count_words(text):
    return len(re.findall(r"\b[\w'-]+\b", str(text or "")))


def _get_story_package_paragraph_bounds(source_word_count, paragraph_only=False):
    source_word_count = max(0, int(source_word_count or 0))
    if paragraph_only:
        target = max(220, int(source_word_count * 0.65))
        min_words = min(target, max(120, source_word_count))
        max_words = None
    else:
        target = max(160, int(source_word_count * 0.4))
        min_words = min(target, max(100, source_word_count))
        max_words = None
    return min_words, max_words


def _should_skip_story_rewrite(story_body):
    threshold_raw = str(os.getenv("SHORTS_STORY_REWRITE_SKIP_WORD_THRESHOLD", "240")).strip()
    try:
        threshold = max(0, int(threshold_raw))
    except ValueError:
        threshold = 240
    return _count_words(story_body) >= threshold


def _has_structured_ai_provider():
    return _has_chat_ai_provider()


def _has_chat_ai_provider():
    return has_any_chat_provider() or is_g4f_available()


def _get_completion_model(model=None):
    return model or get_preferred_chat_model()


def _get_active_completion_provider():
    return get_default_chat_provider()


def _parse_json_response(response_content):
    response_content = response_content.strip()

    try:
        return json.loads(response_content)
    except json.JSONDecodeError:
        pass

    fenced_match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", response_content, re.DOTALL)
    if fenced_match:
        return json.loads(fenced_match.group(1))

    object_match = re.search(r"(\{.*\})", response_content, re.DOTALL)
    if object_match:
        return json.loads(object_match.group(1))

    raise json.JSONDecodeError("No valid JSON object found in response", response_content, 0)


def _extract_completion_content(response):
    """
    Extract text content from different chat-completion payload shapes.
    Supports OpenAI-like payloads and common wrapped variants.
    """
    if not isinstance(response, dict):
        raise ScitelyAPIError(f"Unexpected completion response type: {type(response).__name__}")

    # Common OpenAI/compat shape
    choices = response.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else None
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
            if isinstance(content, list):
                flattened_parts = []
                for item in content:
                    if isinstance(item, str) and item.strip():
                        flattened_parts.append(item.strip())
                    elif isinstance(item, dict):
                        text = item.get("text")
                        if isinstance(text, str) and text.strip():
                            flattened_parts.append(text.strip())
                        else:
                            flattened_parts.append(json.dumps(item, ensure_ascii=False, sort_keys=True))
                    else:
                        flattened_parts.append(str(item))
                flattened = "\n".join(part for part in flattened_parts if part).strip()
                if flattened:
                    return flattened

        # Some providers return direct text on choice
        text = first.get("text") if isinstance(first, dict) else None
        if isinstance(text, str) and text.strip():
            return text.strip()

    # Alternate wrappers seen from proxy providers
    data = response.get("data")
    if isinstance(data, dict):
        nested_choices = data.get("choices")
        if isinstance(nested_choices, list) and nested_choices:
            first = nested_choices[0] if isinstance(nested_choices[0], dict) else {}
            message = first.get("message") if isinstance(first, dict) else None
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()

    # Last-chance plain-text fields
    for key in ("content", "output_text", "text", "response"):
        value = response.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    # Build a short debug preview for logs/errors
    preview = str(response)
    if len(preview) > 600:
        preview = preview[:600] + "..."
    raise ScitelyAPIError(f"Completion response missing usable content. Payload preview: {preview}")


def _create_json_completion(prompt, model, max_tokens, temperature):
    provider = _get_active_completion_provider()
    try:
        response = create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"},
            provider=provider,
        )
    except ScitelyAPIError as exc:
        if getattr(exc, "provider", "") == "scitely":
            disable_scitely(exc)
        if "response_format" not in str(exc).lower():
            raise

        logger.warning(
            "Provider %s rejected JSON mode for model %s, retrying without response_format.",
            provider,
            model,
        )
        response = create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            provider=provider,
        )

    return _extract_completion_content(response)


def _create_text_completion(messages, model, max_tokens, temperature):
    provider = _get_active_completion_provider()
    try:
        response = create_chat_completion(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            provider=provider,
        )
    except ScitelyAPIError as exc:
        if getattr(exc, "provider", "") == "scitely":
            disable_scitely(exc)
        logger.warning(
            "Text completion failed with provider %s, retrying with fallback g4f model selection.",
            provider,
        )
        response = create_chat_completion(
            messages=messages,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            provider="auto",
        )

    return _extract_completion_content(response)


def _rewrite_story_to_immersive_first_person(story_text, model):
    messages = [
        {"role": "system", "content": REDDIT_REWRITE_SYSTEM_PROMPT},
        {"role": "user", "content": REDDIT_REWRITE_USER_TEMPLATE.format(story=story_text)},
    ]
    rewritten = _create_text_completion(
        messages=messages,
        model=model,
        max_tokens=900,
        temperature=0.7,
    )
    return str(rewritten or "").strip()


def _build_story_content_package_prompt(
    topic,
    rewritten_story,
    source_title="",
    source_link="",
    paragraph_only=False,
    paragraph_word_bounds=None,
    generation_context=None,
):
    min_words, max_words = paragraph_word_bounds or (120, 220)
    paragraph_length_guidance = (
        f"- paragraph should be at least {min_words} words.\n"
        "- paragraph has no hard maximum length and should preserve the full incident arc."
    )
    context_block = ""
    if generation_context:
        context_block = f"""
    Selected story ingredients that must remain true:
    - narrator perspective: {generation_context.get("perspective", "")}
    - character involvement: {generation_context.get("character_involvement", "")}
    - emotional theme: {generation_context.get("theme", "")}
    - story idea: {generation_context.get("story_idea", "")}
    - Keep these selected ingredients aligned across the paragraph, title, description, and thumbnail direction.
    - Do not contradict the selected narrator perspective or relationship setup.
    """
    if paragraph_only:
        return f"""
    You are creating a complete YouTube Short content package from a rewritten first-person story.

    Topic context: {topic}
    Source title: {source_title}
    Source permalink: {source_link}
    {context_block}

    Rewritten immersive story:
    {rewritten_story}

    Return ONE valid JSON object with these exact fields:
    1) paragraph       -- a single first-person paragraph used as the full narration audio
    2) title
    3) description
    4) thumbnail_hf_prompt
    5) thumbnail_unsplash_query

    Requirements:
    - paragraph must be exactly one paragraph with no internal line breaks.
    - paragraph must be first-person, conversational, emotionally clear, and easy to narrate aloud.
    - paragraph must use plain, everyday words and avoid literary/descriptive flourish.
    - paragraph may include minor grammar imperfections if it sounds natural.
    - paragraph must stay as one paragraph, but it should use short clear sentences with periods.
    - paragraph must not feel like one giant run-on sentence.
    - paragraph must preserve the full incident arc instead of collapsing into a teaser summary.
    {paragraph_length_guidance}
    - paragraph should end with a comment CTA line such as "Comment what you think about this down in the comments."
    - do not include labels, bullet points, timestamps, or a separate line-by-line script.
    - title should be 40-60 characters and click-worthy.
    - description should be 100-200 characters and include 3-4 hashtags.
    - thumbnail_hf_prompt should be 20-30 words, focused on concrete scene elements, and describe image-only art with no text, captions, or logos.
    - thumbnail_unsplash_query should be 2-4 words.
    - do not output markdown fences or extra keys.
    """

    return f"""
    You are creating a complete YouTube Short content package from a rewritten first-person story.

    Topic context: {topic}
    Source title: {source_title}
    Source permalink: {source_link}
    {context_block}

    Rewritten immersive story:
    {rewritten_story}

    Return ONE valid JSON object with these exact fields:
    1) paragraph       -- a single first-person paragraph (the narrated text used for the narration audio)
    2) script          -- 8 to 16 short lines (newline-separated) derived directly from the paragraph for captions, visual beats and SFX timing
    3) title
    4) description
    5) thumbnail_hf_prompt
    6) thumbnail_unsplash_query

    Requirements:
    - paragraph must be exactly one paragraph (no internal line breaks) in first-person.
    - paragraph must use plain, everyday words and avoid literary/descriptive flourish.
    - paragraph may include minor grammar imperfections if it sounds natural.
    - paragraph must stay as one paragraph, but it should use short clear sentences with periods.
    - paragraph must not feel like one giant run-on sentence.
    - paragraph must preserve the full incident arc instead of collapsing into a teaser summary.
    {paragraph_length_guidance}
    - paragraph should end with a comment CTA line such as "Comment what you think about this down in the comments."
    - script must be derived from the paragraph (break the paragraph into concise caption-sized beats).
    - script should be 8 to 16 lines, one caption/beat per line, each 4-12 words.
    - script must preserve the emotional arc and refer directly to the content of the paragraph.
    - no labels like Hook/Intro/Outro and no call-to-action line.
    - title should be 40-60 characters and click-worthy.
    - description should be 100-200 characters and include 3-4 hashtags.
    - thumbnail_hf_prompt should be 20-30 words, focused on concrete scene elements, and describe image-only art with no text, captions, or logos.
    - thumbnail_unsplash_query should be 2-4 words.
    - do not output markdown fences or extra keys.
    """


def _build_content_package_from_story(topic, story, model, max_tokens, retries, paragraph_only=False):
    if isinstance(story, dict):
        story_body = str(story.get("body", "")).strip()
        source_title = str(story.get("title", "")).strip()
        source_link = str(story.get("permalink", "")).strip()
        generation_context = story.get("generation_context") if isinstance(story.get("generation_context"), dict) else None
    else:
        story_body = str(story or "").strip()
        source_title = ""
        source_link = ""
        generation_context = None

    if not story_body:
        return None

    source_word_count = _count_words(story_body)
    paragraph_word_bounds = _get_story_package_paragraph_bounds(source_word_count, paragraph_only=paragraph_only)

    for attempt in range(retries):
        try:
            if _should_skip_story_rewrite(story_body):
                rewritten_story = story_body
                logger.info(
                    "Skipping story rewrite for long input (%s words) to preserve narrative length.",
                    source_word_count,
                )
            else:
                rewritten_story = _rewrite_story_to_immersive_first_person(story_body, model)
            if not rewritten_story:
                raise ValueError("Rewritten story was empty")

            package_prompt = _build_story_content_package_prompt(
                topic=topic,
                rewritten_story=rewritten_story,
                source_title=source_title,
                source_link=source_link,
                paragraph_only=paragraph_only,
                paragraph_word_bounds=paragraph_word_bounds,
                generation_context=generation_context,
            )
            response_content = _create_json_completion(
                prompt=package_prompt,
                model=model,
                max_tokens=max_tokens,
                temperature=0.7,
            )

            content_package = _parse_json_response(response_content)
            required_fields = ["paragraph", "title", "description", "thumbnail_hf_prompt", "thumbnail_unsplash_query"]
            if not paragraph_only:
                required_fields.append("script")
            missing_fields = [field for field in required_fields if field not in content_package]
            if missing_fields:
                raise ValueError(f"Missing required fields in response: {missing_fields}")

            # Ensure paragraph is a single paragraph and strip unnecessary whitespace
            paragraph = _normalize_paragraph_narration_style(content_package.get("paragraph", ""))
            content_package["paragraph"] = paragraph
            paragraph_word_count = _count_words(paragraph)
            min_words, max_words = paragraph_word_bounds
            if paragraph_word_count < min_words:
                raise ValueError(f"Paragraph word count {paragraph_word_count} was below minimum {min_words}.")

            if paragraph_only:
                content_package["script"] = paragraph
            else:
                # Clean script lines but ensure they are derived from the paragraph
                content_package["script"] = filter_instructional_labels(content_package["script"]) if "script" in content_package else ""
            if source_link:
                content_package["source_story_permalink"] = source_link
            if source_title:
                content_package["source_story_title"] = source_title
            if generation_context:
                content_package["source_story_generation_context"] = generation_context

            content_package = _postprocess_content_package(
                content_package,
                topic=topic,
                model=model,
                retries=retries,
                paragraph_only=paragraph_only,
            )
            logger.info("Generated content package from source story successfully")
            return content_package
        except Exception as exc:
            logger.warning(
                "Story-based package generation failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return None

def filter_instructional_labels(script):
    """
    Filter out instructional labels from the script.

    Args:
        script (str): The raw script from the LLM

    Returns:
        str: Cleaned script with instructional labels removed
    """
    # Filter out common instructional labels
    script = re.sub(r'(?i)(opening shot|hook|attention(-| )grabber|intro|introduction)[:.\s]+', '', script)
    script = re.sub(r'(?i)(call to action|cta|outro|conclusion)[:.\s]+', '', script)
    script = re.sub(r'(?i)(key points?|main points?|talking points?)[:.\s]+', '', script)

    # Remove timestamp indicators
    script = re.sub(r'\(\d+-\d+ seconds?\)', '', script)
    script = re.sub(r'\(\d+ sec(ond)?s?\)', '', script)

    # Move hashtags to the end
    hashtags = re.findall(r'(#\w+)', script)
    script = re.sub(r'#\w+', '', script)

    # Remove lines that are primarily instructional
    lines = script.split('\n')
    filtered_lines = []

    for line in lines:
        line = line.strip()
        # Skip empty lines
        if not line:
            continue

        # Skip lines that are purely instructional
        if re.search(r'(?i)^(section|part|step|hook|cta|intro|outro)[0-9\s]*[:.-]', line):
            continue

        # Skip numbered list items that are purely instructional
        if re.search(r'(?i)^[0-9]+\.\s+(intro|outro|hook|call to action)', line):
            continue

        # Skip lines that are likely comments to the video creator
        if re.search(r'(?i)(remember to|make sure|tip:|note:)', line):
            continue

        filtered_lines.append(line)

    # Preserve line structure so downstream card parsing can honor line-based scripts.
    cleaned_lines = [re.sub(r'\s+', ' ', line).strip() for line in filtered_lines if line.strip()]
    filtered_script = '\n'.join(cleaned_lines).strip()

    # Append hashtags at the end if requested
    if hashtags:
        hashtag_text = ' '.join(hashtags)
        # Don't append hashtags in the actual script, they should be in the video description only
        # filtered_script += f"\n\n{hashtag_text}"

    return filtered_script


def _format_timed_sections_for_prompt(script_sections):
    formatted = []
    for idx, section in enumerate(script_sections or []):
        text = re.sub(r"\s+", " ", str(section.get("text", "") or "")).strip()
        if not text:
            continue
        start_time = float(section.get("start_time", 0.0) or 0.0)
        end_time = float(section.get("end_time", start_time + float(section.get("duration", 0.0) or 0.0)) or start_time)
        duration = max(0.12, float(section.get("duration", max(0.12, end_time - start_time)) or 0.12))
        formatted.append(
            f"--- Section {idx} | start={start_time:.2f}s | end={end_time:.2f}s | duration={duration:.2f}s ---\n{text}"
        )
    return "\n\n".join(formatted)

def generate_batch_video_queries(texts: list[str], overall_topic="technology", model=None, retries=3):
    """
    Generate concise video search queries for a batch of script texts using Scitely's DeepSeek models,
    returning results as a JSON object.
    Args:
        texts (list[str]): A list of text contents from script sections.
        overall_topic (str): The general topic of the video for context.
        model (str): The Scitely model to use.
        retries (int): Number of retry attempts.
    Returns:
        dict: A dictionary mapping the index (int) of the input text to the generated query string (str).
              Returns an empty dictionary on failure after retries.
    """
    if not _has_chat_ai_provider():
        raise ValueError("No supported AI provider is configured. Install g4f.")

    model = _get_completion_model(model)

    # Prepare the input text part of the prompt
    formatted_texts = ""
    for i, text in enumerate(texts):
        formatted_texts += f"--- Card {i} ---\n{text}\n\n"

    prompt = f"""
    You are an assistant that generates search queries for stock video websites (like Pexels, Pixabay).
    Based on the following text sections from a video script about '{overall_topic}', generate a concise (2-4 words) search query for EACH section. Focus on the key visual elements or concepts mentioned in each specific section.

    Input Script Sections:
    {formatted_texts}
    Instructions:
    1. Analyze each "Card [index]" section independently.
    2. For each card index, generate the most relevant 2-4 word search query.
    3. Return ONLY a single JSON object mapping the card index (as an integer key) to its corresponding query string (as a string value).

    Example Output Format:
    {{
      "0": "abstract technology background",
      "1": "glowing data lines",
      "2": "future city animation"
      ...
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=len(texts) * 20 + 50,
                temperature=0.5,
            )

            try:
                query_dict_str_keys = _parse_json_response(response_content)
                # Convert string keys back to integers
                query_dict = {int(k): v for k, v in query_dict_str_keys.items()}

                # Basic validation (check if all indices are present)
                if len(query_dict) == len(texts) and all(isinstance(k, int) and 0 <= k < len(texts) for k in query_dict):
                    logger.info(f"Successfully generated batch video queries for {len(texts)} sections.")
                    # Log individual queries for debugging
                    # for idx, q in query_dict.items():
                    #    logger.debug(f"  Query {idx}: {q}")
                    return query_dict
                else:
                    logger.warning(f"Generated JSON keys do not match expected indices. Response: {response_content}")

            except json.JSONDecodeError as json_e:
                logger.error(f"Failed to parse JSON response from AI provider: {json_e}. Response: {response_content}")
            except Exception as parse_e: # Catch other potential errors during dict conversion
                 logger.error(f"Error processing JSON response: {parse_e}. Response: {response_content}")

        except ScitelyAPIError as e:
            provider = getattr(e, "provider", "ai")
            logger.error(f"{provider.capitalize()} API error generating batch video queries (attempt {attempt + 1}/{retries}): {str(e)}")
            if provider == "scitely":
                disable_scitely(e)

        # If loop continues, it means an error occurred
        if attempt < retries - 1:
             logger.info(f"Retrying batch query generation ({attempt + 2}/{retries})...")
             time.sleep(2 ** attempt)
        else:
            logger.error(f"Failed to generate batch video queries after {retries} attempts.")

    # Fallback: Return empty dict if all retries fail
    return {}

def generate_batch_image_prompts(texts: list[str], overall_topic="technology", model=None, retries=3, timed_sections=None):
    """
    Generate detailed image generation prompts for a batch of script texts using Scitely's DeepSeek models,
    returning results as a JSON object.
    Args:
        texts (list[str]): A list of text contents from script sections.
        overall_topic (str): The general topic of the video for context.
        model (str): The Scitely model to use.
        retries (int): Number of retry attempts.
    Returns:
        dict: A dictionary mapping the index (int) of the input text to the generated image prompt (str).
              Returns an empty dictionary on failure after retries.
    """
    if not _has_chat_ai_provider():
        raise ValueError("No supported AI provider is configured. Install g4f.")

    model = _get_completion_model(model)

    # Prepare the input text part of the prompt
    if timed_sections:
        formatted_texts = _format_timed_sections_for_prompt(timed_sections)
    else:
        formatted_texts = ""
        for i, text in enumerate(texts):
            formatted_texts += f"--- Card {i} ---\n{text}\n\n"

    prompt = f"""
    You are an assistant that generates high-quality image prompts for AI image generation models like Stable Diffusion.
    Based on the following text sections from a video script about '{overall_topic}', create a detailed image prompt for EACH section.
    When timestamps are provided, use them as pacing context so each prompt matches the exact beat of the narration.

    Input Script Sections:
    {formatted_texts}

    Instructions:
    1. Analyze each "Card [index]" section independently.
    2. For each card, create a detailed image prompt (15-30 words) that:
       - Captures the main concept of that specific section
       - Includes clear visual elements and composition
       - Maintains a consistent style/theme across all prompts
       - DO NOT include any style descriptors (like digital art, photorealistic, etc.) as the style will be applied separately
       - Focus only on WHAT should be in the image, not HOW it should be rendered
    3. Return ONLY a single JSON object mapping the card index (as an integer key) to its corresponding image prompt (as a string value).

    Example Output Format:
    {{
      "0": "futuristic digital interface with flowing data, glowing blue elements, dark background, high detail, modern tech aesthetic",
      "1": "AI neural network visualization, interconnected nodes with energy flowing between them, depth of field, dramatic lighting",
      "2": "sleek robotic hand touching human hand, symbolic connection, soft backlighting, shallow depth of field"
      ...
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=len(texts) * 50 + 100,
                temperature=0.7,
            )

            try:
                prompt_dict_str_keys = _parse_json_response(response_content)
                # Convert string keys back to integers
                prompt_dict = {int(k): v for k, v in prompt_dict_str_keys.items()}

                # Basic validation (check if all indices are present)
                if len(prompt_dict) == len(texts) and all(isinstance(k, int) and 0 <= k < len(texts) for k in prompt_dict):
                    logger.info(f"Successfully generated batch image prompts for {len(texts)} sections.")
                    return prompt_dict
                else:
                    logger.warning(f"Generated JSON keys do not match expected indices. Response: {response_content}")

            except json.JSONDecodeError as json_e:
                logger.error(f"Failed to parse JSON response from AI provider: {json_e}. Response: {response_content}")
            except Exception as parse_e:  # Catch other potential errors during dict conversion
                 logger.error(f"Error processing JSON response: {parse_e}. Response: {response_content}")

        except ScitelyAPIError as e:
            provider = getattr(e, "provider", "ai")
            logger.error(f"{provider.capitalize()} API error generating batch image prompts (attempt {attempt + 1}/{retries}): {str(e)}")
            if provider == "scitely":
                disable_scitely(e)

        # If loop continues, it means an error occurred
        if attempt < retries - 1:
             logger.info(f"Retrying batch image prompt generation ({attempt + 2}/{retries})...")
             time.sleep(2 ** attempt)
        else:
            logger.error(f"Failed to generate batch image prompts after {retries} attempts.")

    # Fallback: Return empty dict if all retries fail
    return {}


def generate_sound_effect_plan(script_lines, sound_effect_files, topic="", model=None, retries=3):
    """
    Ask the LLM to place sound effects on script lines.

    Constraints:
    - Minimum effects: ceil(total_lines * 2/3) - 5
    - No line-position spacing limits
    - effect_file must be from provided sound_effect_files list
    """
    if not script_lines or not sound_effect_files:
        return []

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping sound effect planning")
        return []

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for sound effect planning", _get_active_completion_provider())

    total_lines = len(script_lines)
    sound_effect_reduction = max(0, int(os.getenv("SHORTS_SFX_REDUCTION", "5")))
    min_required = max(1, math.ceil((total_lines * 2) / 3) - 5 - sound_effect_reduction)
    max_effects = max(min_required, total_lines - sound_effect_reduction)

    formatted_lines = "\n".join(
        [f"{idx}: {line}" for idx, line in enumerate(script_lines)]
    )
    formatted_sfx = "\n".join([f"- {name}" for name in sound_effect_files])

    prompt = f"""
    You are creating a sound effect timing plan for a short video.
    Topic: {topic}

    Script lines:
    {formatted_lines}

    Available sound effect files (use exact file names only):
    {formatted_sfx}

    Rules:
    1) Choose at least {min_required} sound effects total (you may choose more, up to {max_effects}).
    2) Effects may be placed on any lines (adjacent lines are allowed).
    3) Use only listed file names.
    4) Choose moments that add impact without overusing effects.
    5) offset_seconds is the start time inside that line and should be between 0.0 and 4.0.

    Return ONLY valid JSON in this schema:
    {{
      "effects": [
        {{"line_index": 0, "effect_file": "filename.mp3", "offset_seconds": 0.3}}
      ]
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=300,
                temperature=0.4,
            )
            parsed = _parse_json_response(response_content)
            raw_effects = parsed.get("effects", []) if isinstance(parsed, dict) else []

            # Validate and normalize
            valid_names = set(sound_effect_files)
            normalized = []
            for item in raw_effects:
                if not isinstance(item, dict):
                    continue
                try:
                    idx = int(item.get("line_index"))
                except Exception:
                    continue
                if idx < 0 or idx >= len(script_lines):
                    continue

                effect_file = str(item.get("effect_file", "")).strip()
                if effect_file not in valid_names:
                    continue

                try:
                    offset = float(item.get("offset_seconds", 0.0))
                except Exception:
                    offset = 0.0
                offset = max(0.0, min(4.0, offset))

                normalized.append(
                    {
                        "line_index": idx,
                        "effect_file": effect_file,
                        "offset_seconds": offset,
                    }
                )

            # Enforce deterministic count and validity
            normalized.sort(key=lambda x: x["line_index"])
            final_plan = []
            used_lines = set()
            for entry in normalized:
                if len(final_plan) >= max_effects:
                    break
                idx = entry["line_index"]
                if idx in used_lines:
                    continue
                final_plan.append(entry)
                used_lines.add(idx)

            if len(final_plan) < min_required:
                used_lines = {x["line_index"] for x in final_plan}
                for idx in range(len(script_lines)):
                    if len(final_plan) >= min_required:
                        break
                    if idx in used_lines:
                        continue
                    fallback_name = sound_effect_files[len(final_plan) % len(sound_effect_files)]
                    final_plan.append(
                        {
                            "line_index": idx,
                            "effect_file": fallback_name,
                            "offset_seconds": 0.0,
                        }
                    )
                    used_lines.add(idx)

            final_plan.sort(key=lambda x: x["line_index"])

            logger.info("Generated sound effect plan with %s entries", len(final_plan))
            return final_plan
        except Exception as e:
            logger.warning(
                "Sound effect planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                e,
            )
            if isinstance(e, ScitelyAPIError) and getattr(e, "provider", "") == "scitely":
                disable_scitely(e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def generate_timed_sound_effect_plan(script_sections, sound_effect_files, topic="", model=None, retries=3):
    """
    Ask the LLM to place sound effects on transcript-timed sections.

    offset_seconds remains relative to the chosen section start so the existing
    renderers can apply it without additional timing changes.
    """
    if not script_sections or not sound_effect_files:
        return []

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping timed sound effect planning")
        return []

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for timed sound effect planning", _get_active_completion_provider())

    total_sections = len(script_sections)
    sound_effect_reduction = max(0, int(os.getenv("SHORTS_SFX_REDUCTION", "5")))
    min_required = max(1, math.ceil((total_sections * 2) / 3) - 5 - sound_effect_reduction)
    max_effects = max(min_required, total_sections - sound_effect_reduction)

    formatted_sections = _format_timed_sections_for_prompt(script_sections)
    formatted_sfx = "\n".join([f"- {name}" for name in sound_effect_files])

    prompt = f"""
    You are creating a funny sound effect timing plan for a short-form video.
    Topic: {topic}

    These transcript sections already come from Whisper word timestamps, so the timing is real.

    Transcript sections:
    {formatted_sections}

    Available sound effect files (use exact file names only):
    {formatted_sfx}

    Rules:
    1) Choose at least {min_required} sound effects total, and no more than {max_effects}.
    2) section_index must reference the numbered section above.
    3) effect_file must be one of the listed file names exactly.
    4) offset_seconds is relative to the chosen section start, not absolute video time.
    5) Use small offsets that fit naturally inside each section's duration.
    6) Pick moments that feel funny, punchy, awkward, dramatic, or chaotic.

    Return ONLY valid JSON in this shape:
    {{
      "effects": [
        {{"section_index": 0, "effect_file": "filename.mp3", "offset_seconds": 0.25}}
      ]
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=420,
                temperature=0.5,
            )
            parsed = _parse_json_response(response_content)
            raw_effects = parsed.get("effects", []) if isinstance(parsed, dict) else []

            valid_names = set(sound_effect_files)
            normalized = []
            used_sections = set()
            for item in raw_effects:
                if not isinstance(item, dict):
                    continue
                try:
                    idx = int(item.get("section_index"))
                except Exception:
                    continue
                if idx < 0 or idx >= len(script_sections) or idx in used_sections:
                    continue

                effect_file = str(item.get("effect_file", "")).strip()
                if effect_file not in valid_names:
                    continue

                try:
                    offset = float(item.get("offset_seconds", 0.0))
                except Exception:
                    offset = 0.0

                section_duration = max(0.12, float(script_sections[idx].get("duration", 0.12) or 0.12))
                offset = max(0.0, min(max(0.0, section_duration - 0.05), offset))

                normalized.append(
                    {
                        "line_index": idx,
                        "effect_file": effect_file,
                        "offset_seconds": offset,
                    }
                )
                used_sections.add(idx)

            normalized.sort(key=lambda x: x["line_index"])
            if len(normalized) < min_required:
                used_sections = {entry["line_index"] for entry in normalized}
                for idx in range(len(script_sections)):
                    if len(normalized) >= min_required:
                        break
                    if idx in used_sections:
                        continue
                    fallback_name = sound_effect_files[len(normalized) % len(sound_effect_files)]
                    normalized.append(
                        {
                            "line_index": idx,
                            "effect_file": fallback_name,
                            "offset_seconds": 0.0,
                        }
                    )
                    used_sections.add(idx)

            normalized.sort(key=lambda x: x["line_index"])
            logger.info("Generated timed sound effect plan with %s entries", len(normalized))
            return normalized[:max_effects]
        except Exception as exc:
            logger.warning(
                "Timed sound effect planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def _build_fallback_content_package(topic, paragraph_only=False):
    clean_topic = re.sub(r"\s+", " ", str(topic or "Artificial Intelligence")).strip()
    if not clean_topic:
        clean_topic = "Artificial Intelligence"

    topic_slug = re.sub(r"[^A-Za-z0-9]+", " ", clean_topic).strip() or "Artificial Intelligence"

    script_lines = [
        f"I thought about {topic_slug} all day.",
        "It kept coming back to my mind.",
        "I felt curious and a little nervous.",
        "Then I decided to pay attention.",
        "Everything started to feel more real.",
        "I noticed small details I missed before.",
        "The whole moment felt different.",
        "I kept watching and thinking harder.",
        "It was simple, but it stuck with me.",
        "I could not stop replaying it.",
        "It felt bigger than I expected.",
        "That was the part I remembered most.",
        "I wanted to see what happened next.",
        "It made the whole thing feel personal.",
        "I was more interested than before.",
        "The feeling stayed with me.",
        "I kept going back to it.",
        "It changed how I saw it.",
        "I did not forget that moment.",
        "It still feels close to me.",
    ]

    paragraph = _normalize_paragraph_narration_style(" ".join(script_lines))

    return {
        "paragraph": paragraph,
        "script": paragraph if paragraph_only else "\n".join(script_lines),
        "title": f"{clean_topic[:52]} Story I Could Not Ignore"[:60],
        "description": f"A personal short about {clean_topic}. #shorts #ai #story #viral",
        "thumbnail_hf_prompt": f"Close-up dramatic scene around {clean_topic}, expressive subject, strong contrast, emotional tension, clean composition, image only, no text",
        "thumbnail_unsplash_query": topic_slug[:32],
    }


def generate_meme_insertion_plan(
    script_lines,
    topic="",
    model=None,
    retries=3,
    min_insertions=None,
    max_insertions=15,
):
    """
    Generate a timed meme insertion plan.

    Output items:
    - line_index: int
    - query: str
    - offset_seconds: float
    - duration_seconds: float
    """
    if not script_lines:
        return []

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping meme insertion planning")
        return []

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for meme insertion planning", _get_active_completion_provider())

    meme_duration_min = float(os.getenv("SHORTS_MEME_DURATION_MIN", "2.0"))
    meme_duration_max = float(os.getenv("SHORTS_MEME_DURATION_MAX", "2.5"))
    max_effective = max(1, min(int(max_insertions), 15, len(script_lines)))
    formula_min = math.ceil((len(script_lines) * 2) / 3) - 4
    requested_min = formula_min if min_insertions is None else int(min_insertions)
    min_effective = max(1, min(requested_min, max_effective))

    formatted_lines = "\n".join([f"{idx}: {line}" for idx, line in enumerate(script_lines)])
    prompt = f"""
    You are planning meme image insertions for a short-form video.
    Topic: {topic}

    Script lines:
    {formatted_lines}

    Rules:
    1) Return between {min_effective} and {max_effective} insertions.
    2) Use distinct line_index values.
    3) query must be short (2-6 words) and searchable.
    4) offset_seconds should be 0.0 to 2.0.
    5) duration_seconds should be {meme_duration_min:.1f} to {meme_duration_max:.1f}.
    6) Keep choices relevant to each selected line.

    Return ONLY valid JSON in this exact shape:
    {{
      "insertions": [
        {{"line_index": 1, "query": "surprised pikachu meme", "offset_seconds": 0.3, "duration_seconds": 1.4}}
      ]
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=550,
                temperature=0.5,
            )
            parsed = _parse_json_response(response_content)
            raw_insertions = parsed.get("insertions", []) if isinstance(parsed, dict) else []

            normalized = []
            used_lines = set()
            for item in raw_insertions:
                if not isinstance(item, dict):
                    continue
                try:
                    line_index = int(item.get("line_index"))
                except Exception:
                    continue
                if line_index < 0 or line_index >= len(script_lines) or line_index in used_lines:
                    continue

                query = str(item.get("query", "")).strip()
                if not query:
                    continue

                try:
                    offset_seconds = float(item.get("offset_seconds", 0.0))
                except Exception:
                    offset_seconds = 0.0
                try:
                    duration_seconds = float(item.get("duration_seconds", 1.0))
                except Exception:
                    duration_seconds = 1.0

                normalized.append(
                    {
                        "line_index": line_index,
                        "query": query,
                        "offset_seconds": max(0.0, min(2.0, offset_seconds)),
                        "duration_seconds": max(meme_duration_min, min(meme_duration_max, duration_seconds)),
                    }
                )
                used_lines.add(line_index)

            # Keep deterministic order and enforce max.
            normalized.sort(key=lambda x: x["line_index"])
            normalized = normalized[:max_effective]

            # Ensure minimum insertion count by filling from remaining lines.
            if len(normalized) < min_effective:
                used = {x["line_index"] for x in normalized}
                for idx, line in enumerate(script_lines):
                    if len(normalized) >= min_effective:
                        break
                    if idx in used:
                        continue
                    fallback_query = " ".join(str(line).split()[:6]).strip() or "funny reaction meme"
                    normalized.append(
                        {
                            "line_index": idx,
                            "query": fallback_query,
                            "offset_seconds": 0.3,
                            "duration_seconds": min(meme_duration_max, max(meme_duration_min, 2.25)),
                        }
                    )
                    used.add(idx)

            normalized.sort(key=lambda x: x["line_index"])
            logger.info("Generated meme insertion plan with %s entries", len(normalized))
            return normalized
        except Exception as e:
            logger.warning(
                "Meme insertion planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                e,
            )
            if isinstance(e, ScitelyAPIError) and getattr(e, "provider", "") == "scitely":
                disable_scitely(e)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def generate_timed_meme_insertion_plan(
    script_sections,
    topic="",
    model=None,
    retries=3,
    min_insertions=None,
    max_insertions=15,
):
    """
    Generate a timed meme insertion plan from Whisper-timed transcript sections.
    """
    if not script_sections:
        return []

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping timed meme planning")
        return []

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for timed meme insertion planning", _get_active_completion_provider())

    meme_duration_min = float(os.getenv("SHORTS_MEME_DURATION_MIN", "2.0"))
    meme_duration_max = float(os.getenv("SHORTS_MEME_DURATION_MAX", "2.5"))
    max_effective = max(1, min(int(max_insertions), 15, len(script_sections)))
    formula_min = math.ceil((len(script_sections) * 2) / 3) - 4
    requested_min = formula_min if min_insertions is None else int(min_insertions)
    min_effective = max(1, min(requested_min, max_effective))

    formatted_sections = _format_timed_sections_for_prompt(script_sections)
    prompt = f"""
    You are planning meme image insertions for a short-form video.
    Topic: {topic}

    These transcript sections already come from Whisper timestamps, so use their real timing.

    Transcript sections:
    {formatted_sections}

    Rules:
    1) Return between {min_effective} and {max_effective} insertions.
    2) Use distinct section_index values.
    3) query must be short, searchable, and meme-friendly (2-6 words).
    4) offset_seconds is relative to the section start.
    5) duration_seconds should target {meme_duration_min:.1f} to {meme_duration_max:.1f} seconds when the section is long enough, and must fit inside that section.
    6) Prefer funny reaction images, awkward reaction faces, chaotic memes, or iconic visual jokes that match the beat.

    Return ONLY valid JSON in this exact shape:
    {{
      "insertions": [
        {{"section_index": 1, "query": "surprised pikachu meme", "offset_seconds": 0.3, "duration_seconds": 1.4}}
      ]
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=650,
                temperature=0.5,
            )
            parsed = _parse_json_response(response_content)
            raw_insertions = parsed.get("insertions", []) if isinstance(parsed, dict) else []

            normalized = []
            used_sections = set()
            for item in raw_insertions:
                if not isinstance(item, dict):
                    continue
                try:
                    section_index = int(item.get("section_index"))
                except Exception:
                    continue
                if section_index < 0 or section_index >= len(script_sections) or section_index in used_sections:
                    continue

                query = str(item.get("query", "")).strip()
                if not query:
                    continue

                section_duration = max(0.12, float(script_sections[section_index].get("duration", 0.12) or 0.12))
                try:
                    offset_seconds = float(item.get("offset_seconds", 0.0))
                except Exception:
                    offset_seconds = 0.0
                try:
                    duration_seconds = float(item.get("duration_seconds", 1.0))
                except Exception:
                    duration_seconds = 1.0

                offset_seconds = max(0.0, min(max(0.0, section_duration - 0.1), offset_seconds))
                remaining = max(0.25, section_duration - offset_seconds)
                target_duration = max(meme_duration_min, min(meme_duration_max, duration_seconds))
                duration_seconds = max(0.25, min(remaining, target_duration))

                normalized.append(
                    {
                        "line_index": section_index,
                        "query": query,
                        "offset_seconds": offset_seconds,
                        "duration_seconds": duration_seconds,
                    }
                )
                used_sections.add(section_index)

            normalized.sort(key=lambda x: x["line_index"])
            if len(normalized) < min_effective:
                used = {entry["line_index"] for entry in normalized}
                for idx, section in enumerate(script_sections):
                    if len(normalized) >= min_effective:
                        break
                    if idx in used:
                        continue
                    fallback_query = " ".join(str(section.get("text", "")).split()[:6]).strip() or "funny reaction meme"
                    fallback_duration = min(
                        max(0.25, float(section.get("duration", meme_duration_max) or meme_duration_max)),
                        max(meme_duration_min, meme_duration_max),
                    )
                    normalized.append(
                        {
                            "line_index": idx,
                            "query": fallback_query,
                            "offset_seconds": 0.2,
                            "duration_seconds": fallback_duration,
                        }
                    )
                    used.add(idx)

            normalized.sort(key=lambda x: x["line_index"])
            logger.info("Generated timed meme insertion plan with %s entries", len(normalized))
            return normalized[:max_effective]
        except Exception as exc:
            logger.warning(
                "Timed meme planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def generate_paired_meme_plan(
    script_sections,
    sound_effect_files,
    topic="",
    model=None,
    retries=3,
    min_events=5,
    max_events=10,
):
    """
    Generate a unified meme plan where each event includes both the meme image
    query and the paired sound effect file.
    """
    if not script_sections or not sound_effect_files:
        return []

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping paired meme planning")
        return []

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for paired meme planning", _get_active_completion_provider())

    min_effective = max(1, min(int(min_events), len(script_sections)))
    max_effective = max(min_effective, min(int(max_events), len(script_sections)))
    target_events = max(min_effective, min(max_effective, _get_default_meme_event_count()))

    meme_duration_min = float(os.getenv("SHORTS_MEME_DURATION_MIN", "2.0"))
    meme_duration_max = float(os.getenv("SHORTS_MEME_DURATION_MAX", "2.5"))
    formatted_sections = _format_timed_sections_for_prompt(script_sections)
    formatted_sfx = "\n".join([f"- {name}" for name in sorted(sound_effect_files)])

    prompt = f"""
    You are planning meme reaction beats for a narrated short-form video.
    Topic: {topic}

    These sections come from real Whisper timestamps, so timing must fit the narration naturally.

    Transcript sections:
    {formatted_sections}

    Available sound effect files (use exact file names only):
    {formatted_sfx}

    Rules:
    1) Return between {min_effective} and {max_effective} events. Aim for exactly {target_events}.
    2) Each event must include BOTH a meme-friendly searchable image query and one exact sound effect file.
    3) Use distinct section_index values.
    4) query must be 2-6 words and suitable for browser/image search.
    5) offset_seconds is relative to the section start.
    6) duration_seconds should be {meme_duration_min:.1f} to {meme_duration_max:.1f} seconds when the section is long enough, and must fit inside the section.
    7) Choose memes and sounds that match the meaning and tone of the selected section.

    Return ONLY valid JSON in this exact shape:
    {{
      "events": [
        {{
          "section_index": 1,
          "query": "awkward stare meme",
          "sound_effect_file": "Vine boom sound effect.mp3",
          "offset_seconds": 0.3,
          "duration_seconds": 2.2
        }}
      ]
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=900,
                temperature=0.45,
            )
            parsed = _parse_json_response(response_content)
            raw_events = parsed.get("events", []) if isinstance(parsed, dict) else []

            valid_names = set(sound_effect_files)
            normalized = []
            used_sections = set()
            for item in raw_events:
                if not isinstance(item, dict):
                    continue
                try:
                    section_index = int(item.get("section_index"))
                except Exception:
                    continue
                if section_index < 0 or section_index >= len(script_sections) or section_index in used_sections:
                    continue

                query = str(item.get("query", "")).strip()
                sound_effect_file = str(item.get("sound_effect_file", "")).strip()
                if not query or sound_effect_file not in valid_names:
                    continue

                section_duration = max(0.12, float(script_sections[section_index].get("duration", 0.12) or 0.12))
                try:
                    offset_seconds = float(item.get("offset_seconds", 0.0))
                except Exception:
                    offset_seconds = 0.0
                try:
                    duration_seconds = float(item.get("duration_seconds", 2.0))
                except Exception:
                    duration_seconds = 2.0

                offset_seconds = max(0.0, min(max(0.0, section_duration - 0.1), offset_seconds))
                remaining = max(0.25, section_duration - offset_seconds)
                duration_seconds = max(
                    0.25,
                    min(remaining, max(meme_duration_min, min(meme_duration_max, duration_seconds))),
                )

                normalized.append(
                    {
                        "section_index": section_index,
                        "query": query,
                        "sound_effect_file": sound_effect_file,
                        "offset_seconds": offset_seconds,
                        "duration_seconds": duration_seconds,
                    }
                )
                used_sections.add(section_index)

            normalized.sort(key=lambda x: x["section_index"])

            if len(normalized) < min_effective:
                used_sections = {entry["section_index"] for entry in normalized}
                for idx, section in enumerate(script_sections):
                    if len(normalized) >= min_effective:
                        break
                    if idx in used_sections:
                        continue

                    fallback_query = " ".join(str(section.get("text", "")).split()[:6]).strip() or "reaction meme"
                    fallback_duration = min(
                        max(0.25, float(section.get("duration", meme_duration_max) or meme_duration_max)),
                        meme_duration_max,
                    )
                    normalized.append(
                        {
                            "section_index": idx,
                            "query": fallback_query,
                            "sound_effect_file": sound_effect_files[len(normalized) % len(sound_effect_files)],
                            "offset_seconds": 0.2,
                            "duration_seconds": max(meme_duration_min, fallback_duration),
                        }
                    )
                    used_sections.add(idx)

            normalized.sort(key=lambda x: x["section_index"])
            logger.info("Generated paired meme plan with %s events", len(normalized[:max_effective]))
            return normalized[:max_effective]
        except Exception as exc:
            logger.warning(
                "Paired meme planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def generate_timed_word_color_plan(script_sections, topic="", model=None, retries=3):
    """
    Ask the LLM which transcript words deserve custom highlight colors.
    """
    if not script_sections:
        return {}

    if not _has_chat_ai_provider():
        logger.warning("No AI provider available; skipping timed word color planning")
        return {}

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for timed word color planning", _get_active_completion_provider())

    formatted_sections = _format_timed_sections_for_prompt(script_sections)
    prompt = f"""
    You are selecting colored highlight words for short-video captions.
    Topic: {topic}

    These sections already come from Whisper timestamps.

    Transcript sections:
    {formatted_sections}

    Rules:
    1) Pick up to 18 single words that actually appear in the transcript.
    2) Focus on emotionally important, funny, dramatic, awkward, or surprising words.
    3) Use only hex colors in #RRGGBB format.
    4) Return lower-case words only.
    5) Do not return phrases.

    Return ONLY valid JSON in this exact shape:
    {{
      "colors": {{
        "beautiful": "#FFD54F",
        "war": "#FF8A80"
      }}
    }}
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=320,
                temperature=0.4,
            )
            parsed = _parse_json_response(response_content)
            raw = parsed.get("colors", {}) if isinstance(parsed, dict) else {}
            result = {}
            transcript_words = {
                str(word).strip().lower()
                for section in script_sections
                for word in re.findall(r"[A-Za-z0-9']+", str(section.get("text", "") or ""))
                if str(word).strip()
            }
            for word, hex_color in raw.items():
                key = str(word or "").strip().lower()
                value = str(hex_color or "").strip()
                if key and key in transcript_words and re.match(r"^#[0-9A-Fa-f]{6}$", value):
                    result[key] = value
            logger.info("Generated timed caption color plan with %s entries", len(result))
            return result
        except Exception as exc:
            logger.warning(
                "Timed word color planning failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return {}


def _fallback_segment_paragraph_for_tts(paragraph_text):
    text = " ".join(str(paragraph_text or "").split()).strip()
    if not text:
        return []

    raw_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", text) if part.strip()]
    if not raw_parts:
        raw_parts = [text]

    segments = []
    for part in raw_parts:
        clauses = [part]
        word_count = len(part.split())
        if word_count > 20:
            clauses = [chunk.strip(" ,") for chunk in re.split(r"\s*(?:,|;|:|\s+\-\s+)\s*", part) if chunk.strip(" ,")]
        if len(clauses) == 1 and word_count > 20:
            clauses = [chunk.strip() for chunk in re.split(r"\s+(?=(?:and|but|so|because|then|when|while|after|before|finally)\b)", part) if chunk.strip()]

        current = []
        current_words = 0
        for clause in clauses:
            clause_words = len(clause.split())
            if current and current_words + clause_words > 16:
                joined = " ".join(current).strip()
                if joined:
                    segments.append(
                        {
                            "text": joined,
                            "pause_after_seconds": 0.26 if joined[-1:] in ".!?" else 0.18,
                        }
                    )
                current = [clause]
                current_words = clause_words
            else:
                current.append(clause)
                current_words += clause_words

        if current:
            joined = " ".join(current).strip()
            if joined:
                segments.append(
                    {
                        "text": joined,
                        "pause_after_seconds": 0.26 if joined[-1:] in ".!?" else 0.18,
                    }
                )

    normalized = []
    for idx, segment in enumerate(segments):
        segment_text = " ".join(str(segment.get("text") or "").split()).strip()
        if not segment_text:
            continue
        pause_after = float(segment.get("pause_after_seconds", 0.18) or 0.18)
        normalized.append(
            {
                "index": idx,
                "text": segment_text,
                "pause_after_seconds": max(0.0, min(0.45, pause_after)),
            }
        )

    if normalized:
        normalized[-1]["pause_after_seconds"] = 0.0
    return normalized


def segment_paragraph_for_tts(paragraph_text, model=None, retries=3):
    paragraph = _normalize_paragraph_narration_style(paragraph_text)
    if not paragraph:
        return []

    if not _has_chat_ai_provider():
        return _fallback_segment_paragraph_for_tts(paragraph)

    model = _get_completion_model(model)
    prompt = f"""
You are splitting spoken narration into natural breath-sized chunks for text-to-speech.

Narration paragraph:
{paragraph}

Rules:
- Split by meaning and breathing, not by every sentence.
- Each chunk should sound natural when spoken aloud in one breath.
- Keep all original meaning and wording.
- Each chunk should usually be around 5 to 22 words.
- pause_after_seconds should usually be between 0.10 and 0.35.
- The final chunk must use pause_after_seconds of 0.0.

Return ONLY valid JSON in this exact shape:
{{
  "segments": [
    {{"text": "first spoken chunk", "pause_after_seconds": 0.18}},
    {{"text": "next spoken chunk", "pause_after_seconds": 0.24}}
  ]
}}
"""

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=_get_segmentation_max_tokens(),
                temperature=0.35,
            )
            parsed = _parse_json_response(response_content)
            raw_segments = parsed.get("segments", []) if isinstance(parsed, dict) else []
            normalized = []
            for idx, item in enumerate(raw_segments):
                if not isinstance(item, dict):
                    continue
                segment_text = " ".join(str(item.get("text") or "").split()).strip()
                if not segment_text:
                    continue
                try:
                    pause_after = float(item.get("pause_after_seconds", 0.18) or 0.18)
                except Exception:
                    pause_after = 0.18
                normalized.append(
                    {
                        "index": idx,
                        "text": segment_text,
                        "pause_after_seconds": max(0.0, min(0.45, pause_after)),
                    }
                )

            if normalized:
                normalized[-1]["pause_after_seconds"] = 0.0
                original_words = _count_words(paragraph)
                segmented_words = sum(_count_words(item.get("text")) for item in normalized)
                if segmented_words >= max(8, int(original_words * 0.7)):
                    return normalized
                raise ValueError(f"Segmented narration kept too few words ({segmented_words}/{original_words})")
        except Exception as exc:
            logger.warning(
                "Narration segmentation failed (attempt %s/%s): %s",
                attempt + 1,
                retries,
                exc,
            )
            if isinstance(exc, ScitelyAPIError) and getattr(exc, "provider", "") == "scitely":
                disable_scitely(exc)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return _fallback_segment_paragraph_for_tts(paragraph)


def generate_comprehensive_content(topic, model=None, max_tokens=None, retries=3, source_story_text=None, paragraph_only=False):
    """
    Generate a comprehensive content package for a YouTube Short in a single API call.

    Args:
        topic (str): The topic to create content for
        model (str): The Scitely model to use
        max_tokens (int): Maximum tokens for the response
        retries (int): Number of retry attempts

    Returns:
        dict: A dictionary containing all generated content elements:
            - script: The full script text
            - title: An engaging title for the short
            - description: Full description with hashtags
            - thumbnail_hf_prompt: Detailed prompt for downstream image selection
            - thumbnail_unsplash_query: Simple query for Unsplash image search
    """
    if not _has_chat_ai_provider():
        raise ValueError("No supported AI provider is configured. Install g4f.")

    model = _get_completion_model(model)
    logger.info("Using AI provider %s for comprehensive content generation", _get_active_completion_provider())
    if max_tokens is None:
        max_tokens = _get_content_package_max_tokens()

    user_story_text = str(source_story_text or "").strip()
    if user_story_text:
        logger.info("Using user-provided story input for script generation")
        package = _build_content_package_from_story(
            topic=topic,
            story={"body": user_story_text},
            model=model,
            max_tokens=max_tokens,
            retries=retries,
            paragraph_only=paragraph_only,
        )
        if package:
            package["effective_topic"] = str(package.get("source_story_title") or topic or package.get("title") or "").strip()
            return package
        logger.warning("User-provided story generation failed; falling back to topic-based generation")

    synthetic_story = _generate_synthetic_structured_story(topic=topic, model=model, retries=retries)
    if synthetic_story:
        logger.info("Using synthetic first-person source story for script generation")
        package = _build_content_package_from_story(
            topic=synthetic_story.get("source_title") or topic,
            story={
                "body": synthetic_story.get("story_body", ""),
                "title": synthetic_story.get("source_title", ""),
                "generation_context": synthetic_story.get("generation_context"),
            },
            model=model,
            max_tokens=max_tokens,
            retries=retries,
            paragraph_only=paragraph_only,
        )
        if package:
            package["source_story_flair"] = synthetic_story.get("source_flair")
            package["source_story_mode"] = synthetic_story.get("story_mode")
            package["source_story_seed_terms"] = synthetic_story.get("seed_terms_used", [])
            package["source_story_body_raw"] = synthetic_story.get("story_body", "")
            package["source_story_generated"] = True
            package["source_story_seed_file"] = synthetic_story.get("seed_source_file")
            package["source_story_character_involvement"] = synthetic_story.get("character_involvement")
            package["source_story_theme"] = synthetic_story.get("theme")
            package["source_story_idea"] = synthetic_story.get("story_idea")
            package["source_story_perspective"] = synthetic_story.get("perspective")
            package["source_story_generation_context"] = synthetic_story.get("generation_context")
            package["source_story_source_links_file"] = synthetic_story.get("source_links_file")
            package["effective_topic"] = str(
                synthetic_story.get("source_title")
                or package.get("title")
                or topic
                or ""
            ).strip()
            return package
        logger.warning("Synthetic source story package generation failed; falling back to topic-based generation")

    # Current date for relevance
    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")
    script_template = _load_script_template()
    topic_for_prompt = str(topic or "").strip() or "unexpected personal story"

    if paragraph_only:
        prompt = f"""
    Create a complete content package for a YouTube Short about this topic: "{topic_for_prompt}"
    Date: {current_date}

    House script template:
    {script_template}

    Narrative style rules to follow strictly:
    - Write in first person singular (I, me, my).
    - Use modern, simple English that a 12-16 year old can easily understand.
    - Keep it natural and conversational, like one person directly talking.
    - Tell a personal experience story as if I went through it.
    - Build the story from the user topic only.
    - Avoid decorative wording, metaphors, and exaggerated hype.
    - Do not give tips, steps, advice, or "how-to" instructions.

    Provide ALL the following elements in a single JSON response:

    1. "paragraph": One single paragraph that:
       - Is the full narration script for text-to-speech
       - Has no internal line breaks
       - Starts directly on the topic
       - Is written in first person and sounds like natural speech
       - Uses simple, clear wording for young teens (12-16)
       - Uses short clear sentences with periods even though it stays one paragraph
       - Does not read like one giant run-on sentence
       - DOES NOT include labels like "Hook:", "Intro:", etc.
       - DOES NOT include tips, advice, steps, or list formats
       - DOES NOT include a presenter intro or title readout
       - DOES NOT use external citations, statistics, or quotes
       - Preserves the full story arc with no hard maximum length
       - Ends with a comment CTA sentence

    2. "title": A catchy, engaging title for the YouTube Short (40-60 characters)

    3. "description": A compelling video description (100-200 characters)
       - Includes 3-4 relevant hashtags

    4. "thumbnail_hf_prompt": A detailed image prompt for AI image generation (20-30 words)
       - Focus on WHAT should be in the image, not HOW it should be rendered

    5. "thumbnail_unsplash_query": A simple 2-4 word query for searching stock photos

    Format the response as a valid JSON object with these exact field names.
    """
    else:
        prompt = f"""
    Create a complete content package for a YouTube Short about this topic: "{topic_for_prompt}"
    Date: {current_date}

    House script template:
    {script_template}

    Narrative style rules to follow strictly:
    - Write in first person singular (I, me, my).
    - Use modern, simple English that a 12-16 year old can easily understand.
    - Keep it natural and conversational, like one person directly talking.
    - Tell a personal experience story as if I went through it.
    - Build the story from the user topic only.
    - Avoid decorative wording, metaphors, and exaggerated hype.
    - Do not give tips, steps, advice, or "how-to" instructions.

    Provide ALL the following elements in a single JSON response:

     1. "script": A script of 20 to 30 short lines that:
         - Uses newline-separated lines (one spoken beat per line)
         - Starts directly on the topic (no intro labels)
         - Is written in first person and sounds like natural speech
         - Is a coherent personal-experience story built from the topic
         - Uses simple, clear wording for young teens (12-16)
         - DOES NOT include tips, advice, steps, or list formats
         - DOES NOT include labels like "Hook:", "Intro:", etc.
         - DOES NOT include a presenter intro or title readout
         - DOES NOT use external citations, statistics, or quotes
         - DOES NOT add CTA/promotional lines
         - Is written as plain text to be spoken
         - Keeps each line concise and punchy (roughly 4 to 12 words)

    2. "title": A catchy, engaging title for the YouTube Short (40-60 characters)
       - Should grab attention and hint at valuable content
       - Include relevant keywords for search

    3. "description": A compelling video description (100-200 characters)
       - Summarizes the content
       - Includes 3-4 relevant trending hashtags

    4. "thumbnail_hf_prompt": A detailed image prompt for AI image generation (20-30 words)
       - Should represent the core visual concept for the thumbnail
       - Include specific visual elements, composition details
       - DO NOT include style descriptors (like "digital art", "photorealistic")
       - Focus on WHAT should be in the image, not HOW it should be rendered
       - Should make viewers want to click

    5. "thumbnail_unsplash_query": A simple 2-4 word query for searching stock photos
       - Should capture the core visual concept for a fallback thumbnail
       - Use common terms that would yield good stock photo results

    Format the response as a valid JSON object with these exact field names.
    """

    for attempt in range(retries):
        try:
            response_content = _create_json_completion(
                prompt=prompt,
                model=model,
                max_tokens=max_tokens,
                temperature=0.7,
            )

            try:
                # Parse and validate the JSON response
                content_package = _parse_json_response(response_content)

                # Check if all required fields are present
                required_fields = ["title", "description", "thumbnail_hf_prompt", "thumbnail_unsplash_query"]
                if paragraph_only:
                    required_fields.append("paragraph")
                else:
                    required_fields.append("script")
                missing_fields = [field for field in required_fields if field not in content_package]

                if missing_fields:
                    logger.warning(f"JSON response missing required fields: {missing_fields}")
                    raise ValueError(f"Missing required fields in response: {missing_fields}")

                if paragraph_only:
                    paragraph = _normalize_paragraph_narration_style(content_package.get("paragraph", ""))
                    content_package["paragraph"] = paragraph
                    content_package["script"] = paragraph
                else:
                    # Clean the script text of any remaining instructional labels
                    content_package["script"] = filter_instructional_labels(content_package["script"])

                content_package = _postprocess_content_package(
                    content_package,
                    topic=topic_for_prompt,
                    model=model,
                    retries=retries,
                    paragraph_only=paragraph_only,
                )
                content_package["effective_topic"] = str(content_package.get("title") or topic_for_prompt).strip()

                logger.info(f"Successfully generated comprehensive content package:")
                logger.info(f"Title: {content_package['title']}")
                if paragraph_only:
                    logger.info(f"Paragraph length: {len(str(content_package['paragraph']).split())} words")
                else:
                    logger.info(f"Script length: {len([ln for ln in str(content_package['script']).splitlines() if ln.strip()])} lines")
                logger.info(f"Thumbnail image prompt: {content_package['thumbnail_hf_prompt'][:50]}...")
                logger.info(f"Thumbnail Unsplash query: {content_package['thumbnail_unsplash_query']}")

                return content_package

            except json.JSONDecodeError as json_e:
                logger.error(f"Failed to parse JSON response from AI provider: {json_e}")
                logger.error(f"Raw response: {response_content}")
                if attempt == retries - 1:
                    break
            except ValueError as ve:
                logger.error(f"Invalid response format: {str(ve)}")
                if attempt == retries - 1:
                    break

        except ScitelyAPIError as e:
            provider = getattr(e, "provider", "ai")
            logger.error(f"{provider.capitalize()} API error (attempt {attempt + 1}/{retries}): {str(e)}")
            if provider == "scitely":
                disable_scitely(e)
            if attempt == retries - 1:
                break

        # If we get here, retry with exponential backoff
        wait_time = 2 ** attempt
        logger.info(f"Retrying in {wait_time} seconds (attempt {attempt + 1}/{retries})...")
        time.sleep(wait_time)

    logger.warning(
        "Falling back to local content package generation for topic '%s' after %s failed attempt(s).",
        topic_for_prompt,
        retries,
    )
    package = _build_fallback_content_package(topic_for_prompt, paragraph_only=paragraph_only)
    package = _postprocess_content_package(
        package,
        topic=topic_for_prompt,
        model=model,
        retries=retries,
        paragraph_only=paragraph_only,
    )
    package["effective_topic"] = str(package.get("title") or topic_for_prompt).strip()
    return package

if __name__ == "__main__": # This is used to run the script directly for testing
    # Example usage for batch query generation
    import logging
    from pprint import pprint

    # Configure basic logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

    # Define a test function for the new comprehensive content generation
    def test_comprehensive_content():
        print("Testing comprehensive content generation...")
        test_topic = "AI assistants are revolutionizing remote work"
        print(f"Topic: {test_topic}")

        try:
            content_package = generate_comprehensive_content(test_topic)
            print("\n===== GENERATED CONTENT PACKAGE =====")
            print(f"Title: {content_package['title']}")
            print(f"\nDescription: {content_package['description']}")
            print(f"\nThumbnail Image Prompt: {content_package['thumbnail_hf_prompt']}")
            print(f"\nThumbnail Unsplash Query: {content_package['thumbnail_unsplash_query']}")
            print(f"\nScript ({len(content_package['script'].split())} words):")
            print(content_package['script'])
            print("\n=====  END OF CONTENT PACKAGE  =====")
            return content_package
        except Exception as e:
            print(f"Error testing comprehensive content generation: {e}")
            return None

    # Choose which test to run
    test_comprehensive_content()
