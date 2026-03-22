import logging
import time
import re
import json
from pathlib import Path

from automation.scitely_client import (
    ScitelyAPIError,
    create_chat_completion,
    get_scitely_api_key,
    get_scitely_model,
)

# Configure logging - don't use basicConfig since main.py handles this
logger = logging.getLogger(__name__)

SCRIPT_TEMPLATE_PATH = Path(__file__).resolve().parent / "prompts" / "ai_shorts_script_template.txt"


def _load_script_template():
    try:
        return SCRIPT_TEMPLATE_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        logger.warning("Failed to load AI shorts script template from %s: %s", SCRIPT_TEMPLATE_PATH, exc)
        return ""


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
    try:
        response = create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"},
        )
    except ScitelyAPIError as exc:
        if "response_format" not in str(exc).lower():
            raise

        logger.warning(
            "Scitely model %s rejected JSON mode, retrying without response_format.",
            model,
        )
        response = create_chat_completion(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
        )

    return _extract_completion_content(response)

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
    if not get_scitely_api_key():
        raise ValueError("Scitely API key is not set. Please set SCITELY_API_KEY in .env.")

    model = model or get_scitely_model()

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
                logger.error(f"Failed to parse JSON response from Scitely: {json_e}. Response: {response_content}")
            except Exception as parse_e: # Catch other potential errors during dict conversion
                 logger.error(f"Error processing JSON response: {parse_e}. Response: {response_content}")

        except ScitelyAPIError as e:
            logger.error(f"Scitely API error generating batch video queries (attempt {attempt + 1}/{retries}): {str(e)}")

        # If loop continues, it means an error occurred
        if attempt < retries - 1:
             logger.info(f"Retrying batch query generation ({attempt + 2}/{retries})...")
             time.sleep(2 ** attempt)
        else:
            logger.error(f"Failed to generate batch video queries after {retries} attempts.")

    # Fallback: Return empty dict if all retries fail
    return {}

def generate_batch_image_prompts(texts: list[str], overall_topic="technology", model=None, retries=3):
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
    if not get_scitely_api_key():
        raise ValueError("Scitely API key is not set. Please set SCITELY_API_KEY in .env.")

    model = model or get_scitely_model()

    # Prepare the input text part of the prompt
    formatted_texts = ""
    for i, text in enumerate(texts):
        formatted_texts += f"--- Card {i} ---\n{text}\n\n"

    prompt = f"""
    You are an assistant that generates high-quality image prompts for AI image generation models like Stable Diffusion.
    Based on the following text sections from a video script about '{overall_topic}', create a detailed image prompt for EACH section.

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
                logger.error(f"Failed to parse JSON response from Scitely: {json_e}. Response: {response_content}")
            except Exception as parse_e:  # Catch other potential errors during dict conversion
                 logger.error(f"Error processing JSON response: {parse_e}. Response: {response_content}")

        except ScitelyAPIError as e:
            logger.error(f"Scitely API error generating batch image prompts (attempt {attempt + 1}/{retries}): {str(e)}")

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
    - 6 to 10 effects
    - At least 1 line gap between used lines (index delta >= 2)
    - effect_file must be from provided sound_effect_files list
    """
    if not script_lines or not sound_effect_files:
        return []

    if not get_scitely_api_key():
        logger.warning("Scitely API key not available; skipping sound effect planning")
        return []

    model = model or get_scitely_model()

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
    1) Choose between 6 and 10 sound effects total.
    2) Effects must be spaced with at least one line between them.
    3) Use only listed file names.
    4) Choose moments that add impact without overusing effects.
    5) offset_seconds is the start time inside that line and should be between 0.0 and 2.0.

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
                offset = max(0.0, min(2.0, offset))

                normalized.append(
                    {
                        "line_index": idx,
                        "effect_file": effect_file,
                        "offset_seconds": offset,
                    }
                )

            # Enforce spacing and 6-10 count deterministically
            normalized.sort(key=lambda x: x["line_index"])
            final_plan = []
            last_idx = None
            used_lines = set()
            for entry in normalized:
                if len(final_plan) >= 10:
                    break
                idx = entry["line_index"]
                if idx in used_lines:
                    continue
                if last_idx is not None and (idx - last_idx) < 2:
                    continue
                final_plan.append(entry)
                used_lines.add(idx)
                last_idx = idx

            if len(final_plan) < 6:
                used_lines = {x["line_index"] for x in final_plan}
                for idx in range(len(script_lines)):
                    if len(final_plan) >= 6:
                        break
                    if idx in used_lines:
                        continue
                    if any(abs(idx - x["line_index"]) < 2 for x in final_plan):
                        continue
                    fallback_name = sound_effect_files[len(final_plan) % len(sound_effect_files)]
                    final_plan.append(
                        {
                            "line_index": idx,
                            "effect_file": fallback_name,
                            "offset_seconds": 0.2,
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
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []


def generate_meme_insertion_plan(
    script_lines,
    topic="",
    model=None,
    retries=3,
    min_insertions=5,
    max_insertions=11,
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

    if not get_scitely_api_key():
        logger.warning("Scitely API key not available; skipping meme insertion planning")
        return []

    model = model or get_scitely_model()

    max_effective = max(1, min(int(max_insertions), 11, len(script_lines)))
    min_effective = max(1, min(int(min_insertions), max_effective))

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
    5) duration_seconds should be 2.0 to 5.0.
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
                        "duration_seconds": max(2.0, min(5.0, duration_seconds)),
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
                            "duration_seconds": 3.0,
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
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    return []

def generate_comprehensive_content(topic, model=None, max_tokens=800, retries=3):
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
    if not get_scitely_api_key():
        raise ValueError("Scitely API key is not set. Please set SCITELY_API_KEY in .env.")

    model = model or get_scitely_model()

    # Current date for relevance
    from datetime import datetime
    current_date = datetime.now().strftime("%Y-%m-%d")
    script_template = _load_script_template()

    prompt = f"""
    Create a complete content package for a YouTube Short about this topic: "{topic}"
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
                required_fields = ["script", "title", "description", "thumbnail_hf_prompt", "thumbnail_unsplash_query"]
                missing_fields = [field for field in required_fields if field not in content_package]

                if missing_fields:
                    logger.warning(f"JSON response missing required fields: {missing_fields}")
                    raise ValueError(f"Missing required fields in response: {missing_fields}")

                # Clean the script text of any remaining instructional labels
                content_package["script"] = filter_instructional_labels(content_package["script"])

                logger.info(f"Successfully generated comprehensive content package:")
                logger.info(f"Title: {content_package['title']}")
                logger.info(f"Script length: {len([ln for ln in str(content_package['script']).splitlines() if ln.strip()])} lines")
                logger.info(f"Thumbnail image prompt: {content_package['thumbnail_hf_prompt'][:50]}...")
                logger.info(f"Thumbnail Unsplash query: {content_package['thumbnail_unsplash_query']}")

                return content_package

            except json.JSONDecodeError as json_e:
                logger.error(f"Failed to parse JSON response from Scitely: {json_e}")
                logger.error(f"Raw response: {response_content}")
                if attempt == retries - 1:
                    raise
            except ValueError as ve:
                logger.error(f"Invalid response format: {str(ve)}")
                if attempt == retries - 1:
                    raise

        except ScitelyAPIError as e:
            logger.error(f"Scitely API error (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt == retries - 1:
                raise Exception(f"Failed to generate content package after {retries} attempts: {str(e)}")

        # If we get here, retry with exponential backoff
        wait_time = 2 ** attempt
        logger.info(f"Retrying in {wait_time} seconds (attempt {attempt + 1}/{retries})...")
        time.sleep(wait_time)

    # If we get here, all retries failed
    raise Exception(f"Failed to generate comprehensive content package after {retries} attempts")

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
