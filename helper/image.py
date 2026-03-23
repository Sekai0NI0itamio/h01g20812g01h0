import time
import random
import os
import requests
import logging
import concurrent.futures
import re
from urllib.parse import quote_plus
from moviepy  import VideoClip, concatenate_videoclips, ColorClip, CompositeVideoClip, ImageClip, TextClip
from helper.blur import custom_blur, custom_edge_blur
from helper.minor_helper import measure_time
from helper.network import create_requests_session
from helper.text import TextHelper
from dotenv import load_dotenv
from typing import Optional, List, Tuple, Dict, Any, Union

load_dotenv()


def get_pexels_api_key():
    return os.getenv("PEXELS_API_KEY")


def get_pixabay_api_key():
    return os.getenv("PIXABAY_API_KEY")


def get_unsplash_api_key():
    raw_value = (
        os.getenv("UNSPLASH_API_KEY")
        or os.getenv("UNSPLASH_ACCESS_KEY")
        or os.getenv("UNSPLASH_ACCESS_ID")
    )
    return _extract_unsplash_access_key(raw_value)


def _extract_unsplash_access_key(raw_value):
    """Extract Unsplash Access ID from either plain key or combined multi-line credential text."""
    if not raw_value:
        return None

    text = str(raw_value).strip()
    if not text:
        return None

    # If key material is stored as one blob, prefer the explicit Access ID field.
    match = re.search(r"(?im)^\s*access\s*(?:id|key)\s*:\s*([^\s]+)\s*$", text)
    if match:
        return match.group(1).strip()

    # Accept Application ID / Secret ID labels but keep only the first token if unlabeled.
    first_line = text.splitlines()[0].strip()
    if ":" in first_line and not re.search(r"(?i)access\s*(?:id|key)", first_line):
        return None

    # Plain single-token access key path.
    return first_line.split()[0]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

resolution = (1080, 1920)  # Assuming a standard resolution for YouTube Shorts

# Get temp directory from environment variable or use default
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp"))
# Create images subdirectory
temp_dir = os.path.join(TEMP_DIR, "generated_images")
os.makedirs(temp_dir, exist_ok=True)  # Create temp directory if it doesn't exist
REQUESTS_SESSION = create_requests_session()


def _log_proxy_usage(provider_name):
    proxy = REQUESTS_SESSION.proxies.get("https") if REQUESTS_SESSION.proxies else None
    if proxy:
        logger.info("%s requests using proxy: %s", provider_name, proxy)
    else:
        logger.warning("%s requests are not using Tor proxy", provider_name)

REALISTIC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]


def _browser_headers(referer):
    return {
        "User-Agent": random.choice(REALISTIC_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _download_image_url(image_url, file_path, referer):
    headers = _browser_headers(referer)
    resp = REQUESTS_SESSION.get(image_url, headers=headers, timeout=15)
    if resp.status_code != 200:
        return None
    if not resp.content:
        return None
    with open(file_path, "wb") as f:
        f.write(resp.content)
    return file_path


def _extract_candidate_image_urls(html_text):
    if not html_text:
        return []

    candidates = []

    # Common search-engine image CDN patterns.
    patterns = [
        r"https://imgs\.search\.brave\.com/[^\"'\s<>]+",
        r"https://images\.ecosia\.org/[^\"'\s<>]+",
        r"https://external-content\.duckduckgo\.com/iu/\?u=[^\"'\s<>]+",
        r"https://[^\"'\s<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\"'\s<>]*)?",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, html_text, flags=re.IGNORECASE)
        for m in matches:
            if m not in candidates:
                candidates.append(m)

    return candidates


def _fetch_image_from_brave(query, file_path):
    if not query:
        return None

    try:
        url = f"https://search.brave.com/images?q={quote_plus(query)}"
        resp = REQUESTS_SESSION.get(url, headers=_browser_headers("https://search.brave.com/"), timeout=15)
        if resp.status_code != 200:
            logger.warning("Brave image search failed: %s", resp.status_code)
            return None

        for image_url in _extract_candidate_image_urls(resp.text):
            written = _download_image_url(image_url, file_path, "https://search.brave.com/")
            if written:
                logger.info("Brave image downloaded to %s for query '%s'", file_path, query)
                return written
    except Exception as e:
        logger.warning("Brave image fetch failed for '%s': %s", query, e)

    return None


def _fetch_image_from_ecosia(query, file_path):
    if not query:
        return None

    try:
        url = f"https://www.ecosia.org/images?q={quote_plus(query)}"
        resp = REQUESTS_SESSION.get(url, headers=_browser_headers("https://www.ecosia.org/"), timeout=15)
        if resp.status_code != 200:
            logger.warning("Ecosia image search failed: %s", resp.status_code)
            return None

        for image_url in _extract_candidate_image_urls(resp.text):
            written = _download_image_url(image_url, file_path, "https://www.ecosia.org/")
            if written:
                logger.info("Ecosia image downloaded to %s for query '%s'", file_path, query)
                return written
    except Exception as e:
        logger.warning("Ecosia image fetch failed for '%s': %s", query, e)

    return None

@measure_time
def fetch_image_from_duckduckgo(query, file_path=None):
    """
    Fetch the first DuckDuckGo image result for a query and save it locally.
    Returns local file path or None.
    """
    if not query:
        return None

    if not file_path:
        file_path = os.path.join(temp_dir, f"duckduckgo_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    headers = _browser_headers("https://duckduckgo.com/")

    try:
        _log_proxy_usage("DuckDuckGo")

        # 1) Get vqd token from initial search page.
        token_resp = REQUESTS_SESSION.get(
            "https://duckduckgo.com/",
            params={"q": query},
            headers=headers,
            timeout=12,
        )
        if token_resp.status_code != 200:
            logger.warning("DuckDuckGo token request failed: %s", token_resp.status_code)
            return None

        token_match = re.search(r"vqd='([^']+)'|vqd=\"([^\"]+)\"", token_resp.text)
        if not token_match:
            logger.warning("DuckDuckGo vqd token not found for query: %s", query)
            return None
        vqd = token_match.group(1) or token_match.group(2)

        # 2) Query image JSON endpoint.
        image_resp = REQUESTS_SESSION.get(
            "https://duckduckgo.com/i.js",
            params={
                "q": query,
                "vqd": vqd,
                "o": "json",
                "l": "us-en",
                "f": ",,,",
                "p": "1",
            },
            headers=headers,
            timeout=12,
        )
        if image_resp.status_code != 200:
            logger.warning("DuckDuckGo image search failed: %s", image_resp.status_code)
            brave_fallback = _fetch_image_from_brave(query, file_path)
            if brave_fallback:
                return brave_fallback
            return _fetch_image_from_ecosia(query, file_path)

        payload = image_resp.json() or {}
        results = payload.get("results", [])
        if not results:
            logger.warning("DuckDuckGo image search returned no results for query: %s", query)
            return None

        first = results[0]
        image_url = first.get("image") or first.get("thumbnail")
        if not image_url:
            logger.warning("DuckDuckGo first result missing image URL for query: %s", query)
            return None

        # 3) Download image.
        download_resp = REQUESTS_SESSION.get(image_url, headers=headers, timeout=12)
        if download_resp.status_code != 200:
            logger.warning("Failed to download DuckDuckGo image: %s", download_resp.status_code)
            brave_fallback = _fetch_image_from_brave(query, file_path)
            if brave_fallback:
                return brave_fallback
            return _fetch_image_from_ecosia(query, file_path)

        with open(file_path, "wb") as f:
            f.write(download_resp.content)

        logger.info("DuckDuckGo image downloaded to %s for query '%s'", file_path, query)
        return file_path
    except Exception as e:
        logger.warning("DuckDuckGo image fetch failed for '%s': %s", query, e)
        brave_fallback = _fetch_image_from_brave(query, file_path)
        if brave_fallback:
            return brave_fallback
        return _fetch_image_from_ecosia(query, file_path)

@measure_time
def generate_images_parallel(prompts, style="photorealistic", max_workers=None):
    """
    Generate multiple images in parallel based on prompts

    Args:
        prompts (list): List of image generation prompts
        style (str): Style to apply to the images
        max_workers (int): Maximum number of concurrent workers

    Returns:
        list: List of paths to generated images
    """
    start_time = time.time()
    logger.info(f"Generating {len(prompts)} images in parallel")

    def generate_single_image(prompt):
        try:
            logger.info(f"Trying DuckDuckGo for: {prompt[:30]}...")
            image_path = fetch_image_from_duckduckgo(prompt)
            if image_path:
                return image_path

            logger.info(f"Trying Unsplash for: {prompt[:30]}...")
            image_path = _fetch_image_from_unsplash(prompt)
            if image_path:
                return image_path

            # If Unsplash fails, try Pexels
            logger.info(f"Trying Pexels for: {prompt[:30]}...")
            image_path = _fetch_image_from_pexels(prompt)
            if image_path:
                return image_path

            # If all services fail
            logger.error(f"All image generation methods failed for prompt: {prompt[:50]}...")
            return None
        except Exception as e:
            logger.error(f"Error generating image: {e}")
            return None

    if not max_workers:
        # Use fewer workers for API calls to avoid rate limiting
        max_workers = min(len(prompts), 4)

    # Image generation is I/O bound (API calls), so use ThreadPoolExecutor
    image_paths = [None] * len(prompts)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(generate_single_image, prompt): idx
            for idx, prompt in enumerate(prompts)
        }

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                image_path = future.result()
                image_paths[idx] = image_path
            except Exception as e:
                logger.error(f"Failed to get result from image generation: {e}")
                image_paths[idx] = None

    total_time = time.time() - start_time
    success_count = len([p for p in image_paths if p])
    logger.info(f"Generated {success_count}/{len(prompts)} images in {total_time:.2f} seconds")

    # Log warning if some images failed but not all
    if 0 < success_count < len(prompts):
        logger.warning(f"Some image generation requests failed ({len(prompts) - success_count}/{len(prompts)})")

    return image_paths

@measure_time
def _generate_image_from_prompt(prompt, style="photorealistic", file_path=None, model=None):
  """
  Attempt to generate an image with the primary AI provider.

  Args:
      prompt (str): Image generation prompt
      style (str): Style to apply to the image (e.g., "digital art", "realistic", "photorealistic")
      file_path (str): Path to save the image, if None a path will be generated
      model (str): Optional override model name

  Returns:
      str: Path to the generated image or None if failed
  """
  # OpenRouter image generation is disabled by design.
  logger.info("OpenRouter image generation disabled; returning None for AI image request")
  return None


@measure_time
def generate_image_from_prompt(prompt, style="photorealistic", file_path=None, model=None):
    return _generate_image_from_prompt(prompt, style=style, file_path=file_path, model=model)

@measure_time
def _fetch_image_from_unsplash(query, file_path=None):
    """
    Fetch an image from Unsplash based on query

    Args:
        query (str): Search query
        file_path (str): Path to save the image, if None a path will be generated

    Returns:
        str: Path to the fetched image or None if failed
    """
    if not file_path:
        file_path = os.path.join(temp_dir, f"unsplash_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    unsplash_api_key = get_unsplash_api_key()
    if not unsplash_api_key:
        logger.warning("No Unsplash API key provided")
        return None

    try:
        # Prepare Unsplash API request
        url = "https://api.unsplash.com/search/photos"
        params = {
            "query": query,
            "per_page": 1,
            "orientation": "portrait",  # For YouTube shorts
            "client_id": unsplash_api_key
        }

        # Make request
        _log_proxy_usage("Unsplash")
        response = REQUESTS_SESSION.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            if data["results"]:
                image_url = data["results"][0]["urls"]["regular"]

                # Download image
                img_response = REQUESTS_SESSION.get(image_url)
                if img_response.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(img_response.content)
                    logger.info(f"Unsplash image downloaded to {file_path}")
                    return file_path
            else:
                logger.warning(f"No results found on Unsplash for query: {query}")
        else:
            logger.error(f"Unsplash API error: {response.status_code} - {response.text}")

    except Exception as e:
        logger.error(f"Error fetching image from Unsplash: {e}")

    return None

@measure_time
def _fetch_image_from_pexels(query, file_path=None):
    """
    Fetch an image from Pexels based on query

    Args:
        query (str): Search query
        file_path (str): Path to save the image, if None a path will be generated

    Returns:
        str: Path to the fetched image or None if failed
    """
    if not file_path:
        file_path = os.path.join(temp_dir, f"pexels_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    pexels_api_key = get_pexels_api_key()
    if not pexels_api_key:
        logger.warning("No Pexels API key provided")
        return None

    try:
        # Prepare Pexels API request
        url = "https://api.pexels.com/v1/search"
        headers = {"Authorization": pexels_api_key}
        params = {
            "query": query,
            "per_page": 1,
            "orientation": "portrait"  # For YouTube shorts
        }

        # Make request
        _log_proxy_usage("Pexels")
        response = REQUESTS_SESSION.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            if data.get("photos"):
                image_url = data["photos"][0]["src"]["large"]

                # Download image
                img_response = REQUESTS_SESSION.get(image_url)
                if img_response.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(img_response.content)
                    logger.info(f"Pexels image downloaded to {file_path}")
                    return file_path
            else:
                logger.warning(f"No results found on Pexels for query: {query}")
        else:
            logger.error(f"Pexels API error: {response.status_code} - {response.text}")

    except Exception as e:
        logger.error(f"Error fetching image from Pexels: {e}")

    return None

@measure_time
def create_clip(args):
    """
    Helper function to create an image clip (moved outside create_image_clips_parallel to fix serialization issues)

    Args:
        args (tuple): Tuple containing (image_path, duration, text)

    Returns:
        VideoClip: Created image clip
    """
    image_path, duration, text = args
    if not image_path:
        return None
    try:
        return _create_still_image_clip(image_path, duration, text, with_zoom=True)
    except Exception as e:
        logger.error(f"Error creating image clip: {e}")
        return None


def _center_crop_clip(clip, width, height):
    """MoviePy compatibility helper for center-cropping across versions."""
    w, h = clip.size
    x1 = max(0, int((w - width) / 2))
    y1 = max(0, int((h - height) / 2))

    if hasattr(clip, "cropped"):
        return clip.cropped(x1=x1, y1=y1, width=width, height=height)
    if hasattr(clip, "crop"):
        return clip.crop(x1=x1, y1=y1, width=width, height=height)
    raise AttributeError("Image clip does not support crop/cropped")

@measure_time
def create_image_clips_parallel(image_paths, durations, texts=None, with_zoom=True, max_workers=None):
    """
    Create still image clips in parallel

    Args:
        image_paths (list): List of paths to images
        durations (list): List of durations for each clip
        texts (list): Optional list of text overlays
        with_zoom (bool): Whether to add zoom effect
        max_workers (int): Maximum number of concurrent workers

    Returns:
        list: List of video clips
    """
    start_time = time.time()
    logger.info(f"Creating {len(image_paths)} image clips in parallel")

    if not texts:
        texts = [None] * len(image_paths)

    # Make sure all lists have the same length
    if len(durations) != len(image_paths):
        logger.warning(f"Duration list length {len(durations)} doesn't match image paths length {len(image_paths)}")
        # Pad or truncate durations list
        if len(durations) < len(image_paths):
            durations.extend([5.0] * (len(image_paths) - len(durations)))
        else:
            durations = durations[:len(image_paths)]

    if len(texts) != len(image_paths):
        texts = [None] * len(image_paths)

    if not max_workers:
        max_workers = min(len(image_paths), os.cpu_count())

    # Image clip creation is CPU bound, but use ThreadPoolExecutor instead of ProcessPoolExecutor
    # to avoid serialization issues
    clips = [None] * len(image_paths)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(create_clip, (img, dur, txt)): idx
            for idx, (img, dur, txt) in enumerate(zip(image_paths, durations, texts))
        }

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                clip = future.result()
                clips[idx] = clip
            except Exception as e:
                logger.error(f"Failed to get result from image clip creation: {e}")
                clips[idx] = None

    total_time = time.time() - start_time
    valid_clips = [clip for clip in clips if clip]
    logger.info(f"Created {len(valid_clips)} image clips in {total_time:.2f} seconds")

    return clips

@measure_time
def _create_still_image_clip(image_path, duration, text=None, text_position=('center','center'),
                          font_size=60, with_zoom=True, zoom_factor=0.05):
  """
  Create a still image clip with optional text and zoom effect

  Args:
      image_path (str): Path to the image
      duration (float): Duration of the clip in seconds
      text (str): Optional text overlay
      text_position (str): Position of text ('top', 'center', ('center','center'))
      font_size (int): Font size for text
      with_zoom (bool): Whether to add a subtle zoom effect
      zoom_factor (float): Rate of zoom (higher = faster zoom)

  Returns:
      VideoClip: MoviePy clip containing the image and effects
  """
  # Load image
  image = ImageClip(image_path)

  # resized to fill screen while maintaining aspect ratio
  img_ratio = image.size[0] / image.size[1]
  target_ratio = resolution[0] / resolution[1]

  if img_ratio > target_ratio:  # Image is wider
      new_height = resolution[1]
      new_width = int(new_height * img_ratio)
  else:  # Image is taller
      new_width = resolution[0]
      new_height = int(new_width / img_ratio)

  image = image.resized(new_size=(new_width, new_height))

  # Center crop if needed
  if new_width > resolution[0] or new_height > resolution[1]:
      image = _center_crop_clip(image, resolution[0], resolution[1])

  # Add zoom effect if requested
  if with_zoom:
      def zoom(t):
          # Start at 1.0 zoom and gradually increase
          zoom_level = 1 + (t / duration) * zoom_factor
          return zoom_level

      # Replace lambda with named function
      def zoom_func(t):
          return zoom(t)

      image = image.resized(zoom_func)

  # Make sure the image is the right duration
  image = image.with_duration(duration)

  # Add text if provided
  if text:
      txt = TextClip(
          text=text,
          font_size=font_size,
          color='white',
          font=r"/home/addy/projects/youtube-shorts-automation/packages/fonts/default_font.ttf",
          stroke_color='black',
          stroke_width=1,
          method='caption',
          size=(resolution[0] - 100, None)
      ).with_duration(duration)

      # Add shadow for text
      txt_shadow = TextClip(
          text=text,
          font_size=font_size,
          color='black',
          font=r"/home/addy/projects/youtube-shorts-automation/packages/fonts/default_font.ttf",
          method='caption',
          size=(resolution[0] - 100, None)
      ).with_position((2, 2), relative=True).with_opacity(0.6).with_duration(duration)

      # Position the text
      txt = txt.with_position(text_position)
      txt_shadow = txt_shadow.with_position(text_position)

      # Composite all together
      return CompositeVideoClip([image, txt_shadow, txt], size=resolution)
  else:
      return image
