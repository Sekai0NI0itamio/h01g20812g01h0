import asyncio
import base64
import binascii
import os
import concurrent.futures
import logging
import random
import re
import threading
import time
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import unquote, urlparse

import requests
from moviepy  import VideoClip, concatenate_videoclips, ColorClip, CompositeVideoClip, ImageClip, TextClip
from PIL import Image, ImageDraw, ImageFilter, ImageOps
from helper.blur import custom_blur, custom_edge_blur
from helper.minor_helper import measure_time
from helper.network import create_requests_session
from helper.text import TextHelper
from dotenv import load_dotenv
from typing import Optional, List, Tuple, Dict, Any, Union

load_dotenv()


def _env_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return int(default)


def _env_float(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return float(default)


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
IMAGE_SEARCH_USE_TOR_TUNNEL = _env_bool("IMAGE_SEARCH_USE_TOR_TUNNEL", False)
REQUESTS_SESSION = create_requests_session(use_tor=IMAGE_SEARCH_USE_TOR_TUNNEL)
logger.info("Image search Tor tunnel enabled: %s", IMAGE_SEARCH_USE_TOR_TUNNEL)
_PROVIDER_RATE_LOCK = threading.Lock()
_LAST_PROVIDER_REQUEST_TS = {}


def _log_proxy_usage(provider_name):
    proxy = REQUESTS_SESSION.proxies.get("https") if REQUESTS_SESSION.proxies else None
    if proxy:
        logger.info("%s requests using proxy: %s", provider_name, proxy)
    else:
        logger.info("%s requests using direct network (Tor disabled)", provider_name)


def _throttle_provider(provider_key, default_interval_seconds, env_name):
    wait_floor = max(0.0, _env_float(env_name, default_interval_seconds))
    if wait_floor <= 0:
        return

    with _PROVIDER_RATE_LOCK:
        now = time.monotonic()
        last = _LAST_PROVIDER_REQUEST_TS.get(provider_key, 0.0)
        wait_time = (last + wait_floor) - now
        if wait_time > 0:
            time.sleep(wait_time)
            now = time.monotonic()
        _LAST_PROVIDER_REQUEST_TS[provider_key] = now


def _backoff_after_status(status_code, env_name, default_seconds):
    if status_code not in {403, 429, 500, 502, 503, 504}:
        return

    cooldown = max(0.0, _env_float(env_name, default_seconds))
    if cooldown > 0:
        time.sleep(cooldown)


def _load_g4f():
    try:
        from g4f.client import AsyncClient
        import g4f.Provider as provider_module
    except Exception as exc:
        raise RuntimeError(
            "g4f is not installed or failed to import. Install dependencies first, for example: pip install g4f"
        ) from exc
    return AsyncClient, provider_module


def _resolve_g4f_provider(provider_name, provider_module):
    provider = getattr(provider_module, provider_name, None)
    if provider is None:
        raise ValueError(f"Unknown g4f provider: {provider_name}")
    return provider


def _get_g4f_response_items(response):
    data = getattr(response, "data", None)
    if data is not None:
        return list(data)
    if isinstance(response, dict):
        return list(response.get("data") or [])
    if hasattr(response, "model_dump"):
        payload = response.model_dump(exclude_none=True)
        return list(payload.get("data") or [])
    return []


def _get_g4f_item_value(item, key):
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key, None)


def _save_g4f_data_url(data_url, destination):
    _, encoded = data_url.split(",", 1)
    destination.write_bytes(base64.b64decode(encoded))


def _save_g4f_url(url, destination):
    parsed = urlparse(url)
    if url.startswith("data:"):
        _save_g4f_data_url(url, destination)
        return

    if parsed.scheme in {"http", "https"}:
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        destination.write_bytes(response.content)
        return

    if parsed.scheme == "file":
        source = Path(unquote(parsed.path))
        destination.write_bytes(source.read_bytes())
        return

    source = Path(url).expanduser()
    if source.exists():
        destination.write_bytes(source.read_bytes())
        return

    raise RuntimeError(f"Could not save image from response URL: {url}")


def _save_g4f_image_item(item, destination):
    b64_json = _get_g4f_item_value(item, "b64_json")
    if isinstance(b64_json, str) and b64_json.strip():
        try:
            destination.write_bytes(base64.b64decode(b64_json))
            return
        except binascii.Error as exc:
            raise RuntimeError("Invalid base64 payload returned by g4f.") from exc

    url = _get_g4f_item_value(item, "url")
    if isinstance(url, str) and url.strip():
        _save_g4f_url(url, destination)
        return

    raise RuntimeError("g4f returned an image item without b64_json or url.")


def _normalize_ai_image_prompt(prompt, style):
    clean_prompt = " ".join(str(prompt or "").split()).strip()
    if not clean_prompt:
        clean_prompt = "dramatic short-form storytelling scene"

    style_text = str(style or "cinematic illustration").strip()
    return (
        f"{clean_prompt}. Create a high-quality vertical 9:16 visual for a YouTube Shorts background. "
        f"Use a polished {style_text} finish. Strong subject clarity. Cinematic lighting. Rich detail. "
        "Centered composition that survives portrait cropping. No text. No captions. No watermark."
    )


def _inspect_image_quality(image_path):
    if not image_path or not os.path.exists(image_path):
        return False, "missing file"

    try:
        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            width, height = img.size
    except Exception as exc:
        return False, f"unreadable image: {exc}"

    if min(width, height) < MIN_ACCEPTABLE_IMAGE_DIMENSION:
        return False, f"resolution too small ({width}x{height})"
    if width * height < MIN_ACCEPTABLE_IMAGE_PIXELS:
        return False, f"pixel count too small ({width}x{height})"

    return True, f"{width}x{height}"


def _accept_image_candidate(image_path, provider_name):
    ok, detail = _inspect_image_quality(image_path)
    if ok:
        logger.info("Accepted %s image asset %s (%s)", provider_name, os.path.basename(image_path), detail)
        return True

    logger.warning("Rejected %s image asset %s: %s", provider_name, image_path, detail)
    return False

REALISTIC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
]

DEFAULT_AI_IMAGE_PROVIDER = "HuggingSpace"
DEFAULT_AI_IMAGE_MODEL = "flux-kontext-dev"
MIN_ACCEPTABLE_IMAGE_DIMENSION = int(os.getenv("MIN_ACCEPTABLE_IMAGE_DIMENSION", "720"))
MIN_ACCEPTABLE_IMAGE_PIXELS = int(os.getenv("MIN_ACCEPTABLE_IMAGE_PIXELS", "650000"))
ENABLE_G4F_IMAGE_FALLBACK = _env_bool("ENABLE_G4F_IMAGE_FALLBACK", True)

QUERY_STOPWORDS = {
    "the", "and", "with", "from", "that", "this", "into", "over", "under", "through",
    "photorealistic", "portrait", "background", "showing", "displaying", "screen", "phone",
    "meme",
}


def _build_query_candidates(query, max_terms=8):
    text = (query or "").strip().lower()
    if not text:
        return []

    normalized = re.sub(r"[^a-z0-9\s]+", " ", text)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    candidates = []
    if normalized:
        candidates.append(normalized)

    words = []
    for token in normalized.split():
        if len(token) < 3:
            continue
        if token.isdigit():
            continue
        if token in QUERY_STOPWORDS:
            continue
        if token not in words:
            words.append(token)

    if words:
        candidates.append(" ".join(words[: min(len(words), 6)]))

    for window_size in (3, 2):
        for idx in range(0, max(0, len(words) - window_size + 1)):
            phrase = " ".join(words[idx:idx + window_size])
            if phrase and phrase not in candidates:
                candidates.append(phrase)
            if len(candidates) >= max_terms:
                break
        if len(candidates) >= max_terms:
            break

    for word in words[:max_terms]:
        if word not in candidates:
            candidates.append(word)

    return candidates


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


def _build_g4f_attempts(provider_name, model_name):
    attempts = []

    def add_attempt(provider, model):
        key = (str(provider or "").strip(), str(model or "").strip())
        if not key[0] or not key[1]:
            return
        if key not in attempts:
            attempts.append(key)

    add_attempt(provider_name, model_name)
    add_attempt(os.getenv("G4F_IMAGE_PROVIDER"), os.getenv("G4F_IMAGE_MODEL"))
    add_attempt("HuggingSpace", "flux-kontext-dev")
    add_attempt("PollinationsAI", "kontext")
    add_attempt("PollinationsAI", "flux")

    return attempts


def _generate_local_fallback_image(prompt, style="photorealistic", file_path=None):
    """Last-resort local fallback so image-mode renders can still complete."""
    if not _env_bool("ENABLE_LOCAL_IMAGE_PLACEHOLDER_FALLBACK", True):
        return None

    if not file_path:
        file_path = os.path.join(temp_dir, f"local_fallback_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    seed = sum((idx + 1) * ord(ch) for idx, ch in enumerate(str(prompt or "")[:240]))
    rng = random.Random(seed)

    palette = [
        (17, 24, 39),
        (24, 58, 98),
        (81, 45, 168),
        (16, 94, 78),
        (180, 83, 9),
        (153, 27, 27),
    ]
    bg_a = palette[seed % len(palette)]
    bg_b = palette[(seed + 3) % len(palette)]

    img = Image.new("RGB", resolution, bg_a)
    overlay = Image.new("RGBA", resolution, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)

    for idx in range(10):
        accent = palette[(seed + idx) % len(palette)]
        alpha = 45 + ((idx * 9) % 45)
        width = rng.randint(int(resolution[0] * 0.25), int(resolution[0] * 0.9))
        height = rng.randint(int(resolution[1] * 0.10), int(resolution[1] * 0.38))
        x0 = rng.randint(-180, resolution[0] - 60)
        y0 = rng.randint(-220, resolution[1] - 120)
        overlay_draw.ellipse(
            [(x0, y0), (x0 + width, y0 + height)],
            fill=(*accent, alpha),
        )

    overlay = overlay.filter(ImageFilter.GaussianBlur(92))
    composite = Image.alpha_composite(img.convert("RGBA"), overlay)

    gradient = Image.new("RGBA", resolution, (0, 0, 0, 0))
    gradient_draw = ImageDraw.Draw(gradient)
    for y in range(resolution[1]):
        mix = y / max(1, resolution[1] - 1)
        color = tuple(int(bg_a[i] * (1 - mix) + bg_b[i] * mix) for i in range(3))
        gradient_draw.line([(0, y), (resolution[0], y)], fill=(*color, 28))
    composite = Image.alpha_composite(composite, gradient)

    frame = Image.new("RGBA", resolution, (0, 0, 0, 0))
    frame_draw = ImageDraw.Draw(frame)
    panel_margin = int(resolution[0] * 0.08)
    frame_draw.rounded_rectangle(
        [
            (panel_margin, int(resolution[1] * 0.14)),
            (resolution[0] - panel_margin, int(resolution[1] * 0.86)),
        ],
        radius=48,
        outline=(255, 255, 255, 36),
        width=3,
        fill=(255, 255, 255, 10),
    )
    composite = Image.alpha_composite(composite, frame)
    composite.convert("RGB").save(file_path, quality=95)

    logger.info("Local fallback image created at %s for prompt '%s'", file_path, (prompt or "")[:60])
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


@measure_time
def _fetch_image_from_pixabay(query, file_path=None):
    """Fetch an image from Pixabay based on query."""
    if not file_path:
        file_path = os.path.join(temp_dir, f"pixabay_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    pixabay_api_key = get_pixabay_api_key()
    if not pixabay_api_key:
        logger.warning("No Pixabay API key provided")
        return None

    try:
        _log_proxy_usage("Pixabay")
        for candidate in _build_query_candidates(query):
            _throttle_provider("pixabay_image", 0.8, "PIXABAY_IMAGE_MIN_INTERVAL_SECONDS")
            params = {
                "key": pixabay_api_key,
                "q": candidate,
                "image_type": "photo",
                "orientation": "vertical",
                "safesearch": "true",
                "per_page": 5,
                "min_width": 720,
            }
            response = REQUESTS_SESSION.get("https://pixabay.com/api/", params=params, timeout=20)

            if response.status_code != 200:
                logger.warning("Pixabay API error: %s for query '%s'", response.status_code, candidate)
                _backoff_after_status(
                    response.status_code,
                    "PIXABAY_IMAGE_RATE_LIMIT_BACKOFF_SECONDS",
                    1.2,
                )
                continue

            hits = (response.json() or {}).get("hits") or []
            if not hits:
                logger.info("Pixabay returned no results for query '%s'", candidate)
                continue

            for hit in hits[:5]:
                image_url = hit.get("largeImageURL") or hit.get("webformatURL")
                if not image_url:
                    continue

                img_response = REQUESTS_SESSION.get(image_url, timeout=20)
                if img_response.status_code != 200 or not img_response.content:
                    continue

                with open(file_path, "wb") as f:
                    f.write(img_response.content)
                logger.info("Pixabay image downloaded to %s (query='%s')", file_path, candidate)
                return file_path

    except Exception as e:
        logger.error(f"Error fetching image from Pixabay: {e}")

    return None


@measure_time
def _fetch_image_from_wikimedia_commons(query, file_path=None):
    """Fetch an image from Wikimedia Commons without requiring an API key."""
    if not file_path:
        file_path = os.path.join(temp_dir, f"wikimedia_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

    try:
        _log_proxy_usage("Wikimedia Commons")
        headers = _browser_headers("https://commons.wikimedia.org/")

        for candidate in _build_query_candidates(query):
            _throttle_provider("wikimedia_image", 0.4, "WIKIMEDIA_IMAGE_MIN_INTERVAL_SECONDS")
            search_response = REQUESTS_SESSION.get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query",
                    "format": "json",
                    "list": "search",
                    "srsearch": candidate,
                    "srnamespace": 6,
                    "srlimit": 5,
                },
                headers=headers,
                timeout=20,
            )
            if search_response.status_code != 200:
                logger.warning("Wikimedia search error: %s for query '%s'", search_response.status_code, candidate)
                continue

            results = (search_response.json() or {}).get("query", {}).get("search") or []
            if not results:
                logger.info("Wikimedia returned no results for query '%s'", candidate)
                continue

            titles = [item.get("title") for item in results if item.get("title")]
            if not titles:
                continue

            image_response = REQUESTS_SESSION.get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query",
                    "format": "json",
                    "prop": "imageinfo",
                    "iiprop": "url|size",
                    "iiurlwidth": 1280,
                    "titles": "|".join(titles),
                },
                headers=headers,
                timeout=20,
            )
            if image_response.status_code != 200:
                logger.warning("Wikimedia image lookup error: %s for query '%s'", image_response.status_code, candidate)
                continue

            pages = ((image_response.json() or {}).get("query") or {}).get("pages") or {}
            for page in pages.values():
                imageinfo = (page or {}).get("imageinfo") or []
                if not imageinfo:
                    continue

                image_url = imageinfo[0].get("thumburl") or imageinfo[0].get("url")
                if not image_url:
                    continue

                written = _download_image_url(image_url, file_path, "https://commons.wikimedia.org/")
                if written:
                    logger.info("Wikimedia Commons image downloaded to %s (query='%s')", file_path, candidate)
                    return written

    except Exception as e:
        logger.error(f"Error fetching image from Wikimedia Commons: {e}")

    return None


def _fetch_image_from_brave(query, file_path):
    if not query:
        return None

    try:
        _throttle_provider("brave_image", 0.45, "BRAVE_IMAGE_MIN_INTERVAL_SECONDS")
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
        _throttle_provider("ecosia_image", 0.45, "ECOSIA_IMAGE_MIN_INTERVAL_SECONDS")
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
        _throttle_provider("duckduckgo_image", 0.45, "DUCKDUCKGO_IMAGE_MIN_INTERVAL_SECONDS")

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
            return fetch_best_image_for_prompt(prompt, style=style)
        except Exception as e:
            logger.error(f"Error generating image: {e}")
            return None

    if not max_workers:
        # Use fewer workers for API calls, especially when Tor is enabled.
        if REQUESTS_SESSION.proxies:
            max_workers = min(len(prompts), max(1, _env_int("SHORTS_IMAGE_FETCH_MAX_WORKERS_PROXY", 2)))
        else:
            max_workers = min(len(prompts), max(1, _env_int("SHORTS_IMAGE_FETCH_MAX_WORKERS", 3)))

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
    Generate an image with g4f for prompts that stock providers cannot satisfy cleanly.
    """
    if not ENABLE_G4F_IMAGE_FALLBACK:
        logger.info("g4f image fallback disabled by environment")
        return None

    if not prompt:
        return None

    if not file_path:
        file_path = os.path.join(temp_dir, f"g4f_{int(time.time())}_{random.randint(1000, 9999)}.png")

    prompt_text = _normalize_ai_image_prompt(prompt, style)
    provider_name = os.getenv("G4F_IMAGE_PROVIDER", DEFAULT_AI_IMAGE_PROVIDER)
    model_name = model or os.getenv("G4F_IMAGE_MODEL", DEFAULT_AI_IMAGE_MODEL)

    async def _run():
        AsyncClient, provider_module = _load_g4f()
        client = AsyncClient()
        last_exc = None

        for attempt_provider_name, attempt_model_name in _build_g4f_attempts(provider_name, model_name):
            try:
                provider = _resolve_g4f_provider(attempt_provider_name, provider_module)
                response = await client.images.generate(
                    prompt=prompt_text,
                    model=attempt_model_name,
                    provider=provider,
                    response_format="b64_json",
                    n=1,
                )
                items = _get_g4f_response_items(response)
                if not items:
                    raise RuntimeError("g4f returned no images.")
                _save_g4f_image_item(items[0], Path(file_path))
                logger.info(
                    "g4f image generated via provider=%s model=%s",
                    attempt_provider_name,
                    attempt_model_name,
                )
                return
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "g4f image attempt failed for provider=%s model=%s prompt='%s': %s",
                    attempt_provider_name,
                    attempt_model_name,
                    prompt[:50],
                    exc,
                )

        raise last_exc or RuntimeError("g4f image generation failed after all configured attempts.")

    try:
        asyncio.run(_run())
    except Exception as exc:
        logger.warning("g4f image generation failed for '%s': %s", prompt[:50], exc)
        return None

    if _accept_image_candidate(file_path, "g4f"):
        return file_path

    return None


@measure_time
def generate_image_from_prompt(prompt, style="photorealistic", file_path=None, model=None):
    return _generate_image_from_prompt(prompt, style=style, file_path=file_path, model=model)


def fetch_best_image_for_prompt(
    prompt,
    style="photorealistic",
    allow_ai_fallback=True,
    allow_generated_fallback=True,
):
    """Fetch the best available visual for a prompt, optionally restricting fallback to browser/API search results only."""
    if not prompt:
        return None

    # Small jitter reduces burst-rate bans from public search endpoints.
    jitter_min = max(0.0, _env_float("IMAGE_FETCH_PROMPT_JITTER_MIN_SECONDS", 0.25))
    jitter_max = max(jitter_min, _env_float("IMAGE_FETCH_PROMPT_JITTER_MAX_SECONDS", 0.7))
    time.sleep(random.uniform(jitter_min, jitter_max))

    attempts = [
        ("DuckDuckGo", lambda: fetch_image_from_duckduckgo(prompt)),
        ("Unsplash", lambda: _fetch_image_from_unsplash(prompt)),
        ("Pexels", lambda: _fetch_image_from_pexels(prompt)),
        ("Pixabay", lambda: _fetch_image_from_pixabay(prompt)),
        ("Wikimedia Commons", lambda: _fetch_image_from_wikimedia_commons(prompt)),
    ]

    for provider_name, loader in attempts:
        logger.info("Trying %s for: %s...", provider_name, prompt[:30])
        image_path = loader()
        if image_path and _accept_image_candidate(image_path, provider_name):
            return image_path

    if allow_ai_fallback and allow_generated_fallback:
        logger.info("Trying g4f AI image fallback for: %s...", prompt[:30])
        image_path = generate_image_from_prompt(prompt, style=style)
        if image_path:
            return image_path

    if allow_generated_fallback:
        logger.info("Trying local generated image fallback for: %s...", prompt[:30])
        image_path = _generate_local_fallback_image(prompt, style=style)
        if image_path and _accept_image_candidate(image_path, "local"):
            return image_path

    logger.error("All image generation methods failed for prompt: %s...", prompt[:50])
    return None

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
        _log_proxy_usage("Unsplash")
        for candidate in _build_query_candidates(query):
            _throttle_provider("unsplash_image", 0.85, "UNSPLASH_IMAGE_MIN_INTERVAL_SECONDS")
            params = {
                "query": candidate,
                "per_page": 1,
                "orientation": "portrait",  # For YouTube shorts
                "client_id": unsplash_api_key
            }
            response = REQUESTS_SESSION.get("https://api.unsplash.com/search/photos", params=params, timeout=20)

            if response.status_code != 200:
                logger.warning("Unsplash API error: %s for query '%s'", response.status_code, candidate)
                _backoff_after_status(
                    response.status_code,
                    "UNSPLASH_IMAGE_RATE_LIMIT_BACKOFF_SECONDS",
                    1.5,
                )
                continue

            data = response.json()
            if not data.get("results"):
                logger.info("Unsplash returned no results for query '%s'", candidate)
                continue

            image_url = data["results"][0]["urls"]["regular"]
            img_response = REQUESTS_SESSION.get(image_url, timeout=20)
            if img_response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(img_response.content)
                logger.info("Unsplash image downloaded to %s (query='%s')", file_path, candidate)
                return file_path

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
        headers = {"Authorization": pexels_api_key}
        _log_proxy_usage("Pexels")
        for candidate in _build_query_candidates(query):
            _throttle_provider("pexels_image", 1.1, "PEXELS_IMAGE_MIN_INTERVAL_SECONDS")
            params = {
                "query": candidate,
                "per_page": 1,
                "orientation": "portrait"  # For YouTube shorts
            }
            response = REQUESTS_SESSION.get(
                "https://api.pexels.com/v1/search",
                headers=headers,
                params=params,
                timeout=20,
            )

            if response.status_code != 200:
                logger.warning("Pexels API error: %s for query '%s'", response.status_code, candidate)
                _backoff_after_status(
                    response.status_code,
                    "PEXELS_IMAGE_RATE_LIMIT_BACKOFF_SECONDS",
                    2.0,
                )
                continue

            data = response.json()
            if not data.get("photos"):
                logger.info("Pexels returned no results for query '%s'", candidate)
                continue

            for photo in data["photos"][:5]:
                src = photo.get("src", {})
                image_url = (
                    src.get("portrait")
                    or src.get("large2x")
                    or src.get("original")
                    or src.get("large")
                )
                if not image_url:
                    continue

                img_response = REQUESTS_SESSION.get(image_url, timeout=20)
                if img_response.status_code == 200 and img_response.content:
                    with open(file_path, "wb") as f:
                        f.write(img_response.content)
                    logger.info("Pexels image downloaded to %s (query='%s')", file_path, candidate)
                    return file_path

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


def _build_processed_story_image(image_path):
    """
    Prepare a portrait-safe still frame that avoids ugly crops and low-res fullscreen stretching.
    """
    with Image.open(image_path) as raw_img:
        img = ImageOps.exif_transpose(raw_img).convert("RGB")
        width, height = img.size
        source_ratio = width / max(1, height)
        is_low_res = min(width, height) < 900
        needs_focus_layout = is_low_res or source_ratio > 0.8 or source_ratio < 0.45

        if not needs_focus_layout:
            return ImageOps.fit(img, resolution, method=Image.LANCZOS, centering=(0.5, 0.5))

        background = ImageOps.fit(img, resolution, method=Image.LANCZOS, centering=(0.5, 0.5))
        background = background.filter(ImageFilter.GaussianBlur(28))

        # Darken the blurred background slightly so the focused image sits cleanly on top.
        dim_overlay = Image.new("RGB", resolution, (12, 12, 18))
        background = Image.blend(background, dim_overlay, 0.26).convert("RGBA")

        foreground = img.convert("RGBA")
        max_foreground_size = (int(resolution[0] * 0.92), int(resolution[1] * 0.74))
        foreground.thumbnail(max_foreground_size, Image.LANCZOS)

        mask = Image.new("L", foreground.size, 0)
        mask_draw = ImageDraw.Draw(mask)
        mask_draw.rounded_rectangle(
            [(0, 0), (foreground.size[0] - 1, foreground.size[1] - 1)],
            radius=36,
            fill=255,
        )
        foreground.putalpha(mask)

        shadow = Image.new("RGBA", foreground.size, (0, 0, 0, 165)).filter(ImageFilter.GaussianBlur(20))
        x = (resolution[0] - foreground.width) // 2
        y = int((resolution[1] - foreground.height) * 0.45)

        background.paste(shadow, (x + 10, y + 16), shadow)
        background.paste(foreground, (x, y), foreground)
        return background.convert("RGB")


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
  processed_path = os.path.join(
      temp_dir,
      f"prepared_{int(time.time() * 1000)}_{random.randint(1000, 9999)}.jpg"
  )
  prepared = _build_processed_story_image(image_path)
  prepared.save(processed_path, quality=95)

  image = ImageClip(processed_path)

  # Add zoom effect if requested
  if with_zoom:
      def zoom(t):
          # Start at 1.0 zoom and gradually increase
          zoom_level = 1 + (t / max(duration, 0.1)) * min(zoom_factor, 0.03)
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
