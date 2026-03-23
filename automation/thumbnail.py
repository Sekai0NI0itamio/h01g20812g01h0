import asyncio
import base64
import binascii
import logging
import os
import random
import requests
import textwrap
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps
from dotenv import load_dotenv

# Custom helpers
from helper.minor_helper import cleanup_temp_directories
from helper.network import create_requests_session
from helper.shorts_assets import get_default_font_path
from helper.image import fetch_best_image_for_prompt, fetch_image_from_duckduckgo, get_unsplash_api_key

# Configure logging
logger = logging.getLogger(__name__)
REQUESTS_SESSION = create_requests_session()


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


def _wrap_text_to_width(draw, text, font, max_width):
    words = [word for word in str(text or "").split() if word]
    if not words:
        return ""

    lines = []
    current_line = words[0]
    for word in words[1:]:
        candidate = f"{current_line} {word}"
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if (bbox[2] - bbox[0]) <= max_width:
            current_line = candidate
        else:
            lines.append(current_line)
            current_line = word
    lines.append(current_line)
    return "\n".join(lines)

# Timer function for performance monitoring
def measure_time(func):
    """Decorator to measure the execution time of functions"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_datetime = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        logger.info(f"STARTING {func.__name__} at {start_datetime}")
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"COMPLETED {func.__name__} in {duration:.2f} seconds")
        return result
    return wrapper

# Get temp directory from environment variable or use default
TEMP_DIR = os.getenv("TEMP_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp"))
# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

class ThumbnailGenerator:
    def __init__(self, output_dir="output"):
        """
        Initialize the thumbnail generator with necessary settings

        Args:
            output_dir (str): Directory to save output thumbnails
        """
        # Load environment variables
        load_dotenv()

        # Setup directories
        self.output_dir = output_dir
        self.temp_dir = os.path.join(TEMP_DIR, f"thumbnail_{int(time.time())}")
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)

        # Font settings
        self.fonts_dir = os.path.join(os.path.dirname(__file__), 'fonts')
        self.title_font_path = get_default_font_path()

        # Unsplash API (for fallback)
        self.unsplash_api_key = get_unsplash_api_key()
        self.unsplash_api_url = "https://api.unsplash.com/search/photos"
        self.pexels_api_key = os.getenv("PEXELS_API_KEY")
        self.pixabay_api_key = os.getenv("PIXABAY_API_KEY")

        # Thumbnail settings
        self.thumbnail_size = (1080, 1920)  # YouTube Shorts recommended size (9:16 aspect ratio)

    @measure_time
    def fetch_image_pexels(self, query, file_path=None):
        if not file_path:
            file_path = os.path.join(self.temp_dir, f"thumbnail_pexels_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

        if not self.pexels_api_key:
            return None

        try:
            url = "https://api.pexels.com/v1/search"
            headers = {"Authorization": self.pexels_api_key}
            params = {"query": query, "per_page": 20, "orientation": "portrait"}
            response = REQUESTS_SESSION.get(url, headers=headers, params=params, timeout=10)
            if response.status_code != 200:
                return None

            photos = response.json().get("photos", [])
            if not photos:
                return None

            photo = random.choice(photos[: min(10, len(photos))])
            image_url = photo.get("src", {}).get("large2x") or photo.get("src", {}).get("large")
            if not image_url:
                return None

            img_response = REQUESTS_SESSION.get(image_url, timeout=10)
            if img_response.status_code != 200:
                return None

            with open(file_path, "wb") as f:
                f.write(img_response.content)
            logger.info(f"Pexels image downloaded to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error fetching image from Pexels: {e}")
            return None

    @measure_time
    def fetch_image_pixabay(self, query, file_path=None):
        if not file_path:
            file_path = os.path.join(self.temp_dir, f"thumbnail_pixabay_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

        if not self.pixabay_api_key:
            return None

        try:
            url = "https://pixabay.com/api/"
            params = {
                "key": self.pixabay_api_key,
                "q": query,
                "image_type": "photo",
                "per_page": 30,
                "safesearch": "true",
                "orientation": "vertical",
            }
            response = REQUESTS_SESSION.get(url, params=params, timeout=10)
            if response.status_code != 200:
                return None

            hits = response.json().get("hits", [])
            if not hits:
                return None

            image = random.choice(hits[: min(10, len(hits))])
            image_url = image.get("largeImageURL") or image.get("webformatURL")
            if not image_url:
                return None

            img_response = REQUESTS_SESSION.get(image_url, timeout=10)
            if img_response.status_code != 200:
                return None

            with open(file_path, "wb") as f:
                f.write(img_response.content)
            logger.info(f"Pixabay image downloaded to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"Error fetching image from Pixabay: {e}")
            return None

    @measure_time
    def fetch_image_unsplash(self, query, file_path=None):
        """
        Fetch an image from Unsplash API based on query

        Args:
            query (str): Search query for Unsplash
            file_path (str): Path to save the image, if None a path will be generated

        Returns:
            str: Path to the downloaded image or None if failed
        """
        if not file_path:
            file_path = os.path.join(self.temp_dir, f"thumbnail_unsplash_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

        # Check if Unsplash API key is available
        if not self.unsplash_api_key:
            logger.error("No Unsplash API key provided.")
            return None

        try:
            # Clean query for Unsplash search
            clean_query = query.replace("eye-catching", "").replace("thumbnail", "").replace("YouTube Shorts", "")
            # Remove any double spaces
            while "  " in clean_query:
                clean_query = clean_query.replace("  ", " ")
            clean_query = clean_query.strip(" ,")

            logger.info(f"Searching Unsplash with query: {clean_query}")

            # Make request to Unsplash API
            params = {
                "query": clean_query,
                "orientation": "landscape",
                "per_page": 30,
                "client_id": self.unsplash_api_key
            }

            response = REQUESTS_SESSION.get(self.unsplash_api_url, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()

                # Check if we have results
                if data["results"] and len(data["results"]) > 0:
                    # Pick a random image from top results for variety
                    max_index = min(10, len(data["results"]))
                    image_data = random.choice(data["results"][:max_index])
                    image_url = image_data["urls"]["regular"]

                    # Download the image
                    img_response = REQUESTS_SESSION.get(image_url, timeout=10)
                    if img_response.status_code == 200:
                        with open(file_path, "wb") as f:
                            f.write(img_response.content)
                        logger.info(f"Unsplash image downloaded to {file_path}")

                        # Add attribution as required by Unsplash API guidelines
                        attribution = f"Photo by {image_data['user']['name']} on Unsplash"
                        logger.info(f"Image attribution: {attribution}")

                        return file_path
                    else:
                        logger.error(f"Failed to download image from Unsplash: {img_response.status_code}")
                else:
                    logger.error("No results found on Unsplash")
            else:
                logger.error(f"Unsplash API error: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Error fetching image from Unsplash: {e}")

        return None

    def fetch_stock_background_image(self, query):
        """Fetch background image from stock providers (Pexels/Pixabay/Unsplash)."""
        clean_query = (query or "technology news background").strip()
        return (
            fetch_image_from_duckduckgo(clean_query)
            or self.fetch_image_pexels(clean_query)
            or self.fetch_image_pixabay(clean_query)
            or self.fetch_image_unsplash(clean_query)
        )

    def fetch_anime_character_image(self):
        """Pick a random local anime character image from AnimeCharacters folder."""
        project_root = os.path.dirname(os.path.dirname(__file__))
        anime_dir = os.path.join(project_root, "AnimeCharacters")
        if not os.path.isdir(anime_dir):
            logger.warning("AnimeCharacters directory not found at %s", anime_dir)
            return None

        valid_exts = (".png", ".jpg", ".jpeg", ".webp")
        candidates = []
        for name in os.listdir(anime_dir):
            path = os.path.join(anime_dir, name)
            if os.path.isfile(path) and name.lower().endswith(valid_exts):
                candidates.append(path)

        if not candidates:
            logger.warning("No anime character images found in %s", anime_dir)
            return None

        selected = random.choice(candidates)
        logger.info("Selected anime character image: %s", os.path.basename(selected))
        return selected

    def generate_thumbnail_query(self, title, script_sections=None):
        """Build a thumbnail scene description when the content package did not provide one."""
        scene_bits = []
        for section in script_sections or []:
            if not isinstance(section, dict):
                continue
            text = str(section.get("text", "")).strip()
            if text:
                scene_bits.append(text.rstrip("."))
            if len(scene_bits) >= 3:
                break

        context = ". ".join(scene_bits)
        title = str(title or "").strip()
        prompt_parts = [part for part in [title, context] if part]
        if not prompt_parts:
            return "dramatic anime thumbnail scene with a strong emotional reaction"
        return ". ".join(prompt_parts)

    def build_thumbnail_meme_query(self, title, prompt=None):
        combined = f"{title or ''} {prompt or ''}".lower()
        if any(keyword in combined for keyword in ["angry", "furious", "slam", "fight", "war", "chaos"]):
            return "dramatic phone chat screenshot argument meme with unread texts"
        if any(keyword in combined for keyword in ["side eye", "awkward", "embarrass", "cringe"]):
            return "awkward group chat screenshot meme with side eye energy"
        if any(keyword in combined for keyword in ["cute", "crush", "beauty", "flirt", "blush"]):
            return "flirty text message screenshot meme with blush reaction"
        if any(keyword in combined for keyword in ["confused", "wonder", "comment", "think"]):
            return "confused chat history screenshot meme with reaction messages"
        return "funny chat history screenshot meme conversation on phone"

    def fetch_thumbnail_meme_image(self, title, prompt=None):
        meme_query = self.build_thumbnail_meme_query(title, prompt)
        try:
            meme_path = fetch_best_image_for_prompt(
                meme_query,
                style="mobile phone chat history screenshot, texting app UI, meme-style reaction image",
                allow_ai_fallback=True,
            )
            if meme_path:
                logger.info("Fetched thumbnail meme image for query: %s", meme_query)
                return meme_path
        except Exception as exc:
            logger.warning("Failed to fetch thumbnail meme image for '%s': %s", meme_query, exc)
        return None

    def build_g4f_thumbnail_prompt(self, prompt, style="photorealistic", has_reference_image=True):
        """Convert the AI thumbnail description into a generation-ready image prompt."""
        clean_prompt = " ".join(str(prompt or "").split()).strip()
        if not clean_prompt:
            clean_prompt = "dramatic anime thumbnail scene"

        style_text = str(style or "anime illustration").strip()
        character_instruction = (
            "Use the anime girl from the reference image as the clear main character in the foreground."
            if has_reference_image
            else "Make an expressive anime girl the clear main character in the foreground."
        )
        return (
            f"{clean_prompt}. Create a YouTube Shorts thumbnail illustration with an anime visual identity and a "
            f"polished {style_text} finish. {character_instruction} Strong facial emotion. Dynamic pose. "
            "Bold cinematic lighting. Clean focal point. Rich contrast. Highly readable composition. Vertical 9:16. "
            "No text. No title. No captions. No speech bubbles. No logo. No watermark."
        )

    def _save_resized_thumbnail(self, source_path, output_path):
        """Normalize generated art to the project thumbnail size and save it."""
        with Image.open(source_path) as img:
            if img.mode in ("RGBA", "LA"):
                background = Image.new("RGBA", img.size, (18, 18, 18, 255))
                background.alpha_composite(img.convert("RGBA"))
                img = background.convert("RGB")
            else:
                img = img.convert("RGB")

            fitted = ImageOps.fit(img, self.thumbnail_size, method=Image.LANCZOS, centering=(0.5, 0.5))
            fitted.save(output_path, quality=95)

        logger.info("Saved normalized thumbnail image to %s", output_path)
        return output_path

    def create_thumbnail_image_only(self, image_path, output_path=None, anime_image_path=None):
        """Create an image-only thumbnail fallback with no text overlays."""
        if not output_path:
            output_path = os.path.join(self.output_dir, f"thumbnail_{int(time.time())}.jpg")

        logger.info("Creating image-only thumbnail with base image: %s", image_path)

        try:
            img = Image.open(image_path).convert("RGBA")
            img = ImageOps.fit(img, self.thumbnail_size, method=Image.LANCZOS, centering=(0.5, 0.5))

            overlay = Image.new("RGBA", self.thumbnail_size, (0, 0, 0, 0))
            overlay_draw = ImageDraw.Draw(overlay)
            for y in range(self.thumbnail_size[1]):
                alpha = int(85 * y / max(1, self.thumbnail_size[1] - 1))
                overlay_draw.line([(0, y), (self.thumbnail_size[0], y)], fill=(0, 0, 0, alpha))
            img = Image.alpha_composite(img, overlay)

            if anime_image_path and os.path.exists(anime_image_path):
                try:
                    anime_img = Image.open(anime_image_path).convert("RGBA")
                    target_h = int(self.thumbnail_size[1] * 0.62)
                    ratio = target_h / max(1, anime_img.height)
                    target_w = int(anime_img.width * ratio)
                    anime_img = anime_img.resize((target_w, target_h), Image.LANCZOS)

                    mask = Image.new("L", anime_img.size, 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.rounded_rectangle(
                        [(0, 0), (anime_img.size[0] - 1, anime_img.size[1] - 1)],
                        radius=28,
                        fill=255,
                    )
                    anime_img.putalpha(mask)

                    x = self.thumbnail_size[0] - anime_img.width - 30
                    y = self.thumbnail_size[1] - anime_img.height - 120
                    shadow = Image.new("RGBA", anime_img.size, (0, 0, 0, 145)).filter(ImageFilter.GaussianBlur(12))
                    img.paste(shadow, (x + 10, y + 12), shadow)
                    img.paste(anime_img, (x, y), anime_img)
                    logger.info("Added anime girl overlay to image-only thumbnail fallback")
                except Exception as exc:
                    logger.warning("Failed to add anime image to fallback thumbnail: %s", exc)

            img.convert("RGB").save(output_path, quality=95)
            logger.info("Image-only thumbnail saved to %s", output_path)
            return output_path
        except Exception as exc:
            logger.error("Error creating image-only thumbnail: %s", exc)
            return None

    async def _generate_thumbnail_with_g4f(self, prompt, anime_image_path=None, style="photorealistic"):
        """Generate thumbnail artwork with g4f, using the anime image as a reference when available."""
        AsyncClient, provider_module = _load_g4f()
        client = AsyncClient()
        has_reference_image = bool(anime_image_path and os.path.exists(anime_image_path))
        final_prompt = self.build_g4f_thumbnail_prompt(
            prompt,
            style=style,
            has_reference_image=has_reference_image,
        )

        if has_reference_image:
            provider_name = os.getenv("G4F_THUMBNAIL_VARIATION_PROVIDER", "HuggingSpace")
            model_name = os.getenv("G4F_THUMBNAIL_VARIATION_MODEL", "flux-kontext-dev")
            provider = _resolve_g4f_provider(provider_name, provider_module)
            response = await client.images.create_variation(
                image=Path(anime_image_path).expanduser().resolve(),
                prompt=final_prompt,
                model=model_name,
                provider=provider,
                response_format="b64_json",
                n=1,
            )
        else:
            provider_name = os.getenv("G4F_THUMBNAIL_TEXT_PROVIDER", "PollinationsAI")
            model_name = os.getenv("G4F_THUMBNAIL_TEXT_MODEL", "flux")
            provider = _resolve_g4f_provider(provider_name, provider_module)
            response = await client.images.generate(
                prompt=final_prompt,
                model=model_name,
                provider=provider,
                response_format="b64_json",
                n=1,
            )

        items = _get_g4f_response_items(response)
        if not items:
            raise RuntimeError("g4f returned no thumbnail images.")

        raw_output_path = os.path.join(
            self.temp_dir,
            f"g4f_thumbnail_raw_{int(time.time())}_{random.randint(1000, 9999)}.png",
        )
        _save_g4f_image_item(items[0], Path(raw_output_path))
        revised_prompt = _get_g4f_item_value(items[0], "revised_prompt")
        logger.info(
            "Generated thumbnail artwork with g4f provider=%s model=%s revised_prompt=%s",
            provider_name,
            model_name,
            bool(revised_prompt),
        )
        return raw_output_path

    @measure_time
    def create_thumbnail(self, title, image_path, output_path=None, anime_image_path=None, meme_image_path=None):
        """
        Create a thumbnail with text overlay using the given image

        Args:
            title (str): Title text to overlay on the thumbnail
            image_path (str): Path to the base image
            output_path (str): Path to save the final thumbnail

        Returns:
            str: Path to the created thumbnail
        """
        if not output_path:
            output_path = os.path.join(self.output_dir, f"thumbnail_{int(time.time())}.jpg")

        logger.info(f"Creating thumbnail for title: '{title}'")
        logger.info(f"Using base image: {image_path}")
        logger.info(f"Output path: {output_path}")

        try:
            # Open the image
            img = Image.open(image_path)
            logger.info(f"Base image size: {img.size}")

            # Normalize to YouTube thumbnail dimensions.
            img = ImageOps.fit(img.convert("RGBA"), self.thumbnail_size, method=Image.LANCZOS, centering=(0.5, 0.5))
            logger.info(f"Normalized image to YouTube thumbnail dimensions: {self.thumbnail_size}")

            # Create layered gradients for readability without placing a solid panel behind the title.
            overlay = Image.new('RGBA', self.thumbnail_size, (0, 0, 0, 0))
            overlay_draw = ImageDraw.Draw(overlay)
            top_fade_height = int(self.thumbnail_size[1] * 0.16)
            for y in range(top_fade_height):
                alpha = int(48 * (1 - (y / max(1, top_fade_height))))
                overlay_draw.line([(0, y), (self.thumbnail_size[0], y)], fill=(0, 0, 0, alpha))
            bottom_start = int(self.thumbnail_size[1] * 0.72)
            for y in range(bottom_start, self.thumbnail_size[1]):
                alpha = int(150 * ((y - bottom_start) / max(1, self.thumbnail_size[1] - bottom_start)))
                overlay_draw.line([(0, y), (self.thumbnail_size[0], y)], fill=(0, 0, 0, alpha))

            # Composite the image with the overlay
            img = Image.alpha_composite(img, overlay)
            logger.info("Added gradient overlay to thumbnail")

            # Add meme image at the bottom as a chat-style screenshot card.
            if meme_image_path and os.path.exists(meme_image_path):
                try:
                    meme_img = Image.open(meme_image_path).convert("RGBA")
                    max_size = (int(self.thumbnail_size[0] * 0.54), int(self.thumbnail_size[1] * 0.25))
                    meme_img.thumbnail(max_size, Image.LANCZOS)

                    mask = Image.new("L", meme_img.size, 0)
                    mask_draw = ImageDraw.Draw(mask)
                    mask_draw.rounded_rectangle(
                        [(0, 0), (meme_img.size[0] - 1, meme_img.size[1] - 1)],
                        radius=26,
                        fill=255,
                    )
                    meme_img.putalpha(mask)

                    border = Image.new("RGBA", (meme_img.width + 10, meme_img.height + 10), (0, 0, 0, 0))
                    border_mask = Image.new("L", border.size, 0)
                    border_draw = ImageDraw.Draw(border_mask)
                    border_draw.rounded_rectangle(
                        [(0, 0), (border.size[0] - 1, border.size[1] - 1)],
                        radius=30,
                        fill=255,
                    )
                    border.paste((255, 255, 255, 240), (0, 0), border_mask)
                    border.paste(meme_img, (5, 5), meme_img)

                    x = (self.thumbnail_size[0] - border.width) // 2
                    y = self.thumbnail_size[1] - border.height - 72
                    shadow = Image.new("RGBA", border.size, (0, 0, 0, 155)).filter(ImageFilter.GaussianBlur(14))
                    img.paste(shadow, (x + 8, y + 10), shadow)
                    img.paste(border, (x, y), border)
                    logger.info("Added meme overlay to thumbnail")
                except Exception as e:
                    logger.warning(f"Failed to overlay meme image: {e}")

            # Add title text.
            draw = ImageDraw.Draw(img)

            # Try to load a large font and step down until it fits.
            try:
                max_text_width = int(self.thumbnail_size[0] * 0.84)
                font_size = 122 if len(title) < 28 else 108 if len(title) < 42 else 94 if len(title) < 58 else 80
                wrapped_text = None
                font = None
                while font_size >= 58:
                    font = ImageFont.truetype(self.title_font_path, font_size)
                    candidate_text = _wrap_text_to_width(draw, title, font, max_text_width)
                    bbox = draw.multiline_textbbox((0, 0), candidate_text, font=font, spacing=10, align="center")
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                    if text_width <= max_text_width and text_height <= int(self.thumbnail_size[1] * 0.2):
                        wrapped_text = candidate_text
                        break
                    font_size -= 6
                if wrapped_text is None:
                    wrapped_text = textwrap.fill(title, width=18)
                    bbox = draw.multiline_textbbox((0, 0), wrapped_text, font=font, spacing=10, align="center")
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                logger.info(f"Selected title font size: {font_size}")
            except Exception as e:
                font = ImageFont.load_default()
                wrapped_text = textwrap.fill(title, width=18)
                bbox = draw.multiline_textbbox((0, 0), wrapped_text, font=font, spacing=8, align="center")
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]
                logger.warning(f"Using default font as custom font could not be loaded: {e}")

            text_x = (self.thumbnail_size[0] - text_width) // 2
            text_y = max(90, int(self.thumbnail_size[1] * 0.46) - (text_height // 2))
            logger.info(f"Text position calculated: x={text_x}, y={text_y}, width={text_width}, height={text_height}")

            draw.multiline_text(
                (text_x, text_y),
                wrapped_text,
                font=font,
                fill=(255, 255, 255, 255),
                align="center",
                spacing=10,
                stroke_width=10,
                stroke_fill=(0, 0, 0, 255),
            )
            logger.info("Added main title text")

            # Convert back to RGB for saving as JPG
            img = img.convert("RGB")

            # Save the final thumbnail with high quality
            img.save(output_path, quality=95)
            logger.info(f"Thumbnail created and saved to {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error creating thumbnail: {e}")
            import traceback
            logger.error(f"Thumbnail creation traceback: {traceback.format_exc()}")
            return None

    @measure_time
    def generate_thumbnail(self, title, script_sections=None, prompt=None, style="photorealistic", output_path=None):
        """
        Main function to generate a finished thumbnail for a YouTube Short.

        Args:
            title (str): Title of the short
            script_sections (list): List of script sections for context
            prompt (str): AI-generated thumbnail scene description
            style (str): Visual style for the image prompt
            output_path (str): Path to save the final thumbnail

        Returns:
            str: Path to the generated thumbnail
        """
        if not output_path:
            timestamp = int(time.time())
            output_filename = f"thumbnail_{timestamp}.jpg"
            output_path = os.path.join(self.output_dir, output_filename)

        if not prompt:
            prompt = self.generate_thumbnail_query(title, script_sections)

        logger.info("Using AI thumbnail description: %s", prompt)
        anime_image_path = self.fetch_anime_character_image()
        meme_image_path = self.fetch_thumbnail_meme_image(title, prompt)
        base_art_path = os.path.join(self.temp_dir, f"thumbnail_base_{int(time.time())}_{random.randint(1000, 9999)}.jpg")

        try:
            g4f_image_path = asyncio.run(
                self._generate_thumbnail_with_g4f(
                    prompt=prompt,
                    anime_image_path=anime_image_path,
                    style=style,
                )
            )
            self._save_resized_thumbnail(g4f_image_path, base_art_path)
            logger.info("Successfully generated g4f thumbnail art at: %s", base_art_path)
        except Exception as exc:
            logger.warning("g4f thumbnail generation failed, falling back to image-only composition: %s", exc)
            image_path = self.fetch_stock_background_image(prompt)
            if not image_path:
                logger.warning("Stock background fetch failed. Creating fallback background.")
                temp_path = os.path.join(self.temp_dir, f"fallback_bg_{int(time.time())}.jpg")
                Image.new("RGB", self.thumbnail_size, color=(33, 33, 33)).save(temp_path, quality=95)
                image_path = temp_path

            self.create_thumbnail_image_only(
                image_path=image_path,
                output_path=base_art_path,
                anime_image_path=anime_image_path,
            )

        thumbnail_path = self.create_thumbnail(
            title=title,
            image_path=base_art_path,
            output_path=output_path,
            meme_image_path=meme_image_path,
        )

        if thumbnail_path:
            logger.info("Successfully generated final thumbnail at: %s", thumbnail_path)
            return thumbnail_path

        logger.error("Failed to generate thumbnail")
        return None

    def cleanup(self):
        """Clean up temporary files"""
        from helper.minor_helper import cleanup_temp_directories

        if hasattr(self, 'temp_dir') and self.temp_dir:
            logger.info(f"Cleaning up thumbnail temporary directory: {self.temp_dir}")
            cleanup_temp_directories(specific_dir=self.temp_dir)


# Simple test function
def test_thumbnail_generator():
    generator = ThumbnailGenerator(output_dir="output/thumbnails")
    title = "How AI is Revolutionizing Healthcare"
    script_sections = [
        {"text": "AI is transforming how doctors diagnose diseases with unprecedented accuracy.", "duration": 5},
        {"text": "Machine learning algorithms can now detect patterns that human doctors might miss.", "duration": 5},
        {"text": "This technology is already saving lives in hospitals around the world.", "duration": 5}
    ]

    thumbnail_path = generator.generate_thumbnail(
        title=title,
        script_sections=script_sections,
        style="photorealistic"
    )

    print(f"Thumbnail generated at: {thumbnail_path}")
    generator.cleanup()


if __name__ == "__main__":
    # Set up basic logging for stand-alone testing
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    )

    # Run test
    test_thumbnail_generator()
