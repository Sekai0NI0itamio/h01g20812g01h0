#!/usr/bin/env python3
"""
Standalone CLI for g4f image generation.

Examples:
  python3 g4f_image_generator.py generate \
      --prompt "A neon samurai standing in the rain, cinematic lighting"

  python3 g4f_image_generator.py variation \
      --prompt "Turn this into a cyberpunk movie poster" \
      --image AnimeCharacters/1.png

  python3 g4f_image_generator.py variation \
      --prompt "Remove the background and add a sunset sky" \
      --image https://example.com/input.jpg \
      --provider PollinationsAI \
      --model kontext
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

DEFAULT_OUTPUT_DIR = Path("output/g4f_images")
TEXT_TO_IMAGE_DEFAULT_PROVIDER = "PollinationsAI"
TEXT_TO_IMAGE_DEFAULT_MODEL = "flux"
REMOTE_VARIATION_DEFAULT_PROVIDER = "PollinationsAI"
REMOTE_VARIATION_DEFAULT_MODEL = "kontext"
LOCAL_VARIATION_DEFAULT_PROVIDER = "HuggingSpace"
LOCAL_VARIATION_DEFAULT_MODEL = "flux-kontext-dev"
SUPPORTED_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate images with g4f from text alone or from text plus a reference image."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate one or more images from a text prompt.",
    )
    add_common_arguments(generate_parser, default_prefix="generated")
    generate_parser.add_argument("--prompt", required=True, help="Text prompt for the image.")
    generate_parser.add_argument(
        "--width",
        type=int,
        default=None,
        help="Optional output width for providers that support it.",
    )
    generate_parser.add_argument(
        "--height",
        type=int,
        default=None,
        help="Optional output height for providers that support it.",
    )

    variation_parser = subparsers.add_parser(
        "variation",
        help="Generate an image from a prompt plus a reference image.",
    )
    add_common_arguments(variation_parser, default_prefix="variation")
    variation_parser.add_argument("--prompt", required=True, help="Edit or guidance prompt.")
    variation_parser.add_argument(
        "--image",
        required=True,
        help="Local image path or remote image URL.",
    )

    return parser


def add_common_arguments(parser: argparse.ArgumentParser, default_prefix: str) -> None:
    parser.add_argument(
        "--provider",
        default="",
        help="Optional g4f provider name, for example PollinationsAI or HuggingSpace.",
    )
    parser.add_argument(
        "--model",
        default="",
        help="Optional model override. If omitted, the script chooses a default that matches the mode.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of images to request.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Directory for saved images. Default: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--output-prefix",
        default=default_prefix,
        help="Prefix for output file names.",
    )
    parser.add_argument(
        "--proxy",
        default="",
        help="Optional proxy URL forwarded to g4f.",
    )


def load_g4f():
    try:
        from g4f.client import AsyncClient
        import g4f.Provider as provider_module
    except Exception as exc:
        raise RuntimeError(
            "g4f is not installed or failed to import. Install dependencies first, for example: pip install g4f requests python-dotenv"
        ) from exc
    return AsyncClient, provider_module


def looks_like_url(value: str) -> bool:
    parsed = urlparse(str(value))
    return parsed.scheme in {"http", "https"}


def resolve_provider(provider_name: str, provider_module: Any) -> Any:
    if not provider_name:
        return None
    provider = getattr(provider_module, provider_name, None)
    if provider is None:
        raise ValueError(f"Unknown g4f provider: {provider_name}")
    return provider


def resolve_generate_defaults(args: argparse.Namespace) -> tuple[str, str]:
    provider_name = args.provider or TEXT_TO_IMAGE_DEFAULT_PROVIDER
    model_name = args.model or TEXT_TO_IMAGE_DEFAULT_MODEL
    return provider_name, model_name


def resolve_variation_defaults(args: argparse.Namespace) -> tuple[str, str]:
    if args.provider and args.model:
        return args.provider, args.model
    if looks_like_url(args.image):
        provider_name = args.provider or REMOTE_VARIATION_DEFAULT_PROVIDER
        model_name = args.model or REMOTE_VARIATION_DEFAULT_MODEL
        return provider_name, model_name
    provider_name = args.provider or LOCAL_VARIATION_DEFAULT_PROVIDER
    model_name = args.model or LOCAL_VARIATION_DEFAULT_MODEL
    return provider_name, model_name


def coerce_image_input(image_value: str) -> str | Path:
    if looks_like_url(image_value):
        return image_value

    image_path = Path(image_value).expanduser().resolve()
    if not image_path.exists():
        raise FileNotFoundError(f"Image file not found: {image_path}")
    return image_path


def get_response_data(response: Any) -> Iterable[Any]:
    data = getattr(response, "data", None)
    if data is not None:
        return data
    if isinstance(response, dict):
        return response.get("data") or []
    if hasattr(response, "model_dump"):
        payload = response.model_dump(exclude_none=True)
        return payload.get("data") or []
    return []


def get_item_value(item: Any, key: str) -> Any:
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key, None)


def extension_for_item(item: Any, default: str = ".png") -> str:
    url = get_item_value(item, "url")
    if isinstance(url, str):
        parsed = urlparse(url)
        suffix = Path(parsed.path).suffix.lower()
        if suffix in SUPPORTED_SUFFIXES:
            return suffix
    return default


def save_data_url(data_url: str, destination: Path) -> None:
    _, encoded = data_url.split(",", 1)
    destination.write_bytes(base64.b64decode(encoded))


def save_from_url(url: str, destination: Path) -> None:
    parsed = urlparse(url)

    if url.startswith("data:"):
        save_data_url(url, destination)
        return

    if parsed.scheme in {"http", "https"}:
        response = requests.get(url, timeout=180)
        response.raise_for_status()
        destination.write_bytes(response.content)
        return

    if parsed.scheme == "file":
        source = Path(unquote(parsed.path))
        shutil.copy2(source, destination)
        return

    source = Path(url).expanduser()
    if source.exists():
        shutil.copy2(source, destination)
        return

    raise RuntimeError(f"Could not save image from response URL: {url}")


def save_image_item(item: Any, destination: Path) -> None:
    b64_json = get_item_value(item, "b64_json")
    if isinstance(b64_json, str) and b64_json.strip():
        try:
            destination.write_bytes(base64.b64decode(b64_json))
            return
        except binascii.Error as exc:
            raise RuntimeError("Invalid base64 payload returned by g4f.") from exc

    url = get_item_value(item, "url")
    if isinstance(url, str) and url.strip():
        save_from_url(url, destination)
        return

    raise RuntimeError("g4f returned an image item without b64_json or url.")


def build_output_path(output_dir: Path, prefix: str, index: int, item: Any) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = extension_for_item(item)
    return output_dir / f"{prefix}_{timestamp}_{index:02d}{suffix}"


async def generate_from_text(args: argparse.Namespace) -> tuple[list[Path], str, str, str | None]:
    AsyncClient, provider_module = load_g4f()
    provider_name, model_name = resolve_generate_defaults(args)
    provider = resolve_provider(provider_name, provider_module)
    client = AsyncClient()

    request_kwargs = {
        "prompt": args.prompt,
        "model": model_name,
        "provider": provider,
        "response_format": "b64_json",
        "n": args.count,
    }
    if args.width:
        request_kwargs["width"] = args.width
    if args.height:
        request_kwargs["height"] = args.height
    if args.proxy:
        request_kwargs["proxy"] = args.proxy

    response = await client.images.generate(**request_kwargs)
    return save_response_images(
        response=response,
        output_dir=Path(args.output_dir),
        prefix=args.output_prefix,
        provider_name=provider_name,
        model_name=model_name,
    )


async def generate_from_text_and_image(
    args: argparse.Namespace,
) -> tuple[list[Path], str, str, str | None]:
    AsyncClient, provider_module = load_g4f()
    provider_name, model_name = resolve_variation_defaults(args)
    provider = resolve_provider(provider_name, provider_module)
    client = AsyncClient()

    request_kwargs = {
        "image": coerce_image_input(args.image),
        "prompt": args.prompt,
        "model": model_name,
        "provider": provider,
        "response_format": "b64_json",
        "n": args.count,
    }
    if args.proxy:
        request_kwargs["proxy"] = args.proxy

    response = await client.images.create_variation(**request_kwargs)
    return save_response_images(
        response=response,
        output_dir=Path(args.output_dir),
        prefix=args.output_prefix,
        provider_name=provider_name,
        model_name=model_name,
    )


def save_response_images(
    *,
    response: Any,
    output_dir: Path,
    prefix: str,
    provider_name: str,
    model_name: str,
) -> tuple[list[Path], str, str, str | None]:
    output_dir.mkdir(parents=True, exist_ok=True)
    items = list(get_response_data(response))
    if not items:
        raise RuntimeError("g4f returned no images.")

    saved_paths: list[Path] = []
    revised_prompt = None

    for index, item in enumerate(items, start=1):
        if revised_prompt is None:
            revised_prompt = get_item_value(item, "revised_prompt")
        destination = build_output_path(output_dir, prefix, index, item)
        save_image_item(item, destination)
        saved_paths.append(destination.resolve())

    return saved_paths, provider_name, model_name, revised_prompt


async def run(args: argparse.Namespace) -> int:
    if args.count < 1:
        raise ValueError("--count must be at least 1")

    if args.command == "generate":
        saved_paths, provider_name, model_name, revised_prompt = await generate_from_text(args)
    elif args.command == "variation":
        saved_paths, provider_name, model_name, revised_prompt = await generate_from_text_and_image(args)
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(f"provider: {provider_name}")
    print(f"model: {model_name}")
    if revised_prompt:
        print(f"revised_prompt: {revised_prompt}")
    print("saved_files:")
    for path in saved_paths:
        print(path)

    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
