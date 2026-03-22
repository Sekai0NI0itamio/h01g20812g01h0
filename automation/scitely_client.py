import os
import logging

import requests
from dotenv import load_dotenv
from helper.network import create_requests_session

load_dotenv()

REQUESTS_SESSION = create_requests_session()
logger = logging.getLogger(__name__)

DEFAULT_SCITELY_BASE_URL = "https://api.scitely.com/v1"
DEFAULT_SCITELY_MODEL = "deepseek-v3.2"
DEFAULT_NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "nvidia/nemotron-3-nano-30b-a3b"

_SCITELY_DISABLED = False


class ScitelyAPIError(RuntimeError):
    pass


def get_scitely_api_key():
    return os.getenv("SCITELY_API_KEY") or os.getenv("SCITELY_AUTH_TOKEN")


def get_scitely_base_url():
    return os.getenv("SCITELY_BASE_URL", DEFAULT_SCITELY_BASE_URL).rstrip("/")


def get_scitely_model(default=None):
    return os.getenv("SCITELY_MODEL", default or DEFAULT_SCITELY_MODEL)


def get_nvidia_api_key():
    return os.getenv("NVIDIA_API_KEY")


def get_nvidia_base_url():
    return os.getenv("NVIDIA_BASE_URL", DEFAULT_NVIDIA_BASE_URL).rstrip("/")


def get_nvidia_model(default=None):
    return os.getenv("NVIDIA_MODEL", default or DEFAULT_NVIDIA_MODEL)


def _post_chat_completion(base_url, api_key, model, messages, max_tokens, temperature, response_format, stream, timeout):
    payload = {
        "model": model,
        "messages": messages,
        "stream": stream,
    }
    if max_tokens is not None:
        payload["max_tokens"] = max_tokens
    if temperature is not None:
        payload["temperature"] = temperature
    if response_format is not None:
        payload["response_format"] = response_format

    response = REQUESTS_SESSION.post(
        f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )

    if not response.ok:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = {}

        message = (
            error_payload.get("error", {}).get("message")
            or error_payload.get("message")
            or response.text
            or f"HTTP {response.status_code}"
        )
        raise ScitelyAPIError(message)

    return response.json()


def create_chat_completion(
    messages,
    model=None,
    max_tokens=None,
    temperature=0.7,
    response_format=None,
    stream=False,
    timeout=90,
):
    global _SCITELY_DISABLED

    scitely_api_key = get_scitely_api_key()
    nvidia_api_key = get_nvidia_api_key()

    scitely_error = None

    if scitely_api_key and not _SCITELY_DISABLED:
        try:
            return _post_chat_completion(
                base_url=get_scitely_base_url(),
                api_key=scitely_api_key,
                model=model or get_scitely_model(),
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                response_format=response_format,
                stream=stream,
                timeout=timeout,
            )
        except ScitelyAPIError as exc:
            scitely_error = exc
            _SCITELY_DISABLED = True
            if nvidia_api_key:
                logger.warning("Scitely chat completion failed; switching to NVIDIA for subsequent requests: %s", exc)
            else:
                logger.warning("Scitely chat completion failed and no NVIDIA key is configured: %s", exc)

    if nvidia_api_key:
        return _post_chat_completion(
            base_url=get_nvidia_base_url(),
            api_key=nvidia_api_key,
            model=get_nvidia_model(),
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format=response_format,
            stream=stream,
            timeout=timeout,
        )

    if scitely_error:
        raise scitely_error

    raise ValueError(
        "No chat API key is configured. Set SCITELY_API_KEY/SCITELY_AUTH_TOKEN or NVIDIA_API_KEY in .env."
    )
