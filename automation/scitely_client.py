import os

import requests
from dotenv import load_dotenv
from helper.network import create_requests_session

load_dotenv()

REQUESTS_SESSION = create_requests_session()

DEFAULT_SCITELY_BASE_URL = "https://api.scitely.com/v1"
DEFAULT_SCITELY_MODEL = "deepseek-v3.2"


class ScitelyAPIError(RuntimeError):
    pass


def get_scitely_api_key():
    return os.getenv("SCITELY_API_KEY") or os.getenv("SCITELY_AUTH_TOKEN")


def get_scitely_base_url():
    return os.getenv("SCITELY_BASE_URL", DEFAULT_SCITELY_BASE_URL).rstrip("/")


def get_scitely_model(default=None):
    return os.getenv("SCITELY_MODEL", default or DEFAULT_SCITELY_MODEL)


def create_chat_completion(
    messages,
    model=None,
    max_tokens=None,
    temperature=0.7,
    response_format=None,
    stream=False,
    timeout=90,
):
    api_key = get_scitely_api_key()
    if not api_key:
        raise ValueError(
            "Scitely API key is not set. Please set SCITELY_API_KEY or SCITELY_AUTH_TOKEN in .env."
        )

    payload = {
        "model": model or get_scitely_model(),
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
        f"{get_scitely_base_url()}/chat/completions",
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
