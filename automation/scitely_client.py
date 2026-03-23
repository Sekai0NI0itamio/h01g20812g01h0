import os
import logging
from typing import List

import requests
from dotenv import load_dotenv
from helper.network import create_requests_session

try:
    from g4f.client import Client as G4FClient
except Exception:
    G4FClient = None

load_dotenv()

AI_USE_TOR_TUNNEL = os.getenv("AI_USE_TOR_TUNNEL", "false").strip().lower() == "true"
REQUESTS_SESSION = create_requests_session(use_tor=AI_USE_TOR_TUNNEL)
logger = logging.getLogger(__name__)

if not AI_USE_TOR_TUNNEL:
    logger.info("AI provider requests will bypass Tor tunnel")

DEFAULT_SCITELY_BASE_URL = "https://api.scitely.com/v1"
DEFAULT_SCITELY_MODEL = "deepseek-v3.2"
DEFAULT_NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_MODEL = "nvidia/nemotron-3-nano-30b-a3b"
DEFAULT_G4F_MODEL = "gemini-2.5-pro"
DEFAULT_G4F_MODEL_FALLBACKS = (
    "gpt-4.1",
    "gpt-4o",
    "deepseek-v3-0324",
    "gpt-4o-mini",
)

_SCITELY_DISABLED = False
_G4F_CLIENT = None


class ScitelyAPIError(RuntimeError):
    def __init__(self, message, provider="unknown"):
        super().__init__(message)
        self.provider = provider


def get_scitely_api_key():
    return os.getenv("SCITELY_API_KEY") or os.getenv("SCITELY_AUTH_TOKEN")


def get_scitely_base_url():
    value = (os.getenv("SCITELY_BASE_URL") or "").strip()
    return (value or DEFAULT_SCITELY_BASE_URL).rstrip("/")


def get_scitely_model(default=None):
    value = (os.getenv("SCITELY_MODEL") or "").strip()
    return value or (default or DEFAULT_SCITELY_MODEL)


def get_nvidia_api_key():
    return os.getenv("NVIDIA_API_KEY")


def get_nvidia_base_url():
    value = (os.getenv("NVIDIA_BASE_URL") or "").strip()
    return (value or DEFAULT_NVIDIA_BASE_URL).rstrip("/")


def get_nvidia_model(default=None):
    value = (os.getenv("NVIDIA_MODEL") or "").strip()
    return value or (default or DEFAULT_NVIDIA_MODEL)


def get_g4f_model(default=None):
    value = (os.getenv("G4F_MODEL") or "").strip()
    return value or (default or DEFAULT_G4F_MODEL)


def get_g4f_model_fallbacks():
    raw = (os.getenv("G4F_MODEL_FALLBACKS") or "").strip()
    configured = [item.strip() for item in raw.split(",") if item.strip()]
    candidates = [get_g4f_model()] + configured + list(DEFAULT_G4F_MODEL_FALLBACKS)
    deduped = []
    for candidate in candidates:
        if candidate and candidate not in deduped:
            deduped.append(candidate)
    return deduped


def is_g4f_available():
    return G4FClient is not None


def has_any_chat_provider():
    return bool(get_scitely_api_key() or get_nvidia_api_key() or is_g4f_available())


def get_preferred_chat_model(provider=None, default=None):
    candidate = (provider or get_default_chat_provider() or "g4f").strip().lower()
    if candidate == "auto":
        provider_order = _get_provider_order()
        candidate = provider_order[0] if provider_order else "g4f"
    if candidate == "nvidia":
        return get_nvidia_model(default)
    if candidate == "g4f":
        return get_g4f_model(default)
    return get_scitely_model(default)


def get_default_chat_provider():
    provider = os.getenv("AI_PROVIDER", "").strip().lower()
    if provider in {"scitely", "nvidia", "g4f"}:
        return provider

    return "g4f"


def _get_provider_order() -> List[str]:
    explicit_order = os.getenv("AI_PROVIDER_ORDER", "").strip()
    if explicit_order:
        parsed = [item.strip().lower() for item in explicit_order.split(",") if item.strip()]
        order = [item for item in parsed if item in {"scitely", "nvidia", "g4f"}]
        if order:
            return order

    default_provider = get_default_chat_provider()
    fallback_priority = [default_provider, "g4f", "nvidia", "scitely"]

    order = []
    for candidate in fallback_priority:
        if candidate in {"scitely", "nvidia", "g4f"} and candidate not in order:
            order.append(candidate)

    return order or ["g4f", "nvidia", "scitely"]


def _extract_completion_text(response):
    if isinstance(response, dict):
        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            message = first.get("message") if isinstance(first, dict) else None
            if isinstance(message, dict):
                content = message.get("content")
                if isinstance(content, str) and content.strip():
                    return content.strip()
        for key in ("content", "output_text", "text", "response"):
            value = response.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _get_g4f_client():
    global _G4F_CLIENT
    if G4FClient is None:
        raise ValueError("g4f is not installed")
    if _G4F_CLIENT is None:
        _G4F_CLIENT = G4FClient()
    return _G4F_CLIENT


def is_scitely_disabled():
    return _SCITELY_DISABLED


def disable_scitely(reason=None):
    global _SCITELY_DISABLED
    _SCITELY_DISABLED = True
    if reason:
        logger.warning("Scitely disabled for the remainder of this run: %s", reason)
    else:
        logger.warning("Scitely disabled for the remainder of this run")


def _post_chat_completion(base_url, api_key, model, messages, max_tokens, temperature, response_format, stream, timeout, provider_name):
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

    try:
        response = REQUESTS_SESSION.post(
            f"{base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise ScitelyAPIError(
            f"{provider_name} request error calling {base_url}/chat/completions: {exc}",
            provider=provider_name,
        ) from exc

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

        request_id = (
            response.headers.get("x-request-id")
            or response.headers.get("x-correlation-id")
            or response.headers.get("nvidia-request-id")
            or ""
        )
        body_preview = (response.text or "").strip().replace("\n", " ")[:800]
        detail = (
            f"provider={provider_name}; "
            f"http_status={response.status_code}; "
            f"url={response.url}; "
            f"model={model}; "
            f"request_id={request_id or 'n/a'}; "
            f"message={message}; "
            f"body_preview={body_preview or 'n/a'}"
        )
        raise ScitelyAPIError(detail, provider=provider_name)

    return response.json()


def create_chat_completion(
    messages,
    model=None,
    max_tokens=None,
    temperature=0.7,
    response_format=None,
    stream=False,
    timeout=90,
    provider="auto",
):
    global _SCITELY_DISABLED

    scitely_api_key = get_scitely_api_key()
    nvidia_api_key = get_nvidia_api_key()

    scitely_error = None

    provider = (provider or get_default_chat_provider()).strip().lower()
    if provider not in {"auto", "scitely", "nvidia", "g4f"}:
        raise ValueError(f"Unsupported provider: {provider}")

    def call_nvidia():
        if not nvidia_api_key:
            raise ValueError("NVIDIA_API_KEY is not set")
        # NVIDIA Integrate API is OpenAI-compatible, but this project keeps
        # requests minimal to avoid host-specific param mismatches.
        return _post_chat_completion(
            base_url=get_nvidia_base_url(),
            api_key=nvidia_api_key,
            model=model if provider == "nvidia" and model else get_nvidia_model(),
            messages=messages,
            max_tokens=None,
            temperature=None,
            response_format=None,
            stream=stream,
            timeout=timeout,
            provider_name="nvidia",
        )

    def call_g4f():
        last_exc = None
        requested_model = (model or "").strip()
        candidate_models = [requested_model] if requested_model else get_g4f_model_fallbacks()

        for candidate_model in candidate_models:
            try:
                response = _get_g4f_client().chat.completions.create(
                    model=candidate_model,
                    messages=messages,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning("g4f chat completion failed with model %s: %s", candidate_model, exc)
                continue

            if hasattr(response, "model_dump"):
                try:
                    return response.model_dump(exclude_none=True)
                except Exception:
                    pass
            if hasattr(response, "dict"):
                try:
                    return response.dict(exclude_none=True)
                except Exception:
                    pass
            if isinstance(response, dict):
                return response
            return {"content": str(response)}

        raise ScitelyAPIError(
            f"g4f request failed across candidate models {candidate_models}: {last_exc}",
            provider="g4f",
        ) from last_exc

    def call_scitely():
        if not scitely_api_key:
            raise ValueError("Scitely API key is not set")
        return _post_chat_completion(
            base_url=get_scitely_base_url(),
            api_key=scitely_api_key,
            model=model if provider == "scitely" and model else get_scitely_model(),
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            response_format=response_format,
            stream=stream,
            timeout=timeout,
            provider_name="scitely",
        )

    if provider == "nvidia":
        return call_nvidia()

    if provider == "g4f":
        return call_g4f()

    if provider == "scitely":
        try:
            return call_scitely()
        except ScitelyAPIError as exc:
            disable_scitely(exc)
            raise

    for candidate in _get_provider_order():
        if candidate == "scitely":
            if not scitely_api_key or _SCITELY_DISABLED:
                continue
            try:
                return call_scitely()
            except ScitelyAPIError as exc:
                scitely_error = exc
                disable_scitely(exc)
                logger.warning("Scitely chat completion failed; trying next provider: %s", exc)
                continue

        if candidate == "nvidia":
            if not nvidia_api_key:
                continue
            try:
                return call_nvidia()
            except ScitelyAPIError as exc:
                logger.warning("NVIDIA chat completion failed; trying next provider: %s", exc)
                scitely_error = exc
                continue

        if candidate == "g4f":
            try:
                return call_g4f()
            except ScitelyAPIError as exc:
                logger.warning("g4f chat completion failed; trying next provider: %s", exc)
                scitely_error = exc
                continue

    if scitely_error:
        raise scitely_error

    raise ValueError(
        "No working AI provider is configured. Install g4f or explicitly configure Scitely/NVIDIA."
    )


def select_working_provider_for_run():
    test_messages = [{"role": "user", "content": "Reply with exactly: OK"}]
    errors = []
    provider_order = _get_provider_order()

    logger.info("Startup AI provider probe order: %s", provider_order)

    for provider in provider_order:
        try:
            response = create_chat_completion(
                messages=test_messages,
                provider=provider,
                timeout=30,
            )
            text = _extract_completion_text(response)
            if text:
                os.environ["AI_PROVIDER"] = provider
                logger.info("Selected AI provider for this run: %s", provider)
                return provider
            errors.append(f"{provider}: empty response")
        except Exception as exc:
            errors.append(f"{provider}: {exc}")

    raise RuntimeError(
        "No AI provider passed startup probe. " + " | ".join(errors)
    )
