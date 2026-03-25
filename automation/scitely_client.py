import json
import logging
import os
from typing import Any, Iterable, List

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


DEFAULT_G4F_MODEL = "gpt-4o"
DEFAULT_G4F_MODEL_FALLBACKS = (
    "gpt-4o",
    "chatgpt-4o-latest",
    "gpt-4o-mini",
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
)

FREE_MODEL_ERROR_MARKERS = (
    'add a "api_key"',
    "add an api_key",
    "api key",
    "api_key",
    "model does not exist",
    "model not found",
    "does not exist in",
    "invalid model",
    "unsupported model",
    "rate limit",
    "too many requests",
    "quota",
    "permission denied",
    "access denied",
    "authentication",
    "unauthorized",
    "invalid api key",
)

_G4F_CLIENT = None


class ScitelyAPIError(RuntimeError):
    def __init__(self, message, provider="unknown"):
        super().__init__(message)
        self.provider = provider


def get_scitely_api_key():
    return ""


def get_scitely_base_url():
    return ""


def get_scitely_model(default=None):
    return default or ""


def get_nvidia_api_key():
    return ""


def get_nvidia_base_url():
    return ""


def get_nvidia_model(default=None):
    return default or ""


def is_scitely_disabled():
    return True


def disable_scitely(reason=None):
    del reason
    return None


def get_g4f_model(default=None):
    value = (os.getenv("G4F_MODEL") or "").strip()
    return value or (default or DEFAULT_G4F_MODEL)


def is_g4f_available():
    return G4FClient is not None


def has_any_chat_provider():
    return bool(is_g4f_available())


def get_default_chat_provider():
    configured = (os.getenv("AI_PROVIDER") or "").strip().lower()
    if configured and configured != "g4f":
        logger.warning("Ignoring unsupported AI_PROVIDER=%s; using g4f only.", configured)
    return "g4f"


def get_preferred_chat_model(provider=None, default=None):
    del provider
    return get_g4f_model(default)


def _get_provider_order() -> List[str]:
    return ["g4f"]


def _is_probably_free_g4f_model(candidate_obj) -> bool:
    for attr in ("requires_api_key", "requires_auth", "needs_auth", "login_required"):
        if getattr(candidate_obj, attr, None) is True:
            return False

    provider_obj = getattr(candidate_obj, "best_provider", None)
    for attr in ("requires_api_key", "requires_auth", "needs_auth", "login_required"):
        if getattr(provider_obj, attr, None) is True:
            return False
    return True


def _collect_g4f_model_name(candidate) -> str:
    if isinstance(candidate, str):
        text = candidate.strip()
    else:
        text = str(getattr(candidate, "name", "") or "").strip()

    if not text:
        return ""
    if text.startswith("_") or len(text) > 120 or " " in text:
        return ""
    if text.lower() in {"model", "models", "provider", "providers"}:
        return ""
    return text


def _is_gpt_family_model_name(name: str) -> bool:
    lower = (name or "").strip().lower()
    if not lower:
        return False
    return lower.startswith("gpt-") or lower.startswith("chatgpt-")


def _discover_g4f_free_models():
    try:
        import g4f.models as g4f_models
    except Exception:
        return []

    discovered = []

    def add_candidate(candidate):
        if not _is_probably_free_g4f_model(candidate):
            return
        name = _collect_g4f_model_name(candidate)
        if name and name not in discovered:
            discovered.append(name)

    model_utils = getattr(g4f_models, "ModelUtils", None)
    convert = getattr(model_utils, "convert", None) if model_utils else None
    if isinstance(convert, dict):
        for key, value in convert.items():
            add_candidate(value)
            add_candidate(key)

    for _, value in vars(g4f_models).items():
        add_candidate(value)

    return discovered


def get_g4f_model_fallbacks():
    raw = (os.getenv("G4F_MODEL_FALLBACKS") or "").strip()
    configured = [item.strip() for item in raw.split(",") if item.strip()]
    discovered = [name for name in _discover_g4f_free_models() if _is_gpt_family_model_name(name)]

    candidates = [get_g4f_model()] + configured + list(DEFAULT_G4F_MODEL_FALLBACKS) + discovered
    deduped = []
    for candidate in candidates:
        name = _collect_g4f_model_name(candidate)
        if name and _is_gpt_family_model_name(name) and name not in deduped:
            deduped.append(name)
    return deduped


def _stringify_message_content(content: Any) -> str:
    if content is None:
        return ""

    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                text = item.strip()
                if text:
                    parts.append(text)
                continue

            if isinstance(item, dict):
                item_type = str(item.get("type") or "").strip().lower()
                if item_type == "text" and isinstance(item.get("text"), str):
                    text = item["text"].strip()
                    if text:
                        parts.append(text)
                    continue
                if item_type in {"input_text", "output_text"} and isinstance(item.get("text"), str):
                    text = item["text"].strip()
                    if text:
                        parts.append(text)
                    continue

            try:
                parts.append(json.dumps(item, ensure_ascii=False, sort_keys=True))
            except Exception:
                parts.append(str(item))
        return "\n".join(part for part in parts if part).strip()

    if isinstance(content, dict):
        try:
            return json.dumps(content, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(content).strip()

    return str(content).strip()


def _serialize_tool_calls(tool_calls: Any) -> str:
    if not tool_calls:
        return ""

    if not isinstance(tool_calls, list):
        try:
            return "Tool calls:\n" + json.dumps(tool_calls, ensure_ascii=False, sort_keys=True)
        except Exception:
            return f"Tool calls:\n{tool_calls}"

    rendered = []
    for call in tool_calls:
        if isinstance(call, dict):
            function_data = call.get("function") if isinstance(call.get("function"), dict) else {}
            name = function_data.get("name") or call.get("name") or "unknown_tool"
            arguments = function_data.get("arguments") or call.get("arguments") or ""
            if not isinstance(arguments, str):
                try:
                    arguments = json.dumps(arguments, ensure_ascii=False, sort_keys=True)
                except Exception:
                    arguments = str(arguments)
            rendered.append(f"- {name}: {arguments}".strip())
        else:
            rendered.append(f"- {call}")

    return "Tool calls:\n" + "\n".join(rendered)


def _normalize_g4f_messages(messages: Iterable[dict]):
    normalized = []

    for message in messages or []:
        if not isinstance(message, dict):
            text = str(message).strip()
            if text:
                normalized.append({"role": "user", "content": text})
            continue

        original_role = str(message.get("role") or "user").strip().lower()
        role = original_role if original_role in {"system", "user", "assistant"} else "user"

        parts = []
        if role != original_role and original_role:
            parts.append(f"[original role: {original_role}]")

        name = str(message.get("name") or "").strip()
        if name:
            parts.append(f"[name: {name}]")

        tool_call_id = str(message.get("tool_call_id") or "").strip()
        if tool_call_id:
            parts.append(f"[tool_call_id: {tool_call_id}]")

        tool_calls_text = _serialize_tool_calls(message.get("tool_calls"))
        if tool_calls_text:
            parts.append(tool_calls_text)

        function_call = message.get("function_call")
        if function_call:
            try:
                parts.append("Function call:\n" + json.dumps(function_call, ensure_ascii=False, sort_keys=True))
            except Exception:
                parts.append(f"Function call:\n{function_call}")

        content_text = _stringify_message_content(message.get("content"))
        if content_text:
            parts.append(content_text)

        if not parts:
            continue

        normalized.append({"role": role, "content": "\n\n".join(parts).strip()})

    if not normalized:
        normalized.append({"role": "user", "content": "Reply with exactly: OK"})

    return normalized


def _extract_completion_text(response):
    if response is None:
        return ""

    if isinstance(response, dict):
        choices = response.get("choices")
        if isinstance(choices, list) and choices:
            first = choices[0] if isinstance(choices[0], dict) else {}
            message = first.get("message") if isinstance(first, dict) else None
            if isinstance(message, dict):
                content = message.get("content")
                text = _stringify_message_content(content)
                if text:
                    return text
            text = first.get("text") if isinstance(first, dict) else None
            if isinstance(text, str) and text.strip():
                return text.strip()

        for key in ("content", "output_text", "text", "response"):
            value = response.get(key)
            text = _stringify_message_content(value)
            if text:
                return text

        return ""

    choices = getattr(response, "choices", None)
    if isinstance(choices, list) and choices:
        first = choices[0]
        message = getattr(first, "message", None)
        content = getattr(message, "content", None)
        text = _stringify_message_content(content)
        if text:
            return text
        text = getattr(first, "text", None)
        if isinstance(text, str) and text.strip():
            return text.strip()

    content = getattr(response, "content", None)
    text = _stringify_message_content(content)
    if text:
        return text

    text = str(response).strip()
    return text


def _coerce_completion_payload(response):
    if isinstance(response, dict):
        text = _extract_completion_text(response)
        if text and "choices" not in response:
            return {"choices": [{"message": {"role": "assistant", "content": text}}]}
        return response

    for method_name in ("model_dump", "dict"):
        method = getattr(response, method_name, None)
        if callable(method):
            try:
                payload = method(exclude_none=True)
                if isinstance(payload, dict):
                    text = _extract_completion_text(payload)
                    if text and "choices" not in payload:
                        return {"choices": [{"message": {"role": "assistant", "content": text}}]}
                    return payload
            except Exception:
                pass

    text = _extract_completion_text(response)
    if text:
        return {"choices": [{"message": {"role": "assistant", "content": text}}]}
    return {"content": str(response)}


def _looks_like_g4f_error_text(text: str) -> bool:
    if not text:
        return False
    lower = text.strip().lower()
    return any(marker in lower for marker in FREE_MODEL_ERROR_MARKERS)


def _get_g4f_client():
    global _G4F_CLIENT
    if G4FClient is None:
        raise ScitelyAPIError("g4f is not installed", provider="g4f")
    if _G4F_CLIENT is None:
        _G4F_CLIENT = G4FClient()
    return _G4F_CLIENT


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
    del max_tokens, temperature, response_format, stream, timeout

    normalized_messages = _normalize_g4f_messages(messages)
    requested_model = _collect_g4f_model_name(model) if model else ""
    candidate_models = ([requested_model] if requested_model else []) + get_g4f_model_fallbacks()

    deduped_models = []
    for candidate in candidate_models:
        if candidate and candidate not in deduped_models:
            deduped_models.append(candidate)

    provider = (provider or get_default_chat_provider()).strip().lower()
    if provider not in {"auto", "g4f"}:
        raise ValueError(f"Unsupported provider: {provider}. Only g4f is supported.")

    last_exc = None
    client = _get_g4f_client()

    for candidate_model in deduped_models:
        try:
            response = client.chat.completions.create(
                model=candidate_model,
                messages=normalized_messages,
            )
            payload = _coerce_completion_payload(response)
            text = _extract_completion_text(payload)
            if not text:
                raise ScitelyAPIError(
                    f"g4f returned an empty response for model {candidate_model}",
                    provider="g4f",
                )
            if _looks_like_g4f_error_text(text):
                raise ScitelyAPIError(
                    f"g4f returned provider error text for model {candidate_model}: {text}",
                    provider="g4f",
                )
            return payload
        except Exception as exc:
            last_exc = exc
            logger.warning("g4f chat completion failed with model %s: %s", candidate_model, exc)
            continue

    raise ScitelyAPIError(
        f"g4f request failed across candidate models {deduped_models}: {last_exc}",
        provider="g4f",
    ) from last_exc


def select_working_provider_for_run():
    test_messages = [{"role": "user", "content": "Reply with exactly: OK"}]
    logger.info("Startup AI provider probe order: %s", _get_provider_order())

    response = create_chat_completion(
        messages=test_messages,
        provider="g4f",
        timeout=30,
    )
    text = _extract_completion_text(response)
    if not text:
        raise RuntimeError("No AI provider passed startup probe: g4f returned an empty response")

    os.environ["AI_PROVIDER"] = "g4f"
    os.environ["AI_PROVIDER_ORDER"] = "g4f"
    logger.info("Selected AI provider for this run: g4f")
    return "g4f"
