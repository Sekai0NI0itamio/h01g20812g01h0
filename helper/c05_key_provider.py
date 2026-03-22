import os
import logging

import requests

logger = logging.getLogger(__name__)

DEFAULT_C05_BASE_URL = "http://127.0.0.1:8129"

HOSTER_ENV_MAP = {
    "scitely": "SCITELY_API_KEY",
    "openrouter": "OPENROUTER_API_KEY",
    "pexels": "PEXELS_API_KEY",
    "unsplash": "UNSPLASH_API_KEY",
    "pixabay": "PIXABAY_API_KEY",
}


def get_c05_base_url():
    return os.getenv("C05_BASE_URL", DEFAULT_C05_BASE_URL).rstrip("/")


def request_api_key(
    hoster,
    require_usable=True,
    allow_fallback_to_any=False,
    timeout=10,
    exclude_keys=None,
):
    try:
        response = requests.post(
            f"{get_c05_base_url()}/RequestApiKey",
            json={
                "hoster": hoster,
                "require_usable": require_usable,
                "allow_fallback_to_any": allow_fallback_to_any,
                "mask_key": False,
                "exclude_keys": exclude_keys or [],
            },
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to reach C05 key provider at {get_c05_base_url()} for hoster '{hoster}': {exc}"
        ) from exc

    if not response.ok:
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        detail = payload.get("detail") or response.text or f"HTTP {response.status_code}"
        raise RuntimeError(f"C05 key request failed for hoster '{hoster}': {detail}")

    payload = response.json()
    key = payload.get("key")
    if not key:
        raise RuntimeError(f"C05 key request for hoster '{hoster}' succeeded without returning a key.")
    return key


def configure_provider_keys_from_c05(hosters=None):
    configured = {}
    requested_hosters = hosters or (
        "scitely",
        "openrouter",
        "pexels",
        "unsplash",
        "pixabay",
    )

    logger.info("Requesting provider keys from C05 at %s", get_c05_base_url())

    for hoster in requested_hosters:
        env_var = HOSTER_ENV_MAP.get(hoster)
        if not env_var:
            continue

        key = request_api_key(hoster)
        os.environ[env_var] = key
        configured[hoster] = env_var
        logger.info("Configured %s API key from C05 into %s", hoster, env_var)

    return configured
