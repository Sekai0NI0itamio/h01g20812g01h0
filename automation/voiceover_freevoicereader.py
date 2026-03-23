import logging
import os
import re
import time

import requests
from helper.network import create_requests_session

logger = logging.getLogger(__name__)


def _env_bool_or_default(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


FREEVOICEREADER_USE_TOR = _env_bool_or_default("FREEVOICEREADER_USE_TOR_TUNNEL", False)
REQUESTS_SESSION = create_requests_session(use_tor=FREEVOICEREADER_USE_TOR)


def _env_or_default(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def _env_int_or_default(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %s", name, value, default)
        return default


class FreeVoiceReaderVoiceover:
    """
    FreeVoiceReader client using the /api/free-tts multipart form endpoint.
    """

    def __init__(self, output_dir="temp"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.timeout = _env_int_or_default("FREEVOICEREADER_TTS_TIMEOUT", 90)
        self.request_retries = max(1, _env_int_or_default("FREEVOICEREADER_TTS_REQUEST_RETRIES", 3))
        self.retry_backoff_seconds = float(_env_or_default("FREEVOICEREADER_TTS_RETRY_BACKOFF_SECONDS", "1.0"))
        self.voice = _env_or_default("FREEVOICEREADER_TTS_VOICE", "en-US-AndrewNeural")
        self.api_url = _env_or_default(
            "FREEVOICEREADER_TTS_API_URL",
            "https://www.freevoicereader.com/api/free-tts",
        )
        self.user_agent = _env_or_default(
            "FREEVOICEREADER_TTS_USER_AGENT",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        )
        self.cookie_header = os.getenv("FREEVOICEREADER_TTS_COOKIE", "").strip()
        self.cookie_token = os.getenv("FREEVOICEREADER_TTS_COOKIE_TOKEN", "").strip()
        self.cookie_name = _env_or_default("FREEVOICEREADER_TTS_COOKIE_NAME", "token")

        if self.cookie_header:
            logger.info("FreeVoiceReader custom Cookie header configured via FREEVOICEREADER_TTS_COOKIE")
        elif self.cookie_token:
            logger.info("FreeVoiceReader cookie token configured via FREEVOICEREADER_TTS_COOKIE_TOKEN")
        else:
            logger.warning("FreeVoiceReader cookie token is not configured; API may return HTTP 403")

        if FREEVOICEREADER_USE_TOR:
            logger.info("FreeVoiceReader requests are routed through Tor tunnel")
        else:
            logger.info("FreeVoiceReader requests bypass Tor tunnel")

    def _headers(self):
        headers = {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://www.freevoicereader.com",
            "Priority": "u=1, i",
            "Referer": "https://www.freevoicereader.com/",
            "Sec-CH-UA": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": self.user_agent,
        }
        if self.cookie_header:
            headers["Cookie"] = self.cookie_header
        elif self.cookie_token:
            headers["Cookie"] = f"{self.cookie_name}={self.cookie_token}"
        return headers

    def _safe_error_preview(self, response_text):
        text = (response_text or "").strip()
        if not text:
            return ""

        # Strip token-like segments to avoid leaking secrets in logs.
        text = re.sub(r"\b\d{8,}-[A-Za-z0-9]{10,}\b", "[redacted-token]", text)
        text = re.sub(r"\b[A-Za-z0-9_\-]{24,}\b", "[redacted]", text)
        return text[:240]

    def _ensure_wav_path(self, output_filename):
        if not output_filename:
            output_filename = os.path.join(
                self.output_dir,
                f"freevoicereader_tts_{int(time.time())}.wav",
            )
        base, _ = os.path.splitext(output_filename)
        return f"{base}.wav"

    def _title_from_output_filename(self, output_filename):
        base = os.path.splitext(os.path.basename(output_filename))[0]
        cleaned = "".join(ch if ch.isalnum() or ch in ("_", "-") else "_" for ch in base).strip("_")
        return cleaned or f"tts_{int(time.time())}"

    def _write_audio_payload(self, response, output_filename):
        content_type = (response.headers.get("content-type") or "").lower()
        if response.content and "application/json" not in content_type:
            with open(output_filename, "wb") as f:
                f.write(response.content)
            return output_filename

        try:
            data = response.json()
        except Exception:
            return None

        if not isinstance(data, dict):
            return None

        for key in ("audio_url", "audioUrl", "url", "file_url", "fileUrl"):
            audio_url = data.get(key)
            if isinstance(audio_url, str) and audio_url.strip():
                dl = REQUESTS_SESSION.get(audio_url.strip(), timeout=self.timeout)
                dl.raise_for_status()
                with open(output_filename, "wb") as f:
                    f.write(dl.content)
                return output_filename

        return None

    def generate_speech(self, text, output_filename=None, voice_style=None):
        del voice_style  # narration is currently forced to male in audio helper

        text = (text or "").strip()
        if not text:
            text = "No text provided."

        output_filename = self._ensure_wav_path(output_filename)

        files = {
            "text": (None, text),
            "voice": (None, self.voice),
            "title": (None, self._title_from_output_filename(output_filename)),
        }
        if self.cookie_token:
            # Some FreeVoiceReader deployments validate token in form-data in addition to cookie.
            files["token"] = (None, self.cookie_token)

        last_error = None
        for attempt in range(1, self.request_retries + 1):
            try:
                response = REQUESTS_SESSION.post(
                    self.api_url,
                    headers=self._headers(),
                    files=files,
                    timeout=self.timeout,
                )

                if response.status_code >= 400:
                    safe_preview = self._safe_error_preview(response.text)
                    if safe_preview:
                        raise RuntimeError(f"HTTP {response.status_code}: {safe_preview}")
                    raise RuntimeError(f"HTTP {response.status_code}")

                written = self._write_audio_payload(response, output_filename)
                if written and os.path.exists(written) and os.path.getsize(written) > 0:
                    logger.info("FreeVoiceReader TTS synthesized successfully via %s", self.api_url)
                    return written

                raise RuntimeError("FreeVoiceReader response did not contain usable audio payload")
            except Exception as exc:
                last_error = exc
                if attempt < self.request_retries:
                    logger.warning(
                        "FreeVoiceReader request failed (attempt %s/%s): %s",
                        attempt,
                        self.request_retries,
                        exc,
                    )
                    time.sleep(self.retry_backoff_seconds * attempt)
                    continue

        raise RuntimeError(str(last_error or "FreeVoiceReader TTS request failed"))
