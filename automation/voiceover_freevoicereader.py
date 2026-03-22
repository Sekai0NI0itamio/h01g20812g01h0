import logging
import os
import time

import requests

logger = logging.getLogger(__name__)


class FreeVoiceReaderVoiceover:
    """
    FreeVoiceReader client using the /api/free-tts multipart form endpoint.
    """

    def __init__(self, output_dir="temp"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.timeout = int(os.getenv("FREEVOICEREADER_TTS_TIMEOUT", "90"))
        self.voice = os.getenv("FREEVOICEREADER_TTS_VOICE", "en-TZ-ElimuNeural")
        self.api_url = os.getenv(
            "FREEVOICEREADER_TTS_API_URL",
            "https://www.freevoicereader.com/api/free-tts",
        ).strip()

    def _headers(self):
        headers = {
            "Accept": "*/*",
            "Origin": "https://www.freevoicereader.com",
            "Referer": "https://www.freevoicereader.com/",
            "User-Agent": "Mozilla/5.0",
        }
        return headers

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
                dl = requests.get(audio_url.strip(), timeout=self.timeout)
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

        response = requests.post(
            self.api_url,
            headers=self._headers(),
            files=files,
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code}: {response.text[:240]}")

        written = self._write_audio_payload(response, output_filename)
        if written and os.path.exists(written) and os.path.getsize(written) > 0:
            logger.info("FreeVoiceReader TTS synthesized successfully via %s", self.api_url)
            return written

        raise RuntimeError("FreeVoiceReader response did not contain usable audio payload")
