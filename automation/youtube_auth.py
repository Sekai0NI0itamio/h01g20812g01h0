import os
import json
import pickle
import sys
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Load environment variables
load_dotenv()

# Set up paths for credential files
CREDENTIALS_DIR = os.path.join(os.path.dirname(__file__), "credentials")
os.makedirs(CREDENTIALS_DIR, exist_ok=True)

CLIENT_SECRETS_FILE = os.getenv("YOUTUBE_CLIENT_SECRET_PATH", os.path.join(CREDENTIALS_DIR, "client_secret.json"))
TOKEN_FILE = os.getenv("YOUTUBE_TOKEN_PATH", os.path.join(CREDENTIALS_DIR, "token.pickle"))

if not os.path.exists(CLIENT_SECRETS_FILE):
    try:
        from helper.secrets import Secrets

        secrets = Secrets()
        youtube_creds_path = secrets.write_temp_credentials(CREDENTIALS_DIR)
        if youtube_creds_path:
            CLIENT_SECRETS_FILE = youtube_creds_path
    except Exception as secret_error:
        print(f"⚠️ Secret Manager credentials bootstrap unavailable: {secret_error}")

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube.readonly"
]


def _load_token_credentials(token_path):
    if not os.path.exists(token_path):
        return None

    lower = token_path.lower()
    if lower.endswith(".json"):
        return Credentials.from_authorized_user_file(token_path, SCOPES)

    # Try pickle first for backward compatibility.
    try:
        with open(token_path, "rb") as token_file:
            return pickle.load(token_file)
    except Exception:
        pass

    # Fallback: token path may be .pickle but content could be JSON.
    try:
        with open(token_path, "r", encoding="utf-8") as token_file:
            payload = json.load(token_file)
        return Credentials.from_authorized_user_info(payload, SCOPES)
    except Exception:
        return None


def _save_token_credentials(credentials, token_path):
    lower = token_path.lower()
    if lower.endswith(".json"):
        with open(token_path, "w", encoding="utf-8") as token_file:
            token_file.write(credentials.to_json())
        return

    with open(token_path, "wb") as token_file:
        pickle.dump(credentials, token_file)


def _is_non_interactive_environment():
    return os.getenv("GITHUB_ACTIONS", "false").lower() == "true" or not sys.stdin.isatty()

def authenticate_youtube():
    credentials = None

    # Load existing credentials if available
    if os.path.exists(TOKEN_FILE):
        try:
            credentials = _load_token_credentials(TOKEN_FILE)
            print("📂 Loaded credentials from credentials directory")
        except Exception as e:
            print(f"⚠️ Error loading token file: {e}")
            credentials = None

    # Check if credentials are invalid or expired
    if not credentials or not credentials.valid:
        try:
            if credentials and credentials.expired and credentials.refresh_token:
                # Refresh the token if possible
                credentials.refresh(Request())
                print("🔄 Token refreshed successfully!")
            else:
                # Check if client secrets file exists
                if not os.path.exists(CLIENT_SECRETS_FILE):
                    raise FileNotFoundError(f"Client secrets file not found at: {CLIENT_SECRETS_FILE}")

                if _is_non_interactive_environment():
                    raise RuntimeError(
                        "Interactive OAuth is not available in this environment. "
                        "Provide a valid YouTube token secret (JSON token content)."
                    )

                # Prompt user for re-authentication if no valid refresh token
                print("🔑 Token expired or invalid. Re-authenticating...")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                credentials = flow.run_local_server(port=8080)
        except Exception as e:
            print(f"❌ Error during token refresh or authentication: {e}")
            if os.path.exists(CLIENT_SECRETS_FILE):
                if _is_non_interactive_environment():
                    raise
                print("⚠️ Re-authenticating...")
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
                credentials = flow.run_local_server(port=8080)
            else:
                raise FileNotFoundError(f"Client secrets file not found at: {CLIENT_SECRETS_FILE}")

        # Save the new or refreshed credentials
        try:
            _save_token_credentials(credentials, TOKEN_FILE)
            print("💾 Token saved successfully in credentials directory!")
        except Exception as e:
            print(f"⚠️ Error saving token file: {e}")

    print("✅ Authentication successful!")
    return credentials
