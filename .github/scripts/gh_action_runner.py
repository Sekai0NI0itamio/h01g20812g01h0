#!/usr/bin/env python3
"""
Helper script for GitHub Actions to prepare credentials and run the project.

It writes provided base64-encoded secrets into files where the project expects them
and then runs `python main.py`.

Environment variables the workflow should set (examples):
- GCP_SA_KEY_B64: base64 of Google service account JSON (optional)
- YOUTUBE_CLIENT_JSON_B64: base64 of OAuth client_secrets JSON (optional)
- TOKEN_PICKLE_B64: base64 of token.pickle (optional, binary)
- ENABLE_YOUTUBE_UPLOAD: "true" or "false"

Usage: python .github/scripts/gh_action_runner.py --upload true
"""
import os
import sys
import argparse
import base64
from pathlib import Path
import subprocess


def write_base64_to_file(env_name: str, out_path: Path, binary: bool = False) -> bool:
    data = os.environ.get(env_name)
    if not data:
        return False
    try:
        decoded = base64.b64decode(data)
        mode = 'wb' if binary else 'w'
        with open(out_path, mode) as f:
            if binary:
                f.write(decoded)
            else:
                f.write(decoded.decode('utf-8'))
        print(f"Wrote {env_name} -> {out_path}")
        return True
    except Exception as e:
        print(f"Failed to write {env_name} to {out_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--upload', choices=['true', 'false'], default=os.environ.get('ENABLE_YOUTUBE_UPLOAD', 'false'))
    parser.add_argument('--count', default=os.environ.get('SHORTS_VIDEO_COUNT', '1'))
    parser.add_argument('--topic-direction', default=os.environ.get('SHORTS_TOPIC_DIRECTION', ''))
    parser.add_argument('--story-text', default=os.environ.get('SHORTS_SOURCE_STORY', ''))
    parser.add_argument('--main-video-mode', choices=['yes-main', 'no-main'], default=os.environ.get('SHORTS_MAIN_VIDEO_MODE', 'yes-main'))
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]

    # Prepare credentials directory expected by the project
    cred_dir = repo_root / 'automation' / 'credentials'
    cred_dir.mkdir(parents=True, exist_ok=True)

    # 1) Optionally write Google Application Credentials (service account) for secret manager usage
    sa_written = False
    if os.environ.get('GCP_SA_KEY_B64'):
        sa_path = repo_root / 'automation' / 'gcp_service_account.json'
        if write_base64_to_file('GCP_SA_KEY_B64', sa_path, binary=True):
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(sa_path)
            sa_written = True

    # 2) Optionally write YouTube OAuth client secrets
    # Supports either raw JSON in YOUTUBE_CLIENT_JSON env (not recommended) or base64 version
    youtube_written = False
    if os.environ.get('YOUTUBE_CLIENT_JSON_B64'):
        client_path = cred_dir / 'client_secret.json'
        if write_base64_to_file('YOUTUBE_CLIENT_JSON_B64', client_path, binary=False):
            youtube_written = True
    elif os.environ.get('YOUTUBE_CLIENT_JSON'):
        # raw JSON
        client_path = cred_dir / 'client_secret.json'
        try:
            with open(client_path, 'w') as f:
                f.write(os.environ.get('YOUTUBE_CLIENT_JSON'))
            youtube_written = True
            print(f"Wrote YOUTUBE_CLIENT_JSON -> {client_path}")
        except Exception as e:
            print(f"Failed to write YOUTUBE_CLIENT_JSON: {e}")

    # 3) Optionally write token.pickle (binary) to avoid interactive auth in CI
    if os.environ.get('TOKEN_PICKLE_B64'):
        token_path = cred_dir / 'token.pickle'
        write_base64_to_file('TOKEN_PICKLE_B64', token_path, binary=True)

    # 4) Set ENABLE_YOUTUBE_UPLOAD env var according to arg
    os.environ['ENABLE_YOUTUBE_UPLOAD'] = args.upload
    os.environ.setdefault('SHORTS_RUNTIME_MODE', 'github_actions')
    os.environ.setdefault('SHORTS_VIDEO_ONLY', 'true')
    os.environ.setdefault('SHORTS_CREATOR_MODE', 'video')
    os.environ['SHORTS_MAIN_VIDEO_MODE'] = args.main_video_mode
    os.environ.setdefault('SHORTS_ALLOW_LOCAL_DEV', 'false')
    os.environ.setdefault('USE_C05_LOCAL_KEYS', 'false')
    print(f"ENABLE_YOUTUBE_UPLOAD={args.upload}")

    # 5) Run the staged Actions pipeline
    try:
        cmd = [
            sys.executable,
            '-m',
            'automation.actions_pipeline',
            'run-batch',
            '--count',
            str(args.count),
            '--topic-direction',
            args.topic_direction,
            '--story-text',
            args.story_text,
            '--artifacts-dir',
            'workflow_artifacts',
        ]
        print(f"Running: {' '.join(cmd)}")
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        print(f"Pipeline failed with exit code {e.returncode}")
        raise

    # 6) Optionally upload the produced artifacts
    if args.upload == 'true':
        upload_cmd = [
            sys.executable,
            '-m',
            'automation.upload_artifacts_to_youtube',
            '--artifacts-dir',
            'workflow_artifacts',
        ]
        print(f"Running: {' '.join(upload_cmd)}")
        subprocess.check_call(upload_cmd)


if __name__ == '__main__':
    main()
