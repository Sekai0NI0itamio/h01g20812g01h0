#!/usr/bin/env python3
"""
Dispatch the workflow with a FreeVoiceReader cookie captured from Google Chrome.

Behavior:
- Attempts to read document.cookie from an open Chrome tab whose URL contains
  "freevoicereader.com" using AppleScript (macOS). This requires granting
  Automation permission to Terminal/Python when prompted.
- If a cookie string is found it will be attached to the workflow input
  `freevoicereader_cookie` and the workflow will be dispatched with upload enabled.

Usage examples:
  python3 ActionsRun_with_cookie.py
  python3 ActionsRun_with_cookie.py --token $GITHUB_TOKEN
  python3 ActionsRun_with_cookie.py --token $GITHUB_TOKEN --story-file redit_story.txt
  python3 ActionsRun_with_cookie.py --dry-run

Note: This script uses only stdlib modules and requires macOS + Google Chrome.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
from urllib.parse import quote, urlencode


def _read_story_text(path):
    if not path:
        return ""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Story file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def get_cookie_from_chrome(domain_substring="freevoicereader.com"):
    """Try to read cookies directly from Chrome storage using `browser_cookie3`.
    Falls back to the AppleScript approach if `browser_cookie3` isn't available.
    Returns the raw cookie string (e.g. "k1=v1; k2=v2") or None.
    """
    try:
        import browser_cookie3
    except Exception:
        browser_cookie3 = None

    if browser_cookie3:
        try:
            jar = browser_cookie3.chrome(domain_name=domain_substring)
            pairs = []
            for c in jar:
                try:
                    pairs.append(f"{c.name}={c.value}")
                except Exception:
                    continue
            if pairs:
                return "; ".join(pairs)
        except Exception:
            pass

    # Fallback: attempt to read document.cookie from an open Chrome tab (requires Automation permission)
    applescript = r'''
tell application "Google Chrome"
    repeat with w in windows
        repeat with t in tabs of w
            set u to URL of t
            if u contains "%s" then
                try
                    return execute t javascript "document.cookie"
                end try
            end if
        end repeat
    end repeat
end tell
return ""
''' % domain_substring

    try:
        proc = subprocess.run(["osascript", "-e", applescript], capture_output=True, text=True, check=False)
        out = proc.stdout.strip()
        if out:
            return out
    except Exception:
        return None
    return None


def detect_repo_from_git():
    try:
        out = subprocess.check_output(["git", "remote", "get-url", "origin"], stderr=subprocess.DEVNULL, text=True).strip()
        # URL examples: git@github.com:owner/repo.git or https://github.com/owner/repo.git
        if out.startswith("git@"):
            path = out.split(":", 1)[1]
        elif out.startswith("http"):
            path = out.split("/", 3)[-1]
        else:
            path = out
        if path.endswith(".git"):
            path = path[:-4]
        return path
    except Exception:
        return None


def detect_current_branch():
    try:
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None

    if not branch or branch == "HEAD":
        return None
    return branch


def _find_token_from_parent_folder_name():
    """
    Look two levels up from this script for a directory whose name starts with
    `github_pat`. The directory name itself is treated as the token.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    search_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

    try:
        entries = sorted(os.listdir(search_root))
    except Exception:
        return None

    for entry in entries:
        if not entry.startswith("github_pat"):
            continue
        candidate_path = os.path.join(search_root, entry)
        if os.path.isdir(candidate_path):
            return entry

    return None


def _get_token(token_arg):
    token = (token_arg or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if token:
        return token

    folder_token = _find_token_from_parent_folder_name()
    if folder_token:
        print("Using GitHub token inferred from a github_pat* folder in ../..")
        return folder_token

    return ""


def github_api_request(url, token, data=None, method="GET"):
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "ActionsRun-with-cookie")
    if token:
        req.add_header("Authorization", f"token {token}")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None, str(e)


def dispatch_workflow(repo, workflow_file, ref, token, inputs):
    workflow_path = quote(workflow_file, safe="")
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_path}/dispatches"
    payload = {"ref": ref, "inputs": inputs}
    data = json.dumps(payload).encode("utf-8")
    return github_api_request(url, token, data=data, method="POST")


def _parse_json(body):
    if not body:
        return {}
    try:
        parsed = json.loads(body)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def get_repo_default_branch(repo, token):
    status, body = github_api_request(f"https://api.github.com/repos/{repo}", token)
    if status != 200:
        return None
    return _parse_json(body).get("default_branch")


def resolve_ref(repo, token, explicit_ref):
    normalized = str(explicit_ref or "").strip()
    if normalized:
        return normalized

    current_branch = detect_current_branch()
    if current_branch:
        return current_branch

    default_branch = get_repo_default_branch(repo, token)
    if default_branch:
        return default_branch

    return "main"


def _parse_github_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def find_recent_workflow_run(repo, workflow_file, ref, token, started_after):
    workflow_path = quote(workflow_file, safe="")
    query = urlencode(
        {
            "event": "workflow_dispatch",
            "branch": ref,
            "per_page": 10,
        }
    )
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_path}/runs?{query}"
    status, body = github_api_request(url, token)
    if status != 200:
        return None

    payload = _parse_json(body)
    workflow_runs = payload.get("workflow_runs")
    if not isinstance(workflow_runs, list):
        return None

    lower_bound = started_after - timedelta(minutes=2)
    for run in workflow_runs:
        if not isinstance(run, dict):
            continue
        created_at = _parse_github_datetime(run.get("created_at"))
        if created_at and created_at >= lower_bound:
            return run
    return None


def wait_for_workflow_run(repo, workflow_file, ref, token, started_after, timeout_seconds=45):
    deadline = time.time() + max(1, timeout_seconds)
    while time.time() < deadline:
        run = find_recent_workflow_run(repo, workflow_file, ref, token, started_after)
        if run:
            return run
        time.sleep(3)
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--token", help="GitHub token (fallback: GITHUB_TOKEN, GH_TOKEN, or a github_pat* folder in ../..)")
    p.add_argument("--story-file", default="", help="Optional path to a story text file. Leave empty to let the app synthesize its own structured first-person story.")
    p.add_argument("--topic-direction", default="", help="Optional topic or story-direction bias for synthetic story generation")
    p.add_argument("--video-count", default="1", help="Number of shorts to generate")
    p.add_argument("--creator-mode", choices=["video"], default="video")
    p.add_argument("--main-video-mode", choices=["yes-main", "no-main"], default="yes-main")
    p.add_argument("--upload-to-youtube", choices=["true", "false"], default="true")
    p.add_argument("--use-tor-tunnel", choices=["true", "false"], default="true")
    p.add_argument("--workflow", default="generate-shorts.yml", help="workflow filename in .github/workflows")
    p.add_argument("--repo", help="owner/repo (auto-detected from git origin if omitted)")
    p.add_argument("--ref", default="", help="Git ref to dispatch against. Default: current branch, then repo default branch.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    token = _get_token(args.token)
    story_path = (args.story_file or "").strip()
    try:
        story = _read_story_text(story_path)
    except FileNotFoundError as exc:
        print(str(exc))
        sys.exit(2)

    # Attempt to capture cookie from Chrome if available
    cookie_string = get_cookie_from_chrome()
    if cookie_string:
        print("Found cookie in Chrome. Using it for freevoicereader_cookie input.")
    else:
        print("No Chrome cookie found for freevoicereader.com (ensure a tab is open and grant Automation permission).")

    repo = args.repo or detect_repo_from_git()
    if not repo:
        print("Could not detect repo. Please provide --repo owner/repo or ensure git origin is set.")
        sys.exit(3)

    ref = resolve_ref(repo, token, args.ref)

    inputs = {
        "video_count": str(args.video_count),
        "topic_direction": str(args.topic_direction or ""),
        "upload_to_youtube": str(args.upload_to_youtube),
        "story_text": story,
        "creator_mode": args.creator_mode,
        "main_video_mode": args.main_video_mode,
        "use_tor_tunnel": str(args.use_tor_tunnel),
        "freevoicereader_cookie": cookie_string or ""
    }

    if args.dry_run:
        print("Dry run payload:")
        print(json.dumps({"repo": repo, "workflow": args.workflow, "ref": ref, "inputs": {k: (v if k != "story_text" else f"<len={len(v)}>") for k, v in inputs.items()}}, indent=2))
        return

    if not token:
        print("No GitHub token provided. Use --token, set GITHUB_TOKEN/GH_TOKEN, or create a github_pat* folder in ../..")
        sys.exit(4)

    dispatch_started_at = datetime.now(timezone.utc)
    status, body = dispatch_workflow(repo, args.workflow, ref, token, inputs)

    if status not in {200, 201, 202, 204} and not args.ref:
        default_branch = get_repo_default_branch(repo, token)
        if default_branch and default_branch != ref:
            print(f"Dispatch on ref '{ref}' failed with status {status}. Retrying on default branch '{default_branch}'.")
            ref = default_branch
            dispatch_started_at = datetime.now(timezone.utc)
            status, body = dispatch_workflow(repo, args.workflow, ref, token, inputs)

    print("Dispatch response:", status)
    if body:
        print(body)

    if status not in {200, 201, 202, 204}:
        sys.exit(5)

    run = wait_for_workflow_run(repo, args.workflow, ref, token, dispatch_started_at)
    if not run:
        print("Workflow dispatch succeeded, but the run URL was not detected yet.")
        return

    print(f"Workflow run started successfully on ref '{ref}'.")
    print(f"Run URL: {run.get('html_url')}")
    print(f"Run status: {run.get('status')}")
    print(f"Run conclusion: {run.get('conclusion')}")


if __name__ == '__main__':
    main()
