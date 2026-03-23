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
  python3 ActionsRun_with_cookie.py --token $GITHUB_TOKEN
  python3 ActionsRun_with_cookie.py --token $GITHUB_TOKEN --story-file redit_story.txt
  python3 ActionsRun_with_cookie.py --dry-run

Note: This script uses only stdlib modules and requires macOS + Google Chrome.
"""

import argparse
import json
import os
import shlex
import subprocess
import sys
import urllib.request
import urllib.error


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
                                # cookiejar cookie objects have .name and .value
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
        path = path.rstrip(".git")
        return path
    except Exception:
        return None


def dispatch_workflow(repo, workflow_file, ref, token, inputs):
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {"ref": ref, "inputs": inputs}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--token", help="GitHub token (or set GITHUB_TOKEN env)")
    p.add_argument("--story-file", default="", help="Optional path to a story text file. Leave empty to let the app synthesize its own Reddit-style story.")
    p.add_argument("--topic-direction", default="", help="Optional topic or story-direction bias for synthetic story generation")
    p.add_argument("--video-count", default="1", help="Number of shorts to generate")
    p.add_argument("--creator-mode", choices=["auto", "video", "image"], default="auto")
    p.add_argument("--upload-to-youtube", choices=["true", "false"], default="true")
    p.add_argument("--use-tor-tunnel", choices=["true", "false"], default="true")
    p.add_argument("--workflow", default="generate-shorts.yml", help="workflow filename in .github/workflows")
    p.add_argument("--repo", help="owner/repo (auto-detected from git origin if omitted)")
    p.add_argument("--ref", default="main", help="git ref to dispatch against")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
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

    inputs = {
        "video_count": str(args.video_count),
        "topic_direction": str(args.topic_direction or ""),
        "upload_to_youtube": str(args.upload_to_youtube),
        "story_text": story,
        "creator_mode": args.creator_mode,
        "use_tor_tunnel": str(args.use_tor_tunnel),
        "freevoicereader_cookie": cookie_string or ""
    }

    if args.dry_run:
        print("Dry run payload:")
        print(json.dumps({"repo": repo, "workflow": args.workflow, "ref": args.ref, "inputs": {k: (v if k != "story_text" else f"<len={len(v)}>") for k, v in inputs.items()}}, indent=2))
        return

    if not token:
        print("No GitHub token provided. Use --token or set GITHUB_TOKEN in the environment.")
        sys.exit(4)

    status, body = dispatch_workflow(repo, args.workflow, args.ref, token, inputs)
    print("Dispatch response:", status)
    if body:
        print(body)


if __name__ == '__main__':
    main()
