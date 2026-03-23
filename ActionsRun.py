#!/usr/bin/env python3
"""
Dispatch the GitHub Actions workflow that generates shorts.

Examples:
  python3 ActionsRun.py --video_count 1 --creator_mode image
  python3 ActionsRun.py --topic_direction "relationship drama"
  python3 ActionsRun.py --redit_story story.txt --video_count 1

Authentication:
  Set GITHUB_TOKEN (or GH_TOKEN) with repo workflow permission.
"""

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from typing import Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_WORKFLOW_FILE = "generate-shorts.yml"
DEFAULT_REF = "main"
API_BASE = "https://api.github.com"


def _coerce_bool(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return "true"
    if normalized in {"0", "false", "no", "n", "off"}:
        return "false"
    raise ValueError(f"Invalid boolean value: {value}")


def _read_story_text(path: str) -> str:
    if not path:
        return ""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Story file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _parse_repo(raw: str) -> Tuple[str, str]:
    value = (raw or "").strip()
    if not value:
        raise ValueError("Empty repo value")

    # Accept owner/repo directly
    if re.match(r"^[^/]+/[^/]+$", value):
        owner, repo = value.split("/", 1)
        return owner, repo

    # Accept https://github.com/owner/repo(.git)
    m = re.search(r"github\.com[:/]([^/]+)/([^/.]+)(?:\.git)?$", value)
    if m:
        return m.group(1), m.group(2)

    raise ValueError(f"Could not parse repo from: {value}")


def _repo_from_git_remote() -> Optional[Tuple[str, str]]:
    try:
        result = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None

    url = (result.stdout or "").strip()
    if not url:
        return None

    try:
        return _parse_repo(url)
    except Exception:
        return None


def _resolve_repo(repo_arg: str) -> Tuple[str, str]:
    if repo_arg:
        return _parse_repo(repo_arg)

    from_env = os.getenv("GITHUB_REPOSITORY", "").strip()
    if from_env:
        return _parse_repo(from_env)

    from_git = _repo_from_git_remote()
    if from_git:
        return from_git

    raise RuntimeError(
        "Could not determine repository. Pass --repo owner/repo or set GITHUB_REPOSITORY."
    )


def _get_token(token_arg: str) -> str:
    token = (token_arg or os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("Missing token. Use --token or set GITHUB_TOKEN / GH_TOKEN.")
    return token


def _dispatch_workflow(
    owner: str,
    repo: str,
    workflow_file: str,
    token: str,
    ref: str,
    inputs: dict,
) -> None:
    url = f"{API_BASE}/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches"
    payload = {"ref": ref, "inputs": inputs}
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, headers=headers, method="POST")

    try:
        with urlopen(req, timeout=30) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            if status != 204:
                detail = (resp.read() or b"").decode("utf-8", errors="replace").strip()[:800]
                raise RuntimeError(
                    f"Workflow dispatch failed ({status}). URL={url}. Response={detail}"
                )
    except HTTPError as exc:
        detail = (exc.read() or b"").decode("utf-8", errors="replace").strip()[:800]
        raise RuntimeError(
            f"Workflow dispatch failed ({exc.code}). URL={url}. Response={detail}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Network error dispatching workflow: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dispatch GitHub workflow: .github/workflows/generate-shorts.yml"
    )
    parser.add_argument("--repo", default="", help="Repo as owner/repo or full GitHub URL")
    parser.add_argument("--token", default="", help="GitHub token (fallback: GITHUB_TOKEN/GH_TOKEN)")
    parser.add_argument("--ref", default=DEFAULT_REF, help="Git ref/branch for workflow dispatch")
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW_FILE, help="Workflow file name")

    parser.add_argument("--video_count", default="1", help="Workflow input: video_count")
    parser.add_argument("--topic_direction", default="", help="Workflow input: topic_direction")
    parser.add_argument("--creator_mode", choices=["auto", "video", "image"], default="auto")
    parser.add_argument("--upload_to_youtube", default="false", help="Workflow input bool")
    parser.add_argument("--use_tor_tunnel", default="true", help="Workflow input bool")

    # User requested this exact flag spelling.
    parser.add_argument(
        "--redit_story",
        default="",
        help="Path to text file with story content; maps to workflow input story_text",
    )
    # Extra friendly alias.
    parser.add_argument(
        "--reddit_story",
        default="",
        help="Alias of --redit_story",
    )

    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Print resolved payload without dispatching",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    owner, repo = _resolve_repo(args.repo)

    story_path = args.redit_story or args.reddit_story
    story_text = _read_story_text(story_path) if story_path else ""

    inputs = {
        "video_count": str(args.video_count),
        "topic_direction": str(args.topic_direction or ""),
        "story_text": story_text,
        "creator_mode": args.creator_mode,
        "upload_to_youtube": _coerce_bool(args.upload_to_youtube),
        "use_tor_tunnel": _coerce_bool(args.use_tor_tunnel),
    }

    if args.dry_run:
        print("[dry-run] Would dispatch workflow with:")
        print(json.dumps(
            {
                "repo": f"{owner}/{repo}",
                "workflow": args.workflow,
                "ref": args.ref,
                "inputs": {
                    **inputs,
                    "story_text": f"<len={len(story_text)} chars>" if story_text else "",
                },
            },
            indent=2,
        ))
        return 0

    token = _get_token(args.token)

    _dispatch_workflow(
        owner=owner,
        repo=repo,
        workflow_file=args.workflow,
        token=token,
        ref=args.ref,
        inputs=inputs,
    )

    run_hint_time = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    actions_url = f"https://github.com/{owner}/{repo}/actions/workflows/{args.workflow}"
    print("Workflow dispatch accepted.")
    print(f"Repository: {owner}/{repo}")
    print(f"Workflow: {args.workflow}")
    print(f"Ref: {args.ref}")
    print(f"Submitted at: {run_hint_time}")
    print(f"Open runs: {actions_url}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
