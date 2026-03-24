import os


def _normalized_env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default) or "").strip().lower()


def is_github_actions_runtime() -> bool:
    runtime_mode = _normalized_env("SHORTS_RUNTIME_MODE")
    if runtime_mode in {"github_actions", "github-actions", "actions", "ci"}:
        return True
    return _normalized_env("GITHUB_ACTIONS") == "true"


def is_video_only_runtime() -> bool:
    if _normalized_env("SHORTS_VIDEO_ONLY") == "true":
        return True
    return is_github_actions_runtime()


def coerce_creator_mode(choice: str | None) -> str:
    if is_video_only_runtime():
        return "video"

    normalized = str(choice or "").strip().lower()
    if normalized in {"auto", "video", "image"}:
        return normalized

    configured = _normalized_env("SHORTS_CREATOR_MODE", "auto")
    if configured in {"auto", "video", "image"}:
        return configured

    return "auto"


def should_use_local_c05_keys(default: bool = True) -> bool:
    if is_github_actions_runtime():
        return False
    return default


def allow_local_dev_runtime() -> bool:
    return _normalized_env("SHORTS_ALLOW_LOCAL_DEV") == "true"


def require_actions_runtime(feature_name: str = "this command") -> None:
    if is_github_actions_runtime() or allow_local_dev_runtime():
        return
    raise RuntimeError(
        f"{feature_name} is now GitHub-Actions-first. "
        "Run it through ActionsRun_with_cookie.py or set SHORTS_ALLOW_LOCAL_DEV=true for developer-only local use."
    )
