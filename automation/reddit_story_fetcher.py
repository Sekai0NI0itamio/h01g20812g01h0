#!/usr/bin/env python3
"""
Fetch a random text story from r/stories using Playwright WebKit (Safari engine).
This module exposes both async and sync helpers for reuse in the generation pipeline.
"""

import asyncio
import json
import logging
import os
import random
from typing import Any, Dict, Optional

import requests

SUBREDDIT_URL = "https://www.reddit.com/r/stories/"
TIMEOUT_MS = int(os.getenv("REDDIT_FETCH_TIMEOUT_MS", "45000"))
FETCH_RETRIES = max(1, int(os.getenv("REDDIT_FETCH_RETRIES", "3")))
FETCH_SCROLL_ROUNDS = max(1, int(os.getenv("REDDIT_FETCH_SCROLL_ROUNDS", "4")))
FETCH_DEBUG = os.getenv("REDDIT_FETCH_DEBUG", "true").strip().lower() == "true"
REDDIT_JSON_TIMEOUT_S = int(os.getenv("REDDIT_JSON_TIMEOUT_S", "25"))
REDDIT_API_FIRST = os.getenv("REDDIT_API_FIRST", "false").strip().lower() == "true"

REDDIT_JSON_ENDPOINTS = [
    "https://www.reddit.com/r/stories/hot.json?limit=100&raw_json=1",
    "https://old.reddit.com/r/stories/hot.json?limit=100&raw_json=1",
    "https://www.reddit.com/r/stories/.json?limit=100&raw_json=1",
]

logger = logging.getLogger(__name__)


def _clean_text(value: Optional[str]) -> str:
    return " ".join(str(value or "").split()).strip()


def _looks_like_block_page(text: str) -> bool:
    hay = _clean_text(text).lower()
    if not hay:
        return False
    block_markers = [
        "you've been blocked by network security",
        "you have been blocked by network security",
        "to continue, log in to your reddit account",
        "file a ticket",
    ]
    return any(marker in hay for marker in block_markers)


def _fetch_random_story_via_reddit_json(timeout_s: int = REDDIT_JSON_TIMEOUT_S, debug: bool = False) -> Optional[Dict[str, Any]]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
        ),
        "Accept": "application/json,text/plain,*/*",
    }

    for endpoint in REDDIT_JSON_ENDPOINTS:
        try:
            if debug:
                logger.info("[reddit-fetch-json] requesting endpoint=%s", endpoint)
            resp = requests.get(endpoint, headers=headers, timeout=timeout_s)
            if resp.status_code != 200:
                if debug:
                    logger.info("[reddit-fetch-json] non-200 status=%s endpoint=%s", resp.status_code, endpoint)
                continue

            payload = resp.json()
            posts = (((payload or {}).get("data") or {}).get("children") or [])
            candidates = []
            for post in posts:
                data = (post or {}).get("data") or {}
                body = _clean_text(data.get("selftext", ""))
                if not body:
                    continue
                if data.get("stickied"):
                    continue
                permalink = str(data.get("permalink", "")).strip()
                if not permalink:
                    continue
                if not permalink.startswith("http"):
                    permalink = f"https://www.reddit.com{permalink}"

                candidates.append(
                    {
                        "title": _clean_text(data.get("title", "")) or "Untitled story",
                        "body": body,
                        "permalink": permalink,
                        "subreddit_url": SUBREDDIT_URL,
                    }
                )

            if candidates:
                chosen = random.choice(candidates)
                if debug:
                    logger.info("[reddit-fetch-json] selected story permalink=%s body_chars=%s", chosen.get("permalink"), len(chosen.get("body", "")))
                return chosen

        except Exception as exc:
            if debug:
                logger.info("[reddit-fetch-json] endpoint failed endpoint=%s error=%s", endpoint, exc)

    return None


def _extract_story_candidates_from_json_payload(payload: Dict[str, Any]) -> list[Dict[str, Any]]:
    posts = (((payload or {}).get("data") or {}).get("children") or [])
    candidates = []

    for post in posts:
        data = (post or {}).get("data") or {}
        body = _clean_text(data.get("selftext", ""))
        if not body:
            continue
        if data.get("stickied"):
            continue

        permalink = str(data.get("permalink", "")).strip()
        if not permalink:
            continue
        if not permalink.startswith("http"):
            permalink = f"https://www.reddit.com{permalink}"

        candidates.append(
            {
                "title": _clean_text(data.get("title", "")) or "Untitled story",
                "body": body,
                "permalink": permalink,
                "subreddit_url": SUBREDDIT_URL,
            }
        )

    return candidates


async def _fetch_random_story_via_browser_json(page, timeout_ms: int, debug: bool = False) -> Optional[Dict[str, Any]]:
    for endpoint in REDDIT_JSON_ENDPOINTS:
        try:
            if debug:
                logger.info("[reddit-fetch-browser-json] opening endpoint in browser=%s", endpoint)

            await page.goto(endpoint, timeout=timeout_ms, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            await _log_page_snapshot(page, "browser-json", debug=debug)

            body_text = _clean_text(await _get_body_text(page))
            if _looks_like_block_page(body_text):
                if debug:
                    logger.info("[reddit-fetch-browser-json] blocked content detected endpoint=%s", endpoint)
                continue

            parsed_payload = None
            try:
                parsed_payload = json.loads(body_text)
            except Exception:
                try:
                    pre = await page.query_selector("pre")
                    pre_text = _clean_text(await pre.inner_text()) if pre else ""
                    if pre_text:
                        parsed_payload = json.loads(pre_text)
                except Exception:
                    parsed_payload = None

            if not isinstance(parsed_payload, dict):
                if debug:
                    logger.info("[reddit-fetch-browser-json] could not parse JSON endpoint=%s", endpoint)
                continue

            candidates = _extract_story_candidates_from_json_payload(parsed_payload)
            if not candidates:
                if debug:
                    logger.info("[reddit-fetch-browser-json] no story candidates endpoint=%s", endpoint)
                continue

            chosen = random.choice(candidates)
            if debug:
                logger.info(
                    "[reddit-fetch-browser-json] selected story permalink=%s body_chars=%s",
                    chosen.get("permalink"),
                    len(chosen.get("body", "")),
                )
            return chosen
        except Exception as exc:
            if debug:
                logger.info("[reddit-fetch-browser-json] endpoint failed endpoint=%s error=%s", endpoint, exc)

    return None


async def fetch_random_story(
    subreddit_url: str = SUBREDDIT_URL,
    timeout_ms: int = TIMEOUT_MS,
    headless: bool = True,
    scroll_rounds: int = FETCH_SCROLL_ROUNDS,
    debug: bool = FETCH_DEBUG,
) -> Optional[Dict[str, Any]]:
    if REDDIT_API_FIRST:
        json_story = _fetch_random_story_via_reddit_json(debug=debug)
        if json_story:
            return json_story

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is not installed. Run: pip install playwright && playwright install webkit"
        ) from exc

    async with async_playwright() as p:
        browser = await p.webkit.launch(headless=headless)
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
                )
            )
            page = await context.new_page()

            if debug:
                logger.info("[reddit-fetch] opening subreddit: %s", subreddit_url)

            await page.goto(subreddit_url, timeout=timeout_ms, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            await _log_page_snapshot(page, "subreddit-initial", debug=debug)

            initial_body_text = await _get_body_text(page)
            if _looks_like_block_page(initial_body_text):
                logger.warning("[reddit-fetch] browser page appears blocked; trying browser JSON endpoints")
                browser_json_story = await _fetch_random_story_via_browser_json(page, timeout_ms=timeout_ms, debug=debug)
                if browser_json_story:
                    return browser_json_story

                logger.warning("[reddit-fetch] browser JSON endpoints failed; falling back to direct Reddit JSON endpoint")
                fallback_story = _fetch_random_story_via_reddit_json(debug=debug)
                if fallback_story:
                    return fallback_story

            await page.wait_for_selector("shreddit-post", timeout=timeout_ms)

            for _ in range(max(0, int(scroll_rounds))):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(1200)

            await _log_page_snapshot(page, "subreddit-after-scroll", debug=debug)

            post_nodes = await page.query_selector_all("shreddit-post")
            if debug:
                logger.info("[reddit-fetch] discovered %s shreddit-post nodes", len(post_nodes))
            candidates = []
            for idx, node in enumerate(post_nodes):
                permalink = await node.get_attribute("permalink")
                if not permalink:
                    link = await node.query_selector('a[slot="title"]')
                    if link:
                        permalink = await link.get_attribute("href")
                if not permalink:
                    if debug:
                        logger.info("[reddit-fetch] post[%s] skipped: no permalink", idx)
                    continue
                if "/comments/" not in permalink:
                    if debug:
                        logger.info("[reddit-fetch] post[%s] skipped: non-story permalink=%s", idx, permalink)
                    continue
                if debug:
                    title_preview = _clean_text(await node.get_attribute("post-title"))
                    if not title_preview:
                        link = await node.query_selector('a[slot="title"]')
                        if link:
                            title_preview = _clean_text(await link.inner_text())
                    logger.info(
                        "[reddit-fetch] post[%s] candidate permalink=%s title=%s",
                        idx,
                        permalink,
                        title_preview or "(empty)",
                    )
                candidates.append((node, permalink))

            if not candidates:
                await _log_page_snapshot(page, "subreddit-no-candidates", debug=debug)
                browser_json_story = await _fetch_random_story_via_browser_json(page, timeout_ms=timeout_ms, debug=debug)
                if browser_json_story:
                    return browser_json_story
                return None

            node, permalink = random.choice(candidates)
            if not permalink.startswith("http"):
                permalink = f"https://www.reddit.com{permalink}"

            if debug:
                logger.info("[reddit-fetch] selected post permalink=%s", permalink)

            title = ""
            title_attr = await node.get_attribute("post-title")
            if title_attr:
                title = _clean_text(title_attr)
            if not title:
                title_link = await node.query_selector('a[slot="title"]')
                if title_link:
                    title = _clean_text(await title_link.inner_text())

            await page.goto(permalink, timeout=timeout_ms, wait_until="domcontentloaded")
            await page.wait_for_load_state("networkidle", timeout=timeout_ms)
            await _log_page_snapshot(page, "story-page", debug=debug)
            await page.wait_for_selector("shreddit-post", timeout=timeout_ms)

            post_element = await page.query_selector("shreddit-post")
            if post_element:
                if not title:
                    h1 = await post_element.query_selector("h1")
                    if h1:
                        title = _clean_text(await h1.inner_text())

                body_element = await post_element.query_selector('[slot="text-body"]')
                if not body_element:
                    body_element = await page.query_selector('div[data-testid="post-content"]')

                body = _clean_text(await body_element.inner_text()) if body_element else ""
                if debug:
                    logger.info("[reddit-fetch] body length chars=%s", len(body))
                if not body:
                    await _log_page_snapshot(page, "story-empty-body", debug=debug)
                    return None

                return {
                    "title": title or "Untitled story",
                    "body": body,
                    "permalink": permalink,
                    "subreddit_url": subreddit_url,
                }

            return None
        except Exception as exc:
            await _log_page_snapshot(page, "exception-snapshot", debug=debug)
            logger.warning("[reddit-fetch] fetch failed with exception: %s", exc)
            raise
        finally:
            await browser.close()


async def _log_page_snapshot(page, label: str, debug: bool = False) -> None:
    if not debug:
        return

    try:
        url = page.url
    except Exception:
        url = "unknown"

    text_content = ""
    html_content = ""
    text_content = await _get_body_text(page)

    try:
        html_content = await page.content()
    except Exception:
        html_content = ""

    cleaned = _clean_text(text_content)
    logger.info("[reddit-fetch:%s] url=%s", label, url)
    logger.info("[reddit-fetch:%s] visible_text_chars=%s html_chars=%s", label, len(cleaned), len(html_content))

    # User requested to print everything seen; emit full visible page text.
    if cleaned:
        logger.info("[reddit-fetch:%s] visible_text_full_start", label)
        logger.info(cleaned)
        logger.info("[reddit-fetch:%s] visible_text_full_end", label)


async def _get_body_text(page) -> str:
    try:
        body = await page.query_selector("body")
        if body:
            return await body.inner_text()
    except Exception:
        return ""
    return ""


def fetch_random_story_sync(
    subreddit_url: str = SUBREDDIT_URL,
    timeout_ms: int = TIMEOUT_MS,
    headless: bool = True,
    scroll_rounds: int = FETCH_SCROLL_ROUNDS,
    retries: int = FETCH_RETRIES,
    debug: bool = FETCH_DEBUG,
) -> Optional[Dict[str, Any]]:
    attempt_count = max(1, int(retries))
    for attempt in range(attempt_count):
        try:
            return asyncio.run(
                fetch_random_story(
                    subreddit_url=subreddit_url,
                    timeout_ms=timeout_ms,
                    headless=headless,
                    scroll_rounds=scroll_rounds,
                    debug=debug,
                )
            )
        except RuntimeError as exc:
            if "asyncio.run() cannot be called from a running event loop" not in str(exc):
                if attempt < attempt_count - 1:
                    logger.warning("[reddit-fetch] attempt %s/%s failed: %s", attempt + 1, attempt_count, exc)
                    continue
                raise
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(
                    fetch_random_story(
                        subreddit_url=subreddit_url,
                        timeout_ms=timeout_ms,
                        headless=headless,
                        scroll_rounds=scroll_rounds,
                        debug=debug,
                    )
                )
            finally:
                loop.close()
        except Exception as exc:
            logger.warning("[reddit-fetch] attempt %s/%s failed: %s", attempt + 1, attempt_count, exc)
            if attempt >= attempt_count - 1:
                raise

    return None


if __name__ == "__main__":
    try:
        story = fetch_random_story_sync(headless=True)
        if not story:
            print("No story found.")
        else:
            print("=" * 80)
            print(f"Title: {story['title']}")
            print(f"Link: {story['permalink']}")
            print("\nStory:")
            print(story["body"])
            print("=" * 80)
    except Exception as exc:
        print(f"Error: {exc}")
