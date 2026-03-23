#!/usr/bin/env python3
"""
Fetch a random text story from r/stories using Playwright WebKit (Safari engine).
This module exposes both async and sync helpers for reuse in the generation pipeline.
"""

import asyncio
import logging
import os
import random
from typing import Any, Dict, Optional

SUBREDDIT_URL = "https://www.reddit.com/r/stories/"
TIMEOUT_MS = int(os.getenv("REDDIT_FETCH_TIMEOUT_MS", "45000"))
FETCH_RETRIES = max(1, int(os.getenv("REDDIT_FETCH_RETRIES", "3")))
FETCH_SCROLL_ROUNDS = max(1, int(os.getenv("REDDIT_FETCH_SCROLL_ROUNDS", "4")))
FETCH_DEBUG = os.getenv("REDDIT_FETCH_DEBUG", "true").strip().lower() == "true"

logger = logging.getLogger(__name__)


def _clean_text(value: Optional[str]) -> str:
    return " ".join(str(value or "").split()).strip()


async def fetch_random_story(
    subreddit_url: str = SUBREDDIT_URL,
    timeout_ms: int = TIMEOUT_MS,
    headless: bool = True,
    scroll_rounds: int = FETCH_SCROLL_ROUNDS,
    debug: bool = FETCH_DEBUG,
) -> Optional[Dict[str, Any]]:
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
    try:
        body = await page.query_selector("body")
        if body:
            text_content = await body.inner_text()
    except Exception:
        text_content = ""

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
