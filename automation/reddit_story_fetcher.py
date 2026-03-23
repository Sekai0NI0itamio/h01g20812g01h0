#!/usr/bin/env python3
"""
Fetch a random text story from r/stories using Playwright WebKit (Safari engine).
This module exposes both async and sync helpers for reuse in the generation pipeline.
"""

import asyncio
import random
from typing import Any, Dict, Optional

SUBREDDIT_URL = "https://www.reddit.com/r/stories/"
TIMEOUT_MS = 15000


def _clean_text(value: Optional[str]) -> str:
    return " ".join(str(value or "").split()).strip()


async def fetch_random_story(
    subreddit_url: str = SUBREDDIT_URL,
    timeout_ms: int = TIMEOUT_MS,
    headless: bool = True,
    scroll_rounds: int = 2,
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

            await page.goto(subreddit_url, timeout=timeout_ms)
            await page.wait_for_selector("shreddit-post", timeout=timeout_ms)

            for _ in range(max(0, int(scroll_rounds))):
                await page.evaluate("window.scrollBy(0, window.innerHeight)")
                await page.wait_for_timeout(900)

            post_nodes = await page.query_selector_all("shreddit-post")
            candidates = []
            for node in post_nodes:
                permalink = await node.get_attribute("permalink")
                if not permalink:
                    link = await node.query_selector('a[slot="title"]')
                    if link:
                        permalink = await link.get_attribute("href")
                if not permalink:
                    continue
                if "/comments/" not in permalink:
                    continue
                candidates.append((node, permalink))

            if not candidates:
                return None

            node, permalink = random.choice(candidates)
            if not permalink.startswith("http"):
                permalink = f"https://www.reddit.com{permalink}"

            title = ""
            title_attr = await node.get_attribute("post-title")
            if title_attr:
                title = _clean_text(title_attr)
            if not title:
                title_link = await node.query_selector('a[slot="title"]')
                if title_link:
                    title = _clean_text(await title_link.inner_text())

            await page.goto(permalink, timeout=timeout_ms)
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
                if not body:
                    return None

                return {
                    "title": title or "Untitled story",
                    "body": body,
                    "permalink": permalink,
                    "subreddit_url": subreddit_url,
                }

            return None
        finally:
            await browser.close()


def fetch_random_story_sync(
    subreddit_url: str = SUBREDDIT_URL,
    timeout_ms: int = TIMEOUT_MS,
    headless: bool = True,
    scroll_rounds: int = 2,
) -> Optional[Dict[str, Any]]:
    try:
        return asyncio.run(
            fetch_random_story(
                subreddit_url=subreddit_url,
                timeout_ms=timeout_ms,
                headless=headless,
                scroll_rounds=scroll_rounds,
            )
        )
    except RuntimeError as exc:
        if "asyncio.run() cannot be called from a running event loop" not in str(exc):
            raise
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                fetch_random_story(
                    subreddit_url=subreddit_url,
                    timeout_ms=timeout_ms,
                    headless=headless,
                    scroll_rounds=scroll_rounds,
                )
            )
        finally:
            loop.close()


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
