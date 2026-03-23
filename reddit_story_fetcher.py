#!/usr/bin/env python3
"""
CLI wrapper to fetch and print a random story from r/stories using Playwright WebKit.
"""

from automation.reddit_story_fetcher import fetch_random_story_sync


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
