import os
import json
import random
import datetime
import requests
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_news_api_key():
    return os.getenv("NEWS_API_KEY")

def get_latest_news():

    """Get the latest technology or AI news with minimal API calls."""
    news_api_key = get_news_api_key()
    if not news_api_key:
        raise ValueError("NewsAPI key is missing. Set NEWS_API_KEY in the current environment.")

    # Calculate the date two weeks ago
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=2)
    # Format the date as YYYY-MM-DD for the News API
    from_date = two_weeks_ago.strftime("%Y-%m-%d")

    # Get today's date for the cache key
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # Setup cache file
    cache_dir = Path.home() / ".news_cache"
    cache_dir.mkdir(exist_ok=True)
    cache_file = cache_dir / f"news_cache_{today}.json"

    # Read from cache if exists
    used_articles = []
    if cache_file.exists():
        try:
            with open(cache_file, 'r') as f:
                used_articles = json.load(f)
        except json.JSONDecodeError:
            used_articles = []

    # Combine all topics in a single query with OR operators
    topics = ["artificial intelligence", "tech innovation",
              "machine learning", "gaming", "robotics", "world news"]

    # Create a query string with OR between each topic
    query = " OR ".join(topics)

    # Make a single API call with all topics
    url = f"https://newsapi.org/v2/top-headlines?q={query}&category=technology&from={from_date}&sortBy=popularity&pageSize=30&apiKey={news_api_key}"
    response = requests.get(url)

    articles = []
    if response.status_code == 200:
        articles = response.json().get('articles', [])

    # If no articles found or API call failed, fallback to general technology
    if not articles:
        url = f"https://newsapi.org/v2/top-headlines?category=technology&apiKey={news_api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            articles = response.json().get('articles', [])

    # Filter out already used articles
    unused_articles = [a for a in articles if a['title'] not in used_articles]

    # If we've used all articles, reset the cache
    if not unused_articles and articles:
        unused_articles = articles
        used_articles = []

    # Choose a random article from unused ones
    if unused_articles:
        # Take the top 10 articles or all if less than 10
        top_articles = unused_articles[:min(10, len(unused_articles))]
        chosen_article = random.choice(top_articles)

        # Add to used articles
        used_articles.append(chosen_article['title'])

        # Update cache file
        with open(cache_file, 'w') as f:
            json.dump(used_articles, f)

        return chosen_article['title']

    return "Latest Technology Innovation News"


def search_recent_news(topic, max_articles=6):
    """
    Search recent web news for a user topic and return normalized article records.

    Returns:
        list[dict]: [{"title", "source", "published_at", "description", "url"}, ...]
    """
    api_key = get_news_api_key()
    if not api_key:
        logger.warning("NEWS_API_KEY is missing; cannot fetch recent topic news")
        return []

    if not topic or not topic.strip():
        logger.warning("Topic is empty for search_recent_news; returning no articles")
        return []

    two_weeks_ago = datetime.datetime.utcnow() - datetime.timedelta(days=14)
    from_date = two_weeks_ago.strftime("%Y-%m-%d")

    params = {
        "q": topic.strip(),
        "from": from_date,
        "sortBy": "publishedAt",
        "language": "en",
        "pageSize": max(1, min(int(max_articles), 20)),
        "apiKey": api_key,
    }

    try:
        response = requests.get("https://newsapi.org/v2/everything", params=params, timeout=30)
        if response.status_code != 200:
            logger.warning("NewsAPI topic search failed (%s): %s", response.status_code, response.text[:300])
            return []

        payload = response.json() or {}
        articles = payload.get("articles", []) or []

        normalized = []
        for article in articles:
            title = (article.get("title") or "").strip()
            if not title:
                continue

            normalized.append(
                {
                    "title": title,
                    "source": (article.get("source") or {}).get("name", "Unknown"),
                    "published_at": article.get("publishedAt", ""),
                    "description": (article.get("description") or "").strip(),
                    "url": article.get("url", ""),
                }
            )

        logger.info("Fetched %s recent articles for topic '%s'", len(normalized), topic)
        return normalized
    except Exception as exc:
        logger.warning("Recent news lookup failed for topic '%s': %s", topic, exc)
        return []
