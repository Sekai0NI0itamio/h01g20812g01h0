import logging
import os
from urllib.parse import urlparse

import httplib2
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

load_dotenv()


def is_tor_tunnel_enabled():
    return os.getenv("ENABLE_TOR_TUNNEL", "false").lower() == "true"


def get_tor_proxy_url():
    if not is_tor_tunnel_enabled():
        return None

    explicit_url = os.getenv("TOR_PROXY_URL", "").strip()
    if explicit_url:
        return explicit_url

    host = os.getenv("TOR_PROXY_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("TOR_PROXY_PORT", "9050").strip() or "9050"
    return f"socks5h://{host}:{port}"


def create_requests_session(use_tor=None):
    session = requests.Session()

    pool_connections = max(4, int(os.getenv("REQUESTS_POOL_CONNECTIONS", "32")))
    pool_maxsize = max(pool_connections, int(os.getenv("REQUESTS_POOL_MAXSIZE", "32")))
    retry_total = max(0, int(os.getenv("REQUESTS_HTTP_RETRY_TOTAL", "2")))
    retry_backoff = max(0.0, float(os.getenv("REQUESTS_HTTP_RETRY_BACKOFF_SECONDS", "0.5")))
    retry = Retry(
        total=retry_total,
        connect=retry_total,
        read=retry_total,
        status=retry_total,
        backoff_factor=retry_backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD", "OPTIONS"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=retry,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    if use_tor is False:
        session.trust_env = False
        return session

    proxy_url = get_tor_proxy_url() if use_tor is not False else None

    if proxy_url:
        session.trust_env = False
        session.proxies.update({"http": proxy_url, "https": proxy_url})
        logger.info("Tor tunnel enabled for requests via %s", proxy_url)

    return session


def get_httplib2_proxy_info():
    proxy_url = get_tor_proxy_url()
    if not proxy_url:
        return None

    parsed = urlparse(proxy_url)
    if parsed.scheme not in {"socks5", "socks5h"}:
        logger.warning("Unsupported Tor proxy scheme for httplib2: %s", parsed.scheme)
        return None

    try:
        import socks
    except Exception as exc:
        logger.warning("PySocks is required for Tor-enabled YouTube uploads: %s", exc)
        return None

    proxy_type = socks.PROXY_TYPE_SOCKS5
    host = parsed.hostname or "127.0.0.1"
    port = int(parsed.port or 9050)
    return httplib2.ProxyInfo(proxy_type, host, port, proxy_rdns=True)
