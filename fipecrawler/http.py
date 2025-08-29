import threading
from typing import Optional

import requests
from requests.adapters import HTTPAdapter, Retry


# Thread-local para manter uma Session por thread
_thread_local = threading.local()


class RequestLimiter:
    def __init__(self, limit: int, margin: int, used: int = 0, date_key: Optional[str] = None):
        self.limit = max(0, int(limit))
        self.margin = max(0, int(margin))
        self.used = max(0, int(used))
        self.date_key = date_key  # e.g., YYYY-MM-DD

    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def can_make_request(self) -> bool:
        # stop when remaining <= margin
        return self.remaining() > self.margin

    def on_request(self):
        self.used += 1


def _timeout_request_wrapper(original_request, timeout: int):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    return wrapped


def build_session(timeout: int, retries: int, backoff: float, token: Optional[str] = None) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.request = _timeout_request_wrapper(session.request, timeout)
    if token:
        session.headers.update({"X-Subscription-Token": token, "accept": "application/json"})
    return session


def get_thread_session(timeout: int, retries: int, backoff: float, token: Optional[str]) -> requests.Session:
    """Retorna uma Session específica da thread, criando se necessário."""
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = build_session(timeout=timeout, retries=retries, backoff=backoff, token=token)
        _thread_local.session = sess
    return sess


def get_json(session: requests.Session, url: str, limiter: Optional[RequestLimiter] = None) -> Optional[dict]:
    import sys

    try:
        if limiter is not None:
            if not limiter.can_make_request():
                raise RuntimeError("Daily request limit margin reached; stopping to resume later.")
            limiter.on_request()
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[WARN] Falha ao requisitar {url}: {e}", file=sys.stderr)
        return None
