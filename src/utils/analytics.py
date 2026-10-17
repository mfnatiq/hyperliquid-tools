"""Umami analytics: server-side events plus the page-view script loader."""
import logging
import os
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

UMAMI_API = "https://cloud.umami.is/api/send"
UMAMI_SCRIPT = "https://cloud.umami.is/script.js"
UMAMI_WEBSITE_ID = "d055b0ff-48a4-4617-a9fd-4124a5346705"


def build_event_payload(event_name: str, additional_data: Optional[dict] = None) -> dict[str, Any]:
    # Umami's send API rejects events without a website id
    payload: dict[str, Any] = {
        "website": UMAMI_WEBSITE_ID,
        "name": event_name,
        "url": "/",
        "hostname": os.getenv("UMAMI_HOSTNAME") or os.getenv("HOSTNAME") or "localhost",
        "language": "en-US",
        "referrer": "",
        "screen": "1920x1080",
        "title": "Hyperliquid Tools",
    }
    if additional_data and isinstance(additional_data, dict):
        payload["data"] = additional_data
    return {"type": "event", "payload": payload}


def track_event(event_name: str, additional_data: Optional[dict] = None) -> bool:
    """Send one event to Umami. Returns True only when Umami answers 2xx."""
    headers = {
        "Content-Type": "application/json",
        # Umami drops requests whose user agent looks like a bot or is missing
        "User-Agent": "Mozilla/5.0 (compatible; Hyperliquid-Tools/1.0)",
    }
    try:
        resp = requests.post(
            UMAMI_API, json=build_event_payload(event_name, additional_data), headers=headers, timeout=5
        )
    except requests.RequestException as e:
        logger.warning(f"umami analytics tracking failed for {event_name}: {e}")
        return False
    if not resp.ok:
        logger.warning(f"umami rejected event {event_name}: {resp.status_code} {resp.text[:200]}")
        return False
    logger.info(f"tracked event {event_name} to umami ({resp.status_code})")
    return True


# runs inside a zero-height component frame, but adds the script to the parent page
# so Umami sees the real hostname and path, and every st.navigation page view
PAGEVIEW_LOADER = f"""
<script>
(function () {{
    var doc = window.parent.document;
    if (doc.getElementById("umami-script")) return;
    var s = doc.createElement("script");
    s.id = "umami-script";
    s.defer = true;
    s.src = "{UMAMI_SCRIPT}";
    s.setAttribute("data-website-id", "{UMAMI_WEBSITE_ID}");
    doc.head.appendChild(s);
}})();
</script>
"""


if __name__ == "__main__":
    # one-line check: python -m src.utils.analytics
    ok = track_event("manual_test")
    print("OK, event accepted" if ok else "FAILED, see warning above")
    raise SystemExit(0 if ok else 1)
