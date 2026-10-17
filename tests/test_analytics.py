from unittest.mock import MagicMock, patch

from src.utils.analytics import UMAMI_WEBSITE_ID, build_event_payload, track_event


def test_payload_has_website_id_and_data():
    p = build_event_payload("run_analysis", {"addresses_input": "0xabc"})
    assert p["type"] == "event"
    assert p["payload"]["website"] == "d055b0ff-48a4-4617-a9fd-4124a5346705"
    assert p["payload"]["name"] == "run_analysis"
    assert p["payload"]["data"] == {"addresses_input": "0xabc"}


def test_payload_omits_empty_data():
    assert "data" not in build_event_payload("login")["payload"]


def test_track_event_false_on_rejection():
    resp = MagicMock(ok=False, status_code=400, text="bad")
    with patch("src.utils.analytics.requests.post", return_value=resp):
        assert track_event("login") is False


def test_track_event_true_on_success():
    resp = MagicMock(ok=True, status_code=200)
    with patch("src.utils.analytics.requests.post", return_value=resp) as post:
        assert track_event("login") is True
    assert post.call_args.kwargs["json"]["payload"]["website"] == UMAMI_WEBSITE_ID
