from types import SimpleNamespace

from src.auth.session import sync_login


class User(SimpleNamespace):
    def __contains__(self, k):
        return k in self.__dict__

    def __bool__(self):
        return bool(self.__dict__)


ME = User(email="a@b.co")
NOBODY = User()


def test_login_starts_the_trial_once_and_sets_the_email():
    s, calls = {}, []
    def trial(email, log):
        calls.append(email)
    assert sync_login(s, ME, None, trial) is None
    assert sync_login(s, ME, None, trial) is None
    assert s == {"user_email": "a@b.co"} and calls == ["a@b.co"]


def test_a_failed_trial_start_leaves_the_user_logged_out_and_retries_next_run():
    s, calls = {}, []
    def trial(email, log):
        calls.append(email)
        return "database error" if len(calls) == 1 else None
    assert sync_login(s, ME, None, trial) == "database error"
    assert "user_email" not in s
    assert sync_login(s, ME, None, trial) is None
    assert s["user_email"] == "a@b.co" and calls == ["a@b.co", "a@b.co"]


def test_logout_removes_the_email_and_the_cached_user():
    s = {"user_email": "a@b.co", "user_object": object()}
    assert sync_login(s, NOBODY, None) is None
    assert s == {}


def test_the_trial_page_logs_in_without_starting_a_trial():
    s = {}
    assert sync_login(s, ME, None) is None
    assert s == {"user_email": "a@b.co"}
