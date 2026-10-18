from streamlit.testing.v1 import AppTest

from src.utils.welcome import FOLLOW_TEXT, REMEMBER_JS


def test_the_remember_script_sets_a_one_year_cookie_on_the_parent_page():
    assert 'window.parent.document.cookie = "hide_welcome=1; max-age=31536000; path=/; SameSite=Lax";' in REMEMBER_JS


def test_the_follow_text_links_the_twitter_profile_and_thanks_referral_users():
    assert FOLLOW_TEXT == ("If you like what you see, please [follow me on Twitter](https://x.com/fnatiqmambo). "
                           "And if you use my referral links, thank you!")


def test_the_banner_shows_without_the_cookie():
    def app():
        import streamlit as st
        from src.utils.welcome import welcome_hidden
        st.write("hidden" if welcome_hidden() else "shown")
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert at.markdown[0].value == "shown"


def test_the_banner_stays_hidden_with_the_cookie_and_shows_for_any_other_value():
    from types import SimpleNamespace
    from unittest.mock import patch

    from src.utils.welcome import welcome_hidden
    for cookies, expected in [({"hide_welcome": "1"}, True), ({"hide_welcome": "0"}, False), ({}, False)]:
        fake_st = SimpleNamespace(context=SimpleNamespace(cookies=cookies))
        with patch("src.utils.welcome.st", fake_st):
            assert welcome_hidden() is expected
