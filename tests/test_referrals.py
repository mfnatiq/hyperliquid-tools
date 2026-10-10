from streamlit.testing.v1 import AppTest

from src.utils.render_utils import REFERRALS, donation_address, referral_url


def test_the_donation_address_is_unchanged_because_payments_are_checked_against_it():
    assert donation_address == "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"


def test_every_referral_has_an_https_link_and_a_code_and_each_venue_appears_once():
    assert [r["venue"] for r in REFERRALS] == ["Variational", "Extended", "Pacifica", "Bullpen"]
    assert all(r["url"].startswith("https://") and r["code"] for r in REFERRALS)


def test_referral_url_finds_a_venue_whatever_its_case_and_is_none_for_an_unknown_one():
    assert referral_url("extended") == "https://app.extended.exchange/join/MAMBO"
    assert referral_url("VARIATIONAL") == "https://omni.variational.io/?ref=OMNIMAMBO"
    assert referral_url("Hyperliquid") is None


def test_the_header_popover_shows_every_link_code_and_the_donation_address():
    def app():
        from src.utils.render_utils import support_popover
        support_popover()
    at = AppTest.from_function(app).run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    for r in REFERRALS:
        assert f"[{r['venue']}]({r['url']})" in text and f"`{r['code']}`" in text
    assert [c.value for c in at.code] == [donation_address]
