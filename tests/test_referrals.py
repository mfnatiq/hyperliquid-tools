from streamlit.testing.v1 import AppTest

from src.utils.render_utils import REFERRALS, donation_address, referral_url


def test_the_donation_address_is_unchanged_because_payments_are_checked_against_it():
    assert donation_address == "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"


def test_referral_url_ignores_case_and_the_rfq_tag_and_is_none_for_an_unknown_venue():
    assert referral_url("extended") == "https://app.extended.exchange/join/MAMBO"
    assert referral_url("Variational (RFQ)") == "https://omni.variational.io/?ref=OMNIMAMBO"
    assert referral_url("Lighter") is None


def test_the_header_popover_shows_every_link_and_code():
    def app():
        from src.utils.render_utils import referrals_popover
        referrals_popover()
    at = AppTest.from_function(app).run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    for r in REFERRALS:
        assert f"[{r['venue']}]({r['url']})" in text and f"`{r['code']}`" in text
