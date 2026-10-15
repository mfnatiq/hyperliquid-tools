from streamlit.testing.v1 import AppTest

from src.utils.render_utils import REFERRALS, donation_address, referral_url


def test_the_donation_address_is_unchanged_because_payments_are_checked_against_it():
    assert donation_address == "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"


def test_every_referral_has_an_https_link_and_a_code_and_each_venue_appears_once():
    assert [r["venue"] for r in REFERRALS] == [
        "Variational", "TxFlow", "Phoenix", "RISEx", "Extended", "Nado", "Pacifica", "Bullpen",
    ]
    assert all(r["url"].startswith("https://") and r["code"] for r in REFERRALS)


def test_referral_url_finds_a_venue_whatever_its_case_and_is_none_for_an_unknown_one():
    assert referral_url("extended") == "https://app.extended.exchange/join/MAMBO"
    assert referral_url("VARIATIONAL") == "https://omni.variational.io/?ref=OMNIMAMBO"
    assert referral_url("txflow") == "https://app.txflow.com/r/EJFMW1T960"
    assert referral_url("Hyperliquid") is None


def test_referral_url_ignores_the_rfq_tag_the_liquidity_tables_add():
    assert referral_url("Variational (RFQ)") == "https://omni.variational.io/?ref=OMNIMAMBO"


def test_the_header_popover_shows_every_link_and_code_but_no_donation_address():
    def app():
        from src.utils.render_utils import referrals_popover
        referrals_popover()
    at = AppTest.from_function(app).run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    assert at.markdown[0].value == "Signing up through these links supports the site"
    for r in REFERRALS:
        assert f"[{r['venue']}]({r['url']})" in text and f"`{r['code']}`" in text
    assert not at.code


def test_the_footer_shows_the_maker_and_the_donation_address_with_a_copy_button():
    def app():
        from src.utils.render_utils import site_footer
        site_footer()
    at = AppTest.from_function(app).run()
    assert not at.exception
    assert "[@fnatiqmambo](https://x.com/fnatiqmambo)" in at.markdown[0].value
    assert [c.value for c in at.code] == [donation_address]


def test_venue_referral_shows_a_sign_up_link_only_for_a_venue_with_a_referral():
    def app():
        from src.utils.render_utils import venue_referral
        venue_referral("Extended")
        venue_referral("Hyperliquid")
    at = AppTest.from_function(app).run()
    assert [c.value for c in at.caption] == [
        "New to Extended? [Sign up here](https://app.extended.exchange/join/MAMBO) to support the site"
    ]
