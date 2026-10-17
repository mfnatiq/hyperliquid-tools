from streamlit.testing.v1 import AppTest

from src.utils.render_utils import REFERRALS, _md, donation_address, referral, trade_button


def test_the_donation_address_is_unchanged_because_payments_are_checked_against_it():
    assert donation_address == "0xB17648Ed98C9766B880b5A24eEcAebA19866d1d7"


def test_referral_ignores_case_and_the_rfq_tag_and_is_none_for_an_unknown_venue():
    assert referral("extended")["url"] == "https://app.extended.exchange/join/MAMBO"
    assert referral("Variational (RFQ)")["url"] == "https://omni.variational.io/?ref=OMNIMAMBO"
    assert referral("Lighter on Robinhood") is None


def test_trade_button_links_the_referral_with_the_perk_on_hover_and_is_empty_without_a_referral():
    assert trade_button("Extended") == (' <a class="trade" href="https://app.extended.exchange/join/MAMBO" title="10% off fees"'
                                        ' target="_blank" rel="noopener">Trade ↗</a>')
    assert 'href="https://app.lighter.xyz/?referral=MAMBO&amp;source=none"' in trade_button("Lighter")
    assert trade_button("RISEx") == ' <a class="trade" href="https://www.rise.trade/invite/mambo" target="_blank" rel="noopener">Trade ↗</a>'
    assert trade_button("Lighter on Robinhood") == ""


def test_md_escapes_dollars_so_markdown_does_not_start_a_maths_block():
    assert _md("first $25M volume") == "first \\$25M volume"


def test_the_header_popover_shows_every_link_and_code():
    def app():
        from src.utils.render_utils import referrals_popover
        referrals_popover()
    at = AppTest.from_function(app).run()
    assert not at.exception
    text = " ".join(m.value for m in at.markdown)
    for r in REFERRALS:
        assert f"[{r['venue']}]({r['url']})" in text and f"`{r['code']}`" in text
