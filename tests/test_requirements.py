from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def lines(name):
    return [x.strip() for x in (ROOT / name).read_text().splitlines() if x.strip() and not x.startswith("#")]


def test_main_requirements_need_no_token():
    assert not [x for x in lines("requirements.txt") if "trading-tools" in x or "GH_TOKEN" in x]


def test_trading_tools_is_pinned_to_a_commit_in_its_own_file():
    (line,) = lines("requirements-trading-tools.txt")
    assert line.startswith("trading-tools @ git+https://${GH_TOKEN}@github.com/mfnatiq/trading-tools@")
    assert len(line.rsplit("@", 1)[1]) == 40
