"""drive the Arb Opportunities page like a user. usage: python drive_arb.py [port] [out_dir]
steps: open the page, wait for rows (first venue build can take a minute or more), screenshot, press the first Details
button, screenshot the drawer, then check the recorder side effect in the throwaway sqlite db. exit 1 on any failure."""
import sqlite3, sys, time
from pathlib import Path
from playwright.sync_api import sync_playwright

port = sys.argv[1] if len(sys.argv) > 1 else "8599"
out = Path(sys.argv[2] if len(sys.argv) > 2 else "evidence"); out.mkdir(parents=True, exist_ok=True)
url = f"http://localhost:{port}/arb_opportunities"

with sync_playwright() as p:
    b = p.chromium.launch(channel="chrome", headless=True)  # system Chrome, no download
    pg = b.new_page(viewport={"width": 1500, "height": 900})
    pg.goto(url)
    pg.get_by_text("Funding Arb Opportunities").wait_for(timeout=60_000)
    pg.screenshot(path=str(out / "1-loading-or-rows.png"))
    details = pg.get_by_role("button", name="Details ›")
    details.first.wait_for(timeout=240_000)  # the background snapshot build polls every 3s
    n = details.count()
    pg.screenshot(path=str(out / "2-table.png"), full_page=True)
    details.first.click()
    drawer = pg.locator('div[data-testid="stDialog"] div[role="dialog"]')
    drawer.wait_for(timeout=30_000)
    text = drawer.inner_text()
    pg.screenshot(path=str(out / "3-detail-drawer.png"))
    for needle in ("Net APR", "Break-even", "Round trip", "Carry averages", "Cost per leg"):
        assert needle in text, f"detail panel lacks {needle!r}: {text[:300]!r}"
    # a fresh db has no history, so the averages read "No history yet"; 24h/7d/30d appear only once samples exist
    db = Path(sys.argv[3]) if len(sys.argv) > 3 else None  # optional: the throwaway sqlite db, to check the recorder
    if db:
        for _ in range(60):  # the recorder first runs 60s after boot, then hourly
            try:
                rows = sqlite3.connect(db).execute("select count(*) from funding_samples").fetchone()[0]
            except sqlite3.Error:
                rows = 0
            if rows:
                break
            time.sleep(5)
        assert rows > 0, "recorder stored no funding_samples within 5 minutes"
        print(f"OK funding_samples rows: {rows}")
    print(f"OK rows with Details: {n}; drawer text starts: {text[:80]!r}")
    b.close()
