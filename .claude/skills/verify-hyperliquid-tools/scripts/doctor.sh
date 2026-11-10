#!/bin/bash
# read-only: is this instance worth driving?
. "$(dirname "$0")/env.sh"; rc=0; cd "$REPO"
ok(){ echo "ok   $*"; }; bad(){ echo "FAIL $*"; rc=1; }
[ -f "$OUT/pid" ] && kill -0 "$(cat "$OUT/pid")" 2>/dev/null && ok "our server pid $(cat "$OUT/pid")" || bad "no server started by launch.sh"
curl -fs "http://localhost:$PORT/_stcore/health" >/dev/null && ok "health on :$PORT" || bad "health on :$PORT"
"$PY" -c "import streamlit;assert streamlit.__version__=='1.48.0',streamlit.__version__" && ok "streamlit 1.48.0" || bad "streamlit version"
"$PY" -c "from src import funding_arbs;assert funding_arbs.AVAILABLE" 2>/dev/null && ok "trading-tools importable" || bad "trading-tools missing: pip install -e ../trading-tools (the Arb page then shows 'unavailable')"
"$PY" -c "import playwright" 2>/dev/null && ok "playwright" || echo "warn playwright missing: use the claude-in-chrome skill or 'pip install playwright' (system Chrome is used, no browser download)"
exit $rc
