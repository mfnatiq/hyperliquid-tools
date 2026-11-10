#!/bin/bash
# start the real site on $PORT with a throwaway sqlite db. prints READY when the Streamlit health endpoint answers.
. "$(dirname "$0")/env.sh"
if [ -f "$OUT/pid" ] && kill -0 "$(cat "$OUT/pid")" 2>/dev/null; then echo "already running pid $(cat "$OUT/pid"); run cleanup.sh first"; exit 1; fi
if curl -fs "http://localhost:$PORT/_stcore/health" >/dev/null; then echo "port $PORT is owned by someone else; set VERIFY_PORT"; exit 1; fi
cd "$REPO"
rm -f "$OUT/verify.db"
DATABASE_URL="sqlite:///$OUT/verify.db" nohup "$PY" -m streamlit run home.py --server.port "$PORT" --server.headless true \
  --browser.gatherUsageStats false > "$OUT/server.log" 2>&1 &
echo $! > "$OUT/pid"
for i in $(seq 1 60); do
  curl -fs "http://localhost:$PORT/_stcore/health" >/dev/null && { echo "READY http://localhost:$PORT (pid $(cat "$OUT/pid"), log $OUT/server.log)"; exit 0; }
  sleep 1
done
echo "NOT READY after 60s"; tail -20 "$OUT/server.log"; exit 1
