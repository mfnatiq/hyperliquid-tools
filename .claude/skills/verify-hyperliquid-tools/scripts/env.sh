# sourced by the other scripts. never prints secrets.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
if [ -x "$REPO/venv/Scripts/python.exe" ]; then PY="$REPO/venv/Scripts/python.exe"; elif [ -x "$REPO/venv/bin/python" ]; then PY="$REPO/venv/bin/python"; else PY="$(command -v python)"; fi
PORT="${VERIFY_PORT:-8599}"
OUT="${VERIFY_OUT:-${TMPDIR:-/tmp}/hl-tools-verify}"   # evidence + scratch db + pid file. cleanup never deletes $OUT/evidence
mkdir -p "$OUT/evidence"
command -v cygpath >/dev/null && OUT="$(cygpath -m "$OUT")"   # Windows python cannot open msys-style /tmp paths
