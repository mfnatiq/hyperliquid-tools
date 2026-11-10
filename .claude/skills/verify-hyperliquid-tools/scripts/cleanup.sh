#!/bin/bash
# stop only the server launch.sh started, remove scratch db. evidence in $OUT/evidence is kept.
. "$(dirname "$0")/env.sh"
if [ -f "$OUT/pid" ]; then
  pid="$(cat "$OUT/pid")"
  # $! is an msys pid on Windows; taskkill needs the Windows pid, and /T takes the streamlit child with the venv launcher
  if [ -r "/proc/$pid/winpid" ]; then taskkill //PID "$(cat "/proc/$pid/winpid")" //T //F >/dev/null 2>&1; else kill "$pid" 2>/dev/null; fi
  rm -f "$OUT/pid"
fi
rm -f "$OUT/verify.db"
echo "cleaned. evidence kept in $OUT/evidence"
