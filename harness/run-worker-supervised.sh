#!/usr/bin/env bash
# Restart a worker after crashes or an observable hard-limit exit.
set -euo pipefail

MAX_RESTARTS=3
BACKOFF_SECS=2
STATUS_FILE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --max-restarts)
      MAX_RESTARTS="$2"
      shift 2
      ;;
    --backoff-secs)
      BACKOFF_SECS="$2"
      shift 2
      ;;
    --status-file)
      STATUS_FILE="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "run-worker-supervised: unknown option $1" >&2
      exit 64
      ;;
  esac
done

if [ "$#" -eq 0 ]; then
  echo "usage: run-worker-supervised.sh [options] -- <worker command...>" >&2
  exit 64
fi

restarts=0
while true; do
  started=$SECONDS
  "$@" &
  child=$!
  trap 'kill -TERM "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; exit 130' INT TERM
  set +e
  wait "$child"
  code=$?
  set -e
  trap - INT TERM

  if [ "$code" -eq 0 ]; then
    exit 0
  fi

  elapsed=$((SECONDS - started))
  hard_limit=no
  if [ -n "$STATUS_FILE" ] && [ -f "$STATUS_FILE" ] && grep -q '"hard_limit_tripped": "' "$STATUS_FILE"; then
    hard_limit=yes
  fi
  echo "run-worker-supervised: worker exited code=$code elapsed=${elapsed}s hard_limit=$hard_limit restart=$restarts/$MAX_RESTARTS" >&2

  if [ "$restarts" -ge "$MAX_RESTARTS" ]; then
    echo "run-worker-supervised: restart budget exhausted" >&2
    exit "$code"
  fi
  restarts=$((restarts + 1))
  sleep "$BACKOFF_SECS"
done
