#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/Tripletex}"
BRANCH="${BRANCH:-main}"
SERVICE_NAME="${SERVICE_NAME:-tt-susoft.service}"

echo "[deploy] app dir: $APP_DIR"
cd "$APP_DIR"

echo "[deploy] fetching latest code"
git fetch --all --prune
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

if [ ! -d ".venv" ]; then
  echo "[deploy] creating virtual environment"
  python3 -m venv .venv
fi

echo "[deploy] installing dependencies"
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "[deploy] running tests"
.venv/bin/python -m unittest discover -s tests -v

echo "[deploy] restarting service: $SERVICE_NAME"
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager -l

echo "[deploy] done"
