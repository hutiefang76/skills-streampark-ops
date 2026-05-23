#!/usr/bin/env bash
# streampark-ops skill — Setup (Mac/Linux)
# 等价 setup.bat, 创建 .venv + 装依赖 + 引导 config.ini
set -euo pipefail

SKILL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SKILL_DIR/.venv"

echo "============================================================"
echo "  streampark-ops skill — Setup"
echo "============================================================"
echo ""

# === 1. Python ===
echo "[1/3] Checking Python..."
PYTHON_CMD=""
for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1; then
        ver=$("$cand" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "0.0")
        major=$(echo "$ver" | cut -d. -f1)
        minor=$(echo "$ver" | cut -d. -f2)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 8 ]; then
            PYTHON_CMD="$cand"
            echo "[OK] Python $ver ($cand)"
            break
        fi
    fi
done
if [ -z "$PYTHON_CMD" ]; then
    echo "[ERROR] Python 3.8+ not found. Install: https://www.python.org/downloads/" >&2
    exit 1
fi

# === 2. venv + deps ===
echo ""
echo "[2/3] Setting up virtual environment..."
if [ ! -d "$VENV_DIR" ]; then
    "$PYTHON_CMD" -m venv "$VENV_DIR"
    echo "[OK] Virtual environment created"
else
    echo "[OK] Virtual environment exists"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if ! pip install -r "$SKILL_DIR/requirements.txt" -q 2>/dev/null; then
    echo "[!] PyPI failed, trying Tsinghua mirror..."
    pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn \
        -r "$SKILL_DIR/requirements.txt" -q
fi
echo "[OK] Dependencies installed"

# === 3. Config ===
echo ""
echo "[3/3] Checking config.ini..."
if [ -f "$SKILL_DIR/config.ini" ]; then
    echo "[OK] config.ini found"
else
    cp "$SKILL_DIR/config.ini.example" "$SKILL_DIR/config.ini"
    echo "[!] config.ini created from template."
    echo "    Edit $SKILL_DIR/config.ini with real credentials before use."
fi

echo ""
echo "============================================================"
echo "[OK] Setup complete"
echo ""
echo "Usage:"
echo "  $VENV_DIR/bin/python $SKILL_DIR/scripts/sp_apps_list.py --env local"
echo "  $VENV_DIR/bin/python $SKILL_DIR/scripts/sp_app_show.py --env local --name <job>"
echo ""
echo "Demo (本地 docker 起 StreamPark):"
echo "  cd <frank-repo>/deploy/test-stack && docker compose up -d streampark"
echo "============================================================"
