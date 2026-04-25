#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
echo "================================================================"
echo "  GW 건설관리 시스템 - 로컬 서버 시작"
echo "================================================================"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[오류] python3 가 필요합니다."
  exit 1
fi

if [ ! -d ".venv" ]; then
  echo "[1/3] 가상환경 생성..."
  python3 -m venv .venv
fi

source .venv/bin/activate

if [ ! -f ".venv/.installed" ]; then
  echo "[2/3] 패키지 설치..."
  pip install --upgrade pip >/dev/null
  pip install -r requirements.txt
  touch .venv/.installed
fi

if [ ! -f "backend/construction.db" ]; then
  echo "[3/3] 샘플 데이터 입력..."
  (cd backend && python seed.py)
fi

echo
echo "서버 시작! 브라우저:  http://localhost:8765/"
echo "같은 와이파이 폰에서:  http://$(hostname -I | awk '{print $1}'):8765/m"
echo
cd backend
python app.py
