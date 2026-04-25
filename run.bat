@echo off
chcp 65001 >nul
title GW 건설관리 시스템
cd /d %~dp0

echo ================================================================
echo   GW 건설관리 시스템 - 로컬 서버 시작
echo ================================================================
echo.

REM Python 확인
python --version >nul 2>&1
if errorlevel 1 (
  echo [오류] Python 3 가 설치되어 있지 않습니다.
  echo https://www.python.org/downloads/  에서 Python 3.10+ 설치 후 다시 실행하세요.
  pause
  exit /b 1
)

REM 가상환경
if not exist ".venv" (
  echo [1/3] 가상환경 생성 중...
  python -m venv .venv
)

call .venv\Scripts\activate.bat

REM 패키지 설치 (최초 1회)
if not exist ".venv\.installed" (
  echo [2/3] 패키지 설치 중... (1~2분 소요)
  python -m pip install --upgrade pip
  python -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [오류] 패키지 설치 실패.
    pause
    exit /b 1
  )
  echo. > .venv\.installed
)

REM 시드 데이터 (DB가 없을 때만)
if not exist "backend\construction.db" (
  echo [3/3] 샘플 데이터 입력 중...
  cd backend
  python seed.py
  cd ..
)

echo.
echo ================================================================
echo   서버 시작! 브라우저에서 아래 주소 열기:
echo.
echo     본사 PC:    http://localhost:8765/
echo     같은 와이파이 폰:    http://[이 PC 의 IP]:8765/m
echo.
echo   같은 와이파이의 다른 기기에서 접속하려면 이 PC IP 를 확인:
echo.
ipconfig | findstr /i "IPv4"
echo.
echo   종료: Ctrl + C
echo ================================================================
echo.

cd backend
python app.py
pause
