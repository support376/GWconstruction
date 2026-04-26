@echo off
chcp 65001 > nul
setlocal

REM Local dev server with auto-reload.
REM   1) Double-click this file (or run from cmd)
REM   2) Open http://localhost:8765
REM   3) Edit code -> save -> server auto-reloads -> browser F5
REM   4) git push only when you are happy with the result
REM
REM First time: run "pip install -r requirements.txt" once.

pushd "%~dp0backend"
if errorlevel 1 (
  echo [ERROR] Cannot enter backend folder: %~dp0backend
  pause
  exit /b 1
)

echo.
echo ============================================================
echo   GW Construction - Local Dev Server
echo ============================================================
echo   URL:    http://localhost:8765
echo   Login:  admin / admin1234
echo   Edit code, save, browser F5. Ctrl+C to stop.
echo ============================================================
echo.

python -m uvicorn app:app --reload --reload-dir . --host 127.0.0.1 --port 8765

popd
pause
