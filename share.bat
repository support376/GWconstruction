@echo off
chcp 65001 >nul
title GW 건설관리 - 외부 공유 (Cloudflare Tunnel)
cd /d %~dp0

echo ================================================================
echo   GW 건설관리 - 외부 공유 (Cloudflare Quick Tunnel)
echo ================================================================
echo.
echo   주의:
echo   1) 다른 cmd 창에서 run.bat ^(또는 python app.py^) 가
echo      먼저 실행되어 있어야 합니다.
echo   2) 이 창을 닫으면 외부 URL 도 즉시 사라집니다.
echo   3) URL 을 받은 사람은 누구나 접속 가능합니다.
echo      대표님 외 다른 사람에게는 보내지 마세요.
echo   4) 시연용 일회성 URL 입니다 (영구 X). 다시 켜면 새 URL.
echo.

REM ----- cloudflared 자동 다운로드 -----
if not exist "cloudflared.exe" (
  echo [1/2] cloudflared.exe 가 없습니다. 다운로드 받는 중... ^(약 30MB, 1~2분^)
  powershell -NoProfile -Command "Invoke-WebRequest -Uri 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe' -OutFile 'cloudflared.exe' -UseBasicParsing"
  if errorlevel 1 (
    echo.
    echo [다운로드 실패] 아래 URL 로 직접 받아 이 폴더에 cloudflared.exe 로 저장하세요:
    echo   https://github.com/cloudflare/cloudflared/releases
    pause
    exit /b 1
  )
  echo 다운로드 완료.
  echo.
)

echo [2/2] 터널 시작합니다. 잠시 후 아래에
echo       https://xxxxxxxx.trycloudflare.com  같은 URL 이 표시됩니다.
echo       그 URL 을 복사해서 대표님께 보내세요.
echo.
echo   대표님께 보낼 링크 예시:
echo       (URL)/                ^<- 본사용 대시보드/배치보드/지도
echo       (URL)/m               ^<- 폰 출퇴근 (GPS)
echo       (URL)/register        ^<- 직원 신규 가입
echo.
echo ================================================================
echo.

cloudflared.exe tunnel --url http://localhost:8765
pause
