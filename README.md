# GW 건설관리 시스템 (로컬 프로토타입)

전문건설업 그룹사용 인력·현장·기성 통합관리 시스템의 1차 프로토타입.

## 무엇이 되는가

- **대시보드** — 법인별·현장별 운영 현황, 계약·수금·잔액 한눈에
- **프로젝트 개요** — 현장을 큰 카드로. 간트형 일정바, 누적 인일·노무비, 예상 손익
- **인력 배치 보드** — 사람을 끌어다 놓아 현장에 배치 (드래그앤드롭 + 빠른 이동 버튼)
- **계획 / 실적 / 신고** 3종 트래킹 — 한 화면에서 탭으로 전환, 서로 복사 가능
- **직원 관리** — 일용직·사무직 통합 마스터 (인적사항·연락처·일당·계좌)
- **현장 관리** — GPS 좌표·지오펜스·계약금액·기성, 주소→좌표 자동 변환
- **현장 지도** — 모든 현장 위치, 지오펜스 원, 오늘 출근 GPS 점, 현장별 출역 명단
- **모바일 출퇴근** — 폰에서 `/m` 접속, GPS 로 현장 반경 검증 후 출/퇴근
- **직원 자가 가입** — 폰에서 `/register` 접속, 이름+번호로 신규 가입

## 실행 방법 (Windows)

1. Python 3.10+ 설치 (https://www.python.org/downloads/)
2. `run.bat` 더블클릭
3. 자동으로 가상환경 생성 → 패키지 설치 → 샘플 데이터 입력 → 서버 시작
4. 브라우저에서 `http://localhost:8765/` 접속

## 같은 와이파이의 폰/현장 PC 에서 접속

`run.bat` 실행 화면에 `IPv4` 라고 뜨는 본사 PC IP 를 확인 (예: `192.168.0.42`).
폰 브라우저에서 `http://192.168.0.42:8765/m` 으로 접속하면 출퇴근 화면이 뜹니다.

> 폰에서 GPS 권한을 허용해야 거리가 계산됩니다. 사파리/크롬 모두 동작.

## 외부 (대표·다른 직원) 에게 시연용 링크 보내기

다른 사무실/도시의 사람에게도 보여주고 싶을 때.

1. `run.bat` 으로 서버를 띄워 둔 상태에서
2. **`share.bat` 더블클릭** — 처음에는 cloudflared.exe (약 30MB)를 자동 다운로드
3. 잠시 후 `https://xxxxxxxx.trycloudflare.com` 같은 URL 이 화면에 출력
4. 그 URL 을 카톡/문자로 보내면 끝. HTTPS 라 폰에서도 GPS 권한 잘 동작

대표님께 보낼 링크:
- `(URL)/` → 본사용 화면 (대시보드, 프로젝트, 배치보드, 지도)
- `(URL)/m` → 폰 출퇴근 화면
- `(URL)/register` → 신규 직원 가입 화면

⚠️ 주의:
- URL 은 `share.bat` 창을 닫으면 사라집니다 (재실행하면 새 URL)
- 시연·검토용입니다. URL 받은 사람은 누구나 접속 가능 — 신뢰할 사람에게만
- 정식 운영용 호스팅은 아래 "원격 배포" 섹션 참고

## 폴더

```
gw-construction-mgmt/
├── backend/
│   ├── app.py              # FastAPI 서버 (모든 API)
│   ├── seed.py             # 샘플 데이터
│   └── construction.db     # SQLite DB (자동 생성)
├── frontend/
│   ├── index.html          # 본사용 SPA
│   ├── mobile.html         # 모바일 출퇴근 화면
│   ├── styles.css
│   └── app.js
├── requirements.txt
├── run.bat / run.sh   # 서버 시작
├── share.bat          # 외부 시연용 URL 만들기 (Cloudflare Tunnel)
└── README.md
```

## 데이터 초기화

`backend/construction.db` 파일을 삭제하고 `run.bat` 다시 실행하면 샘플 데이터부터 다시 시작합니다.

## 다음 단계 (원격 배포)

지금은 노트북 한 대에서 도는 1차 버전입니다. 같은 와이파이 안의 폰까지만 접속 가능합니다.
실제로 어디서나 접속하려면 다음이 필요합니다.

1. **클라우드 배포** — AWS/GCP/Naver Cloud 작은 인스턴스 (월 1~3만원 수준)에 같은 코드 올리기
2. **HTTPS 인증서** — Let's Encrypt 무료 인증서, 도메인 1개 필요
3. **로그인 / 권한** — 지금은 누구나 접속 가능. 사번 + 비밀번호 + 역할별 권한 추가
4. **DB 교체** — SQLite → PostgreSQL (동시접속 안정성)
5. **백업** — 일 1회 자동 백업

코드 구조는 SQLite 만 PostgreSQL 로 바꾸면 그대로 동작하도록 작성되어 있습니다.

## API 빠른 참조

`http://localhost:8765/docs` 에서 자동 생성된 API 문서를 볼 수 있습니다 (FastAPI 기본 제공).

주요 엔드포인트:

- `GET  /api/dashboard` — 전사 KPI
- `GET  /api/workers` — 직원 목록
- `GET  /api/sites` — 현장 목록
- `GET  /api/deployments?date=YYYY-MM-DD&kind=plan|actual|reported`
- `POST /api/deployments` — 배치 (worker_id, site_id, date, kind)
- `POST /api/deployments/copy` — kind 간 복사
- `POST /api/clock` — 출/퇴근 (worker_id, site_id, lat, lng, direction)
