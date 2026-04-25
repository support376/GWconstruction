# Render 배포 가이드

이 문서대로 따라하면 약 10분 만에 외부에서 접속 가능한 URL이 나옵니다.

---

## 0. 미리 준비

- [ ] GitHub 계정 (있다고 답하셨음)
- [ ] Render.com 계정 — https://render.com 에서 GitHub 계정으로 1초 가입
- [ ] **GitHub Desktop** 설치 (가장 쉬운 길) — https://desktop.github.com

> 명령줄(Git CLI)이 익숙하시면 4번 항목 끝에 CLI 버전도 같이 적어뒀습니다.

---

## 1. GitHub Desktop 으로 코드 올리기 (5분)

1. **GitHub Desktop** 실행 → 첫 화면에서 GitHub 계정 로그인
2. 상단 메뉴 `File` → `Add local repository...`
3. 폴더 선택 창에서 **`C:\Users\user\Desktop\claude\GWcorporation\gw-construction-mgmt`** 선택
4. "이 폴더는 Git 저장소가 아니다" 같은 경고가 뜨면 **"create a repository"** 링크 클릭
5. 저장소 정보 입력:
   - Name: `gw-construction-mgmt` (그대로)
   - Description: 비워둠
   - Git ignore: `None` (이미 우리가 만든 .gitignore 가 있음)
   - License: `None`
6. **Create Repository** 클릭
7. 화면 가운데에 변경된 파일 목록이 쭉 뜸. 좌측 하단에:
   - Summary: `Initial commit` 입력
   - **Commit to main** 클릭
8. 상단에 **Publish repository** 파란 버튼이 뜸 → 클릭
9. 팝업에서:
   - Name: `gw-construction-mgmt`
   - **"Keep this code private"** 체크 ✅ (회사 코드라서 비공개로)
   - **Publish Repository** 클릭

→ GitHub.com 의 본인 계정에 `gw-construction-mgmt` 저장소가 생겼습니다.

> **Git CLI 버전 (선택):** 폴더에서 cmd 열고:
> ```
> git init
> git add .
> git commit -m "Initial commit"
> git remote add origin https://github.com/<본인아이디>/gw-construction-mgmt.git
> git branch -M main
> git push -u origin main
> ```

---

## 2. Render 에서 새 서비스 만들기 (3분)

1. https://render.com 접속 → **Sign in with GitHub** 로그인
2. 우측 상단 **New +** → **Web Service**
3. **Connect a repository** 화면이 뜸:
   - 처음이면 "Configure account" 클릭 → GitHub에서 Render 가 어떤 저장소에 접근할지 묻는 화면 → **Only select repositories** → `gw-construction-mgmt` 선택 → Save
   - 다시 Render 로 돌아옴, 저장소 목록에 `gw-construction-mgmt` 가 보임
4. 그 저장소 옆 **Connect** 클릭
5. 설정 화면이 뜨는데, **`render.yaml` 을 자동 인식해서 대부분 채워져 있음**:
   - Name: `gw-construction-mgmt`
   - Region: 가까운 곳 (Singapore 추천 — 한국에서 가장 빠름)
   - Branch: `main`
   - Runtime: Python
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `cd backend && uvicorn app:app --host 0.0.0.0 --port $PORT`
   - Plan: **Free**
6. 맨 아래 **Create Web Service** 클릭

---

## 3. 배포 진행 (3~5분, 자동)

화면에 로그가 줄줄 흐릅니다:

```
==> Cloning from https://github.com/...
==> Installing Python version 3.11.x...
==> Running build command 'pip install -r requirements.txt'...
... (의존성 설치)
==> Build successful 🎉
==> Deploying...
==> Running 'cd backend && uvicorn app:app...'
[startup] 샘플 데이터 자동 입력 완료
INFO: Uvicorn running on http://0.0.0.0:10000
==> Your service is live 🎉
```

상단에 **`https://gw-construction-mgmt-xxxx.onrender.com`** 같은 URL 이 뜨면 끝.

---

## 4. 대표님께 보낼 링크

```
대표님, 1차 시연용입니다.

본사 화면:    https://gw-construction-mgmt-xxxx.onrender.com/
폰 출퇴근:    https://gw-construction-mgmt-xxxx.onrender.com/m
직원 가입:    https://gw-construction-mgmt-xxxx.onrender.com/register

폰 GPS 권한 허용해야 출퇴근 거리가 계산됩니다.
처음 접속 시 30초~1분 정도 로딩이 걸립니다 (무료 티어 cold start).
```

---

## 5. 무료 티어 한계 (꼭 알아두실 것)

- ⏱️ **15분 동안 아무도 접속 안 하면 잠듦.** 다음 접속 시 30~60초 걸려서 깨어남. 시연 직전에 한 번 들어가놓으면 깨어있음.
- 💾 **재시작 때마다 DB 가 초기화됨** (free tier 는 영구 디스크 X). 매번 샘플 데이터로 깔끔하게 시작함 — 시연용으로는 오히려 깔끔함.
- 🔁 **GitHub 에 push 하면 자동 재배포.** 코드 수정하고 GitHub Desktop 에서 commit + push 누르면 1~2분 뒤 반영됨.

영구 데이터가 필요해지면(실제 도입 단계):
- Render 유료 디스크 ($1/월, 1GB) 추가하면 SQLite 그대로 영구 보관
- 또는 Render Postgres ($7/월) 로 교체

---

## 6. 자주 막히는 곳

**Q. GitHub Desktop 에서 Publish 가 안 됨 (붉은 글씨)**  
A. 사내 네트워크가 GitHub HTTPS 를 막은 경우. 핫스팟 켜고 재시도, 또는 SSH 키 방식으로 변경.

**Q. Render 가 빌드 실패: "Could not find a version that satisfies..."**  
A. requirements.txt 에 적힌 버전이 Python 3.11 과 충돌. 보통 `requirements.txt` 에서 버전 핀 제거하면 해결:
```
fastapi
uvicorn[standard]
pydantic
```

**Q. 배포 후 페이지가 빈 화면**  
A. 로그 보세요. `FileNotFoundError: ... index.html` 이면 frontend/ 가 GitHub 에 안 올라간 것 — `.gitignore` 확인.

**Q. 첫 접속에 1분 넘게 걸림**  
A. 무료 티어 cold start. 정상. 한 번 깨면 15분간은 즉시 응답.
