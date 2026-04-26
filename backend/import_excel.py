"""
엑셀 4종 → 시스템 일괄 임포트
- 직원,주주명부-5개회사.xlsx → workers + shareholders + worker_certifications
- 회사별아웃라인-(기술인력보유현황포함).xlsx → companies 보강 + 기술자 보강
- 건우건설급여대장.xlsx → 직원명부 보강 (계좌·예금주·전화)
- 2025년10월-일용-총괄.xlsx → (다음 라운드)

멱등성: 이미 있는 행은 갱신, 없으면 추가. 한 번 더 돌려도 안전.
"""
import os, re, sqlite3, json
from datetime import datetime, date
from pathlib import Path

try:
    import openpyxl
except ImportError:
    openpyxl = None

DATA_DIR = Path(__file__).parent / "initial_data"

# ====== 회사명 → DB 매칭 (시드와 일치하는 표준명) ======
COMPANY_NAME_MAP = {
    "건우건설": "건우건설주식회사",
    "건우건설(주)": "건우건설주식회사",
    "건우건설㈜": "건우건설주식회사",
    "건우건설주식회사": "건우건설주식회사",
    "아이엔": "(주)아이엔건설환경",
    "(주)아이엔": "(주)아이엔건설환경",
    "(주)아이엔건설환경": "(주)아이엔건설환경",
    "아이엔건설환경": "(주)아이엔건설환경",
    "인우": "인우건설(주)",
    "인우건설": "인우건설(주)",
    "인우건설(주)": "인우건설(주)",
    "인우건설㈜": "인우건설(주)",
    "새암": "새암건설(주)",
    "새암건설": "새암건설(주)",
    "새암건설(주)": "새암건설(주)",
    "새암건설㈜": "새암건설(주)",
    "다우": "다우건설(주)",
    "다우건설": "다우건설(주)",
    "다우건설(주)": "다우건설(주)",
    "유신건설": "유신건설(주)",  # 시드에 없음 → 자동 추가됨
    "유신건설(주)": "유신건설(주)",
    "유신건설㈜": "유신건설(주)",
    "유신": "유신건설(주)",
}

# ====== 자격증명 정규화 ======
def parse_certifications(cert_str):
    """
    '건축,토목 초급'  → [{name:'건축', level:'초급'}, {name:'토목', level:'초급'}]
    '토목특급,건축초급' → [{name:'토목', level:'특급'}, {name:'건축', level:'초급'}]
    '굴삭기' → [{name:'굴삭기'}]
    """
    if not cert_str: return []
    s = str(cert_str).strip()
    if not s: return []
    levels = ['특급', '고급', '중급', '초급', '기능사', '기사', '기술사', '산업기사']
    out = []
    # 콤마 분리
    parts = re.split(r'[,/、·]+', s)
    last_level = None
    # 두 번 패스: 마지막에 명시된 레벨이 앞에도 적용되는 경우 ('건축,토목 초급')
    parsed_parts = []
    for p in reversed(parts):
        p = p.strip()
        if not p: continue
        level = None
        name = p
        for lv in levels:
            if lv in p:
                level = lv
                name = p.replace(lv, '').strip()
                break
        if level is None and last_level:
            level = last_level
        if level: last_level = level
        parsed_parts.append({"name": name or p, "level": level})
    return list(reversed(parsed_parts))

def normalize_date(d):
    """엑셀 날짜를 ISO로. '23.07.01', datetime, '24.10.31' 등."""
    if d is None: return None
    if isinstance(d, datetime):
        return d.date().isoformat()
    if isinstance(d, date):
        return d.isoformat()
    s = str(d).strip()
    if not s: return None
    # 첫 줄만 (multi-line인 경우)
    s = s.split('\n')[0].strip()
    # YY.MM.DD → 20YY-MM-DD
    m = re.match(r'^(\d{2})\.(\d{1,2})\.(\d{1,2})$', s)
    if m:
        yy, mm, dd = m.groups()
        year = 2000 + int(yy) if int(yy) < 50 else 1900 + int(yy)
        return f"{year:04d}-{int(mm):02d}-{int(dd):02d}"
    # YYYY-MM-DD or YYYY/MM/DD
    m = re.match(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})', s)
    if m:
        y, mm, dd = m.groups()
        return f"{int(y):04d}-{int(mm):02d}-{int(dd):02d}"
    return None

def pick_last_date(d):
    """multi-line 날짜에서 가장 마지막(최신) 날짜를 ISO로.
    '23.08.01\\n24.06.16' → '2024-06-16'
    '24.02.29 08.17 24' → '2024-08-17' (공백 분리도 처리)"""
    if d is None: return None
    if isinstance(d, (datetime, date)):
        return normalize_date(d)
    s = str(d).strip()
    if not s: return None
    # 줄/공백/콤마 모두 구분자
    parts = re.split(r'[\n\r,]+', s)
    candidates = []
    for part in parts:
        # 한 줄 안에 여러 'YY.MM.DD' 가 공백으로 구분된 경우도
        for tok in re.findall(r'\d{2,4}[\.\-/]\d{1,2}[\.\-/]\d{1,2}', part):
            iso = normalize_date(tok)
            if iso: candidates.append(iso)
    if not candidates:
        # 마지막 한 줄에 정규화 시도
        return normalize_date(parts[-1].strip())
    return max(candidates)  # ISO 문자열 비교 = 날짜순 정렬

def clean_name(n):
    if not n: return None
    s = str(n).strip()
    s = re.sub(r'^\d+\.\s*', '', s)  # "1.안 경 이" → "안 경 이"
    s = re.sub(r'\s+', '', s)         # 공백 제거 → "안경이"
    return s or None

def get_or_create_company(c, name, strict=True):
    """COMPANY_NAME_MAP 으로 표준화 후 companies 테이블에서 찾기/생성.
    strict=True: 매핑에 없는 이름이면 None 반환 (잘못된 회사 자동 생성 방지)."""
    if not name: return None
    raw = str(name).strip()
    # bracketed: [건우건설]
    m = re.search(r'\[(.+?)\]', raw)
    if m:
        key = m.group(1).strip()
    else:
        key = raw.split('\n')[0].strip()
    # 정규화 후 매핑
    standard = COMPANY_NAME_MAP.get(key)
    if not standard:
        # 매핑에 정확히 없으면 키워드 기반 휴리스틱 (포함 검사)
        for k, v in COMPANY_NAME_MAP.items():
            if k in key or key in k:
                standard = v
                break
    if not standard:
        if strict: return None
        standard = key
    row = c.execute("SELECT id FROM companies WHERE name=?", (standard,)).fetchone()
    if row: return row[0]
    cur = c.execute("INSERT INTO companies(name) VALUES(?)", (standard,))
    return cur.lastrowid

# 주주명부에서 제외할 라벨·헤더
SHAREHOLDER_EXCLUDE = {
    '직위', '이사', '감사', '주주', '대표', '대표이사', '사내이사', '사외이사',
    '관계', '성명', '주민번호', '주소', '주식수', '지분율', '보유종목', '비고',
    '소계', '합계', '계', '회사', '회사별', '구분', '종목', '직책',
    '철콘', '상하수도', '비계', '구조물', '실내건축', '도장습식', '도장방수',
    '석면해체', '지반조성', '토목', '건축', '전기', '정보통신', '소방',
    '주주명부', '주식의총수', '주식의', '총수', '회 사 별', '지위',
}

# ====== 1) 직원,주주명부 임포트 ======
def wipe_employee_data(c):
    """직원/자격증/면허등재/주주를 모두 삭제.
    회사·면허·일용직 출퇴근/배치 등은 유지. (worker_type='daily' 도 같이 와이프 — 직원 전체 재구축)
    """
    deleted = {}
    for tbl in ["license_workers", "worker_certifications", "shareholders"]:
        cur = c.execute(f"DELETE FROM {tbl}")
        deleted[tbl] = cur.rowcount
    # workers 는 ON DELETE CASCADE 로 deployments/attendances 등도 삭제됨
    cur = c.execute("DELETE FROM workers")
    deleted["workers"] = cur.rowcount
    return deleted

def import_employee_directory(c, path, replace=False):
    if not path.exists():
        print(f"  [skip] {path.name} 없음")
        return {"workers_added": 0, "workers_updated": 0, "certs_added": 0,
                "shareholders": 0, "wiped": None}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    stats = {"workers_added": 0, "workers_updated": 0, "certs_added": 0,
             "shareholders": 0, "wiped": None}
    if replace:
        stats["wiped"] = wipe_employee_data(c)
        print(f"  🗑  와이프: {stats['wiped']}")

    # ===== 직원현황 시트 (모든 회사 직원 통합) =====
    if "직원현황" in wb.sheetnames:
        ws = wb["직원현황"]
        current_company_id = None
        current_company_name = None
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0: continue  # 헤더
            cells = [str(x or '') for x in row]
            company_cell = cells[0]
            # [건우건설] / [아이엔] 등 — 회사 블록 마커
            if company_cell and '[' in company_cell and ']' in company_cell:
                m = re.search(r'\[([^\]]+)\]', company_cell)
                if m:
                    co_name_raw = m.group(1).strip()
                    cur_co_id = get_or_create_company(c, co_name_raw, strict=False)
                    if cur_co_id:
                        current_company_id = cur_co_id
                        current_company_name = co_name_raw
            if not current_company_id: continue

            name_raw = cells[1].strip() if len(cells) > 1 else ''
            if not name_raw: continue
            # "1.안 경 이" → 정규직 #1
            is_regular = bool(re.match(r'^\s*\d+\.', name_raw))
            regular_no = None
            m = re.match(r'^\s*(\d+)\.', name_raw)
            if m: regular_no = int(m.group(1))
            name = clean_name(name_raw)
            if not name or len(name) < 2: continue
            if name in {'이름', '성명', '회사명', '직위'}: continue

            rrn_raw = cells[2].strip() if len(cells) > 2 else ''
            rrn = re.sub(r'\s+', '', rrn_raw)
            if rrn and not re.match(r'^\d{6}-\d{7}$', rrn): rrn = None

            hired_iso = pick_last_date(row[3] if len(row) > 3 else None)
            resigned_iso = pick_last_date(row[4] if len(row) > 4 else None)
            cert_main = cells[5].strip() if len(cells) > 5 else ''
            related = cells[6].strip() if len(cells) > 6 else ''
            asbestos_cell = cells[7].strip() if len(cells) > 7 else ''
            note = cells[8].strip() if len(cells) > 8 else ''
            asbestos_flag = 1 if '석면' in asbestos_cell else 0
            # 사망·퇴사 표시
            is_deceased = ('사망' in asbestos_cell) or ('사망' in note)

            # 이미 같은 회사에 같은 RRN 존재? (replace=False 일 때 멱등성 유지)
            existing = None
            if rrn:
                existing = c.execute(
                    "SELECT id FROM workers WHERE rrn=? AND company_id=?",
                    (rrn, current_company_id)).fetchone()
            if not existing:
                existing = c.execute(
                    "SELECT id FROM workers WHERE name=? AND company_id=?",
                    (name, current_company_id)).fetchone()

            note_combined = note
            if is_deceased and '사망' not in note_combined: note_combined = (note_combined + ' [사망]').strip()
            if related and related not in note_combined:
                note_combined = (note_combined + f' [관련업종: {related}]').strip()
            job_role = '정규직' if is_regular else ('전직원' if resigned_iso else '직원')

            if existing:
                wid = existing[0]
                c.execute(
                    """UPDATE workers SET company_id=?, rrn=COALESCE(?,rrn),
                       hired_date=COALESCE(?,hired_date), resigned_at=?,
                       asbestos_certified=?, note=?, worker_type='office',
                       job_role=?
                       WHERE id=?""",
                    (current_company_id, rrn, hired_iso, resigned_iso,
                     asbestos_flag, note_combined or None, job_role, wid))
                stats["workers_updated"] += 1
            else:
                cur = c.execute(
                    """INSERT INTO workers(company_id, name, rrn, worker_type, hired_date,
                       resigned_at, asbestos_certified, note, job_role)
                       VALUES(?,?,?,'office',?,?,?,?,?)""",
                    (current_company_id, name, rrn, hired_iso, resigned_iso,
                     asbestos_flag, note_combined or None, job_role))
                wid = cur.lastrowid
                stats["workers_added"] += 1

            # 자격증 — 메인 (parse_cert_field 사용)
            if cert_main:
                for cert in parse_cert_field(cert_main):
                    if not cert.get('name'): continue
                    dup = c.execute(
                        "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=? AND IFNULL(cert_level,'')=IFNULL(?,'')",
                        (wid, cert["name"], cert.get("level"))).fetchone()
                    if not dup:
                        c.execute(
                            """INSERT INTO worker_certifications(worker_id, cert_name, cert_level, related_business)
                               VALUES(?,?,?,?)""",
                            (wid, cert["name"], cert.get("level"),
                             related or None))
                        stats["certs_added"] += 1
            # 자격증 — 비고에서 추가 추출
            for nc in parse_note_certs(note):
                if not nc.get('name'): continue
                dup = c.execute(
                    "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=?",
                    (wid, nc["name"])).fetchone()
                if not dup:
                    c.execute(
                        """INSERT INTO worker_certifications(worker_id, cert_name, cert_level, cert_no, related_business, note)
                           VALUES(?,?,?,?,?,?)""",
                        (wid, nc["name"], nc.get("level"), nc.get("cert_no"),
                         related or None, '비고에서 추출'))
                    stats["certs_added"] += 1

    # ===== 주주명부 시트 (회사별) — 고정 컬럼 헤더 파서 =====
    SH_SHEETS = {
        "건우": "건우건설주식회사",
        "아이엔": "(주)아이엔건설환경",
        "인우": "인우건설(주)",
        "새암": "새암건설(주)",
        "다우": "다우건설(주)",
        "유신": "유신건설(주)",
    }
    # 이전분 시트는 historical → 스킵
    for sn in wb.sheetnames:
        if sn not in SH_SHEETS: continue
        ws = wb[sn]
        co_id = get_or_create_company(c, SH_SHEETS[sn], strict=False)
        if not co_id: continue
        # R7 헤더 — 고정 컬럼 위치 파악
        col_idx = {}
        rows_data = list(ws.iter_rows(values_only=True))
        if len(rows_data) < 8: continue
        header = rows_data[6]  # R7 (0-indexed = 6)
        for ci, cell in enumerate(header or []):
            key = (str(cell or '')).replace(' ', '').strip()
            if key == '직위': col_idx['role'] = ci
            elif key == '성명': col_idx['name'] = ci
            elif '주민' in key: col_idx['rrn'] = ci
            elif '주소' in key or key == '주      주소': col_idx['address'] = ci
            elif '주식수' in key: col_idx['shares'] = ci
            elif key in {'비율', '비  율'} or '비' in key and '율' in key:
                col_idx['ratio'] = ci
        if 'name' not in col_idx: continue
        # R8~ 데이터 — '계' 행 만나면 종료
        for ri in range(7, len(rows_data)):
            row = rows_data[ri]
            if not row: continue
            cells = [str(x or '').strip() for x in row]
            # 종료 조건: '계' '합계' '소계' 셀 발견
            first_nonempty = next((c_val for c_val in cells if c_val), '')
            if first_nonempty in {'계', '합계', '소계'}: break
            name_raw = cells[col_idx['name']] if col_idx['name'] < len(cells) else ''
            name = name_raw.replace(' ', '').strip()
            if not name or len(name) < 2: continue
            if not re.match(r'^[가-힣]{2,4}$', name): continue
            if name in SHAREHOLDER_EXCLUDE: continue
            # 주민번호
            rrn = None
            if 'rrn' in col_idx and col_idx['rrn'] < len(cells):
                rrn_raw = cells[col_idx['rrn']]
                rrn = re.sub(r'\s+', '', rrn_raw)
                if not re.match(r'^\d{6}-\d{7}$', rrn or ''): rrn = None
            role = cells[col_idx['role']] if 'role' in col_idx and col_idx['role'] < len(cells) else None
            if role:
                role = role.strip()
                if role in {'직위', ''}: role = None
            address = cells[col_idx['address']] if 'address' in col_idx and col_idx['address'] < len(cells) else None
            if address: address = re.sub(r'\s+', ' ', address.replace('\n', ' ')).strip() or None
            # 지분율 — 0~1 (소수) 또는 0~100 둘다 허용
            ratio = None
            if 'ratio' in col_idx and col_idx['ratio'] < len(cells):
                rraw = cells[col_idx['ratio']].replace('%', '').strip()
                try:
                    v = float(rraw)
                    ratio = v * 100.0 if 0 < v <= 1 else v
                except: pass
            shares_count = None
            if 'shares' in col_idx and col_idx['shares'] < len(cells):
                try: shares_count = int(float(cells[col_idx['shares']]))
                except: pass
            # 직원과 매칭 시도 (rrn 우선, 그 다음 name+회사)
            w_row = None
            if rrn:
                w_row = c.execute("SELECT id FROM workers WHERE rrn=?", (rrn,)).fetchone()
            if not w_row:
                w_row = c.execute("SELECT id FROM workers WHERE name=? AND company_id=?",
                                  (name, co_id)).fetchone()
            # 중복 방지 (replace=False 일 때)
            dup = c.execute(
                "SELECT id FROM shareholders WHERE company_id=? AND name=? AND IFNULL(rrn,'')=IFNULL(?,'')",
                (co_id, name, rrn or '')).fetchone()
            if dup: continue
            c.execute(
                """INSERT INTO shareholders(company_id, name, role, rrn, address, shares_pct, contribution, worker_id)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (co_id, name, role, rrn, address, ratio, shares_count,
                 w_row[0] if w_row else None))
            stats["shareholders"] += 1
    wb.close()
    return stats

# ====== 자격증 ↔ 면허 자동 매칭 (전체 직원 대상) ======
CERT_KEYWORD_TO_LICENSE = [
    ("토목",      ["토목공사업","토목건축공사업","지반조성·포장공사업","상·하수도설비공사업"]),
    ("건축",      ["건축공사업","토목건축공사업","실내건축공사업","철근·콘크리트공사업","도장·습식·방수·석공사업","금속창호·지붕건축물조립공사업"]),
    ("실내건축",  ["실내건축공사업"]),
    ("기계설비",  ["기계가스설비공사업","가스난방공사업"]),
    ("전기",      ["전기공사업"]),
    ("정보통신",  ["정보통신공사업"]),
    ("소방",      ["소방시설공사업"]),
    ("가스",      ["가스난방공사업","기계가스설비공사업"]),
    ("건설안전",  ["구조물해체·비계공사업"]),
    ("산업안전",  ["구조물해체·비계공사업"]),
    ("콘크리트",  ["철근·콘크리트공사업"]),
    ("굴삭기",    ["지반조성·포장공사업","구조물해체·비계공사업"]),
    ("굴착기",    ["지반조성·포장공사업","구조물해체·비계공사업"]),
    ("비계",      ["구조물해체·비계공사업"]),
    ("방수",      ["도장·습식·방수·석공사업"]),
    ("도장",      ["도장·습식·방수·석공사업"]),
    ("습식",      ["도장·습식·방수·석공사업"]),
    ("배관",      ["기계가스설비공사업","가스난방공사업","상·하수도설비공사업"]),
    ("석면",      ["석면해체·제거업"]),
]

def auto_register_workers_to_licenses(c):
    """모든 재직 직원의 자격증 ↔ 회사 면허 매칭 → license_workers 자동 등록."""
    rows = c.execute("""
        SELECT w.id, w.company_id, wc.cert_name FROM workers w
        JOIN worker_certifications wc ON wc.worker_id = w.id
        WHERE (w.resigned_at IS NULL OR w.resigned_at = '')
          AND w.company_id IS NOT NULL
    """).fetchall()
    auto_count = 0
    # 회사별 면허 캐시
    lic_cache = {}
    for wid, co_id, cert_name in rows:
        if not cert_name: continue
        if co_id not in lic_cache:
            lic_cache[co_id] = c.execute(
                "SELECT id, license_type FROM licenses WHERE company_id=? AND status='active'",
                (co_id,)).fetchall()
        for lic_id, lic_type in lic_cache[co_id]:
            for kw, lic_list in CERT_KEYWORD_TO_LICENSE:
                if kw in cert_name and lic_type in lic_list:
                    cur = c.execute(
                        """INSERT OR IGNORE INTO license_workers(license_id, worker_id, role, note)
                           VALUES(?,?,?,?)""",
                        (lic_id, wid, '기술인 (자동매칭)',
                         f'{cert_name} → {lic_type}'))
                    if cur.rowcount: auto_count += 1
                    break
    return auto_count

# ====== 자격증 파싱 (메인 + 비고) ======
def parse_cert_field(cert_str):
    """ '토목+건축 초급' / '토목 특급, 건축 초급' / '토목기사(중급인정)' / '굴삭기운전기능사' 모두 처리. """
    if not cert_str: return []
    s = str(cert_str).strip()
    levels = ['특급', '고급', '중급', '초급', '기능사', '기술사', '기사', '산업기사']
    result = []
    # "토목기사(중급인정)" 패턴
    m = re.match(r'^(\S+?)기사\(([특고중초]급)인정\)$', s)
    if m:
        return [{"name": m.group(1), "level": m.group(2)}]
    # "+" 으로 분리
    if '+' in s:
        last_level = None
        for lv in levels:
            if lv in s:
                last_level = lv
                s = s.replace(lv, '').strip()
                break
        for part in s.split('+'):
            p = part.strip()
            if p: result.append({"name": p, "level": last_level})
        return result
    # "," 으로 분리
    if ',' in s:
        for part in s.split(','):
            p = part.strip()
            sub_level = None
            for lv in levels:
                if lv in p:
                    sub_level = lv
                    p = p.replace(lv, '').strip()
                    break
            if p: result.append({"name": p, "level": sub_level})
        return result
    # 단일
    for lv in levels:
        if lv in s:
            name = s.replace(lv, '').strip()
            return [{"name": name or s, "level": lv}]
    return [{"name": s, "level": None}]

def parse_note_certs(note):
    """비고 컬럼에서 추가 자격증 추출."""
    if not note: return []
    s = str(note).strip()
    if not s: return []
    out = []
    for m in re.finditer(r'([가-힣\w]+?)(기능사|산업기사|기사|기술사|특급|고급|중급|초급)\s*\(([A-Z0-9]+)\)', s):
        out.append({"name": m.group(1).strip(), "level": m.group(2).strip(), "cert_no": m.group(3).strip()})
    if not out and '운전기능사' in s:
        for piece in re.split(r'[,、/]', s):
            p = piece.strip()
            m2 = re.match(r'^([가-힣]+)\s*운전기능사', p)
            if m2:
                out.append({"name": m2.group(1) + '운전', "level": "기능사", "cert_no": None})
    return out

def parse_license_no_pair(no_str):
    """'G00629876 (#0630375)' → ('G00629876', '0630375')"""
    if not no_str: return (None, None)
    s = str(no_str).strip()
    main = None; sub = None
    m = re.match(r'^([A-Z0-9-]+)', s)
    if m: main = m.group(1)
    m2 = re.search(r'\(#?([A-Z0-9]+)\)', s)
    if m2: sub = m2.group(1)
    if not main:
        m3 = re.search(r'[A-Z0-9]{8,}', s)
        if m3: main = m3.group(0)
    return (main, sub)

# ====== 2) 회사별 아웃라인 — 회사 정보 보강 + 기술자 정밀 임포트 ======
def import_company_outline(c, path):
    if not path.exists():
        print(f"  [skip] {path.name} 없음")
        return {"companies_updated": 0, "tech_added": 0, "auto_registered": 0}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    stats = {"companies_updated": 0, "tech_added": 0, "auto_registered": 0}

    # ===== 아웃라인(2) — 사업자번호·국민연금·결산일 =====
    if "아웃라인(2)" in wb.sheetnames:
        ws = wb["아웃라인(2)"]
        # R2: 회사명들, R3: 국민연금사업장기호 (사업자번호와 비슷)
        rows_data = list(ws.iter_rows(values_only=True))
        if len(rows_data) >= 3:
            company_cells = rows_data[1] if len(rows_data) > 1 else []
            for ci, name_cell in enumerate(company_cells):
                if not name_cell: continue
                co_id = get_or_create_company(c, name_cell)
                if not co_id: continue
                # 다른 행에서 정보 찾기
                for ri, row in enumerate(rows_data):
                    if ri < 2: continue
                    if not row or len(row) <= ci: continue
                    label = str(row[0] or '').strip() if row else ''
                    val = row[ci] if ci < len(row) else None
                    if not val: continue
                    val_s = str(val).strip()
                    if '사업자' in label or '국민연금' in label:
                        # 사업자번호 형식 추출 (xxx-xx-xxxxx)
                        m = re.search(r'\d{3}-\d{2}-\d{5}', val_s)
                        if m:
                            biz = m.group(0)
                            c.execute(
                                "UPDATE companies SET business_no=COALESCE(business_no, ?) WHERE id=? AND (business_no IS NULL OR business_no='')",
                                (biz, co_id))
                    elif '결산' in label or '결산일' in label:
                        m = re.search(r'(\d{1,2})월', val_s)
                        if m:
                            c.execute("UPDATE companies SET fiscal_year_end=COALESCE(fiscal_year_end,?) WHERE id=?",
                                      (m.group(1), co_id))
                    elif '소재' in label or '주소' in label:
                        c.execute("UPDATE companies SET address=COALESCE(address,?) WHERE id=?", (val_s, co_id))
                stats["companies_updated"] += 1

    # ===== 기술자 시트 — 정밀 임포트 (메인+비고 자격증 + 자동 면허 등재) =====
    if "기술자" in wb.sheetnames:
        ws = wb["기술자"]
        current_co_id = None
        in_resignation_block = False  # "퇴사현황" 블록 여부
        rows_data = list(ws.iter_rows(values_only=True))
        # 컬럼 인덱스 — 시트 R5 헤더 기반 (고정)
        # 0:성명 1:직책 2:주민번호 3:주소 4:자격종목 5:등록번호 6:취득일 7:입사일 8:퇴사일 9:급여 10:연락처 11:비고
        col = {'name': 0, 'position': 1, 'rrn': 2, 'address': 3,
               'cert_main': 4, 'cert_no': 5, 'acquired': 6,
               'hired': 7, 'resigned': 8, 'salary': 9, 'phone': 10, 'note': 11}

        for ri, row in enumerate(rows_data):
            if not row: continue
            cells = [str(x or '').strip() for x in row]
            first = cells[0] if cells else ''
            # "퇴사현황" 블록 진입
            if '퇴사' in first and '현황' in first:
                in_resignation_block = True
                continue
            # 회사명 행 감지 — 건설·아이엔이 cell[0] 에 있고 다른 셀들은 비어있음
            if first and ('건설' in first or '아이엔' in first or '유신' in first or '인우' in first or '새암' in first):
                # 헤더가 아닌 첫 셀에 회사명만 있는 행
                if all(not c for c in cells[1:8]):
                    cid = get_or_create_company(c, first, strict=False)
                    if cid:
                        current_co_id = cid
                        in_resignation_block = False
                    continue
            # 헤더 행 ('성명') 스킵
            if first == '성명': continue
            # 데이터 행 — current_co_id 필요
            if not current_co_id: continue
            if not first: continue
            name = clean_name(first)
            if not name: continue

            rrn = re.sub(r'\s+', '', cells[col['rrn']]) if col['rrn'] < len(cells) else None
            position = cells[col['position']] if col['position'] < len(cells) else None
            address = cells[col['address']] if col['address'] < len(cells) else None
            cert_main = cells[col['cert_main']] if col['cert_main'] < len(cells) else None
            cert_no_raw = cells[col['cert_no']] if col['cert_no'] < len(cells) else None
            acquired = normalize_date(row[col['acquired']]) if col['acquired'] < len(row) else None
            hired = normalize_date(row[col['hired']]) if col['hired'] < len(row) else None
            resigned = normalize_date(row[col['resigned']]) if col['resigned'] < len(row) else None
            salary_raw = row[col['salary']] if col['salary'] < len(row) else None
            phone = cells[col['phone']] if col['phone'] < len(cells) else None
            note = cells[col['note']] if col['note'] < len(cells) else None

            # 직원 찾기/생성 (정규직 사무직)
            existing = None
            if rrn:
                existing = c.execute("SELECT id FROM workers WHERE name=? AND rrn=?", (name, rrn)).fetchone()
            if not existing:
                existing = c.execute("SELECT id FROM workers WHERE name=? AND company_id=?",
                                     (name, current_co_id)).fetchone()
            if existing:
                wid = existing[0]
                c.execute("""UPDATE workers SET
                    company_id=COALESCE(?,company_id),
                    rrn=COALESCE(NULLIF(rrn,''),?),
                    address=COALESCE(NULLIF(address,''),?),
                    position=COALESCE(NULLIF(position,''),?),
                    phone=COALESCE(NULLIF(phone,''),?),
                    hired_date=COALESCE(hired_date,?),
                    resigned_at=COALESCE(?,resigned_at),
                    worker_type='office'
                    WHERE id=?""",
                    (current_co_id, rrn, address, position, phone, hired,
                     resigned if in_resignation_block else None, wid))
            else:
                cur = c.execute(
                    """INSERT INTO workers(company_id,name,rrn,address,position,phone,hired_date,
                                            resigned_at,worker_type)
                       VALUES(?,?,?,?,?,?,?,?,'office')""",
                    (current_co_id, name, rrn, address, position, phone, hired,
                     resigned if in_resignation_block else None))
                wid = cur.lastrowid

            # 메인 자격증 파싱 + 등록번호 분리
            (cert_no_main, cert_no_sub) = parse_license_no_pair(cert_no_raw)
            main_certs = parse_cert_field(cert_main)
            for cert in main_certs:
                # 중복 방지 (같은 사람·자격명·레벨)
                dup = c.execute(
                    "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=? AND IFNULL(cert_level,'')=IFNULL(?,'')",
                    (wid, cert["name"], cert.get("level"))).fetchone()
                if not dup:
                    c.execute(
                        """INSERT INTO worker_certifications(worker_id,cert_name,cert_level,cert_no,acquired_at,note)
                           VALUES(?,?,?,?,?,?)""",
                        (wid, cert["name"], cert.get("level"), cert_no_main, acquired,
                         f"sub: {cert_no_sub}" if cert_no_sub else None))
                    stats["tech_added"] += 1
                else:
                    # 등록번호 보강
                    c.execute(
                        "UPDATE worker_certifications SET cert_no=COALESCE(cert_no,?), acquired_at=COALESCE(acquired_at,?) WHERE id=?",
                        (cert_no_main, acquired, dup[0]))

            # 비고 칼럼에서 추가 자격증 추출
            for nc in parse_note_certs(note):
                dup = c.execute(
                    "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=?",
                    (wid, nc["name"])).fetchone()
                if not dup:
                    c.execute(
                        """INSERT INTO worker_certifications(worker_id,cert_name,cert_level,cert_no,note)
                           VALUES(?,?,?,?,?)""",
                        (wid, nc["name"], nc.get("level"), nc.get("cert_no"), '비고에서 추출'))
                    stats["tech_added"] += 1

            # 석면 인력 표시 (cell 12, 13 등에서 '석면' 검출)
            asbestos = any('석면' in (cells[k] or '') for k in range(12, min(15, len(cells))))
            if asbestos:
                c.execute("UPDATE workers SET asbestos_certified=1 WHERE id=?", (wid,))

            # 자동 면허 등재 — 이 직원이 이 회사의 어느 면허 요건에 매칭되는지
            # 이 회사의 모든 면허에 대해, 직원 자격증과 매칭되면 license_workers에 추가
            if not in_resignation_block:
                co_licenses = c.execute(
                    "SELECT id, license_type FROM licenses WHERE company_id=? AND status='active'",
                    (current_co_id,)).fetchall()
                worker_certs = c.execute(
                    "SELECT cert_name, cert_level FROM worker_certifications WHERE worker_id=?",
                    (wid,)).fetchall()
                for lic_id, lic_type in co_licenses:
                    matched = False
                    for cert_name, cert_level in worker_certs:
                        # 키워드 매칭 — CERT_TO_LICENSE_MAP 단순 적용 (포함 검사)
                        for key, lvl_req, lic_list in [
                            ("토목", None, ["토목공사업","토목건축공사업","지반조성·포장공사업","상·하수도설비공사업"]),
                            ("건축", None, ["건축공사업","토목건축공사업","실내건축공사업","철근·콘크리트공사업","도장·습식·방수·석공사업","금속창호·지붕건축물조립공사업"]),
                            ("기계설비", None, ["기계가스설비공사업","가스난방공사업"]),
                            ("전기", None, ["전기공사업"]),
                            ("정보통신", None, ["정보통신공사업"]),
                            ("소방", None, ["소방시설공사업"]),
                            ("가스", None, ["가스난방공사업","기계가스설비공사업"]),
                            ("건설안전", None, ["구조물해체·비계공사업"]),
                            ("산업안전", None, ["구조물해체·비계공사업"]),
                            ("콘크리트", None, ["철근·콘크리트공사업"]),
                            ("굴삭기", None, ["지반조성·포장공사업","구조물해체·비계공사업"]),
                            ("굴착기", None, ["지반조성·포장공사업","구조물해체·비계공사업"]),
                            ("비계", None, ["구조물해체·비계공사업"]),
                            ("방수", None, ["도장·습식·방수·석공사업"]),
                            ("석면", None, ["석면해체·제거업"]),
                        ]:
                            if key in (cert_name or '') and lic_type in lic_list:
                                matched = True; break
                        if matched: break
                    if matched:
                        try:
                            c.execute(
                                """INSERT OR IGNORE INTO license_workers(license_id,worker_id,role,note)
                                   VALUES(?,?,?,?)""",
                                (lic_id, wid, '기술인 (엑셀 자동매칭)', '엑셀 임포트 시 자격 매칭으로 자동 등재'))
                            if c.execute("SELECT changes()").fetchone()[0]:
                                stats["auto_registered"] += 1
                        except Exception: pass
    wb.close()
    return stats

# ====== 3) 건우건설 급여대장 — 직원명부 보강 ======
def import_payroll_directory(c, path):
    if not path.exists():
        return {"workers_updated": 0}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    stats = {"workers_updated": 0}
    if "직원명부" in wb.sheetnames:
        ws = wb["직원명부"]
        co_id = get_or_create_company(c, "건우건설")
        # 헤더: 번호, 성명, 직책, 주민등록번호, 주소, 연락번호, e-mail, 예금주, 은행명, 계좌, 입사일, 퇴사일
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i < 1: continue  # R1 = 헤더
            cells = [str(x).strip() if x is not None else '' for x in row]
            if len(cells) < 5 or not cells[1]: continue
            name = clean_name(cells[1])
            position = cells[2] if len(cells) > 2 else ''
            rrn = re.sub(r'\s+', '', cells[3]) if len(cells) > 3 else ''
            address = cells[4] if len(cells) > 4 else ''
            phone = cells[5] if len(cells) > 5 else ''
            account_holder = cells[7] if len(cells) > 7 else ''
            bank_name = cells[8] if len(cells) > 8 else ''
            bank_account = cells[9] if len(cells) > 9 else ''
            hired = normalize_date(cells[10]) if len(cells) > 10 else None
            resigned = normalize_date(cells[11]) if len(cells) > 11 else None
            if not name: continue
            w = c.execute("SELECT id FROM workers WHERE name=? AND company_id=?", (name, co_id)).fetchone()
            if not w:
                cur = c.execute(
                    """INSERT INTO workers(company_id, name, rrn, phone, address, position,
                       bank_name, account_holder, bank_account, hired_date, resigned_at, worker_type)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,'office')""",
                    (co_id, name, rrn, phone, address, position, bank_name,
                     account_holder, bank_account, hired, resigned))
                stats["workers_updated"] += 1
            else:
                c.execute(
                    """UPDATE workers SET
                        rrn=COALESCE(NULLIF(rrn,''),?), phone=COALESCE(NULLIF(phone,''),?),
                        address=COALESCE(NULLIF(address,''),?), position=COALESCE(NULLIF(position,''),?),
                        bank_name=COALESCE(NULLIF(bank_name,''),?), account_holder=COALESCE(NULLIF(account_holder,''),?),
                        bank_account=COALESCE(NULLIF(bank_account,''),?),
                        hired_date=COALESCE(hired_date,?), resigned_at=COALESCE(resigned_at,?)
                        WHERE id=?""",
                    (rrn, phone, address, position, bank_name, account_holder, bank_account,
                     hired, resigned, w[0]))
                stats["workers_updated"] += 1
    wb.close()
    return stats

# ====== 메인 ======
def import_all(db_path=None, replace=False):
    """엑셀 일괄 임포트.
    replace=True 면 기존 직원/자격증/주주/면허등재를 모두 와이프하고 다시 구축.
    """
    if not openpyxl:
        return {"error": "openpyxl 미설치"}
    if db_path is None:
        from app import DB_PATH
        db_path = DB_PATH
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")

    report = {"replace_mode": replace}
    print("=" * 60)
    print(f"📥 1) 직원,주주명부 임포트  (replace={replace})")
    report["employees"] = import_employee_directory(
        c, DATA_DIR / "직원,주주명부-5개회사 (자동 저장됨).xlsx",
        replace=replace)
    print(f"  → {report['employees']}")

    print("\n📥 2) 회사별 아웃라인 임포트 (회사정보 + 기술자)")
    report["outline"] = import_company_outline(
        c, DATA_DIR / "회사별아웃라인-(기술인력보유현황포함).xlsx")
    print(f"  → {report['outline']}")

    print("\n📥 3) 건우건설 급여대장 — 직원명부 보강")
    report["payroll"] = import_payroll_directory(
        c, DATA_DIR / "건우건설급여대장.xlsx")
    print(f"  → {report['payroll']}")

    print("\n🔗 4) 자격증 ↔ 면허 자동 매칭 (전체 직원 대상)")
    auto_n = auto_register_workers_to_licenses(c)
    report["auto_register_total"] = auto_n
    print(f"  → 자동 등재 추가: {auto_n}건")

    c.commit()
    # 최종 카운트
    final = {
        "companies":        c.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
        "workers_total":    c.execute("SELECT COUNT(*) FROM workers").fetchone()[0],
        "workers_office":   c.execute("SELECT COUNT(*) FROM workers WHERE worker_type='office'").fetchone()[0],
        "workers_active":   c.execute("SELECT COUNT(*) FROM workers WHERE resigned_at IS NULL OR resigned_at=''").fetchone()[0],
        "workers_resigned": c.execute("SELECT COUNT(*) FROM workers WHERE resigned_at IS NOT NULL AND resigned_at!=''").fetchone()[0],
        "certifications":   c.execute("SELECT COUNT(*) FROM worker_certifications").fetchone()[0],
        "shareholders":     c.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0],
        "license_workers":  c.execute("SELECT COUNT(*) FROM license_workers").fetchone()[0],
        "asbestos_certified": c.execute("SELECT COUNT(*) FROM workers WHERE asbestos_certified=1").fetchone()[0],
    }
    report["final"] = final
    c.close()
    print("\n" + "=" * 60)
    print("✅ 임포트 완료")
    for k, v in final.items():
        print(f"  {k}: {v}")
    return report

if __name__ == "__main__":
    import sys
    replace = '--replace' in sys.argv
    r = import_all(replace=replace)
    print("\n[REPORT]", json.dumps(r, ensure_ascii=False, indent=2, default=str))
