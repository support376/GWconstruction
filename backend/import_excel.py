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
def import_employee_directory(c, path):
    if not path.exists():
        print(f"  [skip] {path.name} 없음")
        return {"workers_added": 0, "workers_updated": 0, "certs_added": 0, "shareholders": 0}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    stats = {"workers_added": 0, "workers_updated": 0, "certs_added": 0, "shareholders": 0}

    # ===== 직원현황 시트 (모든 회사 직원 통합) =====
    if "직원현황" in wb.sheetnames:
        ws = wb["직원현황"]
        current_company_id = None
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0: continue  # 헤더
            company_cell = row[0]
            if company_cell:
                # 회사명은 bracket 형태 [건우건설] 만 인식
                cell_str = str(company_cell)
                if '[' in cell_str and ']' in cell_str:
                    cur_co_id = get_or_create_company(c, company_cell, strict=True)
                    if cur_co_id: current_company_id = cur_co_id
            name_cell = row[1] if len(row) > 1 else None
            rrn = row[2] if len(row) > 2 else None
            hired = row[3] if len(row) > 3 else None
            resigned = row[4] if len(row) > 4 else None
            cert_str = row[5] if len(row) > 5 else None
            related = row[6] if len(row) > 6 else None
            asbestos = row[7] if len(row) > 7 else None
            note = row[8] if len(row) > 8 else None

            name = clean_name(name_cell)
            if not name: continue
            rrn_str = str(rrn).strip() if rrn else None

            # 이미 등록된 사람? (이름+rrn 또는 이름+회사)
            existing = None
            if rrn_str:
                existing = c.execute("SELECT id FROM workers WHERE name=? AND rrn=?", (name, rrn_str)).fetchone()
            if not existing:
                existing = c.execute(
                    "SELECT id FROM workers WHERE name=? AND company_id=?",
                    (name, current_company_id)).fetchone()
            hired_iso = normalize_date(hired)
            resigned_iso = normalize_date(resigned)
            asbestos_flag = 1 if (asbestos and '석면' in str(asbestos)) else 0
            if existing:
                wid = existing[0]
                c.execute(
                    """UPDATE workers SET company_id=?, rrn=COALESCE(?,rrn),
                       hired_date=COALESCE(?,hired_date), resigned_at=?,
                       asbestos_certified=?, note=COALESCE(?,note),
                       worker_type=COALESCE(NULLIF(worker_type,''),'office')
                       WHERE id=?""",
                    (current_company_id, rrn_str, hired_iso, resigned_iso,
                     asbestos_flag, str(note) if note else None, wid))
                stats["workers_updated"] += 1
            else:
                cur = c.execute(
                    """INSERT INTO workers(company_id, name, rrn, worker_type, hired_date,
                       resigned_at, asbestos_certified, note)
                       VALUES(?,?,?,'office',?,?,?,?)""",
                    (current_company_id, name, rrn_str, hired_iso, resigned_iso,
                     asbestos_flag, str(note) if note else None))
                wid = cur.lastrowid
                stats["workers_added"] += 1

            # 자격증 임포트
            if cert_str:
                certs = parse_certifications(cert_str)
                for cert in certs:
                    # 중복 방지
                    dup = c.execute(
                        "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=? AND IFNULL(cert_level,'')=IFNULL(?,'')",
                        (wid, cert["name"], cert.get("level"))).fetchone()
                    if not dup:
                        c.execute(
                            """INSERT INTO worker_certifications(worker_id, cert_name, cert_level, related_business)
                               VALUES(?,?,?,?)""",
                            (wid, cert["name"], cert.get("level"),
                             str(related).strip() if related else None))
                        stats["certs_added"] += 1
            elif related:
                # 자격증 없이 관련업종만 — 정보 저장
                pass

    # ===== 주주명부 시트 (회사별) =====
    SH_SHEETS = ["건우", "아이엔", "인우", "새암", "다우", "유신"]
    for sn in wb.sheetnames:
        if sn not in SH_SHEETS: continue
        ws = wb[sn]
        co_id = get_or_create_company(c, sn)
        if not co_id: continue
        # 보통 R5~ 부터 데이터 (헤더 R3 "주주명부", R4 빈줄)
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i < 4: continue
            if all(x is None or not str(x).strip() for x in row): continue
            # 컬럼 추정: 번호, 직책/관계, 이름, 주민번호, 주소, 지분, ...
            cells = [str(x).strip() if x is not None else '' for x in row]
            # 이름 후보 — 주민번호 옆에 있는 한글 2~4자, 라벨 제외
            name = None
            rrn = None
            role = None
            shares = None
            # 먼저 주민번호 위치 찾기
            for ci, cell in enumerate(cells):
                if re.match(r'^\d{6}-\d{7}$', cell):
                    rrn = cell
                    # 주민번호 바로 왼쪽 셀이 보통 이름
                    for offset in [-1, -2, 1]:
                        idx = ci + offset
                        if 0 <= idx < len(cells):
                            cand = cells[idx].replace(' ', '')
                            if re.match(r'^[가-힣]{2,4}$', cand) and cand not in SHAREHOLDER_EXCLUDE:
                                name = cand; break
                    break
            # 주민번호 없는 경우엔 첫 한글 2~4자 셀 (라벨 제외)
            if not name:
                for cell in cells:
                    cand = cell.replace(' ', '')
                    if re.match(r'^[가-힣]{2,4}$', cand) and cand not in SHAREHOLDER_EXCLUDE:
                        name = cand; break
            for cell in cells:
                if not role and cell in ['대표', '대표이사', '이사', '감사', '사내이사', '사외이사']:
                    role = cell
                if not shares and re.match(r'^\d+(\.\d+)?%?$', cell):
                    try:
                        v = float(cell.replace('%', ''))
                        if 0 < v <= 100: shares = v  # 합리적 지분율만
                    except: pass
            if not name: continue
            if name in SHAREHOLDER_EXCLUDE: continue
            # 중복 방지
            dup = c.execute(
                "SELECT id FROM shareholders WHERE company_id=? AND name=? AND IFNULL(rrn,'')=IFNULL(?,'')",
                (co_id, name, rrn)).fetchone()
            if not dup:
                # 직원과 매칭 시도
                w = c.execute("SELECT id FROM workers WHERE name=? AND company_id=?", (name, co_id)).fetchone()
                c.execute(
                    """INSERT INTO shareholders(company_id, name, role, rrn, shares_pct, worker_id)
                       VALUES(?,?,?,?,?,?)""",
                    (co_id, name, role, rrn, shares, w[0] if w else None))
                stats["shareholders"] += 1
    wb.close()
    return stats

# ====== 2) 회사별 아웃라인 — 회사 정보 보강 ======
def import_company_outline(c, path):
    if not path.exists():
        print(f"  [skip] {path.name} 없음")
        return {"companies_updated": 0, "tech_added": 0}
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    stats = {"companies_updated": 0, "tech_added": 0}

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

    # ===== 기술자 시트 — 기존 직원에 자격증 보강 =====
    if "기술자" in wb.sheetnames:
        ws = wb["기술자"]
        current_co_id = None
        # 헤더 추정: R5 또는 그 근처에 '성명', '주민번호', '자격종목 및 등록번호', '취득일', '입사일'
        rows_data = list(ws.iter_rows(values_only=True))
        col_map = None
        for ri, row in enumerate(rows_data):
            cells = [str(x or '').strip() for x in row]
            joined = ' '.join(cells)
            # 회사명 행 감지
            if any('건설' in str(x) or '아이엔' in str(x) for x in cells if x):
                co_match = next((x for x in cells if x and ('건설' in x or '아이엔' in x)), None)
                if co_match:
                    cid = get_or_create_company(c, co_match)
                    if cid: current_co_id = cid
            # 헤더 행 감지
            if '성명' in cells and ('주민' in joined or '자격' in joined):
                col_map = {}
                for ci, cell in enumerate(cells):
                    if '성명' in cell: col_map['name'] = ci
                    elif '주민' in cell: col_map['rrn'] = ci
                    elif '자격' in cell or '등록번호' in cell: col_map['cert'] = ci
                    elif '취득' in cell: col_map['acquired'] = ci
                    elif '입사' in cell: col_map['hired'] = ci
                    elif '직책' in cell: col_map['position'] = ci
                continue
            if not col_map: continue
            # 데이터 행
            name_idx = col_map.get('name')
            if name_idx is None or name_idx >= len(row): continue
            name = clean_name(row[name_idx])
            if not name: continue
            rrn = str(row[col_map['rrn']]).strip() if col_map.get('rrn') is not None and row[col_map['rrn']] else None
            cert_info = str(row[col_map['cert']]).strip() if col_map.get('cert') is not None and row[col_map['cert']] else None
            acquired = normalize_date(row[col_map['acquired']]) if col_map.get('acquired') is not None else None
            position = str(row[col_map['position']]).strip() if col_map.get('position') is not None and row[col_map['position']] else None

            if not current_co_id: continue
            # 직원 찾기
            w = c.execute("SELECT id FROM workers WHERE name=? AND company_id=?", (name, current_co_id)).fetchone()
            if not w:
                cur = c.execute(
                    "INSERT INTO workers(company_id, name, rrn, worker_type, position) VALUES(?,?,?,'office',?)",
                    (current_co_id, name, rrn, position))
                wid = cur.lastrowid
            else:
                wid = w[0]
                if position:
                    c.execute("UPDATE workers SET position=COALESCE(NULLIF(position,''),?) WHERE id=?", (position, wid))
            # 자격증 정보 추가
            if cert_info:
                # "토목+건축 초급, G00629876 #0630375" 같은 형식
                # 자격명만 추출
                cert_name_part = cert_info.split(',')[0].strip()
                cert_no_match = re.search(r'[A-Z0-9]{6,}', cert_info[len(cert_name_part):])
                cert_no = cert_no_match.group(0) if cert_no_match else None
                certs = parse_certifications(cert_name_part)
                for cert in certs:
                    dup = c.execute(
                        "SELECT id FROM worker_certifications WHERE worker_id=? AND cert_name=?",
                        (wid, cert["name"])).fetchone()
                    if not dup:
                        c.execute(
                            """INSERT INTO worker_certifications(worker_id, cert_name, cert_level, cert_no, acquired_at)
                               VALUES(?,?,?,?,?)""",
                            (wid, cert["name"], cert.get("level"), cert_no, acquired))
                        stats["tech_added"] += 1
                    elif cert_no:
                        c.execute("UPDATE worker_certifications SET cert_no=COALESCE(cert_no,?), acquired_at=COALESCE(acquired_at,?) WHERE worker_id=? AND cert_name=?",
                                  (cert_no, acquired, wid, cert["name"]))
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
def import_all(db_path=None):
    if not openpyxl:
        return {"error": "openpyxl 미설치"}
    if db_path is None:
        from app import DB_PATH
        db_path = DB_PATH
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")

    report = {}
    print("=" * 60)
    print("📥 1) 직원,주주명부 임포트")
    report["employees"] = import_employee_directory(
        c, DATA_DIR / "직원,주주명부-5개회사 (자동 저장됨).xlsx")
    print(f"  → {report['employees']}")

    print("\n📥 2) 회사별 아웃라인 임포트 (회사정보 + 기술자)")
    report["outline"] = import_company_outline(
        c, DATA_DIR / "회사별아웃라인-(기술인력보유현황포함).xlsx")
    print(f"  → {report['outline']}")

    print("\n📥 3) 건우건설 급여대장 — 직원명부 보강")
    report["payroll"] = import_payroll_directory(
        c, DATA_DIR / "건우건설급여대장.xlsx")
    print(f"  → {report['payroll']}")

    c.commit()
    # 최종 카운트
    final = {
        "companies":        c.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
        "workers_total":    c.execute("SELECT COUNT(*) FROM workers").fetchone()[0],
        "workers_office":   c.execute("SELECT COUNT(*) FROM workers WHERE worker_type='office'").fetchone()[0],
        "workers_resigned": c.execute("SELECT COUNT(*) FROM workers WHERE resigned_at IS NOT NULL").fetchone()[0],
        "certifications":   c.execute("SELECT COUNT(*) FROM worker_certifications").fetchone()[0],
        "shareholders":     c.execute("SELECT COUNT(*) FROM shareholders").fetchone()[0],
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
    r = import_all()
    print("\n[REPORT]", json.dumps(r, ensure_ascii=False, indent=2))
