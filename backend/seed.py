"""샘플 데이터 시드. 빈 DB에만 채움."""
import sqlite3, os
from app import DB_PATH, init_db

def seed():
    init_db()
    c = sqlite3.connect(DB_PATH)
    cur = c.cursor()
    if cur.execute("SELECT COUNT(*) FROM companies").fetchone()[0] > 0:
        print("이미 데이터가 있습니다. 시드를 건너뜁니다.")
        c.close()
        return

    companies = [
        ("(주)지더블유종합건설", "123-45-67890", "이대표", "토목공사업, 건축공사업"),
        ("지더블유전문건설(주)", "234-56-78901", "박이사", "철근콘크리트공사업, 토공사업"),
        ("지더블유설비(주)",     "345-67-89012", "김전무", "기계설비공사업, 가스시설공사업"),
    ]
    cur.executemany("INSERT INTO companies(name,business_no,ceo,license_info) VALUES(?,?,?,?)", companies)

    sites = [
        # company_id, name, address, lat, lng, geofence, contract, paid, start, end, status, manager
        (1, "강남 오피스 신축", "서울 강남구 테헤란로 152", 37.5006, 127.0366, 250, 4_800_000_000, 1_200_000_000, "2025-09-01", "2026-12-31", "active", "이소장"),
        (1, "송파 아파트 리모델링", "서울 송파구 잠실로 88", 37.5133, 127.1028, 200, 2_300_000_000, 1_700_000_000, "2025-06-01", "2026-06-30", "active", "최소장"),
        (2, "분당 데이터센터 골조", "성남시 분당구 판교로 235", 37.3950, 127.1106, 300, 6_700_000_000, 800_000_000, "2026-02-01", "2027-04-30", "active", "정소장"),
        (2, "수원 공장 증축", "수원시 영통구 광교로 145", 37.2855, 127.0590, 200, 1_900_000_000, 1_600_000_000, "2025-10-15", "2026-05-31", "active", "한반장"),
        (3, "인천 물류창고 설비", "인천 서구 가좌로 33", 37.5156, 126.6580, 200, 1_200_000_000, 200_000_000, "2026-03-01", "2026-09-30", "active", "오과장"),
    ]
    cur.executemany("""INSERT INTO sites(company_id,name,address,latitude,longitude,geofence_meters,
                       contract_amount,paid_amount,start_date,end_date,status,manager) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", sites)

    workers = [
        # company_id, name, phone, type, wage, role, hired, rrn, bank, note
        (1, "김철수", "010-1111-2222", "daily",  200000, "철근공",   "2024-03-15", "1", "신한 110-...", ""),
        (1, "박영호", "010-2222-3333", "daily",  220000, "형틀목공", "2023-09-02", "1", "국민 210-...", ""),
        (1, "이정민", "010-3333-4444", "daily",  180000, "잡부",     "2025-01-10", "1", "농협 312-...", ""),
        (2, "최성훈", "010-4444-5555", "daily",  240000, "철근반장", "2022-04-22", "1", "신한 110-...", "10년 경력"),
        (2, "정대현", "010-5555-6666", "daily",  210000, "콘크리트공","2024-06-01", "1", "기업 050-...", ""),
        (2, "윤재석", "010-6666-7777", "daily",  200000, "잡부",     "2025-08-15", "1", "국민 210-...", "신규"),
        (3, "강민수", "010-7777-8888", "daily",  230000, "배관공",   "2023-12-10", "1", "신한 110-...", ""),
        (3, "오민철", "010-8888-9999", "daily",  220000, "용접공",   "2024-04-05", "1", "농협 312-...", ""),
        (1, "한지영", "010-9999-0000", "office", 0,     "현장관리",  "2022-02-14", "2", "",              "사무직"),
        (1, "장미경", "010-1010-2020", "office", 0,     "경리",     "2021-07-01", "2", "",              "사무직"),
        (2, "서현수", "010-2020-3030", "office", 0,     "공무",     "2023-05-01", "1", "",              "사무직"),
        (3, "노지훈", "010-3030-4040", "office", 0,     "현장소장", "2020-09-15", "1", "",              "사무직"),
    ]
    cur.executemany("""INSERT INTO workers(company_id,name,phone,worker_type,daily_wage,job_role,hired_date,rrn_last,bank_account,note)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""", workers)

    # 오늘 날짜로 샘플 배치
    from datetime import date
    today = date.today().isoformat()
    deployments = [
        (1, 1, today, "plan", ""), (2, 1, today, "plan", ""),
        (3, 2, today, "plan", ""), (4, 3, today, "plan", ""),
        (5, 3, today, "plan", ""), (6, 4, today, "plan", ""),
        (7, 5, today, "plan", ""), (8, 5, today, "plan", ""),
        # 실적은 일부만 (계획과 다른 케이스 시연)
        (1, 1, today, "actual", ""), (2, 1, today, "actual", ""),
        (3, 2, today, "actual", ""),  (4, 3, today, "actual", ""),
        (5, 3, today, "actual", ""),
    ]
    cur.executemany("INSERT OR REPLACE INTO deployments(worker_id,site_id,date,kind,note) VALUES(?,?,?,?,?)", deployments)

    c.commit()
    c.close()
    print("시드 데이터 입력 완료.")

if __name__ == "__main__":
    seed()
