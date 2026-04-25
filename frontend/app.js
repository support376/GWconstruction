// GW 건설관리 시스템 - 프론트엔드 SPA
'use strict';

// ============================================================
// 공통 유틸
// ============================================================
const $ = sel => document.querySelector(sel);
const $$ = sel => Array.from(document.querySelectorAll(sel));
const fmt = n => (n || 0).toLocaleString('ko-KR');
const fmtMoney = n => '₩ ' + fmt(n);
const today = () => new Date().toISOString().slice(0,10);

function _handle401(r) {
  if (r.status === 401) {
    location.href = '/login';
    return new Promise(() => {}); // 더 이상 진행 안 함
  }
  return r;
}
const api = {
  get: (path) => fetch(path).then(_handle401).then(r => r.json()),
  post: (path, body) => fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}).then(_handle401).then(r => r.json()),
  put: (path, body) => fetch(path, {method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}).then(_handle401).then(r => r.json()),
  del: (path) => fetch(path, {method:'DELETE'}).then(_handle401).then(r => r.json()),
};

async function logout() {
  await fetch('/api/logout', { method: 'POST' });
  location.href = '/login';
}
window.logout = logout;

// 주소 → 좌표 (OpenStreetMap Nominatim, 무료, 키 불필요)
async function geocodeAddress(addr) {
  if (!addr || !addr.trim()) return null;
  const tries = [addr, addr.split(' ').slice(0,3).join(' ')];
  for (const q of tries) {
    if (!q) continue;
    const url = 'https://nominatim.openstreetmap.org/search?format=json&countrycodes=kr&limit=1&q=' + encodeURIComponent(q);
    try {
      const r = await fetch(url, { headers: { 'Accept-Language': 'ko' } });
      const data = await r.json();
      if (data && data[0]) return { lat: +data[0].lat, lng: +data[0].lon, display: data[0].display_name };
    } catch (e) { /* try next */ }
  }
  return null;
}

function tickClock() {
  const d = new Date();
  $('#now-time').textContent = d.toLocaleString('ko-KR', { hour12: false });
}
setInterval(tickClock, 1000); tickClock();

// ============================================================
// 모달
// ============================================================
function modal(title, bodyHtml, onSave) {
  const root = $('#modal-root');
  root.innerHTML = `
    <div class="modal-bg" id="m-bg">
      <div class="modal" onclick="event.stopPropagation()">
        <div class="modal-head">
          <h3>${title}</h3>
          <button class="close-x" id="m-close">×</button>
        </div>
        <div class="modal-body">${bodyHtml}</div>
        <div class="modal-foot">
          <button class="btn" id="m-cancel">취소</button>
          <button class="btn btn-primary" id="m-save">저장</button>
        </div>
      </div>
    </div>`;
  const close = () => { root.innerHTML = ''; };
  $('#m-close').onclick = close;
  $('#m-cancel').onclick = close;
  $('#m-bg').onclick = close;
  $('#m-save').onclick = async () => { const ok = await onSave(); if (ok !== false) close(); };
}

// ============================================================
// 라우터
// ============================================================
const routes = {};
function route(path, fn) { routes[path] = fn; }
async function navigate() {
  const hash = location.hash.replace(/^#/, '') || '/dashboard';
  $$('.nav-link').forEach(a => a.classList.toggle('active', a.getAttribute('href') === '#' + hash));
  const fn = routes[hash] || routes['/dashboard'];
  $('#app').innerHTML = '<div class="card card-pad">로딩 중…</div>';
  try { await fn(); } catch (e) { $('#app').innerHTML = '<div class="card card-pad">오류: ' + e + '</div>'; }
}
window.addEventListener('hashchange', navigate);

// ============================================================
// 대시보드
// ============================================================
route('/dashboard', async () => {
  const d = await api.get('/api/dashboard');
  const remaining = d.contract_total - d.paid_total;
  const progressPct = d.contract_total ? Math.round(d.paid_total / d.contract_total * 100) : 0;

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">전사 대시보드</div>
        <div class="page-sub">${d.today} 기준 · 회사 전체 운영 현황</div>
      </div>
      <div>
        <a class="btn btn-primary" href="#/board">오늘 배치 보드 →</a>
      </div>
    </div>

    <div class="stat-grid">
      <div class="stat accent"><div class="label">진행 현장</div><div class="value">${d.sites_active}</div><div class="delta">활성 현장 수</div></div>
      <div class="stat"><div class="label">총 직원</div><div class="value">${d.workers_total}</div><div class="delta">일용직 + 사무직</div></div>
      <div class="stat success"><div class="label">오늘 실제 투입</div><div class="value">${d.deployed_today}</div><div class="delta">계획 ${d.planned_today}명 대비</div></div>
      <div class="stat warning"><div class="label">계획-실적 차이</div><div class="value">${d.planned_today - d.deployed_today >= 0 ? '+' : ''}${d.planned_today - d.deployed_today}</div><div class="delta">계획 - 실적</div></div>
    </div>

    <div class="stat-grid">
      <div class="stat"><div class="label">전체 계약금액</div><div class="value">${fmtMoney(d.contract_total)}</div><div class="delta">활성 현장 합계</div></div>
      <div class="stat success"><div class="label">누적 수금</div><div class="value">${fmtMoney(d.paid_total)}</div><div class="delta">${progressPct}% 회수</div></div>
      <div class="stat warning"><div class="label">잔여 공사대금</div><div class="value">${fmtMoney(remaining)}</div><div class="delta">아직 받을 금액</div></div>
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-head"><h3>법인별 운영 현황</h3></div>
      <table class="table">
        <thead><tr><th>법인</th><th>현장</th><th>직원</th><th style="text-align:right">계약</th><th style="text-align:right">수금</th><th style="text-align:right">잔액</th></tr></thead>
        <tbody>
          ${d.companies.map(c => `
            <tr>
              <td><b>${c.name}</b></td>
              <td>${c.sites}</td>
              <td>${c.workers}</td>
              <td style="text-align:right">${fmtMoney(c.contract)}</td>
              <td style="text-align:right">${fmtMoney(c.paid)}</td>
              <td style="text-align:right"><b>${fmtMoney(c.contract - c.paid)}</b></td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>

    <div class="card">
      <div class="card-head"><h3>현장별 진행 상황</h3></div>
      <table class="table">
        <thead><tr><th>현장</th><th>오늘 계획</th><th>오늘 실적</th><th style="text-align:right">계약금액</th><th style="text-align:right">기성률</th></tr></thead>
        <tbody>
          ${d.site_summary.map(s => {
            const pct = s.contract_amount ? Math.round(s.paid_amount / s.contract_amount * 100) : 0;
            const diff = s.today_actual - s.today_plan;
            const diffClass = diff < 0 ? 'badge-red' : diff > 0 ? 'badge-orange' : 'badge-gray';
            return `<tr>
              <td><b>${s.name}</b></td>
              <td>${s.today_plan}명</td>
              <td>${s.today_actual}명 <span class="badge ${diffClass}">${diff > 0 ? '+' : ''}${diff}</span></td>
              <td style="text-align:right">${fmtMoney(s.contract_amount)}</td>
              <td style="text-align:right"><b>${pct}%</b></td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
    </div>
  `;
});

// ============================================================
// 배치 보드 (드래그앤드롭)
// ============================================================
let boardState = { date: today(), kind: 'plan', workers: [], sites: [], deployments: [] };

route('/board', async () => {
  await loadBoard();
  renderBoard();
});

async function loadBoard() {
  const [workers, sites, deployments] = await Promise.all([
    api.get('/api/workers'),
    api.get('/api/sites?active_only=true'),
    api.get('/api/deployments?date=' + boardState.date + '&kind=' + boardState.kind),
  ]);
  boardState.workers = workers;
  boardState.sites = sites;
  boardState.deployments = deployments;
}

function renderBoard() {
  const { workers, sites, deployments, date, kind } = boardState;
  const deployedWorkerIds = new Set(deployments.map(d => d.worker_id));
  const pool = workers.filter(w => !deployedWorkerIds.has(w.id));

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">인력 배치 보드</div>
        <div class="page-sub">사람을 끌어다 놓거나 → 버튼으로 옮기세요. 계획/실적/신고를 따로 운영합니다.</div>
      </div>
    </div>

    <div class="board-controls">
      <span style="font-size:12px; color: var(--text-muted)">날짜</span>
      <input type="date" id="board-date" value="${date}">
      <span class="kind-tabs">
        <button data-kind="plan"     class="${kind==='plan'?'active':''}">계획</button>
        <button data-kind="actual"   class="${kind==='actual'?'active':''}">실적</button>
        <button data-kind="reported" class="${kind==='reported'?'active':''}">신고</button>
      </span>
      <button class="btn btn-sm" id="copy-plan-actual">계획 → 실적 복사</button>
      <button class="btn btn-sm" id="copy-actual-reported">실적 → 신고 복사</button>
    </div>

    ${kind === 'reported' ? `<div class="diff-banner">
      ⚠️ 신고용 배치는 일용근로내용확인신고 등 외부 신고에 사용됩니다.
      실적과 차이가 클 경우 노무·세무 리스크가 있을 수 있으니 사내 정책에 따라 운영하세요.
    </div>` : ''}

    <div class="board">
      <div class="pool" id="pool">
        <h4>대기 인력 (${pool.length}명)</h4>
        <input class="search" id="pool-search" placeholder="이름·역할 검색…">
        <div id="pool-list">
          ${pool.length === 0 ? '<div class="empty-pool">전원 배치됨</div>' :
            pool.map(w => workerChipHtml(w)).join('')}
        </div>
      </div>

      <div class="sites-grid" id="sites-grid">
        ${sites.map(s => {
          const here = deployments.filter(d => d.site_id === s.id);
          const wage = here.reduce((sum, d) => sum + (d.daily_wage || 0), 0);
          return `<div class="site-col" data-sid="${s.id}">
            <div class="site-col-head">
              <div>
                <div class="name">${s.name}</div>
                <div class="meta">${s.address || ''}</div>
                <div class="meta">${here.length}명 · 일당합계 ${fmtMoney(wage)}</div>
              </div>
            </div>
            <div class="site-col-body">
              ${here.length === 0 ? '<div class="empty-site">여기로 사람을 끌어다 놓으세요</div>' :
                here.map(d => workerChipHtml({
                  id: d.worker_id, name: d.worker_name,
                  worker_type: d.worker_type, daily_wage: d.daily_wage,
                  job_role: '', _site_id: s.id,
                }, sites)).join('')}
            </div>
          </div>`;
        }).join('')}
      </div>
    </div>
  `;

  // Bindings
  $('#board-date').onchange = async e => { boardState.date = e.target.value; await loadBoard(); renderBoard(); };
  $$('.kind-tabs button').forEach(b => b.onclick = async () => {
    boardState.kind = b.dataset.kind; await loadBoard(); renderBoard();
  });
  $('#copy-plan-actual').onclick = async () => {
    if (!confirm(`${boardState.date} 의 계획 배치를 실적으로 복사합니다. 진행할까요?`)) return;
    await api.post('/api/deployments/copy', { src_kind:'plan', dst_kind:'actual', date: boardState.date });
    await loadBoard(); renderBoard();
  };
  $('#copy-actual-reported').onclick = async () => {
    if (!confirm(`${boardState.date} 의 실적을 신고로 복사합니다. 신고 내용은 별도 수정 가능합니다. 진행할까요?`)) return;
    await api.post('/api/deployments/copy', { src_kind:'actual', dst_kind:'reported', date: boardState.date });
    await loadBoard(); renderBoard();
  };

  $('#pool-search').oninput = e => {
    const q = e.target.value.trim().toLowerCase();
    $('#pool-list').innerHTML = pool.filter(w =>
      !q || w.name.toLowerCase().includes(q) || (w.job_role||'').toLowerCase().includes(q)
    ).map(workerChipHtml).join('') || '<div class="empty-pool">검색 결과 없음</div>';
    bindDrag();
  };

  bindDrag();
  bindMoveButtons(sites);
}

function workerChipHtml(w, sites) {
  const moveBtns = sites
    ? `<div style="display:flex; gap:2px; flex-wrap:wrap; justify-content:flex-end">
         <button class="move-btn" data-wid="${w.id}" data-sid="0" title="대기로">↩</button>
         ${sites.filter(s => s.id !== w._site_id).slice(0,3).map(s =>
           `<button class="move-btn" data-wid="${w.id}" data-sid="${s.id}" title="${s.name}">→${s.name.slice(0,4)}</button>`
         ).join('')}
       </div>`
    : '';
  return `<div class="worker-chip ${w.worker_type === 'office' ? 'office' : 'daily'}" draggable="true" data-wid="${w.id}">
    <div>
      <span>${w.name}</span>
      <span class="role">${w.job_role || (w.worker_type==='office'?'사무직':'')}${w.daily_wage ? ' · '+fmt(w.daily_wage)+'원' : ''}</span>
    </div>
    ${moveBtns}
  </div>`;
}

function bindDrag() {
  $$('.worker-chip[draggable=true]').forEach(chip => {
    chip.ondragstart = e => {
      chip.classList.add('dragging');
      e.dataTransfer.setData('text/plain', chip.dataset.wid);
      e.dataTransfer.effectAllowed = 'move';
    };
    chip.ondragend = () => chip.classList.remove('dragging');
  });
  $$('.site-col').forEach(col => {
    col.ondragover = e => { e.preventDefault(); col.classList.add('drag-over'); };
    col.ondragleave = () => col.classList.remove('drag-over');
    col.ondrop = async e => {
      e.preventDefault();
      col.classList.remove('drag-over');
      const wid = +e.dataTransfer.getData('text/plain');
      const sid = +col.dataset.sid;
      if (!wid || !sid) return;
      await assignWorker(wid, sid);
    };
  });
  const pool = $('#pool');
  pool.ondragover = e => { e.preventDefault(); pool.style.background = '#eef3fc'; };
  pool.ondragleave = () => pool.style.background = '';
  pool.ondrop = async e => {
    e.preventDefault(); pool.style.background = '';
    const wid = +e.dataTransfer.getData('text/plain');
    if (!wid) return;
    await unassignWorker(wid);
  };
}

function bindMoveButtons(sites) {
  $$('.move-btn').forEach(btn => {
    btn.onclick = async e => {
      e.stopPropagation();
      const wid = +btn.dataset.wid;
      const sid = +btn.dataset.sid;
      if (sid === 0) await unassignWorker(wid);
      else await assignWorker(wid, sid);
    };
  });
}

async function assignWorker(wid, sid) {
  const dep = boardState.deployments.find(d => d.worker_id === wid);
  if (dep) await api.del('/api/deployments/' + dep.id);
  await api.post('/api/deployments', {
    worker_id: wid, site_id: sid, date: boardState.date, kind: boardState.kind
  });
  await loadBoard(); renderBoard();
}

async function unassignWorker(wid) {
  const dep = boardState.deployments.find(d => d.worker_id === wid);
  if (dep) await api.del('/api/deployments/' + dep.id);
  await loadBoard(); renderBoard();
}

// ============================================================
// 직원 관리
// ============================================================
route('/workers', async () => {
  const [workers, companies] = await Promise.all([api.get('/api/workers'), api.get('/api/companies')]);
  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">직원 관리</div>
        <div class="page-sub">일용직 + 사무직 통합 인사 마스터</div>
      </div>
      <button class="btn btn-primary" id="add-worker">+ 직원 등록</button>
    </div>
    <div class="card">
      <table class="table">
        <thead><tr>
          <th>이름</th><th>구분</th><th>법인</th><th>역할</th><th>일당</th><th>연락처</th><th>입사일</th><th></th>
        </tr></thead>
        <tbody>
          ${workers.map(w => `<tr>
            <td><b>${w.name}</b></td>
            <td><span class="badge ${w.worker_type==='office'?'badge-orange':'badge-blue'}">${w.worker_type==='office'?'사무직':'일용직'}</span></td>
            <td>${w.company_name || '-'}</td>
            <td>${w.job_role || '-'}</td>
            <td>${w.daily_wage ? fmt(w.daily_wage) + '원' : '-'}</td>
            <td>${w.phone || '-'}</td>
            <td>${w.hired_date || '-'}</td>
            <td>
              <button class="btn btn-sm" data-edit="${w.id}">수정</button>
              <button class="btn btn-sm btn-danger" data-del="${w.id}">삭제</button>
            </td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>
  `;
  $('#add-worker').onclick = () => workerModal(null, companies);
  $$('[data-edit]').forEach(b => b.onclick = () => {
    const w = workers.find(x => x.id == b.dataset.edit);
    workerModal(w, companies);
  });
  $$('[data-del]').forEach(b => b.onclick = async () => {
    if (!confirm('삭제하시겠습니까? 관련 배치/출퇴근 기록도 삭제됩니다.')) return;
    await api.del('/api/workers/' + b.dataset.del);
    navigate();
  });
});

function workerModal(w, companies) {
  const isNew = !w;
  modal(isNew ? '직원 등록' : '직원 수정', `
    <div class="form-grid">
      <div class="form-row"><label>이름 *</label><input id="f-name" value="${w?.name || ''}"></div>
      <div class="form-row"><label>구분 *</label>
        <select id="f-type">
          <option value="daily" ${w?.worker_type!=='office'?'selected':''}>일용직</option>
          <option value="office" ${w?.worker_type==='office'?'selected':''}>사무직</option>
        </select>
      </div>
      <div class="form-row"><label>법인</label>
        <select id="f-company">
          <option value="">선택</option>
          ${companies.map(c => `<option value="${c.id}" ${w?.company_id==c.id?'selected':''}>${c.name}</option>`).join('')}
        </select>
      </div>
      <div class="form-row"><label>역할/직무</label><input id="f-role" value="${w?.job_role || ''}"></div>
      <div class="form-row"><label>일당 (원)</label><input id="f-wage" type="number" value="${w?.daily_wage || 0}"></div>
      <div class="form-row"><label>연락처</label><input id="f-phone" value="${w?.phone || ''}"></div>
      <div class="form-row"><label>입사일</label><input id="f-hired" type="date" value="${w?.hired_date || ''}"></div>
      <div class="form-row"><label>계좌</label><input id="f-bank" value="${w?.bank_account || ''}"></div>
    </div>
    <div class="form-row"><label>비고</label><textarea id="f-note" rows="2">${w?.note || ''}</textarea></div>
  `, async () => {
    const payload = {
      name: $('#f-name').value.trim(),
      worker_type: $('#f-type').value,
      company_id: $('#f-company').value ? +$('#f-company').value : null,
      job_role: $('#f-role').value,
      daily_wage: +$('#f-wage').value || 0,
      phone: $('#f-phone').value,
      hired_date: $('#f-hired').value,
      bank_account: $('#f-bank').value,
      note: $('#f-note').value,
    };
    if (!payload.name) { alert('이름을 입력하세요'); return false; }
    if (isNew) await api.post('/api/workers', payload);
    else      await api.put('/api/workers/' + w.id, payload);
    navigate();
  });
}

// ============================================================
// 현장 관리
// ============================================================
route('/sites', async () => {
  const [sites, companies] = await Promise.all([api.get('/api/sites'), api.get('/api/companies')]);
  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">현장 관리</div>
        <div class="page-sub">현장별 GPS 좌표·지오펜스·계약·기성</div>
      </div>
      <button class="btn btn-primary" id="add-site">+ 현장 등록</button>
    </div>
    <div class="card">
      <table class="table">
        <thead><tr>
          <th>현장명</th><th>법인</th><th>주소</th><th>GPS</th><th style="text-align:right">계약</th><th style="text-align:right">수금</th><th>상태</th><th></th>
        </tr></thead>
        <tbody>
          ${sites.map(s => `<tr>
            <td><b>${s.name}</b><div style="font-size:11px; color: var(--text-muted)">담당: ${s.manager || '-'}</div></td>
            <td>${s.company_name || '-'}</td>
            <td style="font-size:12px">${s.address || '-'}</td>
            <td style="font-size:11px">${s.latitude ? s.latitude.toFixed(4)+', '+s.longitude.toFixed(4) : '-'}<br>지오펜스 ${s.geofence_meters||200}m</td>
            <td style="text-align:right">${fmtMoney(s.contract_amount)}</td>
            <td style="text-align:right">${fmtMoney(s.paid_amount)}</td>
            <td><span class="badge ${s.status==='active'?'badge-green':'badge-gray'}">${s.status==='active'?'진행':'마감'}</span></td>
            <td>
              <button class="btn btn-sm" data-edit="${s.id}">수정</button>
              <button class="btn btn-sm btn-danger" data-del="${s.id}">삭제</button>
            </td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>
  `;
  $('#add-site').onclick = () => siteModal(null, companies);
  $$('[data-edit]').forEach(b => b.onclick = () => {
    const s = sites.find(x => x.id == b.dataset.edit);
    siteModal(s, companies);
  });
  $$('[data-del]').forEach(b => b.onclick = async () => {
    if (!confirm('삭제하시겠습니까?')) return;
    await api.del('/api/sites/' + b.dataset.del);
    navigate();
  });
});

function siteModal(s, companies) {
  const isNew = !s;
  modal(isNew ? '현장 등록' : '현장 수정', `
    <div class="form-grid">
      <div class="form-row"><label>현장명 *</label><input id="f-name" value="${s?.name || ''}"></div>
      <div class="form-row"><label>법인</label>
        <select id="f-company">
          <option value="">선택</option>
          ${companies.map(c => `<option value="${c.id}" ${s?.company_id==c.id?'selected':''}>${c.name}</option>`).join('')}
        </select>
      </div>
      <div class="form-row"><label>담당자</label><input id="f-manager" value="${s?.manager || ''}"></div>
      <div class="form-row"><label>상태</label>
        <select id="f-status">
          <option value="active" ${s?.status!=='closed'?'selected':''}>진행</option>
          <option value="closed" ${s?.status==='closed'?'selected':''}>마감</option>
        </select>
      </div>
    </div>
    <div class="form-row">
      <label>주소</label>
      <div style="display:flex; gap:6px;">
        <input id="f-address" value="${s?.address || ''}" style="flex:1;">
        <button class="btn" id="geocode-btn" type="button">🔍 주소로 좌표 찾기</button>
      </div>
      <div class="geo-hint" id="geo-hint">주소 입력 후 버튼을 누르면 OSM에서 좌표를 찾아줍니다 (무료, 키 불필요).</div>
    </div>
    <div class="form-grid">
      <div class="form-row"><label>위도 (lat)</label><input id="f-lat" type="number" step="0.00001" value="${s?.latitude || ''}"></div>
      <div class="form-row"><label>경도 (lng)</label><input id="f-lng" type="number" step="0.00001" value="${s?.longitude || ''}"></div>
      <div class="form-row"><label>지오펜스 반경 (m)</label><input id="f-fence" type="number" value="${s?.geofence_meters || 200}"></div>
      <div class="form-row"><label>&nbsp;</label><button class="btn" id="grab-gps" type="button">📍 현재 위치 가져오기</button></div>
      <div class="form-row"><label>계약금액</label><input id="f-contract" type="number" value="${s?.contract_amount || 0}"></div>
      <div class="form-row"><label>누적 수금</label><input id="f-paid" type="number" value="${s?.paid_amount || 0}"></div>
      <div class="form-row"><label>착공일</label><input id="f-start" type="date" value="${s?.start_date || ''}"></div>
      <div class="form-row"><label>준공예정</label><input id="f-end" type="date" value="${s?.end_date || ''}"></div>
    </div>
  `, async () => {
    const payload = {
      name: $('#f-name').value.trim(),
      company_id: $('#f-company').value ? +$('#f-company').value : null,
      manager: $('#f-manager').value,
      status: $('#f-status').value,
      address: $('#f-address').value,
      latitude: $('#f-lat').value ? +$('#f-lat').value : null,
      longitude: $('#f-lng').value ? +$('#f-lng').value : null,
      geofence_meters: +$('#f-fence').value || 200,
      contract_amount: +$('#f-contract').value || 0,
      paid_amount: +$('#f-paid').value || 0,
      start_date: $('#f-start').value,
      end_date: $('#f-end').value,
    };
    if (!payload.name) { alert('현장명을 입력하세요'); return false; }
    if (isNew) await api.post('/api/sites', payload);
    else      await api.put('/api/sites/' + s.id, payload);
    navigate();
  });
  setTimeout(() => {
    const gbtn = $('#grab-gps');
    if (gbtn) gbtn.onclick = () => {
      navigator.geolocation.getCurrentPosition(p => {
        $('#f-lat').value = p.coords.latitude.toFixed(6);
        $('#f-lng').value = p.coords.longitude.toFixed(6);
      }, e => alert('GPS 권한이 필요합니다: ' + e.message), { enableHighAccuracy: true });
    };
    const gcb = $('#geocode-btn');
    const hint = $('#geo-hint');
    if (gcb) gcb.onclick = async () => {
      const addr = $('#f-address').value.trim();
      if (!addr) { hint.textContent = '주소를 먼저 입력하세요.'; return; }
      hint.textContent = '🔄 주소를 좌표로 변환 중…';
      const r = await geocodeAddress(addr);
      if (r) {
        $('#f-lat').value = r.lat.toFixed(6);
        $('#f-lng').value = r.lng.toFixed(6);
        hint.textContent = '✅ 변환 성공: ' + r.display;
      } else {
        hint.textContent = '❌ 주소를 찾지 못했습니다. 시·구·도로명·번지 형태로 단순화하거나, 📍 버튼으로 직접 입력하세요.';
      }
    };
  }, 50);
}

// ============================================================
// 지도 (Leaflet + OpenStreetMap) — 현장별 배치/출근/GPS
// ============================================================
let mapInstance = null;
route('/map', async () => {
  const tdy = today();
  const [sites, todayClock, planDep, actualDep] = await Promise.all([
    api.get('/api/sites'),
    api.get('/api/clock/today'),
    api.get('/api/deployments?date=' + tdy + '&kind=plan'),
    api.get('/api/deployments?date=' + tdy + '&kind=actual'),
  ]);
  const withCoords = sites.filter(s => s.latitude && s.longitude);

  // 현장별로 묶기
  const bySite = {};
  sites.forEach(s => bySite[s.id] = { site: s, plan: [], actual: [], clocks: [] });
  planDep.forEach(d => bySite[d.site_id] && bySite[d.site_id].plan.push(d));
  actualDep.forEach(d => bySite[d.site_id] && bySite[d.site_id].actual.push(d));
  todayClock.forEach(c => bySite[c.site_id] && bySite[c.site_id].clocks.push(c));

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">현장 지도 — ${tdy}</div>
        <div class="page-sub">현장 위치 + 오늘 배치(계획/실적) + 출근 GPS. 마커 클릭하면 명단이 나옵니다.</div>
      </div>
      <div class="map-legend">
        <span class="dot site"></span> 현장 (${withCoords.length}/${sites.length})
        &nbsp; <span class="dot in"></span> 출근 GPS (정상)
        &nbsp; <span class="dot out"></span> 현장 밖
      </div>
    </div>
    <div id="map" class="map-container"></div>

    <div style="margin-top:14px; display:grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap:12px;" id="site-roster">
      ${sites.map(s => siteRosterCard(bySite[s.id])).join('')}
    </div>
  `;

  if (mapInstance) { mapInstance.remove(); mapInstance = null; }
  let center = [37.5665, 126.9780];
  if (withCoords.length) center = [withCoords[0].latitude, withCoords[0].longitude];
  mapInstance = L.map('map').setView(center, 9);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19, attribution: '© OpenStreetMap'
  }).addTo(mapInstance);

  const bounds = [];

  // 현장 마커 + 지오펜스 + 명단 팝업
  withCoords.forEach(s => {
    const grp = bySite[s.id];
    const popupHtml =
      '<div style="min-width:230px"><b style="font-size:13px">' + s.name + '</b>' +
      '<div style="font-size:11px;color:#707682;margin-bottom:6px">' + (s.address || '') + '</div>' +
      '<div style="font-size:12px;margin-bottom:4px;"><b>📋 계획 ' + grp.plan.length + '명</b><br>' +
        (grp.plan.length ? grp.plan.map(d => d.worker_name).join(', ') : '<i style="color:#999">없음</i>') + '</div>' +
      '<div style="font-size:12px;margin-bottom:4px;"><b>✅ 실적 ' + grp.actual.length + '명</b><br>' +
        (grp.actual.length ? grp.actual.map(d => d.worker_name).join(', ') : '<i style="color:#999">없음</i>') + '</div>' +
      '<div style="font-size:12px;"><b>📍 GPS 출근 ' + grp.clocks.length + '명</b><br>' +
        (grp.clocks.length ? grp.clocks.map(c =>
          c.worker_name + ' (' + (c.clock_in ? c.clock_in.split('T')[1].slice(0,5) : '-') + ', ' +
          (c.in_verified ? '✅' : '⚠️') + ' ' +
          (c.in_distance_m != null ? Math.round(c.in_distance_m) + 'm' : '-') + ')'
        ).join('<br>') : '<i style="color:#999">없음</i>') +
      '</div></div>';

    L.marker([s.latitude, s.longitude], { title: s.name })
      .addTo(mapInstance).bindPopup(popupHtml);
    L.circle([s.latitude, s.longitude], {
      radius: s.geofence_meters || 200,
      color: '#2d4a8a', weight: 1, fillColor: '#2d4a8a', fillOpacity: 0.08
    }).addTo(mapInstance);
    bounds.push([s.latitude, s.longitude]);
  });

  // 출근 GPS 마커 + 현장으로 잇는 선
  todayClock.forEach(r => {
    if (!r.in_lat || !r.in_lng) return;
    const color = r.in_verified ? '#15803d' : '#c0392b';
    const t = r.clock_in ? r.clock_in.split('T')[1].slice(0,5) : '';
    L.circleMarker([r.in_lat, r.in_lng], {
      radius: 7, color, fillColor: color, fillOpacity: 0.85, weight: 2
    }).addTo(mapInstance).bindPopup(
      '<b>' + r.worker_name + '</b><br>' + r.site_name + ' · ' + t +
      '<br>거리 ' + (r.in_distance_m != null ? Math.round(r.in_distance_m) + 'm' : '-')
    );
    // 실제 GPS → 현장 까지 잇는 점선
    const site = sites.find(s => s.id === r.site_id);
    if (site && site.latitude && site.longitude) {
      L.polyline([[r.in_lat, r.in_lng], [site.latitude, site.longitude]], {
        color, weight: 1.5, opacity: 0.6, dashArray: '4, 4'
      }).addTo(mapInstance);
    }
    bounds.push([r.in_lat, r.in_lng]);
  });

  if (bounds.length) mapInstance.fitBounds(bounds, { padding: [40, 40] });
});

function siteRosterCard(grp) {
  const s = grp.site;
  const planNames   = new Set(grp.plan.map(d => d.worker_name));
  const actualNames = new Set(grp.actual.map(d => d.worker_name));
  const clockedNames = new Set(grp.clocks.map(c => c.worker_name));

  const noShow = [...planNames].filter(n => !actualNames.has(n)); // 계획만 있고 실적 없음
  const extras = [...actualNames].filter(n => !planNames.has(n)); // 계획 없이 실적만

  return `<div class="card" style="padding:14px 16px">
    <div style="display:flex; justify-content:space-between; align-items:flex-start; gap:8px; margin-bottom:8px">
      <div>
        <div style="font-weight:700; font-size:14px">${s.name}</div>
        <div style="font-size:11px; color:var(--text-muted)">${s.address || ''}</div>
      </div>
      <span class="badge ${s.status==='active'?'badge-green':'badge-gray'}">${s.status==='active'?'진행':'마감'}</span>
    </div>

    <div style="display:grid; grid-template-columns: repeat(3,1fr); gap:6px; font-size:11px; margin-bottom:10px;">
      <div style="text-align:center; padding:6px; background:#eef3fc; border-radius:6px;">
        <div style="color:#707682">📋 계획</div><div style="font-weight:700; font-size:14px; color:var(--primary)">${grp.plan.length}</div>
      </div>
      <div style="text-align:center; padding:6px; background:#e7f6ec; border-radius:6px;">
        <div style="color:#707682">✅ 실적</div><div style="font-weight:700; font-size:14px; color:#15803d">${grp.actual.length}</div>
      </div>
      <div style="text-align:center; padding:6px; background:#fef0e0; border-radius:6px;">
        <div style="color:#707682">📍 GPS</div><div style="font-weight:700; font-size:14px; color:#a35907">${grp.clocks.length}</div>
      </div>
    </div>

    ${grp.actual.length ? `<div style="font-size:12px; margin-bottom:6px;">
      <b style="color:#15803d">출역 명단:</b><br>
      ${grp.actual.map(d => {
        const c = grp.clocks.find(x => x.worker_id === d.worker_id);
        if (c) {
          const ico = c.in_verified ? '✅' : '⚠️';
          const dist = c.in_distance_m != null ? Math.round(c.in_distance_m)+'m' : '?';
          const t = c.clock_in ? c.clock_in.split('T')[1].slice(0,5) : '';
          return `${d.worker_name} <span style="color:#707682; font-size:11px">${ico} ${t} ${dist}</span>`;
        }
        return `${d.worker_name} <span style="color:#a35907; font-size:11px">(GPS 출근 미체크)</span>`;
      }).join('<br>')}
    </div>` : '<div style="font-size:11px; color:var(--text-muted); margin-bottom:6px;">오늘 실적 없음</div>'}

    ${noShow.length ? `<div style="font-size:11.5px; padding:6px 8px; background:#fbe9ea; border-radius:6px; color:#b3373d; margin-bottom:4px;">
      ⚠️ 계획 있고 실적 없음: ${noShow.join(', ')}
    </div>` : ''}
    ${extras.length ? `<div style="font-size:11.5px; padding:6px 8px; background:#fef0e0; border-radius:6px; color:#a35907; margin-bottom:4px;">
      ➕ 계획 없이 추가 투입: ${extras.join(', ')}
    </div>` : ''}
  </div>`;
}

// ============================================================
// 프로젝트 개요 (현장을 큰 카드로 — 일정·인력·비용·손익)
// ============================================================
route('/projects', async () => {
  const projects = await api.get('/api/projects?include_closed=false');

  const totalContract = projects.reduce((s,p) => s + (p.contract_amount||0), 0);
  const totalPaid     = projects.reduce((s,p) => s + (p.paid_amount||0), 0);
  const totalLabor    = projects.reduce((s,p) => s + (p.labor_cost||0), 0);
  const totalEstProfit = projects.reduce((s,p) => s + (p.estimated_profit||0), 0);
  const totalPersonDays = projects.reduce((s,p) => s + (p.person_days||0), 0);

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">프로젝트 개요</div>
        <div class="page-sub">활성 현장 ${projects.length}개 · 일정·투입 인력·노무비·예상 손익을 한 화면에서</div>
      </div>
    </div>

    <div class="stat-grid">
      <div class="stat accent"><div class="label">활성 현장</div><div class="value">${projects.length}</div></div>
      <div class="stat"><div class="label">누적 투입 인일</div><div class="value">${fmt(totalPersonDays)}</div><div class="delta">실적 기준 person-days</div></div>
      <div class="stat warning"><div class="label">누적 노무비</div><div class="value">${fmtMoney(totalLabor)}</div><div class="delta">전 현장 합계</div></div>
      <div class="stat success"><div class="label">예상 총 손익</div><div class="value">${fmtMoney(totalEstProfit)}</div><div class="delta">계약 - 예상 노무비</div></div>
    </div>

    <div id="proj-list">${projects.map(projectCardHtml).join('')}</div>

    <div class="diff-banner" style="margin-top:8px">
      💡 <b>예상 손익 계산법:</b> 일정 진행률 기준으로 현재까지의 노무비를 선형 외삽해 예상 총 노무비를 추정한 뒤,
      계약금액에서 뺀 값입니다. 자재비·외주비는 아직 시스템에 없어 포함되지 않으므로 실제 마진보다 낙관적으로 보입니다.
    </div>
  `;
});

function projectCardHtml(p) {
  const dStart = p.start_date || '-';
  const dEnd   = p.end_date   || '-';
  const pct    = Math.max(0, Math.min(100, p.progress_pct || 0));
  const overdue = p.schedule_status === 'overdue';
  const upcoming = p.schedule_status === 'upcoming';
  const profitClass = p.estimated_profit >= 0 ? 'profit' : 'profit-neg';
  const statusBadge = overdue
    ? '<span class="badge badge-red">기한 초과</span>'
    : upcoming ? '<span class="badge badge-gray">예정</span>'
    : p.status === 'closed' ? '<span class="badge badge-gray">마감</span>'
    : '<span class="badge badge-green">진행</span>';

  return `<div class="project-card">
    <div class="pc-head">
      <div>
        <div class="pc-name">${p.name}</div>
        <div class="pc-sub">${p.company_name || ''} · ${p.address || ''} · 담당 ${p.manager || '-'}</div>
      </div>
      <div class="pc-tags">${statusBadge}</div>
    </div>

    <div>
      <div class="gantt">
        <div class="gantt-fill ${overdue?'overdue':''}" style="width:${pct}%"></div>
        ${(!upcoming && pct > 0 && pct < 100) ? `<div class="gantt-today" style="left:${pct}%"></div>` : ''}
      </div>
      <div class="gantt-labels">
        <span><b>${dStart}</b> 착공</span>
        <span>경과 <b>${p.days_elapsed}일</b> / 총 <b>${p.days_total}일</b> · 남은 <b>${p.days_remaining}일</b> · 진행 <b>${pct}%</b></span>
        <span>준공 <b>${dEnd}</b></span>
      </div>
    </div>

    <div class="pc-stats">
      <div class="pc-stat"><div class="l">오늘 투입</div><div class="v">${p.today_count}명</div><div class="h">실적 기준</div></div>
      <div class="pc-stat"><div class="l">누적 인일</div><div class="v">${fmt(p.person_days)}</div><div class="h">${p.unique_workers}명 누적</div></div>
      <div class="pc-stat labor"><div class="l">누적 노무비</div><div class="v">${fmtMoney(p.labor_cost)}</div><div class="h">실적 × 일당 합</div></div>
      <div class="pc-stat labor"><div class="l">예상 총 노무비</div><div class="v">${fmtMoney(p.projected_labor)}</div><div class="h">진행률 기반 추정</div></div>
      <div class="pc-stat contract"><div class="l">계약금액</div><div class="v">${fmtMoney(p.contract_amount)}</div></div>
      <div class="pc-stat contract"><div class="l">누적 수금</div><div class="v">${fmtMoney(p.paid_amount)}</div><div class="h">잔여 ${fmtMoney(p.contract_remaining)}</div></div>
      <div class="pc-stat ${profitClass}"><div class="l">예상 손익</div><div class="v">${fmtMoney(p.estimated_profit)}</div><div class="h">계약 - 예상 노무비</div></div>
    </div>
  </div>`;
}

// ============================================================
// 아침 요약 (Phase 5) — 경영진용 한 페이지
// ============================================================
route('/morning', async () => {
  const d = await api.get('/api/morning');
  const k = d.kpi, n = d.notifications;
  const sevIcon = { urgent: '🚨', warning: '⚠️', info: 'ℹ️' };

  $('#app').innerHTML = `
    <div class="morning-hero">
      <h2>오늘 아침 — ${d.as_of}</h2>
      <div class="date">한 페이지 요약. 빨간 항목부터 처리하시면 됩니다.</div>
      <div class="quick">
        <div><div class="label">활성 현장</div><div class="value">${k.active_sites}</div><div class="delta">총 직원 ${k.workers_total}명</div></div>
        <div><div class="label">오늘 출근</div><div class="value">${k.clocked_today}</div><div class="delta">어제 ${k.clocked_yesterday}명</div></div>
        <div><div class="label">신규 가입 (7일)</div><div class="value">${k.new_signups_week}</div></div>
        <div><div class="label">잔여 공사대금</div><div class="value">${fmtMoney(k.remaining)}</div><div class="delta">계약 ${fmtMoney(k.contract_total)}</div></div>
      </div>
    </div>

    <div class="stat-grid">
      <div class="stat ${n.urgent>0?'warning':''}"><div class="label">🚨 긴급</div><div class="value">${n.urgent}</div></div>
      <div class="stat warning"><div class="label">⚠️ 경고</div><div class="value">${n.warning}</div></div>
      <div class="stat accent"><div class="label">ℹ️ 안내</div><div class="value">${n.info}</div></div>
    </div>

    <div class="card" style="margin-bottom:14px">
      <div class="card-head"><h3>오늘의 액션 (Top ${d.top_actions.length})</h3></div>
      <div class="card-pad">
        ${d.top_actions.length === 0 ? '<div style="color:var(--text-muted)">처리할 액션 없음 ✅</div>' :
          d.top_actions.map(a => `<div class="action-card">
            <div>
              <span class="ico">${sevIcon[a.severity]||'•'}</span>
              <span class="${a.severity}"><b>${a.title}</b></span>
            </div>
            <a href="${a.link||'#'}" class="btn btn-sm">이동 →</a>
          </div>`).join('')}
      </div>
    </div>

    <div class="card">
      <div class="card-head"><h3>진행 중 프로세스</h3></div>
      <table class="table">
        <thead><tr><th>워크플로우</th><th>상태</th><th style="text-align:right">건수</th></tr></thead>
        <tbody>
          ${d.processes_by_state.length === 0 ? '<tr><td colspan="3" style="text-align:center;color:var(--text-muted)">없음</td></tr>' :
            d.processes_by_state.map(p => `<tr>
              <td>${(window.__procDefs||{})[p.workflow]?.name || p.workflow}</td>
              <td>${p.current_state}</td>
              <td style="text-align:right"><b>${p.cnt}</b></td>
            </tr>`).join('')}
        </tbody>
      </table>
    </div>
  `;
});

// ============================================================
// 프로세스 칸반 (Phase 4)
// ============================================================
let procState = { workflow: null, defs: null };

route('/processes', async () => {
  if (!procState.defs) {
    procState.defs = await api.get('/api/process-definitions');
    window.__procDefs = {};
    procState.defs.forEach(d => window.__procDefs[d.id] = d);
  }
  if (!procState.workflow) procState.workflow = procState.defs[0]?.id || 'sales';

  const procs = await api.get('/api/processes?workflow=' + procState.workflow);
  const def = procState.defs.find(d => d.id === procState.workflow);
  if (!def) { $('#app').innerHTML = '정의 없음'; return; }

  // 카운트 per workflow
  const allProcs = await api.get('/api/processes');
  const cntByWf = {};
  allProcs.forEach(p => { cntByWf[p.workflow] = (cntByWf[p.workflow] || 0) + 1; });

  // 상태별로 묶기
  const byState = {};
  def.states.forEach(s => byState[s] = []);
  procs.forEach(p => {
    if (byState[p.current_state]) byState[p.current_state].push(p);
    else byState[p.current_state] = [p];
  });

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">프로세스 보드</div>
        <div class="page-sub">7개 업무 흐름 — 카드 클릭하면 다음 단계로 진행할 수 있습니다.</div>
      </div>
    </div>

    <div class="proc-tabs">
      ${procState.defs.map(d => `<span class="proc-tab ${d.id===procState.workflow?'active':''}" data-w="${d.id}">${d.name}<span class="cnt">${cntByWf[d.id]||0}</span></span>`).join('')}
    </div>

    <div class="kanban">
      ${def.states.map(state => {
        const cards = byState[state] || [];
        const isTerminal = (def.terminal||[]).includes(state);
        return `<div class="kanban-col ${isTerminal?'terminal':''}">
          <h5>${state}<span class="cnt">${cards.length}</span></h5>
          ${cards.length === 0 ? '<div class="proc-empty">—</div>' :
            cards.map(p => `<div class="proc-card" data-pid="${p.id}" data-cur="${p.current_state}">
              <div class="name">${p.subject_name||('#'+p.subject_id)}</div>
              ${p.scope_key ? `<div class="scope">${p.scope_key}</div>` : ''}
              <div class="updated">${(p.updated_at||p.started_at||'').slice(0,16).replace('T',' ')}</div>
            </div>`).join('')}
        </div>`;
      }).join('')}
    </div>
  `;

  $$('.proc-tab').forEach(t => t.onclick = () => {
    procState.workflow = t.dataset.w; navigate();
  });
  $$('.proc-card').forEach(card => card.onclick = async () => {
    const pid = +card.dataset.pid;
    const cur = card.dataset.cur;
    const states = def.states;
    const idx = states.indexOf(cur);
    const choices = states.slice(idx+1);
    if (choices.length === 0) { alert('이미 마지막 상태입니다.'); return; }
    const target = prompt(
      '다음 상태를 선택하세요:\n' + choices.map((s,i) => `${i+1}. ${s}`).join('\n') + '\n\n번호 또는 상태명 입력:',
      '1');
    if (!target) return;
    const num = parseInt(target);
    const targetState = (!isNaN(num) && num >= 1 && num <= choices.length) ? choices[num-1] :
                        choices.includes(target) ? target : null;
    if (!targetState) { alert('잘못된 선택'); return; }
    await api.post('/api/processes/'+pid+'/advance', { target_state: targetState });
    navigate();
  });
});

// ============================================================
// 알림 (Phase 5)
// ============================================================
route('/inbox', async () => {
  // refresh 룰 한 번
  await fetch('/api/notifications/refresh', { method: 'POST' });
  const notifs = await api.get('/api/notifications');

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">📬 알림</div>
        <div class="page-sub">자동 룰 엔진이 만든 ${notifs.length}건의 처리 항목</div>
      </div>
      <button class="btn" id="all-read">전부 읽음 처리</button>
    </div>

    ${notifs.length === 0 ? '<div class="card card-pad" style="text-align:center; color:var(--text-muted)">처리할 알림 없음 ✅</div>' :
      notifs.map(n => `<div class="notif ${n.is_read?'':'unread'}" data-id="${n.id}" data-link="${n.link||''}">
        <div class="sev-bar ${n.severity}"></div>
        <div class="body">
          <div class="title">${n.title}</div>
          ${n.message ? `<div class="msg">${n.message}</div>` : ''}
          <div class="meta">${(n.created_at||'').slice(0,16).replace('T',' ')} · ${n.rule_type}</div>
        </div>
        <div class="actions">
          <span class="notif-tag">${{urgent:'🚨긴급',warning:'⚠️경고',info:'ℹ️안내'}[n.severity]||n.severity}</span>
          ${n.link ? `<a href="${n.link}" class="btn btn-sm" onclick="event.stopPropagation()">이동 →</a>` : ''}
        </div>
      </div>`).join('')}
  `;

  $('#all-read').onclick = async () => {
    await fetch('/api/notifications/read-all', { method: 'POST' });
    navigate();
    refreshNotifBadge();
  };
  $$('.notif').forEach(el => el.onclick = async () => {
    const id = +el.dataset.id;
    await fetch('/api/notifications/'+id+'/read', { method: 'POST' });
    el.classList.remove('unread');
    refreshNotifBadge();
  });
});

async function refreshNotifBadge() {
  try {
    const r = await fetch('/api/notifications/count').then(r => r.json());
    const badge = document.getElementById('notif-badge');
    if (!badge) return;
    if (r.unread > 0) {
      badge.style.display = 'inline-block';
      badge.textContent = r.unread > 99 ? '99+' : r.unread;
      badge.style.background = r.urgent > 0 ? '#b3373d' : '#d97706';
    } else {
      badge.style.display = 'none';
    }
  } catch (e) {}
}

// ============================================================
// 3 시점 (현장/행정/재무) — Phase 3
// ============================================================
let lensState = { tab: 'field' };

route('/lens', async () => { await renderLens(); });

async function renderLens() {
  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">3 시점 — 현장 · 행정 · 재무</div>
        <div class="page-sub">같은 사실을 3가지 관점에서. 모두 events 데이터 기반 — 디지털 트윈의 본질적 UX.</div>
      </div>
    </div>

    <div class="lens-tabs">
      <button data-t="field"   class="${lensState.tab==='field'?'active':''}">
        <span class="ico">🌳</span>현장 시점
      </button>
      <button data-t="admin"   class="${lensState.tab==='admin'?'active':''}">
        <span class="ico">📋</span>행정 시점
      </button>
      <button data-t="finance" class="${lensState.tab==='finance'?'active':''}">
        <span class="ico">💰</span>재무 시점
      </button>
    </div>

    <div id="lens-body">로딩…</div>
  `;
  $$('.lens-tabs button').forEach(b => b.onclick = () => {
    lensState.tab = b.dataset.t;
    renderLens();
  });
  if (lensState.tab === 'field')   await renderLensField();
  else if (lensState.tab === 'admin') await renderLensAdmin();
  else                                await renderLensFinance();
}

async function renderLensField() {
  const d = await api.get('/api/views/field?days=1');
  const totalToday = d.active_sites.reduce((s, x) => s + (x.clocked_in_today||0), 0);
  $('#lens-body').innerHTML = `
    <div class="stat-grid">
      <div class="stat accent"><div class="label">활성 현장</div><div class="value">${d.active_sites.length}</div></div>
      <div class="stat success"><div class="label">오늘 출근 인원</div><div class="value">${totalToday}</div><div class="delta">GPS 인증 기준</div></div>
      <div class="stat warning"><div class="label">최근 활동 이벤트</div><div class="value">${d.recent_events.length}</div><div class="delta">출퇴근·배치</div></div>
    </div>

    <div class="lens-section">
      <h3>🌳 활성 현장별 오늘 인원</h3>
      ${d.active_sites.length === 0 ? '<div class="empty">활성 현장 없음</div>' :
        '<table class="table"><thead><tr><th>현장</th><th>주소</th><th>지오펜스</th><th style="text-align:right">오늘 인원</th></tr></thead><tbody>'
        + d.active_sites.map(s => `<tr>
          <td><b>${s.name}</b></td>
          <td style="font-size:11px;color:var(--text-muted)">${s.address||'-'}</td>
          <td style="font-size:11px">${s.geofence_meters||200}m</td>
          <td style="text-align:right"><b>${s.clocked_in_today}명</b></td>
        </tr>`).join('') + '</tbody></table>'}
    </div>

    <div class="lens-section">
      <h3>🌳 최근 24시간 활동 (${d.recent_events.length}건)</h3>
      <div class="events-mini">
        ${d.recent_events.length === 0 ? '<div class="empty">없음</div>' :
          d.recent_events.map(eventCardHtml).join('')}
      </div>
    </div>
  `;
}

async function renderLensAdmin() {
  const d = await api.get('/api/views/admin');
  const cnt = d.pending_review.length + d.sites_missing_gps.length +
              d.workers_no_company.length + d.workers_no_wage.length;
  $('#lens-body').innerHTML = `
    <div class="stat-grid">
      <div class="stat warning"><div class="label">처리 대기</div><div class="value">${cnt}</div><div class="delta">관리자 액션 필요</div></div>
      <div class="stat accent"><div class="label">이번 달 신고 대상</div><div class="value">${d.report_targets_this_month.length}</div><div class="delta">일용근로내용 확인신고</div></div>
      <div class="stat"><div class="label">최근 7일 신규</div><div class="value">${d.recent_signups_7d.length}</div><div class="delta">신규 가입자</div></div>
    </div>

    <div class="lens-section">
      <h3>📋 본사 검토 대기 (${d.pending_review.length})</h3>
      ${d.pending_review.length === 0 ? '<div class="empty">처리 대기 없음 ✅</div>' :
        d.pending_review.map(w => `<div class="todo-card">
          <div class="head">${w.name} · ${w.phone||'-'} · 가입일 ${w.hired_date||'-'}</div>
          <ul><li>${w.note || '검토 필요'}</li><li>→ <a href="#/workers" style="color:var(--primary)">직원 페이지에서 일당·법인 보강</a></li></ul>
        </div>`).join('')}
    </div>

    <div class="lens-section">
      <h3>📋 이번 달 일용근로내용확인신고 대상 (${d.report_targets_this_month.length}명)</h3>
      ${d.report_targets_this_month.length === 0 ? '<div class="empty">대상 없음</div>' :
        '<table class="table"><thead><tr><th>이름</th><th style="text-align:right">출근일수</th><th>현장</th></tr></thead><tbody>'
        + d.report_targets_this_month.map(r => `<tr>
          <td><b>${r.worker_name||'#'+r.worker_id}</b></td>
          <td style="text-align:right"><b>${r.days}일</b></td>
          <td style="font-size:11px;color:var(--text-muted)">${(r.sites||'').replace(/null,?/g,'').replace(/,$/,'')||'-'}</td>
        </tr>`).join('') + '</tbody></table>'}
    </div>

    ${d.sites_missing_gps.length ? `<div class="lens-section">
      <h3>📋 GPS 좌표 미설정 현장 (${d.sites_missing_gps.length})</h3>
      ${d.sites_missing_gps.map(s => `<div class="todo-card">
        <div class="head">${s.name} · ${s.address||'-'}</div>
        <ul><li>출퇴근 GPS 검증이 안 됩니다 — 현장 가서 좌표 등록 필요</li></ul>
      </div>`).join('')}
    </div>` : ''}

    ${d.workers_no_company.length ? `<div class="lens-section">
      <h3>📋 법인 미배정 직원 (${d.workers_no_company.length})</h3>
      ${d.workers_no_company.map(w => `<div class="todo-card">
        <div class="head">${w.name} · ${w.phone||'-'} · ${w.worker_type==='office'?'사무직':'일용직'}</div>
        <ul><li>4대보험 가입·세무 처리에 법인 배정이 필요합니다</li></ul>
      </div>`).join('')}
    </div>` : ''}

    ${d.workers_no_wage.length ? `<div class="lens-section">
      <h3>📋 일당 미설정 일용직 (${d.workers_no_wage.length})</h3>
      ${d.workers_no_wage.map(w => `<div class="todo-card">
        <div class="head">${w.name} · ${w.phone||'-'}</div>
        <ul><li>일당이 0원 — 노무비 자동 집계가 안 됩니다</li></ul>
      </div>`).join('')}
    </div>` : ''}
  `;
}

async function renderLensFinance() {
  const d = await api.get('/api/views/finance');
  $('#lens-body').innerHTML = `
    <div class="stat-grid">
      <div class="stat accent"><div class="label">계약 (수주)</div><div class="value">${fmtMoney(d.totals.contract)}</div><div class="delta">SiteCreated 합산</div></div>
      <div class="stat warning"><div class="label">누적 비용 (노무비)</div><div class="value">${fmtMoney(d.totals.expense)}</div><div class="delta">ClockIn × 일당</div></div>
      <div class="stat success"><div class="label">실수금 (revenue)</div><div class="value">${fmtMoney(d.totals.revenue)}</div><div class="delta">아직 입력 안 됨</div></div>
    </div>

    <div class="lens-section">
      <h3>💰 현장별 (${d.by_site.length})</h3>
      ${d.by_site.length === 0 ? '<div class="empty">데이터 없음</div>' :
        d.by_site.map(r => `<div class="fin-row">
          <span class="label">${r.name}</span>
          <span class="num contract" title="계약">${fmtMoney(r.contract)}</span>
          <span class="num expense" title="비용">−${fmtMoney(r.expense)}</span>
          <span class="num" title="잔액"><b>${fmtMoney(r.contract - r.expense)}</b></span>
          <span class="count">${r.count}건</span>
        </div>`).join('')}
    </div>

    <div class="lens-section">
      <h3>💰 법인별 (${d.by_company.length})</h3>
      ${d.by_company.length === 0 ? '<div class="empty">데이터 없음</div>' :
        d.by_company.map(r => `<div class="fin-row">
          <span class="label">${r.name}</span>
          <span class="num contract">${fmtMoney(r.contract)}</span>
          <span class="num expense">−${fmtMoney(r.expense)}</span>
          <span class="num"><b>${fmtMoney(r.contract - r.expense)}</b></span>
          <span class="count">${r.count}건</span>
        </div>`).join('')}
    </div>

    <div class="lens-section">
      <h3>💰 계정 과목별</h3>
      ${d.by_account.length === 0 ? '<div class="empty">데이터 없음</div>' :
        d.by_account.map(r => {
          const total = r.contract + r.expense + r.revenue;
          const cls = r.expense > 0 ? 'expense' : r.contract > 0 ? 'contract' : 'revenue';
          return `<div class="fin-row">
            <span class="label">${r.name}</span>
            <span class="num ${cls}">${fmtMoney(total)}</span>
            <span class="count">${r.count}건</span>
            <span></span><span></span>
          </div>`;
        }).join('')}
    </div>

    <div class="lens-section">
      <h3>💰 월별 추이 (${d.by_month.length})</h3>
      ${d.by_month.length === 0 ? '<div class="empty">데이터 없음 — 이벤트가 쌓일수록 추이가 만들어집니다</div>' :
        d.by_month.map(r => `<div class="fin-row">
          <span class="label">${r.name}</span>
          <span class="num contract">${fmtMoney(r.contract)}</span>
          <span class="num expense">−${fmtMoney(r.expense)}</span>
          <span class="num"><b>${fmtMoney(r.contract - r.expense)}</b></span>
          <span class="count">${r.count}건</span>
        </div>`).join('')}
    </div>
  `;
}

// ============================================================
// 그래프 / 온톨로지 (Phase 2)
// ============================================================
const ENTITY_LABELS = { Person:'사람', Place:'현장', Org:'법인', User:'관리자' };
const PREDICATE_LABELS = {
  employed_by:'소속',
  owned_by:'소속',
  has_role_in:'권한 보유',
  manages:'담당',
};
let graphState = { tab:'Person', selectedType:null, selectedId:null };

route('/graph', async () => {
  await renderGraph();
});

async function renderGraph() {
  const stats = await api.get('/api/graph/stats');
  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">엔티티 그래프 (온톨로지)</div>
        <div class="page-sub">사람·현장·법인을 클릭하면 그것과 연결된 모든 관계 + 최근 이벤트가 한 화면에. (Person ${stats.Person} · Place ${stats.Place} · Org ${stats.Org} · 관계 ${stats.Relations})</div>
      </div>
    </div>

    <div class="graph-layout">
      <div class="graph-side">
        <div class="graph-tabs">
          <button data-tab="Person" class="${graphState.tab==='Person'?'active':''}">사람 (${stats.Person})</button>
          <button data-tab="Place"  class="${graphState.tab==='Place'?'active':''}">현장 (${stats.Place})</button>
          <button data-tab="Org"    class="${graphState.tab==='Org'?'active':''}">법인 (${stats.Org})</button>
          <button data-tab="User"   class="${graphState.tab==='User'?'active':''}">관리자 (${stats.User})</button>
        </div>
        <div id="ent-list">로딩…</div>
      </div>
      <div class="graph-main" id="graph-main">
        <div class="empty">왼쪽에서 엔티티를 선택하세요.</div>
      </div>
    </div>
  `;

  $$('.graph-tabs button').forEach(b => b.onclick = async () => {
    graphState.tab = b.dataset.tab;
    graphState.selectedType = null;
    graphState.selectedId = null;
    await renderGraph();
  });

  await loadEntityList();
  if (graphState.selectedType && graphState.selectedId) {
    await loadEntityDetail(graphState.selectedType, graphState.selectedId);
  }
}

async function loadEntityList() {
  const ents = await api.get('/api/graph/entities?entity_type=' + graphState.tab);
  $('#ent-list').innerHTML = ents.length === 0
    ? '<div style="font-size:12px; color:var(--text-muted); padding:8px">없음</div>'
    : ents.map(e => {
        const isActive = (graphState.selectedType === graphState.tab && graphState.selectedId === e.id);
        let meta = '';
        if (graphState.tab === 'Person') meta = e.job_role || (e.worker_type==='office'?'사무직':'일용직');
        else if (graphState.tab === 'Place') meta = e.status === 'active' ? '진행' : '마감';
        else if (graphState.tab === 'Org') meta = e.business_no || '';
        else if (graphState.tab === 'User') meta = e.role;
        return `<div class="ent-item ${isActive?'active':''}" data-id="${e.id}">
          <span>${e.name||e.username||'#'+e.id}</span>
          <span class="meta">${meta}</span>
        </div>`;
      }).join('');
  $$('.ent-item').forEach(el => el.onclick = () => {
    const id = +el.dataset.id;
    graphState.selectedType = graphState.tab;
    graphState.selectedId = id;
    loadEntityDetail(graphState.tab, id);
    $$('.ent-item').forEach(x => x.classList.remove('active'));
    el.classList.add('active');
  });
}

async function loadEntityDetail(type, id) {
  $('#graph-main').innerHTML = '<div class="empty">로딩 중…</div>';
  let data;
  try { data = await api.get(`/api/graph/entity/${type}/${id}`); }
  catch (e) { $('#graph-main').innerHTML = '<div class="empty">불러오기 실패</div>'; return; }

  const e = data.entity;
  const name = e.name || e.username || '(이름 없음)';
  const meta = type === 'Person'
    ? `${e.worker_type==='office'?'사무직':'일용직'}${e.job_role?' · '+e.job_role:''}${e.phone?' · '+e.phone:''}`
    : type === 'Place'
    ? `${e.address||''}${e.manager?' · 담당 '+e.manager:''}`
    : type === 'Org'
    ? `${e.business_no||''}${e.ceo?' · 대표 '+e.ceo:''}`
    : `${e.role}${e.username?' · '+e.username:''}`;

  $('#graph-main').innerHTML = `
    <div class="entity-header">
      <div>
        <div class="name">${name}</div>
        <div class="meta">${meta}</div>
      </div>
      <span class="type-badge">${ENTITY_LABELS[type]}</span>
    </div>

    <div class="rel-section">
      <h4>나가는 관계 (${data.outgoing_relations.length})</h4>
      ${data.outgoing_relations.length === 0
        ? '<div style="font-size:12px; color:var(--text-muted)">없음</div>'
        : data.outgoing_relations.map(r => `<div class="rel-row">
            <b>이 ${ENTITY_LABELS[type]||type}</b>
            <span class="pred">${PREDICATE_LABELS[r.predicate]||r.predicate}</span>
            <span class="target" data-type="${r.object_type}" data-id="${r.object_id}">${r.object_name}</span>
            <span class="type-tag">${ENTITY_LABELS[r.object_type]||r.object_type}</span>
          </div>`).join('')}
    </div>

    <div class="rel-section">
      <h4>들어오는 관계 (${data.incoming_relations.length})</h4>
      ${data.incoming_relations.length === 0
        ? '<div style="font-size:12px; color:var(--text-muted)">없음</div>'
        : data.incoming_relations.map(r => `<div class="rel-row">
            <span class="target" data-type="${r.subject_type}" data-id="${r.subject_id}">${r.subject_name}</span>
            <span class="pred">${PREDICATE_LABELS[r.predicate]||r.predicate}</span>
            <b>이 ${ENTITY_LABELS[type]||type}</b>
            <span class="type-tag">${ENTITY_LABELS[r.subject_type]||r.subject_type}</span>
          </div>`).join('')}
    </div>

    <div class="rel-section">
      <h4>최근 이벤트 (${data.recent_events.length})</h4>
      <div class="events-mini">
        ${data.recent_events.length === 0
          ? '<div style="font-size:12px; color:var(--text-muted)">없음</div>'
          : data.recent_events.map(eventCardHtml).join('')}
      </div>
    </div>
  `;

  // 관계 끝의 엔티티 클릭하면 그쪽으로 이동
  $$('.rel-row .target').forEach(t => t.onclick = () => {
    const ttype = t.dataset.type, tid = +t.dataset.id;
    // 탭이 다르면 탭 전환부터
    if (ttype !== graphState.tab) {
      graphState.tab = ttype;
      graphState.selectedType = ttype;
      graphState.selectedId = tid;
      renderGraph();
    } else {
      graphState.selectedType = ttype;
      graphState.selectedId = tid;
      loadEntityDetail(ttype, tid);
    }
  });
}

// ============================================================
// 타임라인 (Phase 1 — 디지털 트윈 코어)
// ============================================================
const EVENT_DESC = {
  ClockIn:           { cat:'t-clock',  label:'GPS 출근',  fmt: e => `<b>${e.actors.worker_name||''}</b> 님 <b>${e.place.site_name||''}</b> 출근 — ${e.payload.verified?'✅':'⚠️'} 거리 ${e.payload.distance_m??'?'}m` },
  ClockOut:          { cat:'t-clock',  label:'GPS 퇴근',  fmt: e => `<b>${e.actors.worker_name||''}</b> 님 <b>${e.place.site_name||''}</b> 퇴근 — 거리 ${e.payload.distance_m??'?'}m` },
  Deploy:            { cat:'t-deploy', label:'배치',      fmt: e => `worker #${e.actors.worker_id} → site #${e.place.site_id} (${e.payload.kind}, ${e.payload.date})` },
  DeploymentRemoved: { cat:'t-deploy', label:'배치 해제',  fmt: e => `worker #${e.actors.worker_id} 배치 해제 (${e.payload.kind}, ${e.payload.date})` },
  DeploymentsCopied: { cat:'t-deploy', label:'배치 복사',  fmt: e => `${e.payload.date}: ${e.payload.src_kind} → ${e.payload.dst_kind} ${e.payload.count}건` },
  SiteCreated:       { cat:'t-site',   label:'현장 신규',  fmt: e => `<b>${e.payload.name}</b> 현장 등록 — 계약 ${e.financial.amount?'₩'+e.financial.amount.toLocaleString('ko-KR'):'미정'}` },
  SiteUpdated:       { cat:'t-site',   label:'현장 수정',  fmt: e => `<b>${e.payload.name}</b> — 상태 ${e.payload.status}, 누적수금 ${e.payload.paid_amount?'₩'+e.payload.paid_amount.toLocaleString('ko-KR'):'-'}` },
  SiteDeleted:       { cat:'t-site',   label:'현장 삭제',  fmt: e => `현장 #${e.place.site_id} 삭제` },
  WorkerCreated:     { cat:'t-worker', label:'직원 등록',  fmt: e => `<b>${e.payload.name}</b> (${e.payload.worker_type==='office'?'사무직':'일용직'}, ${e.payload.job_role||'-'}, 일당 ${e.payload.daily_wage?fmt(e.payload.daily_wage)+'원':'-'}) 등록` },
  WorkerUpdated:     { cat:'t-worker', label:'직원 수정',  fmt: e => `<b>${e.payload.name}</b> 정보 수정 — 일당 ${e.payload.daily_wage?fmt(e.payload.daily_wage)+'원':'-'}` },
  WorkerDeleted:     { cat:'t-worker', label:'직원 삭제',  fmt: e => `worker #${e.actors.worker_id} 삭제` },
  WorkerSelfRegistered:{cat:'t-worker',label:'자가 가입',  fmt: e => `<b>${e.payload.name}</b> (${e.payload.phone}) 폰에서 자가 가입` },
  CompanyCreated:    { cat:'t-other',  label:'법인 등록',  fmt: e => `<b>${e.payload.name}</b> 법인 등록` },
  AdminSignedUp:     { cat:'t-other',  label:'관리자 가입', fmt: e => `<b>${e.payload.name}</b> (${e.payload.username}, ${e.payload.role}) 관리자 가입` },
  ProcessStarted:    { cat:'t-other',  label:'프로세스 시작', fmt: e => `${(window.__procDefs||{})[e.payload.workflow]?.name || e.payload.workflow} — ${e.payload.subject_type} #${e.payload.subject_id} → "${e.payload.state}"` },
  ProcessAdvanced:   { cat:'t-other',  label:'프로세스 진행', fmt: e => `${(window.__procDefs||{})[e.payload.workflow]?.name || e.payload.workflow} — ${e.payload.subject_type} #${e.payload.subject_id}: ${e.payload.from} → <b>${e.payload.to}</b>${e.payload.manual?' (수동)':''}` },
};

let tlState = { type: '', source: '', from: '', to: '' };

route('/timeline', async () => {
  await loadTimeline();
});

async function loadTimeline() {
  const params = new URLSearchParams();
  if (tlState.type) params.set('type', tlState.type);
  if (tlState.source) params.set('source', tlState.source);
  if (tlState.from) params.set('from_date', tlState.from);
  if (tlState.to) params.set('to_date', tlState.to);
  params.set('limit', '300');

  const [events, types] = await Promise.all([
    api.get('/api/events?' + params.toString()),
    api.get('/api/events/types'),
  ]);

  const totalAll = types.reduce((s, t) => s + t.cnt, 0);

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">이벤트 타임라인</div>
        <div class="page-sub">디지털 트윈 코어 — 모든 도메인 행위가 시간순 단일 로그로 기록됩니다 (총 ${fmt(totalAll)}건)</div>
      </div>
    </div>

    <div class="tl-filters">
      <span style="font-size:12px; color:var(--text-muted)">기간</span>
      <input type="date" id="tl-from" value="${tlState.from}">
      <span style="color:var(--text-muted)">~</span>
      <input type="date" id="tl-to" value="${tlState.to}">
      <select id="tl-source">
        <option value="">전체 소스</option>
        <option value="admin_ui" ${tlState.source==='admin_ui'?'selected':''}>관리자</option>
        <option value="mobile"   ${tlState.source==='mobile'?'selected':''}>폰(모바일)</option>
        <option value="public"   ${tlState.source==='public'?'selected':''}>공개(자가가입)</option>
      </select>
      <button class="btn btn-sm" id="tl-reset">필터 초기화</button>
    </div>

    <div class="tl-stats">
      <span class="tl-pill ${!tlState.type?'active':''}" data-t="">전체<span class="cnt">${fmt(totalAll)}</span></span>
      ${types.map(t => `<span class="tl-pill ${tlState.type===t.type?'active':''}" data-t="${t.type}">${(EVENT_DESC[t.type]||{label:t.type}).label}<span class="cnt">${t.cnt}</span></span>`).join('')}
    </div>

    <div id="tl-list">
      ${events.length === 0 ? '<div class="card card-pad" style="text-align:center; color:var(--text-muted)">조회된 이벤트가 없습니다.</div>' :
        events.map(eventCardHtml).join('')}
    </div>
  `;

  $('#tl-from').onchange = e => { tlState.from = e.target.value; loadTimeline(); };
  $('#tl-to').onchange   = e => { tlState.to   = e.target.value; loadTimeline(); };
  $('#tl-source').onchange = e => { tlState.source = e.target.value; loadTimeline(); };
  $('#tl-reset').onclick = () => { tlState = { type:'', source:'', from:'', to:'' }; loadTimeline(); };
  $$('.tl-pill').forEach(p => p.onclick = () => {
    tlState.type = p.dataset.t; loadTimeline();
  });
}

function eventCardHtml(e) {
  const def = EVENT_DESC[e.type] || { cat:'t-other', label: e.type, fmt: () => `<i style="color:var(--text-muted)">${e.type}</i>` };
  let desc = '';
  try { desc = def.fmt(e); } catch (err) { desc = '<i>오류: ' + err + '</i>'; }
  const dt = (e.occurred_at || '').replace('T', ' ').slice(0, 19);
  const [date, time] = dt.split(' ');
  const sourceLabel = { admin_ui:'관리자', mobile:'📱폰', public:'🌐공개', system:'⚙️시스템' }[e.source] || e.source;
  const fin = (e.financial && e.financial.amount) ?
    `<div style="font-size:11px; color:#a35907; margin-top:3px">💰 ${fmtMoney(e.financial.amount)} — ${e.financial.account||''}</div>` : '';
  return `<div class="event-card">
    <div class="event-time">
      <div class="date">${date||''}</div>
      <div>${time||''}</div>
    </div>
    <div class="event-body">
      <span class="type ${def.cat}">${def.label}</span>
      <span class="desc">${desc}</span>
      ${fin}
    </div>
    <span class="event-source">${sourceLabel}</span>
  </div>`;
}

// ============================================================
// 사용자 관리 (admin 전용)
// ============================================================
route('/users', async () => {
  let users;
  try { users = await api.get('/api/users'); }
  catch (e) {
    $('#app').innerHTML = '<div class="card card-pad">관리자 권한이 필요합니다.</div>';
    return;
  }
  if (!Array.isArray(users)) {
    $('#app').innerHTML = '<div class="card card-pad">' + (users.detail || '권한 없음') + '</div>';
    return;
  }

  $('#app').innerHTML = `
    <div class="page-header">
      <div>
        <div class="page-title">사용자 관리</div>
        <div class="page-sub">본사·현장 관리 계정. 관리자만 접근 가능</div>
      </div>
    </div>
    <div class="card">
      <table class="table">
        <thead><tr>
          <th>이름</th><th>아이디</th><th>역할</th><th>가입일</th><th></th>
        </tr></thead>
        <tbody>
          ${users.map(u => `<tr>
            <td><b>${u.name || '-'}</b></td>
            <td>${u.username}</td>
            <td>
              <select data-role="${u.id}" style="padding:4px 8px; border:1px solid var(--border); border-radius:4px; font-size:12px;">
                <option value="admin" ${u.role==='admin'?'selected':''}>관리자</option>
                <option value="manager" ${u.role==='manager'?'selected':''}>매니저</option>
              </select>
            </td>
            <td style="font-size:11px; color:var(--text-muted)">${(u.created_at||'').split('T')[0]}</td>
            <td>
              <button class="btn btn-sm" data-pw="${u.id}" data-name="${u.name||u.username}">비번 재설정</button>
              <button class="btn btn-sm btn-danger" data-del="${u.id}" data-name="${u.name||u.username}">삭제</button>
            </td>
          </tr>`).join('')}
        </tbody>
      </table>
    </div>
    <div class="diff-banner" style="margin-top:8px">
      💡 새 관리자 추가: 가입 페이지(<a href="/signup" target="_blank">/signup</a>) 링크를 본사 직원에게 공유하세요.
      초대 코드 보호가 필요하면 Render 환경변수 <code>ADMIN_INVITE_CODE</code> 를 설정하세요.
    </div>
  `;

  $$('[data-role]').forEach(sel => sel.onchange = async e => {
    await api.put('/api/users/' + sel.dataset.role, { role: e.target.value });
  });
  $$('[data-pw]').forEach(b => b.onclick = async () => {
    const newpw = prompt(b.dataset.name + ' 의 새 비밀번호 (6자 이상):');
    if (!newpw) return;
    if (newpw.length < 6) { alert('6자 이상이어야 합니다'); return; }
    await api.put('/api/users/' + b.dataset.pw, { new_password: newpw });
    alert('재설정 완료');
  });
  $$('[data-del]').forEach(b => b.onclick = async () => {
    if (!confirm(b.dataset.name + ' 계정을 삭제할까요?')) return;
    await api.del('/api/users/' + b.dataset.del);
    navigate();
  });
});

// ============================================================
// 시작 — 로그인 체크 후 진입
// ============================================================
(async () => {
  try {
    const me = await fetch('/api/me').then(r => r.json());
    if (!me.authenticated) {
      location.href = '/login';
      return;
    }
    const right = document.querySelector('.topnav .right');
    if (right) {
      const roleLabel = me.role === 'admin' ? '관리자' : '매니저';
      right.innerHTML = `
        <span style="margin-right:10px">${me.name || me.username} <span style="opacity:0.7">(${roleLabel})</span></span>
        <a href="#" onclick="logout(); return false;" style="color:rgba(255,255,255,0.85); text-decoration: underline; font-size: 12px;">로그아웃</a>
      `;
    }
    if (me.role === 'admin') {
      const navUsers = document.getElementById('nav-users');
      if (navUsers) navUsers.style.display = '';
    }
  } catch (e) {
    location.href = '/login';
    return;
  }
  navigate();
  refreshNotifBadge();
  setInterval(refreshNotifBadge, 60000); // 1분마다 알림 카운트 갱신
})();
