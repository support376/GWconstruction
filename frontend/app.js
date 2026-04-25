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
// 시작 — 로그인 체크 후 진입
// ============================================================
(async () => {
  try {
    const me = await fetch('/api/me').then(r => r.json());
    if (!me.authenticated) {
      location.href = '/login';
      return;
    }
    // 우상단에 사용자명/로그아웃 표시
    const right = document.querySelector('.topnav .right');
    if (right) {
      right.innerHTML = `
        <span style="margin-right:10px">${me.name || me.username} (${me.role})</span>
        <a href="#" onclick="logout(); return false;" style="color:rgba(255,255,255,0.85); text-decoration: underline; font-size: 12px;">로그아웃</a>
      `;
    }
  } catch (e) {
    location.href = '/login';
    return;
  }
  navigate();
})();
