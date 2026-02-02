/**
 * Bourbon Release Calendar — Frontend Application
 */

// ─── State ───────────────────────────────────────────────────
const state = {
  releases: [],
  filtered: [],
  view: 'calendar',
  filters: { search: '', month: '', type: '', distillery: '' },
};

// ─── DOM Refs ────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);
const $content = $('content');
const $loading = $('loading');
const $search = $('search');
const $filterMonth = $('filter-month');
const $filterType = $('filter-type');
const $filterDistillery = $('filter-distillery');
const $viewCalendar = $('view-calendar');
const $viewList = $('view-list');
const $modal = $('modal');
const $modalBody = $('modal-body');
const $statsBar = $('stats-bar');

// ─── API ─────────────────────────────────────────────────────
async function fetchJSON(url) {
  const r = await fetch(url);
  return r.json();
}

// ─── Init ────────────────────────────────────────────────────
async function init() {
  try {
    const [relData, stats, distilleries] = await Promise.all([
      fetchJSON('/api/releases'),
      fetchJSON('/api/stats'),
      fetchJSON('/api/distilleries'),
    ]);

    state.releases = relData.releases || [];
    state.filtered = state.releases;

    renderStats(stats);
    populateDistilleries(distilleries);
    render();
    $loading.classList.add('hidden');
  } catch (e) {
    console.error('Load failed:', e);
    $loading.innerHTML = `
      <div class="empty-state">
        <div class="empty-icon">⚠️</div>
        <p>Failed to load releases. Run the seed or scraper first:<br>
        <code style="background:#f0e8dc;padding:0.2rem 0.5rem;border-radius:4px;margin-top:0.5rem;display:inline-block">python3 seed.py</code></p>
      </div>`;
  }
}

// ─── Stats ───────────────────────────────────────────────────
function renderStats(s) {
  $statsBar.innerHTML = `
    <div class="stat-item"><div class="stat-value">${s.totalReleases||0}</div><div class="stat-label">Releases</div></div>
    <div class="stat-item"><div class="stat-value">${s.totalSources||0}</div><div class="stat-label">Sources</div></div>
    <div class="stat-item"><div class="stat-value">${s.avgProof?s.avgProof.toFixed(1):'—'}</div><div class="stat-label">Avg Proof</div></div>
    <div class="stat-item"><div class="stat-value">${s.avgPrice?'$'+s.avgPrice.toFixed(0):'—'}</div><div class="stat-label">Avg Price</div></div>`;
}

function populateDistilleries(list) {
  for (const d of list) {
    if (!d.distillery) continue;
    const o = document.createElement('option');
    o.value = d.distillery;
    o.textContent = `${d.distillery} (${d.count})`;
    $filterDistillery.appendChild(o);
  }
}

// ─── Filtering ───────────────────────────────────────────────
function applyFilters() {
  let r = state.releases;
  const q = state.filters.search.toLowerCase();
  if (q) r = r.filter(x =>
    (x.product_name||'').toLowerCase().includes(q) ||
    (x.distillery||'').toLowerCase().includes(q) ||
    (x.notes||'').toLowerCase().includes(q) ||
    (x.finish||'').toLowerCase().includes(q));
  if (state.filters.month) r = r.filter(x => x.release_month === state.filters.month);
  if (state.filters.type) r = r.filter(x => x.type === state.filters.type);
  if (state.filters.distillery) r = r.filter(x => x.distillery === state.filters.distillery);
  state.filtered = r;
  render();
}

// ─── Rendering ───────────────────────────────────────────────
function render() {
  if (!state.filtered.length) {
    $content.innerHTML = `<div class="empty-state"><div class="empty-icon">🔍</div><p>No releases found matching your filters.</p></div>`;
    return;
  }
  state.view === 'calendar' ? renderCalendar() : renderList();
}

function renderCalendar() {
  const MONTH_ORDER = [
    'January 2026','February 2026','March 2026','April 2026',
    'May 2026','June 2026','July 2026','August 2026',
    'September 2026','October 2026','November 2026','December 2026',null
  ];
  const grouped = {};
  for (const r of state.filtered) {
    const m = r.release_month || 'TBD';
    (grouped[m] = grouped[m] || []).push(r);
  }
  const months = Object.keys(grouped).sort((a,b) => {
    const ia = MONTH_ORDER.indexOf(a), ib = MONTH_ORDER.indexOf(b);
    return (ia<0?99:ia) - (ib<0?99:ib);
  });

  $content.innerHTML = months.map(m => {
    const rels = grouped[m];
    return `<section class="month-section">
      <h2 class="month-header">${m==='TBD'?'Date TBD':m}
        <span class="month-count">${rels.length} release${rels.length!==1?'s':''}</span></h2>
      <div class="releases-grid">${rels.map(renderCard).join('')}</div>
    </section>`;
  }).join('');
  attachListeners();
}

function renderCard(r) {
  const src = (r.sources||[]).map(s => `<span class="badge badge-source">${fmtSrc(s)}</span>`).join('');
  return `<div class="release-card" data-id="${r.id}">
    <div class="card-badges">
      <span class="badge badge-type">${fmtType(r.type)}</span>
      ${r.is_new?'<span class="badge badge-new">New</span>':''}
      ${r.is_limited?'<span class="badge badge-limited">Limited</span>':''}
      ${src}
    </div>
    ${r.distillery?`<div class="card-distillery">${esc(r.distillery)}</div>`:''}
    <h3>${esc(r.product_name)}</h3>
    <div class="card-details">
      ${r.proof?`<div class="detail-item"><span class="detail-label">Proof</span><span class="detail-value">${r.proof}</span></div>`:''}
      ${r.age_years?`<div class="detail-item"><span class="detail-label">Age</span><span class="detail-value">${r.age_years} yr</span></div>`:''}
      ${r.msrp?`<div class="detail-item"><span class="detail-label">MSRP</span><span class="detail-value price">$${r.msrp}</span></div>`:''}
      ${r.finish?`<div class="detail-item"><span class="detail-label">Finish</span><span class="detail-value">${esc(r.finish)}</span></div>`:''}
    </div>
    ${r.notes?`<div class="card-notes">${esc(r.notes)}</div>`:''}
  </div>`;
}

function renderList() {
  $content.innerHTML = `<div class="list-view">
    <div class="list-header"><div>Name</div><div>Distillery</div><div>Proof</div><div>Age</div><div>MSRP</div><div>Month</div></div>
    ${state.filtered.map(r => `<div class="list-row" data-id="${r.id}">
      <div class="name">${esc(r.product_name)}</div>
      <div class="distillery">${esc(r.distillery||'—')}</div>
      <div class="proof">${r.proof||'—'}</div>
      <div>${r.age_years?r.age_years+' yr':'—'}</div>
      <div class="price">${r.msrp?'$'+r.msrp:'—'}</div>
      <div class="month">${r.release_month?r.release_month.replace(' 2026',''):'TBD'}</div>
    </div>`).join('')}
  </div>`;
  attachListeners();
}

// ─── Modal ───────────────────────────────────────────────────
function showModal(r) {
  const src = r.sources||[];
  $modalBody.innerHTML = `
    <div class="card-badges" style="margin-bottom:1rem">
      <span class="badge badge-type">${fmtType(r.type)}</span>
      ${r.is_new?'<span class="badge badge-new">New</span>':''}
      ${r.is_limited?'<span class="badge badge-limited">Limited</span>':''}
    </div>
    <h2 class="modal-title">${esc(r.product_name)}</h2>
    ${r.distillery?`<div class="modal-distillery">${esc(r.distillery)}</div>`:''}
    <div class="modal-details">
      ${dtl('Proof',r.proof)}${dtl('ABV',r.abv?r.abv+'%':null)}
      ${dtl('Age',r.age_years?r.age_years+' years':null)}
      ${dtl('MSRP',r.msrp?'$'+r.msrp:null)}
      ${dtl('Release',r.release_month||'TBD')}
      ${dtl('Bottle',r.bottle_size_ml?r.bottle_size_ml+'ml':null)}
      ${dtl('Type',fmtType(r.type))}${dtl('Batch',r.batch)}
      ${dtl('Finish',r.finish)}${dtl('Mashbill',r.mashbill)}
    </div>
    ${r.notes?`<div class="modal-notes"><strong>Notes:</strong> ${esc(r.notes)}</div>`:''}
    <div class="modal-sources"><h4>Sources (${src.length})</h4>
      <div class="modal-source-list">${src.map(s=>`<span class="source-tag">${fmtSrc(s)}</span>`).join('')}</div>
    </div>`;
  $modal.classList.remove('hidden');
  document.body.style.overflow = 'hidden';
}

function hideModal() {
  $modal.classList.add('hidden');
  document.body.style.overflow = '';
}

function dtl(label, val) {
  return val ? `<div class="modal-detail"><span class="modal-detail-label">${label}</span><span class="modal-detail-value">${esc(String(val))}</span></div>` : '';
}

// ─── Events ──────────────────────────────────────────────────
function attachListeners() {
  document.querySelectorAll('.release-card,.list-row').forEach(el => {
    el.addEventListener('click', () => {
      const r = state.filtered.find(x => x.id === el.dataset.id);
      if (r) showModal(r);
    });
  });
}

let searchTimer;
$search.addEventListener('input', e => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => { state.filters.search = e.target.value; applyFilters(); }, 250);
});

$filterMonth.addEventListener('change', e => { state.filters.month = e.target.value; applyFilters(); });
$filterType.addEventListener('change', e => { state.filters.type = e.target.value; applyFilters(); });
$filterDistillery.addEventListener('change', e => { state.filters.distillery = e.target.value; applyFilters(); });

$viewCalendar.addEventListener('click', () => {
  state.view = 'calendar'; $viewCalendar.classList.add('active'); $viewList.classList.remove('active'); render();
});
$viewList.addEventListener('click', () => {
  state.view = 'list'; $viewList.classList.add('active'); $viewCalendar.classList.remove('active'); render();
});

$modal.querySelector('.modal-backdrop').addEventListener('click', hideModal);
$modal.querySelector('.modal-close').addEventListener('click', hideModal);
document.addEventListener('keydown', e => { if (e.key === 'Escape') hideModal(); });

// ─── Utilities ───────────────────────────────────────────────
function esc(s) { if (!s) return ''; const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function fmtType(t) {
  return {bourbon:'Bourbon',rye:'Rye',wheat:'Wheat',tennessee:'Tennessee',
    single_malt:'Single Malt',blend:'Blend',scotch:'Scotch',japanese:'Japanese'}[t]||t||'Bourbon';
}

function fmtSrc(n) {
  return {'breaking-bourbon':'BB','bourbon-bossman':'Bossman','soaking-oak':'Soaking Oak',
    'articles/blackwells':"Blackwell's",'articles/alcohol-professor':'AlcProf',
    'articles/frootbat':'Frootbat','articles/seelbachs':'Seelbachs'}[n]||n;
}

// ─── Start ───────────────────────────────────────────────────
init();
