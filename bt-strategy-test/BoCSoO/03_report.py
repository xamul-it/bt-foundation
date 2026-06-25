#!/usr/bin/env python3
"""
03_report.py — Genera report HTML interattivo dalla decomposizione AH/RTH
=========================================================================
Legge: out/decompose_results.json  (prodotto da 02_decompose.py)
Scrive: out/ah_rth_report.html

Il report è self-contained (tutti i dati embedded, Bootstrap+Plotly via CDN)
e permette:
  1. Tabella metriche: period selector + filtro date arbitrario + sort
  2. Dettaglio asset: equity curve AH vs RTH, rolling 12M AH%
  3. Factor analysis: scatter AH% vs Volume, Volatility, Beta, Price
  4. Export CSV per lista asset AH / RTH / Mixed

Usage:
    python 03_report.py
"""

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent

OUT_DIR    = BASE_DIR / "out"
INPUT_JSON = OUT_DIR / "decompose_results.json"
OUT_HTML   = OUT_DIR / "ah_rth_report.html"


def load_data() -> dict:
    with open(INPUT_JSON, encoding="utf-8") as f:
        return json.load(f)


def build_html(data: dict) -> str:
    symbols   = data["symbols"]
    m_dates   = data["dates_monthly"]
    m_data    = data["monthly_data"]
    factors   = data["factor_data"]
    stability = data.get("stability", {})
    ah_thr    = data["ah_threshold"]
    rth_thr   = data["rth_threshold"]

    # Filtra simboli con dati mensili presenti
    symbols = [s for s in symbols if s in m_data]

    embed = {
        "symbols":   symbols,
        "dates":     m_dates,
        "ah_thr":    ah_thr,
        "rth_thr":   rth_thr,
        "ah":        {s: m_data[s]["ah"]    for s in symbols},
        "rth":       {s: m_data[s]["rth"]   for s in symbols},
        "total":     {s: m_data[s]["total"] for s in symbols},
        "factors":   {s: factors[s]    for s in symbols if s in factors},
        "stability": {s: stability[s]  for s in symbols if s in stability},
    }
    data_json = json.dumps(embed, separators=(",", ":"), allow_nan=False,
                           default=lambda _: None)

    return HTML_TEMPLATE.replace("/*DATA_PLACEHOLDER*/", data_json)


# =============================================================================
# HTML Template
# =============================================================================
# CDN: Bootstrap 5.3, Plotly 2.x
# Sicurezza innerHTML: tutti i valori stringa da file (simboli, classi) vengono
# passati attraverso esc() prima dell'inserimento nel DOM.
# I valori numerici (metriche, percentuali) sono formattati con toFixed() e
# non possono contenere HTML, quindi non richiedono escape.
# =============================================================================

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>AH/RTH Study — Decomposizione Rendimenti</title>
<link rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
<script src="https://cdn.jsdelivr.net/npm/plotly.js-dist@2.32.0/plotly.min.js"></script>
<style>
:root{--ah:#3b82f6;--rth:#10b981;--mix:#f59e0b;}
body{font-size:.9rem;background:#f8f9fa;}
.badge-ah  {background:var(--ah) !important;color:#fff;}
.badge-rth {background:var(--rth)!important;color:#fff;}
.badge-mix {background:var(--mix)!important;color:#000;}
#tbl th{cursor:pointer;white-space:nowrap;user-select:none;
        background:#212529;color:#fff;}
#tbl th:hover{background:#374151;}
#tbl td{white-space:nowrap;}
.sort-icon::after{content:" ↕";}
.sort-asc::after {content:" ↑";}
.sort-desc::after{content:" ↓";}
.section-card{background:#fff;border-radius:.5rem;padding:1.25rem;
              box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:1.5rem;}
.period-btn{margin:.2rem;}
.pos{color:#059669;} .neg{color:#dc2626;}
#chart-equity,#chart-rolling{height:380px;}
#chart-factors{height:420px;}
</style>
</head>
<body>

<nav class="navbar navbar-dark bg-dark px-3 py-2 mb-3">
  <span class="navbar-brand fw-bold">AH / RTH Study</span>
  <span class="text-secondary small">Decomposizione rendimenti giornalieri Yahoo</span>
</nav>

<div class="container-fluid px-3">

<!-- CONTROLLI -->
<div class="section-card">
  <div class="row g-2 align-items-end">
    <div class="col-auto">
      <label class="form-label fw-semibold mb-1">Periodo predefinito</label>
      <div id="period-btns">
        <button class="btn btn-sm btn-primary period-btn"        data-period="all"   onclick="setPeriod('all')">All</button>
        <button class="btn btn-sm btn-outline-primary period-btn" data-period="2010+" onclick="setPeriod('2010+')">2010+</button>
        <button class="btn btn-sm btn-outline-primary period-btn" data-period="2015+" onclick="setPeriod('2015+')">2015+</button>
        <button class="btn btn-sm btn-outline-primary period-btn" data-period="2020+" onclick="setPeriod('2020+')">2020+</button>
        <button class="btn btn-sm btn-outline-primary period-btn" data-period="2023+" onclick="setPeriod('2023+')">2023+</button>
        <button class="btn btn-sm btn-outline-secondary period-btn" data-period="custom" onclick="setPeriod('custom')">Custom</button>
      </div>
    </div>
    <div class="col-auto" id="custom-range" style="display:none">
      <label class="form-label fw-semibold mb-1">Range personalizzato</label>
      <div class="d-flex gap-2 align-items-center">
        <input type="month" id="inp-from" class="form-control form-control-sm"
               style="width:150px" onchange="applyCustomRange()">
        <span>&#8594;</span>
        <input type="month" id="inp-to"   class="form-control form-control-sm"
               style="width:150px" onchange="applyCustomRange()">
      </div>
    </div>
    <div class="col-auto ms-auto">
      <label class="form-label fw-semibold mb-1">Classificazione</label>
      <select id="filter-class" class="form-select form-select-sm" style="width:140px"
              onchange="state.filterClass=this.value;renderTable()">
        <option value="">Tutti</option>
        <option value="AH">AH dominant</option>
        <option value="RTH">RTH dominant</option>
        <option value="Mixed">Mixed</option>
      </select>
    </div>
    <div class="col-auto">
      <label class="form-label fw-semibold mb-1">Cerca symbol</label>
      <input id="search-sym" type="text" class="form-control form-control-sm"
             style="width:120px" placeholder="es. AAPL" oninput="renderTable()">
    </div>
  </div>
</div>

<!-- CARDS -->
<div class="row g-3 mb-3">
  <div class="col-md-3">
    <div class="section-card text-center py-2">
      <div id="c-period" class="fw-bold fs-6 text-primary">—</div>
      <div class="text-muted small">Periodo analizzato</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="section-card text-center py-2" style="border-left:4px solid var(--ah)">
      <div id="c-ah-count" class="fw-bold fs-4" style="color:var(--ah)">—</div>
      <div class="text-muted small">AH dominant (&gt;<span id="c-ah-thr">60</span>%)</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="section-card text-center py-2" style="border-left:4px solid var(--rth)">
      <div id="c-rth-count" class="fw-bold fs-4" style="color:var(--rth)">—</div>
      <div class="text-muted small">RTH dominant (&gt;<span id="c-rth-thr">60</span>%)</div>
    </div>
  </div>
  <div class="col-md-3">
    <div class="section-card text-center py-2" style="border-left:4px solid var(--mix)">
      <div id="c-mix-count" class="fw-bold fs-4" style="color:var(--mix)">—</div>
      <div class="text-muted small">Mixed</div>
    </div>
  </div>
</div>

<!-- TABELLA -->
<div class="section-card">
  <h5 class="mb-3">Tabella Metriche — clicca intestazione per ordinare</h5>
  <div class="table-responsive">
    <table class="table table-sm table-hover table-bordered" id="tbl">
      <thead>
        <tr>
          <th class="sort-icon" onclick="sortBy('sym')">Symbol</th>
          <th onclick="sortBy('cls')">Class</th>
          <th class="sort-icon" onclick="sortBy('stab')">Stab.</th>
          <th class="sort-icon" onclick="sortBy('ah_pct')">AH%</th>
          <th class="sort-icon" onclick="sortBy('tot_ret')">Tot Ret%</th>
          <th class="sort-icon" onclick="sortBy('ah_ret')">AH Ret%</th>
          <th class="sort-icon" onclick="sortBy('rth_ret')">RTH Ret%</th>
          <th class="sort-icon" onclick="sortBy('ah_sh')">AH Sharpe</th>
          <th class="sort-icon" onclick="sortBy('rth_sh')">RTH Sharpe</th>
          <th class="sort-icon" onclick="sortBy('ah_so')">AH Sortino</th>
          <th class="sort-icon" onclick="sortBy('rth_so')">RTH Sortino</th>
          <th class="sort-icon" onclick="sortBy('ah_dd')">AH MaxDD%</th>
          <th class="sort-icon" onclick="sortBy('rth_dd')">RTH MaxDD%</th>
          <th class="sort-icon" onclick="sortBy('ah_ddm')">AH DD Mo</th>
          <th class="sort-icon" onclick="sortBy('rth_ddm')">RTH DD Mo</th>
          <th></th>
        </tr>
      </thead>
      <tbody id="tbl-body"></tbody>
    </table>
  </div>
  <div id="tbl-info" class="text-muted small mt-1"></div>
</div>

<!-- DETTAGLIO ASSET -->
<div class="section-card">
  <div class="d-flex align-items-center gap-3 mb-3">
    <h5 class="mb-0">Dettaglio Asset</h5>
    <select id="asset-select" class="form-select form-select-sm" style="width:130px"
            onchange="state.selAsset=this.value;renderAsset()"></select>
  </div>
  <div class="row">
    <div class="col-lg-8">
      <div class="fw-semibold mb-1 small text-muted">Equity curve cumulata (log %)</div>
      <div id="chart-equity"></div>
    </div>
    <div class="col-lg-4">
      <div class="fw-semibold mb-1 small text-muted">AH% rolling 12 mesi</div>
      <div id="chart-rolling"></div>
    </div>
  </div>
  <div class="mt-3">
    <div class="fw-semibold mb-2 small text-muted">Metriche per sub-periodo</div>
    <div id="asset-period-table"></div>
  </div>
</div>

<!-- FACTOR ANALYSIS -->
<div class="section-card">
  <h5 class="mb-1">Factor Analysis</h5>
  <div class="text-muted small mb-3">
    Correlazione tra AH% (periodo completo) e fattori strutturali.
    Colori: <span style="color:var(--ah)">&#9632; AH</span>
            <span style="color:var(--rth)">&#9632; RTH</span>
            <span style="color:var(--mix)">&#9632; Mixed</span>
  </div>
  <div id="chart-factors"></div>
</div>

<!-- EXPORT -->
<div class="section-card">
  <h5 class="mb-3">Export Liste Asset</h5>
  <p class="text-muted small mb-3">
    Classificazione calcolata sul range di date attualmente selezionato.
  </p>
  <div class="d-flex gap-2 flex-wrap">
    <button class="btn btn-sm" style="background:var(--ah);color:#fff"
            onclick="downloadList('AH')">&#8595; AH dominant CSV</button>
    <button class="btn btn-sm" style="background:var(--rth);color:#fff"
            onclick="downloadList('RTH')">&#8595; RTH dominant CSV</button>
    <button class="btn btn-sm" style="background:var(--mix);color:#000"
            onclick="downloadList('Mixed')">&#8595; Mixed CSV</button>
    <button class="btn btn-sm btn-outline-secondary"
            onclick="downloadList('all')">&#8595; Tutti (con classificazione)</button>
  </div>
</div>

</div>

<!-- DATI EMBEDDED -->
<script>
const D = /*DATA_PLACEHOLDER*/;
</script>

<!-- APPLICAZIONE -->
<script>
// ─────────────────────────────────────────────
// Escape HTML — usato prima di ogni innerHTML
// con valori che provengono da file esterni
// ─────────────────────────────────────────────
function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ─────────────────────────────────────────────
// Stato globale
// ─────────────────────────────────────────────
const state = {
  fromDate:    null,
  toDate:      null,
  sortCol:     'ah_pct',
  sortAsc:     false,
  filterClass: '',
  selAsset:    D.symbols[0] || '',
};

const PERIODS_MAP = {
  'all':   [null, null],
  '2010+': ['2010-01', null],
  '2015+': ['2015-01', null],
  '2020+': ['2020-01', null],
  '2023+': ['2023-01', null],
};

// ─────────────────────────────────────────────
// Metriche da serie mensile di log-return
// ─────────────────────────────────────────────
function computeMetrics(arr) {
  if (!arr || arr.length < 3) return null;
  const n = arr.length;
  let sum = 0, sumSq = 0, sumNeg = 0, cntNeg = 0;
  for (const r of arr) {
    sum += r; sumSq += r * r;
    if (r < 0) { sumNeg += r * r; cntNeg++; }
  }
  const mean = sum / n;
  const variance = Math.max((sumSq - n * mean * mean) / Math.max(n - 1, 1), 0);
  const std = Math.sqrt(variance);

  // Annualizzato: monthly → annuale (mean*12, std*sqrt(12))
  const sharpe  = std > 0 ? mean / std * Math.sqrt(12) : 0;
  const downStd = cntNeg > 0 ? Math.sqrt(sumNeg / cntNeg) : 0;
  const sortino = downStd > 0 ? mean / downStd * Math.sqrt(12) : 0;

  let equity = 1, peak = 1, maxDD = 0, ddMonths = 0;
  const eqCurve = [1];
  for (const r of arr) {
    equity *= Math.exp(r);
    eqCurve.push(equity);
    if (equity > peak) peak = equity;
    const dd = (equity - peak) / peak;
    if (dd < maxDD) maxDD = dd;
    if (dd < -0.001) ddMonths++;
  }

  return {
    totalReturn: (equity - 1) * 100,
    sharpe,
    sortino,
    maxDD:    maxDD * 100,
    ddMonths,
    eqCurve,
    n,
  };
}

// ─────────────────────────────────────────────
// Slice per date range
// ─────────────────────────────────────────────
function getSlice(sym, from, to) {
  const dates = D.dates;
  let s = from ? dates.findIndex(d => d >= from) : 0;
  let e = to   ? dates.findIndex(d => d  > to)   : dates.length;
  if (s < 0) s = 0;
  if (e < 0) e = dates.length;
  const hasData = k => D[k] && D[k][sym];
  return {
    ah:    hasData('ah')    ? D.ah[sym].slice(s, e)    : [],
    rth:   hasData('rth')   ? D.rth[sym].slice(s, e)   : [],
    total: hasData('total') ? D.total[sym].slice(s, e) : [],
    dates: dates.slice(s, e),
  };
}

// ─────────────────────────────────────────────
// Classificazione
// ─────────────────────────────────────────────
function classify(ahArr, totArr) {
  const ahSum  = ahArr.reduce((a, b) => a + b, 0);
  const totSum = totArr.reduce((a, b) => a + b, 0);
  if (Math.abs(totSum) < 1e-6) return 'Mixed';
  const ahPct = ahSum / totSum * 100;
  if (ahPct > D.ah_thr * 100)         return 'AH';
  if (100 - ahPct > D.rth_thr * 100)  return 'RTH';
  return 'Mixed';
}

// Restituisce span badge classificazione — usa esc() sul valore da file
function classBadge(cls) {
  const cssCls = {AH: 'badge-ah', RTH: 'badge-rth', Mixed: 'badge-mix'}[cls] || 'badge-mix';
  return '<span class="badge ' + esc(cssCls) + '">' + esc(cls) + '</span>';
}

// Badge stabilità — n_unique è sempre 1/2/3, nessun valore da file esterno
function stabBadge(nUnique) {
  if (nUnique === 1) return '<span class="badge" style="background:#059669">&#10003; 1</span>';
  if (nUnique === 2) return '<span class="badge" style="background:#d97706">~ 2</span>';
  return '<span class="badge" style="background:#dc2626">&#10007; ' + nUnique + '</span>';
}

// Formatta numero con colore (il valore è sempre numerico, no XSS)
function fmtN(v, dec = 2) {
  if (v === null || v === undefined || isNaN(v)) return '&mdash;';
  const s = Number(v).toFixed(dec);
  const cls = parseFloat(s) >= 0 ? 'pos' : 'neg';
  const sign = parseFloat(s) >= 0 ? '+' : '';
  return '<span class="' + cls + '">' + sign + s + '</span>';
}
function fmtPlain(v, dec = 0) {
  if (v === null || v === undefined || isNaN(v)) return '&mdash;';
  return Number(v).toFixed(dec);
}

// ─────────────────────────────────────────────
// Costruzione righe tabella
// ─────────────────────────────────────────────
let cachedRows = [];

function buildRows(from, to) {
  const rows = [];
  for (const sym of D.symbols) {
    const sl = getSlice(sym, from, to);
    if (!sl.total.length) continue;

    const ahSum  = sl.ah.reduce((a, b) => a + b, 0);
    const totSum = sl.total.reduce((a, b) => a + b, 0);
    const ahPct  = Math.abs(totSum) > 1e-6 ? ahSum / totSum * 100 : 50;

    const mAH  = computeMetrics(sl.ah);
    const mRTH = computeMetrics(sl.rth);
    const mTot = computeMetrics(sl.total);
    const cls  = classify(sl.ah, sl.total);
    const stab = D.stability && D.stability[sym] ? D.stability[sym].n_unique : null;

    rows.push({
      sym, cls, ahPct, stab,
      tot_ret:  mTot ? mTot.totalReturn : null,
      ah_ret:   mAH  ? mAH.totalReturn  : null,
      rth_ret:  mRTH ? mRTH.totalReturn : null,
      ah_sh:    mAH  ? mAH.sharpe       : null,
      rth_sh:   mRTH ? mRTH.sharpe      : null,
      ah_so:    mAH  ? mAH.sortino      : null,
      rth_so:   mRTH ? mRTH.sortino     : null,
      ah_dd:    mAH  ? mAH.maxDD        : null,
      rth_dd:   mRTH ? mRTH.maxDD       : null,
      ah_ddm:   mAH  ? mAH.ddMonths     : null,
      rth_ddm:  mRTH ? mRTH.ddMonths    : null,
    });
  }
  return rows;
}

// ─────────────────────────────────────────────
// Render tabella
// ─────────────────────────────────────────────
function renderTable() {
  const from = state.fromDate, to = state.toDate;
  cachedRows = buildRows(from, to);

  const fc  = state.filterClass;
  const sch = document.getElementById('search-sym').value.toUpperCase().trim();
  let rows  = cachedRows.filter(r =>
    (!fc  || r.cls === fc) &&
    (!sch || r.sym.toUpperCase().includes(sch))
  );

  // Ordina
  const col = state.sortCol;
  rows.sort((a, b) => {
    const va = a[col], vb = b[col];
    if (va == null) return 1;
    if (vb == null) return -1;
    if (typeof va === 'string') return state.sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    return state.sortAsc ? va - vb : vb - va;
  });

  // Aggiorna icone header
  document.querySelectorAll('#tbl th').forEach(th => {
    th.classList.remove('sort-asc', 'sort-desc');
    th.classList.add('sort-icon');
  });
  const colIdx = ['sym','cls','stab','ah_pct','tot_ret','ah_ret','rth_ret',
                  'ah_sh','rth_sh','ah_so','rth_so','ah_dd','rth_dd','ah_ddm','rth_ddm'];
  const idx = colIdx.indexOf(col);
  if (idx >= 0) {
    const th = document.querySelectorAll('#tbl th')[idx];
    th.classList.remove('sort-icon');
    th.classList.add(state.sortAsc ? 'sort-asc' : 'sort-desc');
  }

  // Costruisce righe con DOM API + innerHTML solo per celle numeriche (sicure)
  const tbody = document.getElementById('tbl-body');
  tbody.textContent = '';               // svuota in modo sicuro
  for (const r of rows) {
    const tr = document.createElement('tr');

    // Cella symbol — textContent (dati da file)
    const tdSym = document.createElement('td');
    const b = document.createElement('b');
    b.textContent = r.sym;
    tdSym.appendChild(b);
    tr.appendChild(tdSym);

    // Badge classificazione — innerHTML limitato a classi/label nostri
    const tdCls = document.createElement('td');
    tdCls.innerHTML = classBadge(r.cls);
    tr.appendChild(tdCls);

    // Badge stabilità — valore numerico 1/2/3, nessun dato da file
    const tdStab = document.createElement('td');
    tdStab.innerHTML = r.stab !== null ? stabBadge(r.stab) : '&mdash;';
    tr.appendChild(tdStab);

    // Celle numeriche — innerHTML sicuro (valori prodotti da toFixed)
    const numCells = [
      fmtPlain(r.ahPct, 1) + '%',
      fmtN(r.tot_ret,  1) + '%',
      fmtN(r.ah_ret,   1) + '%',
      fmtN(r.rth_ret,  1) + '%',
      fmtN(r.ah_sh),
      fmtN(r.rth_sh),
      fmtN(r.ah_so),
      fmtN(r.rth_so),
      fmtN(r.ah_dd,    1) + '%',
      fmtN(r.rth_dd,   1) + '%',
      fmtPlain(r.ah_ddm),
      fmtPlain(r.rth_ddm),
    ];
    for (const html of numCells) {
      const td = document.createElement('td');
      td.innerHTML = html;
      tr.appendChild(td);
    }

    // Pulsante dettaglio — onclick usa solo il sym escapato
    const tdBtn = document.createElement('td');
    const btn = document.createElement('button');
    btn.className = 'btn btn-sm btn-outline-secondary py-0 px-1';
    btn.style.fontSize = '.75rem';
    btn.textContent = '→';
    btn.addEventListener('click', () => selectAsset(r.sym));
    tdBtn.appendChild(btn);
    tr.appendChild(tdBtn);

    tbody.appendChild(tr);
  }

  document.getElementById('tbl-info').textContent =
    rows.length + ' / ' + D.symbols.length + ' asset mostrati';

  renderCards(rows);
}

function sortBy(col) {
  if (state.sortCol === col) state.sortAsc = !state.sortAsc;
  else { state.sortCol = col; state.sortAsc = false; }
  renderTable();
}

// ─────────────────────────────────────────────
// Cards sommario
// ─────────────────────────────────────────────
function renderCards(rows) {
  const from = state.fromDate, to = state.toDate;
  const label = (from || to)
    ? (from || D.dates[0]) + ' → ' + (to || D.dates[D.dates.length - 1])
    : 'Periodo completo';

  // textContent — nessun HTML injection
  document.getElementById('c-period').textContent  = label;
  document.getElementById('c-ah-thr').textContent  = Math.round(D.ah_thr  * 100);
  document.getElementById('c-rth-thr').textContent = Math.round(D.rth_thr * 100);

  const cnt = { AH: 0, RTH: 0, Mixed: 0 };
  rows.forEach(r => { if (cnt[r.cls] !== undefined) cnt[r.cls]++; });
  document.getElementById('c-ah-count').textContent  = cnt.AH;
  document.getElementById('c-rth-count').textContent = cnt.RTH;
  document.getElementById('c-mix-count').textContent = cnt.Mixed;
}

// ─────────────────────────────────────────────
// Dettaglio asset
// ─────────────────────────────────────────────
function renderAsset() {
  const sym = state.selAsset;
  if (!sym || !D.ah[sym]) return;

  const sel = document.getElementById('asset-select');
  if (sel.value !== sym) sel.value = sym;

  // Equity curve
  const sl   = getSlice(sym, state.fromDate, state.toDate);
  const mAH  = computeMetrics(sl.ah);
  const mRTH = computeMetrics(sl.rth);
  const mTot = computeMetrics(sl.total);

  const toLogPct = arr => arr.map(v => Math.log(v) * 100);
  const xDates = [''].concat(sl.dates);

  Plotly.newPlot('chart-equity', [
    { x: xDates, y: toLogPct(mTot.eqCurve), name: 'Total',
      line: { color: '#6b7280', width: 1.5 }, mode: 'lines' },
    { x: xDates, y: toLogPct(mAH.eqCurve),  name: 'AH (overnight)',
      line: { color: '#3b82f6', width: 2 },   mode: 'lines' },
    { x: xDates, y: toLogPct(mRTH.eqCurve), name: 'RTH (intraday)',
      line: { color: '#10b981', width: 2 },   mode: 'lines' },
  ], {
    margin: { t: 20, b: 40, l: 55, r: 10 },
    legend: { orientation: 'h', y: -0.18 },
    yaxis:  { title: 'Log return cumulato (%)', zeroline: true },
    plot_bgcolor: '#fff', paper_bgcolor: '#fff',
  }, { responsive: true, displayModeBar: false });

  // Rolling 12M AH%
  const allAH  = D.ah[sym];
  const allTot = D.total[sym];
  const W = 12;
  const rollDates = [], rollPct = [];
  for (let i = W; i <= allAH.length; i++) {
    const ahS  = allAH.slice(i - W, i).reduce((a, b) => a + b, 0);
    const totS = allTot.slice(i - W, i).reduce((a, b) => a + b, 0);
    rollDates.push(D.dates[i - 1]);
    rollPct.push(Math.abs(totS) > 1e-6 ? ahS / totS * 100 : 50);
  }
  const x0 = rollDates[0], xN = rollDates[rollDates.length - 1];
  Plotly.newPlot('chart-rolling', [
    { x: rollDates, y: rollPct, mode: 'lines',
      line: { color: '#3b82f6', width: 2 }, name: 'AH% 12M',
      fill: 'tozeroy', fillcolor: 'rgba(59,130,246,0.08)' },
    { x: [x0, xN], y: [50, 50], mode: 'lines',
      line: { color: '#9ca3af', dash: 'dash', width: 1 }, showlegend: false },
    { x: [x0, xN], y: [D.ah_thr*100, D.ah_thr*100], mode: 'lines',
      line: { color: '#3b82f6', dash: 'dot', width: 1 }, showlegend: false },
    { x: [x0, xN], y: [100 - D.rth_thr*100, 100 - D.rth_thr*100], mode: 'lines',
      line: { color: '#10b981', dash: 'dot', width: 1 }, showlegend: false },
  ], {
    margin: { t: 20, b: 40, l: 50, r: 10 },
    yaxis:  { title: 'AH%', zeroline: true, range: [-80, 180] },
    plot_bgcolor: '#fff', paper_bgcolor: '#fff',
  }, { responsive: true, displayModeBar: false });

  // Tabella sub-periodi — costruita con DOM API
  const PERIOD_STARTS = {
    'all': '', '2010+': '2010-01', '2015+': '2015-01',
    '2020+': '2020-01', '2023+': '2023-01',
  };
  const table = document.createElement('table');
  table.className = 'table table-sm table-bordered';
  table.style.fontSize = '.8rem';

  const thead = table.createTHead();
  const hRow  = thead.insertRow();
  for (const h of ['Periodo','AH%','AH Ret%','RTH Ret%','AH Sh','RTH Sh','AH MaxDD%','RTH MaxDD%','Class']) {
    const th = document.createElement('th');
    th.className = 'table-dark';
    th.textContent = h;
    hRow.appendChild(th);
  }

  const tbody2 = table.createTBody();
  for (const [pname, pstart] of Object.entries(PERIOD_STARTS)) {
    const ps = getSlice(sym, pstart || null, null);
    if (!ps.total.length) continue;

    const mA  = computeMetrics(ps.ah);
    const mR  = computeMetrics(ps.rth);
    const ahS  = ps.ah.reduce((a, b) => a + b, 0);
    const totS = ps.total.reduce((a, b) => a + b, 0);
    const ahP  = Math.abs(totS) > 1e-6 ? (ahS / totS * 100).toFixed(1) + '%' : '—';
    const cls  = classify(ps.ah, ps.total);
    const row  = tbody2.insertRow();

    const cells = [
      ['text', pname],
      ['text', ahP],
      ['html', fmtN(mA?.totalReturn, 1) + '%'],
      ['html', fmtN(mR?.totalReturn, 1) + '%'],
      ['html', fmtN(mA?.sharpe)],
      ['html', fmtN(mR?.sharpe)],
      ['html', fmtN(mA?.maxDD, 1) + '%'],
      ['html', fmtN(mR?.maxDD, 1) + '%'],
      ['html', classBadge(cls)],
    ];
    for (const [type, val] of cells) {
      const td = row.insertCell();
      if (type === 'text') td.textContent = val;
      else td.innerHTML = val;         // solo numeri da toFixed e classi nostri
    }
  }

  const container = document.getElementById('asset-period-table');
  container.textContent = '';          // svuota in modo sicuro
  container.appendChild(table);
}

function selectAsset(sym) {
  state.selAsset = sym;
  document.getElementById('asset-select').value = sym;
  renderAsset();
  document.getElementById('asset-select').scrollIntoView({ behavior: 'smooth', block: 'center' });
}

// ─────────────────────────────────────────────
// Factor Analysis
// ─────────────────────────────────────────────
function renderFactors() {
  const FACTORS = ['log_volume', 'volatility', 'beta', 'log_price'];
  const LABELS  = ['Log Volume (proxy liquidità)', 'Volatilità Annua', 'Beta vs SPY', 'Log Prezzo Medio'];
  const COLORS  = { AH: '#3b82f6', RTH: '#10b981', Mixed: '#f59e0b' };

  const subplots = FACTORS.map((f, i) => {
    const syms = [], xVals = [], yVals = [], cols = [];
    for (const sym of D.symbols) {
      const fd = D.factors[sym];
      if (!fd || fd[f] === null || fd[f] === undefined || isNaN(fd[f])) continue;
      syms.push(sym);
      xVals.push(fd[f]);
      yVals.push(fd.ah_pct);
      cols.push(COLORS[fd.classification] || COLORS.Mixed);
    }
    return { syms, xVals, yVals, cols, label: LABELS[i], field: f };
  });

  const traces = subplots.map((sp, i) => ({
    x: sp.xVals, y: sp.yVals, text: sp.syms,
    mode: 'markers', type: 'scatter',
    marker: { color: sp.cols, size: 8, opacity: 0.8, line: { width: 1, color: '#fff' } },
    hovertemplate: '<b>%{text}</b><br>' + sp.label + ': %{x:.2f}<br>AH%: %{y:.1f}%<extra></extra>',
    showlegend: false,
    xaxis: 'x' + (i > 0 ? i + 1 : ''),
    yaxis: 'y' + (i > 0 ? i + 1 : ''),
  }));

  const annotations = subplots.map((sp, i) => ({
    text: sp.label,
    xref: 'x' + (i > 0 ? i + 1 : '') + ' domain',
    yref: 'y' + (i > 0 ? i + 1 : '') + ' domain',
    x: 0.5, y: 1.08, showarrow: false, font: { size: 11, color: '#374151' },
  }));

  const layout = {
    grid: { rows: 2, columns: 2, pattern: 'independent' },
    annotations,
    margin: { t: 50, b: 40, l: 50, r: 20 },
    plot_bgcolor: '#fff', paper_bgcolor: '#f8f9fa',
    height: 420,
  };
  for (let i = 0; i < 4; i++) {
    const ax = i > 0 ? i + 1 : '';
    layout['yaxis' + ax] = { title: i % 2 === 0 ? 'AH%' : '', zeroline: true };
    layout['xaxis' + ax] = {};
  }

  Plotly.newPlot('chart-factors', traces, layout,
    { responsive: true, displayModeBar: false });
}

// ─────────────────────────────────────────────
// Export CSV
// ─────────────────────────────────────────────
function downloadList(filterCls) {
  const from = state.fromDate, to = state.toDate;
  let rows = buildRows(from, to);
  if (filterCls !== 'all') rows = rows.filter(r => r.cls === filterCls);

  const header = 'symbol,classification,ah_pct,tot_return_%,ah_return_%,rth_return_%,' +
                 'ah_sharpe,rth_sharpe,ah_sortino,rth_sortino,ah_max_dd_%,rth_max_dd_%,' +
                 'ah_dd_months,rth_dd_months';
  const body = rows.map(r => [
    r.sym, r.cls, (r.ahPct||0).toFixed(1), (r.tot_ret||0).toFixed(2),
    (r.ah_ret||0).toFixed(2), (r.rth_ret||0).toFixed(2),
    (r.ah_sh||0).toFixed(3),  (r.rth_sh||0).toFixed(3),
    (r.ah_so||0).toFixed(3),  (r.rth_so||0).toFixed(3),
    (r.ah_dd||0).toFixed(2),  (r.rth_dd||0).toFixed(2),
    r.ah_ddm || 0, r.rth_ddm || 0,
  ].join(',')).join('\n');

  const blob = new Blob([header + '\n' + body], { type: 'text/csv' });
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  const lbl  = filterCls === 'all' ? 'all' : filterCls.toLowerCase();
  a.href     = url;
  a.download = 'ah_rth_' + lbl + '_' + (from || 'all') + '.csv';
  a.click();
  URL.revokeObjectURL(url);
}

// ─────────────────────────────────────────────
// Period / range controls
// ─────────────────────────────────────────────
function setPeriod(period) {
  document.querySelectorAll('.period-btn').forEach(b => {
    const isSel = b.dataset.period === period;
    b.classList.toggle('btn-primary',           isSel && period !== 'custom');
    b.classList.toggle('btn-outline-primary',   !isSel && b.dataset.period !== 'custom');
    b.classList.toggle('btn-outline-secondary', b.dataset.period === 'custom' && !isSel);
    b.classList.toggle('btn-secondary',         b.dataset.period === 'custom' && isSel);
  });

  document.getElementById('custom-range').style.display =
    period === 'custom' ? '' : 'none';

  if (period === 'custom') return;

  const [from, to] = PERIODS_MAP[period] || [null, null];
  state.fromDate = from;
  state.toDate   = to;
  renderTable();
  renderAsset();
}

function applyCustomRange() {
  state.fromDate = document.getElementById('inp-from').value || null;
  state.toDate   = document.getElementById('inp-to').value   || null;
  renderTable();
  renderAsset();
}

// ─────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  // Popola asset select con textContent (sicuro)
  const sel = document.getElementById('asset-select');
  for (const sym of D.symbols) {
    const opt = document.createElement('option');
    opt.value       = sym;
    opt.textContent = sym;
    sel.appendChild(opt);
  }
  if (state.selAsset) sel.value = state.selAsset;

  if (D.dates.length) {
    document.getElementById('inp-from').value = D.dates[0];
    document.getElementById('inp-to').value   = D.dates[D.dates.length - 1];
  }

  renderTable();
  renderAsset();
  renderFactors();
});
</script>
</body>
</html>
"""

# =============================================================================
# Entry point
# =============================================================================

def main():
    if not INPUT_JSON.exists():
        print(f"Errore: file non trovato: {INPUT_JSON}")
        print("  Eseguire prima: python 02_decompose.py")
        return

    data = load_data()
    html = build_html(data)
    OUT_HTML.write_text(html, encoding="utf-8")
    mb = OUT_HTML.stat().st_size / 1e6
    print(f"Report salvato in {OUT_HTML}  ({mb:.1f} MB)")
    print(f"Apri con: xdg-open {OUT_HTML}")


if __name__ == "__main__":
    main()
