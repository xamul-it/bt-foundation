#!/usr/bin/env python3
"""
04_annual_matrix.py — Matrice annuale AH/RTH per simbolo
========================================================
Legge i CSV Yahoo giornalieri e genera:
    out/annual_matrix_report.html

Righe: anni.
Colonne: simboli.
Celle: ritorno percentuale annuo e Sharpe annuo.

La vista HTML permette di scegliere quale componente mostrare:
    - AH+RTH: rendimento totale, equivalente a buy & hold sul periodo
    - AH: solo overnight / after-hours
    - RTH: solo regular trading hours
"""

from __future__ import annotations

import json
import math
import warnings
from pathlib import Path

import numpy as np
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent.parent
DATA_DIR = REPO_ROOT / "config-common" / "data" / "d" / "yahoo"
OUT_DIR = BASE_DIR / "out"
INPUT_JSON = OUT_DIR / "decompose_results.json"
OUT_HTML = OUT_DIR / "annual_matrix_report.html"


def load_asset(symbol: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{symbol}.csv"
    if not path.exists():
        return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_csv(path, parse_dates=["Date"])
    df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.normalize().dt.tz_localize(None)
    df = df.set_index("Date").sort_index()
    df = df.rename(columns={"Adj Close": "AdjClose"})
    required = {"Open", "Close", "AdjClose", "Volume"}
    if not required.issubset(df.columns):
        return None
    df = df[list(required)].dropna()
    df = df[
        (df["Volume"] > 0)
        & (df["Open"] > 0)
        & (df["Close"] > 0)
        & (df["AdjClose"] > 0)
    ]
    return df if len(df) >= 20 else None


def compute_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df[["Open", "Close", "AdjClose"]].copy()
    out["r_total"] = np.log(df["AdjClose"] / df["AdjClose"].shift(1))
    out["r_rth"] = np.log(df["Close"] / df["Open"])
    out["r_ah"] = out["r_total"] - out["r_rth"]
    return out.dropna(subset=["r_total", "r_rth", "r_ah"])


def annual_metrics(r: pd.Series) -> dict | None:
    r = r.dropna()
    n = int(len(r))
    if n < 20:
        return None
    total_return = float(np.expm1(r.sum()) * 100.0)
    std = float(r.std(ddof=1))
    sharpe = float(r.mean() / std * math.sqrt(252.0)) if std > 0 else 0.0
    return {
        "return": round(total_return, 2),
        "sharpe": round(sharpe, 2),
        "n": n,
    }


def load_reference_metadata(symbols: list[str]) -> tuple[dict, dict]:
    if not INPUT_JSON.exists():
        return {}, {}
    with open(INPUT_JSON, encoding="utf-8") as f:
        data = json.load(f)
    classifications = data.get("classifications", {})
    stability = data.get("stability", {})
    meta = {}
    for sym in symbols:
        meta[sym] = {
            "classification": classifications.get(sym, "Mixed"),
            "stable": bool(stability.get(sym, {}).get("stable", False)),
        }
    return meta, data.get("counts", {})


def build_payload() -> dict:
    symbols = sorted(p.stem for p in DATA_DIR.glob("*.csv"))
    meta, _ = load_reference_metadata(symbols)
    result = {"AH+RTH": {}, "AH": {}, "RTH": {}}
    years: set[int] = set()

    for symbol in symbols:
        df = load_asset(symbol)
        if df is None:
            continue
        returns = compute_returns(df)
        if returns.empty:
            continue

        symbol_rows = {"AH+RTH": {}, "AH": {}, "RTH": {}}
        for year, ydf in returns.groupby(returns.index.year):
            metrics_total = annual_metrics(ydf["r_total"])
            metrics_ah = annual_metrics(ydf["r_ah"])
            metrics_rth = annual_metrics(ydf["r_rth"])
            if metrics_total is None and metrics_ah is None and metrics_rth is None:
                continue
            years.add(int(year))
            if metrics_total is not None:
                symbol_rows["AH+RTH"][str(year)] = metrics_total
            if metrics_ah is not None:
                symbol_rows["AH"][str(year)] = metrics_ah
            if metrics_rth is not None:
                symbol_rows["RTH"][str(year)] = metrics_rth

        for mode in result:
            if symbol_rows[mode]:
                result[mode][symbol] = symbol_rows[mode]

    symbols = [s for s in symbols if any(s in result[mode] for mode in result)]
    meta, _ = load_reference_metadata(symbols)
    return {
        "symbols": symbols,
        "years": sorted(years),
        "metrics": result,
        "meta": meta,
    }


def build_html(payload: dict) -> str:
    data_json = json.dumps(payload, separators=(",", ":"), allow_nan=False)
    return HTML_TEMPLATE.replace("/*DATA_PLACEHOLDER*/", data_json)


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>BoCSoO — Matrice Annuale AH/RTH</title>
<link rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
<style>
:root{--ah:#3b82f6;--rth:#10b981;--mix:#f59e0b;--dark:#212529;}
body{font-size:.88rem;background:#f8f9fa;}
.section-card{background:#fff;border-radius:.5rem;padding:1rem;
  box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:1rem;}
.period-btn{margin:.15rem;}
.matrix-wrap{max-height:78vh;overflow:auto;border:1px solid #dee2e6;border-radius:.35rem;}
#matrix{border-collapse:separate;border-spacing:0;}
#matrix th{position:sticky;top:0;background:var(--dark);color:#fff;z-index:2;
  white-space:nowrap;cursor:pointer;user-select:none;}
#matrix th.year-head{left:0;z-index:3;}
#matrix td.year-cell{position:sticky;left:0;background:#f1f3f5;font-weight:700;z-index:1;}
#matrix td,#matrix th{vertical-align:middle;text-align:center;white-space:nowrap;}
.cell-main{font-weight:700;line-height:1.05;}
.cell-sub{font-size:.74rem;color:#6c757d;line-height:1.05;}
.pos{color:#059669;} .neg{color:#dc2626;} .muted{color:#adb5bd;}
.badge-ah{background:var(--ah)!important;color:#fff;}
.badge-rth{background:var(--rth)!important;color:#fff;}
.badge-mix{background:var(--mix)!important;color:#000;}
.symbol-meta{font-size:.7rem;color:#ced4da;font-weight:400;}
.legend-chip{display:inline-block;width:.8rem;height:.8rem;border-radius:.2rem;margin-right:.25rem;}
</style>
</head>
<body>
<nav class="navbar navbar-dark bg-dark px-3 py-2 mb-3">
  <span class="navbar-brand fw-bold">BoCSoO — Matrice Annuale AH/RTH</span>
  <span class="text-secondary small">Righe anni, colonne simboli, celle ritorno % + Sharpe</span>
</nav>

<div class="container-fluid px-3">
  <div class="section-card">
    <div class="row g-2 align-items-end">
      <div class="col-auto">
        <label class="form-label fw-semibold mb-1">Componente</label>
        <select id="mode" class="form-select form-select-sm" style="width:140px" onchange="state.mode=this.value;render()">
          <option value="AH+RTH">AH+RTH</option>
          <option value="AH">AH</option>
          <option value="RTH">RTH</option>
        </select>
      </div>
      <div class="col-auto">
        <label class="form-label fw-semibold mb-1">Periodo</label>
        <div id="period-btns">
          <button class="btn btn-sm btn-primary period-btn" data-period="all" onclick="setPeriod('all')">All</button>
          <button class="btn btn-sm btn-outline-primary period-btn" data-period="2010+" onclick="setPeriod('2010+')">2010+</button>
          <button class="btn btn-sm btn-outline-primary period-btn" data-period="2015+" onclick="setPeriod('2015+')">2015+</button>
          <button class="btn btn-sm btn-outline-primary period-btn" data-period="2020+" onclick="setPeriod('2020+')">2020+</button>
          <button class="btn btn-sm btn-outline-primary period-btn" data-period="2023+" onclick="setPeriod('2023+')">2023+</button>
          <button class="btn btn-sm btn-outline-secondary period-btn" data-period="custom" onclick="setPeriod('custom')">Custom</button>
        </div>
      </div>
      <div class="col-auto" id="custom-range" style="display:none">
        <label class="form-label fw-semibold mb-1">Range anni</label>
        <div class="d-flex gap-2 align-items-center">
          <input type="number" id="year-from" class="form-control form-control-sm" style="width:95px" onchange="applyCustomRange()">
          <span>&#8594;</span>
          <input type="number" id="year-to" class="form-control form-control-sm" style="width:95px" onchange="applyCustomRange()">
        </div>
      </div>
      <div class="col-auto ms-auto">
        <label class="form-label fw-semibold mb-1">Classificazione</label>
        <select id="filter-class" class="form-select form-select-sm" style="width:140px" onchange="state.filterClass=this.value;render()">
          <option value="">Tutti</option>
          <option value="AH">AH dominant</option>
          <option value="RTH">RTH dominant</option>
          <option value="Mixed">Mixed</option>
        </select>
      </div>
      <div class="col-auto">
        <label class="form-label fw-semibold mb-1">Stabilità</label>
        <select id="filter-stable" class="form-select form-select-sm" style="width:120px" onchange="state.filterStable=this.value;render()">
          <option value="">Tutti</option>
          <option value="stable">Solo stabili</option>
          <option value="unstable">Solo instabili</option>
        </select>
      </div>
      <div class="col-auto">
        <label class="form-label fw-semibold mb-1">Cerca symbol</label>
        <input id="search-sym" type="text" class="form-control form-control-sm" style="width:130px" placeholder="es. AAPL" oninput="render()">
      </div>
    </div>
  </div>

  <div class="row g-3 mb-3">
    <div class="col-md-3"><div class="section-card py-2 text-center">
      <div id="c-period" class="fw-bold fs-6 text-primary">—</div><div class="text-muted small">Periodo</div>
    </div></div>
    <div class="col-md-3"><div class="section-card py-2 text-center">
      <div id="c-mode" class="fw-bold fs-4">—</div><div class="text-muted small">Componente</div>
    </div></div>
    <div class="col-md-3"><div class="section-card py-2 text-center">
      <div id="c-symbols" class="fw-bold fs-4">—</div><div class="text-muted small">Simboli visibili</div>
    </div></div>
    <div class="col-md-3"><div class="section-card py-2 text-center">
      <div id="c-years" class="fw-bold fs-4">—</div><div class="text-muted small">Anni visibili</div>
    </div></div>
  </div>

  <div class="section-card">
    <div class="d-flex align-items-center justify-content-between mb-2">
      <h5 class="mb-0">Matrice Annuale — <span id="matrix-mode-label">AH+RTH</span></h5>
      <div class="text-muted small" id="matrix-info"></div>
    </div>
    <div class="matrix-wrap">
      <table class="table table-sm table-bordered table-hover mb-0" id="matrix">
        <thead id="matrix-head"></thead>
        <tbody id="matrix-body"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const D = /*DATA_PLACEHOLDER*/;

const state = {
  mode: 'AH+RTH',
  period: 'all',
  fromYear: null,
  toYear: null,
  filterClass: '',
  filterStable: '',
  sortSymbol: null,
  sortDir: 1,
};

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function clsBadge(cls) {
  const c = cls || 'Mixed';
  const k = c === 'AH' ? 'ah' : (c === 'RTH' ? 'rth' : 'mix');
  return `<span class="badge badge-${k}">${esc(c)}</span>`;
}
function fmtRet(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '-';
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v > 0 ? '+' : '';
  return `<div class="cell-main ${cls}">${sign}${v.toFixed(1)}%</div>`;
}
function fmtSharpe(v) {
  if (v === null || v === undefined || Number.isNaN(v)) return '<div class="cell-sub">S -</div>';
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v > 0 ? '+' : '';
  return `<div class="cell-sub ${cls}">S ${sign}${v.toFixed(2)}</div>`;
}
function visibleYears() {
  let years = D.years.slice();
  if (state.fromYear !== null) years = years.filter(y => y >= state.fromYear);
  if (state.toYear !== null) years = years.filter(y => y <= state.toYear);
  return years;
}
function visibleSymbols(years) {
  const q = document.getElementById('search-sym').value.trim().toUpperCase();
  let syms = D.symbols.filter(sym => {
    const m = D.meta[sym] || {};
    if (q && !sym.includes(q)) return false;
    if (state.filterClass && m.classification !== state.filterClass) return false;
    if (state.filterStable === 'stable' && !m.stable) return false;
    if (state.filterStable === 'unstable' && m.stable) return false;
    return years.some(y => D.metrics[state.mode][sym] && D.metrics[state.mode][sym][String(y)]);
  });
  if (state.sortSymbol) {
    const y = String(state.sortSymbol);
    syms.sort((a, b) => {
      const av = D.metrics[state.mode][a]?.[y]?.return;
      const bv = D.metrics[state.mode][b]?.[y]?.return;
      const an = av === undefined ? -Infinity : av;
      const bn = bv === undefined ? -Infinity : bv;
      return (an - bn) * state.sortDir;
    });
  }
  return syms;
}
function setPeriod(period) {
  state.period = period;
  document.querySelectorAll('.period-btn').forEach(btn => {
    const active = btn.dataset.period === period;
    btn.className = 'btn btn-sm period-btn ' + (active ? 'btn-primary' : 'btn-outline-primary');
    if (period === 'custom' && btn.dataset.period === 'custom') btn.className = 'btn btn-sm btn-primary period-btn';
    if (period !== 'custom' && btn.dataset.period === 'custom') btn.className = 'btn btn-sm btn-outline-secondary period-btn';
  });
  document.getElementById('custom-range').style.display = period === 'custom' ? '' : 'none';
  const starts = {'all': null, '2010+': 2010, '2015+': 2015, '2020+': 2020, '2023+': 2023};
  if (period !== 'custom') {
    state.fromYear = starts[period];
    state.toYear = null;
  } else {
    applyCustomRange();
  }
  render();
}
function applyCustomRange() {
  const fy = parseInt(document.getElementById('year-from').value || '', 10);
  const ty = parseInt(document.getElementById('year-to').value || '', 10);
  state.fromYear = Number.isFinite(fy) ? fy : null;
  state.toYear = Number.isFinite(ty) ? ty : null;
  render();
}
function sortByYear(year) {
  if (state.sortSymbol === year) {
    state.sortDir *= -1;
  } else {
    state.sortSymbol = year;
    state.sortDir = -1;
  }
  render();
}
function render() {
  state.mode = document.getElementById('mode').value;
  state.filterClass = document.getElementById('filter-class').value;
  state.filterStable = document.getElementById('filter-stable').value;
  const years = visibleYears();
  const syms = visibleSymbols(years);

  document.getElementById('c-period').textContent =
    (state.fromYear || Math.min(...D.years)) + ' - ' + (state.toYear || Math.max(...D.years));
  document.getElementById('c-mode').textContent = state.mode;
  document.getElementById('matrix-mode-label').textContent = state.mode;
  document.getElementById('c-symbols').textContent = syms.length;
  document.getElementById('c-years').textContent = years.length;
  document.getElementById('matrix-info').textContent =
    `${state.mode}: ritorno annuo composto su log-return giornalieri; Sharpe annualizzato sqrt(252).`;

  const head = document.getElementById('matrix-head');
  head.innerHTML = '<tr><th class="year-head">Anno</th>' + syms.map(sym => {
    const m = D.meta[sym] || {};
    const stable = m.stable ? '✓' : '~';
    return `<th title="${esc(sym)}">${esc(sym)}<div class="symbol-meta">${clsBadge(m.classification)} ${stable}</div></th>`;
  }).join('') + '</tr>';

  const body = document.getElementById('matrix-body');
  body.innerHTML = years.map(y => {
    const cells = syms.map(sym => {
      const rec = D.metrics[state.mode][sym]?.[String(y)];
      if (!rec) return '<td class="muted">-</td>';
      return `<td>${fmtRet(rec.return)}${fmtSharpe(rec.sharpe)}</td>`;
    }).join('');
    const arrow = state.sortSymbol === y ? (state.sortDir > 0 ? ' ↑' : ' ↓') : ' ↕';
    return `<tr><td class="year-cell" onclick="sortByYear(${y})" title="Ordina simboli per ritorno ${y}">${y}${arrow}</td>${cells}</tr>`;
  }).join('');
}

document.getElementById('year-from').min = Math.min(...D.years);
document.getElementById('year-from').max = Math.max(...D.years);
document.getElementById('year-to').min = Math.min(...D.years);
document.getElementById('year-to').max = Math.max(...D.years);
setPeriod('all');
</script>
</body>
</html>
"""


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    payload = build_payload()
    OUT_HTML.write_text(build_html(payload), encoding="utf-8")
    print(f"✓ {OUT_HTML} symbols={len(payload['symbols'])} years={len(payload['years'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
