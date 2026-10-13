"""
HTMLReport — report HTML self-contained per analisi strategia.

Genera un file .html con:
- Metriche chiave (backtest, simulazione, paper trading)
- Distribuzione trade (outcome, side, bars_held)
- Monte Carlo: distribuzione rendimenti, drawdown, Sharpe
- Stabilità parametri (heatmap testuale)
- Confronto backtest/sim/paper
- Sezione "riproduci" con il codice Python che ha generato il report

Nessuna dipendenza esterna oltre pandas/numpy (CSS/JS inline).
"""
from __future__ import annotations

import base64
import hashlib
import html
import json
import textwrap
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ──────────────────────────────────────────────────────────────────
# CSS / JS inline (nessuna CDN, tutto embedded)
# ──────────────────────────────────────────────────────────────────

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1117;
       color: #e1e4e8; font-size: 14px; }
h1 { font-size: 1.6em; padding: 20px 30px 8px; color: #58a6ff; }
h2 { font-size: 1.1em; color: #79c0ff; margin: 20px 0 10px; border-bottom: 1px solid #30363d;
     padding-bottom: 6px; }
h3 { font-size: 0.95em; color: #8b949e; margin: 14px 0 6px; }
.container { max-width: 1400px; margin: 0 auto; padding: 10px 30px 40px; }
.meta { color: #8b949e; font-size: 0.85em; padding: 0 30px 14px; }

/* Cards */
.card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
        padding: 16px 20px; margin-bottom: 16px; }
.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
             gap: 12px; margin-bottom: 16px; }
.metric { background: #161b22; border: 1px solid #30363d; border-radius: 6px;
          padding: 12px 16px; text-align: center; }
.metric .label { font-size: 0.78em; color: #8b949e; text-transform: uppercase;
                 letter-spacing: .04em; margin-bottom: 4px; }
.metric .value { font-size: 1.4em; font-weight: 700; }
.pos { color: #3fb950; } .neg { color: #f85149; } .neu { color: #d29922; }

/* Tables */
table { width: 100%; border-collapse: collapse; font-size: 0.88em; }
th { background: #21262d; color: #8b949e; text-align: left; padding: 7px 10px;
     font-weight: 600; text-transform: uppercase; font-size: 0.78em; letter-spacing: .04em; }
td { padding: 6px 10px; border-bottom: 1px solid #21262d; }
tr:hover td { background: #1c2128; }
.badge { display: inline-block; border-radius: 3px; padding: 1px 7px; font-size: 0.8em;
         font-weight: 600; }
.badge-tp  { background:#1a3a1a; color:#3fb950; }
.badge-sl  { background:#3a1a1a; color:#f85149; }
.badge-eod { background:#1a2a3a; color:#58a6ff; }
.badge-sig { background:#2a2a1a; color:#d29922; }

/* Distribution bar */
.dist-bar { background: #21262d; border-radius: 3px; height: 14px; margin: 2px 0; }
.dist-fill { height: 100%; border-radius: 3px; background: #1f6feb; }

/* Monte Carlo percentile bars */
.mc-row { display: flex; align-items: center; gap: 8px; margin: 4px 0; }
.mc-label { width: 80px; font-size: 0.82em; color: #8b949e; text-align: right; }
.mc-bar-wrap { flex: 1; background: #21262d; border-radius: 3px; height: 12px; position: relative; }
.mc-bar-p5p95 { position: absolute; height: 100%; background: #1f6feb44; border-radius: 3px; }
.mc-bar-median { position: absolute; width: 3px; height: 100%; background: #58a6ff; border-radius: 2px; }
.mc-val { width: 70px; font-size: 0.82em; text-align: left; }

/* Code block */
pre { background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 14px 16px;
      font-size: 0.82em; overflow-x: auto; color: #c9d1d9; line-height: 1.55; }
.copy-btn { background: #21262d; border: 1px solid #30363d; color: #8b949e; border-radius: 5px;
            padding: 4px 12px; font-size: 0.8em; cursor: pointer; margin-bottom: 8px; }
.copy-btn:hover { background: #30363d; }

/* Tabs */
.tabs { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
.tab { background: #21262d; border: 1px solid #30363d; border-radius: 5px; padding: 6px 14px;
       cursor: pointer; font-size: 0.85em; color: #8b949e; }
.tab.active { background: #1f6feb; border-color: #1f6feb; color: #fff; }
.tab-content { display: none; } .tab-content.active { display: block; }

/* Heatmap */
.hmap { display: grid; gap: 2px; }
.hmap-cell { padding: 4px 6px; border-radius: 3px; font-size: 0.78em; text-align: center; }
.section { margin-bottom: 28px; }
"""

_JS = """
function showTab(group, idx) {
  document.querySelectorAll('[data-group="'+group+'"]').forEach((el,i) => {
    el.classList.toggle('active', i===idx);
  });
  document.querySelectorAll('[data-tab-group="'+group+'"]').forEach((el,i) => {
    el.classList.toggle('active', i===idx);
  });
}
function copyCode(id) {
  navigator.clipboard.writeText(document.getElementById(id).textContent);
}
"""


# ──────────────────────────────────────────────────────────────────

class HTMLReport:
    """
    Genera report HTML self-contained per analisi strategia.

    Uso
    ---
    rpt = HTMLReport(title="HMA NVDA 2025-01")
    rpt.add_backtest(metrics, trades, label="Backtest")
    rpt.add_montecarlo(mc_results, mc_stats)
    rpt.add_stability(stability_df)
    rpt.add_coherence(coherence_df)
    rpt.add_reproduce_code(code_str)
    rpt.save("output/report_nvda.html")
    """

    def __init__(self, title: str = "Strategy Report", subtitle: str = ""):
        self.title = title
        self.subtitle = subtitle
        self._sections: List[str] = []
        self._created = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ──────────────────────────────────────────────────────────────
    # Public add_* methods
    # ──────────────────────────────────────────────────────────────

    def add_backtest(
        self,
        metrics: Dict,
        trades: Optional[pd.DataFrame] = None,
        label: str = "Backtest",
        params: Optional[Dict] = None,
    ) -> "HTMLReport":
        """Aggiunge sezione con metriche e tabella trade."""
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')

        if params:
            p_str = " &nbsp;·&nbsp; ".join(f"<b>{k}</b>={v}" for k, v in params.items())
            buf.write(f'<p style="color:#8b949e;font-size:.85em;margin-bottom:10px">{p_str}</p>\n')

        buf.write(self._metrics_cards(metrics))

        if trades is not None and len(trades) > 0:
            buf.write("<h3>Distribuzione outcome</h3>")
            buf.write(self._outcome_dist(trades))
            buf.write("<h3>Ultimi 30 trade</h3>")
            buf.write(self._trades_table(trades.tail(30)))

        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_montecarlo(
        self,
        results_df: pd.DataFrame,
        stats: Dict,
        n_real_trades: int = 0,
        label: str = "Monte Carlo",
    ) -> "HTMLReport":
        """Aggiunge sezione Monte Carlo con distribuzione e percentili."""
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')

        if n_real_trades:
            buf.write(f'<p style="color:#8b949e;font-size:.85em;margin-bottom:10px">'
                      f'Trade reali campione: <b>{n_real_trades}</b> &nbsp;·&nbsp; '
                      f'Simulazioni: <b>{len(results_df):,}</b></p>\n')

        buf.write(self._mc_summary_cards(stats))
        buf.write("<h3>Distribuzione rendimenti annuali</h3>")
        buf.write(self._mc_histogram(results_df, "total_return", scale=100, unit="%"))
        buf.write("<h3>Distribuzione Max Drawdown</h3>")
        buf.write(self._mc_histogram(results_df, "max_drawdown", scale=100, unit="%"))
        buf.write("<h3>Percentili chiave</h3>")
        buf.write(self._mc_percentile_bars(stats))

        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_stability(
        self,
        stability_df: pd.DataFrame,
        label: str = "Stabilità parametri",
        llm_comment: str = "",
    ) -> "HTMLReport":
        """
        Aggiunge sezione stabilità su finestre Monte Carlo.

        stability_df: output di StabilityAnalyzer.analyze()
        colonne: window, n_trades, expectancy_pct, win_rate, sharpe, sqn
        """
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')

        if llm_comment:
            buf.write(f'<div class="card" style="border-left:3px solid #58a6ff">'
                      f'<pre style="background:none;border:none;padding:0;color:#c9d1d9">'
                      f'{html.escape(llm_comment)}</pre></div>\n')

        if stability_df is not None and len(stability_df) > 0:
            buf.write("<h3>Metriche per finestra temporale</h3>")
            buf.write(self._stability_table(stability_df))
            buf.write("<h3>Heatmap expectancy per finestra</h3>")
            buf.write(self._stability_heatmap(stability_df))

        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_coherence(
        self,
        coherence_df: pd.DataFrame,
        label: str = "Confronto Backtest / Sim / Paper",
        llm_comment: str = "",
    ) -> "HTMLReport":
        """
        Aggiunge sezione confronto tra 3 contesti.

        coherence_df: output di CoherenceAnalyzer.compare()
        """
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')

        if llm_comment:
            buf.write(f'<div class="card" style="border-left:3px solid #d29922">'
                      f'<pre style="background:none;border:none;padding:0;color:#c9d1d9">'
                      f'{html.escape(llm_comment)}</pre></div>\n')

        if coherence_df is not None and len(coherence_df) > 0:
            buf.write(self._coherence_table(coherence_df))

        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_divergences(
        self,
        diff_df: pd.DataFrame,
        summary: Dict,
        label: str = "Divergenze Pandas ↔ BT",
        llm_comment: str = "",
    ) -> "HTMLReport":
        """Aggiunge sezione diff trade-by-trade."""
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')

        if llm_comment:
            buf.write(f'<div class="card" style="border-left:3px solid #f85149">'
                      f'<pre style="background:none;border:none;padding:0;color:#c9d1d9">'
                      f'{html.escape(llm_comment)}</pre></div>\n')

        # Summary badges
        buf.write('<div class="card-grid">')
        for k, v in summary.items():
            color = "#3fb950" if k in ("match", "match_pct") else "#f85149" if k != "total" else "#58a6ff"
            buf.write(f'<div class="metric"><div class="label">{html.escape(str(k))}</div>'
                      f'<div class="value" style="color:{color}">{v}</div></div>')
        buf.write("</div>")

        if diff_df is not None and len(diff_df) > 0:
            non_match = diff_df[diff_df.get("divergence_type", pd.Series()) != "match"] \
                if "divergence_type" in diff_df.columns else diff_df
            if len(non_match) > 0:
                buf.write("<h3>Trade divergenti</h3>")
                buf.write(self._generic_table(non_match.head(50)))

        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_reproduce_code(self, code: str, label: str = "Riproduci") -> "HTMLReport":
        """Aggiunge blocco codice Python riproducibile."""
        code_id = f"code_{hashlib.md5(code.encode()).hexdigest()[:8]}"
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')
        buf.write(f'<p style="color:#8b949e;font-size:.85em;margin-bottom:8px">'
                  f'Codice esatto utilizzato per generare questo report.</p>\n')
        buf.write(f'<button class="copy-btn" onclick="copyCode(\'{code_id}\')">Copia</button>\n')
        buf.write(f'<pre id="{code_id}">{html.escape(code)}</pre>')
        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def add_llm_analysis(self, text: str, label: str = "Analisi AI") -> "HTMLReport":
        """Aggiunge blocco testo analisi LLM."""
        buf = StringIO()
        buf.write(f'<div class="section"><h2>{html.escape(label)}</h2>\n')
        buf.write(f'<div class="card" style="border-left:3px solid #79c0ff">'
                  f'<pre style="background:none;border:none;padding:0;color:#c9d1d9;white-space:pre-wrap">'
                  f'{html.escape(text)}</pre></div>\n')
        buf.write("</div>")
        self._sections.append(buf.getvalue())
        return self

    def save(self, path: str | Path) -> Path:
        """Scrive il report HTML e ritorna il path."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self._render(), encoding="utf-8")
        return path

    def to_string(self) -> str:
        return self._render()

    # ──────────────────────────────────────────────────────────────
    # Rendering helpers
    # ──────────────────────────────────────────────────────────────

    def _render(self) -> str:
        body = "\n".join(self._sections)
        return f"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(self.title)}</title>
<style>{_CSS}</style>
</head>
<body>
<h1>{html.escape(self.title)}</h1>
<p class="meta">{html.escape(self.subtitle) + ' &nbsp;·&nbsp; ' if self.subtitle else ''}Generato: {self._created}</p>
<div class="container">
{body}
</div>
<script>{_JS}</script>
</body>
</html>"""

    def _metrics_cards(self, metrics: Dict) -> str:
        display = {
            "n_trades": ("Trade", "", "neu"),
            "win_rate": ("Win Rate", "%", "pos" if metrics.get("win_rate", 0) > 0.5 else "neg"),
            "expectancy_pct": ("Expectancy", "%", "pos" if metrics.get("expectancy_pct", 0) > 0 else "neg"),
            "sharpe": ("Sharpe", "", "pos" if metrics.get("sharpe", 0) > 1 else "neg"),
            "sqn": ("SQN", "", "pos" if metrics.get("sqn", 0) > 2 else "neu"),
            "max_drawdown_pct": ("Max DD", "%", "neg"),
            "total_return_pct": ("Total Return", "%", "pos" if metrics.get("total_return_pct", 0) > 0 else "neg"),
            "profit_factor": ("Profit Factor", "", "pos" if metrics.get("profit_factor", 0) > 1 else "neg"),
        }
        buf = '<div class="card-grid">'
        for key, (label, unit, css) in display.items():
            val = metrics.get(key)
            if val is None:
                continue
            if key == "win_rate":
                fmt = f"{val*100:.1f}{unit}"
            elif isinstance(val, float):
                fmt = f"{val:.2f}{unit}"
            else:
                fmt = f"{val}{unit}"
            buf += (f'<div class="metric"><div class="label">{label}</div>'
                    f'<div class="value {css}">{fmt}</div></div>')
        buf += "</div>"
        return buf

    def _outcome_dist(self, trades: pd.DataFrame) -> str:
        if "outcome" not in trades.columns:
            return ""
        vc = trades["outcome"].value_counts()
        total = len(trades)
        badge_map = {"tp": "badge-tp", "sl": "badge-sl", "eod": "badge-eod", "signal": "badge-sig",
                     "exitbar": "badge-eod"}
        buf = '<div class="card">'
        for outcome, cnt in vc.items():
            pct = cnt / total * 100
            bc = badge_map.get(str(outcome), "badge-sig")
            buf += (f'<div style="display:flex;align-items:center;gap:10px;margin:5px 0">'
                    f'<span class="badge {bc}" style="width:70px;text-align:center">{outcome}</span>'
                    f'<div class="dist-bar" style="flex:1">'
                    f'<div class="dist-fill" style="width:{pct:.1f}%"></div></div>'
                    f'<span style="width:60px;text-align:right;font-size:.85em">'
                    f'{cnt} ({pct:.1f}%)</span></div>')
        buf += "</div>"
        return buf

    def _trades_table(self, trades: pd.DataFrame) -> str:
        cols = [c for c in ["symbol", "side", "entry_time", "entry_price",
                             "exit_time", "exit_price", "exit_pct", "outcome", "bars_held"]
                if c in trades.columns]
        return self._generic_table(trades[cols])

    def _generic_table(self, df: pd.DataFrame) -> str:
        buf = '<div style="overflow-x:auto"><table>'
        buf += "<thead><tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns) + "</tr></thead>"
        buf += "<tbody>"
        for _, row in df.iterrows():
            buf += "<tr>"
            for col, val in zip(df.columns, row):
                cell = self._fmt_cell(col, val)
                buf += f"<td>{cell}</td>"
            buf += "</tr>"
        buf += "</tbody></table></div>"
        return buf

    @staticmethod
    def _fmt_cell(col: str, val) -> str:
        if pd.isna(val) if not isinstance(val, str) else False:
            return '<span style="color:#484f58">—</span>'
        if col == "outcome":
            badge_map = {"tp": "badge-tp", "sl": "badge-sl", "eod": "badge-eod",
                         "signal": "badge-sig", "exitbar": "badge-eod", "match": "badge-tp"}
            bc = badge_map.get(str(val), "badge-sig")
            return f'<span class="badge {bc}">{html.escape(str(val))}</span>'
        if col in ("exit_pct", "expectancy_pct", "total_return_pct"):
            try:
                f = float(val)
                css = "pos" if f > 0 else "neg"
                return f'<span class="{css}">{f:+.3f}%</span>'
            except (ValueError, TypeError):
                pass
        if col in ("win_rate",):
            try:
                f = float(val)
                return f"{f*100:.1f}%"
            except (ValueError, TypeError):
                pass
        if isinstance(val, float):
            return f"{val:.4f}"
        if isinstance(val, (pd.Timestamp, datetime)):
            return str(val)[:16]
        return html.escape(str(val))

    def _mc_summary_cards(self, stats: Dict) -> str:
        tr = stats.get("total_return", {})
        dd = stats.get("max_drawdown", {})
        sh = stats.get("sharpe_ratio", {})

        def _pct(v): return f"{v*100:.1f}%" if v is not None else "—"
        def _f2(v): return f"{v:.2f}" if v is not None else "—"

        cards = [
            ("Rendimento medio", _pct(tr.get("mean")), "pos" if tr.get("mean", 0) > 0 else "neg"),
            ("Rendimento mediano", _pct(tr.get("median")), "pos" if tr.get("median", 0) > 0 else "neg"),
            ("P5 / P95", f"{_pct(tr.get('p5'))} / {_pct(tr.get('p95'))}", "neu"),
            ("Prob > 0%", f"{tr.get('prob_positive', 0)*100:.1f}%", "pos"),
            ("Prob > 10%", f"{tr.get('prob_gt_10pct', 0)*100:.1f}%", "pos"),
            ("MaxDD medio", _pct(dd.get("mean")), "neg"),
            ("MaxDD p95", _pct(dd.get("p95")), "neg"),
            ("Sharpe medio", _f2(sh.get("mean")), "pos" if sh.get("mean", 0) > 1 else "neg"),
        ]
        buf = '<div class="card-grid">'
        for label, val, css in cards:
            buf += (f'<div class="metric"><div class="label">{label}</div>'
                    f'<div class="value {css}" style="font-size:1.1em">{val}</div></div>')
        buf += "</div>"
        return buf

    def _mc_histogram(self, df: pd.DataFrame, col: str, scale: float = 1.0, unit: str = "") -> str:
        """Istogramma ASCII-style con barre HTML."""
        vals = df[col].dropna() * scale
        if len(vals) == 0:
            return "<p>Nessun dato</p>"

        n_bins = 30
        counts, edges = np.histogram(vals, bins=n_bins)
        max_c = counts.max()

        buf = '<div class="card" style="padding:12px">'
        buf += (f'<div style="font-size:.8em;color:#8b949e;margin-bottom:8px">'
                f'Min: {vals.min():.2f}{unit} &nbsp; '
                f'Med: {vals.median():.2f}{unit} &nbsp; '
                f'Max: {vals.max():.2f}{unit} &nbsp; '
                f'Std: {vals.std():.2f}{unit}</div>')

        for i, (cnt, left, right) in enumerate(zip(counts, edges[:-1], edges[1:])):
            bar_pct = cnt / max_c * 100 if max_c > 0 else 0
            mid = (left + right) / 2
            is_zero_crossing = left < 0 <= right
            bar_color = "#3fb950" if mid >= 0 else "#f85149"
            border = " border: 1px solid #58a6ff;" if is_zero_crossing else ""
            buf += (f'<div style="display:flex;align-items:center;gap:6px;margin:1px 0">'
                    f'<span style="width:50px;text-align:right;font-size:.75em;color:#8b949e">'
                    f'{left:.1f}</span>'
                    f'<div style="flex:1;background:#21262d;border-radius:2px;height:12px;{border}">'
                    f'<div style="width:{bar_pct:.1f}%;height:100%;background:{bar_color};border-radius:2px"></div>'
                    f'</div>'
                    f'<span style="width:35px;font-size:.75em;color:#8b949e">{cnt:,}</span>'
                    f'</div>')
        buf += "</div>"
        return buf

    def _mc_percentile_bars(self, stats: Dict) -> str:
        metrics_to_show = [
            ("total_return", "Rendimento", 100, "%"),
            ("max_drawdown", "Max Drawdown", 100, "%"),
            ("sharpe_ratio", "Sharpe", 1, ""),
        ]
        buf = '<div class="card">'
        for key, label, scale, unit in metrics_to_show:
            s = stats.get(key, {})
            p5 = s.get("p5", 0) * scale
            p95 = s.get("p95", 0) * scale
            med = s.get("median", 0) * scale
            rng = max(abs(p5), abs(p95), 0.01)

            # Normalizza [0,100]
            p5_pct = max(0, min(100, (p5 + rng) / (2 * rng) * 100))
            p95_pct = max(0, min(100, (p95 + rng) / (2 * rng) * 100))
            med_pct = max(0, min(100, (med + rng) / (2 * rng) * 100))

            left = min(p5_pct, p95_pct)
            width = abs(p95_pct - p5_pct)

            buf += (f'<div class="mc-row">'
                    f'<span class="mc-label">{label}</span>'
                    f'<div class="mc-bar-wrap">'
                    f'<div class="mc-bar-p5p95" style="left:{left:.1f}%;width:{width:.1f}%"></div>'
                    f'<div class="mc-bar-median" style="left:{med_pct:.1f}%"></div>'
                    f'</div>'
                    f'<span class="mc-val" style="font-size:.8em;color:#8b949e">'
                    f'P5:{p5:.2f}{unit} M:{med:.2f}{unit} P95:{p95:.2f}{unit}'
                    f'</span></div>')
        buf += "</div>"
        return buf

    def _stability_table(self, df: pd.DataFrame) -> str:
        return self._generic_table(df)

    def _stability_heatmap(self, df: pd.DataFrame) -> str:
        if "expectancy_pct" not in df.columns:
            return ""
        vals = df["expectancy_pct"].values
        vmin, vmax = vals.min(), vals.max()
        rng = max(abs(vmin), abs(vmax), 0.001)

        def _color(v):
            # Verde per positivo, rosso per negativo
            if v >= 0:
                intensity = min(int(v / rng * 200), 200)
                return f"background:rgb({20},{50+intensity},{20})"
            else:
                intensity = min(int(abs(v) / rng * 200), 200)
                return f"background:rgb({50+intensity},{20},{20})"

        label_col = next((c for c in ["window", "period", "label"] if c in df.columns), None)

        buf = '<div class="card"><div class="hmap" style="grid-template-columns:repeat(auto-fill,minmax(120px,1fr))">'
        for _, row in df.iterrows():
            v = row["expectancy_pct"]
            label = str(row[label_col])[:20] if label_col else ""
            color = _color(v)
            txt_color = "#3fb950" if v >= 0 else "#f85149"
            buf += (f'<div class="hmap-cell" style="{color}">'
                    f'<div style="color:#8b949e;font-size:.75em">{html.escape(label)}</div>'
                    f'<div style="color:{txt_color};font-weight:700">{v:+.3f}%</div>'
                    f'</div>')
        buf += "</div></div>"
        return buf

    def _coherence_table(self, df: pd.DataFrame) -> str:
        return self._generic_table(df)
