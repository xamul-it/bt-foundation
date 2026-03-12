#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera report da execution audit JSONL (BT vs Alpaca).

Come generare i log audit:
1. Avvia paper/live con audit full:
   - `btmain.py ... --mode paper --live --audit-full`
   - oppure `bin/parallel_sim/run_parallel.py ... --audit-full`
2. I file JSONL vengono scritti in:
   - `logs/execution_audit/YYYYMMDD/<run_id>.jsonl`
3. Esegui questo script:
   - `python bin/audit_report.py --jsonl <file.jsonl> --out-dir logs/audit_report`
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from tabulate import tabulate
except Exception:  # pragma: no cover
    tabulate = None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reportistica execution audit JSONL")
    p.add_argument("--jsonl", required=True, help="Percorso file audit JSONL")
    p.add_argument("--run-id", default=None, help="Filtra uno specifico run_id")
    p.add_argument("--out-dir", default=None, help="Cartella output report (default: cartella del JSONL)")
    p.add_argument("--bar-seconds", type=float, default=60.0, help="Durata barra in secondi per metriche delay in barre")
    return p.parse_args()


def _coalesce_order_key(row: pd.Series) -> str:
    # Canonical key: client_order_id remains stable across submit/accept/fill/cancel.
    # Alpaca IDs appear only after submit_accepted and would split one lifecycle in two groups.
    for k in ("client_order_id", "alpaca_order_id", "order_id", "order_ref"):
        v = row.get(k)
        if pd.notna(v) and str(v).strip():
            return str(v)
    return "unknown"


def _valid_id(v: Any) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip()
    return bool(s and s.lower() != "none" and s.lower() != "nan")


def load_jsonl(path: Path) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    bad_lines = 0
    with path.open("r", encoding="utf-8") as fh:
        for n, line in enumerate(fh, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                bad_lines += 1
                continue
    if bad_lines:
        print(f"[WARN] Righe JSONL non valide ignorate: {bad_lines}")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts_event_utc" in df.columns:
        df["ts_event_utc"] = pd.to_datetime(df["ts_event_utc"], utc=True, errors="coerce")
    return df


def first_not_null(s: pd.Series) -> Any:
    ss = s.dropna()
    return None if ss.empty else ss.iloc[0]


def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce").dropna()


def parse_dt_utc(value: Any) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors="coerce")


def summarize_orders(events: pd.DataFrame, bar_seconds: float = 60.0) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    df = events.copy()
    if "event_type" not in df.columns:
        return pd.DataFrame()

    # Build a stable cross-reference so WS-only rows (order_id/alpaca_id only)
    # are merged with strategy rows keyed by client_order_id.
    id_to_client: Dict[str, str] = {}
    if "client_order_id" in df.columns:
        valid_client = df["client_order_id"].map(_valid_id)
        for id_col in ("alpaca_order_id", "order_id"):
            if id_col not in df.columns:
                continue
            valid_id = df[id_col].map(_valid_id)
            pairs = df.loc[valid_client & valid_id, [id_col, "client_order_id"]]
            for _, r in pairs.iterrows():
                id_to_client[str(r[id_col]).strip()] = str(r["client_order_id"]).strip()

    def _resolve_order_key(row: pd.Series) -> str:
        client = row.get("client_order_id")
        if _valid_id(client):
            return str(client).strip()

        for k in ("alpaca_order_id", "order_id"):
            v = row.get(k)
            if _valid_id(v):
                mapped = id_to_client.get(str(v).strip())
                if mapped:
                    return mapped

        return _coalesce_order_key(row)

    df["order_key"] = df.apply(_resolve_order_key, axis=1)
    df = df.sort_values("ts_event_utc")

    grouped = df.groupby("order_key", dropna=False)
    rows: List[Dict[str, Any]] = []

    for key, g in grouped:
        evs = list(g["event_type"].dropna().astype(str))
        symbols = g["symbol"].dropna().astype(str)
        symbol = symbols.iloc[0] if not symbols.empty else None
        requested_qty = _numeric_series(g, "requested_qty")
        requested_qty = float(requested_qty.iloc[0]) if not requested_qty.empty else None

        # Prefer state-writer qty (filled_qty). bt_notify.executed_size può essere cumulativo
        # e non affidabile a livello singolo ordine.
        filled_qty_series = _numeric_series(g, "filled_qty")
        if not filled_qty_series.empty:
            filled_qty = float(filled_qty_series.max())
        else:
            executed_qty = _numeric_series(g, "executed_size").abs()
            if not executed_qty.empty:
                inferred_qty = float(executed_qty.max())
                if requested_qty is not None:
                    inferred_qty = min(inferred_qty, abs(float(requested_qty)))
                filled_qty = inferred_qty
            else:
                filled_qty = 0.0

        # Preferiamo il timestamp evento reale di submit per mantenere la sequenza cronologica.
        submit_events = g[g["event_type"].isin(["submit_attempt", "submit_accepted"])]
        submit_event_ts = submit_events["ts_event_utc"].min() if not submit_events.empty else pd.NaT
        submit_ts = submit_event_ts if pd.notna(submit_event_ts) else parse_dt_utc(first_not_null(g.get("dt_submit", pd.Series(dtype=object))))
        signal_bar_ts = parse_dt_utc(first_not_null(g.get("signal_dt", pd.Series(dtype=object))))
        if pd.notna(signal_bar_ts) and bar_seconds > 0:
            signal_ts = signal_bar_ts + pd.to_timedelta(bar_seconds, unit="s")
        else:
            signal_ts = signal_bar_ts
        valid_until_ts = parse_dt_utc(first_not_null(g.get("valid_until_dt", pd.Series(dtype=object))))
        first_ts = g["ts_event_utc"].min()
        last_ts = g["ts_event_utc"].max()

        # Prezzo richiesto vs eseguito
        requested_price = _numeric_series(g, "requested_price")
        if requested_price.empty:
            requested_price = _numeric_series(g, "limit_price")
        requested_price = float(requested_price.iloc[0]) if not requested_price.empty else None
        submit_price = _numeric_series(g, "submitted_price")
        if submit_price.empty:
            submit_price = _numeric_series(g, "price")
        submit_price = float(submit_price.iloc[0]) if not submit_price.empty else requested_price
        fill_price = _numeric_series(g, "filled_avg_price")
        if fill_price.empty:
            fill_price = _numeric_series(g, "executed_price")
        actual_fill_price = float(fill_price.iloc[-1]) if not fill_price.empty else None

        price_diff_submit_vs_req = None
        if submit_price is not None and requested_price is not None:
            price_diff_submit_vs_req = submit_price - requested_price
        slippage_abs = None
        slippage_bps = None
        if (
            actual_fill_price is not None
            and requested_price not in (None, 0)
            and filled_qty is not None
            and float(filled_qty) > 0
        ):
            slippage_abs = actual_fill_price - requested_price
            slippage_bps = (slippage_abs / requested_price) * 10000.0

        outcome = "open"
        if "submit_error" in evs or "reject" in evs:
            outcome = "rejected"
        elif "cancel" in evs:
            outcome = "canceled"
        elif "fill" in evs:
            outcome = "filled"
        elif "partial_fill" in evs:
            outcome = "partial"
        else:
            status_vals = g.get("status", pd.Series(dtype=object)).dropna().astype(str).str.upper().tolist()
            has_status_partial = any("PARTIALLY_FILLED" in s for s in status_vals)
            has_status_filled = any(s.endswith("FILLED") and "PARTIALLY" not in s for s in status_vals)
            has_status_canceled = any("CANCELED" in s for s in status_vals)
            has_status_rejected = any(("REJECT" in s) or ("FAILED" in s) for s in status_vals)
            if has_status_rejected:
                outcome = "rejected"
            elif has_status_canceled:
                outcome = "canceled"
            elif has_status_filled:
                outcome = "filled"
            elif has_status_partial:
                outcome = "partial"

        cancel_origins = g.get("cancel_origin", pd.Series(dtype=object)).dropna().astype(str).unique().tolist()
        cancel_origin = cancel_origins[0] if cancel_origins else None
        if outcome == "canceled" and cancel_origin == "local":
            outcome = "TTL cancelled"

        has_submit_accepted = "submit_accepted" in evs
        has_ttl_expired = "ttl_expired" in evs
        has_cancel_local = "local" in cancel_origins
        has_cancel_external = "alpaca_or_external" in cancel_origins

        err_classes = g.get("error_class", pd.Series(dtype=object)).dropna().astype(str).unique().tolist()
        has_crypto_short_error = "crypto_short_not_allowed" in err_classes

        # Close candidate: lato ordine contrario rispetto a posizione esistente.
        asset_pos_size = _numeric_series(g, "asset_pos_size")
        asset_pos_size = float(asset_pos_size.iloc[0]) if not asset_pos_size.empty else None
        side_vals = g.get("side", pd.Series(dtype=object)).dropna().astype(str).tolist()
        if not side_vals:
            side_vals = g.get("order_side", pd.Series(dtype=object)).dropna().astype(str).tolist()
        side = side_vals[0].upper() if side_vals else None
        is_close_candidate = bool(
            asset_pos_size is not None
            and side in ("BUY", "SELL")
            and ((asset_pos_size > 0 and side == "SELL") or (asset_pos_size < 0 and side == "BUY"))
        )

        fill_ratio = None
        if requested_qty and requested_qty > 0:
            fill_ratio = float(filled_qty) / float(requested_qty)

        # Latenze
        fill_events = g[g["event_type"].isin(["partial_fill", "fill"])]
        first_fill_ts = fill_events["ts_event_utc"].min() if not fill_events.empty else pd.NaT
        delay_from_submit_s = (first_fill_ts - submit_ts).total_seconds() if pd.notna(first_fill_ts) and pd.notna(submit_ts) else None
        delay_from_signal_s = (first_fill_ts - signal_ts).total_seconds() if pd.notna(first_fill_ts) and pd.notna(signal_ts) else None
        delay_from_signal_bars = (delay_from_signal_s / bar_seconds) if (delay_from_signal_s is not None and bar_seconds > 0) else None

        # TTL coherency
        flag_fill_after_ttl = bool(pd.notna(first_fill_ts) and pd.notna(valid_until_ts) and first_fill_ts > valid_until_ts)
        flag_signal_bar_first_event_out_of_order = bool(
            pd.notna(signal_bar_ts) and pd.notna(first_ts) and first_ts < signal_bar_ts
        )
        flag_rejected_order = outcome == "rejected"
        flag_close_not_executed = bool(is_close_candidate and (filled_qty <= 0))
        if flag_close_not_executed and outcome == "TTL cancelled":
            flag_close_not_executed = False
        flag_close_too_late = bool(
            is_close_candidate and delay_from_signal_bars is not None and delay_from_signal_bars > 1.0
        )

        flag_bt_full_vs_paper_partial = bool(
            requested_qty and requested_qty > 0 and (0 < filled_qty < requested_qty * 0.999)
        )
        flag_submit_ok_but_canceled_external = bool(has_submit_accepted and has_cancel_external)
        flag_ttl_expected_but_not_canceled_local = bool(has_ttl_expired and not has_cancel_local)
        reason = ""
        if flag_signal_bar_first_event_out_of_order:
            reason = "Signal bar after first event (out-of-order)"
        elif flag_rejected_order:
            reason = "Order rejected"
        elif has_crypto_short_error:
            reason = "Crypto short not allowed"
        elif flag_bt_full_vs_paper_partial:
            reason = "BT full vs paper partial fill mismatch"
        elif flag_submit_ok_but_canceled_external:
            reason = "Submit accepted but canceled external"
        elif flag_ttl_expected_but_not_canceled_local:
            reason = "TTL expired without local cancel"
        elif flag_fill_after_ttl:
            reason = "Fill after TTL"
        elif flag_close_not_executed:
            reason = "Close order not executed"
        elif flag_close_too_late:
            reason = "Close executed too late"
        elif outcome == "TTL cancelled":
            reason = "TTL cancelled locally"

        rows.append(
            {
                "order_key": key,
                "symbol": symbol,
                "reason": reason,
                "run_id": first_not_null(g.get("run_id", pd.Series(dtype=object))),
                "outcome": outcome,
                "requested_qty": requested_qty,
                "filled_qty": filled_qty,
                "fill_ratio": fill_ratio,
                "requested_price": requested_price,
                "submitted_price": submit_price,
                "actual_fill_price": actual_fill_price,
                "price_diff_submit_vs_req": price_diff_submit_vs_req,
                "slippage_abs": slippage_abs,
                "slippage_bps": slippage_bps,
                "submit_ts": submit_ts,
                "signal_bar_ts": signal_bar_ts,
                "signal_ts": signal_ts,
                "valid_until_ts": valid_until_ts,
                "first_fill_ts": first_fill_ts,
                "delta_fill_s": delay_from_signal_s,
                "delay_from_submit_s": delay_from_submit_s,
                "delay_from_signal_s": delay_from_signal_s,
                "delay_from_signal_bars": delay_from_signal_bars,
                "first_event_ts": first_ts,
                "last_event_ts": last_ts,
                "lifecycle_s": (last_ts - first_ts).total_seconds() if pd.notna(first_ts) and pd.notna(last_ts) else None,
                "cancel_origin": cancel_origin,
                "error_class": "|".join(err_classes) if err_classes else None,
                "asset_pos_size": asset_pos_size,
                "side": side,
                "is_close_candidate": is_close_candidate,
                "flag_crypto_short_not_allowed": has_crypto_short_error,
                "flag_bt_full_vs_paper_partial": flag_bt_full_vs_paper_partial,
                "flag_submit_ok_but_canceled_external": flag_submit_ok_but_canceled_external,
                "flag_ttl_expected_but_not_canceled_local": flag_ttl_expected_but_not_canceled_local,
                "flag_fill_after_ttl": flag_fill_after_ttl,
                "flag_signal_bar_first_event_out_of_order": flag_signal_bar_first_event_out_of_order,
                "flag_rejected_order": flag_rejected_order,
                "flag_close_not_executed": flag_close_not_executed,
                "flag_close_too_late": flag_close_too_late,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty:
        out["flag_is_anomaly"] = (
            out["flag_rejected_order"]
            | out["flag_crypto_short_not_allowed"]
            | out["flag_bt_full_vs_paper_partial"]
            | out["flag_submit_ok_but_canceled_external"]
            | out["flag_ttl_expected_but_not_canceled_local"]
            | out["flag_fill_after_ttl"]
            | out["flag_signal_bar_first_event_out_of_order"]
            | out["flag_close_not_executed"]
            | out["flag_close_too_late"]
        )
    return out.sort_values(["first_event_ts", "signal_bar_ts", "submit_ts", "symbol", "order_key"], na_position="last")


def build_summary(orders: pd.DataFrame, events: pd.DataFrame) -> Dict[str, Any]:
    total_orders = int(len(orders))
    submit_attempts = int((events.get("event_type") == "submit_attempt").sum()) if "event_type" in events.columns else 0
    submit_accepted = int((events.get("event_type") == "submit_accepted").sum()) if "event_type" in events.columns else 0

    summary = {
        "orders_total": total_orders,
        "submit_attempts": submit_attempts,
        "submit_accepted": submit_accepted,
        "submit_success_rate": (submit_accepted / submit_attempts) if submit_attempts else None,
        "filled_count": int((orders["outcome"] == "filled").sum()) if not orders.empty else 0,
        "partial_count": int((orders["outcome"] == "partial").sum()) if not orders.empty else 0,
        "rejected_count": int((orders["outcome"] == "rejected").sum()) if not orders.empty else 0,
        "canceled_count": int((orders["outcome"] == "canceled").sum()) if not orders.empty else 0,
        "avg_fill_ratio": float(orders["fill_ratio"].dropna().mean()) if not orders.empty else None,
        "external_cancel_count": int((orders["cancel_origin"] == "alpaca_or_external").sum()) if not orders.empty else 0,
        "local_cancel_count": int((orders["cancel_origin"] == "local").sum()) if not orders.empty else 0,
        "crypto_short_not_allowed_count": int(orders["flag_crypto_short_not_allowed"].sum()) if not orders.empty else 0,
        "rejected_order_count": int(orders["flag_rejected_order"].sum()) if not orders.empty else 0,
        "anomaly_count": int(orders["flag_is_anomaly"].sum()) if not orders.empty else 0,
        "avg_slippage_bps": float(orders["slippage_bps"].dropna().mean()) if not orders.empty else None,
        "avg_abs_slippage_bps": float(orders["slippage_bps"].dropna().abs().mean()) if not orders.empty else None,
        "signal_bar_first_event_out_of_order_count": int(orders["flag_signal_bar_first_event_out_of_order"].sum()) if not orders.empty else 0,
        "close_not_executed_count": int(orders["flag_close_not_executed"].sum()) if not orders.empty else 0,
        "close_too_late_count": int(orders["flag_close_too_late"].sum()) if not orders.empty else 0,
        "fill_after_ttl_count": int(orders["flag_fill_after_ttl"].sum()) if not orders.empty else 0,
    }
    return summary


def _sort_and_reorder_timeline(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sort_cols = [c for c in ["signal_bar_ts", "submit_ts", "first_event_ts", "symbol", "order_key"] if c in df.columns]
    if "first_event_ts" in sort_cols:
        sort_cols = ["first_event_ts"] + [c for c in sort_cols if c != "first_event_ts"]
    out = df.sort_values(sort_cols, na_position="last") if sort_cols else df.copy()

    front = [c for c in ["first_event_ts", "signal_bar_ts", "symbol", "reason", "outcome", "delta_fill_s"] if c in out.columns]
    tail = [c for c in ["order_key", "run_id"] if c in out.columns]
    middle = [c for c in out.columns if c not in front and c not in tail]
    return out[front + middle + tail]


def _format_time_columns_for_export(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    time_cols = [c for c in out.columns if c.endswith("_ts") or c.endswith("_dt")]
    for c in time_cols:
        def _fmt(v: Any) -> Any:
            if pd.isna(v):
                return None
            ts = pd.to_datetime(v, utc=True, errors="coerce")
            if pd.isna(ts):
                return str(v)
            ts = ts.tz_convert(None)
            return ts.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        out[c] = out[c].map(_fmt)
    return out


def print_terminal(summary: Dict[str, Any], orders: pd.DataFrame, pivot_outcomes: pd.DataFrame, pivot_flags: pd.DataFrame) -> None:
    print("\n=== EXECUTION AUDIT SUMMARY ===")
    for k, v in summary.items():
        print(f"{k}: {v}")

    print("\n=== OUTCOMES BY SYMBOL ===")
    if pivot_outcomes.empty:
        print("No data")
    elif tabulate:
        print(tabulate(pivot_outcomes, headers="keys", tablefmt="github", showindex=False))
    else:
        print(pivot_outcomes.to_string(index=False))

    print("\n=== DISCREPANCY FLAGS BY SYMBOL ===")
    if pivot_flags.empty:
        print("No data")
    elif tabulate:
        print(tabulate(pivot_flags, headers="keys", tablefmt="github", showindex=False))
    else:
        print(pivot_flags.to_string(index=False))

    if not orders.empty:
        print("\n=== PRICE/DELAY OVERVIEW ===")
        cols = [
            "symbol",
            "order_key",
            "outcome",
            "requested_price",
            "submitted_price",
            "actual_fill_price",
            "slippage_bps",
            "delay_from_submit_s",
            "delay_from_signal_bars",
            "is_close_candidate",
            "flag_close_not_executed",
            "flag_close_too_late",
        ]
        view = orders[cols].head(20)
        if tabulate:
            print(tabulate(view, headers="keys", tablefmt="github", showindex=False))
        else:
            print(view.to_string(index=False))

    if not orders.empty:
        anomalies = orders[
            orders["flag_is_anomaly"]
        ].copy()
        print("\n=== TOP ANOMALIES ===")
        if anomalies.empty:
            print("No anomalies")
        else:
            cols = [
                "symbol",
                "order_key",
                "outcome",
                "requested_qty",
                "filled_qty",
                "fill_ratio",
                "requested_price",
                "actual_fill_price",
                "slippage_bps",
                "delay_from_signal_bars",
                "cancel_origin",
                "error_class",
            ]
            anomalies = anomalies[cols].head(20)
            if tabulate:
                print(tabulate(anomalies, headers="keys", tablefmt="github", showindex=False))
            else:
                print(anomalies.to_string(index=False))


def main() -> int:
    args = parse_args()
    src = Path(args.jsonl)
    if not src.exists():
        raise FileNotFoundError(f"JSONL non trovato: {src}")

    events = load_jsonl(src)
    if events.empty:
        print("Nessun evento da analizzare.")
        return 0

    if args.run_id:
        if "run_id" not in events.columns:
            print("run_id non presente nel file; filtro ignorato.")
        else:
            events = events[events["run_id"] == args.run_id].copy()
            if events.empty:
                print(f"Nessun evento per run_id={args.run_id}")
                return 0

    orders = summarize_orders(events, bar_seconds=args.bar_seconds)
    summary = build_summary(orders, events)

    if orders.empty:
        pivot_outcomes = pd.DataFrame(columns=["symbol"])
        pivot_flags = pd.DataFrame(columns=["symbol"])
    else:
        pivot_outcomes = (
            pd.pivot_table(
                orders,
                index="symbol",
                columns="outcome",
                values="order_key",
                aggfunc="count",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        flag_cols = [
            "flag_crypto_short_not_allowed",
            "flag_bt_full_vs_paper_partial",
            "flag_submit_ok_but_canceled_external",
            "flag_ttl_expected_but_not_canceled_local",
            "flag_fill_after_ttl",
            "flag_signal_bar_first_event_out_of_order",
            "flag_rejected_order",
            "flag_close_not_executed",
            "flag_close_too_late",
        ]
        pivot_flags = (
            orders.groupby("symbol", dropna=False)[flag_cols]
            .sum()
            .reset_index()
        )

    base_out = Path(args.out_dir) if args.out_dir else src.parent
    out = base_out / src.stem
    out.mkdir(parents=True, exist_ok=True)

    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    orders_export = _format_time_columns_for_export(_sort_and_reorder_timeline(orders))
    orders_export.to_csv(out / "orders.csv", index=False)
    pivot_outcomes.to_csv(out / "pivot_outcomes.csv", index=False)
    pivot_flags.to_csv(out / "pivot_discrepancies.csv", index=False)

    if not orders.empty:
        anomalies = orders[
            orders["flag_is_anomaly"]
        ].copy()
        anomalies = _format_time_columns_for_export(_sort_and_reorder_timeline(anomalies))
    else:
        anomalies = pd.DataFrame()
    anomalies.to_csv(out / "anomalies.csv", index=False)

    print_terminal(summary, orders, pivot_outcomes, pivot_flags)
    print(f"\nReport salvato in: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
