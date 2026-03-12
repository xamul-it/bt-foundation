#!/usr/bin/env python3
"""Unified forensic reconciliation for paper vs Alpaca vs execution_audit.

Outputs:
- forensic_orders.csv
- forensic_anomalies.csv
- forensic_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

UTC = timezone.utc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Forensic reconciliation across paper/alpaca/audit")
    p.add_argument("--paper-dir", required=True, help="Paper output directory (contains orderhistory.json)")
    p.add_argument("--alpaca-dir", default=None, help="Alpaca reconcile output dir (contains order_alpaca.csv)")
    p.add_argument("--audit-jsonl", default=None, help="Execution audit JSONL file")
    p.add_argument("--out-dir", default=None, help="Output directory")
    p.add_argument("--qty-tol", type=float, default=1e-8, help="Qty tolerance")
    p.add_argument("--price-tol", type=float, default=1e-6, help="Price tolerance")
    p.add_argument("--verbose", action="store_true", help="Verbose")
    return p.parse_args()


def _log(msg: str, verbose: bool = True) -> None:
    if verbose:
        print(msg)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v.astimezone(UTC)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=UTC)
        except Exception:
            continue
    return None


def _fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _canonical_status(s: Any) -> str:
    x = str(s or "").strip().lower()
    if x in ("completed", "filled"):
        return "filled"
    if x in ("partially filled", "partially_filled", "partial", "partial_fill"):
        return "partial"
    if x in ("canceled", "cancelled"):
        return "canceled"
    if x in ("rejected", "margin"):
        return "rejected"
    if x in ("expired",):
        return "expired"
    if x in ("accepted", "submitted", "new", "pending_new", "pending_replace", "pending_cancel"):
        return "accepted"
    if x in ("created",):
        return "created"
    return x or "unknown"


def _best_order_key(order: Dict[str, Any]) -> str:
    info = order.get("info") or {}
    for k in ("client_order_id", "id", "order_id"):
        v = info.get(k)
        if v not in (None, ""):
            return str(v)
    v = order.get("id")
    if v not in (None, ""):
        return str(v)
    return "unknown"


def _aggregate_paper_exec(order: Dict[str, Any]) -> Dict[str, Any]:
    events = order.get("events") or []
    prev_cum = 0.0
    filled_qty = 0.0
    notional = 0.0
    fill_count = 0
    first_exec: Optional[datetime] = None
    last_exec: Optional[datetime] = None
    monotonic = True

    for ev in events:
        px_raw = ev.get("exec_price")
        sz_raw = ev.get("exec_size")
        if px_raw in (None, "") or sz_raw in (None, ""):
            continue
        px = _to_float(px_raw, 0.0)
        cum = abs(_to_float(sz_raw, 0.0))
        if px <= 0 or cum <= 0:
            continue

        if cum + 1e-12 < prev_cum:
            monotonic = False
        delta = (cum - prev_cum) if cum >= prev_cum else cum
        if delta <= 0:
            continue
        prev_cum = max(prev_cum, cum)

        filled_qty += delta
        notional += delta * px
        fill_count += 1

        ed = _parse_dt(ev.get("exec_dt") or ev.get("datetime"))
        if ed:
            if first_exec is None or ed < first_exec:
                first_exec = ed
            if last_exec is None or ed > last_exec:
                last_exec = ed

    avg = (notional / filled_qty) if filled_qty > 0 else 0.0
    return {
        "filled_qty": filled_qty,
        "filled_avg_price": avg,
        "fill_count": fill_count,
        "first_exec_dt": first_exec,
        "last_exec_dt": last_exec,
        "fill_monotonic": monotonic,
    }


def summarize_paper_orders(orderhistory_path: Path) -> List[Dict[str, Any]]:
    raw = _load_json(orderhistory_path)
    if isinstance(raw, dict):
        orders = list(raw.values())
    else:
        orders = list(raw or [])

    out = []
    by_asset = defaultdict(list)

    for o in orders:
        info = o.get("info") or {}
        created = o.get("created") or {}
        side = str(o.get("type") or "").lower()
        asset = o.get("asset") or ""
        key = _best_order_key(o)
        agg = _aggregate_paper_exec(o)
        signal_dt = _parse_dt(created.get("signal_dt") or info.get("signal_dt") or o.get("datetime"))
        dt = _parse_dt(o.get("datetime")) or signal_dt
        row = {
            "order_key": key,
            "paper_order_id": str(o.get("id") or ""),
            "asset": asset,
            "side": side,
            "paper_status_raw": str(o.get("status") or ""),
            "paper_status": _canonical_status(o.get("status")),
            "paper_is_close": bool(created.get("is_close") if created.get("is_close") is not None else info.get("is_close", False)),
            "paper_signal_dt": signal_dt,
            "paper_datetime": dt,
            "paper_filled_qty": agg["filled_qty"],
            "paper_filled_avg_price": agg["filled_avg_price"],
            "paper_fill_count": agg["fill_count"],
            "paper_first_exec_dt": agg["first_exec_dt"],
            "paper_last_exec_dt": agg["last_exec_dt"],
            "invariant_fill_monotonic": bool(agg["fill_monotonic"]),
            "paper_event_statuses": "|".join(str(e.get("status") or "") for e in (o.get("events") or [])),
        }
        out.append(row)
        by_asset[asset].append(row)

    # is_close coherence invariant: close side must reduce existing position sign
    for asset, arr in by_asset.items():
        arr.sort(key=lambda r: (r["paper_datetime"] or datetime.max.replace(tzinfo=UTC), r.get("paper_order_id") or ""))
        pos = 0.0
        for r in arr:
            side = r.get("side")
            fq = float(r.get("paper_filled_qty") or 0.0)
            signed = fq if side == "buy" else (-fq if side == "sell" else 0.0)
            incoherent = False
            if r.get("paper_is_close") and fq > 0:
                if side == "sell" and pos <= 0:
                    incoherent = True
                if side == "buy" and pos >= 0:
                    incoherent = True
            r["invariant_is_close_coherent"] = not incoherent
            pos += signed

    return out


def summarize_alpaca_orders(order_csv_path: Path) -> Dict[str, Dict[str, Any]]:
    rows = _load_csv(order_csv_path)
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = str(r.get("client_order_id") or r.get("order_id") or "unknown")
        out[key] = {
            "alpaca_order_id": str(r.get("order_id") or ""),
            "alpaca_client_order_id": str(r.get("client_order_id") or ""),
            "alpaca_asset": str(r.get("symbol") or ""),
            "alpaca_side": str(r.get("side") or "").lower(),
            "alpaca_status": _canonical_status(r.get("status")),
            "alpaca_status_raw": str(r.get("status") or ""),
            "alpaca_filled_qty": _to_float(r.get("filled_qty"), 0.0),
            "alpaca_filled_avg_price": _to_float(r.get("filled_avg_price"), 0.0),
            "alpaca_submitted_at": _parse_dt(r.get("submitted_at")),
            "alpaca_filled_at": _parse_dt(r.get("filled_at")),
            "alpaca_updated_at": _parse_dt(r.get("updated_at")),
        }
    return out


def summarize_audit(jsonl_path: Path) -> Dict[str, Dict[str, Any]]:
    rows = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                continue

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    id_to_client: Dict[str, str] = {}

    for r in rows:
        cid = str(r.get("client_order_id") or "").strip()
        for idk in ("alpaca_order_id", "order_id"):
            oid = str(r.get(idk) or "").strip()
            if cid and oid:
                id_to_client[oid] = cid

    for r in rows:
        cid = str(r.get("client_order_id") or "").strip()
        if not cid:
            mapped = None
            for idk in ("alpaca_order_id", "order_id"):
                oid = str(r.get(idk) or "").strip()
                if oid and oid in id_to_client:
                    mapped = id_to_client[oid]
                    break
            cid = mapped or str(r.get("alpaca_order_id") or r.get("order_id") or r.get("order_ref") or "unknown")
        groups[cid].append(r)

    out: Dict[str, Dict[str, Any]] = {}
    for k, arr in groups.items():
        evs = [str(x.get("event_type") or "") for x in arr]
        status_vals = [_canonical_status(x.get("status")) for x in arr if x.get("status") is not None]

        filled_qty = 0.0
        for x in arr:
            q = x.get("filled_qty")
            if q not in (None, ""):
                filled_qty = max(filled_qty, _to_float(q, 0.0))

        first_fill = None
        valid_until = None
        signal_dt = None
        cancel_origin = ""
        for x in arr:
            if signal_dt is None:
                signal_dt = _parse_dt(x.get("signal_dt"))
            if valid_until is None:
                valid_until = _parse_dt(x.get("valid_until_dt"))
            if not cancel_origin and x.get("cancel_origin") is not None:
                cancel_origin = str(x.get("cancel_origin") or "")
            if str(x.get("event_type") or "") in ("fill", "partial_fill"):
                t = _parse_dt(x.get("ts_event_utc"))
                if t and (first_fill is None or t < first_fill):
                    first_fill = t

        fill_after_ttl = bool(first_fill and valid_until and first_fill > valid_until)
        has_terminal = any(e in ("fill", "partial_fill", "cancel", "reject") for e in evs) or any(s in ("filled", "partial", "canceled", "expired", "rejected") for s in status_vals)

        out[k] = {
            "audit_events": "|".join(evs),
            "audit_statuses": "|".join(status_vals),
            "audit_filled_qty": filled_qty,
            "audit_cancel_origin": cancel_origin,
            "audit_has_submit_accepted": "submit_accepted" in evs,
            "audit_has_ttl_expired": "ttl_expired" in evs,
            "audit_first_fill_ts": first_fill,
            "audit_valid_until": valid_until,
            "audit_fill_after_ttl": fill_after_ttl,
            "audit_has_terminal": has_terminal,
            "audit_signal_dt": signal_dt,
        }

    return out


def _classify(row: Dict[str, Any], qty_tol: float, price_tol: float) -> Tuple[str, str]:
    has_alpaca = bool((row.get("alpaca_order_id") or "").strip() or (row.get("alpaca_client_order_id") or "").strip())
    has_audit = bool((row.get("audit_events") or "").strip())

    if not has_alpaca and not has_audit:
        return "OK", "nessun match alpaca/audit per questo ordine"

    # explicit invariants first
    if not row.get("invariant_fill_monotonic", True):
        return "PARTIAL_MISAPPLIED", "paper exec_size non monotono (cumulativo incoerente)"
    if not row.get("invariant_is_close_coherent", True):
        return "CLOSE_PATH_ERROR", "is_close incoerente con segno posizione paper"

    pap_st = row.get("paper_status", "unknown")
    alp_st = row.get("alpaca_status", "unknown") if has_alpaca else "unknown"
    pap_q = _to_float(row.get("paper_filled_qty"), 0.0)
    alp_q = _to_float(row.get("alpaca_filled_qty"), 0.0)

    if row.get("audit_fill_after_ttl"):
        return "TTL_RACE", "fill dopo ttl_expired"

    if row.get("audit_has_ttl_expired") and alp_q > 0 and pap_st in ("canceled", "expired"):
        return "TTL_RACE", "ttl_expired + filled_qty broker > 0"

    if has_alpaca and abs(pap_q - alp_q) > qty_tol and max(pap_q, alp_q) > 0:
        return "PARTIAL_MISAPPLIED", "filled_qty paper != alpaca"

    terminals = {"filled", "partial", "canceled", "expired", "rejected"}
    if has_alpaca and alp_st in terminals and pap_st not in terminals:
        return "STATE_DIVERGENCE", "broker terminale non propagato a stato paper"

    if row.get("audit_has_submit_accepted") and row.get("audit_has_terminal") and pap_st in ("created", "accepted"):
        return "STATE_DIVERGENCE", "audit mostra lifecycle terminale ma paper resta open"

    if has_alpaca and pap_st != alp_st and not (pap_st == "partial" and alp_st == "filled" and alp_q > 0 and abs(pap_q - alp_q) <= qty_tol):
        return "STATE_DIVERGENCE", "status paper diverso da alpaca"

    pap_px = _to_float(row.get("paper_filled_avg_price"), 0.0)
    alp_px = _to_float(row.get("alpaca_filled_avg_price"), 0.0)
    if has_alpaca and pap_q > 0 and alp_q > 0 and abs(pap_q - alp_q) <= qty_tol and abs(pap_px - alp_px) > price_tol:
        return "LOG_ONLY", "qty coerente ma prezzo medio diverge (possibile errore log/report)"

    return "OK", ""


def run_forensic(
    paper_dir: Path,
    alpaca_dir: Optional[Path],
    audit_jsonl: Optional[Path],
    out_dir: Path,
    qty_tol: float,
    price_tol: float,
    verbose: bool,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    orderhistory_path = paper_dir / "orderhistory.json"
    if not orderhistory_path.exists():
        raise FileNotFoundError(f"Missing paper orderhistory: {orderhistory_path}")

    paper_rows = summarize_paper_orders(orderhistory_path)
    _log(f"Paper orders summarized: {len(paper_rows)}", verbose)

    alp_map: Dict[str, Dict[str, Any]] = {}
    if alpaca_dir:
        order_csv = alpaca_dir / "order_alpaca.csv"
        if order_csv.exists():
            alp_map = summarize_alpaca_orders(order_csv)
            _log(f"Alpaca orders summarized: {len(alp_map)}", verbose)
        else:
            _log(f"Alpaca order csv missing: {order_csv}", verbose)

    aud_map: Dict[str, Dict[str, Any]] = {}
    if audit_jsonl:
        if audit_jsonl.exists():
            aud_map = summarize_audit(audit_jsonl)
            _log(f"Audit orders summarized: {len(aud_map)}", verbose)
        else:
            _log(f"Audit jsonl missing: {audit_jsonl}", verbose)

    merged = []
    for r in paper_rows:
        key = str(r.get("order_key") or "unknown")
        a = alp_map.get(key, {})
        u = aud_map.get(key, {})
        x = dict(r)
        x.update(a)
        x.update(u)

        if "alpaca_status" not in x:
            x["alpaca_status"] = "unknown"
            x["alpaca_filled_qty"] = 0.0
            x["alpaca_filled_avg_price"] = 0.0

        cls, reason = _classify(x, qty_tol=qty_tol, price_tol=price_tol)
        x["classification"] = cls
        x["classification_reason"] = reason

        # invariant: terminal propagated
        pap_st = x.get("paper_status", "unknown")
        alp_st = x.get("alpaca_status", "unknown")
        terminals = {"filled", "partial", "canceled", "expired", "rejected"}
        x["invariant_terminal_propagated"] = not (alp_st in terminals and pap_st not in terminals)

        # format datetime fields
        for kdt in (
            "paper_signal_dt", "paper_datetime", "paper_first_exec_dt", "paper_last_exec_dt",
            "alpaca_submitted_at", "alpaca_filled_at", "alpaca_updated_at",
            "audit_first_fill_ts", "audit_valid_until", "audit_signal_dt",
        ):
            if isinstance(x.get(kdt), datetime):
                x[kdt] = _fmt_dt(x[kdt])

        merged.append(x)

    merged.sort(key=lambda z: (_parse_dt(z.get("paper_datetime")) or datetime.max.replace(tzinfo=UTC), z.get("asset") or "", z.get("order_key") or ""))

    headers = [
        "order_key", "paper_order_id", "asset", "side", "paper_is_close",
        "paper_status", "alpaca_status", "paper_filled_qty", "alpaca_filled_qty",
        "paper_filled_avg_price", "alpaca_filled_avg_price",
        "paper_signal_dt", "paper_datetime", "paper_first_exec_dt", "paper_last_exec_dt",
        "alpaca_order_id", "alpaca_client_order_id", "alpaca_submitted_at", "alpaca_filled_at", "alpaca_updated_at",
        "audit_events", "audit_statuses", "audit_filled_qty", "audit_cancel_origin", "audit_has_submit_accepted",
        "audit_has_ttl_expired", "audit_fill_after_ttl", "audit_first_fill_ts", "audit_valid_until",
        "invariant_fill_monotonic", "invariant_terminal_propagated", "invariant_is_close_coherent",
        "classification", "classification_reason",
    ]

    orders_csv = out_dir / "forensic_orders.csv"
    with orders_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in merged:
            w.writerow({h: r.get(h, "") for h in headers})

    anomalies = [r for r in merged if r.get("classification") != "OK" or not r.get("invariant_fill_monotonic", True) or not r.get("invariant_terminal_propagated", True) or not r.get("invariant_is_close_coherent", True)]
    anomalies_csv = out_dir / "forensic_anomalies.csv"
    with anomalies_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in anomalies:
            w.writerow({h: r.get(h, "") for h in headers})

    class_counts = Counter(r.get("classification") for r in anomalies)
    matched_alpaca = sum(1 for r in merged if (r.get("alpaca_order_id") or r.get("alpaca_client_order_id")))
    matched_audit = sum(1 for r in merged if (r.get("audit_events") or ""))
    inv_counts = {
        "fill_monotonic_false": sum(1 for r in merged if not r.get("invariant_fill_monotonic", True)),
        "terminal_propagation_false": sum(1 for r in merged if not r.get("invariant_terminal_propagated", True)),
        "is_close_coherent_false": sum(1 for r in merged if not r.get("invariant_is_close_coherent", True)),
        "fill_after_ttl_true": sum(1 for r in merged if bool(r.get("audit_fill_after_ttl"))),
    }

    if anomalies:
        if alp_map and aud_map:
            confidence = "high"
        elif alp_map:
            confidence = "medium"
        else:
            confidence = "low"
    else:
        confidence = "low"

    summary = {
        "paper_dir": str(paper_dir),
        "alpaca_dir": str(alpaca_dir) if alpaca_dir else "",
        "audit_jsonl": str(audit_jsonl) if audit_jsonl else "",
        "rows_total": len(merged),
        "rows_with_alpaca_match": matched_alpaca,
        "rows_with_audit_match": matched_audit,
        "anomalies_total": len(anomalies),
        "classification_counts": dict(class_counts),
        "invariant_counts": inv_counts,
        "confidence": confidence,
        "verdict": "anomaly_detected" if anomalies else "no_anomaly_detected",
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
    }

    (out_dir / "forensic_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")

    return summary


def _find_latest_jsonl() -> Optional[Path]:
    base = Path("logs") / "execution_audit"
    if not base.exists():
        return None
    cands = sorted(base.rglob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None


def _find_latest_alpaca_reconcile_dir() -> Optional[Path]:
    base = Path("out") / "reconcile" / "alpaca"
    if not base.exists():
        return None
    dirs = [p for p in base.iterdir() if p.is_dir()]
    dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return dirs[0] if dirs else None


def main() -> int:
    args = parse_args()

    paper_dir = Path(args.paper_dir).resolve()
    alpaca_dir = Path(args.alpaca_dir).resolve() if args.alpaca_dir else _find_latest_alpaca_reconcile_dir()
    audit_jsonl = Path(args.audit_jsonl).resolve() if args.audit_jsonl else _find_latest_jsonl()

    ts = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).resolve() if args.out_dir else (Path("out") / "reconcile" / "forensic" / ts).resolve()

    summary = run_forensic(
        paper_dir=paper_dir,
        alpaca_dir=alpaca_dir,
        audit_jsonl=audit_jsonl,
        out_dir=out_dir,
        qty_tol=float(args.qty_tol),
        price_tol=float(args.price_tol),
        verbose=bool(args.verbose),
    )

    print("=== Forensic Reconcile ===")
    print(f"Out dir                 : {out_dir}")
    print(f"Rows total              : {summary['rows_total']}")
    print(f"Anomalies total         : {summary['anomalies_total']}")
    print(f"Classification counts   : {summary['classification_counts']}")
    print(f"Invariant counts        : {summary['invariant_counts']}")
    print(f"Confidence              : {summary['confidence']}")
    print(f"Verdict                 : {summary['verdict']}")
    if summary.get("audit_jsonl"):
        print(f"Audit source            : {summary['audit_jsonl']}")
    if summary.get("alpaca_dir"):
        print(f"Alpaca source           : {summary['alpaca_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
