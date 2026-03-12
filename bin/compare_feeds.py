#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Confronta riga-per-riga i dati provenienti da due cartelle (es. Yahoo vs Alpaca).
Supporta normalizzazione timezone (default UTC) e allineamento per DATA (default) o TIMESTAMP.

Esempio d'uso:
python compare_feeds.py --folder-a ./data/yahoo --folder-b ./data/alpaca --start 2016-01-01 --end 2016-12-31 --tickers AAPL --align date --tz UTC --atol 1e-8 --rtol 1e-6
"""

import argparse
import os
import sys
import pandas as pd
import numpy as np
from typing import List, Tuple, Optional

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Confronta CSV in due cartelle per ticker, su range date e colonne comuni.")
    p.add_argument("--folder-a", required=True, help="Cartella dati A (es. Yahoo)")
    p.add_argument("--folder-b", required=True, help="Cartella dati B (es. Alpaca)")
    p.add_argument("--start", required=True, help="Data inizio (YYYY-MM-DD) inclusa")
    p.add_argument("--end", required=True, help="Data fine (YYYY-MM-DD) inclusa")
    p.add_argument("--tickers", default="", help="Lista di tickers separati da virgola (opzionale). Se vuoto, usa intersezione dei file .csv presenti in entrambe le cartelle.")
    p.add_argument("--atol", type=float, default=0.0, help="Tolleranza assoluta per confronto numerico (default 0.0)")
    p.add_argument("--rtol", type=float, default=0.0, help="Tolleranza relativa per confronto numerico (default 0.0)")
    p.add_argument("--date-col", default=None, help="Nome della colonna datetime se NON è l'indice (default: usa prima colonna come indice). Se impostata, verrà usata come index_col.")
    p.add_argument("--output", default="./out/diff_reports", help="Cartella in cui salvare i CSV di differenze per ogni ticker.")
    p.add_argument("--align", choices=["date", "timestamp"], default="date", help="Allineamento su 'date' (solo giorno) o 'timestamp' (giorno+ora). Default: date.")
    p.add_argument("--tz", default="UTC", help="Timezone a cui convertire gli indici tz-aware prima di rimuovere la tz. Default: UTC.")
    p.add_argument("--ignore-volume", action="store_true",
               help="Esclude la colonna Volume dal confronto.")
    return p.parse_args()

def read_csv_standardized(path: str, date_col: Optional[str], tz_target: str, align: str) -> pd.DataFrame:
    if not os.path.exists(path) or os.stat(path).st_size == 0:
        raise FileNotFoundError(f"File mancante o vuoto: {path}")

    if date_col:
        df = pd.read_csv(path, parse_dates=[date_col])
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.set_index(date_col)
    else:
        df = pd.read_csv(path, parse_dates=True, index_col=0)

    df.columns = [str(c).strip().title() for c in df.columns]
    df = df.dropna(axis=1, how='all')

    if not pd.api.types.is_datetime64_any_dtype(df.index):
        try:
            df.index = pd.to_datetime(df.index, errors='coerce')
        except Exception:
            pass
    df = df[~df.index.isna()]
    df = df.sort_index()

    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(tz_target).tz_localize(None)

    if align == "date":
        df.index = df.index.normalize()

    return df

def filter_date_range(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end) + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    return df.loc[(df.index >= start_dt) & (df.index <= end_dt)]

def align_on_common(df_a: pd.DataFrame, df_b: pd.DataFrame, args) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], pd.DatetimeIndex]:
    cols = sorted(list(set(df_a.columns).intersection(set(df_b.columns))))
    if getattr(args, "ignore-volume", True):
        cols = [c for c in cols if c.lower() != "volume"]
    dates = df_a.index.intersection(df_b.index)
    df_a_al = df_a.loc[dates, cols].sort_index()
    df_b_al = df_b.loc[dates, cols].sort_index()
    return df_a_al, df_b_al, cols, dates

def compare_frames(df_a: pd.DataFrame, df_b: pd.DataFrame, cols: List[str], atol: float, rtol: float) -> pd.DataFrame:
    diffs = []
    for col in cols:
        a = df_a[col]
        b = df_b[col]
        a, b = a.align(b)
        is_num = pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b)

        if is_num:
            equal_mask = pd.Series(
                np.isclose(a.astype(float).values, b.astype(float).values, atol=atol, rtol=rtol, equal_nan=True),
                index=a.index
            )
            abs_diff = (a - b).abs()
            with np.errstate(divide='ignore', invalid='ignore'):
                rel_diff = abs_diff / np.where(np.abs(b) > 0, np.abs(b), np.nan)
                rel_diff = pd.Series(rel_diff, index=a.index)
        else:
            equal_mask = (a.astype(object).fillna("__NaN__") == b.astype(object).fillna("__NaN__"))
            abs_diff = pd.Series([np.nan]*len(a), index=a.index)
            rel_diff = pd.Series([np.nan]*len(a), index=a.index)

        for dt in equal_mask.index[~equal_mask]:
            diffs.append({
                "Date": dt,
                "Column": col,
                "Value_A": a.loc[dt],
                "Value_B": b.loc[dt],
                "Abs_Diff": abs_diff.loc[dt] if is_num else np.nan,
                "Rel_Diff": rel_diff.loc[dt] if is_num else np.nan,
                "Equal": False
            })

    diff_df = pd.DataFrame(diffs)
    if not diff_df.empty:
        diff_df = diff_df.sort_values(["Date","Column"]).reset_index(drop=True)
    else:
        diff_df = pd.DataFrame(columns=["Date","Column","Value_A","Value_B","Abs_Diff","Rel_Diff","Equal"])
    return diff_df

def find_missing_dates(df_a: pd.DataFrame, df_b: pd.DataFrame):
    missing_in_b = df_a.index.difference(df_b.index)
    missing_in_a = df_b.index.difference(df_a.index)
    return missing_in_b, missing_in_a

def discover_tickers(folder_a: str, folder_b: str):
    files_a = {os.path.splitext(f)[0] for f in os.listdir(folder_a) if f.lower().endswith(".csv")}
    files_b = {os.path.splitext(f)[0] for f in os.listdir(folder_b) if f.lower().endswith(".csv")}
    return sorted(list(files_a.intersection(files_b)))

def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

def main():
    args = parse_args()
    ensure_outdir(args.output)

    if args.tickers.strip():
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = discover_tickers(args.folder_a, args.folder_b)
        if not tickers:
            print("Nessun ticker comune trovato tra le due cartelle.", file=sys.stderr)
            sys.exit(2)

    print(f"Trovati {len(tickers)} ticker da confrontare.\n")

    summary_rows = []

    for ticker in tickers:
        path_a = os.path.join(args.folder_a, f"{ticker}.csv")
        path_b = os.path.join(args.folder_b, f"{ticker}.csv")

        try:
            df_a = read_csv_standardized(path_a, args.date_col, args.tz, args.align)
            df_b = read_csv_standardized(path_b, args.date_col, args.tz, args.align)

            df_a = filter_date_range(df_a, args.start, args.end)
            df_b = filter_date_range(df_b, args.start, args.end)

            miss_in_b, miss_in_a = find_missing_dates(df_a, df_b)

            df_a_al, df_b_al, cols, dates = align_on_common(df_a, df_b, args)

            if not cols:
                print(f"[{ticker}] Nessuna colonna in comune dopo normalizzazione. Skip.\n")
                summary_rows.append((ticker, 0, len(miss_in_b), len(miss_in_a), 0))
                continue
            if dates.empty:
                print(f"[{ticker}] Nessuna data in comune nel range. Skip.\n")
                summary_rows.append((ticker, len(cols), len(miss_in_b), len(miss_in_a), 0))
                continue

            diff_df = compare_frames(df_a_al, df_b_al, cols, atol=args.atol, rtol=args.rtol)

            print(f"[{ticker}] Colonne comuni: {cols}")
            if len(miss_in_b) > 0:
                print(f"  Date presenti in A ma NON in B: {len(miss_in_b)} (es. {miss_in_b.min()} … {miss_in_b.max()})")
            if len(miss_in_a) > 0:
                print(f"  Date presenti in B ma NON in A: {len(miss_in_a)} (es. {miss_in_a.min()} … {miss_in_a.max()})")

            if diff_df.empty:
                print(f"  Nessuna differenza sui valori (entro tol: atol={args.atol}, rtol={args.rtol}).\n")
            else:
                out_path = os.path.join(args.output, f"{ticker}_diff.csv")
                df_to_save = diff_df.copy()
                df_to_save["Date"] = pd.to_datetime(df_to_save["Date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
                df_to_save.to_csv(out_path, index=False)
                print(f"  Differenze trovate: {len(diff_df)} righe. Salvato: {out_path}\n")

            summary_rows.append((ticker, len(cols), len(miss_in_b), len(miss_in_a), 0 if diff_df.empty else len(diff_df)))

        except Exception as e:
            print(f"[{ticker}] ERRORE: {e}", file=sys.stderr)
            summary_rows.append((ticker, 0, 0, 0, -1))

    summary = pd.DataFrame(summary_rows, columns=["Ticker","Common_Cols","Dates_A_NotIn_B","Dates_B_NotIn_A","Diff_Rows"])
    print("\n=== RIEPILOGO ===")
    print(summary.to_string(index=False))

    summary_path = os.path.join(args.output, "summary.csv")
    summary.to_csv(summary_path, index=False)
    print(f"\nRiepilogo salvato in: {summary_path}")

if __name__ == "__main__":
    main()
