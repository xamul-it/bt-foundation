#!/usr/bin/env python3
"""
Simula l'impatto della leva sul portafoglio.

Due scenari a confronto:
  A)  Con leva (reale, paper) — nessun costo finanziario
  A1) Con leva + interessi simulati sul margin loan
  B)  Senza leva — stessi ordini scalati per non superare la cassa

Usage:
    python bin/alpaca_audit/leverage_simulation.py --account pa1
    python bin/alpaca_audit/leverage_simulation.py --account pa1 --rate 0.12
"""
import argparse
import json
from pathlib import Path
from collections import defaultdict

import pandas as pd
import numpy as np

BASE_OUT = Path(__file__).parent / "out"


def load_data(account: str):
    out = BASE_OUT / account
    orders = pd.read_csv(out / "orders.csv", parse_dates=["created_at", "filled_at"])
    hist   = pd.read_csv(out / "portfolio_history.csv", parse_dates=["date"])
    pos    = pd.read_csv(out / "positions.csv")
    with open(out / "account_snapshot.json") as f:
        acct = json.load(f)
    return orders, hist, pos, acct


def get_filled(orders: pd.DataFrame) -> pd.DataFrame:
    filled = orders[orders["status"] == "filled"].copy()
    filled["exec_dt"] = filled["filled_at"].fillna(filled["created_at"])
    # normalizza a tz-naive per confronto con portfolio_history
    filled["exec_dt"] = pd.to_datetime(filled["exec_dt"]).dt.tz_convert(None)
    filled["exec_date"] = filled["exec_dt"].dt.normalize()
    filled = filled.sort_values("exec_dt").reset_index(drop=True)
    filled["notional"] = filled["filled_qty"].astype(float) * filled["filled_avg_price"].astype(float)
    return filled


def simulate(filled: pd.DataFrame, initial_equity: float):
    """
    Ricostruisce due traiettorie di cassa e posizioni:
      actual    — cassa reale (negativa = margin loan)
      unlevered — cassa vincolata >= 0, acquisti scalati
    Ritorna: DataFrame timeline, stato finale qty, cash
    """
    cash_act   = initial_equity
    cash_unlev = initial_equity
    qty_act    = defaultdict(float)
    qty_unlev  = defaultdict(float)
    rows = []

    for _, o in filled.iterrows():
        sym      = o["symbol"]
        side     = o["side"]
        qty      = float(o["filled_qty"])
        price    = float(o["filled_avg_price"])
        notional = qty * price

        if side == "buy":
            # actual
            cash_act -= notional
            qty_act[sym] += qty
            # unlevered: acquisto scalato alla cassa disponibile
            avail = max(0.0, cash_unlev)
            scale = min(1.0, avail / notional) if notional > 0 else 0.0
            cash_unlev -= notional * scale
            qty_unlev[sym] += qty * scale

        else:  # sell
            # actual
            qty_act_before = qty_act[sym]  # prima di aggiornare
            cash_act      += qty * price
            qty_act[sym]  -= qty
            if qty_act[sym] < 1e-6:
                qty_act[sym] = 0.0

            # unlevered: vendi in proporzione a ciò che abbiamo
            ratio = (qty_unlev[sym] / qty_act_before) if qty_act_before > 1e-6 else 0.0
            sold_unlev     = qty * ratio
            cash_unlev    += sold_unlev * price
            qty_unlev[sym] -= sold_unlev
            if qty_unlev[sym] < 1e-6:
                qty_unlev[sym] = 0.0

        rows.append({
            "exec_date":   o["exec_date"],
            "exec_dt":     o["exec_dt"],
            "symbol":      sym,
            "side":        side,
            "qty":         qty,
            "price":       price,
            "notional":    notional,
            "cash_act":    cash_act,
            "cash_unlev":  cash_unlev,
            "margin_loan": max(0.0, -cash_act),
        })

    tl = pd.DataFrame(rows)
    return tl, dict(qty_act), dict(qty_unlev), cash_act, cash_unlev


def build_daily_loan(timeline: pd.DataFrame, market_days: pd.DatetimeIndex) -> pd.Series:
    """
    Espande il margin loan a ogni giorno di mercato.
    Tra due ordini, il loan rimane costante (cassa non si muove senza ordini).
    """
    # prendi l'ultimo stato di ogni giorno di operatività
    by_day = timeline.groupby("exec_date")["margin_loan"].last()
    # reindex su tutti i giorni di mercato, forward-fill
    loan_series = by_day.reindex(market_days).ffill().fillna(0.0)
    return loan_series


def calc_interest_on_loan(loan_series: pd.Series, rate: float) -> tuple:
    daily_rate   = rate / 252
    daily_int    = loan_series * daily_rate
    cum_int      = daily_int.cumsum()
    total_int    = daily_int.sum()
    return daily_int, cum_int, total_int


def unlevered_portfolio_value(qty_unlev: dict, cash_unlev: float,
                              positions: pd.DataFrame) -> float:
    pos_price = {}
    if not positions.empty:
        pos_price = dict(zip(positions["symbol"],
                             positions["current_price"].astype(float)))
    value = cash_unlev
    for sym, q in qty_unlev.items():
        if q > 1e-4 and sym in pos_price:
            value += q * pos_price[sym]
    return value


def section(title: str):
    print(f"\n{'='*65}")
    print(f" {title}")
    print('='*65)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default="default")
    parser.add_argument("--rate", type=float, default=0.10,
                        help="Tasso annuo margin loan di default (default 0.10 = 10%%)")
    args = parser.parse_args()

    orders, hist, positions, acct = load_data(args.account)
    filled = get_filled(orders)

    hist_valid = hist[hist["equity"] > 0].sort_values("date").copy()
    hist_valid["date"] = hist_valid["date"].dt.normalize()
    market_days = pd.DatetimeIndex(hist_valid["date"])

    initial_equity = float(hist_valid["equity"].iloc[0])
    current_equity = float(acct["equity"])
    actual_return  = (current_equity / initial_equity - 1) * 100

    label = args.account.upper()
    print(f"\n{'='*65}")
    print(f" ALPACA {label} — SIMULAZIONE LEVA vs CAPITALE PROPRIO")
    print(f" Tasso margin loan (simulato): {args.rate*100:.1f}% annuo")
    print('='*65)
    print(f"\n  Equity iniziale : {initial_equity:>10,.2f} USD")
    print(f"  Equity attuale  : {current_equity:>10,.2f} USD")
    print(f"  Periodo         : {hist_valid['date'].iloc[0].date()} → {hist_valid['date'].iloc[-1].date()}")

    # --- simulazione ---
    timeline, qty_act_final, qty_unlev_final, cash_act_final, cash_unlev_final = \
        simulate(filled, initial_equity)

    loan_series = build_daily_loan(timeline, market_days)

    # --- Scenario unlevered ---
    port_unlev    = unlevered_portfolio_value(qty_unlev_final, cash_unlev_final, positions)
    return_unlev  = (port_unlev / initial_equity - 1) * 100

    section("RIEPILOGO CONFRONTO SCENARI")
    print(f"\n  {'SCENARIO':<42} {'VAL.FINALE':>12} {'RETURN':>9}")
    print(f"  {'-'*65}")
    print(f"  {'A)  Con leva — paper (no interessi)':<42} {current_equity:>12,.2f} {actual_return:>+8.2f}%")

    rates_to_show = [0.05, 0.08, args.rate, 0.12, 0.15]
    rates_to_show = sorted(set(rates_to_show))
    first_custom = True
    for r in rates_to_show:
        _, _, total_int = calc_interest_on_loan(loan_series, r)
        eq_r   = current_equity - total_int
        ret_r  = (eq_r / initial_equity - 1) * 100
        marker = " ← tasso scelto" if r == args.rate and first_custom else ""
        if r == args.rate:
            first_custom = False
        label_r = f"A1) Con leva + interessi {r*100:.0f}%/a"
        print(f"  {label_r:<42} {eq_r:>12,.2f} {ret_r:>+8.2f}%{marker}")

    print(f"  {'B)  Senza leva (solo capitale proprio)':<42} {port_unlev:>12,.2f} {return_unlev:>+8.2f}%")

    section("DETTAGLIO MARGIN LOAN")

    snapshot_loan = abs(float(acct["cash"])) if float(acct["cash"]) < 0 else 0.0
    days_with_loan = int((loan_series > 0).sum())
    avg_loan = loan_series[loan_series > 0].mean() if days_with_loan > 0 else 0.0
    max_loan = loan_series.max()
    max_loan_date = loan_series.idxmax() if max_loan > 0 else None

    print(f"\n  Margin loan snapshot (oggi):       {snapshot_loan:>10,.2f} USD")
    print(f"  Giorni con margin loan > 0:        {days_with_loan:>10}")
    if days_with_loan > 0:
        print(f"  Loan medio (giorni attivi):        {avg_loan:>10,.2f} USD")
        print(f"  Loan massimo:                      {max_loan:>10,.2f} USD"
              + (f"  (il {max_loan_date.date()})" if max_loan_date is not None else ""))
    else:
        print("  Il margin loan ricostruito dagli ordini è 0.")
        print("  NOTA: il loan attuale ($" + f"{snapshot_loan:,.0f})" +
              " nasce dall'apprezzamento delle")
        print("  posizioni oltre l'equity iniziale — non da acquisti che superavano")
        print("  la cassa al momento dell'ordine. Vedi spiegazione sotto.")

    section("SPIEGAZIONE: come si è formato il margin loan")
    print("""
  Il margin loan di PA1 NON nasce da acquisti 'scoperti' al momento dell'ordine.
  Tutte le posizioni sono state aperte quando la cassa era ancora sufficiente.

  Il debito si è formato per APPREZZAMENTO delle posizioni:
    1. Hai comprato titoli per ~$150k (entro la cassa disponibile man mano).
    2. I titoli sono saliti (AMD +96%, MU +44%, CSCO +48%, INTC +33%).
    3. Il long_market_value è diventato $196k.
    4. Ma la cassa non è aumentata di pari passo (resta legata ai soli
       acquisti/vendite in contanti).

  Risultato:
    equity          = long_market_value + cash
    84,273          = 196,402 + (−112,129)

  Il "cash negativo" è la controparte contabile dell'apprezzamento non realizzato:
  il broker ha in pegno titoli che valgono più di quanto inizialmente prestato.
  Non è un prestito esplicito — è il bilanciamento contabile del conto margin.

  In live trading, Alpaca applica gli interessi sull'importo NETTO negativo
  (cioè $112k), indipendentemente da come ci si è arrivati.
    """.strip())

    section("INTERESSI SIMULATI (applicati al loan attuale)")
    print(f"\n  Margin loan corrente su cui si applicano gli interessi: {snapshot_loan:,.2f} USD")
    print(f"  (Ipotesi: loan costante da oggi in poi — scenario prospettico)")
    print()
    print(f"  {'Tasso':>7}  {'Interesse/giorno':>17}  {'Interesse/mese':>15}  {'Interesse/anno':>15}")
    for r in [0.05, 0.08, 0.10, 0.12, 0.15]:
        daily  = snapshot_loan * r / 252
        monthly = snapshot_loan * r / 12
        annual  = snapshot_loan * r
        print(f"  {r*100:>6.0f}%  {daily:>17,.2f}  {monthly:>15,.2f}  {annual:>15,.2f}")

    print()
    print(f"  Se il loan restasse a {snapshot_loan:,.0f} USD per un anno intero:")
    _, _, total_1y_default = calc_interest_on_loan(
        pd.Series([snapshot_loan] * 252), args.rate)
    eq_after_1y = current_equity - total_1y_default
    print(f"    tasso {args.rate*100:.0f}% → interessi = {total_1y_default:,.2f} USD")
    print(f"    equity netta = {eq_after_1y:,.2f} USD  "
          f"(vs {current_equity:,.2f} attuali)")

    section("SCENARIO B — SENZA LEVA: DETTAGLIO POSIZIONI")
    pos_price = dict(zip(positions["symbol"], positions["current_price"].astype(float))) \
        if not positions.empty else {}

    print(f"\n  Cassa finale (unlevered):          {cash_unlev_final:>10,.2f} USD")
    print(f"  Posizioni aperte (unlevered):")
    pos_value = 0.0
    for sym, q in sorted(qty_unlev_final.items(), key=lambda x: -x[1]):
        if q > 1e-4:
            p_now = pos_price.get(sym)
            mkt   = q * p_now if p_now else 0.0
            pos_value += mkt
            scale_vs_actual = q / qty_act_final.get(sym, q) * 100 if qty_act_final.get(sym, 0) > 0 else 100
            p_str = f"{p_now:.2f}" if p_now else "?"
            print(f"    {sym:<8}  qty={q:>8.2f}  ({scale_vs_actual:.0f}% del reale)"
                  f"  prezzo={p_str:>8}  valore={mkt:>10,.2f}")
    print(f"  Totale posizioni:                  {pos_value:>10,.2f} USD")
    print(f"  Portafoglio totale (B):            {port_unlev:>10,.2f} USD  ({return_unlev:>+.2f}%)")

    section("CONCLUSIONI")
    _, _, total_int_default = calc_interest_on_loan(loan_series, args.rate)
    # interesse prospettico: se il loan restasse com'è per il resto dell'anno
    # (non calcolabile esattamente senza futuro — usiamo solo il retrospettivo)
    gain_vs_unlev  = actual_return - return_unlev
    net_after_cost = actual_return - (snapshot_loan * args.rate / initial_equity * 100)

    print(f"""
  Return con leva (reale, paper):          {actual_return:>+7.2f}%
  Return senza leva:                       {return_unlev:>+7.2f}%
  Vantaggio lordo della leva:              {gain_vs_unlev:>+7.2f}%

  Il loan attuale di {snapshot_loan:,.0f} USD al {args.rate*100:.0f}% annuo costa:
    {snapshot_loan*args.rate:>8,.0f} USD/anno  =  {snapshot_loan*args.rate/initial_equity*100:>+.2f}% sull'equity iniziale

  Il portafoglio dovrà crescere di almeno {snapshot_loan*args.rate/current_equity*100:.2f}%/anno
  per coprire il costo del debito.

  Oggi AMD, MU, CSCO, INTC hanno guadagnato ben oltre il break-even
  del prestito — la leva ha AMPLIFICATO i guadagni.
  Ma il rischio è simmetrico: se i titoli scendono, le perdite
  si amplificano nello stesso modo.
    """.strip())
    print()


if __name__ == "__main__":
    main()
