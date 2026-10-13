#!/usr/bin/env python3
"""
Analizza i dati scaricati da fetch_data.py e produce un report testuale.

Usage:
    python bin/alpaca_audit/analyze.py

Legge i file in bin/alpaca_audit/out/ prodotti da fetch_data.py.
"""
import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np

BASE_OUT = Path(__file__).parent / "out"

NEGATIVE_CASH_EXPLANATION = """
CASH NEGATIVO — Spiegazione:
  In un conto margin Alpaca, il cash può diventare negativo quando
  si acquistano più titoli di quanto si abbia liquidità.
  Alpaca presta la differenza (margin loan).
  Questo NON è un errore — è debito verso il broker.

  Formule chiave:
    equity = long_market_value + short_market_value + cash
    buying_power = equity * multiplier - initial_margin_required

  Se equity scende abbastanza → margin call → posizioni liquidate.
""".strip()

NEGATIVE_BP_EXPLANATION = """
BUYING POWER NEGATIVO — Spiegazione:
  Buying power = (equity × multiplier) − margine richiesto.
  Se le posizioni richiedono più margine di quanto l'equity moltiplicata
  consenta, il buying power diventa negativo.
  In paper trading, Alpaca non esegue la margin call automaticamente
  come farebbe in live, quindi la situazione può persistere.
""".strip()


def load_data(out_dir: Path):
    errors = []
    hist, orders, positions, acct = None, None, None, None

    hist_path = out_dir / "portfolio_history.csv"
    orders_path = out_dir / "orders.csv"
    pos_path = out_dir / "positions.csv"
    acct_path = out_dir / "account_snapshot.json"

    if hist_path.exists():
        hist = pd.read_csv(hist_path, parse_dates=["date"])
    else:
        errors.append(f"Manca {hist_path}")

    if orders_path.exists():
        orders = pd.read_csv(orders_path, parse_dates=["created_at", "filled_at"])
    else:
        errors.append(f"Manca {orders_path}")

    if pos_path.exists():
        try:
            positions = pd.read_csv(pos_path)
        except Exception:
            positions = pd.DataFrame()
    else:
        errors.append(f"Manca {pos_path}")

    if acct_path.exists():
        with open(acct_path) as f:
            acct = json.load(f)
    else:
        errors.append(f"Manca {acct_path}")

    if errors:
        print("ERRORE: file mancanti. Esegui prima fetch_data.py")
        for e in errors:
            print(f"  {e}")
        sys.exit(1)

    return hist, orders, positions, acct


def section(title):
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)


def analyze_account(acct):
    section("STATO CONTO (SNAPSHOT)")
    print(f"  Equity:          {acct['equity']:>12,.2f} USD")
    print(f"  Cash:            {acct['cash']:>12,.2f} USD")
    print(f"  Buying power:    {acct['buying_power']:>12,.2f} USD")
    print(f"  Long mkt value:  {acct['long_market_value']:>12,.2f} USD")
    print(f"  Short mkt value: {acct['short_market_value']:>12,.2f} USD")
    print(f"  Portfolio value: {acct['portfolio_value']:>12,.2f} USD")
    print(f"  Init margin:     {acct['initial_margin']:>12,.2f} USD")
    print(f"  Maint margin:    {acct['maintenance_margin']:>12,.2f} USD")
    print(f"  Last equity:     {acct['last_equity']:>12,.2f} USD")
    print(f"  Multiplier:      {acct['multiplier']}")
    print(f"  PDT:             {acct['pattern_day_trader']}")
    print(f"  Daytrade count:  {acct['daytrade_count']}")
    print(f"  Snapshot:        {acct['as_of']}")

    alerts = []
    if acct["cash"] < 0:
        loan = abs(acct["cash"])
        equity = acct["equity"]
        leverage = (equity + loan) / equity if equity > 0 else float("inf")
        alerts.append(f"⚠️  CASH NEGATIVO: −{loan:,.2f} USD (margin loan stimato)")
        alerts.append(f"   Leva implicita: {leverage:.2f}x")
        alerts.append("")
        alerts.append(NEGATIVE_CASH_EXPLANATION)

    if acct["buying_power"] < 0:
        alerts.append(f"\n⚠️  BUYING POWER NEGATIVO: {acct['buying_power']:,.2f} USD")
        alerts.append(NEGATIVE_BP_EXPLANATION)

    if alerts:
        print()
        for a in alerts:
            print(a)


def analyze_history(hist):
    section("PORTFOLIO HISTORY — ANALISI EQUITY CURVE")

    hist = hist.dropna(subset=["equity"]).copy()
    if hist.empty:
        print("Nessun dato di history disponibile.")
        return

    hist = hist.sort_values("date").reset_index(drop=True)

    # Rimuovi righe con equity=0 (mercato chiuso / festività)
    valid = hist[hist["equity"] > 0].copy()

    print(f"\n  Periodo: {valid['date'].iloc[0]} → {valid['date'].iloc[-1]}")
    print(f"  Giorni di dati: {len(valid)}")
    print(f"  Equity iniziale: {valid['equity'].iloc[0]:>10,.2f}")
    print(f"  Equity finale:   {valid['equity'].iloc[-1]:>10,.2f}")

    total_return = (valid["equity"].iloc[-1] / valid["equity"].iloc[0] - 1) * 100
    print(f"  Return totale:   {total_return:>+.2f}%")

    # Drawdown
    running_max = valid["equity"].cummax()
    drawdown = (valid["equity"] - running_max) / running_max * 100
    max_dd = drawdown.min()
    max_dd_date = valid.loc[drawdown.idxmin(), "date"]
    print(f"  Max drawdown:    {max_dd:>+.2f}% (il {max_dd_date})")

    # Giorni con variazione estrema
    section("GIORNI CON VARIAZIONE ANOMALA (|pnl%| > 5%)")
    pnl_col = "profit_loss_pct"
    if pnl_col in valid.columns:
        anomalies = valid[valid[pnl_col].abs() > 0.05].copy()
        if anomalies.empty:
            print("  Nessuna giornata con variazione > 5%")
        else:
            for _, row in anomalies.iterrows():
                flag = "🔴" if row[pnl_col] < -0.10 else ("🟡" if row[pnl_col] < -0.05 else "🟢")
                print(f"  {flag} {row['date']}  equity={row['equity']:>10,.2f}  "
                      f"P&L={row['profit_loss']:>+10,.2f}  P&L%={row[pnl_col]*100:>+7.2f}%")

    # Giorni con -40%+ (il problema segnalato)
    section("GIORNI CON CROLLO ≥ -40% (problema segnalato)")
    if pnl_col in valid.columns:
        bad = valid[valid[pnl_col] <= -0.40]
        if bad.empty:
            print("  Nessun giorno con -40% o peggio nel periodo scaricato.")
            print("  (Potrebbe essere avvenuto prima del periodo coperto, o è un artefatto UI)")
        else:
            for _, row in bad.iterrows():
                print(f"  ❌ {row['date']}  equity={row['equity']:>10,.2f}  "
                      f"P&L={row['profit_loss']:>+10,.2f}  P&L%={row[pnl_col]*100:>+7.2f}%")
            print()
            print("  POSSIBILI CAUSE di un -40% anomalo:")
            print("  1. Paper trading glitch: Alpaca paper può resettare equity erraticamente")
            print("  2. Posizione con alta leva liquidata (margin call simulata)")
            print("  3. Short squeeze su posizione short non coperta")
            print("  4. Artefatto del calcolo pnl_reset='per_day': il confronto base")
            print("     è l'equity di APERTURA della giornata, non quella del giorno prima")


def analyze_orders(orders):
    section("ORDINI — RIEPILOGO")
    if orders is None or orders.empty:
        print("  Nessun ordine nel periodo.")
        return

    print(f"  Totale ordini: {len(orders)}")
    print(f"  Periodo: {orders['created_at'].min()} → {orders['created_at'].max()}")

    # Distribuzione per status
    print("\n  Per status:")
    for status, cnt in orders["status"].value_counts().items():
        print(f"    {status:<20} {cnt}")

    # Distribuzione per simbolo (filled only)
    filled = orders[orders["status"] == "filled"]
    print(f"\n  Ordini eseguiti: {len(filled)}")
    if not filled.empty:
        print("\n  Top simboli (eseguiti):")
        for sym, cnt in filled["symbol"].value_counts().head(15).items():
            print(f"    {sym:<10} {cnt} ordini")

        # Volume nozionale approssimativo
        filled_val = filled.copy()
        filled_val["notional"] = filled_val["filled_qty"] * filled_val["filled_avg_price"]
        total_notional = filled_val["notional"].sum()
        print(f"\n  Volume nozionale totale (eseguiti): {total_notional:,.0f} USD")

    # Ordini cancellati/rejected
    canceled = orders[orders["status"].isin(["canceled", "expired", "rejected"])]
    if not canceled.empty:
        print(f"\n  Ordini non eseguiti (canceled/expired/rejected): {len(canceled)}")

    # Extended hours
    if "extended_hours" in orders.columns:
        eh = orders[orders["extended_hours"] == True]
        print(f"\n  Ordini extended hours: {len(eh)}")


def analyze_positions(positions):
    section("POSIZIONI APERTE")
    if positions is None or positions.empty:
        print("  Nessuna posizione aperta.")
        return

    print(f"  Posizioni aperte: {len(positions)}")
    total_mkt = positions["market_value"].sum()
    total_pl = positions["unrealized_pl"].sum()
    print(f"  Market value totale: {total_mkt:>12,.2f} USD")
    print(f"  P&L non realizzato:  {total_pl:>12,.2f} USD")

    positions_sorted = positions.sort_values("market_value", ascending=False)
    print()
    print(f"  {'SYMBOL':<8} {'QTY':>8} {'ENTRY':>8} {'CURR':>8} {'MKT_VAL':>12} {'P&L':>10} {'P&L%':>7}")
    for _, row in positions_sorted.iterrows():
        print(f"  {row['symbol']:<8} {row['qty']:>8.2f} {row['avg_entry_price']:>8.2f} "
              f"{row['current_price']:>8.2f} {row['market_value']:>12,.2f} "
              f"{row['unrealized_pl']:>+10,.2f} {row['unrealized_plpc']*100:>+7.2f}%")


def margin_diagnosis(acct, positions):
    section("DIAGNOSI MARGINE / PRESTITO")
    cash = acct["cash"]
    equity = acct["equity"]
    long_val = acct["long_market_value"]
    mult = acct.get("multiplier", "2")

    if cash >= 0:
        print("  Cash positivo: nessun margin loan attivo.")
        return

    loan = abs(cash)
    print(f"  Margin loan stimato:     {loan:>10,.2f} USD")
    print(f"  Long market value:       {long_val:>10,.2f} USD")
    print(f"  Equity (patrimonio netto): {equity:>10,.2f} USD")

    if equity > 0:
        leverage = long_val / equity
        print(f"  Leva implicita:          {leverage:>10.2f}x")
        interest_daily = loan * 0.10 / 252
        print(f"  Interesse stimato/giorno (10% annuo): {interest_daily:>8,.2f} USD")
        print(f"  Interesse stimato/anno:               {loan*0.10:>8,.2f} USD")

    print()
    print("  NOTA: In Alpaca paper trading il margin loan non comporta")
    print("  interessi reali, ma in live trading SÌ (tasso ~8-10% annuo).")
    print("  Alpaca supporta margin su conti con >2000 USD e abilitazione")
    print("  esplicita. Se non hai richiesto margin, il cash negativo")
    print("  potrebbe essere un artefatto del paper trading.")
    print()
    print("  VERIFICA: vai su app.alpaca.markets → Account → Margin")
    print("  e controlla se il conto è impostato come 'Margin Account'.")


def analyze_daily_exposure(orders, hist):
    """
    Ricostruisce l'esposizione intraday giorno per giorno.
    Il cash negativo e il -40% nel pannello sono spiegabili
    dalla leva usata intraday dalla strategia overnight.
    """
    section("ESPOSIZIONE INTRADAY GIORNO PER GIORNO")

    if orders is None or orders.empty:
        print("  Nessun dato ordini.")
        return

    filled = orders[orders["status"] == "filled"].copy()
    if filled.empty:
        print("  Nessun ordine eseguito.")
        return

    filled["notional"] = filled["filled_qty"] * filled["filled_avg_price"]
    filled["date"] = pd.to_datetime(filled["created_at"]).dt.date

    # Merge con equity giornaliera
    hist_valid = hist[hist["equity"] > 0].copy()
    hist_valid["date"] = pd.to_datetime(hist_valid["date"]).dt.date
    equity_map = dict(zip(hist_valid["date"], hist_valid["equity"]))

    daily = filled.groupby(["date", "side"])["notional"].sum().unstack(fill_value=0).reset_index()
    if "buy" not in daily.columns:
        daily["buy"] = 0.0
    if "sell" not in daily.columns:
        daily["sell"] = 0.0

    daily["gross_turnover"] = daily["buy"] + daily["sell"]
    daily["net"] = daily["buy"] - daily["sell"]
    daily["equity"] = daily["date"].map(equity_map)
    daily["leverage_buy"] = daily.apply(
        lambda r: r["buy"] / r["equity"] if r["equity"] and r["equity"] > 0 else np.nan, axis=1
    )

    print()
    print(f"  {'DATA':<12} {'BUY':>12} {'SELL':>12} {'NET':>10} {'EQUITY':>10} {'LEVA BUY':>9}")
    for _, row in daily.sort_values("date").iterrows():
        leva_s = f"{row['leverage_buy']:.1f}x" if pd.notna(row["leverage_buy"]) else "   --"
        eq_s = f"{row['equity']:>10,.0f}" if pd.notna(row["equity"]) else "        --"
        flag = " ⚠️" if pd.notna(row["leverage_buy"]) and row["leverage_buy"] > 3 else ""
        print(f"  {str(row['date']):<12} {row['buy']:>12,.0f} {row['sell']:>12,.0f} "
              f"{row['net']:>+10,.0f} {eq_s} {leva_s:>9}{flag}")

    max_leva = daily["leverage_buy"].max()
    max_leva_date = daily.loc[daily["leverage_buy"].idxmax(), "date"] if pd.notna(max_leva) else "?"
    print()
    print(f"  Leva massima buy-side: {max_leva:.1f}x il {max_leva_date}")
    print()
    print("  NOTA sul cash negativo e variazioni % estreme nel pannello:")
    print("  ─────────────────────────────────────────────────────────────")
    print("  Quando le posizioni aperte superano l'equity disponibile,")
    print("  Alpaca finanzia la differenza con un margin loan:")
    print()
    print("    cash = equity − long_market_value")
    print()
    print("  Il pannello Alpaca mostra la % giornaliera come:")
    print("    P&L_giornaliero / equity_apertura")
    print("  Con leva > 1x, movimenti moderati del mercato producono")
    print("  variazioni % amplificate sull'equity.")
    if pd.notna(max_leva) and max_leva > 1.0:
        print(f"\n  Leva massima rilevata: {max_leva:.1f}x — un movimento del mercato")
        print(f"  del 5% produce una variazione di ~{max_leva*5:.0f}% sull'equity.")


def analyze_pnl_reconstruction(orders, hist):
    """Ricostruisce il P&L trade-by-trade per i filled orders."""
    section("RICOSTRUZIONE P&L PER SIMBOLO (ordini eseguiti)")

    if orders is None or orders.empty:
        return

    filled = orders[orders["status"] == "filled"].copy()
    filled["notional"] = filled["filled_qty"] * filled["filled_avg_price"]
    filled["signed_notional"] = filled.apply(
        lambda r: -r["notional"] if r["side"] == "buy" else r["notional"], axis=1
    )

    by_sym = filled.groupby("symbol").agg(
        n_orders=("id", "count"),
        total_buy_notional=("notional", lambda x: x[filled.loc[x.index, "side"] == "buy"].sum()),
        total_sell_notional=("notional", lambda x: x[filled.loc[x.index, "side"] == "sell"].sum()),
        net_cash_flow=("signed_notional", "sum"),
    ).reset_index()

    # net_cash_flow positivo = più vendite che acquisti = probabile P&L positivo
    by_sym = by_sym.sort_values("net_cash_flow", ascending=False)
    total_net = by_sym["net_cash_flow"].sum()

    print()
    print(f"  {'SYMBOL':<8} {'N.ORD':>6} {'BUY_NOT':>12} {'SELL_NOT':>12} {'NET_FLOW':>12}")
    for _, row in by_sym.iterrows():
        flag = " 🔴" if row["net_cash_flow"] < -500 else ("" if row["net_cash_flow"] > 0 else " 🟡")
        print(f"  {row['symbol']:<8} {row['n_orders']:>6} {row['total_buy_notional']:>12,.0f} "
              f"{row['total_sell_notional']:>12,.0f} {row['net_cash_flow']:>+12,.0f}{flag}")

    print(f"  {'TOTALE':<8} {'':>6} {'':>12} {'':>12} {total_net:>+12,.0f}")
    print()
    print("  Net cash flow = sell_notional − buy_notional")
    print("  Positivo = complessivamente più venduto che comprato (posizione ridotta)")
    print("  Negativo = più comprato che venduto (posizione ancora aperta o loss)")
    print()

    # Equity spiegata
    hist_valid = hist[hist["equity"] > 0]
    if not hist_valid.empty:
        eq_start = hist_valid["equity"].iloc[0]
        eq_end = hist_valid["equity"].iloc[-1]
        total_pl = eq_end - eq_start
        print(f"  Equity start ({hist_valid['date'].iloc[0]}): {eq_start:>10,.2f} USD")
        print(f"  Equity end   ({hist_valid['date'].iloc[-1]}): {eq_end:>10,.2f} USD")
        print(f"  Variazione totale:                    {total_pl:>+10,.2f} USD  ({total_pl/eq_start*100:>+.2f}%)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", type=str, default="default",
                        help="Nome account (es. pa1, pa2) — deve corrispondere a quello usato in fetch_data.py")
    args = parser.parse_args()
    out_dir = BASE_OUT / args.account

    hist, orders, positions, acct = load_data(out_dir)

    label = args.account.upper() if args.account != "default" else ""
    print("\n" + "="*60)
    print(f" ALPACA {label} — AUDIT PORTAFOGLIO")
    print("="*60)

    analyze_account(acct)
    analyze_history(hist)
    analyze_daily_exposure(orders, hist)
    analyze_pnl_reconstruction(orders, hist)
    analyze_orders(orders)
    analyze_positions(positions)
    margin_diagnosis(acct, positions)

    section("CONCLUSIONI")
    issues = []
    if acct["cash"] < 0:
        issues.append(f"Cash negativo ({acct['cash']:,.2f} USD) → margin loan attivo")
    if acct["buying_power"] < 0:
        issues.append(f"Buying power negativo ({acct['buying_power']:,.2f} USD) → over-margined")

    if not issues:
        print("  Nessuna anomalia critica rilevata.")
    else:
        for i in issues:
            print(f"  ⚠️  {i}")
        print()
        print("  Azione raccomandata:")
        print("  1. Ridurre le posizioni per portare il cash in positivo")
        print("  2. Verificare su Alpaca se il conto è margin o cash")
        print("  3. Per paper trading: considera di resettare l'account da")
        print("     app.alpaca.markets → Account → Reset Paper Account")

    print()


if __name__ == "__main__":
    main()
