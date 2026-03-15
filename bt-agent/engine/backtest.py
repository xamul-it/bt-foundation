"""
PandasBacktest — engine per strategie intraday.

Comportamento:
- Entry al close della barra del segnale (identico a BT)
- SL/TP su high/low della barra corrente
- exitbar: due modalità selezionabili
- minutes_before_close: blocca nuovi ingressi N min prima delle 16:00
- EOD close all'ultima barra del giorno

exitbar_mode
-----------
'unconditional' (default):
    Chiude la posizione dopo esattamente N barre, indipendentemente dai segnali.
    Utile per strategie a tempo fisso.

'bt_style':
    Replica semantica di intraday.HMA in bt-core.
    Chiude SOLO se arriva un segnale same-direction mentre counter > exitbar.
    Il counter viene incrementato ogni barra e azzerato all'apertura.
    Non chiude mai "a orologio".
"""
from __future__ import annotations

import logging
from datetime import time
from typing import Optional

import pandas as pd

from strategies.base import BaseStrategy

LOG = logging.getLogger(__name__)


class PandasBacktest:
    """
    Engine di backtest pandas per strategie intraday.

    Parameters
    ----------
    sl_pct : float
        Stop loss % (es. 0.005 = 0.5%). 0 = disabilitato.
    tp_pct : float | None
        Take profit %. None = disabilitato.
    exitbar : int | None
        Soglia barre per chiusura. None = disabilitato.
    exitbar_mode : str
        'unconditional' = chiudi dopo N barre sempre.
        'bt_style'      = chiudi solo su segnale same-dir con counter > N (replica bt-core).
    minutes_before_close : int
        Blocca nuovi ingressi N minuti prima delle 16:00 (default 15).
    """

    MARKET_CLOSE = time(16, 0)

    def __init__(
        self,
        sl_pct: float = 0.0,
        tp_pct: Optional[float] = None,
        exitbar: Optional[int] = None,
        exitbar_mode: str = "unconditional",
        minutes_before_close: int = 15,
        minutes_after_open: int = 10,
        overnight: bool = False,
        sl_on_close: bool = False,
    ):
        self.sl_pct = sl_pct
        self.tp_pct = tp_pct
        self.exitbar = exitbar
        self.exitbar_mode = exitbar_mode
        self.minutes_before_close = minutes_before_close
        self.minutes_after_open = minutes_after_open  # replica BT: blocca entry nei primi N min
        self.overnight = overnight
        self.sl_on_close = sl_on_close  # True = check SL sul close (replica bt-core), False = low/high (realistico)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        df: pd.DataFrame,
        strategy: BaseStrategy,
        symbol: str = "UNKNOWN",
        signals: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """
        Esegue il backtest su un DataFrame OHLCV.

        Parameters
        ----------
        df : DataFrame con colonne open/high/low/close/volume, indice DatetimeIndex.
            Deve contenere solo le barre su cui eseguire la simulazione (es. RTH-only).
        strategy : istanza di BaseStrategy o SignalStrategy.
        symbol : nome del simbolo.
        signals : segnali pre-calcolati (pd.Series +1/-1/0). Se forniti, vengono
            usati al posto di strategy.generate_signals(df). Utile quando i segnali
            devono essere calcolati su un dataset più ampio (es. AH+PM+RTH) per
            garantire la continuità degli indicatori, ma la simulazione deve girare
            solo su un sottoinsieme (es. RTH). I segnali vengono riallineati all'indice
            di df con fill_value=0.

        Returns
        -------
        DataFrame trades con colonne:
            symbol, entry_time, entry_price, exit_time, exit_price,
            exit_pct, outcome, bars_held, side
        """
        df = df.copy().sort_index()
        if signals is not None:
            signals = signals.reindex(df.index, fill_value=0)
        else:
            signals = strategy.generate_signals(df)

        all_trades = []
        if self.overnight:
            all_trades = self._simulate_continuous(df, signals, symbol)
        else:
            for day, day_df in df.groupby(df.index.normalize()):
                day_signals = signals.loc[day_df.index]
                trades = self._simulate_day(day_df, day_signals, symbol)
                all_trades.extend(trades)

        if not all_trades:
            return pd.DataFrame(columns=[
                "symbol", "entry_time", "entry_price",
                "exit_time", "exit_price", "exit_pct",
                "outcome", "bars_held", "side"
            ])

        cols = ["symbol", "entry_time", "entry_price",
                "exit_time", "exit_price", "exit_pct",
                "outcome", "bars_held", "side"]
        return pd.DataFrame(all_trades)[cols].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Internal simulation
    # ------------------------------------------------------------------

    def _simulate_continuous(self, df: pd.DataFrame, signals: pd.Series, symbol: str) -> list:
        """
        Modalità overnight (use_calendar=False in bt-core).

        Processa il DataFrame come serie continua senza chiudere le posizioni
        a fine giornata. Le posizioni sopravvivono al gap notturno (16:00 → 9:30).

        Differenze rispetto a _simulate_day:
        - Nessun EOD close automatico
        - Nessun cutoff_time (no minutes_before_close per nuovi ingressi)
        - Il counter exitbar conta barre totali, non solo barre di giornata
        """
        if len(df) < 2:
            return []

        trades = []
        position = None
        counter = 0
        skip_entry = False  # BT: dopo SL/TP close order pending per 1 barra → no new entry
        last_entry_signal = 0  # replica BT last_entry_signal (aggiornato alla submission)
        pending_entry = None   # entry con delay 1 barra, cancellabile da segnale opposto

        # ── Accesso numpy per velocità ────────────────────────────────────────
        times  = df.index
        opens  = df["open"].values
        highs  = df["high"].values
        lows   = df["low"].values
        closes = df["close"].values
        sigs   = signals.values

        for i in range(len(df)):
            ts  = times[i]
            sig = int(sigs[i]) if sigs[i] != 0 else 0
            bar = {"open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i]}

            # ── FILL O CANCELLA ENTRY PENDENTE ────────────────────────
            if pending_entry is not None and position is None:
                p_sig = 1 if pending_entry["side"] == "long" else -1
                if sig == -p_sig:
                    pending_entry = None  # cancella; il segnale opposto può aprire nuova pending
                else:
                    entry_price = pending_entry["limit_price"]
                    sl = entry_price * (1 - self.sl_pct) if self.sl_pct > 0 and pending_entry["side"] == "long" \
                         else entry_price * (1 + self.sl_pct) if self.sl_pct > 0 else None
                    tp = entry_price * (1 + self.tp_pct) if self.tp_pct and pending_entry["side"] == "long" \
                         else entry_price * (1 - self.tp_pct) if self.tp_pct else None
                    position = {
                        "symbol": symbol, "side": pending_entry["side"],
                        "entry_idx": i, "entry_time": ts,
                        "entry_price": entry_price, "sl": sl, "tp": tp,
                    }
                    counter = 0
                    pending_entry = None

            # ── SL / TP ──────────────────────────────────────────────
            if position is not None:
                hit = self._check_sl_tp(position, bar, ts, i)
                if hit:
                    trades.append(hit)
                    position = None
                    counter = 0
                    skip_entry = True  # replica BT: close order pending → blocca entry sulla barra successiva
                    continue

            # ── EXITBAR ──────────────────────────────────────────────
            if position is not None and self.exitbar is not None:
                counter += 1
                if self.exitbar_mode == "unconditional":
                    if counter >= self.exitbar:
                        trades.append(self._close_position(position, bar, ts, i, "exitbar"))
                        position = None
                        counter = 0
                        sig = 0
                elif self.exitbar_mode == "bt_style":
                    same_dir = (position["side"] == "long" and sig == 1) or \
                               (position["side"] == "short" and sig == -1)
                    if same_dir and counter > self.exitbar:
                        trades.append(self._close_position(position, bar, ts, i, "exitbar"))
                        position = None
                        counter = 0
                        sig = 0
                        last_entry_signal = 0  # BT: reset su same_side_exitbar

            # ── SEGNALE OPPOSTO ──────────────────────────────────────
            if position is not None:
                opposite = (position["side"] == "long" and sig == -1) or \
                           (position["side"] == "short" and sig == 1)
                if opposite:
                    trades.append(self._close_position(position, bar, ts, i, "signal"))
                    position = None
                    counter = 0
                    sig = 0  # no reverse immediato (replica BT backtest)

            # ── NUOVO INGRESSO PENDING (nessun cutoff in overnight mode) ──
            if sig != 0 and position is None and pending_entry is None:
                if skip_entry:
                    skip_entry = False
                elif sig != last_entry_signal:
                    pending_entry = {
                        "side": "long" if sig == 1 else "short",
                        "limit_price": bar["close"],
                        "signal_ts": ts,
                    }
                    last_entry_signal = sig

        # Chiudi posizione aperta alla fine del dataset
        if position is not None:
            last_bar = df.iloc[-1]
            trades.append(self._close_position(
                position, last_bar, df.index[-1], len(df) - 1, "eod"
            ))

        return trades

    def _cutoff_time(self, day_df: pd.DataFrame) -> time:
        from datetime import datetime, timedelta
        close_dt = datetime.combine(day_df.index[0].date(), self.MARKET_CLOSE)
        return (close_dt - timedelta(minutes=self.minutes_before_close)).time()

    def _open_time(self, day_df: pd.DataFrame) -> time:
        """Tempo minimo per nuovi ingressi (replica BT: open + minutes_after_open)."""
        from datetime import datetime, timedelta
        if self.minutes_after_open <= 0:
            return time(0, 0)
        # Usa il primo timestamp del giorno come base per l'orario di apertura
        first_ts = day_df.index[0]
        # Arrotonda all'ora:minuto della prima barra e aggiungi il buffer
        open_dt = datetime.combine(first_ts.date(), first_ts.time())
        return (open_dt + timedelta(minutes=self.minutes_after_open)).time()

    def _to_scalar(self, raw) -> int:
        if hasattr(raw, "__len__"):
            return int(raw.iloc[0]) if len(raw) > 0 else 0
        return int(raw)

    def _simulate_day(self, day_df: pd.DataFrame, signals: pd.Series, symbol: str) -> list:
        if len(day_df) < 2:
            return []

        cutoff   = self._cutoff_time(day_df)
        open_buf = self._open_time(day_df)  # nessun entry prima di open + N min
        trades = []
        position = None
        counter = 0   # barre trascorse dall'apertura (modalità bt_style)
        skip_entry = False  # BT: dopo SL/TP l'ordine di chiusura è pending per 1 barra → no new entry
        last_entry_signal = 0  # replica BT last_entry_signal: aggiornato al momento della submission
        pending_entry = None   # BT: entry Limit ha delay 1 barra; può essere cancellato dal segnale opposto
        #   Format: {'side': 'long'|'short', 'limit_price': float, 'signal_ts': ts}

        # ── Accesso numpy per velocità (evita iterrows overhead) ─────────────
        times  = day_df.index
        opens  = day_df["open"].values
        highs  = day_df["high"].values
        lows   = day_df["low"].values
        closes = day_df["close"].values
        sigs   = signals.values   # già reindexato a day_df.index in run()

        for i in range(len(day_df)):
            ts  = times[i]
            sig = int(sigs[i]) if sigs[i] != 0 else 0
            # Crea un dict-like per _check_sl_tp e _close_position
            bar = {"open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i]}

            # ── FILL O CANCELLA ENTRY PENDENTE ─────────────────────────
            # Replica BT: entry Limit submit a close[T], fill a bar T+1.
            # Se a T+1 arriva segnale opposto → _cancel_pending_open_entries → cancella.
            # last_entry_signal è già stato aggiornato alla submission (bar T).
            if pending_entry is not None and position is None:
                p_sig = 1 if pending_entry["side"] == "long" else -1
                if sig == -p_sig:
                    # Segnale opposto: cancella entry pendente
                    # (last_entry_signal rimane impostato al lato cancellato —
                    #  il codice di new entry sotto verificherà se il nuovo segnale è diverso)
                    pending_entry = None
                    # non resettare sig: il segnale opposto servirà per aprire nuova entry
                else:
                    # Fill: apri posizione al prezzo limite della barra precedente
                    entry_price = pending_entry["limit_price"]
                    sl = entry_price * (1 - self.sl_pct) if self.sl_pct > 0 and pending_entry["side"] == "long" \
                         else entry_price * (1 + self.sl_pct) if self.sl_pct > 0 else None
                    tp = entry_price * (1 + self.tp_pct) if self.tp_pct and pending_entry["side"] == "long" \
                         else entry_price * (1 - self.tp_pct) if self.tp_pct else None
                    position = {
                        "symbol": symbol,
                        "side": pending_entry["side"],
                        "entry_idx": i,
                        "entry_time": ts,
                        "entry_price": entry_price,
                        "sl": sl,
                        "tp": tp,
                    }
                    counter = 0
                    pending_entry = None

            # ── SL / TP ──────────────────────────────────────────────
            if position is not None:
                hit = self._check_sl_tp(position, bar, ts, i)
                if hit:
                    trades.append(hit)
                    position = None
                    counter = 0
                    skip_entry = True  # replica BT: close order pending → blocca entry sulla barra successiva
                    continue

            # ── EXITBAR ──────────────────────────────────────────────
            if position is not None and self.exitbar is not None:
                counter += 1

                if self.exitbar_mode == "unconditional":
                    # Chiudi dopo N barre sempre
                    if counter >= self.exitbar:
                        trades.append(self._close_position(position, bar, ts, i, "exitbar"))
                        position = None
                        counter = 0
                        sig = 0   # non aprire subito dopo

                elif self.exitbar_mode == "bt_style":
                    # Chiudi solo su segnale same-direction con counter > exitbar
                    same_dir = (position["side"] == "long" and sig == 1) or \
                               (position["side"] == "short" and sig == -1)
                    if same_dir and counter > self.exitbar:
                        trades.append(self._close_position(position, bar, ts, i, "exitbar"))
                        position = None
                        counter = 0
                        sig = 0
                        last_entry_signal = 0  # BT: reset su same_side_exitbar → permetti re-entry

            # ── SEGNALE OPPOSTO: chiudi (NO ribaltamento immediato) ──────
            # Replica BT backtest: BackBroker non ha get_orders_open(), quindi
            # _has_pending_close_order() restituisce sempre False → _can_send_open_entry
            # blocca il reverse con 'position_not_flat_no_close_pending'.
            # BT chiude la posizione sul segnale opposto ma NON apre subito il reverse;
            # attende il prossimo segnale indipendente su barra futura (dopo fill T+1).
            if position is not None:
                opposite = (position["side"] == "long" and sig == -1) or \
                           (position["side"] == "short" and sig == 1)
                if opposite:
                    trades.append(self._close_position(position, bar, ts, i, "signal"))
                    position = None
                    counter = 0
                    sig = 0  # non riaprire il reverse nella stessa barra
                    # last_entry_signal rimane al valore del lato chiuso:
                    # replica BT dove last_entry_signal non viene resettato su reversal close.
                    # Il prossimo ingresso contrario andrà comunque in quanto
                    # signal(nuovo) != last_entry_signal(vecchio).

            # ── NUOVO INGRESSO (pending) ──────────────────────────────
            # Replica BT: entry Limit a close[T], fill a T+1.
            # last_entry_signal aggiornato alla submission (non al fill).
            if sig != 0 and position is None and pending_entry is None:
                if skip_entry:
                    skip_entry = False  # consumato: dalla barra successiva si può entrare
                elif ts.time() >= open_buf and ts.time() < cutoff:
                    if sig != last_entry_signal:  # replica BT: no re-entry stesso lato
                        pending_entry = {
                            "side": "long" if sig == 1 else "short",
                            "limit_price": bar["close"],
                            "signal_ts": ts,
                        }
                        last_entry_signal = sig  # aggiornato alla submission (come BT)

        # ── EOD ───────────────────────────────────────────────────────
        # Cancella entry pendente a fine giornata (non eseguita)
        pending_entry = None
        if position is not None:
            last_bar = day_df.iloc[-1]
            trades.append(self._close_position(
                position, last_bar, day_df.index[-1], len(day_df) - 1, "eod"
            ))

        return trades

    # ------------------------------------------------------------------
    # Position helpers
    # ------------------------------------------------------------------

    def _open_position(self, sig: int, bar, ts, idx: int, symbol: str) -> dict:
        side = "long" if sig == 1 else "short"
        entry = bar["close"]
        pos = {
            "symbol": symbol,
            "side": side,
            "entry_idx": idx,
            "entry_time": ts,
            "entry_price": entry,
            "sl": entry * (1 - self.sl_pct) if self.sl_pct > 0 and side == "long"
                  else entry * (1 + self.sl_pct) if self.sl_pct > 0
                  else None,
            "tp": entry * (1 + self.tp_pct) if self.tp_pct and side == "long"
                  else entry * (1 - self.tp_pct) if self.tp_pct
                  else None,
        }
        return pos

    def _close_position(self, pos: dict, bar, ts, idx: int, outcome: str) -> dict:
        exit_price = bar["close"]
        if pos["side"] == "long":
            exit_pct = (exit_price / pos["entry_price"] - 1) * 100
        else:
            exit_pct = (pos["entry_price"] / exit_price - 1) * 100
        return {
            "symbol": pos["symbol"],
            "side": pos["side"],
            "entry_time": pos["entry_time"],
            "entry_price": pos["entry_price"],
            "exit_time": ts,
            "exit_price": exit_price,
            "exit_pct": exit_pct,
            "outcome": outcome,
            "bars_held": idx - pos["entry_idx"],
        }

    def _check_sl_tp(self, pos: dict, bar, ts, idx: int) -> Optional[dict]:
        side = pos["side"]
        close = bar["close"]

        if side == "long":
            if pos["tp"] and bar["high"] >= pos["tp"]:
                return self._make_trade(pos, ts, idx, pos["tp"],
                                        (pos["tp"] / pos["entry_price"] - 1) * 100, "tp")
            if pos["sl"]:
                # sl_on_close=True (bt-core): scatta se close <= SL, esce al close
                # sl_on_close=False (realistico): scatta se low <= SL, esce al livello SL
                if self.sl_on_close:
                    if close <= pos["sl"]:
                        return self._make_trade(pos, ts, idx, close,
                                                (close / pos["entry_price"] - 1) * 100, "sl")
                else:
                    if bar["low"] <= pos["sl"]:
                        return self._make_trade(pos, ts, idx, pos["sl"],
                                                (pos["sl"] / pos["entry_price"] - 1) * 100, "sl")
        else:
            if pos["tp"] and bar["low"] <= pos["tp"]:
                return self._make_trade(pos, ts, idx, pos["tp"],
                                        (pos["entry_price"] / pos["tp"] - 1) * 100, "tp")
            if pos["sl"]:
                if self.sl_on_close:
                    if close >= pos["sl"]:
                        return self._make_trade(pos, ts, idx, close,
                                                (pos["entry_price"] / close - 1) * 100, "sl")
                else:
                    if bar["high"] >= pos["sl"]:
                        return self._make_trade(pos, ts, idx, pos["sl"],
                                                (pos["entry_price"] / pos["sl"] - 1) * 100, "sl")
        return None

    @staticmethod
    def _make_trade(pos: dict, ts, idx: int, exit_price: float, exit_pct: float, outcome: str) -> dict:
        return {
            "symbol": pos["symbol"],
            "side": pos["side"],
            "entry_time": pos["entry_time"],
            "entry_price": pos["entry_price"],
            "exit_time": ts,
            "exit_price": exit_price,
            "exit_pct": exit_pct,
            "outcome": outcome,
            "bars_held": idx - pos["entry_idx"],
        }
