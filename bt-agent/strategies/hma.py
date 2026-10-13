"""
HMA Strategy — portata da bin/HMA/final_exact_match_test.py.

Logica:
- inverted=True (default): LONG quando HMA scende, SHORT quando sale
- Filtro ATR: segnali solo se ATR% >= atr_threshold
- Filtro ora: solo barre in [hour_from, hour_to)

Validata contro Backtrader su singolo giorno con corrispondenza perfetta.
"""
from __future__ import annotations

import pandas as pd

from indicators.hma import hma
from indicators.atr import atr_sma_pct
from strategies.base import BaseStrategy


class HMAStrategy(BaseStrategy):
    """
    Hull Moving Average strategy intraday.

    Parameters
    ----------
    period : int
        Periodo HMA (default 16).
    inverted : bool
        True = contrarian (default). False = trend-following.
    atr_period : int
        Periodo ATR per filtro volatilità (default 14).
    atr_threshold : float
        ATR% minimo per generare segnali (default 0.7).
        0 = disabilita filtro ATR.
    hour_from : int
        Ora inizio trading (inclusa, default 10).
    hour_to : int
        Ora fine trading (esclusa, default 15).
    """

    def __init__(
        self,
        period: int = 16,
        inverted: bool = True,
        atr_period: int = 14,
        atr_threshold: float = 0.7,
        hour_from: int = 10,
        hour_to: int = 15,
    ):
        self.period = period
        self.inverted = inverted
        self.atr_period = atr_period
        self.atr_threshold = atr_threshold
        self.hour_from = hour_from
        self.hour_to = hour_to

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """
        Genera segnali 1/−1/0 allineati all'indice di df.

        Il segnale viene emesso sulla barra corrente e l'engine
        entra al close della stessa barra (matcha BT).
        """
        sig = pd.Series(0, index=df.index, dtype=int)

        hma_vals = hma(df["close"], self.period)

        # Filtro ATR
        if self.atr_threshold > 0:
            atr_vals = atr_sma_pct(df["high"], df["low"], df["close"], self.atr_period)
        else:
            atr_vals = pd.Series(1.0, index=df.index)  # sempre OK

        hma_prev = hma_vals.shift(1)
        hma_curr = hma_vals

        # Barre valide
        valid = (
            hma_curr.notna() &
            hma_prev.notna() &
            atr_vals.notna() &
            (atr_vals >= self.atr_threshold)
        )

        # Filtro orario
        hour = df.index.hour
        in_hours = (hour >= self.hour_from) & (hour < self.hour_to)
        valid &= in_hours

        if self.inverted:
            # HMA scende → LONG, HMA sale → SHORT
            long_mask = valid & (hma_prev > hma_curr)
            short_mask = valid & (hma_prev < hma_curr)
        else:
            long_mask = valid & (hma_prev < hma_curr)
            short_mask = valid & (hma_prev > hma_curr)

        sig[long_mask] = 1
        sig[short_mask] = -1

        return sig
