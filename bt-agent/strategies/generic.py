"""
GenericStrategy — plugin universale, mai da modificare per nuove strategie.

L'utente passa una signal_fn e i parametri. Nient'altro.

Uso
---
    from strategies.generic import SignalStrategy

    def mio_segnale(df, periodo=20, **kw):
        # df ha colonne: open, high, low, close, volume
        # ritorna Series con 1=long, -1=short, 0=nessuno, allineata a df.index
        ...
        return signals

    strategy = SignalStrategy(signal_fn=mio_segnale, periodo=20)

La signal_fn riceve sempre (df, **params) e deve ritornare una pd.Series
con lo stesso indice di df. Qualsiasi indicatore è accettabile purché
il risultato finale sia 1 / -1 / 0.

Per replicare esattamente bt-core usa gli indicatori di bt-agent/indicators/:
    from indicators.hma import hma
    from indicators.atr import atr_sma, atr_sma_pct

Per usare i valori estratti da Cerebro (massima fedeltà BT) usa
bt_bridge/extractor.py per calcolare prima gli indicatori e passali
nel df già arricchito:
    df['hma'] = extractor_result['hma']
    # poi signal_fn lavora su df['hma'] direttamente
"""
from __future__ import annotations

from typing import Callable

import pandas as pd

from strategies.base import BaseStrategy


class SignalStrategy(BaseStrategy):
    """
    Strategia generica: delega il calcolo dei segnali a signal_fn.

    Parameters
    ----------
    signal_fn : callable
        Funzione con firma: signal_fn(df: DataFrame, **params) -> Series
        La Series deve essere allineata all'indice di df con valori 1/-1/0.
    **params :
        Parametri liberi passati a signal_fn a ogni chiamata.
        Questi parametri compaiono anche nei risultati dello sweep.
    """

    def __init__(self, signal_fn: Callable, **params):
        if not callable(signal_fn):
            raise TypeError("signal_fn deve essere callable")
        self._signal_fn = signal_fn
        self._params = params
        # Esponi i params come attributi per la serializzazione nei risultati
        for k, v in params.items():
            setattr(self, k, v)

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        result = self._signal_fn(df, **self._params)
        if not isinstance(result, pd.Series):
            raise TypeError(f"signal_fn deve ritornare pd.Series, ricevuto {type(result)}")
        if not result.index.equals(df.index):
            result = result.reindex(df.index, fill_value=0)
        return result.fillna(0).astype(int)

    @property
    def name(self) -> str:
        fn_name = getattr(self._signal_fn, "__name__", "signal")
        params_str = "_".join(f"{k}{v}" for k, v in self._params.items())
        return f"{fn_name}_{params_str}" if params_str else fn_name
