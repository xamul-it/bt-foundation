"""
BTIndicatorExtractor — Cerebro minimalista che logga valori indicatori → parquet.

Uso generico: passa una factory function che riceve il data feed BT e
restituisce un dict {nome: indicatore}. Il risultato viene serializzato
barra per barra in un parquet cache.

Cache: bt-agent/indicator_cache/{symbol}_{hash}.parquet
"""
from __future__ import annotations

import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import Callable, Dict, List

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from paths import CACHE_DIR, BT_CORE

LOG = logging.getLogger(__name__)


def _params_hash(label: str, params: dict) -> str:
    s = json.dumps({"label": label, "params": params}, sort_keys=True)
    return hashlib.md5(s.encode()).hexdigest()[:10]


def extract_indicators(
    df: pd.DataFrame,
    indicator_factory: Callable,
    factory_params: dict,
    label: str,
    symbol: str = "UNKNOWN",
    cache: bool = True,
) -> pd.DataFrame:
    """
    Esegue Cerebro con una strategia vuota che logga gli indicatori definiti
    da `indicator_factory`.

    Parameters
    ----------
    df : OHLCV DataFrame, indice DatetimeIndex.
    indicator_factory : callable(data, params) → dict{nome: bt.Indicator}
        Riceve il data feed BT e i parametri, ritorna un dizionario di
        indicatori da loggare. Esempio::

            def factory(data, params):
                return {
                    "hma": bt.indicators.HMA(data, period=params["period"]),
                    "atr": ATR_SMA(data, period=params["atr_period"]),
                }

    factory_params : dict di parametri passati alla factory.
    label : stringa identificativa per la cache (es. "HMA_p16").
    symbol : nome simbolo.
    cache : se True, usa/scrive cache parquet.

    Returns
    -------
    DataFrame con colonne datetime + una colonna per ogni indicatore.
    """
    h = _params_hash(label, factory_params)
    cache_path = CACHE_DIR / f"{symbol}_{label}_{h}.parquet"

    if cache and cache_path.exists():
        LOG.info(f"Cache hit: {cache_path}")
        return pd.read_parquet(cache_path)

    if str(BT_CORE) not in sys.path:
        sys.path.insert(0, str(BT_CORE))

    try:
        import backtrader as bt
    except ImportError as e:
        raise ImportError(f"backtrader non trovato. Aggiungi {bt_core} a sys.path: {e}")

    records: List[Dict] = []
    indicators_ref: Dict = {}

    class _ExtractStrategy(bt.Strategy):
        def __init__(self):
            indicators_ref.update(indicator_factory(self.data, factory_params))

        def next(self):
            row = {"datetime": self.data.datetime.datetime()}
            for name, ind in indicators_ref.items():
                try:
                    row[name] = ind[0]
                except Exception:
                    row[name] = float("nan")
            records.append(row)

    cerebro = bt.Cerebro()
    feed = bt.feeds.PandasData(
        dataname=df,
        datetime=None,
        open="open", high="high", low="low",
        close="close", volume="volume", openinterest=-1,
    )
    cerebro.adddata(feed)
    cerebro.addstrategy(_ExtractStrategy)
    cerebro.run()

    result = pd.DataFrame(records).set_index("datetime")

    if cache:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        result.to_parquet(cache_path)
        LOG.info(f"Cache scritto: {cache_path}")

    return result
