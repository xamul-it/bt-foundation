"""
BaseStrategy — interfaccia comune per tutte le strategie pandas.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseStrategy(ABC):
    """
    Classe base per strategie pandas.

    Subclass deve implementare generate_signals().
    """

    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        """
        Genera segnali dal DataFrame OHLCV.

        Parameters
        ----------
        df : DataFrame con colonne open/high/low/close/volume,
             indice DatetimeIndex tz-aware o tz-naive.

        Returns
        -------
        Series allineata all'indice di df:
            1  = segnale LONG
           -1  = segnale SHORT
            0  = nessun segnale / hold
        """
        ...

    @property
    def name(self) -> str:
        return self.__class__.__name__
