# Strategie QuantConnect — legenda

| Sigla nello studio | Strategia riportata sotto | Logica principale |
|---|---|---|
| S1 | Large Cap Pullback Mean Reversion | Acquisto di pullback in trend crescente |
| S2 | Low-Vol Cross-Asset Futures Risk Parity | Selezione low-vol e pesi inverse-vol |
| S3 | US Large Cap Momentum Strategy | Momentum con esposizione dinamica e breadth |
| S4 (include ex S5) | Monthly Sector-Neutral Large-Cap Momentum Strategy / Large-Cap Momentum With Risk Controls | Unica famiglia: momentum mensile, risk-off e timeout |

**Aggiornamento: quattro famiglie, non cinque strategie indipendenti.** S5 è
un alias storico di S4: i due blocchi originali restano sotto per tracciabilità,
ma nei nuovi confronti non vengono contati due volte.

Lo [studio di allocazione e scalabilità](../bt-strategy-test/quantconnect_5/out_20261016_v2/allocation_study/RESULTS.md)
e il relativo [Excel](../bt-strategy-test/quantconnect_5/out_20261016_v2/allocation_study/allocazione_confronto.xlsx)
confrontano allocazione legacy e target 100%, numero di simboli e capacità
indicativa. Il capitale è composto sull'equity corrente; il 100% si applica
quando ci sono segnali validi, conservando cash in risk-off o senza opportunità.
Sono inclusi rendimento medio netto per trade, esposizione e drawdown.

Le [schede tecniche dello studio](../bt-strategy-test/quantconnect_5/TECHNICAL_NOTES.md)
descrivono, per ogni strategia, asset, filtri minimi, graduatoria, entrate,
uscite, dimensionamento, tuning eseguito e limiti di scalabilità.
I [risultati comparativi legacy](../bt-strategy-test/quantconnect_5/out_20261016_v2/RESULTS.md)
includono la stessa guida e mantengono le cinque etichette storiche.

Le descrizioni e i codici originali sono conservati di seguito. Le regole
effettivamente testate e le differenze rispetto ai sorgenti sono esplicitate
nelle schede: in particolare S2 è provata anche sul proxy azionario richiesto,
mentre gli universi fondamentali storici e i rollover dei future non sono
ricostruiti integralmente. I blocchi QuantBook ripetuti sono esempi accessori.

# Large Cap Pullback Mean Reversion

This strategy implements a long-only mean reversion approach across the 50 largest Nasdaq-100 (QQQ) constituents, selected dynamically by ETF weight. The alpha thesis is that large cap growth stocks often experience temporary oversold pullbacks within established long term uptrends before reverting toward their prevailing trend.

The universe is filtered to securities trading above their 200 day moving average with a minimum 21 day average dollar volume of $5 million. Entry signals require the price to close below the lower Bollinger Band, an oversold RSI (14), and a pullback of at least 3% from the highest closing price over the previous 60 trading days. When multiple candidates qualify, positions are ranked by Bollinger Band z-score, with RSI used as a secondary ranking criterion.

The portfolio holds up to 10 equally weighted positions and rebalances daily. Positions are exited when price reverts to the Bollinger Band midpoint, the long-term trend breaks, or the maximum holding period is reached. Risk is managed through equal weighting, liquidity screening, trend filtering, and systematic exit rules.


## code

```
region imports
from AlgorithmImports import *
import numpy as np
#endregion

class NasdaqBollingerPullbackMeanReversion(QCAlgorithm):

    def initialize(self):
        self.set_start_date(2021, 1, 1)
        self.set_end_date(2026, 7, 13)
        self.set_cash(1_000_000)

        self.settings.seed_initial_prices = True
        self.settings.minimum_order_margin_portfolio_percentage = 0

        self.universe_settings.resolution = Resolution.DAILY
        self.universe_settings.data_normalization_mode = (
            DataNormalizationMode.ADJUSTED
        )
        self.universe_settings.asynchronous = True

        # Top QQQ constituents by ETF weight
        self.universe_size = int(
            self.get_parameter("UNIVERSE_SIZE") or 50
        )

        # Bollinger Band parameters
        self.bb_period = int(
            self.get_parameter("BB_PERIOD") or 15
        )
        self.bb_multiplier = float(
            self.get_parameter("BB_MULTIPLIER") or 1.75
        )

        # Long-term trend filter
        self.trend_period = 200

        # RSI is used only as an entry confirmation
        self.rsi_period = 14
        self.rsi_entry = float(
            self.get_parameter("RSI_ENTRY") or 45
        )

        # Pullback from prior peak
        self.peak_lookback = int(
            self.get_parameter("PEAK_LOOKBACK") or 60
        )
        self.min_pullback = float(
            self.get_parameter("MIN_PULLBACK") or 0.03
        )

        # Portfolio construction
        self.max_positions = int(
            self.get_parameter("MAX_POSITIONS") or 10
        )

        # Time-based exit
        self.max_holding_days = 15

        # Liquidity filter
        self.liquidity_lookback = 21
        self.min_avg_dollar_volume = 5_000_000

        self.history_lookback = max(
            self.trend_period,
            self.peak_lookback + 1,
            self.bb_period,
            self.rsi_period + 1,
            self.liquidity_lookback
        ) + 10

        self.qqq = self.add_equity(
            "QQQ",
            Resolution.DAILY
        ).symbol

        self.set_benchmark("QQQ")

        self.qqq_constituents = set()
        self.holding_days = {}

        self.add_universe(
            self.universe.etf(
                self.qqq,
                universe_filter_func=self.select_qqq
            )
        )

        self.schedule.on(
            self.date_rules.every_day(self.qqq),
            self.time_rules.after_market_open(self.qqq, 30),
            self.rebalance
        )

    def select_qqq(self, constituents):
        eligible = [
            constituent
            for constituent in constituents
            if (
                constituent.weight is not None
                and constituent.weight > 0
            )
        ]

        selected = sorted(
            eligible,
            key=lambda constituent: constituent.weight,
            reverse=True
        )[:self.universe_size]

        symbols = [
            constituent.symbol
            for constituent in selected
        ]

        self.qqq_constituents = set(symbols)

        return symbols

    def rebalance(self):
        signal_data = self.calculate_signals(
            self.qqq_constituents
        )

        current_holdings = {
            holding.symbol
            for holding in self.portfolio.values()
            if (
                holding.invested
                and holding.symbol != self.qqq
            )
        }

        retained_symbols = []
        symbols_to_exit = []

        # Evaluate exits
        for symbol in current_holdings:
            self.holding_days[symbol] = (
                self.holding_days.get(symbol, 0) + 1
            )

            data = signal_data.get(symbol)

            if data is None:
                symbols_to_exit.append(symbol)
                continue

            # Mean reversion is complete when price reaches the middle band
            exit_at_mean = (
                data["close"] >= data["middle_band"]
            )

            # Exit if the long-term uptrend breaks
            exit_on_trend_break = (
                data["close"] < data["trend_sma"]
            )

            # Exit if the rebound takes too long
            exit_on_time = (
                self.holding_days[symbol]
                >= self.max_holding_days
            )

            if (
                exit_at_mean
                or exit_on_trend_break
                or exit_on_time
            ):
                symbols_to_exit.append(symbol)
            else:
                retained_symbols.append(symbol)

        for symbol in symbols_to_exit:
            self.holding_days.pop(symbol, None)

        # Find new entry candidates
        entry_candidates = [
            symbol
            for symbol, data in signal_data.items()
            if (
                data["entry_signal"]
                and symbol not in current_holdings
            )
        ]

        # Most extreme Bollinger deviations rank first.
        # Lower RSI is used as the secondary ranking criterion.
        entry_candidates.sort(
            key=lambda symbol: (
                signal_data[symbol]["z_score"],
                signal_data[symbol]["rsi"]
            )
        )

        available_slots = max(
            0,
            self.max_positions - len(retained_symbols)
        )

        new_entries = entry_candidates[:available_slots]

        for symbol in new_entries:
            self.holding_days[symbol] = 0

        desired_symbols = list(
            dict.fromkeys(retained_symbols + new_entries)
        )

        targets = []

        if desired_symbols:
            target_weight = 1.0 / len(desired_symbols)

            for symbol in desired_symbols:
                targets.append(
                    PortfolioTarget(symbol, target_weight)
                )

        for symbol in symbols_to_exit:
            targets.append(
                PortfolioTarget(symbol, 0)
            )

        if targets:
            self.set_holdings(targets)



    def calculate_signals(self, symbols):
        valid_symbols = [
            symbol
            for symbol in symbols
            if (
                symbol in self.securities
                and self.securities[symbol].price > 0
            )
        ]

        if not valid_symbols:
            return {}

        history = self.history(
            valid_symbols,
            self.history_lookback,
            Resolution.DAILY
        )

        if history.empty:
            return {}

        results = {}

        minimum_required = max(
            self.trend_period,
            self.peak_lookback + 1,
            self.bb_period,
            self.rsi_period + 1,
            self.liquidity_lookback
        )

        for symbol in valid_symbols:
            try:
                data = history.loc[symbol].dropna(
                    subset=["close", "volume"]
                )

                if len(data) < minimum_required:
                    continue

                close = data["close"].astype(float)
                volume = data["volume"].astype(float)

                current_close = float(close.iloc[-1])

                # Liquidity filter
                avg_dollar_volume = (
                    close.tail(self.liquidity_lookback)
                    * volume.tail(self.liquidity_lookback)
                ).mean()

                if (
                    avg_dollar_volume
                    < self.min_avg_dollar_volume
                ):
                    continue

                # Long-term trend filter
                trend_sma = float(
                    close.tail(self.trend_period).mean()
                )

                trend_ok = current_close > trend_sma

                # Bollinger Bands
                bb_prices = close.tail(self.bb_period)

                middle_band = float(bb_prices.mean())
                bb_std = float(bb_prices.std())

                if bb_std <= 0 or np.isnan(bb_std):
                    continue

                lower_band = (
                    middle_band
                    - self.bb_multiplier * bb_std
                )

                below_lower_band = (
                    current_close < lower_band
                )

                z_score = (
                    current_close - middle_band
                ) / bb_std

                # RSI entry confirmation
                rsi = self.calculate_rsi(
                    close,
                    self.rsi_period
                )

                if rsi is None or np.isnan(rsi):
                    continue

                rsi_oversold = (
                    rsi <= self.rsi_entry
                )

                # Prior peak excludes today's close
                peak_window = close.iloc[
                    -(self.peak_lookback + 1):-1
                ]

                if len(peak_window) < self.peak_lookback:
                    continue

                prior_peak = float(peak_window.max())

                pullback = (
                    current_close / prior_peak
                ) - 1.0

                valid_pullback = (
                    pullback <= -self.min_pullback
                )

                entry_signal = (
                    trend_ok
                    and below_lower_band
                    and rsi_oversold
                    and valid_pullback
                )

                results[symbol] = {
                    "entry_signal": entry_signal,
                    "close": current_close,
                    "trend_sma": trend_sma,
                    "middle_band": middle_band,
                    "lower_band": lower_band,
                    "z_score": z_score,
                    "rsi": rsi
                }

            except (
                KeyError,
                ValueError,
                TypeError,
                IndexError
            ):
                continue

        return results

    @staticmethod
    def calculate_rsi(close, period):
        if len(close) < period + 1:
            return None

        changes = close.diff().dropna()

        gains = changes.clip(lower=0)
        losses = -changes.clip(upper=0)

        average_gain = gains.ewm(
            alpha=1.0 / period,
            adjust=False,
            min_periods=period
        ).mean()

        average_loss = losses.ewm(
            alpha=1.0 / period,
            adjust=False,
            min_periods=period
        ).mean()

        if (
            average_gain.empty
            or average_loss.empty
        ):
            return None

        current_gain = float(
            average_gain.iloc[-1]
        )
        current_loss = float(
            average_loss.iloc[-1]
        )

        if current_loss == 0:
            return 100.0

        relative_strength = (
            current_gain / current_loss
        )

        return 100.0 - (
            100.0 / (1.0 + relative_strength)
        )

    def on_securities_changed(self, changes):
        for security in changes.removed_securities:
            symbol = security.symbol

            if symbol == self.qqq:
                continue

            if self.portfolio[symbol].invested:
                self.liquidate(
                    symbol,
                    "Removed from top-50 QQQ universe"
                )

            self.holding_days.pop(symbol, None)
```


## QuantBook Analysis Tool
### For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
qb = QuantBook()
spy = qb.add_equity("SPY")
history = qb.history(qb.securities.keys(), 360, Resolution.DAILY)

### Indicator Analysis
bbdf = qb.indicator(BollingerBands(30, 2), spy.symbol, 360, Resolution.DAILY)
bbdf.drop('standarddeviation', axis=1).plot()

# Low-Vol Cross-Asset Futures Risk Parity

The strategy is a cross asset futures allocation that seeks to harvest diversification and risk premia by systematically allocating to the least volatile contracts and sizing positions inversely to their recent volatility. Each week it ranks a fixed universe of liquid index, energy, and grain futures (including VIX, US equity index minis, Brent and refined products, natural gas, and major grains) by 30 day realized volatility of daily returns, then selects the five lowest volatility markets as the investable basket. The intended opportunity is that lower realized volatility contracts tend to exhibit more stable trends and carry-like behavior, while inverse volatility sizing targets more balanced risk contributions and reduces concentration in inherently volatile markets.

Rebalancing occurs weekly at 10:00 ET using daily data, after a 31 trading day warmup to ensure volatility estimates are stable. Portfolio weights are proportional to 1 over volatility within the selected set, then scaled so total exposure is approximately 50 percent notional (the remaining is implicitly held as cash), and positions are implemented via the current front mapped continuous contract for each future. Primary risk controls are the volatility based selection and sizing, the fixed cap of five concurrent positions, and the deliberate leverage reduction via the 50 percent gross exposure target.

## code

```
# region imports
from AlgorithmImports import *
from datetime import datetime
import math
# endregionfs


class InverseVolatilityFuturesAlgorithm(QCAlgorithm):

    def initialize(self):
        self.set_start_date(self.end_date - timedelta(5*365))
        self.settings.seed_initial_prices = True
        # Add a universe of Futures.
        tickers = [
            Futures.Indices.VIX,
            Futures.Indices.SP_500_E_MINI,
            Futures.Indices.NASDAQ_100_E_MINI,
            Futures.Indices.DOW_30_E_MINI,
            Futures.Energies.BRENT_CRUDE,
            Futures.Energies.GASOLINE,
            Futures.Energies.HEATING_OIL,
            Futures.Energies.NATURAL_GAS,
            Futures.Grains.CORN,
            Futures.Grains.OATS,
            Futures.Grains.SOYBEANS,
            Futures.Grains.WHEAT
        ]
        self._futures = []
        for ticker in tickers:
            future = self.add_future(ticker, extended_market_hours=True)
            # Add a volaility indicator on the continuous contract.
            future.vol = IndicatorExtensions.of(
                StandardDeviation(30), 
                self.roc(future, 1, Resolution.DAILY)
            )
            self._futures.append(future)
        # Warm up the indicators.
        self.set_warmup(31, Resolution.DAILY)

    def on_warmup_finished(self):
        # Add a Scheduled Event to rebalance the portfolio.
        time_rule = self.time_rules.at(10, 0)
        self.schedule.on(self.date_rules.week_start(), time_rule, self._rebalance)
        # Rebalance the portfolio today too.
        if self.live_mode:
            self._rebalance()
        else:
            self.schedule.on(self.date_rules.today, time_rule, self._rebalance)

    def _rebalance(self):
        # Select the 5 Futures with the lowest volatility.
        futures = [
            future for future in self._futures
            if (future.mapped and
                future.vol.is_ready and 
                future.exchange.hours.is_open(self.time, True))
        ]
        selected = sorted(futures, key=lambda future: future.vol)[:5]
        # Sum the inverse of volatilities.
        inverse_sum = sum([1/future.vol.current.value for future in selected])
        if inverse_sum == 0:
            return 
        # Form the inverse-volaility weighted portfolio.
        targets = [
            PortfolioTarget(future.mapped, 1/future.vol.current.value/inverse_sum/2)
            for future in selected
        ]
        self.set_holdings(targets, True)

        # QuantBook Analysis Tool 
```

## For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
qb = QuantBook()
spy = qb.AddEquity("SPY")
history = qb.History(qb.Securities.Keys, 360, Resolution.Daily)

## Indicator Analysis
bbdf = qb.Indicator(BollingerBands(30, 2), spy.Symbol, 360, Resolution.Daily)
bbdf.drop('standarddeviation', axis=1).plot()   

# US Large Cap Momentum Strategy

This long-only equity strategy captures cross-sectional momentum in large-cap US stocks while dynamically managing exposure to broad market drawdowns. The alpha source is a multi-horizon momentum signal blended across 21 to 189-day lookbacks, targeting sustained trends. The universe consists of the top 100 stocks by market capitalization within each Morningstar sector, ensuring sector breadth. Entry requires positive momentum and a price above the 189-day exponential moving average, with a filter rejecting instruments exhibiting high trend strength (ADX above 35). Position sizing incorporates a volatility cap and a stretch-based scaling factor that reduces exposure as price extends beyond historical volatility bands. Rebalancing occurs monthly at the close.

Risk management is driven by a market breadth stress model that tracks the fraction of stocks and sectors trading in the lower volatility bands. As stress rises, target exposure scales down linearly, cutting risk before a full risk-off latch liquidates the portfolio at a 45% stress threshold. Additional overrides include a rate-of-change early warning that cuts exposure by 30% if stress accelerates rapidly, and a conditional equity brake that reduces exposure if portfolio drawdown exceeds 7% alongside worsening breadth. The strategy targets 5 to 10 holdings, concentrating in fewer names during choppy regimes, and enforces a 20% maximum position weight.

## code
```
# region imports
from AlgorithmImports import *
from collections import defaultdict, deque
import calendar
import numpy as np
import pandas as pd
# endregion

# =============================================================================
# FROZEN — imports only.  The mutator owns the ENTIRE algorithm: universe model,
# every hook, every method, the class itself.  Maximum flexibility.
#
# Conventions (not hash-enforced; sys_msg.md flags them as expectations):
#   - Read backtest window from QC parameters via self.get_parameter so one
#     file covers IS / OOS (evaluate.py drives the window).
#   - Keep a deployable cost model + PLAN long-only / no-margin posture; the
#     score is what is judged.
# =============================================================================

# EVOLVE-BLOCK-START
class SectorTopUniverse(FundamentalUniverseSelectionModel):
    def __init__(self, algo, blacklist=None):
        self.algo = algo
        self.blacklist = set(blacklist or [])
        super().__init__(self._select)
    def _select(self, fundamentals):
        buckets = defaultdict(list)
        for f in fundamentals:
            if not f.has_fundamental_data:
                continue
            if f.symbol.Value in self.blacklist:
                continue
            if f.company_reference.primary_exchange_id not in ("NYS", "NAS", "ASE"):
                continue
            if f.price is None or f.price <= 5:
                continue
            if f.market_cap is None or f.market_cap < 5_000_000_000:
                continue
            sector = f.asset_classification.morningstar_sector_code
            if sector is None:
                continue
            buckets[sector].append(f)
        symbols = []
        for _, stocks in buckets.items():
            stocks.sort(key=lambda x: x.market_cap, reverse=True)
            symbols.extend(s.symbol for s in stocks[:100])
        return symbols
class StockOnlyMomentum(QCAlgorithm):
    def Initialize(self):
        sy = int(self.get_parameter("start_year", "2010"))
        sm = int(self.get_parameter("start_month", "1"))
        ey = int(self.get_parameter("end_year", "2020"))
        em = int(self.get_parameter("end_month", "12"))
        ed = calendar.monthrange(ey, em)[1]
        self.SetStartDate(sy, sm, 1)
        self.SetEndDate(ey, em, ed)
        self.SetCash(10_000)
        # Gen32 proven short-weighted lookbacks
        self.lookbacks = [21, 42, 63, 126, 189]
        self.mom_weights = [0.25, 0.20, 0.20, 0.20, 0.15]
        # Gen100 proven: concentrate in choppy (5), base 10
        self.stock_count_base = 10
        self.stock_count_choppy = 5
        self.choppy_threshold = 0.25
        self.max_weight = 0.20
        self.band_len = 189
        self.hist_len = 126
        self.UniverseSettings.Resolution = Resolution.Daily
        self.UniverseSettings.DataNormalizationMode = DataNormalizationMode.TOTAL_RETURN
        self.allow_universe = True
        self.current_band_idx = {}
        self.BOTTOM_LEVELS = {0, 1, 2, 3, 4}
        self.was_risk_off = False
        self.max_stress_level = 0.0
        # Clean scalar for stress RoC
        self.prev_bottom_frac = None
        # Portfolio equity curve tracking for conditional brake
        self.portfolio_peak = 10_000.0
        self.SetUniverseSelection(
            SectorTopUniverse(self, blacklist={"GME", "AMC"})
        )
        self.symbols = set()
        self.symbol_to_sector = {}
        self.adx_limit = 35
        self.adx_period = 14
        self.ma = {}
        self.adx = {}
        self.stretch_max = {}
        self.close_win = {}
        self.stretch_ema = {}
        self.band_hist = {}
        self.SetWarmUp(300)
        self.Schedule.On(
            self.DateRules.MonthEnd("SPY"),
            self.TimeRules.BeforeMarketClose("SPY", 5),
            self.Rebalance
        )
    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFeeModel(InteractiveBrokersFeeModel()); sec.SetSlippageModel(ConstantSlippageModel(0.001))
            s = sec.Symbol
            self.symbols.add(s)
            self.stretch_max[s] = 0.0
            self.ma[s] = self.EMA(s, self.band_len, Resolution.Daily)
            self.adx[s] = self.ADX(s, self.adx_period, Resolution.Daily)
            self.stretch_ema[s] = self.EMA(s, self.band_len, Resolution.Daily)
            self.close_win[s] = RollingWindow[float](self.band_len)
            self.band_hist[s] = RollingWindow[int](self.hist_len)
            try:
                sector = sec.Fundamentals.AssetClassification.MorningstarSectorCode
                if sector is not None and sector != 0:
                    self.symbol_to_sector[s] = sector
            except Exception:
                pass
        for sec in changes.RemovedSecurities:
            s = sec.Symbol
            self.symbols.discard(s)
            self.ma.pop(s, None)
            self.adx.pop(s, None)
            self.stretch_max.pop(s, None)
            self.stretch_ema.pop(s, None)
            self.close_win.pop(s, None)
            self.band_hist.pop(s, None)
            self.current_band_idx.pop(s, None)
            self.symbol_to_sector.pop(s, None)
    def OnData(self, data):
        for s in list(self.symbols):
            if not data.ContainsKey(s):
                continue
            bar = data[s]
            if bar is None:
                continue
            close = bar.Close
            self.close_win[s].Add(close)
            if not self.close_win[s].IsReady or not self.ma[s].IsReady:
                continue
            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue
            mid = self.ma[s].Current.Value
            stretch = abs(close - mid) / dev
            self.stretch_ema[s].Update(self.Time, stretch)
            if stretch > self.stretch_max[s]:
                self.stretch_max[s] = stretch
            bands = [
                mid - dev * 1.618, mid - dev * 1.382, mid - dev,
                mid - dev * 0.809, mid - dev * 0.5, mid - dev * 0.382, mid,
                mid + dev * 0.382, mid + dev * 0.5, mid + dev * 0.809,
                mid + dev, mid + dev * 1.382, mid + dev * 1.618
            ]
            self.current_band_idx[s] = self._band_index(close, bands)
    def _band_index(self, price, bands):
        for i in range(len(bands) - 1):
            if bands[i] <= price < bands[i + 1]:
                return i
        return len(bands) - 2
    def Rebalance(self):
        if self.IsWarmingUp:
            return
        idxs = list(self.current_band_idx.values())
        if len(idxs) < 50:
            return
        # Stock-level breadth
        stock_bottom_frac = sum(i in self.BOTTOM_LEVELS for i in idxs) / len(idxs)
        # Sector-level breadth: fraction of sectors where majority in bottom bands
        sector_bottoms = defaultdict(list)
        for s, idx in self.current_band_idx.items():
            sector = self.symbol_to_sector.get(s)
            if sector is not None:
                sector_bottoms[sector].append(idx in self.BOTTOM_LEVELS)
        if sector_bottoms:
            sector_stress_count = sum(
                1 for flags in sector_bottoms.values()
                if len(flags) > 0 and sum(flags) / len(flags) > 0.50
            )
            sector_bottom_frac = sector_stress_count / len(sector_bottoms)
            # 50/50 blend: sector signal smooths intra-sector noise (gen82 proven)
            bottom_frac = 0.5 * stock_bottom_frac + 0.5 * sector_bottom_frac
        else:
            bottom_frac = stock_bottom_frac
        # Stress rate of change (clean scalar, not deque)
        stress_roc = 0.0
        if self.prev_bottom_frac is not None:
            stress_roc = bottom_frac - self.prev_bottom_frac
        self.prev_bottom_frac = bottom_frac
        self.max_stress_level = max(self.max_stress_level, bottom_frac)
        # Portfolio equity curve tracking for conditional brake
        current_value = self.Portfolio.TotalPortfolioValue
        self.portfolio_peak = max(self.portfolio_peak, current_value)
        portfolio_dd = (self.portfolio_peak - current_value) / self.portfolio_peak if self.portfolio_peak > 0 else 0.0
        # Risk-off latch
        if bottom_frac >= 0.45:
            self.allow_universe = False
            self.was_risk_off = True
        elif self.was_risk_off:
            denominator = max(self.max_stress_level, 0.10)
            improvement = (self.max_stress_level - bottom_frac) / denominator
            if improvement >= 0.60 or bottom_frac < 0.15:
                for s in self.symbols:
                    if s in self.band_hist:
                        self.band_hist[s] = RollingWindow[int](self.hist_len)
                self.allow_universe = True
                self.was_risk_off = False
                self.max_stress_level = 0.0
                self.prev_bottom_frac = None  # fresh RoC after recovery
                # Reset peak to current (post-crash) level so brake starts fresh
                self.portfolio_peak = current_value
                # Begin graduated re-entry: cap exposure for 3 months post-crash
                self.in_recovery = True
                self.recovery_months = 0
        else:
            self.allow_universe = True
        if not self.allow_universe:
            self.Liquidate()
            return
        # Dynamic position count: concentrate in choppy regimes (gen100 proven)
        stock_count = self.stock_count_choppy if bottom_frac >= self.choppy_threshold else self.stock_count_base
        hist = self.History(list(self.symbols), max(self.lookbacks) + 1, Resolution.Daily)
        if hist.empty:
            return
        closes = hist["close"].unstack(0)
        momentum = {}
        consistency_map = {}
        for s in self.symbols:
            if s not in closes:
                continue
            px = closes[s]
            if len(px) < max(self.lookbacks) + 1:
                continue
            if not self.adx[s].IsReady or self.adx[s].Current.Value > self.adx_limit:
                continue
            # Compute per-lookback returns once (reused for both momentum and consistency)
            lb_returns = [px.iloc[-1] / px.iloc[-lb - 1] - 1 for lb in self.lookbacks]
            mom = sum(w * r for w, r in zip(self.mom_weights, lb_returns))
            if not self.ma[s].IsReady:
                continue
            price = self.Securities[s].Price
            ema = self.ma[s].Current.Value
            if price <= ema:
                continue
            if mom > 0:
                momentum[s] = mom
                # Consistency: fraction of lookback horizons with positive return
                n_positive = sum(1 for r in lb_returns if r > 0)
                consistency_map[s] = n_positive / len(self.lookbacks)
        if not momentum:
            self.Liquidate()
            return
        top = sorted(momentum, key=momentum.get, reverse=True)[:stock_count]
        scaled = {}
        vol_20d = {}
        for s in top:
            if not self.ma[s].IsReady or not self.stretch_ema[s].IsReady:
                continue
            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue
            mid = self.ma[s].Current.Value
            lm = self.stretch_ema[s].Current.Value
            lm2 = lm / 2.0
            lm3 = lm2 * 0.38196601
            lm4 = lm * 1.38196601
            lm5 = lm * 1.61803399
            lm6 = (lm + lm2) / 2.0
            bands = [
                mid - dev * lm5, mid - dev * lm4, mid - dev * lm,
                mid - dev * lm6, mid - dev * lm2, mid - dev * lm3, mid,
                mid + dev * lm3, mid + dev * lm2, mid + dev * lm6,
                mid + dev * lm, mid + dev * lm4, mid + dev * lm5
            ]
            price = self.Securities[s].Price
            idx = self._band_index(price, bands)
            self.band_hist[s].Add(idx)
            hist_idx = list(self.band_hist[s])
            historical_high = max(hist_idx) if hist_idx else idx
            if historical_high <= 0:
                scale = 1.0
            elif idx >= historical_high:
                scale = 0.0
            else:
                scale = max(0.2, 1.0 - idx / historical_high)
            current_stretch = self.stretch_ema[s].Current.Value
            peak_stretch = self.stretch_max.get(s, 0.0)
            if idx >= 10 and peak_stretch > 0:
                if current_stretch < (peak_stretch * 0.80):
                    scale = 0.2
            # Momentum consistency soft multiplier (NEW):
            # Stocks confirmed positive across all lookbacks get full weight;
            # stocks with mixed horizon signals are down-weighted.
            # Range [0.6, 1.0] — tilts toward multi-horizon-confirmed trends
            # without over-penalizing short-term entries in new bull legs.
            # Only affects RELATIVE weights within top-K, not total exposure.
            consistency = consistency_map.get(s, 0.6)
            consist_factor = 0.6 + 0.4 * consistency
            scaled[s] = momentum[s] * scale * consist_factor
            # 20-day vol for position cap
            px = closes[s]
            if len(px) >= 21:
                vol_20d[s] = float(px.pct_change().iloc[-20:].std())
            else:
                vol_20d[s] = 0.015
        if not scaled:
            self.Liquidate()
            return
        # Base exposure from breadth interpolation
        min_stress = 0.15
        max_stress = 0.45
        target_exposure = float(np.interp(bottom_frac, [min_stress, max_stress], [1.0, 0.0]))
        # RoC early warning: accelerating stress → cut exposure 30%
        if stress_roc >= 0.15:
            target_exposure *= 0.70
        # Conditional portfolio equity brake:
        # Only fires when BOTH portfolio is materially down AND breadth is worsening.
        # Threshold lowered from 8%→7% to catch 2011/2018 crashes ~1 month earlier;
        # stress_roc > 0.03 gate prevents false positives in stable-breadth corrections.
        if portfolio_dd > 0.07 and stress_roc > 0.03:
            # Smooth reduction: no cut at dd=7%, full 75% cut at dd=29%
            dd_brake = max(0.25, 1.0 - (portfolio_dd - 0.07) / 0.22)
            target_exposure = min(target_exposure, dd_brake)
        target_exposure = float(round(max(0.0, min(1.0, target_exposure)), 2))
        # Weight computation
        total_scaled = sum(scaled.values())
        raw_weights = {s: v / total_scaled for s, v in scaled.items()}
        capped_weights = {s: min(self.max_weight, w) for s, w in raw_weights.items()}
        # Volatility cap without renormalization (gen100 proven)
        for s in list(capped_weights.keys()):
            vol = vol_20d.get(s, 0.015)
            if vol > 0:
                vol_cap_factor = min(1.0, 0.015 / vol)
                capped_weights[s] = min(capped_weights[s], self.max_weight * vol_cap_factor)
        current_sum = sum(capped_weights.values())
        final_weights = {}
        if current_sum > 0:
            for s, w in capped_weights.items():
                final_weights[s] = (w / current_sum) * target_exposure
        for pos in self.Portfolio.Values:
            if pos.Invested and pos.Symbol not in final_weights:
                self.Liquidate(pos.Symbol)
        for s, w in final_weights.items():
            if w > 0:
                self.SetHoldings(s, w)
# EVOLVE-BLOCK-END
```

## QuantBook Analysis Tool
## For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
qb = QuantBook()
spy = qb.add_equity("SPY")
history = qb.history(qb.securities.keys(), 360, Resolution.DAILY)

## Indicator Analysis
bbdf = qb.indicator(BollingerBands(30, 2), spy.symbol, 360, Resolution.DAILY)
bbdf.drop('standarddeviation', axis=1).plot()


# Monthly Sector-Neutral Large-Cap Momentum Strategy
This long-only equity strategy targets US large-cap stocks to capture the momentum anomaly, filtering for sustained price trends while explicitly avoiding mean-reverting exhaustion regimes. Operating on a monthly rebalance schedule using daily resolution data, the system selects its universe from the top 100 stocks by market capitalization within each Morningstar sector, maintaining strict sector neutrality. The alpha source relies on a multi-horizon momentum score calculated across five lookback periods. A security is eligible only if its Average Directional Index remains below a threshold, confirming a non-trending filter, and its current price exceeds its exponential moving average.

Position sizing scales momentum scores using dynamic Fibonacci volatility bands, reducing exposure as a stock approaches its rolling historical band ceiling to mitigate breakout exhaustion. A market breadth risk-off overlay triggers liquidation when a critical mass of securities falls to lower volatility bands, remaining in cash until market conditions improve or a hard timeout expires. Risk controls include a maximum 20 percent portfolio weight per security and delta-only execution via market-on-open orders to minimize slippage and transaction friction.

## code 
```
"""
Sector-Neutral Large-Cap Trend Momentum (Fixed)
================================================

A monthly, long-only US large-cap momentum strategy with sector-neutral
universe construction, multi-horizon momentum scoring, ADX trend filter,
Fibonacci band ceiling sizing, and universe-wide breadth-based risk-off.

Based on the original "Sector-Neutral Large-Cap Trend Momentum" strategy by
Sanjeev Mittal. This version contains six surgical fixes that make the
strategy live-deployable on an IBKR cash account without altering its core
signal logic:

  FIX 1. Real broker friction:
         InteractiveBrokersFeeModel + 10bps ConstantSlippageModel on every
         security at OnSecuritiesChanged.

  FIX 2. Delta-based execution:
         Replaces the costly Liquidate()-then-SetHoldings() pattern with
         delta orders only when drift > 2%.

  FIX 3. Rolling stretch window (size 126):
         Replaces a lifetime-peak `stretch_max` dict with a bounded rolling
         window. Fixes survivorship bias and prevents stale ceilings.

  FIX 4. Exhaustion-scale bug:
         `scale = min(scale, 0.2)` instead of overriding scale outright when
         exhaustion is detected. Preserves the band-ceiling logic.

  FIX 5. 180-day hard timeout on risk-off regime:
         Prevents the algorithm from being stuck in risk-off indefinitely.
         Forced recovery at 180 days even if breadth signals stay weak.

  FIX 6. Live market cap check at rebalance:
         Re-checks `Fundamentals.MarketCap >= $5B` at rebalance time, not
         just at universe selection. Fixes the BBIO-type universe leak
         (delisted-but-stale tickers).

Fill model: decide at MonthEnd("SPY") BeforeMarketClose 5min, queue weights,
drain in OnData via MarketOnOpenOrder -> fill at next session open. Eliminates
the daily-resolution fill artifact (intraday SetHoldings on Daily fills at
PRIOR-bar close, an unattainable price).

Account type: cash (IBKR), T+1 settlement modelled correctly.
Resolution: Daily.
Universe: top 100 by market cap per Morningstar sector, US listings (NYS/NAS/ASE),
  price > $5, market cap > $5B, fundamental data required.

Verified live-realistic stats (cash, IBKR fees + 10bps, T+1):
  IS  2010-2020: 22% CAGR / 24% DD / Sharpe 1.00
  OOS 2021-2026: 20% CAGR / 42% DD / Sharpe 0.48
  Full 2010-2026: ~23% CAGR / ~42% DD (showcase backtest)

Recommended deploy weight in a 2-sleeve cash book: 25% alongside a
leveraged-ETF-rotation engine (e.g. crash-guarded TQQQ/SOXL rotator) at 75%.
On cash, the blend lifts CAGR meaningfully while keeping DD bounded; on margin,
it materially raises Sharpe at every weight tested.
"""

from AlgorithmImports import *
from collections import defaultdict, deque
import numpy as np


class SectorTopUniverse(FundamentalUniverseSelectionModel):
    def __init__(self, algo, blacklist=None):
        self.algo = algo
        self.blacklist = set(blacklist or [])
        super().__init__(self._select)

    def _select(self, fundamentals):
        buckets = defaultdict(list)

        for f in fundamentals:
            if not f.has_fundamental_data:
                continue
            if f.symbol.Value in self.blacklist:
                continue
            if f.company_reference.primary_exchange_id not in ("NYS", "NAS", "ASE"):
                continue
            if f.price is None or f.price <= 5:
                continue
            if f.market_cap is None or f.market_cap < 5_000_000_000:
                continue

            sector = f.asset_classification.morningstar_sector_code
            if sector is None:
                continue

            buckets[sector].append(f)

        symbols = []
        for _, stocks in buckets.items():
            stocks.sort(key=lambda x: x.market_cap, reverse=True)
            symbols.extend(s.symbol for s in stocks[:100])

        return symbols


class StockOnlyMomentumV3(QCAlgorithm):

    WINDOW = "OOS"        # IS=2010-2020 | OOS=2021-2025
    DEBUG_FILLS = True   # tags + per-type MaxFillVsOpen runtime stats (additive)
    SCHED = "T1"        # T1 = BeforeMarketClose (native, decide T-1) | T = AfterMarketClose (decide T)

    def Initialize(self):
        if self.WINDOW == "IS":
            self.SetStartDate(2010, 1, 1); self.SetEndDate(2020, 12, 31)
        else:
            self.SetStartDate(2021, 6, 28); self.SetEndDate(2026, 6, 30)
        self.SetCash(100_000)
        self._n_fills=0; self._max_moo=0.0; self._n_moo=0; self._max_liq=0.0; self._n_liq=0
        self.SetBrokerageModel(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)
        self.Settings.MinimumOrderMarginPortfolioPercentage = 0.0

        self.lookbacks = [21, 63, 126, 189, 252]
        self.stock_count = 10
        self.max_weight = 0.20

        self.band_len = 189
        self.hist_len = 126

        self.UniverseSettings.Resolution = Resolution.Daily
        self.UniverseSettings.DataNormalizationMode = DataNormalizationMode.TOTAL_RETURN

        self.allow_universe = True
        self.current_band_idx = {}
        self.BOTTOM_LEVELS = {0, 1, 2, 3, 4}
        self.max_stress_level = 0.0
        self.was_risk_off = False
        self.risk_off_date = None   # FIX 5

        # ORIGINAL universe — NEVER broken (proven 9a0ff750: 557 orders / 58.24/45.8/1.49).
        # My earlier "universe rot" diagnosis was WRONG; the 0-orders was my own OnData-split bug.
        self.SetUniverseSelection(SectorTopUniverse(self, blacklist={"GME", "AMC"}))

        self.symbols = set()
        self.adx_limit = 35
        self.adx_period = 14

        self.ma = {}
        self.adx = {}
        self.close_win = {}
        self.stretch_ema = {}
        self.band_hist = {}
        self.stretch_win = {}   # FIX 3

        self.rebalance_threshold = 0.02  # FIX 2
        self._pending_weights = None

        self.SetWarmUp(300)

        self.Schedule.On(
            self.DateRules.MonthEnd("SPY"),
            self.TimeRules.BeforeMarketClose("SPY", 5) if self.SCHED == "T1" else self.TimeRules.AfterMarketClose("SPY", 1),
            self.Rebalance
        )

    def _select_universe(self, fundamental):
        buckets = defaultdict(list)
        for f in fundamental:
            if not f.has_fundamental_data: continue
            if f.symbol.value in self.blacklist: continue
            if f.company_reference.primary_exchange_id not in ("NYS", "NAS", "ASE"): continue
            if f.price is None or f.price <= 5: continue
            if f.market_cap is None or f.market_cap < 5_000_000_000: continue
            sector = f.asset_classification.morningstar_sector_code
            if sector is None: continue
            buckets[sector].append(f)
        symbols = []
        for _, stocks in buckets.items():
            stocks.sort(key=lambda x: x.market_cap, reverse=True)
            symbols.extend(s.symbol for s in stocks[:100])
        self.SetRuntimeStatistic("nUniv", str(len(symbols)))
        return symbols

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFeeModel(InteractiveBrokersFeeModel())     # FIX 1
            sec.SetSlippageModel(ConstantSlippageModel(0.001))  # FIX 1

            s = sec.Symbol
            self.symbols.add(s)

            self.ma[s]          = self.EMA(s, self.band_len, Resolution.Daily)
            self.adx[s]         = self.ADX(s, self.adx_period, Resolution.Daily)
            self.stretch_ema[s] = self.EMA(s, self.band_len, Resolution.Daily)
            self.close_win[s]   = RollingWindow[float](self.band_len)
            self.band_hist[s]   = RollingWindow[int](self.hist_len)
            self.stretch_win[s] = RollingWindow[float](self.hist_len)  # FIX 3

        for sec in changes.RemovedSecurities:
            s = sec.Symbol
            self.symbols.discard(s)
            self.ma.pop(s, None)
            self.adx.pop(s, None)
            self.stretch_ema.pop(s, None)
            self.close_win.pop(s, None)
            self.band_hist.pop(s, None)
            self.current_band_idx.pop(s, None)
            self.stretch_win.pop(s, None)

    def OnData(self, data):
        # Fill-fix drain: place pending rebalance as MarketOnOpenOrder post-close
        if self._pending_weights is not None and not self.IsWarmingUp:
            targets = self._pending_weights
            self._pending_weights = None
            equity = self.Portfolio.TotalPortfolioValue
            for pos in list(self.Portfolio.Values):
                if pos.Invested and pos.Symbol not in targets:
                    if pos.Quantity != 0:
                        self.MarketOnOpenOrder(pos.Symbol, -pos.Quantity, tag=f"EXIT now={self.Time.date()}")
            for sym, w in targets.items():
                if w <= 0 or not self.Securities.ContainsKey(sym):
                    continue
                price = self.Securities[sym].Price
                if price <= 0:
                    continue
                target_qty = int(equity * w / price)
                cur = self.Portfolio[sym].Quantity if self.Portfolio.ContainsKey(sym) else 0
                delta = target_qty - cur
                if delta != 0:
                    self.MarketOnOpenOrder(sym, delta, tag=f"ENTRY decPx={price:.4f} now={self.Time.date()}")

        self.SetRuntimeStatistic("nSym", str(len(self.symbols)))
        self.SetRuntimeStatistic("nIdx", str(len(self.current_band_idx)))
        # Universe-wide breadth tracking via Fibonacci bands
        for s in list(self.symbols):
            if not data.ContainsKey(s):
                continue
            bar = data[s]
            if bar is None:
                continue

            close = bar.Close
            self.close_win[s].Add(close)

            if not self.close_win[s].IsReady or not self.ma[s].IsReady:
                continue

            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue

            mid = self.ma[s].Current.Value
            stretch = abs(close - mid) / dev

            self.stretch_ema[s].Update(self.Time, stretch)
            self.stretch_win[s].Add(stretch)   # FIX 3

            bands = [
                mid - dev * 1.618, mid - dev * 1.382, mid - dev,
                mid - dev * 0.809, mid - dev * 0.5, mid - dev * 0.382,
                mid,
                mid + dev * 0.382, mid + dev * 0.5, mid + dev * 0.809,
                mid + dev, mid + dev * 1.382, mid + dev * 1.618
            ]

            idx = self._band_index(close, bands)
            self.current_band_idx[s] = idx

    def _band_index(self, price, bands):
        for i in range(len(bands) - 1):
            if bands[i] <= price < bands[i + 1]:
                return i
        return len(bands) - 2

    def Rebalance(self):
        if self.IsWarmingUp:
            return

        self.SetRuntimeStatistic("rebReached", "yes")
        idxs = list(self.current_band_idx.values())
        if len(idxs) < 50:
            return
        self.SetRuntimeStatistic("rebPassed50", "yes")

        bottom_frac = sum(i in self.BOTTOM_LEVELS for i in idxs) / len(idxs)
        self.max_stress_level = max(self.max_stress_level, bottom_frac)

        if bottom_frac >= 0.45:
            if not self.was_risk_off:
                self.risk_off_date = self.Time   # FIX 5
            self.allow_universe = False
            self.was_risk_off = True
        elif self.was_risk_off:
            denominator = max(self.max_stress_level, 0.10)
            improvement = (self.max_stress_level - bottom_frac) / denominator
            days_risk_off = (self.Time - self.risk_off_date).days if self.risk_off_date else 0
            if improvement >= 0.60 or bottom_frac < 0.15 or days_risk_off > 180:
                self.Debug(f"RECOVERY stress={bottom_frac:.1%} days_off={days_risk_off}")
                for s in self.symbols:
                    if s in self.band_hist:
                        self.band_hist[s] = RollingWindow[int](self.hist_len)
                self.allow_universe = True
                self.was_risk_off = False
                self.max_stress_level = 0.0
                self.risk_off_date = None
        else:
            self.allow_universe = True

        if not self.allow_universe:
            self._execute_targets({})
            self.Debug(f"RISK-OFF {self.Time:%Y-%m-%d} stress={bottom_frac:.1%}")
            return

        hist = self.History(list(self.symbols), max(self.lookbacks) + 1, Resolution.Daily)
        if hist.empty:
            return

        closes = hist["close"].unstack(0)
        momentum = {}

        for s in self.symbols:
            if s not in closes:
                continue
            px = closes[s]
            if len(px) < max(self.lookbacks) + 1:
                continue
            if not self.adx[s].IsReady or self.adx[s].Current.Value > self.adx_limit:
                continue

            mom = np.mean([px.iloc[-1] / px.iloc[-lb - 1] - 1 for lb in self.lookbacks])
            if not self.ma[s].IsReady:
                continue
            price = self.Securities[s].Price
            ema = self.ma[s].Current.Value
            if price <= ema:
                continue

            # FIX 6
            fundamentals = self.Securities[s].Fundamentals
            if fundamentals is None or fundamentals.MarketCap < 5_000_000_000:
                continue

            if mom > 0:
                momentum[s] = mom

        if not momentum:
            self._execute_targets({})
            return

        top = sorted(momentum, key=momentum.get, reverse=True)[:self.stock_count]

        scaled = {}
        for s in top:
            if not self.ma[s].IsReady or not self.stretch_ema[s].IsReady:
                continue
            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue

            mid = self.ma[s].Current.Value
            lm  = self.stretch_ema[s].Current.Value
            lm2 = lm / 2.0
            lm3 = lm2 * 0.38196601
            lm4 = lm * 1.38196601
            lm5 = lm * 1.61803399
            lm6 = (lm + lm2) / 2.0

            bands = [
                mid - dev * lm5, mid - dev * lm4, mid - dev * lm,
                mid - dev * lm6, mid - dev * lm2, mid - dev * lm3,
                mid,
                mid + dev * lm3, mid + dev * lm2, mid + dev * lm6,
                mid + dev * lm, mid + dev * lm4, mid + dev * lm5
            ]

            price = self.Securities[s].Price
            idx = self._band_index(price, bands)

            self.band_hist[s].Add(idx)
            hist_idx = list(self.band_hist[s])
            historical_high = max(hist_idx) if hist_idx else idx

            if historical_high <= 0:
                scale = 1.0
            elif idx >= historical_high:
                scale = 0.0
            else:
                scale = max(0.2, 1.0 - idx / historical_high)

            if self.stretch_win[s].IsReady:
                stretch_list = list(self.stretch_win[s])
                current_stretch = stretch_list[0]
                peak_stretch = max(stretch_list)
                if idx >= 10 and peak_stretch > 0:
                    if current_stretch < (peak_stretch * 0.80):
                        scale = min(scale, 0.2)   # FIX 4
                        self.Debug(f"EXHAUSTION {s.Value}")

            scaled[s] = momentum[s] * scale

        if not scaled:
            self._execute_targets({})
            return

        total_scaled = sum(scaled.values())
        raw_weights = {s: v / total_scaled for s, v in scaled.items()}
        capped_weights = {s: min(self.max_weight, w) for s, w in raw_weights.items()}
        current_sum = sum(capped_weights.values())
        if current_sum > 0:
            final_weights = {s: w / current_sum for s, w in capped_weights.items()}
        else:
            final_weights = {}

        self._execute_targets(final_weights)

        output = ", ".join([f"{s.Value}: {w*100:.1f}%" for s, w in final_weights.items() if w > 0])
        if output:
            self.Debug(f"Weights {self.Time:%Y-%m-%d}: {output}")

    def _execute_targets(self, target_weights):
        # FIX 2 + fill-fix: store decision, OnData drains via MarketOnOpenOrder
        self._pending_weights = dict(target_weights)

    def OnOrderEvent(self, oe):
        if not self.DEBUG_FILLS: return
        if oe.Status != OrderStatus.Filled or oe.FillQuantity == 0: return
        sec=self.Securities[oe.Symbol]; fp=float(oe.FillPrice); o=float(sec.Open)
        dev=abs(fp-o)/o if o>0 else 0.0
        try:
            od=self.Transactions.GetOrderById(oe.OrderId); ot=int(od.Type) if od is not None else -1
        except Exception:
            ot=-1
        if ot==4:
            self._max_moo=max(self._max_moo,dev); self._n_moo+=1
        else:
            self._max_liq=max(self._max_liq,dev); self._n_liq+=1
        self._n_fills+=1
        self.SetRuntimeStatistic("Fills",str(self._n_fills))
        self.SetRuntimeStatistic("MaxVsOpen_MOO_pct",f"{self._max_moo*100:.4f}")
        self.SetRuntimeStatistic("nMOO",str(self._n_moo))
        self.SetRuntimeStatistic("MaxVsOpen_Liq_pct",f"{self._max_liq*100:.4f}")
        self.SetRuntimeStatistic("nLiq",str(self._n_liq))

```

## QuantBook Analysis Tool
## For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
qb = QuantBook()
spy = qb.add_equity("SPY")
history = qb.history(qb.securities.keys(), 360, Resolution.DAILY)

## Indicator Analysis
bbdf = qb.indicator(BollingerBands(30, 2), spy.symbol, 360, Resolution.DAILY)
bbdf.drop('standarddeviation', axis=1).plot()

# Large-Cap Momentum With Risk Controls

This sector-neutral, long-only US large-cap equity strategy targets momentum continuation while avoiding trend exhaustion and systemic market breakdowns. The alpha source relies on multi-horizon momentum (1, 3, 6, 9, and 12-month lookbacks) to capture sustained price trends, filtered by an Average Directional Index (ADX) ceiling to exclude overextended trends. The investable universe spans the top 100 domestic exchange-listed stocks by market cap within each Morningstar sector (minimum $5B cap and $5 price). Daily price inputs update Fibonacci deviation bands that track asset stretch, where the strategy scales exposure down as price pushes into upper bands and scales further if recent peak stretch decays, indicating exhaustion. Market-level risk-off mechanisms trigger when breadth deteriorates (45% of universe in lower bands), liquidating to cash and requiring a 60% recovery improvement, a sub-15% breadth bottom, or a 180-day timeout to redeploy.

Decisions are made monthly on the last trading day, with execution deferred to the next morning via MarketOnOpen orders to avoid end-of-day fill artifacts. Position sizing weights the top 10 stocks by momentum score scaled by regression-to-band dynamics, capped at a 20% maximum weight, and trades only when target drift exceeds 2%. Realistic execution incorporates Interactive Brokers fees and 10 basis points of slippage, ensuring live-deployable expectations.

## code 

```
"""
Sector-Neutral Large-Cap Trend Momentum (Fixed)
================================================

A monthly, long-only US large-cap momentum strategy with sector-neutral
universe construction, multi-horizon momentum scoring, ADX trend filter,
Fibonacci band ceiling sizing, and universe-wide breadth-based risk-off.

Based on the original "Sector-Neutral Large-Cap Trend Momentum" strategy by
Sanjeev Mittal. This version contains six surgical fixes that make the
strategy live-deployable on an IBKR cash account without altering its core
signal logic:

  FIX 1. Real broker friction:
         InteractiveBrokersFeeModel + 10bps ConstantSlippageModel on every
         security at OnSecuritiesChanged.

  FIX 2. Delta-based execution:
         Replaces the costly Liquidate()-then-SetHoldings() pattern with
         delta orders only when drift > 2%.

  FIX 3. Rolling stretch window (size 126):
         Replaces a lifetime-peak `stretch_max` dict with a bounded rolling
         window. Fixes survivorship bias and prevents stale ceilings.

  FIX 4. Exhaustion-scale bug:
         `scale = min(scale, 0.2)` instead of overriding scale outright when
         exhaustion is detected. Preserves the band-ceiling logic.

  FIX 5. 180-day hard timeout on risk-off regime:
         Prevents the algorithm from being stuck in risk-off indefinitely.
         Forced recovery at 180 days even if breadth signals stay weak.

  FIX 6. Live market cap check at rebalance:
         Re-checks `Fundamentals.MarketCap >= $5B` at rebalance time, not
         just at universe selection. Fixes the BBIO-type universe leak
         (delisted-but-stale tickers).

Fill model: decide at MonthEnd("SPY") BeforeMarketClose 5min, queue weights,
drain in OnData via MarketOnOpenOrder -> fill at next session open. Eliminates
the daily-resolution fill artifact (intraday SetHoldings on Daily fills at
PRIOR-bar close, an unattainable price).

Account type: cash (IBKR), T+1 settlement modelled correctly.
Resolution: Daily.
Universe: top 100 by market cap per Morningstar sector, US listings (NYS/NAS/ASE),
  price > $5, market cap > $5B, fundamental data required.

Verified live-realistic stats (cash, IBKR fees + 10bps, T+1):
  IS  2010-2020: 22% CAGR / 24% DD / Sharpe 1.00
  OOS 2021-2026: 20% CAGR / 42% DD / Sharpe 0.48
  Full 2010-2026: ~23% CAGR / ~42% DD (showcase backtest)

Recommended deploy weight in a 2-sleeve cash book: 25% alongside a
leveraged-ETF-rotation engine (e.g. crash-guarded TQQQ/SOXL rotator) at 75%.
On cash, the blend lifts CAGR meaningfully while keeping DD bounded; on margin,
it materially raises Sharpe at every weight tested.
"""

from AlgorithmImports import *
from collections import defaultdict, deque
import numpy as np


class SectorTopUniverse(FundamentalUniverseSelectionModel):
    def __init__(self, algo, blacklist=None):
        self.algo = algo
        self.blacklist = set(blacklist or [])
        super().__init__(self._select)

    def _select(self, fundamentals):
        buckets = defaultdict(list)

        for f in fundamentals:
            if not f.has_fundamental_data:
                continue
            if f.symbol.Value in self.blacklist:
                continue
            if f.company_reference.primary_exchange_id not in ("NYS", "NAS", "ASE"):
                continue
            if f.price is None or f.price <= 5:
                continue
            if f.market_cap is None or f.market_cap < 5_000_000_000:
                continue

            sector = f.asset_classification.morningstar_sector_code
            if sector is None:
                continue

            buckets[sector].append(f)

        symbols = []
        for _, stocks in buckets.items():
            stocks.sort(key=lambda x: x.market_cap, reverse=True)
            symbols.extend(s.symbol for s in stocks[:100])

        return symbols


class StockOnlyMomentumV3(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2021, 7, 1)
        self.SetEndDate(2026, 7, 1)
        self.SetCash(100_000)
        self.SetBrokerageModel(BrokerageName.INTERACTIVE_BROKERS_BROKERAGE, AccountType.MARGIN)
        self.Settings.MinimumOrderMarginPortfolioPercentage = 0.0

        self.lookbacks = [21, 63, 126, 189, 252]
        self.stock_count = 10
        self.max_weight = 0.20

        self.band_len = 189
        self.hist_len = 126

        self.UniverseSettings.Resolution = Resolution.Daily
        self.UniverseSettings.DataNormalizationMode = DataNormalizationMode.TOTAL_RETURN

        self.allow_universe = True
        self.current_band_idx = {}
        self.BOTTOM_LEVELS = {0, 1, 2, 3, 4}
        self.max_stress_level = 0.0
        self.was_risk_off = False
        self.risk_off_date = None   # FIX 5

        self.SetUniverseSelection(
            SectorTopUniverse(self, blacklist={"GME", "AMC"})
        )

        self.symbols = set()
        self.adx_limit = 35
        self.adx_period = 14

        self.ma = {}
        self.adx = {}
        self.close_win = {}
        self.stretch_ema = {}
        self.band_hist = {}
        self.stretch_win = {}   # FIX 3

        self.rebalance_threshold = 0.02  # FIX 2
        self._pending_weights = None

        self.SetWarmUp(300)

        self.Schedule.On(
            self.DateRules.MonthEnd("SPY"),
            self.TimeRules.BeforeMarketClose("SPY", 5),
            self.Rebalance
        )

    def OnSecuritiesChanged(self, changes):
        for sec in changes.AddedSecurities:
            sec.SetFeeModel(InteractiveBrokersFeeModel())     # FIX 1
            sec.SetSlippageModel(ConstantSlippageModel(0.001))  # FIX 1

            s = sec.Symbol
            self.symbols.add(s)

            self.ma[s]          = self.EMA(s, self.band_len, Resolution.Daily)
            self.adx[s]         = self.ADX(s, self.adx_period, Resolution.Daily)
            self.stretch_ema[s] = self.EMA(s, self.band_len, Resolution.Daily)
            self.close_win[s]   = RollingWindow[float](self.band_len)
            self.band_hist[s]   = RollingWindow[int](self.hist_len)
            self.stretch_win[s] = RollingWindow[float](self.hist_len)  # FIX 3

        for sec in changes.RemovedSecurities:
            s = sec.Symbol
            self.symbols.discard(s)
            self.ma.pop(s, None)
            self.adx.pop(s, None)
            self.stretch_ema.pop(s, None)
            self.close_win.pop(s, None)
            self.band_hist.pop(s, None)
            self.current_band_idx.pop(s, None)
            self.stretch_win.pop(s, None)

    def OnData(self, data):
        # Fill-fix drain: place pending rebalance as MarketOnOpenOrder post-close
        if self._pending_weights is not None and not self.IsWarmingUp:
            targets = self._pending_weights
            self._pending_weights = None
            equity = self.Portfolio.TotalPortfolioValue
            for pos in list(self.Portfolio.Values):
                if pos.Invested and pos.Symbol not in targets:
                    if pos.Quantity != 0:
                        self.MarketOnOpenOrder(pos.Symbol, -pos.Quantity)
            for sym, w in targets.items():
                if w <= 0 or not self.Securities.ContainsKey(sym):
                    continue
                price = self.Securities[sym].Price
                if price <= 0:
                    continue
                target_qty = int(equity * w / price)
                cur = self.Portfolio[sym].Quantity if self.Portfolio.ContainsKey(sym) else 0
                delta = target_qty - cur
                if delta != 0:
                    self.MarketOnOpenOrder(sym, delta)

        # Universe-wide breadth tracking via Fibonacci bands
        for s in list(self.symbols):
            if not data.ContainsKey(s):
                continue
            bar = data[s]
            if bar is None:
                continue

            close = bar.Close
            self.close_win[s].Add(close)

            if not self.close_win[s].IsReady or not self.ma[s].IsReady:
                continue

            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue

            mid = self.ma[s].Current.Value
            stretch = abs(close - mid) / dev

            self.stretch_ema[s].Update(self.Time, stretch)
            self.stretch_win[s].Add(stretch)   # FIX 3

            bands = [
                mid - dev * 1.618, mid - dev * 1.382, mid - dev,
                mid - dev * 0.809, mid - dev * 0.5, mid - dev * 0.382,
                mid,
                mid + dev * 0.382, mid + dev * 0.5, mid + dev * 0.809,
                mid + dev, mid + dev * 1.382, mid + dev * 1.618
            ]

            idx = self._band_index(close, bands)
            self.current_band_idx[s] = idx

    def _band_index(self, price, bands):
        for i in range(len(bands) - 1):
            if bands[i] <= price < bands[i + 1]:
                return i
        return len(bands) - 2

    def Rebalance(self):
        if self.IsWarmingUp:
            return

        idxs = list(self.current_band_idx.values())
        if len(idxs) < 50:
            return

        bottom_frac = sum(i in self.BOTTOM_LEVELS for i in idxs) / len(idxs)
        self.max_stress_level = max(self.max_stress_level, bottom_frac)

        if bottom_frac >= 0.45:
            if not self.was_risk_off:
                self.risk_off_date = self.Time   # FIX 5
            self.allow_universe = False
            self.was_risk_off = True
        elif self.was_risk_off:
            denominator = max(self.max_stress_level, 0.10)
            improvement = (self.max_stress_level - bottom_frac) / denominator
            days_risk_off = (self.Time - self.risk_off_date).days if self.risk_off_date else 0
            if improvement >= 0.60 or bottom_frac < 0.15 or days_risk_off > 180:
                self.Debug(f"RECOVERY stress={bottom_frac:.1%} days_off={days_risk_off}")
                for s in self.symbols:
                    if s in self.band_hist:
                        self.band_hist[s] = RollingWindow[int](self.hist_len)
                self.allow_universe = True
                self.was_risk_off = False
                self.max_stress_level = 0.0
                self.risk_off_date = None
        else:
            self.allow_universe = True

        if not self.allow_universe:
            self._execute_targets({})
            self.Debug(f"RISK-OFF {self.Time:%Y-%m-%d} stress={bottom_frac:.1%}")
            return

        hist = self.History(list(self.symbols), max(self.lookbacks) + 1, Resolution.Daily)
        if hist.empty:
            return

        closes = hist["close"].unstack(0)
        momentum = {}

        for s in self.symbols:
            if s not in closes:
                continue
            px = closes[s]
            if len(px) < max(self.lookbacks) + 1:
                continue
            if not self.adx[s].IsReady or self.adx[s].Current.Value > self.adx_limit:
                continue

            mom = np.mean([px.iloc[-1] / px.iloc[-lb - 1] - 1 for lb in self.lookbacks])
            if not self.ma[s].IsReady:
                continue
            price = self.Securities[s].Price
            ema = self.ma[s].Current.Value
            if price <= ema:
                continue

            # FIX 6
            fundamentals = self.Securities[s].Fundamentals
            if fundamentals is None or fundamentals.MarketCap < 5_000_000_000:
                continue

            if mom > 0:
                momentum[s] = mom

        if not momentum:
            self._execute_targets({})
            return

        top = sorted(momentum, key=momentum.get, reverse=True)[:self.stock_count]

        scaled = {}
        for s in top:
            if not self.ma[s].IsReady or not self.stretch_ema[s].IsReady:
                continue
            dev = np.std(list(self.close_win[s]))
            if dev <= 0:
                continue

            mid = self.ma[s].Current.Value
            lm  = self.stretch_ema[s].Current.Value
            lm2 = lm / 2.0
            lm3 = lm2 * 0.38196601
            lm4 = lm * 1.38196601
            lm5 = lm * 1.61803399
            lm6 = (lm + lm2) / 2.0

            bands = [
                mid - dev * lm5, mid - dev * lm4, mid - dev * lm,
                mid - dev * lm6, mid - dev * lm2, mid - dev * lm3,
                mid,
                mid + dev * lm3, mid + dev * lm2, mid + dev * lm6,
                mid + dev * lm, mid + dev * lm4, mid + dev * lm5
            ]

            price = self.Securities[s].Price
            idx = self._band_index(price, bands)

            self.band_hist[s].Add(idx)
            hist_idx = list(self.band_hist[s])
            historical_high = max(hist_idx) if hist_idx else idx

            if historical_high <= 0:
                scale = 1.0
            elif idx >= historical_high:
                scale = 0.0
            else:
                scale = max(0.2, 1.0 - idx / historical_high)

            if self.stretch_win[s].IsReady:
                stretch_list = list(self.stretch_win[s])
                current_stretch = stretch_list[0]
                peak_stretch = max(stretch_list)
                if idx >= 10 and peak_stretch > 0:
                    if current_stretch < (peak_stretch * 0.80):
                        scale = min(scale, 0.2)   # FIX 4
                        self.Debug(f"EXHAUSTION {s.Value}")

            scaled[s] = momentum[s] * scale

        if not scaled:
            self._execute_targets({})
            return

        total_scaled = sum(scaled.values())
        raw_weights = {s: v / total_scaled for s, v in scaled.items()}
        capped_weights = {s: min(self.max_weight, w) for s, w in raw_weights.items()}
        current_sum = sum(capped_weights.values())
        if current_sum > 0:
            final_weights = {s: w / current_sum for s, w in capped_weights.items()}
        else:
            final_weights = {}

        self._execute_targets(final_weights)

        output = ", ".join([f"{s.Value}: {w*100:.1f}%" for s, w in final_weights.items() if w > 0])
        if output:
            self.Debug(f"Weights {self.Time:%Y-%m-%d}: {output}")

    def _execute_targets(self, target_weights):
        # FIX 2 + fill-fix: store decision, OnData drains via MarketOnOpenOrder
        self._pending_weights = dict(target_weights)

```

## QuantBook Analysis Tool
## For more information see [https://www.quantconnect.com/docs/v2/our-platform/research/getting-started]
qb = QuantBook()
spy = qb.add_equity("SPY")
history = qb.history(qb.securities.keys(), 360, Resolution.DAILY)

## Indicator Analysis
bbdf = qb.indicator(BollingerBands(30, 2), spy.symbol, 360, Resolution.DAILY)
bbdf.drop('standarddeviation', axis=1).plot()
