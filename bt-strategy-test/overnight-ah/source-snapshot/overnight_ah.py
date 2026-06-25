"""
overnight_ah.py — Strategia Overnight AH (Buy Close / Sell Open)
=================================================================
Acquista all'asta di chiusura (MOC) e vende all'asta di apertura
del giorno successivo (MOO), catturando il rendimento overnight.

Flusso comune (barre giornaliere):
  next() seleziona candidati e calcola il sizing con la stessa logica per
  backtest, paper e live.

  Backtest:
         → entry: buy(coc=True) se auction=True, altrimenti market standard
         → exit:  sell(coc=False) per simulare la chiusura all'open successivo

  Paper/live:
         → entry: buy(coc=True) se auction=True, altrimenti market standard
         → exit:  non viene inviata dalla strategia; è gestita dal cron MOO
                  dedicato leggendo le posizioni reali.

Divergenze intenzionali paper/live:
  - valuta solo l'ultima barra di oggi per non riscorrere lo storico nel batch;
  - non crea ordini di chiusura, perché la chiusura è demandata al cron MOO;
  - dopo fill entry lo stato resta LONG; il processo batch termina e lo stato
    non è persistente tra run.

Sizing:
  - max_exposure è il target di esposizione della strategia;
  - margin_leverage è un vincolo del broker e non modifica max_exposure;
  - se max_exposure <= 1.0, la base di sizing è cash;
  - se max_exposure > 1.0, la base di sizing è equity;
  - la funzione _candidate_allocations è unica per backtest, paper e live.

Filtri:
  - Volatilità intraday:  se intraday_vol_filter_side coincide col segno della
    candela, (H-L)/O fuori da [min_intraday_vol, max_intraday_vol] → skip
  - Prezzo minimo:        close < min_price → skip
  - Liquidità minima:     ADV$ rolling < min_adv → skip
  - AH lag1:              rendimento notte precedente < ah_lag1_threshold → skip
  - Earnings: parametro mantenuto, ma la selezione comune corrente usa i
    filtri backtest e non applica earnings calendar.

Universo mensile:
  - monthly_universe_mode='file' usa monthly_universe_file, se fornito;
  - monthly_universe_mode='weak_theme' calcola ogni mese una selezione ex-ante
    usando solo dati precedenti al mese corrente: ranking 6 mesi del titolo
    per rendimento close-to-close e rendimento AH, piu' un tilt debole verso
    titoli correlati al fattore semiconduttori;
  - monthly_universe_mode='weak_theme_switch' usa la selezione dinamica solo
    quando il regime semiconduttori supera la soglia configurata, altrimenti
    usa monthly_universe_static_symbols.

Warmup e confronto:
  - trade_start_date non serve al batch paper/live normale;
  - in backtest serve a caricare storico per indicatori mensili senza tradare
    prima della finestra di confronto.

Esempio run backtest:
  python btmain.py --strat overnight_ah.OvernightAH \
      --ticker stable_ah_top10.json \
      --mode backtest --timeframe daily --commission none \
      --stratargs "max_concurrent=5 min_intraday_vol=0.025 max_intraday_vol=0.045 intraday_vol_filter_side='any' ah_lag1_threshold=-0.1 min_adv=100000000 max_exposure=2" \
      --margin-rate 0.1 --margin-leverage 2

Se SPY è presente nel ticker file viene ignorato come asset tradabile.
"""

import datetime
import csv
import logging
import math
import os

import backtrader as bt
import pytz

from strategies import multiTickerStrategy as mts

logger = logging.getLogger(__name__)

_ET  = pytz.timezone('America/New_York')
_UTC = pytz.utc

_ROLE_MOC         = 'moc_entry'
_ROLE_MOO         = 'moo_exit'
_ROLE_FORCE_CLOSE = 'force_close'


class OvernightAH(mts.MultiTickerStrategy):
    """
    Strategia overnight long-only: MOC entry, MOO exit.

    Parametri
    ---------
    max_concurrent : int
        Numero massimo di posizioni contemporanee. Limita anche quanti
        candidati vengono presi dalla lista ordinata del giorno.
    max_exposure : float
        Esposizione target della strategia. 1.0 = 100% equity/cash, 2.0 = 200%.
        La leva broker resta controllata fuori strategia da margin_leverage.
    auction : bool
        True usa buy(coc=True) per entry in close/CLS. False usa market entry;
        utile per profili non auction, ma il backtest daily significativo usa
        auction=True per simulare close-to-next-open.
    sizing_policy : str
        Politica di allocazione. Valori supportati dalla logica locale:
        legacy, selected_equal, current_slots, rank_decay,
        reverse_rank_decay, selectable_fixed. Nella configurazione operativa
        corrente si usa di fatto allocazione uguale.
    size_by_max_concurrent : bool
        False divide il capitale tra i candidati selezionati quel giorno.
        True divide sempre per max_concurrent, quindi mantiene size piu'
        stabile anche se i candidati giornalieri sono pochi.
    min_intraday_vol, max_intraday_vol : float
        Range ammesso per la volatilita' intraday (high-low)/open della barra
        corrente. Soglie in frazione: 0.025 = 2.5%.
    intraday_vol_filter_side : str
        any applica il filtro volatilita' a tutte le giornate; up solo se
        close >= open; down solo se close < open.
    ah_lag1_threshold : float
        Filtro sul rendimento after-hours precedente, open[t]/close[t-1]-1.
        Si applica solo se la soglia e' negativa; 0.0 disabilita il filtro.
    min_price : float
        Prezzo minimo sul close corrente. 0.0 disabilita.
    min_adv : float
        Dollar ADV minimo: media volume rolling della barra precedente per
        close corrente. 0.0 disabilita.
    earnings_skip : bool
        In paper/live, se il calendario e' disponibile, evita simboli con
        earnings nelle prossime earnings_lookahead_h ore. In backtest non viene
        applicato.
    earnings_lookahead_h : int
        Orizzonte in ore per il filtro earnings.
    entry_minute : int
        Parametro legacy del vecchio flusso intraday. Nel batch daily corrente
        non pianifica l'orario: l'orario e' gestito da cron.
    moo_timeout_min : int
        Parametro legacy per force-close. Nel paper/live corrente la chiusura
        e' gestita da script MOO/fallback dedicati, non da questa strategia.
    min_cash_per_trade : float
        Notional minimo per aprire una posizione. Sotto soglia la entry viene
        saltata.
    monthly_universe_file : str
        CSV con colonne year;month;symbols. Con monthly_universe_mode='file'
        ordina/limita i ticker tradabili mese per mese.
    monthly_universe_mode : str
        file usa monthly_universe_file oppure tutto il ticker file se il CSV
        non e' presente. weak_theme calcola una selezione mensile dinamica
        ex-ante: per ogni ticker misura performance close-to-close 6m e AH 6m
        prima del mese corrente, costruisce un punteggio base 60/40, poi
        aggiunge un tilt leggero verso il tema semiconduttori. weak_theme_switch
        usa questa selezione dinamica solo nei regimi favorevoli, altrimenti
        usa monthly_universe_static_symbols.
    monthly_universe_top_n : int
        Numero massimo di ticker scelti dalla selezione mensile dinamica.
    monthly_universe_base_weight : float
        Peso del punteggio base: 60% rank close-to-close 6m + 40% rank AH 6m.
    monthly_universe_theme_weight : float
        Peso del tilt semiconduttori aggiunto al punteggio base. Con 0.15 il
        tema influenza la classifica, ma non domina la selezione.
    monthly_universe_theme_score : str
        corr12 usa rank della correlazione 12m verso il fattore semis;
        beta12 usa beta 12m verso il fattore semis; structural combina
        corr12, beta12 e appartenenza alla lista semis.
    monthly_universe_spy_dd3m_threshold : float
        Gate di mercato: se SPY e' caricato e il drawdown 3m di SPY e' <=
        soglia, weak_theme restituisce universo vuoto. Se SPY non e' caricato,
        il gate viene bypassato.
    monthly_universe_semis : str
        Lista CSV di simboli usati per costruire il fattore semiconduttori.
    monthly_universe_static_symbols : str
        Lista CSV usata come universo fallback in weak_theme_switch quando il
        regime dinamico non passa.
    monthly_universe_switch_feature : str
        Feature di regime per weak_theme_switch. Valori supportati:
        semis_total_1m/3m/6m/12m, semis_mean_1m/3m/6m/12m,
        semis_ma63_ratio, semis_ma126_ratio.
    monthly_universe_switch_threshold : float
        Soglia: lo switch dinamico passa solo se feature > threshold.
    trade_start_date : str
        Data YYYY-MM-DD prima della quale non apre trade. Serve nei backtest
        walk-forward per usare warmup storico e allineare periodi di confronto.
    """

    live_enabled = True

    params = (
        ('max_concurrent',         10),
        ('max_exposure',           0.95),
        ('auction',                True),
        ('sizing_policy',          'legacy'),
        ('size_by_max_concurrent', False),
        ('min_intraday_vol',       0.0),
        ('max_intraday_vol',       0.03),
        ('intraday_vol_filter_side', 'any'),
        ('ah_lag1_threshold',      0.0),   # 0.0=off; es. -0.01 → skip se notte prec. < -1%
        ('min_price',              0.0),
        ('min_adv',                0.0),
        ('earnings_skip',          True),
        ('earnings_lookahead_h',   36),
        ('entry_minute',           48),
        ('moo_timeout_min',         5),
        ('min_cash_per_trade',     100.0),
        ('monthly_universe_file',   ''),
        ('monthly_universe_mode',   'file'),
        ('monthly_universe_top_n',  50),
        ('monthly_universe_base_weight', 0.85),
        ('monthly_universe_theme_weight', 0.15),
        ('monthly_universe_theme_score', 'corr12'),
        ('monthly_universe_spy_dd3m_threshold', -0.10),
        ('monthly_universe_semis',  'NVDA,AMD,AVGO,MU,ASML,MRVL,ARM,AMAT,LRCX,KLAC,MCHP,ADI,TXN,ON,INTC,GFS'),
        ('monthly_universe_static_symbols', 'NVDA,AVGO,MU,AMD,MSTR,CEG,ASML,MRVL,ARM,MELI'),
        ('monthly_universe_switch_feature', 'semis_total_3m'),
        ('monthly_universe_switch_threshold', 0.0),
        ('trade_start_date',        ''),
    )

    def __init__(self):
        super().__init__()

        # SPY eventualmente presente nel ticker file: escluso dal trading
        self._trade_stocks = [d for d in self.stocks if d._name != 'SPY']

        # State machine: 'FLAT' | 'PENDING_ENTRY' | 'LONG' | 'PENDING_EXIT'
        self._state     = {d: 'FLAT' for d in self._trade_stocks}
        self._moc_order = {d: None   for d in self._trade_stocks}
        self._moo_order = {d: None   for d in self._trade_stocks}

        # Tracking per report/log
        self._entry_price    = {d: None for d in self._trade_stocks}
        self._entry_size     = {d: 0    for d in self._trade_stocks}
        self._entry_date     = {d: None for d in self._trade_stocks}
        self._last_entry_bar = {d: -1   for d in self._trade_stocks}

        self.api_key    = os.environ.get('BROKER_API_KEY', '')
        self.secret_key = os.environ.get('BROKER_SECRET_KEY', '')

        self._earnings_cal = None
        self._monthly_universe = self._load_monthly_universe(self.p.monthly_universe_file)
        self._dynamic_monthly_universe = {}
        self._spy_data = next((d for d in self.stocks if d._name == 'SPY'), None)
        self._semis_symbols = {
            s.strip().upper()
            for s in str(self.p.monthly_universe_semis or '').split(',')
            if s.strip()
        }
        self._static_universe_symbols = [
            s.strip().upper()
            for s in str(self.p.monthly_universe_static_symbols or '').split(',')
            if s.strip()
        ]

        logger.info(
            "OvernightAH init: %d ticker tradabili, mode=%s",
            len(self._trade_stocks),
            self._live_mode,
        )
        if self._monthly_universe:
            logger.info("Monthly universe loaded: %d mesi", len(self._monthly_universe))

    def start(self):
        if hasattr(super(), 'start'):
            super().start()

        if self.p.earnings_skip and self._live_mode in ('live', 'paper'):
            try:
                from broker.earnings_calendar import EarningsCalendar
                syms = [d._name for d in self._trade_stocks]
                self._earnings_cal = EarningsCalendar(syms)
                self._earnings_cal.refresh_if_needed()
                logger.info("EarningsCalendar inizializzato per %d simboli", len(syms))
            except Exception as exc:
                logger.warning("EarningsCalendar non disponibile: %s", exc)

    # ------------------------------------------------------------------ #
    # Entry point principale                                               #
    # ------------------------------------------------------------------ #

    def _is_daily_timeframe(self) -> bool:
        return getattr(self.data, '_timeframe', bt.TimeFrame.Minutes) >= bt.TimeFrame.Days

    def next(self):
        n_active = sum(
            1 for d in self._trade_stocks
            if self._state[d] != 'FLAT' or self.getposition(d).size != 0
        )
        slots = max(0, self.p.max_concurrent - n_active)
        if slots == 0:
            return

        current_date = self.datetime.date()
        if self._before_trade_start(current_date):
            return
        today = datetime.date.today()
        candidates = []
        skipped = []
        for d in self._ordered_monthly_trade_stocks(current_date):
            if self._live_mode in ('live', 'paper') and not (
                len(d) == d.buflen() and d.datetime.date() == today
            ):
                skipped.append(
                    f"{d._name}: data_not_current "
                    f"bar={d.datetime.date()} today={today} len={len(d)} buflen={d.buflen()}"
                )
                continue
            if self._state[d] != 'FLAT':
                skipped.append(f"{d._name}: state={self._state[d]}")
                continue
            if self.getposition(d).size != 0:
                skipped.append(f"{d._name}: position_size={self.getposition(d).size}")
                continue
            filter_reason = self._filter_reason(d)
            if filter_reason is not None:
                skipped.append(f"{d._name}: {filter_reason}")
                continue
            candidates.append(d)
            if len(candidates) >= slots:
                break

        if not candidates:
            if self._live_mode in ('live', 'paper'):
                logger.info(
                    "ENTRY_NO_CANDIDATES date=%s slots=%d skipped=[%s]",
                    current_date,
                    slots,
                    "; ".join(skipped) if skipped else "none",
                )
            return

        cash_avail = self._sizing_equity() * float(self.p.max_exposure)
        allocations = self._candidate_allocations(candidates, cash_avail)

        for d, cash_per_trade in zip(candidates, allocations):
            price = d.close[0]
            if price <= 0 or cash_per_trade < self.p.min_cash_per_trade:
                continue
            entry_notional = self._cap_entry_notional(d, cash_per_trade)
            if entry_notional < self.p.min_cash_per_trade:
                continue
            size = int(entry_notional / price)
            if size < 1:
                logger.info(
                    "ENTRY_SKIP %s: size<1 price=%.2f cash_per_trade=%.2f entry_notional=%.2f",
                    d._name, price, cash_per_trade, entry_notional,
                )
                continue

            logger.info(
                "ENTRY_SIGNAL %s: auction=%s size=%d price=%.2f cash_per_trade=%.2f entry_notional=%.2f",
                d._name, bool(self.p.auction), size, price, cash_per_trade, entry_notional,
            )
            if self.p.auction:
                buy = self.buy(d, size=size, exectype=bt.Order.Market, coc=True)
                if buy is None:
                    logger.warning("ENTRY_ORDER_NONE %s: auction=%s size=%d", d._name, bool(self.p.auction), size)
                    continue
            else:
                buy = self.buy(d, size=size, exectype=bt.Order.Market)
                if buy is None:
                    logger.warning("ENTRY_ORDER_NONE %s: auction=%s size=%d", d._name, bool(self.p.auction), size)
                    continue
            buy.addinfo(overnight_role=_ROLE_MOC, is_close=False)
            logger.info("MOC submitted %s: size=%d @ ~%.2f", d._name, size, price)

            if self._live_mode not in ('live', 'paper'):
                close_info = dict(
                    overnight_role=_ROLE_MOO,
                    is_close=True,
                    signal_intent='CLOSE',
                    signal_side='SELL',
                )
                if self.p.auction:
                    sell = self.sell(d, size=size, coc=False, info=close_info)
                else:
                    sell = self.sell(d, size=size, info=close_info)
                if sell is None:
                    continue
                sell.addinfo(overnight_role=_ROLE_MOO, is_close=True)
                self._moo_order[d] = sell
            else:
                sell = None

            self._state[d]     = 'PENDING_ENTRY'
            self._moc_order[d] = buy
            self._moo_order[d] = sell

            logger.debug(
                "BT entry %s: size=%d @ close=%.2f  cash_available=%.0f entry_notional=%.0f",
                d._name, size, price, cash_avail, entry_notional,
            )

    # ------------------------------------------------------------------ #
    # Filtri                                                               #
    # ------------------------------------------------------------------ #

    def _load_monthly_universe(self, path: str) -> dict[tuple[int, int], list[str]]:
        if not path:
            return {}

        resolved = path
        if not os.path.isabs(resolved):
            root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            candidates = [
                os.path.join(os.getcwd(), resolved),
                os.path.join(root, resolved),
                os.path.join(root, 'config-common', 'tickers', resolved),
                os.path.join(root, 'bt-strategy-test', 'overnight-ah', 'research', 'out', resolved),
            ]
            resolved = next((p for p in candidates if os.path.exists(p)), resolved)

        out: dict[tuple[int, int], list[str]] = {}
        try:
            with open(resolved, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    year = int(str(row.get('year', '')).strip())
                    month = int(str(row.get('month', '')).strip())
                    symbols_text = row.get('symbols', '') or ''
                    symbols = [s.strip() for s in symbols_text.split(',') if s.strip()]
                    out[(year, month)] = symbols
        except Exception as exc:
            logger.warning("Monthly universe non caricato da %s: %s", path, exc)
            return {}

        return out

    def _before_trade_start(self, current_date: datetime.date) -> bool:
        value = str(self.p.trade_start_date or '').strip()
        if not value:
            return False
        try:
            start = datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("trade_start_date non valida: %s", self.p.trade_start_date)
            return False
        return current_date < start

    def _ordered_monthly_trade_stocks(self, current_date: datetime.date):
        mode = str(self.p.monthly_universe_mode or 'file').strip().lower()
        if mode in ('weak_theme', 'theme_weak'):
            return self._ordered_dynamic_monthly_trade_stocks(current_date)
        if mode in ('weak_theme_switch', 'theme_switch', 'regime_switch'):
            return self._ordered_regime_switch_trade_stocks(current_date)
        if mode not in ('file', 'monthly_file', ''):
            logger.warning("monthly_universe_mode non valido: %s", self.p.monthly_universe_mode)
            return []
        if not self._monthly_universe:
            return self._trade_stocks
        symbols = self._monthly_universe.get((current_date.year, current_date.month))
        if not symbols:
            return []
        by_name = {d._name: d for d in self._trade_stocks}
        return [by_name[s] for s in symbols if s in by_name]

    def _ordered_dynamic_monthly_trade_stocks(self, current_date: datetime.date):
        key = (current_date.year, current_date.month)
        if key not in self._dynamic_monthly_universe:
            self._dynamic_monthly_universe[key] = self._compute_weak_theme_monthly_universe(current_date)
        by_name = {d._name: d for d in self._trade_stocks}
        return [by_name[s] for s in self._dynamic_monthly_universe[key] if s in by_name]

    def _ordered_regime_switch_trade_stocks(self, current_date: datetime.date):
        key = (current_date.year, current_date.month, 'switch')
        if key not in self._dynamic_monthly_universe:
            if self._passes_regime_switch(current_date):
                symbols = self._compute_weak_theme_monthly_universe(current_date)
                regime = 'dynamic'
            else:
                symbols = list(self._static_universe_symbols)
                regime = 'static'
            self._dynamic_monthly_universe[key] = symbols
            logger.info(
                "Monthly regime_switch universe %s: regime=%s selected=%d top=[%s]",
                current_date.strftime("%Y-%m"),
                regime,
                len(symbols),
                ",".join(symbols[:10]),
            )
        by_name = {d._name: d for d in self._trade_stocks}
        return [by_name[s] for s in self._dynamic_monthly_universe[key] if s in by_name]

    def _month_start(self, current_date: datetime.date) -> datetime.date:
        return datetime.date(current_date.year, current_date.month, 1)

    def _compute_weak_theme_monthly_universe(self, current_date: datetime.date) -> list[str]:
        """Ex-ante monthly downselect: 85% c2c/AH momentum + 15% semis corr12.

        Only bars strictly before the current month are used. This mirrors the
        study policy selected on validation: c2c60/ah40 baseline, SPY 3m drawdown
        gate, and a weak semiconductor factor tilt.
        """
        if not self._passes_spy_monthly_gate(current_date):
            logger.info("Monthly weak_theme gate closed for %s", current_date.strftime("%Y-%m"))
            return []

        month_start = self._month_start(current_date)
        semis_factor = self._semis_factor_by_date(month_start, lookback=252)
        c2c_values = {}
        ah_values = {}
        corr_values = {}
        beta_values = {}
        for data in self._trade_stocks:
            history = self._historical_returns_before_month(data, month_start, lookback=252)
            if len(history) < 60:
                continue
            six_month_start = self._shift_month(month_start, -6)
            six_month_history = [row for row in history if row['date'] >= six_month_start]
            c2c = [row['c2c'] for row in six_month_history if row['c2c'] is not None and math.isfinite(row['c2c'])]
            ah = [row['ah'] for row in six_month_history if row['ah'] is not None and math.isfinite(row['ah'])]
            if len(c2c) >= 40:
                c2c_values[data._name] = sum(c2c) / len(c2c)
            if len(ah) >= 40:
                ah_values[data._name] = sum(ah) / len(ah)
            aligned = [
                (row['c2c'], semis_factor[row['date']])
                for row in history[-252:]
                if row['date'] in semis_factor
                and row['c2c'] is not None
                and math.isfinite(row['c2c'])
                and math.isfinite(semis_factor[row['date']])
            ]
            if len(aligned) >= 80:
                xs = [x for x, _ in aligned]
                ys = [y for _, y in aligned]
                corr = self._corr(xs, ys)
                if corr is not None and math.isfinite(corr):
                    corr_values[data._name] = corr
                beta = self._beta(xs, ys)
                if beta is not None and math.isfinite(beta):
                    beta_values[data._name] = beta

        rank_c2c = self._rank_pct(c2c_values)
        rank_ah = self._rank_pct(ah_values)
        rank_corr = self._rank_pct(corr_values)
        rank_beta = self._rank_pct(beta_values)
        base_weight = float(self.p.monthly_universe_base_weight)
        theme_weight = float(self.p.monthly_universe_theme_weight)
        theme_score_name = str(self.p.monthly_universe_theme_score or 'corr12').strip().lower()
        top_n = max(0, int(self.p.monthly_universe_top_n))

        rows = []
        for data in self._trade_stocks:
            symbol = data._name
            if symbol not in rank_c2c or symbol not in rank_ah:
                continue
            base_score = 0.60 * rank_c2c[symbol] + 0.40 * rank_ah[symbol]
            theme_score = self._theme_score(symbol, rank_corr, rank_beta, theme_score_name)
            score = base_weight * base_score + theme_weight * theme_score
            rows.append((score, symbol))

        rows.sort(key=lambda item: (-item[0], item[1]))
        selected = [symbol for _, symbol in rows[:top_n]]
        logger.info(
            "Monthly weak_theme universe %s: selected=%d top=[%s]",
            current_date.strftime("%Y-%m"),
            len(selected),
            ",".join(selected[:10]),
        )
        return selected

    def _theme_score(
        self,
        symbol: str,
        rank_corr: dict[str, float],
        rank_beta: dict[str, float],
        theme_score_name: str,
    ) -> float:
        if theme_score_name in ('corr12', 'corr', 'semis_corr12'):
            return rank_corr.get(symbol, 0.50)
        if theme_score_name in ('beta12', 'beta', 'semis_beta12'):
            return rank_beta.get(symbol, 0.50)
        if theme_score_name in ('structural', 'semis_structural'):
            member = 1.0 if symbol in self._semis_symbols else 0.0
            return 0.50 * rank_corr.get(symbol, 0.50) + 0.30 * rank_beta.get(symbol, 0.50) + 0.20 * member
        logger.warning("monthly_universe_theme_score non valido: %s; fallback corr12", theme_score_name)
        return rank_corr.get(symbol, 0.50)

    def _passes_spy_monthly_gate(self, current_date: datetime.date) -> bool:
        threshold = float(self.p.monthly_universe_spy_dd3m_threshold)
        if self._spy_data is None:
            return True
        month_start = self._month_start(current_date)
        closes = self._historical_closes_before_month(self._spy_data, month_start, lookback=63)
        if not closes:
            return True
        peak = max(closes)
        if peak <= 0:
            return True
        drawdown = closes[-1] / peak - 1.0
        return drawdown > threshold

    def _passes_regime_switch(self, current_date: datetime.date) -> bool:
        month_start = self._month_start(current_date)
        feature = str(self.p.monthly_universe_switch_feature or 'semis_total_3m').strip().lower()
        threshold = float(self.p.monthly_universe_switch_threshold)
        value = self._semis_regime_value(month_start, feature)
        if value is None or not math.isfinite(value):
            return False
        return value > threshold

    def _semis_regime_value(self, month_start: datetime.date, feature: str) -> float | None:
        lookback_by_feature = {
            'semis_total_1m': 21,
            'semis_total_3m': 63,
            'semis_total_6m': 126,
            'semis_total_12m': 252,
            'semis_mean_1m': 21,
            'semis_mean_3m': 63,
            'semis_mean_6m': 126,
            'semis_mean_12m': 252,
            'semis_ma63_ratio': 126,
            'semis_ma126_ratio': 252,
        }
        lookback = lookback_by_feature.get(feature)
        if lookback is None:
            logger.warning("monthly_universe_switch_feature non valido: %s", feature)
            return None
        if feature.startswith('semis_ma'):
            return self._semis_ma_ratio(month_start, lookback=lookback, ma_days=63 if '63' in feature else 126)
        factor = self._semis_factor_by_date(month_start, lookback=lookback)
        values = list(factor.values())
        if len(values) < max(10, lookback // 3):
            return None
        if feature.startswith('semis_total'):
            return sum(values)
        return sum(values) / len(values)

    def _semis_ma_ratio(self, month_start: datetime.date, lookback: int, ma_days: int) -> float | None:
        by_date = {}
        semis_data = [d for d in self.stocks if d._name in self._semis_symbols]
        for data in semis_data:
            closes = []
            max_offset = max(0, len(data) - 1)
            for offset in range(0, max_offset + 1):
                try:
                    dt = data.datetime.date(-offset)
                    if dt >= month_start:
                        continue
                    close = float(data.close[-offset])
                except Exception:
                    continue
                if math.isfinite(close) and close > 0:
                    closes.append((dt, close))
                    if len(closes) >= lookback:
                        break
            for dt, close in closes:
                by_date.setdefault(dt, []).append(close)
        series = [sum(values) / len(values) for _, values in sorted(by_date.items()) if values]
        if len(series) < ma_days:
            return None
        ma = sum(series[-ma_days:]) / ma_days
        if ma <= 0:
            return None
        return series[-1] / ma - 1.0

    def _shift_month(self, date_value: datetime.date, months: int) -> datetime.date:
        month_index = date_value.year * 12 + (date_value.month - 1) + months
        year = month_index // 12
        month = month_index % 12 + 1
        return datetime.date(year, month, 1)

    def _historical_closes_before_month(self, data, month_start: datetime.date, lookback: int) -> list[float]:
        closes = []
        max_offset = max(0, len(data) - 1)
        for offset in range(0, max_offset + 1):
            try:
                dt = data.datetime.date(-offset)
                if dt >= month_start:
                    continue
                close = float(data.close[-offset])
            except Exception:
                continue
            if math.isfinite(close) and close > 0:
                closes.append(close)
                if len(closes) >= lookback:
                    break
        closes.reverse()
        return closes

    def _historical_returns_before_month(self, data, month_start: datetime.date, lookback: int) -> list[dict]:
        rows = []
        max_offset = max(0, len(data) - 2)
        for offset in range(0, max_offset + 1):
            try:
                dt = data.datetime.date(-offset)
                if dt >= month_start:
                    continue
                close = float(data.close[-offset])
                prev_close = float(data.close[-offset - 1])
                open_ = float(data.open[-offset])
            except Exception:
                continue
            c2c = close / prev_close - 1.0 if prev_close > 0 else None
            ah = open_ / prev_close - 1.0 if prev_close > 0 else None
            rows.append({'date': dt, 'c2c': c2c, 'ah': ah})
            if len(rows) >= lookback:
                break
        rows.reverse()
        return rows

    def _semis_factor_by_date(self, month_start: datetime.date, lookback: int) -> dict[datetime.date, float]:
        by_date = {}
        semis_data = [d for d in self.stocks if d._name in self._semis_symbols]
        for data in semis_data:
            for row in self._historical_returns_before_month(data, month_start, lookback):
                c2c = row['c2c']
                if c2c is None or not math.isfinite(c2c):
                    continue
                by_date.setdefault(row['date'], []).append(c2c)
        return {
            dt: sum(values) / len(values)
            for dt, values in by_date.items()
            if values
        }

    def _rank_pct(self, values: dict[str, float]) -> dict[str, float]:
        valid = [
            (symbol, float(value))
            for symbol, value in values.items()
            if value is not None and math.isfinite(float(value))
        ]
        if not valid:
            return {}
        valid.sort(key=lambda item: (item[1], item[0]))
        n = len(valid)
        ranks = {}
        idx = 0
        while idx < n:
            end = idx + 1
            while end < n and valid[end][1] == valid[idx][1]:
                end += 1
            avg_rank = (idx + 1 + end) / 2.0
            pct = avg_rank / n
            for j in range(idx, end):
                ranks[valid[j][0]] = pct
            idx = end
        return ranks

    def _corr(self, xs: list[float], ys: list[float]) -> float | None:
        if len(xs) != len(ys) or len(xs) < 2:
            return None
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        var_x = sum((x - mean_x) ** 2 for x in xs)
        var_y = sum((y - mean_y) ** 2 for y in ys)
        denom = math.sqrt(var_x * var_y)
        if denom <= 0:
            return None
        return cov / denom

    def _beta(self, xs: list[float], ys: list[float]) -> float | None:
        if len(xs) != len(ys) or len(xs) < 2:
            return None
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        var_y = sum((y - mean_y) ** 2 for y in ys)
        if var_y <= 0:
            return None
        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        return cov / var_y

    def _filter_reason(self, data) -> str | None:
        """Ritorna la stringa del motivo di scarto, o None se il ticker passa tutti i filtri."""
        if len(data) < 2:
            return "dati insufficienti (len<2)"

        o = data.open[0]
        if o > 0:
            vol = (data.high[0] - data.low[0]) / o
            intraday_body = (data.close[0] - data.open[0]) / o
            vol_filter_side = str(self.p.intraday_vol_filter_side or 'any').strip().lower()
            if vol_filter_side not in ('any', 'up', 'down'):
                return f"intraday_vol_filter_side non valido: {self.p.intraday_vol_filter_side}"
            apply_vol_filter = (
                vol_filter_side == 'any'
                or (vol_filter_side == 'up' and intraday_body >= 0)
                or (vol_filter_side == 'down' and intraday_body < 0)
            )
            if apply_vol_filter:
                if vol < self.p.min_intraday_vol:
                    return f"vol {vol:.3%} < min {self.p.min_intraday_vol:.3%}"
                if vol > self.p.max_intraday_vol:
                    return f"vol {vol:.3%} > max {self.p.max_intraday_vol:.3%}"

        if self.p.min_price and self.p.min_price > 0:
            price = data.close[0]
            if price < self.p.min_price:
                return f"price {price:.2f} < min {self.p.min_price:.2f}"

        min_adv = self._dollar_adv(data)
        if self.p.min_adv and self.p.min_adv > 0:
            if min_adv is None:
                return "ADV$ non disponibile"
            if min_adv < self.p.min_adv:
                return f"ADV$ {min_adv:.0f} < min {self.p.min_adv:.0f}"

        if self.p.ah_lag1_threshold < 0.0:
            prev_close = data.close[-1]
            if prev_close > 0:
                ah_lag1 = (data.open[0] - prev_close) / prev_close
                if ah_lag1 < self.p.ah_lag1_threshold:
                    return f"ah_lag1 {ah_lag1:.3%} < soglia {self.p.ah_lag1_threshold:.3%}"

        if self.p.earnings_skip and self._earnings_cal is not None:
            try:
                et_now = self._current_et_dt(data)
                if self._earnings_cal.has_event(
                    data._name, et_now,
                    lookahead_h=self.p.earnings_lookahead_h,
                ):
                    return f"earnings nelle prossime {self.p.earnings_lookahead_h}h"
            except Exception as exc:
                return f"earnings check fallito: {exc}"

        return None

    def _passes_filters_bt(self, data) -> bool:
        if len(data) < 2:
            return False

        # Volatilità intraday [min, max], applicata solo al segno configurato.
        o = data.open[0]
        if o > 0:
            vol = (data.high[0] - data.low[0]) / o
            intraday_body = (data.close[0] - data.open[0]) / o
            vol_filter_side = str(self.p.intraday_vol_filter_side or 'any').strip().lower()
            if vol_filter_side not in ('any', 'up', 'down'):
                return False
            apply_vol_filter = (
                vol_filter_side == 'any'
                or (vol_filter_side == 'up' and intraday_body >= 0)
                or (vol_filter_side == 'down' and intraday_body < 0)
            )
            if apply_vol_filter:
                if vol < self.p.min_intraday_vol:
                    return False
                if vol > self.p.max_intraday_vol:
                    return False

        if self.p.min_price and self.p.min_price > 0:
            if data.close[0] < self.p.min_price:
                return False

        if self.p.min_adv and self.p.min_adv > 0:
            dollar_adv = self._dollar_adv(data)
            if dollar_adv is None or dollar_adv < self.p.min_adv:
                return False

        # AH lag1: rendimento notte precedente (close[t-1] → open[t])
        if self.p.ah_lag1_threshold < 0.0:
            prev_close = data.close[-1]
            if prev_close > 0:
                ah_lag1 = (data.open[0] - prev_close) / prev_close
                if ah_lag1 < self.p.ah_lag1_threshold:
                    return False

        return True

    def _dollar_adv(self, data) -> float | None:
        try:
            # Use the previous completed bar's ADV. In daily backtests the
            # current bar volume is only known after the close, while the MOC
            # decision is made before it.
            avg_volume = float(self._liquidity_avg_volume[data][-1])
            price = float(data.close[0])
            dollar_adv = avg_volume * price
        except Exception:
            return None
        if not math.isfinite(dollar_adv) or dollar_adv <= 0:
            return None
        return dollar_adv

    def _passes_filters_live(self, data) -> bool:
        if not self._passes_filters_bt(data):
            return False

        if self.p.earnings_skip and self._earnings_cal is not None:
            try:
                et_now = self._current_et_dt(data)
                if self._earnings_cal.has_event(
                    data._name, et_now,
                    lookahead_h=self.p.earnings_lookahead_h,
                ):
                    logger.info(
                        "Skip %s: earnings nelle prossime %dh",
                        data._name, self.p.earnings_lookahead_h,
                    )
                    return False
            except Exception as exc:
                logger.warning("Earnings check fallito per %s: %s", data._name, exc)
                return False

        return True

    # ------------------------------------------------------------------ #
    # Submission ordini live                                               #
    # ------------------------------------------------------------------ #

    def _sizing_equity(self) -> float:
        try:
            snapshot = self.broker.get_margin_snapshot(refresh=True)
            if float(self.p.max_exposure) <= 1.0:
                cash = snapshot.get('cash', None)
                return max(0.0, float(cash or 0.0))
            return max(0.0, float(snapshot.get('equity', 0.0) or 0.0))
        except Exception:
            pass
        try:
            return max(0.0, float(self.broker.get_value()))
        except Exception:
            return max(0.0, float(getattr(self.broker, 'cash', 0.0) or 0.0))

    def _candidate_allocations(self, candidates, cash_avail: float) -> list[float]:
        n = len(candidates)
        if n <= 0:
            return []

        policy = str(self.p.sizing_policy or 'legacy').strip().lower()
        max_concurrent = max(1, int(self.p.max_concurrent))

        if policy == 'legacy':
            divisor = max_concurrent if self.p.size_by_max_concurrent else n
            return [cash_avail / max(1, int(divisor)) for _ in candidates]

        if policy in ('selectable_fixed', 'max_concurrent_fixed'):
            return [cash_avail / max_concurrent for _ in candidates]

        if policy in ('selected_equal', 'equal_selected'):
            return [cash_avail / n for _ in candidates]

        if policy == 'current_slots':
            current = []
            for idx in range(n):
                slots_left = max_concurrent - idx
                current.append(cash_avail / max(1, slots_left))
            return current

        if policy == 'rank_decay':
            weights = list(range(n, 0, -1))
            total = sum(weights)
            return [cash_avail * weight / total for weight in weights]

        if policy in ('reverse_rank_decay', 'rank_growth'):
            weights = list(range(1, n + 1))
            total = sum(weights)
            return [cash_avail * weight / total for weight in weights]

        raise ValueError(
            "sizing_policy non valida: %s. Valori supportati: legacy, "
            "selectable_fixed, selected_equal, current_slots, rank_decay, reverse_rank_decay"
            % self.p.sizing_policy
        )

    def _submit_moc(self, data, cash_per_slot: float) -> bool:
        slots_left = self.p.max_concurrent - sum(
            1 for x in self._trade_stocks
            if self._state[x] in ('PENDING_ENTRY', 'LONG', 'PENDING_EXIT')
        )
        if slots_left <= 0:
            return False

        price = data.close[0]
        if price <= 0 or cash_per_slot < self.p.min_cash_per_trade:
            return False

        entry_notional = self._cap_entry_notional(data, cash_per_slot)
        if entry_notional < self.p.min_cash_per_trade:
            return False

        size = int(entry_notional / price)
        if size < 1:
            return False

        if self.p.auction:
            order = self.buy(data=data, size=size, exectype=bt.Order.Market, coc=True)
        else:
            order = self.buy(data=data, size=size, exectype=bt.Order.Market)
        if order is None:
            return False
        order.addinfo(
            overnight_role=_ROLE_MOC,
            is_close=False,
            signal_dt=str(self._current_et_dt(data)),
        )
        self._state[data]     = 'PENDING_ENTRY'
        self._moc_order[data] = order
        logger.info("MOC submitted %s: size=%d @ ~%.2f", data._name, size, price)
        return True

    def _submit_moo(self, data, size: int) -> None:
        order_info = dict(
            overnight_role=_ROLE_MOO,
            is_close=True,
            signal_dt=str(self._current_et_dt(data)),
        )
        if self.p.auction:
            # Future cleanup: for daily strategies, coc=False already models
            # next-open in backtest. A broker-neutral auction_intent='open'
            # would express Alpaca OPG intent without relying on coo.
            order_info['coo'] = True
        order = self.sell(data=data, size=size, exectype=bt.Order.Market, **order_info)
        if order is None:
            return
        order.addinfo(**order_info)
        self._state[data]     = 'PENDING_EXIT'
        self._moo_order[data] = order
        logger.info("MOO submitted %s: size=%d", data._name, size)

    def _force_close(self, data) -> None:
        pos = self.getposition(data)
        if pos.size <= 0:
            self._state[data] = 'FLAT'
            return
        order = self.close(
            data=data,
            overnight_role=_ROLE_FORCE_CLOSE,
        )
        if order is not None:
            order.addinfo(overnight_role=_ROLE_FORCE_CLOSE, is_close=True)
        self._state[data] = 'PENDING_EXIT'
        logger.warning(
            "FORCE CLOSE %s: size=%d (MOO timeout superato)", data._name, pos.size
        )

    # ------------------------------------------------------------------ #
    # notify_order                                                         #
    # ------------------------------------------------------------------ #

    def notify_order(self, order):
        super().notify_order(order)

        try:
            role = order.info.get('overnight_role')
        except Exception:
            return
        if role is None:
            return

        data = order.data

        if order.status in (order.Rejected, order.Margin,
                            order.Canceled, order.Expired):
            self._on_order_failure(order, role, data)
            return

        if order.status != order.Completed:
            return

        if role == _ROLE_MOC:
            filled_size = int(abs(order.executed.size))
            self._entry_price[data] = order.executed.price
            self._entry_size[data]  = filled_size
            self._entry_date[data]  = self.datetime.date()
            self._state[data]       = 'LONG'
            logger.info(
                "MOC FILLED %s: size=%d @ %.2f (%s)",
                data._name, filled_size,
                order.executed.price, self._entry_date[data],
            )
            if self._live_mode not in ('live', 'paper'):
                self._state[data] = 'PENDING_EXIT'
        elif role in (_ROLE_MOO, _ROLE_FORCE_CLOSE):
            entry = self._entry_price[data] or 0.0
            pnl   = (order.executed.price - entry) * abs(order.executed.size)
            tag   = 'MOO' if role == _ROLE_MOO else 'FORCE'
            logger.info(
                "%s FILLED %s: @ %.2f  PnL≈%.2f  held from %s",
                tag, data._name, order.executed.price,
                pnl, self._entry_date[data],
            )
            self._state[data]       = 'FLAT'
            self._moc_order[data]   = None
            self._moo_order[data]   = None
            self._entry_price[data] = None
            self._entry_size[data]  = 0
            self._entry_date[data]  = None

    def _on_order_failure(self, order, role: str, data) -> None:
        status_str = bt.Order.Status[order.status]
        if role == _ROLE_MOC:
            exit_order = self._moo_order.get(data)
            if exit_order is not None:
                try:
                    self.cancel(exit_order)
                except Exception as exc:
                    logger.warning("MOO cancel failed after MOC failure %s: %s", data._name, exc)
            self._state[data]     = 'FLAT'
            self._moc_order[data] = None
            self._moo_order[data] = None
            logger.warning("MOC FAILED %s: %s", data._name, status_str)
        elif role in (_ROLE_MOO, _ROLE_FORCE_CLOSE):
            if role == _ROLE_MOO:
                self._state[data] = 'LONG'
                self._moo_order[data] = None
                logger.error(
                    "MOO FAILED %s: %s — ritento nella prossima finestra OPG valida",
                    data._name, status_str,
                )
            else:
                logger.error(
                    "FORCE CLOSE FAILED %s: %s — verrà ritentato",
                    data._name, status_str,
                )

    # ------------------------------------------------------------------ #
    # Helpers timezone e timing                                            #
    # ------------------------------------------------------------------ #

    def _current_et_dt(self, data=None):
        src = data if data is not None else self.data
        dt  = bt.num2date(src.datetime[0])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_UTC)
        return dt.astimezone(_ET)

    def _is_moc_entry_bar(self, data) -> bool:
        et = self._current_et_dt(data)
        return et.hour == 15 and et.minute == self.p.entry_minute

    def _is_moo_submit_window(self, data) -> bool:
        et = self._current_et_dt(data)
        # Alpaca rifiuta OPG dopo le 09:28 e prima delle 19:00 ET.
        if et.hour >= 19:
            return True
        if et.hour < 9:
            return True
        return et.hour == 9 and et.minute <= 27

    def _is_moo_timeout(self, data) -> bool:
        et = self._current_et_dt(data)
        return et.hour == 9 and et.minute >= 30 + self.p.moo_timeout_min
