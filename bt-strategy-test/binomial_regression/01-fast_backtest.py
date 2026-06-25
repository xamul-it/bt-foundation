# Fast Backtest Framework for Regressione Multi-Scala
# Input: Daily and Minute CSVs with OHLCV and VWAP
# Purpose: Fast signal validation, parameter tuning, e export per final simulation

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Tuple

# -------------------- CONFIGURATION -------------------- #
ASSET_LIST = ["AAPL", "MSFT", "TSLA"]  # Replace with your asset names
REPO_ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DATA_FOLDER_MIN = REPO_ROOT / "config-common/data/m/alpaca/sip"
WINDOWS_MIN = {"long": 60, "mid": 20, "short": 5}
REGRESSION_MODEL = "linear"  # "linear" (default) | "quadratic" (legacy)
PARAM_GRID = {
    "accZ_thresh": [1.2, 1.5, 2.0],
    "volZ_thresh": [1.0, 1.5],
    "persistence_len": [3, 5]
}
TRAIN_PCT, VALID_PCT = 0.6, 0.2
EXIT_HORIZON_BARS = 1
ANALYSIS_HORIZONS = [5, 10, 15]
OUTPUT_FOLDER = HERE / "results"
OUTPUT_FOLDER.mkdir(exist_ok=True)
LOG_LEVEL = logging.INFO

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("fast_backtest")

# -------------------- DATA LOADING -------------------- #
def load_and_format(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["timestamp"])
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    return df

def split_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    n = len(df)
    train_end = int(n * TRAIN_PCT)
    valid_end = train_end + int(n * VALID_PCT)
    return df.iloc[:train_end], df.iloc[train_end:valid_end], df.iloc[valid_end:]

def infer_bar_minutes(index: pd.Index) -> float:
    if len(index) < 2:
        return float("nan")
    diffs = index.to_series().diff().dropna()
    if diffs.empty:
        return float("nan")
    median_delta = diffs.median()
    return median_delta.total_seconds() / 60.0

def log_run_configuration() -> None:
    logger.info("Starting fast backtest")
    logger.info("Minute data folder: %s", DATA_FOLDER_MIN.resolve())
    logger.info("Assets: %s", ", ".join(ASSET_LIST))
    logger.info("Windows (bars): %s", WINDOWS_MIN)
    logger.info("Regression model: %s", REGRESSION_MODEL)
    logger.info("Param grid: %s", PARAM_GRID)
    logger.info(
        "Split fractions -> train=%.2f valid=%.2f test=%.2f",
        TRAIN_PCT,
        VALID_PCT,
        1 - TRAIN_PCT - VALID_PCT,
    )
    logger.info(
        "Execution model: entry when signal==1 on bar t, return measured close[t+%d] / close[t] - 1",
        EXIT_HORIZON_BARS,
    )
    if REGRESSION_MODEL == "quadratic":
        logger.info(
            "Pullback entry: accZ_5 < -accZ_thresh AND volZ > volZ_thresh AND rolling_mean(accZ_20, persistence_len) > 0"
        )
        logger.info(
            "Breakout entry: accZ_5 > accZ_thresh AND volZ > volZ_thresh AND accZ_20 > 0"
        )
    else:
        logger.info("Linear mode: legacy strategy rules based on accZ are disabled.")
    logger.info(
        "Event-study horizons (bars): %s + EOD",
        ANALYSIS_HORIZONS
    )

def log_asset_details(asset: str, path: Path, df_min: pd.DataFrame) -> None:
    bar_minutes = infer_bar_minutes(df_min.index)
    logger.info(
        "[%s] Loaded minute file: %s | rows=%d | range=%s -> %s | inferred_bar_minutes=%.2f",
        asset,
        path.resolve(),
        len(df_min),
        df_min.index.min(),
        df_min.index.max(),
        bar_minutes,
    )

# -------------------- SIGNAL ENGINE -------------------- #
def regression_features_quadratic(series: pd.Series, window: int) -> pd.DataFrame:
    idx = np.arange(window)
    X = np.vstack([np.ones(window), idx, idx**2]).T
    XTX_inv = np.linalg.inv(X.T @ X)

    slopes, accs, stds = [], [], []
    r2s, mses, rmses, maes = [], [], [], []
    for i in range(window, len(series)):
        y = series.iloc[i - window:i].values
        beta = XTX_inv @ X.T @ y
        residuals = y - X @ beta
        slope = beta[1]
        acc = 2 * beta[2]
        std = np.std(residuals)
        mse = float(np.mean(np.square(residuals)))
        rmse = float(np.sqrt(mse))
        mae = float(np.mean(np.abs(residuals)))
        y_mean = float(np.mean(y))
        ss_res = float(np.sum(np.square(residuals)))
        ss_tot = float(np.sum(np.square(y - y_mean)))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan
        slopes.append(slope)
        accs.append(acc / std if std > 0 else 0)
        stds.append(std)
        r2s.append(r2)
        mses.append(mse)
        rmses.append(rmse)
        maes.append(mae)

    result = pd.DataFrame({
        f"slope_{window}": slopes,
        f"accZ_{window}": accs,
        f"disp_{window}": stds,
        f"r2_{window}": r2s,
        f"mse_{window}": mses,
        f"rmse_{window}": rmses,
        f"mae_{window}": maes,
    }, index=series.index[window:])
    return result


def regression_features_linear(series: pd.Series, window: int) -> pd.DataFrame:
    idx = np.arange(window, dtype=float)
    x_centered = idx - idx.mean()
    den = float(np.sum(x_centered**2))
    ones = np.ones(window, dtype=float)

    slopes, stds = [], []
    r2s, mses, rmses, maes = [], [], [], []
    for i in range(window, len(series)):
        y = series.iloc[i - window:i].values.astype(float)
        if np.isnan(y).any():
            slopes.append(np.nan)
            stds.append(np.nan)
            r2s.append(np.nan)
            mses.append(np.nan)
            rmses.append(np.nan)
            maes.append(np.nan)
            continue

        y_mean = float(np.mean(y))
        slope = float(np.sum((y - y_mean) * x_centered) / den) if den > 0 else np.nan
        intercept = y_mean - slope * float(idx.mean())
        y_hat = intercept * ones + slope * idx
        residuals = y - y_hat

        std = float(np.std(residuals))
        mse = float(np.mean(np.square(residuals)))
        rmse = float(np.sqrt(mse))
        mae = float(np.mean(np.abs(residuals)))
        ss_res = float(np.sum(np.square(residuals)))
        ss_tot = float(np.sum(np.square(y - y_mean)))
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan

        slopes.append(slope)
        stds.append(std)
        r2s.append(r2)
        mses.append(mse)
        rmses.append(rmse)
        maes.append(mae)

    result = pd.DataFrame(
        {
            f"slope_{window}": slopes,
            f"disp_{window}": stds,
            f"r2_{window}": r2s,
            f"mse_{window}": mses,
            f"rmse_{window}": rmses,
            f"mae_{window}": maes,
        },
        index=series.index[window:],
    )
    return result


def regression_features(series: pd.Series, window: int) -> pd.DataFrame:
    if REGRESSION_MODEL == "quadratic":
        return regression_features_quadratic(series, window)
    if REGRESSION_MODEL == "linear":
        return regression_features_linear(series, window)
    raise ValueError(f"Unsupported REGRESSION_MODEL={REGRESSION_MODEL!r}")

def compute_volZ(df: pd.DataFrame, window: int) -> pd.Series:
    log_vol = np.log(df["volume"] + 1)
    return (log_vol - log_vol.rolling(window).mean()) / log_vol.rolling(window).std()

# -------------------- STRATEGY SIMULATION -------------------- #
def simulate_pullback_strategy(df: pd.DataFrame, accZ_thresh: float, volZ_thresh: float, persistence_len: int) -> pd.DataFrame:
    signal = (
        (df["accZ_5"] < -accZ_thresh) &
        (df["volZ"] > volZ_thresh) &
        (df["accZ_20"].rolling(persistence_len).mean() > 0)
    )
    entries = df.index[signal]
    df["signal_pullback"] = 0
    df.loc[entries, "signal_pullback"] = 1
    return df

def simulate_breakout_strategy(df: pd.DataFrame, accZ_thresh: float, volZ_thresh: float) -> pd.DataFrame:
    signal = (
        (df["accZ_5"] > accZ_thresh) &
        (df["volZ"] > volZ_thresh) &
        (df["accZ_20"] > 0)
    )
    entries = df.index[signal]
    df["signal_breakout"] = 0
    df.loc[entries, "signal_breakout"] = 1
    return df

# -------------------- EVALUATION -------------------- #
def sharpe_ratio(returns: pd.Series) -> float:
    std = returns.std()
    if len(returns) < 2 or pd.isna(std) or std <= 0:
        return 0.0
    return returns.mean() / std

def evaluate_signals(df: pd.DataFrame, label: str) -> pd.DataFrame:
    signals = df[df[label] == 1].copy()
    signals["return"] = (
        df["close"].shift(-EXIT_HORIZON_BARS).loc[signals.index] /
        df["close"].loc[signals.index] - 1
    )
    return signals[["return"]].dropna()

def build_event_study(df: pd.DataFrame, label: str, horizons: list[int]) -> pd.DataFrame:
    events = df[df[label] == 1].copy()
    if events.empty:
        return pd.DataFrame()
    if "high" not in df.columns or "low" not in df.columns or "close" not in df.columns:
        logger.warning(
            "Event study skipped for %s: required columns missing (need close/high/low, got %s)",
            label,
            list(df.columns),
        )
        return pd.DataFrame()

    base_cols = [
        c for c in df.columns
        if c.startswith("accZ_") or c.startswith("slope_") or c.startswith("disp_")
    ]
    if "volZ" in df.columns:
        base_cols.append("volZ")

    pos_map = {ts: i for i, ts in enumerate(df.index)}
    day_last_pos = df.groupby(df.index.date).apply(lambda x: x.index[-1])
    day_last_pos = {d: pos_map[last_idx] for d, last_idx in day_last_pos.items()}

    rows = []
    for ts in events.index:
        i = pos_map[ts]
        entry_close = float(df.iloc[i]["close"])
        entry_row = {
            "timestamp": ts,
            "entry_close": entry_close,
            "signal": label
        }
        for c in base_cols:
            entry_row[c] = df.iloc[i][c]

        for h in horizons:
            j = i + h
            if j < len(df):
                window = df.iloc[i + 1:j + 1]
                if len(window) > 0:
                    entry_row[f"close_ret_{h}m"] = float(df.iloc[j]["close"] / entry_close - 1)
                    entry_row[f"high_ret_max_{h}m"] = float(window["high"].max() / entry_close - 1)
                    entry_row[f"low_ret_min_{h}m"] = float(window["low"].min() / entry_close - 1)
                else:
                    entry_row[f"close_ret_{h}m"] = np.nan
                    entry_row[f"high_ret_max_{h}m"] = np.nan
                    entry_row[f"low_ret_min_{h}m"] = np.nan
            else:
                entry_row[f"close_ret_{h}m"] = np.nan
                entry_row[f"high_ret_max_{h}m"] = np.nan
                entry_row[f"low_ret_min_{h}m"] = np.nan

        eod_j = day_last_pos[ts.date()]
        if eod_j > i:
            eod_window = df.iloc[i + 1:eod_j + 1]
            entry_row["close_ret_eod"] = float(df.iloc[eod_j]["close"] / entry_close - 1)
            entry_row["high_ret_max_eod"] = float(eod_window["high"].max() / entry_close - 1)
            entry_row["low_ret_min_eod"] = float(eod_window["low"].min() / entry_close - 1)
        else:
            entry_row["close_ret_eod"] = np.nan
            entry_row["high_ret_max_eod"] = np.nan
            entry_row["low_ret_min_eod"] = np.nan

        rows.append(entry_row)

    return pd.DataFrame(rows).set_index("timestamp")

def add_forward_outcomes_all_rows(df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    out = df.copy()
    n = len(out)
    highs = out["high"].to_numpy()
    lows = out["low"].to_numpy()
    closes = out["close"].to_numpy()

    for h in horizons:
        out[f"close_ret_{h}m"] = out["close"].shift(-h) / out["close"] - 1
        high_max = np.full(n, np.nan, dtype=float)
        low_min = np.full(n, np.nan, dtype=float)
        for i in range(n):
            start = i + 1
            end = min(n, i + h + 1)
            if start < end:
                high_max[i] = highs[start:end].max()
                low_min[i] = lows[start:end].min()
        out[f"high_ret_max_{h}m"] = high_max / closes - 1
        out[f"low_ret_min_{h}m"] = low_min / closes - 1

    out["close_ret_eod"] = np.nan
    out["high_ret_max_eod"] = np.nan
    out["low_ret_min_eod"] = np.nan

    dates_arr = np.array(out.index.date)
    for d in pd.Index(dates_arr).unique():
        day_pos = np.where(dates_arr == d)[0]
        day_close = closes[day_pos]
        day_high = highs[day_pos]
        day_low = lows[day_pos]
        m = len(day_pos)
        if m <= 1:
            continue

        eod_close = day_close[-1]
        out.iloc[day_pos, out.columns.get_loc("close_ret_eod")] = eod_close / day_close - 1

        future_high = np.full(m, np.nan, dtype=float)
        future_low = np.full(m, np.nan, dtype=float)
        for j in range(m - 1):
            future_high[j] = day_high[j + 1:].max()
            future_low[j] = day_low[j + 1:].min()
        out.iloc[day_pos, out.columns.get_loc("high_ret_max_eod")] = future_high / day_close - 1
        out.iloc[day_pos, out.columns.get_loc("low_ret_min_eod")] = future_low / day_close - 1

    return out

# -------------------- DRIVER -------------------- #
if __name__ == "__main__":
    all_summary = []
    log_run_configuration()

    for asset in ASSET_LIST:
        path = DATA_FOLDER_MIN / f"{asset}.csv"
        df_min = load_and_format(path)
        log_asset_details(asset, path, df_min)
        log_price = np.log(df_min["close"])

        # Feature extraction
        feats = []
        for win in WINDOWS_MIN.values():
            feats.append(regression_features(log_price, win))
        feat_df = pd.concat(feats, axis=1)
        feat_df["volZ"] = compute_volZ(df_min, 20).loc[feat_df.index]
        feat_df["close"] = df_min["close"].loc[feat_df.index]
        feat_df["high"] = df_min["high"].loc[feat_df.index]
        feat_df["low"] = df_min["low"].loc[feat_df.index]
        if REGRESSION_MODEL == "linear":
            # Hard guard: in linear mode accZ columns must never reach outputs.
            feat_df = feat_df[[c for c in feat_df.columns if not c.startswith("accZ_")]]
        feat_df = feat_df.reindex(df_min.index)

        # Full analysis table: one row per minute bar (independent from strategy signals)
        full_analysis_df = add_forward_outcomes_all_rows(feat_df, ANALYSIS_HORIZONS)
        full_analysis_df.to_csv(OUTPUT_FOLDER / f"{asset}_feature_outcome_full.csv")
        logger.info(
            "[%s] Full analysis table exported: rows=%d (input minute rows=%d)",
            asset,
            len(full_analysis_df),
            len(df_min),
        )

        # Strategy backtest uses rows where required features are available
        feat_df = feat_df.dropna().copy()

        _train_df, valid_df, test_df = split_data(feat_df)
        logger.info(
            "[%s] Feature rows=%d | valid_rows=%d | test_rows=%d",
            asset, len(feat_df), len(valid_df), len(test_df)
        )

        if REGRESSION_MODEL == "linear":
            logger.info(
                "[%s] Strategy simulation skipped in linear mode (legacy rules depend on accZ_*).",
                asset,
            )
            continue

        for strategy_fn, strategy_name in [
            (simulate_pullback_strategy, "pullback"),
            (simulate_breakout_strategy, "breakout")
        ]:
            results = []
            best_sharpe = -np.inf
            best_params = None

            if strategy_name == "pullback":
                param_combos = [
                    {"accZ_t": accZ_t, "volZ_t": volZ_t, "persist": persist}
                    for accZ_t in PARAM_GRID["accZ_thresh"]
                    for volZ_t in PARAM_GRID["volZ_thresh"]
                    for persist in PARAM_GRID["persistence_len"]
                ]
            else:
                param_combos = [
                    {"accZ_t": accZ_t, "volZ_t": volZ_t, "persist": np.nan}
                    for accZ_t in PARAM_GRID["accZ_thresh"]
                    for volZ_t in PARAM_GRID["volZ_thresh"]
                ]
            logger.info("[%s][%s] Param combinations: %d", asset, strategy_name, len(param_combos))

            for params in param_combos:
                df_valid = valid_df.copy()
                if strategy_name == "pullback":
                    df_valid = strategy_fn(df_valid, params["accZ_t"], params["volZ_t"], int(params["persist"]))
                else:
                    df_valid = strategy_fn(df_valid, params["accZ_t"], params["volZ_t"])

                signal_label = f"signal_{strategy_name}"
                valid_events = build_event_study(df_valid, signal_label, ANALYSIS_HORIZONS)
                valid_rets = valid_events[["close_ret_5m"]].rename(columns={"close_ret_5m": "return"}) if not valid_events.empty else pd.DataFrame(columns=["return"])
                valid_rets = valid_rets.dropna()
                valid_sharpe = sharpe_ratio(valid_rets["return"])

                result = {
                    "asset": asset,
                    "strategy": strategy_name,
                    "accZ_t": params["accZ_t"],
                    "volZ_t": params["volZ_t"],
                    "persist": params["persist"],
                    "valid_count": len(valid_rets),
                    "valid_mean_return_5m": valid_rets["return"].mean(),
                    "valid_std_return_5m": valid_rets["return"].std(),
                    "valid_sharpe": valid_sharpe
                }
                for h in ANALYSIS_HORIZONS:
                    result[f"valid_close_ret_{h}m_mean"] = valid_events[f"close_ret_{h}m"].mean() if not valid_events.empty else np.nan
                    result[f"valid_high_ret_max_{h}m_mean"] = valid_events[f"high_ret_max_{h}m"].mean() if not valid_events.empty else np.nan
                    result[f"valid_low_ret_min_{h}m_mean"] = valid_events[f"low_ret_min_{h}m"].mean() if not valid_events.empty else np.nan
                result["valid_close_ret_eod_mean"] = valid_events["close_ret_eod"].mean() if not valid_events.empty else np.nan
                result["valid_high_ret_max_eod_mean"] = valid_events["high_ret_max_eod"].mean() if not valid_events.empty else np.nan
                result["valid_low_ret_min_eod_mean"] = valid_events["low_ret_min_eod"].mean() if not valid_events.empty else np.nan
                results.append(result)

                if valid_sharpe > best_sharpe and len(valid_rets) > 0:
                    best_sharpe = valid_sharpe
                    best_params = params.copy()

            df_result = pd.DataFrame(results)
            df_result.to_csv(OUTPUT_FOLDER / f"{strategy_name}_{asset}.csv", index=False)

            if best_params is None:
                continue

            df_test = test_df.copy()
            if strategy_name == "pullback":
                df_test = strategy_fn(df_test, best_params["accZ_t"], best_params["volZ_t"], int(best_params["persist"]))
            else:
                df_test = strategy_fn(df_test, best_params["accZ_t"], best_params["volZ_t"])
            signal_label = f"signal_{strategy_name}"
            test_events = build_event_study(df_test, signal_label, ANALYSIS_HORIZONS)
            test_rets = test_events[["close_ret_5m"]].rename(columns={"close_ret_5m": "return"}) if not test_events.empty else pd.DataFrame(columns=["return"])
            test_rets = test_rets.dropna()
            logger.info(
                "[%s][%s] Best params from valid: accZ_t=%.3f volZ_t=%.3f persist=%s | test_signals=%d",
                asset,
                strategy_name,
                best_params["accZ_t"],
                best_params["volZ_t"],
                str(best_params["persist"]),
                len(test_rets),
            )

            if test_rets.shape[0] > 0:
                test_rets.rename(columns={"return": f"{asset}_{strategy_name}"}, inplace=True)
                test_rets.to_csv(OUTPUT_FOLDER / f"{asset}_{strategy_name}_returns.csv")
            if not test_events.empty:
                test_events.to_csv(OUTPUT_FOLDER / f"{asset}_{strategy_name}_test_event_study.csv")

            summary_row = {
                "asset": asset,
                "strategy": strategy_name,
                "accZ_t": best_params["accZ_t"],
                "volZ_t": best_params["volZ_t"],
                "persist": best_params["persist"],
                "test_count": len(test_rets),
                "test_mean_return_5m": test_rets.iloc[:, 0].mean() if len(test_rets) > 0 else np.nan,
                "test_std_return_5m": test_rets.iloc[:, 0].std() if len(test_rets) > 0 else np.nan,
                "test_sharpe": sharpe_ratio(test_rets.iloc[:, 0]) if len(test_rets) > 0 else 0.0
            }
            for h in ANALYSIS_HORIZONS:
                summary_row[f"test_close_ret_{h}m_mean"] = test_events[f"close_ret_{h}m"].mean() if not test_events.empty else np.nan
                summary_row[f"test_high_ret_max_{h}m_mean"] = test_events[f"high_ret_max_{h}m"].mean() if not test_events.empty else np.nan
                summary_row[f"test_low_ret_min_{h}m_mean"] = test_events[f"low_ret_min_{h}m"].mean() if not test_events.empty else np.nan
            summary_row["test_close_ret_eod_mean"] = test_events["close_ret_eod"].mean() if not test_events.empty else np.nan
            summary_row["test_high_ret_max_eod_mean"] = test_events["high_ret_max_eod"].mean() if not test_events.empty else np.nan
            summary_row["test_low_ret_min_eod_mean"] = test_events["low_ret_min_eod"].mean() if not test_events.empty else np.nan
            all_summary.append(summary_row)

    # Aggregated report
    df_summary = pd.DataFrame(all_summary)
    df_summary.to_csv(OUTPUT_FOLDER / "summary_optimal_thresholds.csv", index=False)
    if df_summary.empty:
        logger.info("No valid configurations generated signals.")
    else:
        logger.info("Top configs summary:\n%s", df_summary.sort_values("test_sharpe", ascending=False).head())
