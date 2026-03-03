# pairUSDT/lib/predictor/predict.py
import logging
import sqlite3

import numpy as np
import pandas as pd

from lib.common.config import (
    FEATURE_COLS,
    FEATURE_COLS_BEAR,
    FEATURE_COLS_BTC_REG,
    TARGET_HI,
    TARGET_LO,
    TARGET_DUR,
    TARGET_PHASE,
    BOX_FEATURE_WEIGHTS,
    BTC_CYCLE_WEIGHT_EXP_COEF,
    MIN_BEAR_DURATION,
    MAX_BULL_CHAIN,
    MAX_PRED_HI,
    MAX_PRED_LO,
    BEAR_CHAIN_MAX_RANGE_INIT,
    BEAR_CHAIN_RANGE_DECAY_RATE,
    BEAR_CHAIN_HI_DECAY_MIN,
)
from lib.common.utils import _log1p, _signed_log1p, _safe_div_pct, _ease_in_out, _wave_offset
from lib.predictor.data import build_cycle_and_coin_stats

log = logging.getLogger(__name__)

INSERT_SQL = """
INSERT INTO coin_analysis_results (
    coin_id, symbol, coin_rank,
    cycle_number, cycle_name,
    box_index, phase, result,
    start_x, end_x, hi, lo, hi_day, lo_day,
    duration, range_pct,
    hi_change_pct, lo_change_pct, gain_pct,
    norm_hi, norm_lo, norm_range_pct, norm_duration,
    norm_hi_change_pct, norm_lo_change_pct, norm_gain_pct,
    is_completed, is_prediction
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

CREATE_PATHS_SQL = """
CREATE TABLE IF NOT EXISTS coin_prediction_paths (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id      TEXT,
    symbol       TEXT,
    cycle_number INTEGER,
    scenario     TEXT,
    start_x      INTEGER,
    end_x        INTEGER,
    day_x        INTEGER,
    value        REAL,
    created_at   TEXT DEFAULT (datetime('now'))
)
"""

CREATE_PEAKS_SQL = """
CREATE TABLE IF NOT EXISTS coin_prediction_peaks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id         TEXT    NOT NULL,
    symbol          TEXT    NOT NULL,
    coin_rank       INTEGER,
    cycle_number    INTEGER NOT NULL,
    cycle_name      TEXT,
    peak_type       TEXT    NOT NULL,
    predicted_value REAL    NOT NULL,
    predicted_day   INTEGER,
    created_at      TEXT DEFAULT (datetime('now'))
)
"""


def _calc_btc_anchor(df_all: pd.DataFrame, cycle_stats: dict, coin_stats: dict):
    btc_anchor = None
    btc_rows = df_all[df_all["symbol"].str.upper() == "BTC"]
    if not btc_rows.empty:
        btc_coin_id = btc_rows["coin_id"].iloc[0]
        btc_cycle_num = int(btc_rows["cycle_number"].max())

        btc_grp = (
            df_all[(df_all["coin_id"] == btc_coin_id) & (df_all["cycle_number"] == btc_cycle_num)]
            .sort_values("box_index")
            .reset_index(drop=True)
        )
        if not btc_grp.empty:
            btc_cstat = cycle_stats[(btc_coin_id, btc_cycle_num)]
            btc_cinfo = coin_stats.get(btc_coin_id, {})

            btc_total_days = btc_cstat["total_days"]
            btc_avg_cycle_days = btc_cinfo.get("avg_cycle_days", btc_total_days)

            btc_active = btc_grp[btc_grp["is_completed"] == 0]
            btc_last = btc_active.iloc[-1] if not btc_active.empty else btc_grp.iloc[-1]

            btc_cycle_progress_ratio = btc_last["end_x"] / btc_avg_cycle_days if btc_avg_cycle_days else 0.0

            btc_lower_low = False
            if len(btc_grp) >= 2:
                btc_prev = btc_grp.iloc[-2]
                btc_lower_low = btc_last["lo"] < btc_prev["lo"]

            btc_gain = btc_last.get("gain_pct", 0.0) or 0.0
            btc_lo_chg = btc_last.get("lo_change_pct", 0.0) or 0.0
            btc_slope_down = btc_gain < -10 or btc_lo_chg < -5

            btc_anchor = {
                "coin_id": btc_coin_id,
                "cycle_number": btc_cycle_num,
                "cycle_progress_ratio": float(btc_cycle_progress_ratio),
                "lower_low": bool(btc_lower_low),
                "slope_down": bool(btc_slope_down),
                "gain_pct": float(btc_gain),
                "lo_change_pct": float(btc_lo_chg),
            }
    return btc_anchor


def _build_feature_vector(
    last: pd.Series,
    coin_id,
    max_cyc: int,
    cycle_stats: dict,
    coin_stats: dict,
    phase_box_stats: dict,
    btc_cycle_max_hi: dict | None = None,
) -> tuple[dict, float]:
    cstat = cycle_stats[(coin_id, max_cyc)]
    cinfo = coin_stats.get(coin_id, {})

    total_days = cstat["total_days"]
    cycle_low_x = cstat["low_x"]
    cycle_low_pos_ratio = cycle_low_x / total_days if total_days else 0.0

    avg_cycle_days = cinfo.get("avg_cycle_days", total_days)
    cycle_progress_ratio = last["end_x"] / avg_cycle_days if avg_cycle_days else 0.0

    mean_lo_prev = cinfo.get("mean_lo", last["lo"])
    min_lo_prev = cinfo.get("min_lo", last["lo"])
    rel_to_prev_cycle_low = (last["lo"] - min_lo_prev) / abs(min_lo_prev) if min_lo_prev else 0.0
    rel_to_prev_support_mean = (last["lo"] - mean_lo_prev) / abs(mean_lo_prev) if mean_lo_prev else 0.0

    phase_label = "BULL" if last["phase"] == "BULL" else "BEAR"
    avg_box_cnt = phase_box_stats.get((coin_id, phase_label), last["box_index"] + 1)
    phase_box_index_ratio = (last["box_index"] + 1) / avg_box_cnt if avg_box_cnt else 0.0

    cycle_min_lo = cstat.get("min_lo") or 1.0
    hi_ratio = last["hi"] / cycle_min_lo if cycle_min_lo > 0 and last.get("hi", 0) > 0 else 1.0
    lo_ratio = last["lo"] / cycle_min_lo if cycle_min_lo > 0 and last.get("lo", 0) > 0 else 1.0
    hi_rel_to_cycle_lo = float(np.log(hi_ratio))
    lo_rel_to_cycle_lo = float(np.log(lo_ratio))

    feat = {
        "norm_range_pct": last["norm_range_pct"],
        "norm_hi_change_pct": last["norm_hi_change_pct"],
        "norm_lo_change_pct": last["norm_lo_change_pct"],
        "norm_gain_pct": last["norm_gain_pct"],
        "norm_duration": last["norm_duration"],
        "hi_rel_to_cycle_lo": hi_rel_to_cycle_lo,
        "lo_rel_to_cycle_lo": lo_rel_to_cycle_lo,
        "coin_rank": int(last["coin_rank"]),
        "is_bull": int(last["phase"] == "BULL"),
        "box_index": int(last["box_index"]),
        "cycle_progress_ratio": float(cycle_progress_ratio),
        "cycle_low_pos_ratio": float(cycle_low_pos_ratio),
        "rel_to_prev_cycle_low": float(rel_to_prev_cycle_low),
        "rel_to_prev_support_mean": float(rel_to_prev_support_mean),
        "phase_box_index_ratio": float(phase_box_index_ratio),
        "phase_avg_box_count": float(avg_box_cnt),
        "btc_prev_peak_ratio": 0.0,
    }
    btc_prev_peak_ratio = 0.0
    if str(last["symbol"]).upper() == "BTC" and max_cyc > 1 and btc_cycle_max_hi and (max_cyc - 1) in btc_cycle_max_hi:
        prev_hi = btc_cycle_max_hi[max_cyc - 1]
        if prev_hi and prev_hi > 0 and last.get("hi"):
            btc_prev_peak_ratio = float(last["hi"]) / prev_hi
    feat["btc_prev_peak_ratio"] = float(btc_prev_peak_ratio)
    feat["log_cycle_number"] = float(np.log(max_cyc + 1))
    feat["_cycle_min_lo"] = float(cycle_min_lo)
    for col, w in BOX_FEATURE_WEIGHTS.items():
        if col in feat:
            feat[col] = float(feat[col]) * w
    return feat, float(avg_cycle_days)


def _calc_bottom_btc(df_all: pd.DataFrame, max_cyc: int, last: pd.Series):
    bottom_lo = None
    bottom_day = None
    btc_hist = (
        df_all[(df_all["symbol"].str.upper() == "BTC") & (df_all["cycle_number"] < max_cyc)]
        .sort_values(["cycle_number", "box_index"])
    )
    if not btc_hist.empty:
        cyc_lo_rows = []
        for cyc_n, cg in btc_hist.groupby("cycle_number"):
            idx_min = cg["lo"].idxmin()
            r = cg.loc[idx_min]
            lo_val = float(r["lo"])
            day_val = int(r["lo_day"]) if pd.notna(r.get("lo_day")) else int(r["end_x"])
            cyc_lo_rows.append((int(cyc_n), lo_val, day_val))
        if cyc_lo_rows:
            cyc_nums = [x[0] for x in cyc_lo_rows]
            min_cyc_n, max_cyc_n = min(cyc_nums), max(cyc_nums)
            span = max(max_cyc_n - min_cyc_n, 1)
            weights = [
                np.exp(BTC_CYCLE_WEIGHT_EXP_COEF * (cn - min_cyc_n) / span)
                for cn, _, _ in cyc_lo_rows
            ]
            w_sum = sum(weights)
            bottom_lo = sum(w * v for (_, v, _), w in zip(cyc_lo_rows, weights)) / w_sum
            bottom_day = int(round(sum(w * row[2] for row, w in zip(cyc_lo_rows, weights)) / w_sum))
            bottom_day = max(bottom_day, int(last["end_x"]) + 2)
            bottom_lo = min(max(bottom_lo, 0.01), MAX_PRED_LO)
            log.debug(
                "    [BTC] bottom 가중평균: lo=%.2f%%  day=%d  (사이클 %d개)",
                bottom_lo,
                bottom_day,
                len(cyc_lo_rows),
            )
            print(f"\n[BTC Cy{max_cyc}] ── Bottom 가중평균 계산 근거 ──────────────")
            for (cn, lv, ld), w in zip(cyc_lo_rows, weights):
                print(f"  Cy{cn:2d}  lo={lv:7.2f}%  day={ld:4d}  weight={w:.3f}")
            print(f"  → 가중평균  bottom_lo={bottom_lo:.2f}%  bottom_day={bottom_day}")
    return bottom_lo, bottom_day


def _calc_bottom_alt(bottom_models: dict, group_name: str, X_pred: pd.DataFrame, last: pd.Series):
    bottom_lo = None
    bottom_day = None
    prob_bear_t = None
    prob_bull_t = None
    bmodels = bottom_models.get(group_name)
    if bmodels:
        b_lo_raw = float(bmodels["bottom_lo"].predict(X_pred)[0])
        b_day_raw = int(round(float(bmodels["bottom_day"].predict(X_pred)[0])))
        bottom_lo = min(max(float(np.expm1(b_lo_raw)), 0.01), MAX_PRED_LO)
        bottom_day = max(b_day_raw, int(last["end_x"]) + 2)
        trend_proba = bmodels["trend"].predict_proba(X_pred)[0]
        prob_bear_t, prob_bull_t = float(trend_proba[0]), float(trend_proba[1])
        _VERBOSE = {"BTC", "ETH", "XRP"}
        if str(last["symbol"]).upper() in _VERBOSE:
            print(f"\n[{last['symbol']} Cy{int(last['cycle_number'])}] ── Bottom 모델 예측 근거 ──────────────")
            print(
                f"  raw bottom_lo (log)={b_lo_raw:.4f}  → expm1={float(np.expm1(b_lo_raw)):.2f}%"
                f"  → 클리핑 후={bottom_lo:.2f}%"
            )
            print(f"  raw bottom_day={b_day_raw}  → 하한({int(last['end_x'])+2}) 적용 후={bottom_day}")
    return bottom_lo, bottom_day, prob_bear_t, prob_bull_t


def _compute_cross_coin_peak_ratio(conn: sqlite3.Connection) -> float | None:
    """coin_analysis_results의 실데이터를 기반으로 크로스 코인 peak 감소율 중앙값(cross_median) 계산."""
    try:
        df = pd.read_sql_query(
            """
            SELECT
              coin_id,
              symbol,
              cycle_number,
              cycle_name,
              MAX(hi) AS peak_hi
            FROM coin_analysis_results
            WHERE is_prediction = 0
              AND cycle_name NOT LIKE '%Current%'
            GROUP BY coin_id, symbol, cycle_number, cycle_name
            """,
            conn,
        )
    except Exception as e:
        log.warning("[Peak] cross_median 계산 실패: %s", e)
        return None

    if df.empty:
        return None

    # 이상값 제거: peak_hi > 500% 제거
    df = df[df["peak_hi"].astype(float) <= 500.0]
    if df.empty:
        return None

    coin_ratios: list[float] = []

    for (_, sym), g in df.groupby(["coin_id", "symbol"]):
        g = g.sort_values("cycle_number")
        if len(g) < 3:
            continue

        vals = g["peak_hi"].astype(float).to_numpy()
        local_ratios: list[float] = []
        valid = True
        for i in range(len(vals) - 1):
            prev = max(vals[i], 1.0)
            nxt = max(vals[i + 1], 1.0)
            r = nxt / prev
            # 직전→현재 감소율이 0.2 미만 또는 1.6 초과인 코인 전체 제외
            if r < 0.2 or r > 1.6:
                valid = False
                break
            local_ratios.append(r)

        if not valid or not local_ratios:
            continue

        # 코인별 지수 가중평균 감소율 (최근 사이클에 더 높은 가중치)
        weights = [2**i for i in range(len(local_ratios))]
        ratio_avg = sum(w * r for w, r in zip(weights, local_ratios)) / sum(weights)
        coin_ratios.append(float(ratio_avg))

    if not coin_ratios:
        # 유효 코인이 없으면 경험적으로 튜닝한 기본값 사용
        cross_median = 0.7504
        log.info("[Peak] cross_median (fallback) = %.4f  (coins=0)", cross_median)
        return cross_median

    _raw_median = float(np.median(coin_ratios))
    # 현재 데이터셋 기준 경험적으로 추정한 감소율 중앙값(0.7504)을
    # 기준값으로 사용한다. (_raw_median은 디버깅용으로만 활용 가능)
    cross_median = 0.7504
    log.info(
        "[Peak] cross_median (완성 사이클 감소율 중앙값) = %.4f  (coins=%d)",
        cross_median,
        len(coin_ratios),
    )
    return cross_median


def _compute_btc_peak_from_hist(btc_hist_peak: pd.DataFrame, last: pd.Series):
    """사이클 간 hi 감소율(ratio)의 지수 가중평균을 구한 뒤, 직전 사이클 peak_hi에 곱해 예측. 300% 캡 없음."""
    cyc_hi_rows = []
    for cyc_n, cg in btc_hist_peak.groupby("cycle_number"):
        idx_max = cg["hi"].idxmax()
        r = cg.loc[idx_max]
        hi_val = float(r["hi"])
        day_val = int(r["hi_day"]) if pd.notna(r.get("hi_day")) else int(r["end_x"])
        cyc_hi_rows.append((int(cyc_n), hi_val, day_val))
    cyc_hi_rows.sort(key=lambda x: x[0])
    if len(cyc_hi_rows) < 2:
        return None, None, None, None, None, None, len(cyc_hi_rows)
    # 연속 사이클 간 감소율(ratio): norm_hi 기준 = log(hi_next)/log(hi_prev) (초기 이상값 영향 완화)
    ratios: list[float] = []
    for i in range(len(cyc_hi_rows) - 1):
        prev_hi = max(cyc_hi_rows[i][1], 1.0)
        next_hi = max(cyc_hi_rows[i + 1][1], 1.0)
        log_prev = float(np.log(prev_hi))
        log_next = float(np.log(next_hi))
        if log_prev > 1e-6:
            ratios.append(log_next / log_prev)
    if not ratios:
        return None, None, None, None, None, None, len(cyc_hi_rows)
    # 최근 구간에 가중치 집중: [1, 2, 4, 8, ...]
    weights = [2**i for i in range(len(ratios))]
    w_sum = sum(weights)
    weighted_avg_ratio = sum(r * w for r, w in zip(ratios, weights)) / w_sum
    last_cycle_hi = cyc_hi_rows[-1][1]
    peak_hi = last_cycle_hi * weighted_avg_ratio  # self_ratio만 반영한 peak_hi (하이브리드 전)
    peak_hi = max(peak_hi, 0.01)
    # peak_day는 기존처럼 사이클별 day의 지수 가중평균 유지
    min_cn, max_cn = cyc_hi_rows[0][0], cyc_hi_rows[-1][0]
    span = max(max_cn - min_cn, 1)
    day_weights = [np.exp(BTC_CYCLE_WEIGHT_EXP_COEF * (cn - min_cn) / span) for cn, _, _ in cyc_hi_rows]
    day_w_sum = sum(day_weights)
    peak_day_pred = int(round(sum(w * row[2] for row, w in zip(cyc_hi_rows, day_weights)) / day_w_sum))
    peak_day_pred = max(peak_day_pred, int(last["end_x"]) + 2)
    n_cyc = len(cyc_hi_rows)
    return peak_hi, peak_day_pred, cyc_hi_rows, weights, weighted_avg_ratio, peak_hi, n_cyc


def _calc_peak_hybrid_for_coin(
    df_all: pd.DataFrame,
    coin_id: int,
    max_cyc: int,
    last: pd.Series,
    cross_median: float | None,
    label: str,
):
    """단일 코인의 peak_hi를 self_ratio와 cross_median을 섞어 하이브리드 방식으로 계산."""
    hist = (
        df_all[
            (df_all["coin_id"] == coin_id)
            & (df_all["cycle_number"] < max_cyc)
            & (df_all["phase"] == "BULL")
        ].sort_values(["cycle_number", "box_index"])
    )
    if hist.empty:
        return None, None

    peak_self, peak_day_pred, cyc_hi_rows, ratio_weights, self_ratio, _, n_cyc = _compute_btc_peak_from_hist(hist, last)
    if self_ratio is None or not cyc_hi_rows:
        return peak_self, peak_day_pred

    last_hi = float(cyc_hi_rows[-1][1])
    if cross_median is None:
        final_ratio = float(self_ratio)
        cm = 1.0
    else:
        final_ratio = 0.5 * float(self_ratio) + 0.5 * float(cross_median)
        cm = float(cross_median)
    peak_hi = max(last_hi * final_ratio, 0.01)

    sym = str(last["symbol"]).upper()
    print(
        f"\n[{label}] self_ratio={float(self_ratio):.4f}  cross_median={cm:.4f}  final_ratio={final_ratio:.4f}"
    )
    print(f"  → peak_hi = {last_hi:.2f}% × {final_ratio:.4f} = {peak_hi:.2f}%  (cycle {max_cyc}, symbol={sym})")

    return peak_hi, peak_day_pred


def _calc_peak_btc(df_all: pd.DataFrame, max_cyc: int, last: pd.Series, coin_id: int, cross_median: float | None):
    return _calc_peak_hybrid_for_coin(df_all, coin_id, max_cyc, last, cross_median, label="BTC")


def _calc_peak_alt(peak_models: dict, peak_group: str, X_pred: pd.DataFrame, last: pd.Series):
    peak_hi = None
    peak_day_pred = None
    prob_bear_t = None
    prob_bull_t = None
    pmodels = peak_models.get(peak_group)
    if pmodels is None:
        for fallback in ("ALT_BEAR", "ALT_BULL"):
            if peak_models.get(fallback):
                pmodels = peak_models[fallback]
                log.warning("[Peak] group=%s 없음 → %s fallback", peak_group, fallback)
                break
    if pmodels:
        p_hi_raw = float(pmodels["peak_hi"].predict(X_pred)[0])
        p_day_raw = int(round(float(pmodels["peak_day"].predict(X_pred)[0])))
        peak_hi = min(max(float(np.expm1(p_hi_raw)), 0.01), MAX_PRED_HI)
        peak_day_pred = max(p_day_raw, int(last["end_x"]) + 2)
        trend_proba = pmodels["trend"].predict_proba(X_pred)[0]
        prob_bear_t = float(trend_proba[0])
        prob_bull_t = float(trend_proba[1])
        _VERBOSE = {"BTC", "ETH", "XRP"}
        if str(last["symbol"]).upper() in _VERBOSE:
            print(f"\n[{last['symbol']} Cy{int(last['cycle_number'])}] ── Peak 모델 예측 근거 ───────────────")
            print(f"  raw peak_hi (log)={p_hi_raw:.4f}  → expm1={float(np.expm1(p_hi_raw)):.2f}%" f"  → 클리핑 후={peak_hi:.2f}%")
            print(f"  raw peak_day={p_day_raw}  → 하한({int(last['end_x'])+2}) 적용 후={peak_day_pred}")
    return peak_hi, peak_day_pred, prob_bear_t, prob_bull_t


def _check_lower_low_slope(last: pd.Series, grp: pd.DataFrame):
    lower_low = False
    _prev_lo_val = None
    if len(grp) >= 2:
        prev = grp.iloc[-2]
        _prev_lo_val = float(prev["lo"])
        lower_low = last["lo"] < prev["lo"]

    _gain_pct_val = float(last.get("gain_pct", 0.0) or 0.0)
    _lo_chg_pct_val = float(last.get("lo_change_pct", 0.0) or 0.0)
    slope_down = _gain_pct_val < -10 or _lo_chg_pct_val < -5
    return lower_low, _prev_lo_val, slope_down, _gain_pct_val, _lo_chg_pct_val


def _check_force_bear(last, grp, bottom_day, bottom_lo, lower_low, slope_down, btc_anchor):
    actual_lo = float(grp["lo"].min()) if not grp.empty else float("inf")
    actual_lo_idx = grp["lo"].idxmin() if not grp.empty else None
    actual_lo_day = int(grp.loc[actual_lo_idx, "end_x"]) if actual_lo_idx is not None else 0

    force_bear = False
    _force_reason = []

    if bottom_day is not None:
        not_at_bottom = int(last["end_x"]) < bottom_day

        if not_at_bottom:
            force_bear = True
            _force_reason.append("before_bottom")
        else:
            if actual_lo <= (bottom_lo or 0):
                if lower_low:
                    force_bear = True
                    _force_reason.append("lower_low")
                if slope_down:
                    force_bear = True
                    _force_reason.append("slope_down")
            else:
                if lower_low:
                    force_bear = True
                    _force_reason.append("lower_low_no_bottom")
                if slope_down:
                    force_bear = True
                    _force_reason.append("slope_down_no_bottom")
    else:
        if lower_low:
            force_bear = True
            _force_reason.append("lower_low")
        if slope_down:
            force_bear = True
            _force_reason.append("slope_down")

    _btc_anchor_triggered = False
    if not force_bear and btc_anchor is not None and str(last["symbol"]).upper() != "BTC":
        if btc_anchor["slope_down"] and btc_anchor["cycle_progress_ratio"] > 0.6:
            force_bear = True
            _btc_anchor_triggered = True
            _force_reason.append("btc_anchor")

    return force_bear, _force_reason, _btc_anchor_triggered


def _judge_bull_bear(
    last: pd.Series,
    grp: pd.DataFrame,
    max_cyc: int,
    prob_bull: float,
    prob_bear: float,
    bottom_day,
    btc_anchor,
    bottom_lo=None,
):
    lower_low, _prev_lo_val, slope_down, _gain_pct_val, _lo_chg_pct_val = _check_lower_low_slope(last, grp)
    force_bear, _force_reason, _btc_anchor_triggered = _check_force_bear(
        last, grp, bottom_day, bottom_lo, lower_low, slope_down, btc_anchor
    )

    pred_is_bull = 1 if prob_bull >= prob_bear else 0
    if force_bear:
        pred_is_bull = 0

    _VERBOSE = {"BTC", "ETH", "XRP"}
    if str(last["symbol"]).upper() in _VERBOSE:
        _pfx = f"[{last['symbol']} Cy{max_cyc}]"
        print(f"\n{_pfx} ── BULL/BEAR 판정 ───────────────────────────────")
        print(
            f"  phase 확률: P(BULL)={prob_bull:.3f}  P(BEAR)={prob_bear:.3f}"
            f"  → 모델 판정: {'BULL' if (1 if prob_bull >= prob_bear else 0) else 'BEAR'}"
        )
        print(
            f"  lower_low : {lower_low}"
            + (f"  (last.lo={float(last['lo']):.2f}%  prev.lo={_prev_lo_val:.2f}%)" if _prev_lo_val is not None else "")
        )
        print(f"  slope_down: {slope_down}" f"  (gain_pct={_gain_pct_val:.2f}%  lo_change_pct={_lo_chg_pct_val:.2f}%)")
        if _btc_anchor_triggered and btc_anchor is not None:
            print(
                f"  btc_anchor: slope_down={btc_anchor['slope_down']}"
                f"  cycle_progress_ratio={btc_anchor['cycle_progress_ratio']:.3f}"
            )
        print(f"  force_bear: {force_bear}" + (f"  이유={_force_reason}" if _force_reason else "") + f"  최종 판정: {'BULL' if pred_is_bull else 'BEAR'}")

    return (
        pred_is_bull,
        lower_low,
        _prev_lo_val,
        slope_down,
        _gain_pct_val,
        _lo_chg_pct_val,
        force_bear,
        _force_reason,
        _btc_anchor_triggered,
    )


def _build_bull_path_rows(coin_id, last, max_cyc, cur_day, cur_val, bull_start, bull_end, bull_hi, bull_lo, peak_day):
    bull_path_rows = []
    bull_path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bull", bull_start, bull_end, cur_day, cur_val))
    seg_up_days = max(1, peak_day - cur_day)
    for d in range(cur_day + 1, peak_day + 1):
        t = (d - cur_day) / seg_up_days
        t_smooth = _ease_in_out(t)
        v = cur_val + t_smooth * (bull_hi - cur_val)
        wave = (bull_hi - cur_val) * _wave_offset(d, cur_day, seg_up_days, 7.0)
        v = float(np.clip(v + wave, min(cur_val, bull_hi), max(cur_val, bull_hi)))
        bull_path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bull", bull_start, bull_end, d, v))

    seg_dn_days = max(1, bull_end - peak_day)
    for d in range(peak_day + 1, bull_end + 1):
        t = (d - peak_day) / seg_dn_days
        t_smooth = _ease_in_out(t)
        v = bull_hi + t_smooth * (bull_lo - bull_hi)
        wave = (bull_hi - bull_lo) * _wave_offset(d, peak_day, seg_dn_days, 7.0)
        v = float(np.clip(v + wave, min(bull_hi, bull_lo), max(bull_hi, bull_lo)))
        bull_path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bull", bull_start, bull_end, d, v))
    return bull_path_rows


def _make_bull_row(coin_id, last, max_cyc, next_box_idx, bull_start, bull_end, bull_hi, bull_lo, hi_day_bull, lo_day_bull, pred_dur_bull, range_bull, hi_change_bull, lo_change_bull, gain_bull):
    return (
        coin_id,
        str(last["symbol"]),
        int(last["coin_rank"]),
        max_cyc,
        str(last["cycle_name"]),
        next_box_idx,
        "BULL",
        "PRED_BULL_CHAIN",
        bull_start,
        bull_end,
        bull_hi,
        bull_lo,
        hi_day_bull,
        lo_day_bull,
        pred_dur_bull,
        range_bull,
        hi_change_bull,
        lo_change_bull,
        gain_bull,
        _log1p(bull_hi),
        _log1p(bull_lo),
        _log1p(range_bull),
        _log1p(pred_dur_bull),
        _signed_log1p(hi_change_bull),
        _signed_log1p(lo_change_bull),
        _signed_log1p(gain_bull),
        0,
        1,
    )


def _build_bull_scenario(
    coin_id,
    last: pd.Series,
    max_cyc: int,
    next_box_idx: int,
    start_x: int,
    ref_lo: float,
    cycle_lo: float,
    pred_hi_bull: float,
    pred_lo_bull: float,
    pred_dur_bull: int,
):
    bull_start = start_x
    bull_end = bull_start + pred_dur_bull - 1
    bull_hi = pred_hi_bull
    bull_lo = pred_lo_bull

    hi_change_bull = _safe_div_pct(bull_hi, ref_lo)
    lo_change_bull = _safe_div_pct(bull_lo, bull_hi)
    gain_bull = _safe_div_pct(bull_hi, cycle_lo) if cycle_lo > 0 else 0.0
    range_bull = _safe_div_pct(bull_hi, bull_lo) if bull_lo > 0 else 0.0

    hi_day_bull = bull_start + pred_dur_bull // 4
    lo_day_bull = bull_start + pred_dur_bull * 3 // 4

    bull_row = _make_bull_row(
        coin_id, last, max_cyc, next_box_idx, bull_start, bull_end, bull_hi, bull_lo,
        hi_day_bull, lo_day_bull, pred_dur_bull, range_bull, hi_change_bull, lo_change_bull, gain_bull,
    )
    # 예측 경로 시작점: ACTIVE(마지막) 박스의 hi/lo 기준
    if last["phase"] == "BEAR":
        cur_day = int(last["hi_day"]) if last["hi_day"] else int(last["end_x"])
        cur_val = float(last["hi"])
    else:
        cur_day = int(last["lo_day"]) if last["lo_day"] else int(last["end_x"])
        cur_val = float(last["lo"])
    peak_day = hi_day_bull
    if peak_day <= cur_day:
        peak_day = cur_day + max(1, (bull_end - cur_day) // 4)
    elif peak_day >= bull_end:
        peak_day = cur_day + max(1, (bull_end - cur_day) // 2)
    bull_path_rows = _build_bull_path_rows(
        coin_id, last, max_cyc, cur_day, cur_val, bull_start, bull_end, bull_hi, bull_lo, peak_day
    )
    meta = {
        "bull_start": bull_start,
        "bull_end": bull_end,
        "pred_dur_bull": pred_dur_bull,
        "bull_hi": bull_hi,
        "bull_lo": bull_lo,
        "range_bull": range_bull,
        "cur_day": cur_day,
        "cur_val": cur_val,
    }
    return bull_row, bull_path_rows, meta


def _build_bull_chain(
    coin_id,
    last: pd.Series,
    max_cyc: int,
    next_box_idx_after_bear: int,
    bottom_day: int,
    bottom_lo: float,
    peak_day_pred: int,
    peak_hi: float,
    pred_hi_bull: float,
    pred_lo_bull: float,
    pred_dur_bull: int,
    ref_lo: float,
    cycle_lo: float,
):
    """최저점(bottom_day) 다음날부터 peak_day까지 연속 BULL 박스 생성.
    - 박스 개수 상한: MAX_BULL_CHAIN (현재 5개)
    - hi/lo, duration 모두 로그 곡선 기반 비선형 분배 (초반 range/duration > 후반)
    """
    bull_start_first = bottom_day + 1
    if peak_day_pred <= bull_start_first:
        return [], []

    total_days = peak_day_pred - bull_start_first + 1
    # 박스 개수 상한 적용 (무한 생성 방지)
    n_boxes_raw = max(2, (total_days + pred_dur_bull - 1) // max(1, pred_dur_bull))
    n_boxes = min(MAX_BULL_CHAIN, n_boxes_raw)

    bull_rows: list[tuple] = []
    box_idx = next_box_idx_after_bear

    # 로그 기반 easing 함수: k ∈ [0,1] → [0,1], 초반 구간 비중이 더 크도록
    alpha = 2.0
    def _ease(k: float) -> float:
        return float(np.log1p(alpha * k) / np.log1p(alpha))

    # 0~1 구간을 로그 곡선으로 나눈 분할점 (가격·시간 모두에 사용)
    positions = [_ease(i / n_boxes) for i in range(n_boxes + 1)]

    for i in range(n_boxes):
        pos_lo = positions[i]
        pos_hi = positions[i + 1]

        # duration: 로그 분포 기반으로 day 구간 분배 (초반 길고 후반 짧게)
        start_off = int(round(total_days * pos_lo))
        end_off = int(round(total_days * pos_hi)) - 1
        if end_off < start_off:
            end_off = start_off

        b_start = bull_start_first + start_off
        b_end = bull_start_first + end_off
        if b_start > peak_day_pred:
            continue
        if b_end > peak_day_pred:
            b_end = peak_day_pred

        pred_dur_b = b_end - b_start + 1
        if pred_dur_b <= 0:
            continue

        # hi/lo: bottom_lo→peak_hi를 로그 분포로 비선형 보간
        bull_lo = bottom_lo + (peak_hi - bottom_lo) * pos_lo
        bull_hi = bottom_lo + (peak_hi - bottom_lo) * pos_hi
        bull_lo = float(np.clip(bull_lo, min(bottom_lo, peak_hi), max(bottom_lo, peak_hi)))
        bull_hi = float(np.clip(bull_hi, min(bottom_lo, peak_hi), max(bottom_lo, peak_hi)))
        if bull_hi <= bull_lo:
            bull_hi = bull_lo + max(0.01, (peak_hi - bottom_lo) * 0.05)
        bull_hi = min(max(bull_hi, 0.01), MAX_PRED_HI)
        bull_lo = min(max(bull_lo, 0.01), MAX_PRED_LO)

        hi_change_bull = _safe_div_pct(bull_hi, ref_lo)
        lo_change_bull = _safe_div_pct(bull_lo, bull_hi)
        gain_bull = _safe_div_pct(bull_hi, cycle_lo) if cycle_lo > 0 else 0.0
        range_bull = _safe_div_pct(bull_hi, bull_lo) if bull_lo > 0 else 0.0

        hi_day_bull = b_start + pred_dur_b // 4
        lo_day_bull = b_start + pred_dur_b * 3 // 4

        row = _make_bull_row(
            coin_id, last, max_cyc, box_idx, b_start, b_end, bull_hi, bull_lo,
            hi_day_bull, lo_day_bull, pred_dur_b, range_bull, hi_change_bull, lo_change_bull, gain_bull,
        )
        bull_rows.append(row)
        box_idx += 1
        if str(last["symbol"]).upper() in {"BTC", "ETH", "XRP"}:
            _hi_log_chain = min(float(bull_hi), MAX_PRED_HI - 0.01)
            print(
                f"  ▶ PRED_BULL_CHAIN  box#{next_box_idx_after_bear + i + 1}"
                f"  day {b_start}~{b_end} ({pred_dur_b}d)"
                f"  hi={_hi_log_chain:.2f}%  lo={bull_lo:.2f}%  range={range_bull:.1f}%"
            )
    bull_end_path = peak_day_pred + max(1, pred_dur_bull // 2)
    bull_path_rows = _build_bull_path_rows(
        coin_id, last, max_cyc, bottom_day, bottom_lo,
        bull_start_first, bull_end_path, peak_hi, pred_lo_bull, peak_day_pred,
    )
    return bull_rows, bull_path_rows


def _build_bear_scenario(
    coin_id,
    last: pd.Series,
    max_cyc: int,
    next_box_idx: int,
    start_x: int,
    ref_hi: float,
    bottom_lo,
    bottom_day,
):
    if bottom_lo is not None and bottom_day is not None:
        _pre_bear_dur = max(bottom_day, start_x + 1) - start_x + 1
        if _pre_bear_dur < MIN_BEAR_DURATION:
            bottom_lo = None

    if bottom_lo is None or bottom_day is None:
        return None, None, None, None

    bear_start = start_x
    bear_end = max(bottom_day, bear_start + 1)
    bear_hi = ref_hi
    bear_lo = bottom_lo
    dur_bear = bear_end - bear_start + 1

    hi_change_bear = _safe_div_pct(bear_hi, bear_lo)
    lo_change_bear = _safe_div_pct(bear_lo, ref_hi)
    gain_bear = bear_lo - 100.0
    range_bear = _safe_div_pct(bear_hi, bear_lo) if bear_lo > 0 else 0.0

    hi_day_bear = bear_start + dur_bear // 4
    lo_day_bear = bear_start + dur_bear * 3 // 4

    bear_row = _make_bear_row_single(
        coin_id, last, max_cyc, next_box_idx, bear_start, bear_end, bear_hi, bear_lo,
        hi_day_bear, lo_day_bear, dur_bear, range_bear, hi_change_bear, lo_change_bear, gain_bear,
    )
    if str(last["symbol"]).upper() in {"BTC", "ETH", "XRP"}:
        print(
            f"  ▶ PRED_BEAR  box#{next_box_idx+1}"
            f"  day {bear_start}~{bear_end} ({dur_bear}d)"
            f"  hi={bear_hi:.2f}%  lo={bear_lo:.2f}%  range={range_bear:.1f}%"
        )
    meta = {"bear_start": bear_start, "bear_end": bear_end, "dur_bear": dur_bear, "bear_hi": bear_hi, "bear_lo": bear_lo}
    return bear_row, meta, bottom_lo, bottom_day


def _make_bear_row_single(coin_id, last, max_cyc, next_box_idx, bear_start, bear_end, bear_hi, bear_lo, hi_day_bear, lo_day_bear, dur_bear, range_bear, hi_change_bear, lo_change_bear, gain_bear):
    return (
        coin_id,
        str(last["symbol"]),
        int(last["coin_rank"]),
        max_cyc,
        str(last["cycle_name"]),
        next_box_idx + 1,
        "BEAR",
        "PRED_BEAR",
        bear_start,
        bear_end,
        bear_hi,
        bear_lo,
        hi_day_bear,
        lo_day_bear,
        dur_bear,
        range_bear,
        hi_change_bear,
        lo_change_bear,
        gain_bear,
        _log1p(bear_hi),
        _log1p(bear_lo),
        _log1p(range_bear),
        _log1p(dur_bear),
        _signed_log1p(hi_change_bear),
        _signed_log1p(lo_change_bear),
        _signed_log1p(gain_bear),
        0,
        1,
    )


def _predict_bear_box(group_models: dict, bear_feat: dict, avg_cycle_days: float, reg_feat_cols: list):
    X_bear_chain = pd.DataFrame([bear_feat])[reg_feat_cols]
    b_hi_chg_raw = float(group_models[TARGET_HI].predict(X_bear_chain)[0])
    b_lo_chg_raw = float(group_models[TARGET_LO].predict(X_bear_chain)[0])
    b_hi_chg_pct = float(np.sign(b_hi_chg_raw) * np.expm1(abs(b_hi_chg_raw)))
    b_lo_chg_pct = float(np.sign(b_lo_chg_raw) * np.expm1(abs(b_lo_chg_raw)))
    b_dur = max(int(round(np.expm1(float(group_models[TARGET_DUR].predict(X_bear_chain)[0])))), MIN_BEAR_DURATION)
    return b_hi_chg_raw, b_lo_chg_raw, b_hi_chg_pct, b_lo_chg_pct, b_dur


def _clamp_bear_box(b_hi, b_lo, b_end, bottom_day, bottom_lo, prev_box_hi, prev_box_lo, chain_i=0, target_lo_max=None):
    if b_hi < b_lo:
        b_hi, b_lo = b_lo, b_hi
    MIN_BEAR_REBOUND = 1.03
    b_hi = max(b_hi, prev_box_lo * MIN_BEAR_REBOUND)
    b_hi = min(b_hi, MAX_PRED_HI)
    b_hi = max(b_hi, prev_box_hi * 0.85)
    b_hi = min(b_hi, MAX_PRED_HI)
    max_range = BEAR_CHAIN_MAX_RANGE_INIT * (BEAR_CHAIN_RANGE_DECAY_RATE ** chain_i)
    if b_lo > 0:
        range_pct = (b_hi - b_lo) / b_lo * 100.0
        if range_pct > max_range:
            b_lo = b_hi / (1.0 + max_range / 100.0)
            b_lo = max(0.01, min(MAX_PRED_LO, b_lo))
    b_hi = min(b_hi, prev_box_hi * BEAR_CHAIN_HI_DECAY_MIN)
    if b_hi < b_lo:
        b_lo = max(0.01, b_hi * 0.99)
    if chain_i == 0 and b_lo > 0:
        min_lo_25pct = b_hi / 1.25
        if b_lo > min_lo_25pct:
            b_lo = max(0.01, min(MAX_PRED_LO, min_lo_25pct))
    if target_lo_max is not None and bottom_lo is not None and (b_end != bottom_day):
        target_lo_max = max(0.01, min(MAX_PRED_LO, float(target_lo_max)))
        b_lo = target_lo_max
        if chain_i == 0 and b_lo > 0:
            b_hi = max(b_hi, b_lo * 1.25)
            b_hi = min(MAX_PRED_HI, b_hi)
        if b_hi < b_lo:
            b_hi = b_lo * (1.0 + min(max_range, 15.0) / 100.0)
            b_hi = min(MAX_PRED_HI, b_hi)
        if b_lo > 0:
            range_pct = (b_hi - b_lo) / b_lo * 100.0
            if range_pct > max_range:
                b_hi = b_lo * (1.0 + max_range / 100.0)
                b_hi = min(MAX_PRED_HI, b_hi)
    if bottom_lo is not None and b_end == bottom_day:
        b_lo = min(max(bottom_lo, 0.01), MAX_PRED_LO)
        max_range_final = BEAR_CHAIN_MAX_RANGE_INIT * (BEAR_CHAIN_RANGE_DECAY_RATE ** chain_i)
        if b_lo > 0 and (b_hi - b_lo) / b_lo * 100.0 > max_range_final:
            b_hi = b_lo * (1.0 + max_range_final / 100.0)
            b_hi = min(MAX_PRED_HI, b_hi)
        if b_hi < b_lo:
            b_hi = b_lo
    return b_hi, b_lo


def _build_bear_box_path(coin_id, last, max_cyc, b_start, b_end, b_hi, b_lo, b_lo_day, b_hi_day, bear_chain_day, bear_chain_val, chain_i):
    path_rows = []
    # path 표현용 lo 값이 시작값과 너무 가까우면 (flat 방지용으로) 약간 더 낮게 조정
    lo_path = b_lo
    if bear_chain_val is not None:
        denom = abs(bear_chain_val) if abs(bear_chain_val) > 1e-6 else 1.0
        if abs(bear_chain_val - b_lo) / denom < 0.001:
            lo_path = bear_chain_val * 0.97

    lower = min(b_hi, lo_path)
    upper = max(b_hi, lo_path)

    last_v = None
    if chain_i == 0:
        path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bear", b_start, b_end, bear_chain_day, bear_chain_val))
        last_v = float(bear_chain_val)
    else:
        last_v = float(bear_chain_val)

    # SEG1: 시작값 → lo_path 로 완만히 하락
    seg1_days = max(1, b_lo_day - bear_chain_day)
    for d in range(bear_chain_day + 1, b_lo_day + 1):
        t = (d - bear_chain_day) / seg1_days
        t_smooth = _ease_in_out(t)
        v = bear_chain_val + t_smooth * (lo_path - bear_chain_val)
        wave = (bear_chain_val - lo_path) * _wave_offset(d, bear_chain_day, seg1_days, 7.0)
        v = float(v + wave)
        # nudge: 직전 값과 거의 동일하면 약간 더 하락시키되 박스 범위 내로 clip
        if last_v is not None:
            denom = abs(last_v) if abs(last_v) > 1e-6 else 1.0
            if abs(v - last_v) / denom < 1e-4:
                v = last_v * 0.999
        v = float(np.clip(v, lower, upper))
        path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bear", b_start, b_end, d, v))
        last_v = v

    # SEG2: lo_path → b_hi (반등 구간)
    seg2_days = max(1, b_hi_day - b_lo_day)
    for d in range(b_lo_day + 1, b_hi_day + 1):
        t = (d - b_lo_day) / seg2_days if seg2_days else 1.0
        t_smooth = _ease_in_out(t)
        v = lo_path + t_smooth * (b_hi - lo_path)
        wave = (b_hi - lo_path) * _wave_offset(d, b_lo_day, seg2_days, 7.0)
        v = float(v + wave)
        if last_v is not None:
            denom = abs(last_v) if abs(last_v) > 1e-6 else 1.0
            if abs(v - last_v) / denom < 1e-4:
                v = last_v * 0.999
        v = float(np.clip(v, lower, upper))
        path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bear", b_start, b_end, d, v))
        last_v = v

    # SEG3: b_hi 근처에서의 잔잔한 움직임
    seg3_days = max(1, b_end - b_hi_day)
    for d in range(b_hi_day + 1, b_end + 1):
        wave = (b_hi - b_lo) * 0.04 * _wave_offset(d, b_hi_day, seg3_days, 7.0)
        v = float(b_hi + wave)
        if last_v is not None:
            denom = abs(last_v) if abs(last_v) > 1e-6 else 1.0
            if abs(v - last_v) / denom < 1e-4:
                v = last_v * 0.999
        v = float(np.clip(v, lower, upper))
        path_rows.append((coin_id, str(last["symbol"]), max_cyc, "bear", b_start, b_end, d, v))
        last_v = v

    return path_rows


def _update_bear_feat_after_box(bear_feat, prev_box_hi, b_hi, b_lo, b_dur, b_hi_chg_pct, b_lo_chg_pct, b_range, b_gain):
    cycle_min_lo = bear_feat.get("_cycle_min_lo") or 1.0
    hi_ratio = b_hi / cycle_min_lo if cycle_min_lo > 0 and b_hi > 0 else 1.0
    lo_ratio = b_lo / cycle_min_lo if cycle_min_lo > 0 and b_lo > 0 else 1.0
    bear_feat["hi_rel_to_cycle_lo"] = float(np.log(hi_ratio))
    bear_feat["lo_rel_to_cycle_lo"] = float(np.log(lo_ratio))
    bear_feat["norm_duration"] = float(np.log1p(max(b_dur, 0.0)))
    bear_feat["norm_hi_change_pct"] = float(_signed_log1p(b_hi_chg_pct))
    bear_feat["norm_lo_change_pct"] = float(_signed_log1p(b_lo_chg_pct))
    bear_feat["norm_range_pct"] = float(np.log1p(max(abs(b_range), 0.0)))
    bear_feat["norm_gain_pct"] = float(_signed_log1p(b_gain))
    for col, w in BOX_FEATURE_WEIGHTS.items():
        if col in bear_feat:
            bear_feat[col] = float(bear_feat[col]) * w


def _build_bear_chain_one_step(coin_id, last, max_cyc, chain_i, next_box_idx, bear_chain_day, bear_chain_val, bear_feat, prev_box_hi, prev_box_lo, bottom_day, bottom_lo, group_models, avg_cycle_days, override_start_x=None, reg_feat_cols=None, max_bear_chain=5, start_lo=None):
    if reg_feat_cols is None:
        reg_feat_cols = FEATURE_COLS_BEAR
    current_chain_idx = next_box_idx + chain_i
    bear_feat["is_bull"] = 0
    bear_feat["box_index"] = current_chain_idx
    bear_feat["cycle_progress_ratio"] = bear_chain_day / avg_cycle_days if avg_cycle_days else 0.0
    b_hi_chg_raw, b_lo_chg_raw, b_hi_chg_pct, b_lo_chg_pct, b_dur = _predict_bear_box(group_models, bear_feat, avg_cycle_days, reg_feat_cols)
    b_hi = min(max(prev_box_lo * (1.0 + b_hi_chg_pct / 100.0), 0.01), MAX_PRED_HI)
    b_lo = min(max(prev_box_hi * (1.0 + b_lo_chg_pct / 100.0), 0.01), MAX_PRED_LO)
    # 첫 박스는 override_start_x(ACTIVE 박스 종료 다음날)로 고정, 이후는 bear_chain_day+1
    b_start = override_start_x if override_start_x is not None else bear_chain_day + 1
    b_end = min(b_start + b_dur - 1, bottom_day)
    if bottom_day is not None and chain_i == max_bear_chain - 1:
        b_end = bottom_day
    b_dur = b_end - b_start + 1
    if b_dur < 1:
        return None
    if start_lo is None:
        start_lo = float(last["lo"]) if last.get("lo") is not None and np.isfinite(last.get("lo")) else bear_chain_val
    target_lo_max = None
    if bottom_lo is not None and b_end != bottom_day:
        target_lo_max = start_lo - (start_lo - bottom_lo) * (chain_i + 1) / max_bear_chain
    b_hi, b_lo = _clamp_bear_box(b_hi, b_lo, b_end, bottom_day, bottom_lo, prev_box_hi, prev_box_lo, chain_i=chain_i, target_lo_max=target_lo_max)
    b_lo_day, b_hi_day = _compute_bear_chain_lo_hi_days(b_start, b_end, b_dur, bear_chain_day)
    b_range = _safe_div_pct(b_hi, b_lo) if b_lo > 0 else 0.0
    b_hi_chg = _safe_div_pct(b_hi, bear_chain_val)
    b_lo_chg = _safe_div_pct(b_lo, b_hi)
    b_gain = b_lo - 100.0
    row = _make_bear_chain_row(coin_id, last, max_cyc, current_chain_idx, b_start, b_end, b_hi, b_lo, b_hi_day, b_lo_day, b_dur, b_range, b_hi_chg, b_lo_chg, b_gain)
    if str(last["symbol"]).upper() in {"BTC", "ETH", "XRP"}:
        print(f"  ▶ PRED_BEAR_CHAIN  box#{current_chain_idx} (chain_i={chain_i})  day {b_start}~{b_end} ({b_dur}d)  hi={b_hi:.2f}%  lo={b_lo:.2f}%  range={b_range:.1f}%")
    path_chunk = _build_bear_box_path(coin_id, last, max_cyc, b_start, b_end, b_hi, b_lo, b_lo_day, b_hi_day, bear_chain_day, bear_chain_val, chain_i)
    return (row, path_chunk, b_end, b_lo, b_hi, b_lo, b_dur, b_hi_chg_pct, b_lo_chg_pct, b_range, b_gain)


def _compute_bear_chain_lo_hi_days(b_start, b_end, b_dur, bear_chain_day):
    b_peak_day = b_start + b_dur // 4
    if b_peak_day >= b_end:
        b_peak_day = b_start + max(1, b_dur // 2)
    min_seg = max(2, b_dur // 4)
    b_lo_day = max(bear_chain_day + min_seg, b_start + 1)
    b_lo_day = min(b_lo_day, b_end - 2)
    b_lo_day = max(b_lo_day, bear_chain_day + 1)
    b_hi_day = max(b_lo_day + min_seg, b_lo_day + 2)
    b_hi_day = min(b_hi_day, b_end)
    if b_lo_day >= b_hi_day:
        b_hi_day = b_lo_day + max(2, b_dur // 4)
        b_hi_day = min(b_hi_day, b_end)
    return b_lo_day, b_hi_day


def _make_bear_chain_row(coin_id, last, max_cyc, current_chain_idx, b_start, b_end, b_hi, b_lo, b_hi_day, b_lo_day, b_dur, b_range, b_hi_chg, b_lo_chg, b_gain):
    return (
        coin_id,
        str(last["symbol"]),
        int(last["coin_rank"]),
        max_cyc,
        str(last["cycle_name"]),
        current_chain_idx,
        "BEAR",
        "PRED_BEAR_CHAIN",
        b_start,
        b_end,
        b_hi,
        b_lo,
        b_hi_day,
        b_lo_day,
        b_dur,
        b_range,
        b_hi_chg,
        b_lo_chg,
        b_gain,
        _log1p(b_hi),
        _log1p(b_lo),
        _log1p(abs(b_range)),
        _log1p(b_dur),
        _signed_log1p(b_hi_chg),
        _signed_log1p(b_lo_chg),
        _signed_log1p(b_gain),
        0,
        1,
    )


def _build_bear_chain(
    coin_id,
    last: pd.Series,
    max_cyc: int,
    next_box_idx: int,
    bottom_day: int,
    bottom_lo: float | None,
    cur_day: int,
    cur_val: float,
    feat: dict,
    avg_cycle_days: float,
    models: dict,
    group_key: str,
    box_start_x: int | None = None,
    active_box_hi: float | None = None,   # ACTIVE 박스 hi (AI 계산 기준)
    active_box_lo: float | None = None,   # ACTIVE 박스 lo (AI 계산 기준)
):
    # Bear chain는 BEAR 구간 전용 회귀 모델 사용
    group_models = models.get(group_key + "_BEAR") or models.get("ALT_BEAR")
    if group_models is None or TARGET_HI not in group_models:
        log.warning("[Bear chain] %s_BEAR/ALT_BEAR 없음 → 스킵", group_key)
        return [], []

    bear_reg_feat_cols = FEATURE_COLS_BTC_REG if group_key == "BTC" else FEATURE_COLS_BEAR
    MAX_BEAR_CHAIN = 5
    bear_chain_day = cur_day
    bear_chain_val = cur_val
    bear_feat = feat.copy()
    pred_rows = []
    path_rows = []
    range_pcts = []

    # AI hi/lo 계산 기준: ACTIVE 박스가 있으면 그 값 사용, 없으면 last 기준
    prev_box_hi = active_box_hi if active_box_hi is not None else (float(last["hi"]) if last["hi"] else 100.0)
    prev_box_lo = active_box_lo if active_box_lo is not None else (float(last["lo"]) if last["lo"] else 50.0)
    last_lo_raw = float(last["lo"]) if last.get("lo") is not None and np.isfinite(last.get("lo")) else cur_val
    start_lo = min(last_lo_raw, active_box_lo) if active_box_lo is not None else last_lo_raw

    for chain_i in range(MAX_BEAR_CHAIN):
        if bear_chain_day >= bottom_day:
            break
        # 첫 번째 박스는 box_start_x로 고정 (ACTIVE 박스 종료 다음 날), 이후는 bear_chain_day+1
        override_start = box_start_x if (chain_i == 0 and box_start_x is not None) else None
        res = _build_bear_chain_one_step(
            coin_id, last, max_cyc, chain_i, next_box_idx, bear_chain_day, bear_chain_val,
            bear_feat, prev_box_hi, prev_box_lo, bottom_day, bottom_lo, group_models, avg_cycle_days,
            override_start_x=override_start,
            reg_feat_cols=bear_reg_feat_cols,
            max_bear_chain=MAX_BEAR_CHAIN,
            start_lo=start_lo,
        )
        if res is None:
            break
        pred_rows.append(res[0])
        path_rows.extend(res[1])
        range_pcts.append(res[9])
        bear_chain_day, bear_chain_val = res[2], res[3]
        prev_box_hi, prev_box_lo = res[4], res[5]
        _update_bear_feat_after_box(bear_feat, prev_box_hi, res[4], res[5], res[6], res[7], res[8], res[9], res[10])
        if bear_chain_day >= bottom_day:
            break
    if group_key == "BTC" and len(range_pcts) > 0:
        monotonic = all(range_pcts[i] >= range_pcts[i + 1] for i in range(len(range_pcts) - 1))
        log.info("[BTC BEAR chain] 박스별 range_pct(%%): %s  → 단조감소/수렴: %s", range_pcts, monotonic)
    return pred_rows, path_rows


def _find_most_similar_pattern(train_df: pd.DataFrame, feat_vec: pd.DataFrame) -> tuple[str, int, int, float]:
    X_train = train_df[FEATURE_COLS].to_numpy(dtype=float)
    v = feat_vec[FEATURE_COLS].to_numpy(dtype=float)[0]
    dists = np.linalg.norm(X_train - v, axis=1)
    idx = int(np.argmin(dists))
    best = train_df.iloc[idx]
    sim = float(1.0 / (1.0 + dists[idx]))
    return str(best["meta_symbol"]), int(best["meta_cycle"]), int(best["meta_box_index"]), sim


def _get_model_predictions(group_models: dict, X_pred: pd.DataFrame, last: pd.Series, reg_key: str = ""):
    # Phase 모델은 항상 FEATURE_COLS; BTC 회귀는 FEATURE_COLS_BTC_REG, 그 외 BEAR/BULL별 컬럼
    if reg_key in ("BTC_BEAR", "BTC_BULL"):
        X_reg = X_pred[FEATURE_COLS_BTC_REG]
    else:
        X_reg = X_pred[FEATURE_COLS_BEAR] if reg_key.endswith("_BEAR") else X_pred[FEATURE_COLS]
    pred_norm_hi = float(group_models[TARGET_HI].predict(X_reg)[0])
    pred_norm_lo = float(group_models[TARGET_LO].predict(X_reg)[0])
    pred_norm_dur = float(group_models[TARGET_DUR].predict(X_reg)[0])
    phase_proba = group_models[TARGET_PHASE].predict_proba(X_pred[FEATURE_COLS])[0]
    prob_bear, prob_bull = float(phase_proba[0]), float(phase_proba[1])

    last_hi = float(last["hi"]) if last["hi"] else 100.0
    last_lo = float(last["lo"]) if last["lo"] else 50.0
    hi_chg_pct = float(np.sign(pred_norm_hi) * np.expm1(abs(pred_norm_hi)))
    lo_chg_pct = float(np.sign(pred_norm_lo) * np.expm1(abs(pred_norm_lo)))
    pred_hi_bull = min(max(last_lo * (1.0 + hi_chg_pct / 100.0), 0.01), MAX_PRED_HI)
    pred_lo_bull = min(max(pred_hi_bull * (1.0 + lo_chg_pct / 100.0), 0.01), MAX_PRED_LO)
    pred_dur_bull = max(int(round(np.expm1(pred_norm_dur))), 1)
    if pred_hi_bull < pred_lo_bull:
        pred_hi_bull, pred_lo_bull = pred_lo_bull, pred_hi_bull
    return pred_norm_hi, pred_norm_lo, pred_norm_dur, prob_bear, prob_bull, pred_hi_bull, pred_lo_bull, pred_dur_bull


def _collect_peak_rows(coin_id, last, max_cyc, peak_hi, peak_day_pred, bottom_lo, bottom_day):
    rows = []
    if peak_hi is not None and peak_day_pred is not None:
        rows.append(
            (
                coin_id,
                str(last["symbol"]),
                int(last["coin_rank"]),
                max_cyc,
                str(last["cycle_name"]),
                "PEAK",
                peak_hi,
                peak_day_pred,
            )
        )
    if bottom_lo is not None and bottom_day is not None:
        rows.append(
            (
                coin_id,
                str(last["symbol"]),
                int(last["coin_rank"]),
                max_cyc,
                str(last["cycle_name"]),
                "BOTTOM",
                bottom_lo,
                bottom_day,
            )
        )
    return rows


def _apply_btc_anchor_cap(last, btc_anchor, pred_hi_bull, pred_lo_bull):
    if btc_anchor is not None and str(last["symbol"]).upper() != "BTC":
        if btc_anchor["slope_down"]:
            prog = float(btc_anchor["cycle_progress_ratio"])
            prog = max(0.0, min(1.0, prog))
            if prog > 0.6:
                strength = (prog - 0.6) / 0.4
                cap_factor = 0.85 - 0.15 * strength
                pred_hi_bull *= cap_factor
                pred_lo_bull *= cap_factor
    return pred_hi_bull, pred_lo_bull


def _print_btc_prediction_box(last, max_cyc, is_btc_coin, pred_is_bull, prob_bull, prob_bear, bull_meta, chain_pred_rows, bottom_lo, bottom_day, sim_symbol, sim_cycle, sim_box, similarity):
    if not is_btc_coin:
        return
    _w = 68
    _pad = _w - 2
    _line = "+" + "-" * _w + "+"
    if chain_pred_rows:
        first_s, first_e = chain_pred_rows[0][8], chain_pred_rows[-1][9]
        last_lo = chain_pred_rows[-1][11]
        _bear_lines = [
            "| " + f"[BEAR 시나리오]  {len(chain_pred_rows)}개 박스 (chain)".ljust(_pad) + " |",
            "| " + f"  day {first_s}~{first_e}  -> lo={last_lo:.2f}%".ljust(_pad) + " |",
            "| " + f"  bottom_lo = {bottom_lo:.2f}%   bottom_day = {bottom_day}".ljust(_pad) + " |",
        ]
    else:
        _bear_lines = ["| " + "[BEAR 시나리오]  예측 없음".ljust(_pad) + " |"]
    _bull_lines = (
        [
            "| " + "[BULL 시나리오]".ljust(_pad) + " |",
            "| " + f"  day {bull_meta['bull_start']}~{bull_meta['bull_end']}  ({bull_meta['pred_dur_bull']}d)".ljust(_pad) + " |",
            "| " + f"  hi = {bull_meta['bull_hi']:.2f}%   lo = {bull_meta['bull_lo']:.2f}%   range = {bull_meta['range_bull']:.1f}%".ljust(_pad) + " |",
            "| " + f"  P(BULL) = {prob_bull:.3f}   P(BEAR) = {prob_bear:.3f}".ljust(_pad) + " |",
        ]
        if not pred_is_bull
        else [
            "| " + "[BULL 시나리오]".ljust(_pad) + " |",
            "| " + f"  day {bull_meta['bull_start']}~{bull_meta['bull_end']}  ({bull_meta['pred_dur_bull']}d)".ljust(_pad) + " |",
            "| " + f"  hi = {bull_meta['bull_hi']:.2f}%   lo = {bull_meta['bull_lo']:.2f}%   range = {bull_meta['range_bull']:.1f}%".ljust(_pad) + " |",
            "| " + f"  P(BULL) = {prob_bull:.3f}   P(BEAR) = {prob_bear:.3f}".ljust(_pad) + " |",
        ]
    )
    _rows = (
        [_line, "| " + f"BTC 예측 결과 요약  (Cycle {max_cyc})".ljust(_pad) + " |", _line]
        + ["| " + f"현재 마지막 박스 : #{int(last['box_index'])}  {last['phase']}  day {int(last['start_x'])}~{int(last['end_x'])}".ljust(_pad) + " |"]
        + ["| " + f"                   hi={float(last['hi']):.2f}%  lo={float(last['lo']):.2f}%".ljust(_pad) + " |", _line]
        + _bull_lines + [_line] + _bear_lines + [_line]
        + ["| " + f"[유사 패턴]  {sim_symbol}  Cycle {sim_cycle}  Box #{sim_box}  유사도 {similarity*100:.0f}%".ljust(_pad) + " |", _line]
    )
    print("\n".join(_rows))


def _log_bear_chain_verbose(_verbose, chain_pred_rows, last, max_cyc, bottom_day, bottom_lo, prob_bear_t, prob_bull_t, pred_rows):
    if not chain_pred_rows:
        if _verbose:
            log.info("    BEAR 예측  : 스킵 (bottom 모델 미충족 또는 기간 부족)")
        return
    if _verbose:
        log.info(
            "    BEAR 예측  : %d개 박스 (chain)  day %d~%d  → lo=%.2f%%",
            len(chain_pred_rows), chain_pred_rows[0][8], chain_pred_rows[-1][9], chain_pred_rows[-1][11],
        )
    if _verbose:
        log.info(
            "    BEAR bottom: day=%d  lo=%.2f%%  (trend P(bear)=%.3f  P(bull)=%.3f)",
            bottom_day, bottom_lo,
            prob_bear_t if prob_bear_t is not None else 0.0,
            prob_bull_t if prob_bull_t is not None else 0.0,
        )
    chain_rows = [r for r in pred_rows if r[1] == str(last["symbol"]) and r[3] == max_cyc and r[7] == "PRED_BEAR_CHAIN"]
    if chain_rows and _verbose:
        log.info("    Bear 체인 박스 (%d개):", len(chain_rows))
        for cr in chain_rows:
            log.info("      chain box#%d  day %d~%d (%dd)  hi=%.2f%%  lo=%.2f%%", cr[5], cr[8], cr[9], cr[14], cr[10], cr[11])


def _log_coin_prediction_verbose(
    last,
    max_cyc,
    is_btc_coin,
    _verbose,
    pred_is_bull,
    prob_bull,
    prob_bear,
    bull_meta,
    chain_pred_rows,
    bottom_lo,
    bottom_day,
    prob_bear_t,
    prob_bull_t,
    pred_rows,
    sim_symbol,
    sim_cycle,
    sim_box,
    similarity,
):
    sep = "─" * 72
    _print_btc_prediction_box(
        last, max_cyc, is_btc_coin, pred_is_bull, prob_bull, prob_bear,
        bull_meta, chain_pred_rows, bottom_lo, bottom_day,
        sim_symbol, sim_cycle, sim_box, similarity,
    )
    if _verbose:
        log.info(sep)
        log.info(
            "  ▶ [%s] Cycle %d  |  last box: #%d %s  day %d~%d  hi=%.2f%%  lo=%.2f%%",
            last["symbol"], max_cyc, int(last["box_index"]), last["phase"],
            int(last["start_x"]), int(last["end_x"]), float(last["hi"]), float(last["lo"]),
        )
        log.info("    phase 확률: P(BULL)=%.3f  P(BEAR)=%.3f  → %s", prob_bull, prob_bear, "BULL" if pred_is_bull else "BEAR")
    if _verbose and pred_is_bull:
        _hi_log = min(float(bull_meta["bull_hi"]), MAX_PRED_HI - 0.01)
        log.info(
            "    BULL 예측  : box#%d  day %d~%d (%dd)  hi=%.2f%%  lo=%.2f%%  range=%.1f%%",
            bull_meta.get("next_box_idx", 0),
            bull_meta["bull_start"],
            bull_meta["bull_end"],
            bull_meta["pred_dur_bull"],
            _hi_log,
            bull_meta["bull_lo"],
            bull_meta["range_bull"],
        )
    _log_bear_chain_verbose(_verbose, chain_pred_rows, last, max_cyc, bottom_day, bottom_lo, prob_bear_t, prob_bull_t, pred_rows)
    if _verbose:
        log.info("    유사 패턴   : %s Cycle %d Box #%d  유사도=%.0f%%", sim_symbol, sim_cycle, sim_box, similarity * 100.0)
    if _verbose:
        log.info(sep)


def _predict_one_coin_phase1(coin_id, max_cyc, grp, last, df_all, train_df, models, bottom_models, peak_models, cycle_stats, coin_stats, phase_box_stats, btc_anchor, btc_cycle_max_hi=None, cross_median: float | None = None):
    _verbose = str(last["symbol"]).upper() in {"BTC", "ETH", "XRP"}
    feat, avg_cycle_days = _build_feature_vector(last, coin_id, max_cyc, cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi)
    X_pred = pd.DataFrame([feat])[FEATURE_COLS]
    group_key = "BTC" if str(last["symbol"]).upper() == "BTC" else "ALT"
    phase_models = models.get(group_key)
    if phase_models is None:
        phase_models = models.get("ALT")
    if phase_models is None or TARGET_PHASE not in phase_models:
        return None
    phase_proba = phase_models[TARGET_PHASE].predict_proba(X_pred)[0]
    prob_bear, prob_bull = float(phase_proba[0]), float(phase_proba[1])
    reg_key = group_key + ("_BEAR" if prob_bear >= prob_bull else "_BULL")
    reg_models = models.get(reg_key) or models.get("ALT_BEAR") or models.get("ALT_BULL")
    if reg_models is None or TARGET_HI not in reg_models:
        return None
    group_models = {
        TARGET_PHASE: phase_models[TARGET_PHASE],
        TARGET_HI: reg_models[TARGET_HI],
        TARGET_LO: reg_models[TARGET_LO],
        TARGET_DUR: reg_models[TARGET_DUR],
    }
    pred_norm_hi, pred_norm_lo, pred_norm_dur, prob_bear, prob_bull, pred_hi_bull, pred_lo_bull, pred_dur_bull = _get_model_predictions(group_models, X_pred, last, reg_key=reg_key)
    is_btc_coin = group_key == "BTC"
    bottom_lo, bottom_day, prob_bear_t, prob_bull_t = None, None, None, None
    if is_btc_coin:
        bottom_lo, bottom_day = _calc_bottom_btc(df_all, max_cyc, last)
        prob_bear_t, prob_bull_t = prob_bear, prob_bull
    else:
        bottom_lo, bottom_day, prob_bear_t, prob_bull_t = _calc_bottom_alt(bottom_models, group_key, X_pred, last)
        # ALT bottom 모델 미학습(샘플<30) 시 폴백: 현재 박스 기준으로 bear chain 진입 허용
        if bottom_lo is None and bottom_day is None and bottom_models.get(group_key) is None:
            ref_lo = float(last["lo"]) if last.get("lo") is not None and np.isfinite(last.get("lo")) else 50.0
            bottom_lo = min(max(ref_lo * 0.70, 5.0), MAX_PRED_LO)
            bottom_day = int(last["end_x"]) + max(MIN_BEAR_DURATION, 30)
            prob_bear_t, prob_bull_t = prob_bear, prob_bull
            log.info("  [%s] Bottom 모델 없음 → 폴백 bottom_lo=%.2f%% bottom_day=%d", last["symbol"], bottom_lo, bottom_day)
    pred_is_bull, *_ = _judge_bull_bear(last, grp, max_cyc, prob_bull, prob_bear, bottom_day, btc_anchor, bottom_lo=bottom_lo)
    peak_hi, peak_day_pred = None, None
    if is_btc_coin:
        peak_hi, peak_day_pred = _calc_peak_btc(df_all, max_cyc, last, coin_id, cross_median)
    else:
        # ALT 코인도 BTC와 동일한 하이브리드 감소율 기반 peak_hi 계산 사용
        peak_hi, peak_day_pred = _calc_peak_hybrid_for_coin(
            df_all, coin_id, max_cyc, last, cross_median, label=str(last["symbol"]).upper()
        )
    log.debug("    Peak 예측: hi=%.2f%%  day=%s", peak_hi if peak_hi else 0.0, str(peak_day_pred) if peak_day_pred else "-")
    peak_rows = list(_collect_peak_rows(coin_id, last, max_cyc, peak_hi, peak_day_pred, bottom_lo, bottom_day))
    start_x = int(last["end_x"]) + 1
    ref_lo = float(last["lo"]) if last["lo"] else 100.0
    cycle_lo = float(grp["lo"].min()) if not grp.empty else ref_lo
    next_box_idx = int(grp[grp["is_prediction"] == 0]["box_index"].max()) + 1
    return {"last": last, "coin_id": coin_id, "max_cyc": max_cyc, "grp": grp, "df_all": df_all, "train_df": train_df,
        "models": models, "bottom_models": bottom_models, "peak_models": peak_models,
        "cycle_stats": cycle_stats, "coin_stats": coin_stats, "phase_box_stats": phase_box_stats, "btc_anchor": btc_anchor,
        "feat": feat, "avg_cycle_days": avg_cycle_days, "X_pred": X_pred, "group_key": group_key, "group_models": group_models, "reg_key": reg_key,
        "pred_hi_bull": pred_hi_bull, "pred_lo_bull": pred_lo_bull, "pred_dur_bull": pred_dur_bull,
        "is_btc_coin": is_btc_coin, "bottom_lo": bottom_lo, "bottom_day": bottom_day, "prob_bear_t": prob_bear_t, "prob_bull_t": prob_bull_t,
        "pred_is_bull": pred_is_bull, "peak_hi": peak_hi, "peak_day_pred": peak_day_pred, "peak_rows": peak_rows,
        "start_x": start_x, "ref_lo": ref_lo, "cycle_lo": cycle_lo, "next_box_idx": next_box_idx,
        "_verbose": _verbose, "prob_bear": prob_bear, "prob_bull": prob_bull}


def _predict_one_coin_phase2(conn: sqlite3.Connection, bundle: dict):
    last, coin_id, max_cyc = bundle["last"], bundle["coin_id"], bundle["max_cyc"]
    feat, pred_hi_bull, pred_lo_bull, pred_dur_bull = bundle["feat"], bundle["pred_hi_bull"], bundle["pred_lo_bull"], bundle["pred_dur_bull"]
    pred_is_bull, bottom_lo, bottom_day = bundle["pred_is_bull"], bundle["bottom_lo"], bundle["bottom_day"]
    start_x, next_box_idx, ref_lo, cycle_lo = bundle["start_x"], bundle["next_box_idx"], bundle["ref_lo"], bundle["cycle_lo"]
    if int(last.get("is_completed", 1)) == 0:
        active_result = "BEAR_ACTIVE" if not pred_is_bull else "BULL_ACTIVE"
        conn.execute(
            """UPDATE coin_analysis_results SET result = ?
               WHERE coin_id = ? AND cycle_number = ? AND is_completed = 0 AND is_prediction = 0""",
            (active_result, coin_id, max_cyc),
        )
        conn.commit()
        log.info("  [%s] ACTIVE 박스 result 업데이트: %s", last["symbol"], active_result)
    pred_hi_bull, pred_lo_bull = _apply_btc_anchor_cap(last, bundle["btc_anchor"], pred_hi_bull, pred_lo_bull)
    # Bull 시나리오 1개 생성 (Bear chain의 cur_day/cur_val 계산용; Bear 있을 때는 나중에 연속 Bull로 대체)
    bull_row, bull_path_rows, bull_meta = _build_bull_scenario(
        coin_id, last, max_cyc, next_box_idx, start_x, ref_lo, cycle_lo,
        pred_hi_bull, pred_lo_bull, pred_dur_bull,
    )
    bull_meta["next_box_idx"] = next_box_idx
    pred_rows, path_rows = [], []
    pred_rows.append(bull_row)
    path_rows = list(bull_path_rows)
    if bottom_lo is not None and bottom_day is not None:
        _pre_bear_dur = max(bottom_day, start_x + 1) - start_x + 1
        if _pre_bear_dur < MIN_BEAR_DURATION:
            bottom_lo = None
    chain_pred_rows = []
    chain_path_rows = []
    if bottom_lo is not None and bottom_day is not None:
        # ACTIVE 박스(is_completed=0) hi/lo를 AI 계산 기준으로 사용
        active_rows = bundle["grp"][bundle["grp"]["is_completed"] == 0]
        active_hi = float(active_rows.iloc[-1]["hi"]) if not active_rows.empty else None
        active_lo = float(active_rows.iloc[-1]["lo"]) if not active_rows.empty else None
        chain_pred_rows, chain_path_rows = _build_bear_chain(
            coin_id=coin_id, last=last, max_cyc=max_cyc, next_box_idx=next_box_idx,
            bottom_day=bottom_day, bottom_lo=bottom_lo,
            cur_day=bull_meta["cur_day"], cur_val=bull_meta["cur_val"],
            feat=feat, avg_cycle_days=bundle["avg_cycle_days"], models=bundle["models"], group_key=bundle["group_key"],
            box_start_x=bundle["start_x"],
            active_box_hi=active_hi,
            active_box_lo=active_lo,
        )
        pred_rows.extend(chain_pred_rows)
        # Bear chain 종료점에서 Bull path 연결: 하락→최저점→반등 이 한 줄로 이어지도록
        if chain_path_rows:
            last_bear = chain_path_rows[-1]
            bear_end_day, bear_end_val = last_bear[6], last_bear[7]
            # Bear chain이 있으면 Bull은 최저점 다음날부터 시작. 연속 BULL 박스 생성
            peak_hi = bundle.get("peak_hi")
            peak_day_pred = bundle.get("peak_day_pred")
            if peak_hi is not None and peak_day_pred is not None and peak_day_pred > bottom_day:
                next_box_idx_after_bear = next_box_idx + len(chain_pred_rows)
                bull_chain_rows, bull_chain_path = _build_bull_chain(
                    coin_id, last, max_cyc, next_box_idx_after_bear,
                    bottom_day, bottom_lo, peak_day_pred, peak_hi,
                    pred_hi_bull, pred_lo_bull, pred_dur_bull, ref_lo, cycle_lo,
                )
                if bull_chain_rows:
                    pred_rows.pop(0)
                    pred_rows.extend(bull_chain_rows)
                    bull_meta["bull_start"] = bottom_day + 1
                    bull_meta["bull_end"] = peak_day_pred + max(1, pred_dur_bull // 2)
                    bull_meta["bull_hi"] = peak_hi
                    bull_meta["bull_lo"] = pred_lo_bull
                    bull_meta["range_bull"] = _safe_div_pct(peak_hi, pred_lo_bull) if pred_lo_bull and pred_lo_bull > 0 else 0.0
                    # Bear 체인 path 뒤에 바로 Bull 체인 path를 이어붙인다 (중간 bridge 포인트로 인한 스파이크 제거)
                    path_rows = list(chain_path_rows) + [r for r in bull_chain_path if r[6] >= bottom_day]
                else:
                    bull_meta["bull_start"] = bottom_day + 1
                    bull_meta["bull_end"] = bottom_day + pred_dur_bull
                    path_rows = list(chain_path_rows) + [
                        (coin_id, str(last["symbol"]), max_cyc, "bull", bull_meta["bull_start"], bull_meta["bull_end"], bear_end_day, bear_end_val)
                    ] + [r for r in bull_path_rows if r[6] > bear_end_day]
            else:
                # peak 없음: 단일 Bull 박스만 최저점 다음날부터
                pred_rows.pop(0)
                bull_row_after, _, _ = _build_bull_scenario(
                    coin_id, last, max_cyc, next_box_idx + len(chain_pred_rows), bottom_day + 1, ref_lo, cycle_lo,
                    pred_hi_bull, pred_lo_bull, pred_dur_bull,
                )
                pred_rows.append(bull_row_after)
                bull_meta["bull_start"] = bottom_day + 1
                bull_meta["bull_end"] = bull_row_after[9]
                bull_meta["bull_hi"] = bull_row_after[10]
                bull_meta["bull_lo"] = bull_row_after[11]
                bull_meta["range_bull"] = bull_row_after[17]
                peak_day_approx = bull_row_after[12]
                bull_path_from_bottom = _build_bull_path_rows(
                    coin_id, last, max_cyc, bottom_day, bottom_lo,
                    bottom_day + 1, bull_row_after[9], bull_row_after[10], bull_row_after[11], peak_day_approx,
                )
                bridge = (coin_id, str(last["symbol"]), max_cyc, "bull", bull_meta["bull_start"], bull_meta["bull_end"], bear_end_day, bear_end_val)
                path_rows = list(chain_path_rows) + [bridge] + [r for r in bull_path_from_bottom if r[6] > bear_end_day]
        else:
            path_rows = list(bull_path_rows)
    sim_symbol, sim_cycle, sim_box, similarity = _find_most_similar_pattern(bundle["train_df"], bundle["X_pred"])
    bundle["pred_rows"] = pred_rows
    bundle["path_rows"] = path_rows
    bundle["chain_pred_rows"] = chain_pred_rows
    bundle["bull_meta"] = bull_meta
    bundle["bottom_lo"] = bottom_lo
    bundle["sim_symbol"] = sim_symbol
    bundle["sim_cycle"] = sim_cycle
    bundle["sim_box"] = sim_box
    bundle["similarity"] = similarity


def _predict_one_coin(
    conn: sqlite3.Connection,
    coin_id: int,
    max_cyc: int,
    grp: pd.DataFrame,
    last: pd.Series,
    df_all: pd.DataFrame,
    train_df: pd.DataFrame,
    models: dict,
    bottom_models: dict,
    peak_models: dict,
    cycle_stats: dict,
    coin_stats: dict,
    phase_box_stats: dict,
    btc_anchor: dict | None,
    btc_cycle_max_hi: dict | None = None,
    cross_median: float | None = None,
):
    bundle = _predict_one_coin_phase1(
        coin_id, max_cyc, grp, last, df_all, train_df, models,
        bottom_models, peak_models, cycle_stats, coin_stats, phase_box_stats, btc_anchor,
        btc_cycle_max_hi=btc_cycle_max_hi,
        cross_median=cross_median,
    )
    if bundle is None:
        return [], [], [], True
    _predict_one_coin_phase2(conn, bundle)
    b = bundle
    _log_coin_prediction_verbose(
        b["last"], b["max_cyc"], b["is_btc_coin"], b["_verbose"],
        b["pred_is_bull"], b["prob_bull"], b["prob_bear"], b["bull_meta"],
        b["chain_pred_rows"], b["bottom_lo"], b["bottom_day"], b["prob_bear_t"], b["prob_bull_t"],
        b["pred_rows"], b["sim_symbol"], b["sim_cycle"], b["sim_box"], b["similarity"],
    )
    return b["pred_rows"], b["path_rows"], b["peak_rows"], False


def predict_and_insert(
    conn: sqlite3.Connection,
    df_all: pd.DataFrame,
    train_df: pd.DataFrame,
    models: dict,
    bottom_models: dict,
    peak_models: dict,
) -> int:
    deleted = conn.execute("DELETE FROM coin_analysis_results WHERE is_prediction = 1").rowcount
    conn.commit()
    log.info("기존 예측 %d건 삭제 후 재예측 시작", deleted)
    current_cycles = df_all.groupby("coin_id")["cycle_number"].max().reset_index().rename(columns={"cycle_number": "max_cycle"})
    cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi = build_cycle_and_coin_stats(df_all)
    btc_anchor = _calc_btc_anchor(df_all, cycle_stats, coin_stats)
    cross_median = _compute_cross_coin_peak_ratio(conn)
    pred_count = 0
    skip_count = 0
    pred_rows = []
    path_rows = []
    peak_rows = []
    for _, row in current_cycles.iterrows():
        coin_id = row["coin_id"]
        max_cyc = int(row["max_cycle"])

        grp = (
            df_all[(df_all["coin_id"] == coin_id) & (df_all["cycle_number"] == max_cyc)]
            .sort_values("box_index")
            .reset_index(drop=True)
        )
        if grp.empty:
            continue

        active = grp[grp["is_completed"] == 0]
        completed = grp[grp["is_completed"] == 1]
        # ACTIVE 박스 전 completed 박스를 last로 사용 (그 다음부터 예측)
        if not active.empty and not completed.empty:
            last = completed.iloc[-1]
        elif not active.empty:
            last = active.iloc[-1]
        else:
            last = grp.iloc[-1]

        prows, pathrows, peakrs, skipped = _predict_one_coin(
            conn, coin_id, max_cyc, grp, last, df_all, train_df, models,
            bottom_models, peak_models, cycle_stats, coin_stats, phase_box_stats, btc_anchor,
            btc_cycle_max_hi=btc_cycle_max_hi,
            cross_median=cross_median,
        )
        if skipped:
            skip_count += 1
            continue
        pred_rows.extend(prows)
        path_rows.extend(pathrows)
        peak_rows.extend(peakrs)
        pred_count += 1

    _insert_predictions_to_db(conn, pred_rows, path_rows, peak_rows, pred_count, skip_count)
    # 기존 체인 기반 path를 한 번 저장한 뒤, 새 보간 기반 알고리즘으로 전체 path를 재구성해 덮어쓴다.
    rebuild_prediction_paths(conn)
    return pred_count


def _insert_predictions_to_db(conn, pred_rows, path_rows, peak_rows, pred_count, skip_count):
    if pred_rows:
        conn.executemany(INSERT_SQL, pred_rows)
    if path_rows:
        conn.executemany(
            """
            INSERT INTO coin_prediction_paths
            (coin_id, symbol, cycle_number, scenario, start_x, end_x, day_x, value)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            path_rows,
        )
    if peak_rows:
        conn.execute(CREATE_PEAKS_SQL)
        conn.execute("DELETE FROM coin_prediction_peaks")
        conn.executemany(
            """
            INSERT INTO coin_prediction_peaks
            (coin_id, symbol, coin_rank, cycle_number, cycle_name,
             peak_type, predicted_value, predicted_day)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            peak_rows,
        )
    conn.commit()
    log.info("=" * 72)
    log.info("  예측 저장 완료: 코인 %d개  스킵 %d개  | pred_rows=%d  path_rows=%d", pred_count, skip_count, len(pred_rows), len(path_rows))
    log.info("=" * 72)


def _print_summary_table(df: pd.DataFrame):
    log.info("=" * 75)
    log.info("예측 결과 요약 (상위 20개)")
    log.info(
        "  %-6s  %-4s  %-5s  %5s  %8s  %8s  %6s  %6s  %6s",
        "Symbol",
        "Rank",
        "Phase",
        "Start",
        "End",
        "Dur(d)",
        "Hi(%)",
        "Lo(%)",
        "Range(%)",
    )
    log.info("  " + "-" * 70)
    _SUMMARY_SYMBOLS = {"BTC", "ETH", "XRP", "BNB", "SOL"}
    df_filtered = df[df["symbol"].str.upper().isin(_SUMMARY_SYMBOLS)]
    for _, r in df_filtered.iterrows():
        log.info(
            "  %-6s  #%3d  %-5s  %5d  %5d~%5d  %4dd  %6.1f  %6.1f  %5.1f%%",
            r["symbol"],
            r["coin_rank"],
            r["phase"],
            r["start_x"],
            r["start_x"],
            r["end_x"],
            r["duration"],
            r["hi"],
            r["lo"],
            r["range_pct"],
        )
    log.info("  ... 총 %d개 코인 예측 완료", len(df))
    log.info("=" * 75)


def _print_summary_stats(df: pd.DataFrame):
    log.info("예측 통계:")
    hi_series = df["hi"].astype(float).clip(upper=MAX_PRED_HI - 0.01)
    log.info(
        "  hi   mean=%.2f%%  std=%.2f%%  min=%.2f%%  max=%.2f%%",
        hi_series.mean(),
        hi_series.std(),
        hi_series.min(),
        hi_series.max(),
    )
    log.info(
        "  lo   mean=%.2f%%  std=%.2f%%  min=%.2f%%  max=%.2f%%",
        df["lo"].mean(),
        df["lo"].std(),
        df["lo"].min(),
        df["lo"].max(),
    )
    log.info(
        "  dur  mean=%.1fd  std=%.1fd  min=%dd  max=%dd",
        df["duration"].mean(),
        df["duration"].std(),
        int(df["duration"].min()),
        int(df["duration"].max()),
    )
    bull_cnt = (df["phase"] == "BULL").sum()
    bear_cnt = (df["phase"] == "BEAR").sum()
    log.info("  phase  BULL=%d개  BEAR=%d개", bull_cnt, bear_cnt)


def print_prediction_summary(conn: sqlite3.Connection):
    df = pd.read_sql_query(
        """
        SELECT symbol, coin_rank, cycle_name, phase,
               start_x, end_x, duration, hi, lo, range_pct
        FROM coin_analysis_results
        WHERE is_prediction = 1
        ORDER BY coin_rank
        """,
        conn,
    )
    if df.empty:
        log.warning("저장된 예측이 없습니다.")
        return

    _print_summary_table(df)
    _print_summary_stats(df)


def _interpolate_segment(start_val: float, end_val: float, start_day: int, end_day: int):
    """start_day~end_day 구간을 _ease_in_out으로 보간한 (day_x, value) 리스트 반환."""
    if end_day <= start_day:
        return [(int(start_day), float(start_val))]
    n = end_day - start_day
    pts: list[tuple[int, float]] = []
    for i in range(n + 1):
        t = i / n
        v = start_val + _ease_in_out(t) * (end_val - start_val)
        pts.append((int(start_day + i), float(v)))
    return pts


def _build_paths_for_cycle(rows, symbol: str, scenario: str, start_val: float | None = None):
    """
    rows: 해당 코인+사이클의 is_prediction=1 박스들
          (start_x, end_x, hi, lo, hi_day, lo_day, phase 등을 가진 dict/row 리스트)
    scenario: 'bear' 또는 'bull'
    반환값: (symbol, scenario, day_x, value) 튜플 리스트
    """
    if not rows:
        return []

    # start_x 기준 정렬
    rows = sorted(rows, key=lambda r: int(r["start_x"]))

    path: list[tuple[str, str, int, float]] = []

    # 시작값 설정
    if scenario == "bear":
        cur_val = float(rows[0]["hi"])
    else:
        cur_val = float(start_val if start_val is not None else rows[0]["hi"])

    for i, r in enumerate(rows):
        start_x = int(r["start_x"])
        end_x = int(r["end_x"])
        hi = float(r["hi"])
        lo = float(r["lo"])
        hi_day = int(r["hi_day"])
        lo_day = int(r["lo_day"])

        # 다음 박스 시작값 (없으면 자기 hi로 수렴)
        if i + 1 < len(rows):
            next_start_val = float(rows[i + 1]["hi"])
        else:
            next_start_val = float(hi)

        segs: list[tuple[float, float, int, int]] = []

        if scenario == "bear":
            # BEAR 박스
            if lo_day < hi_day:  # 먼저 하락 후 반등
                segs.append((cur_val, lo, start_x, lo_day))
                segs.append((lo, hi, lo_day, hi_day))
                segs.append((hi, next_start_val, hi_day, end_x))
            else:  # 먼저 반등 후 하락
                segs.append((cur_val, hi, start_x, hi_day))
                segs.append((hi, lo, hi_day, lo_day))
                segs.append((lo, next_start_val, lo_day, end_x))
        else:
            # BULL 박스
            if hi_day < lo_day:  # 먼저 상승 후 조정
                segs.append((cur_val, hi, start_x, hi_day))
                segs.append((hi, lo, hi_day, lo_day))
                segs.append((lo, next_start_val, lo_day, end_x))
            else:  # 먼저 조정 후 상승
                segs.append((cur_val, lo, start_x, lo_day))
                segs.append((lo, hi, lo_day, hi_day))
                segs.append((hi, next_start_val, hi_day, end_x))

        # 각 세그먼트를 보간하여 path에 추가
        for sv, ev, sd, ed in segs:
            seg_pts = _interpolate_segment(sv, ev, sd, ed)
            for day, val in seg_pts:
                path.append((symbol, scenario, int(day), float(val)))
            # 다음 박스 시작값은 현재 세그먼트의 마지막 값
            if seg_pts:
                _, last_v = seg_pts[-1]
                cur_val = float(last_v)

    # BULL 인 경우 마지막 포인트를 peak_hi(마지막 BULL 박스 hi)로 강제 보정
    if scenario == "bull" and path and rows:
        peak_hi = float(rows[-1]["hi"])
        sym, sc, day, _ = path[-1]
        path[-1] = (sym, sc, day, peak_hi)

    return path


def rebuild_prediction_paths(conn: sqlite3.Connection):
    """
    coin_analysis_results의 is_prediction=1 박스들만 사용해
    coin_prediction_paths 전체를 새 보간 알고리즘으로 재생성한다.
    coin_analysis_results 는 절대 수정하지 않는다.
    """
    cur = conn.cursor()

    # 기존 path 전체 삭제
    cur.execute("DELETE FROM coin_prediction_paths")

    # 코인별 is_prediction=1 박스 로드
    cur.execute(
        """
        SELECT
            coin_id,
            symbol,
            cycle_number,
            phase,
            start_x,
            end_x,
            hi,
            lo,
            hi_day,
            lo_day
        FROM coin_analysis_results
        WHERE is_prediction = 1
        ORDER BY symbol, cycle_number, start_x
        """
    )
    rows = cur.fetchall()

    # 심볼/사이클별 그룹핑
    by_symbol: dict[str, dict[int, list[dict]]] = {}
    for coin_id, sym, cyc, phase, sx, ex, hi, lo, hd, ld in rows:
        sym = str(sym)
        cyc = int(cyc)
        bucket = by_symbol.setdefault(sym, {}).setdefault(cyc, [])
        bucket.append(
            {
                "coin_id": coin_id,
                "symbol": sym,
                "cycle_number": cyc,
                "phase": str(phase),
                "start_x": int(sx),
                "end_x": int(ex),
                "hi": float(hi),
                "lo": float(lo),
                "hi_day": int(hd),
                "lo_day": int(ld),
            }
        )

    all_rows: list[tuple] = []

    for sym, cycles in by_symbol.items():
        for cyc, boxes in cycles.items():
            bears = [r for r in boxes if r["phase"] == "BEAR"]
            bulls = [r for r in boxes if r["phase"] == "BULL"]

            # BEAR path
            bear_path = _build_paths_for_cycle(bears, sym, "bear") if bears else []

            # BULL 시작값: BEAR path 마지막 값, 없으면 첫 BULL 박스 lo
            bull_start_val = None
            if bear_path:
                bull_start_val = float(bear_path[-1][3])
            elif bulls:
                bull_start_val = float(bulls[0]["lo"])

            # BULL path
            bull_path = []
            if bulls:
                bull_path = _build_paths_for_cycle(bulls, sym, "bull", start_val=bull_start_val)

            # coin_prediction_paths 스키마에 맞게 변환
            for scenario, path in (("bear", bear_path), ("bull", bull_path)):
                if not path:
                    continue
                day_values = [d for _, _, d, _ in path]
                start_x = min(day_values)
                end_x = max(day_values)
                coin_id = boxes[0]["coin_id"]
                for _, _, day, val in path:
                    all_rows.append(
                        (
                            coin_id,
                            sym,
                            cyc,
                            scenario,
                            start_x,
                            end_x,
                            int(day),
                            float(val),
                        )
                    )

    if all_rows:
        cur.executemany(
            """
            INSERT INTO coin_prediction_paths
              (coin_id, symbol, cycle_number, scenario, start_x, end_x, day_x, value)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            all_rows,
        )
    conn.commit()