import sqlite3

import numpy as np
import pandas as pd

from lib.common.config import FEATURE_COLS, TARGET_HI, TARGET_LO, TARGET_DUR, TARGET_PHASE
from lib.common.utils import _log1p, _signed_log1p


def load_box_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """coin_analysis_results 실측 데이터 전체 로드."""
    df = pd.read_sql_query(
        """
        SELECT *
        FROM coin_analysis_results
        WHERE is_prediction = 0
        ORDER BY coin_id, cycle_number, box_index
        """,
        conn,
    )
    df["is_bull"] = (df["phase"] == "BULL").astype(int)
    df["coin_rank"] = df["coin_rank"].fillna(999).astype(int)
    return df


def build_cycle_and_coin_stats(df: pd.DataFrame):
    cycle_stats = {}
    coin_cycle_rows = []

    for (coin_id, cycle_num), grp in df.groupby(["coin_id", "cycle_number"]):
        g = grp.sort_values("start_x")
        start = int(g["start_x"].min())
        end = int(g["end_x"].max())
        total_days = end - start + 1

        idx_min = g["lo"].idxmin()
        row_min = g.loc[idx_min]
        low_x = int(row_min["lo_day"] if pd.notna(row_min["lo_day"]) else row_min["end_x"])

        num_bull = int((g["phase"] == "BULL").sum())
        num_bear = int((g["phase"] == "BEAR").sum())

        cycle_stats[(coin_id, cycle_num)] = dict(
            start_x=start,
            end_x=end,
            total_days=total_days,
            low_x=low_x,
            num_bull=num_bull,
            num_bear=num_bear,
            min_lo=float(g["lo"].min()),
        )

        coin_cycle_rows.append(
            {
                "coin_id": coin_id,
                "cycle_number": cycle_num,
                "total_days": total_days,
                "low_x": low_x,
                "min_lo": float(g["lo"].min()),
                "mean_lo": float(g["lo"].mean()),
                "num_bull": num_bull,
                "num_bear": num_bear,
            }
        )

    cycles_df = pd.DataFrame(coin_cycle_rows)

    coin_stats = {}
    for coin_id, g in cycles_df.groupby("coin_id"):
        coin_stats[coin_id] = dict(
            avg_cycle_days=float(g["total_days"].mean()),
            avg_low_x_ratio=float((g["low_x"] / g["total_days"]).mean()),
            mean_lo=float(g["mean_lo"].mean()),
            min_lo=float(g["min_lo"].min()),
        )

    phase_counts = df.groupby(["coin_id", "cycle_number", "phase"])["box_index"].max().reset_index()
    phase_counts["box_count"] = phase_counts["box_index"] + 1

    phase_box_stats = {}
    for (coin_id, phase), g in phase_counts.groupby(["coin_id", "phase"]):
        phase_box_stats[(coin_id, phase)] = float(g["box_count"].mean())

    btc_cycle_max_hi = {}
    btc_df = df[df["symbol"].str.upper() == "BTC"]
    if not btc_df.empty:
        for cycle_num, grp in btc_df.groupby("cycle_number"):
            btc_cycle_max_hi[int(cycle_num)] = float(grp["hi"].max())

    return cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi


def build_training_pairs(df: pd.DataFrame) -> pd.DataFrame:
    cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi = build_cycle_and_coin_stats(df)

    records = []
    for (coin_id, cycle_num), grp in df.groupby(["coin_id", "cycle_number"]):
        grp = grp.sort_values("box_index").reset_index(drop=True)
        for i in range(len(grp) - 1):
            curr = grp.iloc[i]
            nxt = grp.iloc[i + 1]
            if curr["is_completed"] != 1:
                continue

            cstat = cycle_stats[(coin_id, cycle_num)]
            cinfo = coin_stats.get(coin_id, {})

            total_days = cstat["total_days"]
            cycle_low_x = cstat["low_x"]
            cycle_low_pos_ratio = cycle_low_x / total_days if total_days else 0.0

            avg_cycle_days = cinfo.get("avg_cycle_days", total_days)
            cycle_progress_ratio = curr["end_x"] / avg_cycle_days if avg_cycle_days else 0.0

            mean_lo_prev = cinfo.get("mean_lo", curr["lo"])
            min_lo_prev = cinfo.get("min_lo", curr["lo"])
            rel_to_prev_cycle_low = (curr["lo"] - min_lo_prev) / abs(min_lo_prev) if min_lo_prev else 0.0
            rel_to_prev_support_mean = (curr["lo"] - mean_lo_prev) / abs(mean_lo_prev) if mean_lo_prev else 0.0

            phase_label = "BULL" if int(curr["is_bull"]) == 1 else "BEAR"
            avg_box_cnt = phase_box_stats.get((coin_id, phase_label), curr["box_index"] + 1)
            phase_box_index_ratio = (curr["box_index"] + 1) / avg_box_cnt if avg_box_cnt else 0.0

            btc_prev_peak_ratio = 0.0
            if str(curr["symbol"]).upper() == "BTC" and cycle_num > 1 and (cycle_num - 1) in btc_cycle_max_hi:
                prev_hi = btc_cycle_max_hi[cycle_num - 1]
                if prev_hi and prev_hi > 0:
                    btc_prev_peak_ratio = float(curr["hi"]) / prev_hi
            log_cycle_number = float(np.log(cycle_num + 1))

            cycle_min_lo = cstat.get("min_lo") or float(grp["lo"].min())
            hi_ratio = curr["hi"] / cycle_min_lo if cycle_min_lo > 0 and curr["hi"] > 0 else 1.0
            lo_ratio = curr["lo"] / cycle_min_lo if cycle_min_lo > 0 and curr["lo"] > 0 else 1.0
            hi_rel_to_cycle_lo = float(np.log(hi_ratio))
            lo_rel_to_cycle_lo = float(np.log(lo_ratio))

            records.append(
                {
                    "norm_range_pct": curr["norm_range_pct"],
                    "norm_hi_change_pct": curr["norm_hi_change_pct"],
                    "norm_lo_change_pct": curr["norm_lo_change_pct"],
                    "norm_gain_pct": curr["norm_gain_pct"],
                    "norm_duration": curr["norm_duration"],
                    "hi_rel_to_cycle_lo": hi_rel_to_cycle_lo,
                    "lo_rel_to_cycle_lo": lo_rel_to_cycle_lo,
                    "coin_rank": int(curr["coin_rank"]),
                    "is_bull": int(curr["is_bull"]),
                    "box_index": int(curr["box_index"]),
                    "cycle_progress_ratio": float(cycle_progress_ratio),
                    "cycle_low_pos_ratio": float(cycle_low_pos_ratio),
                    "rel_to_prev_cycle_low": float(rel_to_prev_cycle_low),
                    "rel_to_prev_support_mean": float(rel_to_prev_support_mean),
                    "phase_box_index_ratio": float(phase_box_index_ratio),
                    "phase_avg_box_count": float(avg_box_cnt),
                    "btc_prev_peak_ratio": float(btc_prev_peak_ratio),
                    "log_cycle_number": log_cycle_number,
                    TARGET_HI: float(nxt["norm_hi_change_pct"]),
                    TARGET_LO: float(nxt["norm_lo_change_pct"]),
                    TARGET_DUR: nxt["norm_duration"],
                    TARGET_PHASE: int(nxt["is_bull"]),
                    "meta_coin_id": coin_id,
                    "meta_symbol": curr["symbol"],
                    "meta_cycle": int(cycle_num),
                    "meta_box_index": int(curr["box_index"]),
                }
            )
    return pd.DataFrame(records)


def build_bottom_dataset(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi = build_cycle_and_coin_stats(df)
    last_cycle = df.groupby("coin_id")["cycle_number"].max()

    for (coin_id, cycle_num), grp in df.groupby(["coin_id", "cycle_number"]):
        if cycle_num == last_cycle[coin_id]:
            continue

        grp = grp.sort_values("box_index")
        idx_min = grp["lo"].idxmin()
        row_min = grp.loc[idx_min]
        bottom_lo = float(row_min["lo"])
        bottom_day = int(row_min["lo_day"] if pd.notna(row_min["lo_day"]) else row_min["end_x"])

        completed = grp[grp["is_completed"] == 1]
        if completed.empty:
            continue
        ref = completed.iloc[-1]

        cstat = cycle_stats[(coin_id, cycle_num)]
        cinfo = coin_stats.get(coin_id, {})

        total_days = cstat["total_days"]
        cycle_low_x = cstat["low_x"]
        cycle_low_pos_ratio = cycle_low_x / total_days if total_days else 0.0

        avg_cycle_days = cinfo.get("avg_cycle_days", total_days)
        cycle_progress_ratio = ref["end_x"] / avg_cycle_days if avg_cycle_days else 0.0

        mean_lo_prev = cinfo.get("mean_lo", ref["lo"])
        min_lo_prev = cinfo.get("min_lo", ref["lo"])
        rel_to_prev_cycle_low = (ref["lo"] - min_lo_prev) / abs(min_lo_prev) if min_lo_prev else 0.0
        rel_to_prev_support_mean = (ref["lo"] - mean_lo_prev) / abs(mean_lo_prev) if mean_lo_prev else 0.0

        phase_label = "BULL" if int(ref["is_bull"]) == 1 else "BEAR"
        avg_box_cnt = phase_box_stats.get((coin_id, phase_label), ref["box_index"] + 1)
        phase_box_index_ratio = (ref["box_index"] + 1) / avg_box_cnt if avg_box_cnt else 0.0

        btc_prev_peak_ratio = 0.0
        if str(ref["symbol"]).upper() == "BTC" and cycle_num > 1 and (cycle_num - 1) in btc_cycle_max_hi:
            prev_hi = btc_cycle_max_hi[cycle_num - 1]
            if prev_hi and prev_hi > 0 and ref.get("hi"):
                btc_prev_peak_ratio = float(ref["hi"]) / prev_hi
        log_cycle_number = float(np.log(cycle_num + 1))

        cycle_min_lo = cstat.get("min_lo") or float(grp["lo"].min())
        hi_ratio = ref["hi"] / cycle_min_lo if cycle_min_lo > 0 and ref["hi"] > 0 else 1.0
        lo_ratio = ref["lo"] / cycle_min_lo if cycle_min_lo > 0 and ref["lo"] > 0 else 1.0
        hi_rel_to_cycle_lo = float(np.log(hi_ratio))
        lo_rel_to_cycle_lo = float(np.log(lo_ratio))

        row = {
            "norm_range_pct": ref["norm_range_pct"],
            "norm_hi_change_pct": ref["norm_hi_change_pct"],
            "norm_lo_change_pct": ref["norm_lo_change_pct"],
            "norm_gain_pct": ref["norm_gain_pct"],
            "norm_duration": ref["norm_duration"],
            "hi_rel_to_cycle_lo": hi_rel_to_cycle_lo,
            "lo_rel_to_cycle_lo": lo_rel_to_cycle_lo,
            "coin_rank": int(ref["coin_rank"]),
            "is_bull": int(ref["is_bull"]),
            "box_index": int(ref["box_index"]),
            "cycle_progress_ratio": float(cycle_progress_ratio),
            "cycle_low_pos_ratio": float(cycle_low_pos_ratio),
            "rel_to_prev_cycle_low": float(rel_to_prev_cycle_low),
            "rel_to_prev_support_mean": float(rel_to_prev_support_mean),
            "phase_box_index_ratio": float(phase_box_index_ratio),
            "phase_avg_box_count": float(avg_box_cnt),
            "btc_prev_peak_ratio": float(btc_prev_peak_ratio),
            "log_cycle_number": log_cycle_number,
        }
        row["symbol"] = ref["symbol"]
        row["coin_id"] = coin_id
        row["bottom_norm_lo"] = _log1p(bottom_lo)
        row["bottom_day"] = bottom_day
        row["trend_label"] = int(ref["is_bull"])
        rows.append(row)

    return pd.DataFrame(rows)


def build_peak_dataset(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    cycle_stats, coin_stats, phase_box_stats, btc_cycle_max_hi = build_cycle_and_coin_stats(df)
    last_cycle = df.groupby("coin_id")["cycle_number"].max()

    for (coin_id, cycle_num), grp in df.groupby(["coin_id", "cycle_number"]):
        if cycle_num == last_cycle[coin_id]:
            continue

        grp = grp.sort_values("box_index")
        bull_grp = grp[grp["phase"] == "BULL"]
        if bull_grp.empty:
            continue
        idx_max = bull_grp["hi"].idxmax()
        row_max = bull_grp.loc[idx_max]
        peak_hi = float(row_max["hi"])
        peak_day = int(row_max["hi_day"] if pd.notna(row_max["hi_day"]) else row_max["end_x"])

        completed = grp[grp["is_completed"] == 1]
        if completed.empty:
            continue
        ref = completed.iloc[-1]

        cstat = cycle_stats[(coin_id, cycle_num)]
        cinfo = coin_stats.get(coin_id, {})
        total_days = cstat["total_days"]
        cycle_low_x = cstat["low_x"]
        cycle_low_pos_ratio = cycle_low_x / total_days if total_days else 0.0
        avg_cycle_days = cinfo.get("avg_cycle_days", total_days)
        cycle_progress_ratio = ref["end_x"] / avg_cycle_days if avg_cycle_days else 0.0
        mean_lo_prev = cinfo.get("mean_lo", ref["lo"])
        min_lo_prev = cinfo.get("min_lo", ref["lo"])
        rel_to_prev_cycle_low = (ref["lo"] - min_lo_prev) / abs(min_lo_prev) if min_lo_prev else 0.0
        rel_to_prev_support_mean = (ref["lo"] - mean_lo_prev) / abs(mean_lo_prev) if mean_lo_prev else 0.0
        phase_label = "BULL" if int(ref["is_bull"]) == 1 else "BEAR"
        avg_box_cnt = phase_box_stats.get((coin_id, phase_label), ref["box_index"] + 1)
        phase_box_index_ratio = (ref["box_index"] + 1) / avg_box_cnt if avg_box_cnt else 0.0

        btc_prev_peak_ratio = 0.0
        if str(ref["symbol"]).upper() == "BTC" and cycle_num > 1 and (cycle_num - 1) in btc_cycle_max_hi:
            prev_hi = btc_cycle_max_hi[cycle_num - 1]
            if prev_hi and prev_hi > 0 and ref.get("hi"):
                btc_prev_peak_ratio = float(ref["hi"]) / prev_hi
        log_cycle_number = float(np.log(cycle_num + 1))

        cycle_min_lo = cstat.get("min_lo") or float(grp["lo"].min())
        hi_ratio = ref["hi"] / cycle_min_lo if cycle_min_lo > 0 and ref["hi"] > 0 else 1.0
        lo_ratio = ref["lo"] / cycle_min_lo if cycle_min_lo > 0 and ref["lo"] > 0 else 1.0
        hi_rel_to_cycle_lo = float(np.log(hi_ratio))
        lo_rel_to_cycle_lo = float(np.log(lo_ratio))

        row = {
            "norm_range_pct": ref["norm_range_pct"],
            "norm_hi_change_pct": ref["norm_hi_change_pct"],
            "norm_lo_change_pct": ref["norm_lo_change_pct"],
            "norm_gain_pct": ref["norm_gain_pct"],
            "norm_duration": ref["norm_duration"],
            "hi_rel_to_cycle_lo": hi_rel_to_cycle_lo,
            "lo_rel_to_cycle_lo": lo_rel_to_cycle_lo,
            "coin_rank": int(ref["coin_rank"]),
            "is_bull": int(ref["is_bull"]),
            "box_index": int(ref["box_index"]),
            "cycle_progress_ratio": float(cycle_progress_ratio),
            "cycle_low_pos_ratio": float(cycle_low_pos_ratio),
            "rel_to_prev_cycle_low": float(rel_to_prev_cycle_low),
            "rel_to_prev_support_mean": float(rel_to_prev_support_mean),
            "phase_box_index_ratio": float(phase_box_index_ratio),
            "phase_avg_box_count": float(avg_box_cnt),
            "btc_prev_peak_ratio": float(btc_prev_peak_ratio),
            "log_cycle_number": log_cycle_number,
            "symbol": ref["symbol"],
            "coin_id": coin_id,
            "peak_norm_hi": _log1p(peak_hi),
            "peak_day": peak_day,
            "trend_label": int(ref["is_bull"]),
        }
        rows.append(row)

    return pd.DataFrame(rows)
