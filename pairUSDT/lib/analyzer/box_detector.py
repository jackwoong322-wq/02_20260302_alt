import logging

from lib.common.config import (
    MIN_BOX_DAYS,
    BEAR_BREAKOUT_RATIO,
    BULL_BREAKOUT_RATIO,
    BEAR_REBOUND_RATIO,
    BULL_DRAWDOWN_RATIO,
    BULL_PEAK_LOOKAHEAD,
)
from lib.common.utils import safe_range_pct

log = logging.getLogger(__name__)


def _find_cycle_phases(data: list):
    cycle_min_low = float("inf")
    cycle_min_idx = 0
    for idx, d in enumerate(data):
        if d["low"] < cycle_min_low:
            cycle_min_low = d["low"]
            cycle_min_idx = idx
    phase1 = data[: cycle_min_idx + 1]
    phase2 = data[cycle_min_idx:]
    log.debug(
        "  phase1=%d일 day%s~%s | phase2=%d일 day%s~%s | cycleLow idx=%d",
        len(phase1),
        phase1[0]["x"] if phase1 else "?",
        phase1[-1]["x"] if phase1 else "?",
        len(phase2),
        phase2[0]["x"] if phase2 else "?",
        phase2[-1]["x"] if phase2 else "?",
        cycle_min_idx,
    )
    return phase1, phase2, cycle_min_idx


def _bull_box_hi_lo(ph2: list, bs: int, be: int):
    lo_idx = bs
    for k in range(bs + 1, be + 1):
        if ph2[k]["low"] < ph2[lo_idx]["low"]:
            lo_idx = k
    lo_day_x = ph2[lo_idx]["x"]
    blo = ph2[lo_idx]["low"]
    hi_idx = bs
    for k in range(bs, be + 1):
        if ph2[k]["high"] > ph2[hi_idx]["high"]:
            hi_idx = k
    hi_day_x = ph2[hi_idx]["x"]
    bhi = ph2[hi_idx]["high"]
    return bhi, blo, hi_day_x, lo_day_x


def _make_bear_zone(phase1, box_start, end_idx, box_hi, box_lo, lo_day_x, hi_day_x, cycle_min_idx, result):
    duration = phase1[end_idx]["x"] - phase1[box_start]["x"] + 1
    if duration < MIN_BOX_DAYS:
        return None
    return {
        "start_x": phase1[box_start]["x"],
        "end_x": phase1[end_idx]["x"],
        "hi": box_hi,
        "lo": box_lo,
        "lo_day": lo_day_x,
        "hi_day": hi_day_x,
        "duration": duration,
        "range_pct": safe_range_pct(box_hi, box_lo),
        "phase": "BEAR",
        "result": result,
        "cycle_min_idx": cycle_min_idx,
    }


def _detect_bear_boxes(phase1: list, cycle_min_idx: int) -> list:
    zones = []
    i = 0
    while i < len(phase1) - 1:
        trough_idx = -1
        for k in range(i, len(phase1)):
            cand_low = phase1[k]["low"]
            confirm_end = min(len(phase1) - 1, k + 3)
            broken3 = any(phase1[m]["low"] < cand_low for m in range(k + 1, confirm_end + 1))
            if not broken3:
                trough_idx = k
                break
        if trough_idx == -1:
            break
        base_low = phase1[trough_idx]["low"]
        rebound_idx = -1
        for k in range(trough_idx + 1, len(phase1)):
            if phase1[k]["high"] >= base_low * BEAR_REBOUND_RATIO:
                rebound_idx = k
                break
        if rebound_idx == -1:
            i = trough_idx + 1
            continue
        box_hi = max(phase1[k]["high"] for k in range(trough_idx, rebound_idx + 1))
        box_lo = min(phase1[k]["low"] for k in range(trough_idx, rebound_idx + 1))
        lo_day_x = phase1[trough_idx]["x"]
        hi_day_x = phase1[rebound_idx]["x"]
        box_start = trough_idx
        box_end = rebound_idx
        broken = False
        for j in range(rebound_idx + 1, len(phase1)):
            if phase1[j]["close"] < box_lo * BEAR_BREAKOUT_RATIO:
                z = _make_bear_zone(phase1, box_start, j - 1, box_hi, box_lo, lo_day_x, hi_day_x, cycle_min_idx, "DOWN")
                if z:
                    zones.append(z)
                i = j
                broken = True
                break
            box_hi = max(box_hi, phase1[j]["high"])
            box_lo = min(box_lo, phase1[j]["low"])
            box_end = j
        if not broken:
            z = _make_bear_zone(phase1, box_start, box_end, box_hi, box_lo, lo_day_x, hi_day_x, cycle_min_idx, "BOTTOM")
            if z:
                zones.append(z)
            break
    return zones


def _make_bull_zone(phase2, box_start, end_idx, bhi, blo, hd, ld, cycle_min_idx, result):
    duration = phase2[end_idx]["x"] - phase2[box_start]["x"] + 1
    if duration < MIN_BOX_DAYS:
        return None
    return {
        "start_x": phase2[box_start]["x"],
        "end_x": phase2[end_idx]["x"],
        "hi": bhi,
        "lo": blo,
        "hi_day": hd,
        "lo_day": ld,
        "duration": duration,
        "range_pct": safe_range_pct(bhi, blo),
        "phase": "BULL",
        "result": result,
        "cycle_min_idx": cycle_min_idx,
    }


def _detect_bull_boxes(phase2: list, cycle_min_idx: int, is_last_cycle: bool) -> list:
    zones = []
    i = 0
    for _guard in range(300):
        if i >= len(phase2) - 1:
            break
        peak_idx = -1
        for k in range(i, len(phase2) - 1):
            hi_end = min(len(phase2) - 1, k + BULL_PEAK_LOOKAHEAD)
            is_peak = all(phase2[m]["high"] < phase2[k]["high"] for m in range(k + 1, hi_end + 1))
            if is_peak:
                peak_idx = k
                break
        if peak_idx == -1:
            break
        peak_hi = phase2[peak_idx]["high"]
        adj_idx = -1
        for k in range(peak_idx + 1, len(phase2)):
            if phase2[k]["low"] <= peak_hi * BULL_DRAWDOWN_RATIO:
                adj_idx = k
                break
        if adj_idx == -1:
            break
        box_start = peak_idx
        box_end = adj_idx
        broken = False
        breakout_threshold = peak_hi * BULL_BREAKOUT_RATIO
        for j in range(adj_idx + 1, len(phase2)):
            if phase2[j]["close"] > breakout_threshold:
                end_idx = j - 1
                bhi, blo, hd, ld = _bull_box_hi_lo(phase2, box_start, end_idx)
                z = _make_bull_zone(phase2, box_start, end_idx, bhi, blo, hd, ld, cycle_min_idx, "UP")
                if z:
                    zones.append(z)
                i = j
                broken = True
                break
            box_end = j
        if not broken:
            bhi, blo, hd, ld = _bull_box_hi_lo(phase2, box_start, box_end)
            z = _make_bull_zone(phase2, box_start, box_end, bhi, blo, hd, ld, cycle_min_idx, "ACTIVE" if is_last_cycle else "UP")
            if z:
                zones.append(z)
            break
    return zones


def detect_box_zones(data: list, is_last_cycle: bool = False) -> list:
    if not data or len(data) < 2:
        return []
    phase1, phase2, cycle_min_idx = _find_cycle_phases(data)
    bear_zones = _detect_bear_boxes(phase1, cycle_min_idx)
    bull_zones = _detect_bull_boxes(phase2, cycle_min_idx, is_last_cycle)
    return bear_zones + bull_zones


def detect_bear_bull(data: list) -> list:
    if not data:
        return []

    min_val = float("inf")
    min_idx = 0
    for i, d in enumerate(data):
        if d["close"] < min_val:
            min_val = d["close"]
            min_idx = i

    bottom_day = data[min_idx]["x"]
    start_day = data[0]["x"]
    end_day = data[-1]["x"]
    segments = []

    if min_idx > 5:
        segments.append(
            {
                "type": "BEAR",
                "start_x": start_day,
                "end_x": bottom_day,
                "start_val": data[0]["close"],
                "end_val": min_val,
                "pct": (min_val - data[0]["close"]) / data[0]["close"] * 100 if data[0]["close"] else 0.0,
                "days": bottom_day - start_day,
            }
        )

    if min_idx < len(data) - 5:
        last_val = data[-1]["close"]
        segments.append(
            {
                "type": "BULL",
                "start_x": bottom_day,
                "end_x": end_day,
                "start_val": min_val,
                "end_val": last_val,
                "pct": (last_val - min_val) / min_val * 100 if min_val else 0.0,
                "days": end_day - bottom_day,
            }
        )

    return segments