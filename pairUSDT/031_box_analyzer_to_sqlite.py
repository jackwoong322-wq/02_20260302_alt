"""031_analyzer_to_sqlite.py

박스권 분석 및 AI 학습용 SQLite DB 구축

Usage: python 031_analyzer_to_sqlite.py
"""

import logging
import sqlite3
from datetime import datetime

import numpy as np

from lib.common.config import DB_PATH
from lib.common.utils import signed_log1p
from lib.analyzer.box_detector import detect_box_zones, detect_bear_bull
from lib.analyzer.finalizer import finalize_hi_lo_days, compute_change_pcts
from lib.analyzer.db import (
    setup_db,
    insert_zones,
    load_all_coins,
    load_cycle_data,
    print_norm_stats,
)

log = logging.getLogger(__name__)


def main():
    conn = sqlite3.connect(DB_PATH)
    setup_db(conn)

    deleted = conn.execute("DELETE FROM coin_analysis_results WHERE is_prediction = 0").rowcount
    conn.commit()
    log.info("기존 분석 데이터 %d건 삭제 후 재분석 시작", deleted)

    coins = load_all_coins(conn)
    if not coins:
        log.error("코인 데이터 없음. alt_cycle_data 테이블을 먼저 채워주세요.")
        conn.close()
        return

    log.info("분석 대상 코인: %d개", len(coins))

    all_norm_range: list = []
    all_norm_hi_chg: list = []
    all_norm_lo_chg: list = []
    all_norm_gain: list = []
    all_norm_duration: list = []

    total_zones = 0

    for coin_id, symbol, name, rank in coins:
        cycles = load_cycle_data(conn, coin_id)
        coin_total = 0

        for cn, cycle in cycles.items():
            data = cycle["data"]
            if len(data) < 2:
                continue

            last_cycle_num = max(cycles.keys())
            zones = detect_box_zones(data, is_last_cycle=(cn == last_cycle_num))
            if not zones:
                log.debug("  [%s] Cycle %d → 박스권 없음", symbol, cn)
                continue

            zones = finalize_hi_lo_days(zones, data)

            # 과거 사이클 마지막 BULL 박스 hi 보정 (compute_change_pcts 전에 실행해야 gain_pct 등에 반영)
            if cn < last_cycle_num:
                next_cycle = cycles.get(cn + 1)
                if next_cycle:
                    this_peak = cycle.get("peak_price")
                    next_peak = next_cycle.get("peak_price")
                    if this_peak and next_peak and this_peak > 0:
                        bull_zones_corr = [z for z in zones if z["phase"] == "BULL"]
                        if bull_zones_corr:
                            last_bull = bull_zones_corr[-1]
                            corrected_hi = next_peak / this_peak * 100
                            last_bull["hi"] = corrected_hi
                            last_bull["hi_day"] = last_bull["end_x"]
                            last_bull["range_pct"] = (
                                abs(corrected_hi - last_bull["lo"]) / last_bull["lo"] * 100
                                if last_bull["lo"] > 0 else 0.0
                            )

            zones = compute_change_pcts(zones, data)

            bear_cnt = sum(1 for z in zones if z["phase"] == "BEAR")
            bull_cnt = sum(1 for z in zones if z["phase"] == "BULL")
            if symbol.upper() == "BTC":
                log.info(
                    "  [%s] %s → BEAR %d개 / BULL %d개 (총 %d개)",
                    symbol,
                    cycle["cycle_name"],
                    bear_cnt,
                    bull_cnt,
                    len(zones),
                )
                print(f"  ┌─────────────────────────────────────────")
                print(f"  │ BEAR:")
                for zi, z in enumerate(zones):
                    if z["phase"] == "BEAR":
                        print(f"  │   #{zi} day {z['start_x']:4d}~{z['end_x']:4d}  hi={z['hi']:7.2f}%  lo={z['lo']:7.2f}%  gain={z.get('gain_pct', 0.0) or 0.0:8.2f}%  result={z['result']}")
                print(f"  │ BULL:")
                for zi, z in enumerate(zones):
                    if z["phase"] == "BULL":
                        print(f"  │   #{zi} day {z['start_x']:4d}~{z['end_x']:4d}  hi={z['hi']:7.2f}%  lo={z['lo']:7.2f}%  gain={z.get('gain_pct', 0.0) or 0.0:8.2f}%  result={z['result']}")
                bull_zones = [z for z in zones if z["phase"] == "BULL"]
                cycle_min_idx_disp = zones[0].get("cycle_min_idx", 0)
                cycle_lo = data[cycle_min_idx_disp]["low"]
                cycle_lo_day = data[cycle_min_idx_disp]["x"]
                if bull_zones:
                    max_bull = max(bull_zones, key=lambda z: z["hi"])
                    cycle_hi = max_bull["hi"]
                    cycle_hi_day = max_bull.get("hi_day", max_bull["end_x"])
                    max_gain_pct = max(z.get("gain_pct", 0.0) or 0.0 for z in bull_zones)
                else:
                    cycle_hi = 0.0
                    cycle_hi_day = 0
                    max_gain_pct = 0.0
                print(f"  ├─────────────────────────────────────────")
                print(f"  │ 사이클 저점 : lo={cycle_lo:7.2f}%  day={cycle_lo_day}")
                print(f"  │ 사이클 고점 : hi={cycle_hi:7.2f}%  day={cycle_hi_day}")
                print(f"  │ gain_pct 최대: {max_gain_pct:.2f}%")
                print(f"  └─────────────────────────────────────────")
                if cn < last_cycle_num:
                    for zi, z in enumerate(zones):
                        if z["result"] == "ACTIVE":
                            log.warning("  [BTC] %s에 ACTIVE 박스 존재 → box#%d (과거 사이클인데 ACTIVE?)", cycle["cycle_name"], zi)

            for zi, z in enumerate(zones):
                rp = z["range_pct"]
                hcp = z.get("hi_change_pct", 0.0) or 0.0
                lcp = z.get("lo_change_pct", 0.0) or 0.0
                gp = z.get("gain_pct", 0.0) or 0.0
                dur = z["duration"]

                log.debug(
                    "    Box #%d [%s] day %d~%d (%dd) "
                    "hi=%.2f%% lo=%.2f%% range=%.1f%% "
                    "hi_chg=%.1f%%(norm=%.3f) "
                    "gain=%.1f%%(norm=%.3f) "
                    "result=%s",
                    zi + 1,
                    z["phase"],
                    z["start_x"],
                    z["end_x"],
                    dur,
                    z["hi"],
                    z["lo"],
                    rp,
                    hcp,
                    signed_log1p(hcp) or 0,
                    gp,
                    signed_log1p(gp) or 0,
                    z["result"],
                )

                all_norm_range.append(float(np.log1p(rp)))  # range_pct 항상 양수; sqrt 대비 log1p가 std 더 안정적(검증됨)
                all_norm_duration.append(float(np.log1p(dur)))
                if hcp is not None:
                    all_norm_hi_chg.append(float(np.sign(hcp) * np.log1p(abs(hcp))))
                if lcp is not None:
                    all_norm_lo_chg.append(float(np.sign(lcp) * np.log1p(abs(lcp))))
                if gp is not None:
                    all_norm_gain.append(float(np.sign(gp) * np.log1p(abs(gp))))

            inserted = insert_zones(conn, coin_id, symbol, rank, cn, cycle["cycle_name"], zones)
            coin_total += inserted

        if symbol.upper() == "BTC":
            log.info("  [BTC] 저장 완료: %d개 박스", coin_total)
        total_zones += coin_total

    # ── Current 사이클 ACTIVE 재태깅 ─────────────────────────────────────────────
    # cycle_name LIKE '%Current%' 인 사이클의 마지막 박스(box_index 최대값)만 ACTIVE (is_completed=0)
    # 나머지 박스는 기존 result 유지 + is_completed=1
    cur_cycles = conn.execute(
        """
        SELECT DISTINCT coin_id, cycle_number
        FROM coin_analysis_results
        WHERE is_prediction = 0
          AND cycle_name LIKE '%Current%'
        """
    ).fetchall()
    for cid, cyc in cur_cycles:
        # 해당 Current 사이클의 모든 박스를 completed로 설정
        conn.execute(
            """
            UPDATE coin_analysis_results
            SET is_completed = 1
            WHERE is_prediction = 0
              AND coin_id = ?
              AND cycle_number = ?
            """,
            (cid, cyc),
        )
        # 마지막 박스(box_index 최대값)를 ACTIVE로 재태깅
        row = conn.execute(
            """
            SELECT MAX(box_index)
            FROM coin_analysis_results
            WHERE is_prediction = 0
              AND coin_id = ?
              AND cycle_number = ?
            """,
            (cid, cyc),
        ).fetchone()
        if row and row[0] is not None:
            max_idx = row[0]
            conn.execute(
                """
                UPDATE coin_analysis_results
                SET is_completed = 0,
                    result       = 'ACTIVE'
                WHERE is_prediction = 0
                  AND coin_id = ?
                  AND cycle_number = ?
                  AND box_index = ?
                """,
                (cid, cyc, max_idx),
            )
    conn.commit()

    log.info("=" * 65)
    log.info("총 저장 박스: %d개", total_zones)
    log.info("로그 변환값 범위 요약:")
    print_norm_stats("norm_range_pct", all_norm_range)
    print_norm_stats("norm_duration", all_norm_duration)
    print_norm_stats("norm_hi_change_pct", all_norm_hi_chg)
    print_norm_stats("norm_lo_change_pct", all_norm_lo_chg)
    print_norm_stats("norm_gain_pct", all_norm_gain)
    log.info("분석 완료 — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    conn.close()


if __name__ == "__main__":
    main()