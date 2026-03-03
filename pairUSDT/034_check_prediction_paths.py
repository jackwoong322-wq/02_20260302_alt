import sqlite3

from lib.common.config import DB_PATH


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    checks: dict[str, object] = {}

    # CHECK 1 & 2 (BTC BEAR)
    cur.execute(
        """
        WITH p AS (
          SELECT day_x, value,
                 LAG(value) OVER (ORDER BY day_x) AS prev_v
          FROM coin_prediction_paths
          WHERE symbol='BTC' AND scenario='bear'
        )
        SELECT
          SUM(CASE WHEN prev_v IS NOT NULL AND ABS(value - prev_v) / ABS(prev_v) < 0.001 THEN 1 ELSE 0 END) AS flat_count,
          SUM(CASE WHEN prev_v IS NOT NULL AND ABS(value - prev_v) / ABS(prev_v) > 0.15 THEN 1 ELSE 0 END) AS spike_count
        FROM p;
        """
    )
    checks["CHECK1_flat_BTC"], checks["CHECK2_spike_BTC"] = cur.fetchone()

    # CHECK 3 (BTC BEAR 경유점 반영)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM (
          SELECT a.hi_day, a.hi, p1.value AS path_hi_val,
                 a.lo_day, a.lo, p2.value AS path_lo_val
          FROM coin_analysis_results a
          LEFT JOIN coin_prediction_paths p1
            ON p1.symbol='BTC' AND p1.day_x=a.hi_day AND p1.scenario='bear'
          LEFT JOIN coin_prediction_paths p2
            ON p2.symbol='BTC' AND p2.day_x=a.lo_day AND p2.scenario='bear'
          WHERE a.symbol='BTC' AND a.is_prediction=1 AND a.phase='BEAR'
        )
        WHERE path_hi_val IS NULL
           OR path_lo_val IS NULL
           OR ABS(path_hi_val - hi) / ABS(hi) > 0.01
           OR ABS(path_lo_val - lo) / ABS(lo) > 0.01;
        """
    )
    checks["CHECK3_mismatch_rows_BTC"] = cur.fetchone()[0]

    # CHECK 4 (BULL 첫 포인트 = BEAR 마지막 포인트, BTC)
    cur.execute(
        """
        SELECT
          (SELECT value FROM coin_prediction_paths
           WHERE symbol='BTC' AND scenario='bear'
           ORDER BY day_x DESC LIMIT 1) AS bear_end,
          (SELECT value FROM coin_prediction_paths
           WHERE symbol='BTC' AND scenario='bull'
           ORDER BY day_x ASC LIMIT 1) AS bull_start;
        """
    )
    checks["CHECK4_bear_end"], checks["CHECK4_bull_start"] = cur.fetchone()

    # CHECK 5 (BULL 마지막 포인트 = peak_hi, BTC)
    cur.execute(
        """
        SELECT value FROM coin_prediction_paths
        WHERE symbol='BTC' AND scenario='bull'
        ORDER BY day_x DESC LIMIT 1;
        """
    )
    row = cur.fetchone()
    checks["CHECK5_bull_last"] = row[0] if row else None

    cur.execute(
        """
        SELECT MAX(CASE WHEN phase='BULL' THEN hi END) AS peak_hi
        FROM coin_analysis_results
        WHERE symbol='BTC' AND is_prediction=1;
        """
    )
    checks["CHECK5_peak_hi"] = cur.fetchone()[0]

    # CHECK 6 (BULL spike 없음, BTC)
    cur.execute(
        """
        WITH p AS (
          SELECT day_x, value,
                 LAG(value) OVER (ORDER BY day_x) AS prev_v
          FROM coin_prediction_paths
          WHERE symbol='BTC' AND scenario='bull'
        )
        SELECT COUNT(*) FROM p
        WHERE prev_v IS NOT NULL
          AND ABS(value - prev_v) / ABS(prev_v) > 0.15;
        """
    )
    checks["CHECK6_spike_BTC"] = cur.fetchone()[0]

    # CHECK 7 (BULL 파형 다양성, BTC)
    cur.execute(
        """
        SELECT COUNT(DISTINCT ROUND(value, 1))
        FROM coin_prediction_paths
        WHERE symbol='BTC' AND scenario='bull';
        """
    )
    checks["CHECK7_distinct_BTC"] = cur.fetchone()[0]

    # CHECK 8 (전체 day 연속성, BTC)
    cur.execute(
        """
        SELECT MAX(day_x) - MIN(day_x) + 1 AS expected,
               COUNT(DISTINCT day_x) AS actual
        FROM coin_prediction_paths WHERE symbol='BTC';
        """
    )
    checks["CHECK8_expected_BTC"], checks["CHECK8_actual_BTC"] = cur.fetchone()

    # CHECK 9 (ETH, BNB, XRP 동일 기준: 1,2,4,5,6)
    for sym in ("ETH", "BNB", "XRP"):
        # 1 & 2 (BEAR flat / spike)
        cur.execute(
            """
            WITH p AS (
              SELECT day_x, value,
                     LAG(value) OVER (ORDER BY day_x) AS prev_v
              FROM coin_prediction_paths
              WHERE symbol=? AND scenario='bear'
            )
            SELECT
              SUM(CASE WHEN prev_v IS NOT NULL AND ABS(value - prev_v) / ABS(prev_v) < 0.001 THEN 1 ELSE 0 END) AS flat_count,
              SUM(CASE WHEN prev_v IS NOT NULL AND ABS(value - prev_v) / ABS(prev_v) > 0.15 THEN 1 ELSE 0 END) AS spike_count
            FROM p;
            """,
            (sym,),
        )
        flat, spike = cur.fetchone()
        checks[f"CHECK9_{sym}_flat"] = flat
        checks[f"CHECK9_{sym}_spike"] = spike

        # 4: BULL 첫 = BEAR 마지막
        cur.execute(
            """
            SELECT
              (SELECT value FROM coin_prediction_paths
               WHERE symbol=? AND scenario='bear'
               ORDER BY day_x DESC LIMIT 1) AS bear_end,
              (SELECT value FROM coin_prediction_paths
               WHERE symbol=? AND scenario='bull'
               ORDER BY day_x ASC LIMIT 1) AS bull_start;
            """,
            (sym, sym),
        )
        be, bs = cur.fetchone()
        checks[f"CHECK9_{sym}_bear_end"] = be
        checks[f"CHECK9_{sym}_bull_start"] = bs

        # 5: BULL 마지막 = peak_hi
        cur.execute(
            """
            SELECT value FROM coin_prediction_paths
            WHERE symbol=? AND scenario='bull'
            ORDER BY day_x DESC LIMIT 1;
            """,
            (sym,),
        )
        row = cur.fetchone()
        last_val = row[0] if row else None
        cur.execute(
            """
            SELECT MAX(CASE WHEN phase='BULL' THEN hi END) AS peak_hi
            FROM coin_analysis_results
            WHERE symbol=? AND is_prediction=1;
            """,
            (sym,),
        )
        peak_hi = cur.fetchone()[0]
        checks[f"CHECK9_{sym}_bull_last"] = last_val
        checks[f"CHECK9_{sym}_peak_hi"] = peak_hi

        # 6: BULL spike 없음
        cur.execute(
            """
            WITH p AS (
              SELECT day_x, value,
                     LAG(value) OVER (ORDER BY day_x) AS prev_v
              FROM coin_prediction_paths
              WHERE symbol=? AND scenario='bull'
            )
            SELECT COUNT(*) FROM p
            WHERE prev_v IS NOT NULL
              AND ABS(value - prev_v) / ABS(prev_v) > 0.15;
            """,
            (sym,),
        )
        checks[f"CHECK9_{sym}_spike_BULL"] = cur.fetchone()[0]

    # CHECK 10 (coin_analysis_results 불변: row count)
    cur.execute(
        """
        SELECT COUNT(*) FROM coin_analysis_results WHERE is_prediction=1;
        """
    )
    checks["CHECK10_pred_rows"] = cur.fetchone()[0]

    # CHECK 11 (BTC peak_hi / bottom_lo 불변)
    cur.execute(
        """
        SELECT
          MAX(CASE WHEN phase='BULL' THEN hi END) AS peak_hi,
          MIN(CASE WHEN phase='BEAR' THEN lo END) AS bottom_lo
        FROM coin_analysis_results
        WHERE symbol='BTC' AND is_prediction=1;
        """
    )
    checks["CHECK11_peak_hi_BTC"], checks["CHECK11_bottom_lo_BTC"] = cur.fetchone()

    # CHECK 12 (전체 path 포인트 수: 각 코인/시나리오 별 cnt)
    cur.execute(
        """
        SELECT symbol, scenario, COUNT(*) AS cnt
        FROM coin_prediction_paths
        GROUP BY symbol, scenario
        ORDER BY symbol, scenario;
        """
    )
    counts = cur.fetchall()

    conn.close()

    for k in sorted(checks):
        print(f"{k} = {checks[k]}")

    print("CHECK12_counts:")
    for row in counts:
        print(row)


if __name__ == "__main__":
    main()

