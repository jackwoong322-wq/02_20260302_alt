import sqlite3

from lib.common.config import DB_PATH


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("=== BTC alt_cycle_data summary ===")
    cur.execute(
        """
        SELECT cycle_number,
               COUNT(*) AS cnt,
               MIN(days_since_peak),
               MAX(days_since_peak)
        FROM alt_cycle_data a
        JOIN coins c ON a.coin_id = c.id
        WHERE UPPER(c.symbol)='BTC'
        GROUP BY cycle_number
        ORDER BY cycle_number
        """
    )
    for row in cur.fetchall():
        print(row)

    print("\n=== BTC CURRENT cycle raw sample (cycle 5) ===")
    cur.execute(
        """
        SELECT cycle_number, days_since_peak, close_rate, high_rate, low_rate
        FROM alt_cycle_data a
        JOIN coins c ON a.coin_id = c.id
        WHERE UPPER(c.symbol)='BTC' AND cycle_number=5
        ORDER BY days_since_peak
        LIMIT 10
        """
    )
    for row in cur.fetchall():
        print(row)

    conn.close()


if __name__ == "__main__":
    main()

