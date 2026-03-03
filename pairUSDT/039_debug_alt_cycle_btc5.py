import sqlite3

from lib.common.config import DB_PATH


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print("BTC alt_cycle_data, cycle_number=5:")
    cur.execute(
        """
        SELECT COUNT(*), MIN(days_since_peak), MAX(days_since_peak)
        FROM alt_cycle_data
        WHERE coin_id = 'bitcoin' AND cycle_number = 5
        """
    )
    print(cur.fetchone())

    conn.close()


if __name__ == "__main__":
    main()

