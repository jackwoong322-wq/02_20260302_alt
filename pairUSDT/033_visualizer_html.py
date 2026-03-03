"""033_visualizer_html.py

기존 UI/차트 시각화 (DB 데이터 기반)

Usage: python 033_visualizer_html.py
"""

import sqlite3
import webbrowser
from pathlib import Path

from lib.common.config import DB_PATH
from lib.visualizer.db import build_json, load_all_coins
from lib.visualizer.renderer import generate_html

OUT_FILE = "./pairUSDT/033_visualizer_html.html"


def main():
    conn = sqlite3.connect(DB_PATH)
    coins = load_all_coins(conn)

    if not coins:
        print("[ERROR] No coin data. Run alt_cycle_analysis.py first.")
        conn.close()
        return

    print(f"Loading data for {len(coins)} coins...")
    data = build_json(conn, coins)
    conn.close()

    html = generate_html(data)
    out = Path(OUT_FILE)
    out.write_text(html, encoding="utf-8")

    print(f"Chart saved: {out.resolve()}")
    webbrowser.open(f"file://{out.resolve()}")


if __name__ == "__main__":
    main()
