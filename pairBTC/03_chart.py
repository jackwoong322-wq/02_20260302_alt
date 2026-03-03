"""
Altcoin Cycle Chart Generator
- SQLite에서 데이터 읽기 → JSON 변환 → HTML 생성 → 브라우저 오픈
- TradingView Lightweight Charts 사용
- 여러 코인 동시 비교 가능 (콤보박스 체크)
- Usage: python chart_cycle.py
"""

import sqlite3
import json
import webbrowser
import os
from pathlib import Path

DB_PATH = "crypto_data.db"
OUT_FILE = "cycle_chart.html"


def load_all_coins(conn):
    """coins 테이블에서 전체 코인 목록"""
    return conn.execute(
        """
        SELECT c.id, c.symbol, c.name, c.rank
        FROM coins c
        WHERE EXISTS (
            SELECT 1 FROM alt_cycle_data a WHERE a.coin_id = c.id
        )
        ORDER BY c.rank
    """
    ).fetchall()


def load_cycle_data(conn, coin_id):
    """코인별 사이클 데이터 로드"""
    rows = conn.execute(
        """
        SELECT cycle_number, cycle_name, days_since_peak,
               close_rate, high_rate, low_rate,
               peak_date, peak_price
        FROM alt_cycle_data
        WHERE coin_id = ?
        ORDER BY cycle_number, days_since_peak
    """,
        (coin_id,),
    ).fetchall()

    cycles = {}
    for row in rows:
        cn = row[0]
        if cn not in cycles:
            cycles[cn] = {
                "cycle_number": cn,
                "cycle_name": row[1],
                "peak_date": row[6],
                "peak_price": row[7],
                "data": [],
            }
        cycles[cn]["data"].append(
            {
                "x": row[2],  # days_since_peak
                "close": round(row[3], 4),  # close_rate
                "high": round(row[4], 4),  # high_rate
                "low": round(row[5], 4),  # low_rate
            }
        )

    return list(cycles.values())


def build_json(conn, coins):
    """전체 코인 데이터를 JSON으로 변환"""
    result = {}
    for coin_id, symbol, name, rank in coins:
        cycles = load_cycle_data(conn, coin_id)
        if cycles:
            result[coin_id] = {
                "symbol": symbol.upper(),
                "name": name,
                "rank": rank,
                "cycles": cycles,
            }
    return result


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Altcoin Cycle Analyzer</title>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600&family=Syne:wght@400;700;800&display=swap');

  :root {
    --bg:        #080c14;
    --panel:     #0d1420;
    --border:    #1e2d45;
    --accent:    #00d4ff;
    --accent2:   #ff6b35;
    --text:      #c8d8f0;
    --text-dim:  #4a6080;
    --green:     #00ff88;
    --red:       #ff4466;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    min-height: 100vh;
    overflow-x: hidden;
  }

  /* ── Header ── */
  .header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 18px 28px;
    border-bottom: 1px solid var(--border);
    background: var(--panel);
  }
  .header-title {
    font-family: 'Syne', sans-serif;
    font-size: 20px;
    font-weight: 800;
    letter-spacing: 2px;
    color: white;
  }
  .header-title span { color: var(--accent); }

  .header-info {
    font-size: 11px;
    color: var(--text-dim);
    letter-spacing: 1px;
  }

  /* ── Layout ── */
  .container {
    display: flex;
    height: calc(100vh - 62px);
  }

  /* ── Sidebar ── */
  .sidebar {
    width: 240px;
    min-width: 240px;
    background: var(--panel);
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  .sidebar-header {
    padding: 14px 16px;
    border-bottom: 1px solid var(--border);
    font-size: 10px;
    letter-spacing: 2px;
    color: var(--text-dim);
    font-weight: 600;
  }

  .search-box {
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
  }
  .search-box input {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 7px 10px;
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    outline: none;
    transition: border-color 0.2s;
  }
  .search-box input:focus { border-color: var(--accent); }
  .search-box input::placeholder { color: var(--text-dim); }

  .coin-list {
    flex: 1;
    overflow-y: auto;
    padding: 6px 0;
  }
  .coin-list::-webkit-scrollbar { width: 4px; }
  .coin-list::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

  .coin-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 9px 16px;
    cursor: pointer;
    transition: background 0.15s;
    border-left: 3px solid transparent;
  }
  .coin-item:hover { background: rgba(0,212,255,0.04); }
  .coin-item.active { border-left-color: var(--accent); background: rgba(0,212,255,0.07); }

  .coin-check {
    width: 14px; height: 14px;
    border: 1px solid var(--border);
    border-radius: 3px;
    display: flex; align-items: center; justify-content: center;
    flex-shrink: 0;
    transition: all 0.15s;
  }
  .coin-item.checked .coin-check {
    background: var(--accent);
    border-color: var(--accent);
  }
  .coin-check svg { display: none; }
  .coin-item.checked .coin-check svg { display: block; }

  .coin-rank {
    font-size: 10px;
    color: var(--text-dim);
    width: 22px;
    text-align: right;
    flex-shrink: 0;
  }
  .coin-symbol {
    font-size: 12px;
    font-weight: 600;
    color: white;
    flex: 1;
  }
  .coin-name {
    font-size: 10px;
    color: var(--text-dim);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 80px;
  }

  .sidebar-actions {
    padding: 12px 16px;
    border-top: 1px solid var(--border);
    display: flex;
    gap: 8px;
  }
  .btn {
    flex: 1;
    padding: 7px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-dim);
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    cursor: pointer;
    letter-spacing: 1px;
    transition: all 0.15s;
  }
  .btn:hover { border-color: var(--accent); color: var(--accent); }
  .btn.primary { border-color: var(--accent); color: var(--accent); background: rgba(0,212,255,0.08); }
  .btn.primary:hover { background: rgba(0,212,255,0.18); }

  /* ── Main Chart Area ── */
  .main {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  .chart-toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 20px;
    border-bottom: 1px solid var(--border);
    background: var(--panel);
    flex-wrap: wrap;
  }

  .toolbar-label {
    font-size: 10px;
    color: var(--text-dim);
    letter-spacing: 1px;
  }

  .cycle-toggles {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
  }

  .cycle-btn {
    padding: 4px 12px;
    border-radius: 20px;
    border: 1px solid;
    font-size: 10px;
    font-family: 'JetBrains Mono', monospace;
    cursor: pointer;
    transition: all 0.15s;
    letter-spacing: 0.5px;
  }

  .chart-area {
    flex: 1;
    position: relative;
    overflow: hidden;
  }

  #chart { width: 100%; height: 100%; }

  /* ── Legend ── */
  .legend {
    position: absolute;
    top: 12px;
    left: 16px;
    background: rgba(8,12,20,0.88);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 10px 14px;
    font-size: 11px;
    pointer-events: none;
    backdrop-filter: blur(8px);
    min-width: 220px;
  }
  .legend-title {
    font-family: 'Syne', sans-serif;
    font-size: 13px;
    font-weight: 700;
    color: white;
    margin-bottom: 8px;
    letter-spacing: 1px;
  }
  .legend-item {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 4px 0;
    color: var(--text);
  }
  .legend-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .legend-info { font-size: 10px; color: var(--text-dim); }

  /* ── Stats Bar ── */
  .stats-bar {
    display: flex;
    gap: 0;
    border-top: 1px solid var(--border);
    background: var(--panel);
    overflow-x: auto;
  }
  .stat-item {
    padding: 8px 20px;
    border-right: 1px solid var(--border);
    flex-shrink: 0;
  }
  .stat-label { font-size: 9px; color: var(--text-dim); letter-spacing: 1px; margin-bottom: 2px; }
  .stat-value { font-size: 12px; color: white; font-weight: 600; }
  .stat-value.up   { color: var(--green); }
  .stat-value.down { color: var(--red); }

  /* ── Empty state ── */
  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: var(--text-dim);
    gap: 12px;
  }
  .empty-state .icon { font-size: 48px; opacity: 0.3; }
  .empty-state p { font-size: 13px; letter-spacing: 1px; }
</style>
</head>
<body>

<div class="header">
  <div class="header-title">ALT<span>/</span>BTC <span style="font-weight:400;font-size:14px;color:var(--text-dim)">CYCLE ANALYZER</span></div>
  <div class="header-info">PEAK-NORMALIZED · BTC PAIR · DAYS SINCE PEAK</div>
</div>

<div class="container">

  <!-- Sidebar -->
  <div class="sidebar">
    <div class="sidebar-header">COINS — SELECT TO COMPARE</div>
    <div class="search-box">
      <input type="text" id="searchInput" placeholder="Search coin...">
    </div>
    <div class="coin-list" id="coinList"></div>
    <div class="sidebar-actions">
      <button class="btn" onclick="clearAll()">CLEAR</button>
      <button class="btn primary" onclick="drawChart()">APPLY</button>
    </div>
  </div>

  <!-- Main -->
  <div class="main">
    <div class="chart-toolbar">
      <span class="toolbar-label">CYCLES:</span>
      <div class="cycle-toggles" id="cycleToggles"></div>
      <div style="margin-left:auto; display:flex; gap:8px; align-items:center;">
        <span class="toolbar-label">SHOW:</span>
        <button class="cycle-btn" id="toggleRange"
          style="border-color:#4a6080;color:#4a6080;"
          onclick="toggleHighLow()">HIGH/LOW</button>
      </div>
    </div>

    <div class="chart-area">
      <div id="chart"></div>
      <div class="legend" id="legend" style="display:none"></div>
    </div>

    <div class="stats-bar" id="statsBar"></div>
  </div>
</div>

<script>
// ── Data ──────────────────────────────────────────────
const ALL_DATA = __CHART_DATA__;

// ── Cycle Colors ──────────────────────────────────────
const CYCLE_COLORS = {
  1: { main:'#FF4D4D', band:'rgba(255,77,77,0.08)'  },
  2: { main:'#00C8FF', band:'rgba(0,200,255,0.08)'  },
  3: { main:'#FFB800', band:'rgba(255,184,0,0.08)'  },
  4: { main:'#7CFF6B', band:'rgba(124,255,107,0.08)'},
  5: { main:'#FF69B4', band:'rgba(255,105,180,0.08)'},
};
const COIN_COLORS = [
  '#00D4FF','#FF6B35','#A8FF3E','#FF3CAC','#784BA0',
  '#2B86C5','#FFD700','#FF6B6B','#4ECDC4','#45B7D1',
];

// ── State ─────────────────────────────────────────────
let selectedCoins  = [];
let activeCycles   = new Set([1,2,3,4,5]);
let showHighLow    = false;
let chart          = null;
let seriesMap      = {};

// ── Init Chart ────────────────────────────────────────
function initChart() {
  const el = document.getElementById('chart');
  chart = LightweightCharts.createChart(el, {
    layout: {
      background: { color: '#080c14' },
      textColor:  '#c8d8f0',
    },
    grid: {
      vertLines: { color: '#1e2d45' },
      horzLines: { color: '#1e2d45' },
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: {
      borderColor: '#1e2d45',
      scaleMargins: { top: 0.05, bottom: 0.05 },
    },
    timeScale: {
      borderColor: '#1e2d45',
      tickMarkFormatter: v => `Day ${v}`,
    },
    handleScroll: true,
    handleScale:  true,
  });

  // Resize observer
  new ResizeObserver(() => {
    if (chart) chart.applyOptions({
      width:  el.clientWidth,
      height: el.clientHeight,
    });
  }).observe(el);
}

// ── Build Coin List ───────────────────────────────────
function buildCoinList(filter='') {
  const el   = document.getElementById('coinList');
  el.innerHTML = '';
  const keys = Object.keys(ALL_DATA).filter(id => {
    const d = ALL_DATA[id];
    return d.symbol.toLowerCase().includes(filter.toLowerCase()) ||
           d.name.toLowerCase().includes(filter.toLowerCase());
  });

  keys.forEach(id => {
    const d    = ALL_DATA[id];
    const sel  = selectedCoins.includes(id);
    const div  = document.createElement('div');
    div.className = 'coin-item' + (sel ? ' checked active' : '');
    div.dataset.id = id;
    div.innerHTML = `
      <div class="coin-check">
        <svg width="8" height="8" viewBox="0 0 8 8">
          <polyline points="1,4 3,6 7,2" fill="none" stroke="#080c14" stroke-width="1.5"/>
        </svg>
      </div>
      <span class="coin-rank">#${d.rank||'?'}</span>
      <span class="coin-symbol">${d.symbol}</span>
      <span class="coin-name">${d.name}</span>
    `;
    div.onclick = () => toggleCoin(id, div);
    el.appendChild(div);
  });
}

function toggleCoin(id, el) {
  const idx = selectedCoins.indexOf(id);
  if (idx >= 0) {
    selectedCoins.splice(idx, 1);
    el.classList.remove('checked','active');
  } else {
    selectedCoins.push(id);
    el.classList.add('checked','active');
  }
}

function clearAll() {
  selectedCoins = [];
  buildCoinList(document.getElementById('searchInput').value);
  drawChart();
}

// ── Cycle Toggles ─────────────────────────────────────
function buildCycleToggles() {
  const el = document.getElementById('cycleToggles');
  el.innerHTML = '';
  // collect all cycle numbers across selected data
  const cycleNums = new Set();
  selectedCoins.forEach(id => {
    (ALL_DATA[id]?.cycles||[]).forEach(c => cycleNums.add(c.cycle_number));
  });
  if (cycleNums.size === 0) {
    [1,2,3,4,5].forEach(n => cycleNums.add(n));
  }
  [...cycleNums].sort().forEach(n => {
    const col  = CYCLE_COLORS[n] || CYCLE_COLORS[1];
    const name = n === 5 ? 'CURRENT' : `CYCLE ${n}`;
    const btn  = document.createElement('button');
    btn.className = 'cycle-btn';
    const active = activeCycles.has(n);
    btn.style.cssText = active
      ? `border-color:${col.main};color:${col.main};background:${col.band}`
      : `border-color:#1e2d45;color:#4a6080;background:transparent`;
    btn.textContent = name;
    btn.onclick = () => {
      if (activeCycles.has(n)) activeCycles.delete(n);
      else activeCycles.add(n);
      buildCycleToggles();
      drawChart();
    };
    el.appendChild(btn);
  });
}

function toggleHighLow() {
  showHighLow = !showHighLow;
  const btn = document.getElementById('toggleRange');
  btn.style.cssText = showHighLow
    ? 'border-color:#00d4ff;color:#00d4ff;background:rgba(0,212,255,0.1)'
    : 'border-color:#4a6080;color:#4a6080;';
  drawChart();
}

// ── Draw Chart ────────────────────────────────────────
function drawChart() {
  // clear
  Object.values(seriesMap).forEach(s => {
    try { chart.removeSeries(s); } catch(e){}
  });
  seriesMap = {};

  buildCycleToggles();

  if (selectedCoins.length === 0) {
    updateLegend([]);
    updateStats([]);
    return;
  }

  const legendItems = [];

  selectedCoins.forEach((coinId, coinIdx) => {
    const coinData = ALL_DATA[coinId];
    if (!coinData) return;
    const baseColor = COIN_COLORS[coinIdx % COIN_COLORS.length];

    coinData.cycles.forEach(cycle => {
      if (!activeCycles.has(cycle.cycle_number)) return;

      const col    = CYCLE_COLORS[cycle.cycle_number] || CYCLE_COLORS[1];
      // multi-coin → use coin color; single coin → use cycle color
      const color  = selectedCoins.length > 1 ? baseColor : col.main;
      const isCurr = cycle.cycle_name === 'Current Cycle';

      // Close line
      const lineSeries = chart.addLineSeries({
        color,
        lineWidth:   selectedCoins.length > 1 ? 1.5 : 2,
        lineStyle:   isCurr ? LightweightCharts.LineStyle.Dashed : LightweightCharts.LineStyle.Solid,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: true,
      });
      const lineData = cycle.data.map(d => ({ time: d.x, value: d.close }));
      lineSeries.setData(lineData);
      seriesMap[`${coinId}_${cycle.cycle_number}_close`] = lineSeries;

      // High/Low bands
      if (showHighLow) {
        const bandColor = selectedCoins.length > 1
          ? baseColor.replace('FF','40').replace('#','rgba(').replace(/(.{2})(.{2})(.{2})/,'$1,$2,$3') 
          : col.band;

        const hiSeries = chart.addLineSeries({
          color: color + '55',
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dotted,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
        hiSeries.setData(cycle.data.map(d => ({ time: d.x, value: d.high })));
        seriesMap[`${coinId}_${cycle.cycle_number}_hi`] = hiSeries;

        const loSeries = chart.addLineSeries({
          color: color + '55',
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dotted,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
        loSeries.setData(cycle.data.map(d => ({ time: d.x, value: d.low })));
        seriesMap[`${coinId}_${cycle.cycle_number}_lo`] = loSeries;
      }

      legendItems.push({
        color,
        label: selectedCoins.length > 1
          ? `${coinData.symbol} · ${cycle.cycle_name}`
          : cycle.cycle_name,
        peak: cycle.peak_date,
        isCurr,
      });
    });
  });

  updateLegend(legendItems);
  updateStats();
  chart.timeScale().fitContent();
}

// ── Legend ────────────────────────────────────────────
function updateLegend(items) {
  const el = document.getElementById('legend');
  if (items.length === 0) { el.style.display='none'; return; }
  el.style.display = 'block';

  const coinLabel = selectedCoins.length === 1
    ? ALL_DATA[selectedCoins[0]]?.symbol + '/BTC'
    : `${selectedCoins.length} COINS`;

  el.innerHTML = `<div class="legend-title">${coinLabel}</div>` +
    items.map(i => `
      <div class="legend-item">
        <div class="legend-dot" style="background:${i.color}"></div>
        <div>
          <div>${i.label}${i.isCurr ? ' <span style="color:#FFB800;font-size:9px">●LIVE</span>':''}</div>
          <div class="legend-info">Peak: ${i.peak}</div>
        </div>
      </div>
    `).join('');
}

// ── Stats Bar ─────────────────────────────────────────
function updateStats() {
  const el = document.getElementById('statsBar');
  if (selectedCoins.length !== 1) { el.innerHTML=''; return; }

  const coinData = ALL_DATA[selectedCoins[0]];
  if (!coinData) return;

  let html = '';
  coinData.cycles.forEach(cycle => {
    if (!activeCycles.has(cycle.cycle_number)) return;
    const minRate = Math.min(...cycle.data.map(d=>d.close));
    const minDay  = cycle.data.find(d=>d.close===minRate)?.x ?? '-';
    const maxDays = cycle.data[cycle.data.length-1]?.x ?? '-';
    const lastRate = cycle.data[cycle.data.length-1]?.close ?? 0;
    const isDown = lastRate < 100;

    html += `
      <div class="stat-item">
        <div class="stat-label">${cycle.cycle_name.toUpperCase()}</div>
        <div class="stat-label" style="margin-top:2px">Peak: ${cycle.peak_date}</div>
      </div>
      <div class="stat-item">
        <div class="stat-label">BOTTOM</div>
        <div class="stat-value down">${minRate.toFixed(1)}%</div>
        <div class="stat-label">day ${minDay}</div>
      </div>
      <div class="stat-item">
        <div class="stat-label">DURATION</div>
        <div class="stat-value">${maxDays}d</div>
      </div>
      <div class="stat-item">
        <div class="stat-label">CURRENT</div>
        <div class="stat-value ${isDown?'down':'up'}">${lastRate.toFixed(1)}%</div>
      </div>
    `;
  });
  el.innerHTML = html;
}

// ── Search ────────────────────────────────────────────
document.getElementById('searchInput').addEventListener('input', e => {
  buildCoinList(e.target.value);
});

// ── Boot ──────────────────────────────────────────────
initChart();
buildCoinList();
buildCycleToggles();
</script>
</body>
</html>
"""


def generate_html(data_json: dict) -> str:
    json_str = json.dumps(data_json, ensure_ascii=False)
    return HTML_TEMPLATE.replace("__CHART_DATA__", json_str)


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
