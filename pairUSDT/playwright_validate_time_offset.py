import asyncio
import json


async def main() -> None:
    from playwright.async_api import async_playwright

    url = "http://localhost:8000/pairUSDT/033_visualizer_html.html"

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        console_messages = []

        def _on_console(msg) -> None:
            # 콘솔 로그는 누적만 하고 터미널에는 출력하지 않는다
            text = msg.text
            console_messages.append((msg.type, text))

        def _on_page_error(err) -> None:
            msg = getattr(err, "message", str(err))
            console_messages.append(("pageerror", msg))

        page.on("console", _on_console)
        page.on("pageerror", _on_page_error)

        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(4000)

        result = await page.evaluate(
            """
() => {
  const out = {};

  // 박스존 및 Bear/Bull 오버레이를 켜서 정렬/마커를 확인한다.
  try {
    if (typeof showBoxZone !== 'undefined' && !showBoxZone && typeof toggleBoxZone === 'function') {
      toggleBoxZone();
    }
    if (typeof showBearBull !== 'undefined' && !showBearBull && typeof toggleBearBull === 'function') {
      toggleBearBull();
    }
  } catch (e) {
    console.log('toggle error', e && e.message);
  }

  const sm = typeof seriesMap !== 'undefined' ? seriesMap : null;
  if (!sm) {
    out.error = 'seriesMap not defined';
    return out;
  }

  const close = sm['bitcoin_5_close'] || null;
  const bull  = sm['bitcoin_5_path_bull'] || null;
  const bear  = sm['bitcoin_5_path_bear'] || null;

  function seriesInfo(s) {
    if (!s || typeof s.data !== 'function') return null;
    const arr = s.data();
    const times = arr
      .map(p => (p && typeof p.time === 'number') ? p.time : null)
      .filter(t => t !== null);
    const minTime = times.length ? Math.min(...times) : null;
    const maxTime = times.length ? Math.max(...times) : null;
    const firstTime = arr.length ? arr[0].time : null;
    let nonIncreasing = false;
    let hasDuplicate = false;
    for (let i = 1; i < times.length; i++) {
      if (times[i] <= times[i - 1]) {
        if (times[i] === times[i - 1]) hasDuplicate = true;
        nonIncreasing = true;
        break;
      }
    }
    return {
      length: arr.length,
      minTime,
      maxTime,
      firstTime,
      first3: arr.slice(0, 3),
      nonIncreasing,
      hasDuplicate,
    };
  }

  out.close    = seriesInfo(close);
  out.pathBull = seriesInfo(bull);
  out.pathBear = seriesInfo(bear);

  // DB 상 bitcoin cycle 5 의 days_since_peak 최소값
  out.dbMinDay      = null;
  out.dbHasMinZero  = false;
  out.dbCycleFound  = false;
  if (window.ALL_DATA) {
    const btcId = Object.keys(ALL_DATA).find(
      id => (ALL_DATA[id]?.symbol || '').toUpperCase() === 'BTC'
    );
    const btc = btcId ? ALL_DATA[btcId] : null;
    if (btc && Array.isArray(btc.cycles)) {
      const cyc5 = btc.cycles.find(c => c.cycle_number === 5);
      if (cyc5 && Array.isArray(cyc5.data) && cyc5.data.length > 0) {
        out.dbCycleFound = true;
        const xs = cyc5.data
          .map(d => (d && typeof d.x === 'number') ? d.x : null)
          .filter(x => x !== null);
        if (xs.length) {
          const mn = Math.min(...xs);
          out.dbMinDay = mn;
          out.dbHasMinZero = (mn === 0);
        }
      }
    }
  }

  // 사이클 저점 마크/예측 경로 끝 마커/박스 마크 존재 여부
  out.hasCycleLowMark   = !!document.querySelector('.cycle-low-mark');
  out.predEndLabels     = Array.from(document.querySelectorAll('.pred-path-end')).map(el => el.textContent);
  out.hasBoxMark        = !!document.querySelector('.bz-mark');
  out.hasBearBullLabel  = !!document.querySelector('.bb-label');

  // timeScale 범위 (logical time)
  out.visibleRange = chart && chart.timeScale
    ? chart.timeScale().getVisibleRange()
    : null;

  return out;
}
"""
        )

        print("JSON_RESULT", json.dumps(result, ensure_ascii=False))

        # ── Validation ─────────────────────────────────────────────
        errors = []

        # 1. 콘솔에 "Value is null" 에러가 없는지
        val_null_count = sum(
            1
            for typ, text in console_messages
            if isinstance(text, str) and "Value is null" in text
        )
        if val_null_count != 0:
            errors.append(f"1: Value is null count={val_null_count}")

        close = result.get("close") or {}
        bull = result.get("pathBull") or {}
        bear = result.get("pathBear") or {}

        # 2. seriesMap['bitcoin_5_close'].data()[0].time === 1 (DB min day 가 0 인 경우)
        if result.get("dbHasMinZero") and close.get("length", 0) > 0:
            if close.get("firstTime") != 1:
                errors.append(
                    f"2: firstTime={close.get('firstTime')} (expected 1 when dbMinDay==0)"
                )

        # 3. Math.min(...times) >= 1
        if close.get("minTime") is not None and close["minTime"] < 1:
            errors.append(f"3: close.minTime={close['minTime']} (expected >=1)")

        # 4/5. path_bull, path_bear length > 0
        if bull.get("length", 0) <= 0:
            errors.append(f"4: pathBull.length={bull.get('length')}")
        if bear.get("length", 0) <= 0:
            errors.append(f"5: pathBear.length={bear.get('length')}")

        # 6. path_bull, path_bear 도 동일한 time 기준(+1 offset)을 사용하고,
        #    time 이 1 이상이고 오름차순인지만 확인 (범위 일치까지는 강제하지 않음)
        for name, info in (("pathBull", bull), ("pathBear", bear)):
            if info.get("length", 0) > 0:
                if info.get("minTime") is not None and info["minTime"] < 1:
                    errors.append(f"6: {name}.minTime={info['minTime']} (expected >=1)")
                if info.get("nonIncreasing"):
                    errors.append(f"6: {name} has non-increasing times")

        # 7. 사이클 저점 마크 존재
        if not result.get("hasCycleLowMark"):
            errors.append("7: cycle-low-mark element not found")

        # 8. 예측 경로 끝 마커 존재 (텍스트 'BULL 예측' / 'BEAR 예측')
        labels = " | ".join(result.get("predEndLabels") or [])
        if "BULL 예측" not in labels or "BEAR 예측" not in labels:
            errors.append(f"8: predEndLabels={labels!r}")

        # 9. 박스 마크 / Bear/Bull 라벨 존재 (기본 정렬 체크용)
        if not result.get("hasBoxMark"):
            errors.append("9: box mark element (.bz-mark) not found")
        if not result.get("hasBearBullLabel"):
            errors.append("9: bear/bull label (.bb-label) not found")

        if errors:
            print("VALIDATION_ERRORS", json.dumps(errors, ensure_ascii=False))
        else:
            print("VALIDATION_OK")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

