import asyncio


async def main() -> None:
    from playwright.async_api import async_playwright

    url = "http://localhost:8000/pairUSDT/033_visualizer_html.html"

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # 콘솔 전체 로그 + 에러 캡처
        page.on("console", lambda msg: print(f"[console] {msg.type}: {msg.text}"))
        def _on_page_error(err):
            # err.stack 가 있으면 함께 출력
            stack = getattr(err, "stack", None)
            print(f"[pageerror] {err.message}")
            if stack:
                print(f"[pageerror stack] {stack}")

        page.on("pageerror", _on_page_error)

        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(4000)

        # activeCycles 정보 출력
        await page.evaluate(
            """
() => {
  try {
    console.log('activeCycles type:', typeof activeCycles);
    console.log('activeCycles is Set:', activeCycles instanceof Set);
    console.log('activeCycles size:', activeCycles?.size);
    console.log('activeCycles values:', JSON.stringify(activeCycles ? [...activeCycles] : null));
  } catch (e) {
    console.log('activeCycles check error:', e && e.message);
  }
}
"""
        )

        # 두 번째 케이스: 사이클 2개 선택 (가능한 경우)
        await page.evaluate(
            """
() => {
  if (!window.ALL_DATA || !window.selectedCoins || window.selectedCoins.length === 0) return;
  const coinId = window.selectedCoins[0];
  const cycles = (ALL_DATA[coinId]?.cycles || []).map(c => c.cycle_number);
  const uniq = Array.from(new Set(cycles));
  if (uniq.length < 2) return;
  // 앞의 두 사이클을 activeCycles 로 설정
  window.activeCycles = new Set([uniq[0], uniq[1]]);
  window.drawChart();
}
"""
        )

        await page.wait_for_timeout(4000)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

