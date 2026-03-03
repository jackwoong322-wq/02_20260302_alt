import asyncio


async def main() -> None:
    from playwright.async_api import async_playwright

    url = "http://localhost:8000/pairUSDT/033_visualizer_html.html"

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        result = await page.evaluate(
            """
() => {
  const keys = typeof seriesMap !== 'undefined' ? Object.keys(seriesMap) : [];
  const closeKey = keys.find(k => k.includes('close')) || null;
  const closeSeries = closeKey ? seriesMap[closeKey] : null;
  const exists = !!closeSeries;
  let items = [];
  let length = 0;
  if (closeSeries && closeSeries._series && closeSeries._series._data && Array.isArray(closeSeries._series._data._items)) {
    items = closeSeries._series._data._items;
    length = items.length;
  }
  const first3 = items.slice(0, 3);
  const last3  = items.slice(-3);
  const vis1 = chart.timeScale().getVisibleRange();
  chart.timeScale().fitContent();
  const vis2 = chart.timeScale().getVisibleRange();
  return {
    closeKey,
    exists,
    length,
    first3,
    last3,
    visibleBefore: vis1,
    visibleAfter: vis2,
  };
}
"""
        )

        print("closeKey:", result["closeKey"])
        print("closeSeries exists:", result["exists"])
        print("data length:", result["length"])
        print("first 3:", result["first3"])
        print("last 3:", result["last3"])
        print("visible range:", result["visibleBefore"])
        print("after fitContent:", result["visibleAfter"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

