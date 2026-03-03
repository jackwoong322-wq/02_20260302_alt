import asyncio


async def main() -> None:
    from playwright.async_api import async_playwright

    url = "http://localhost:8000/pairUSDT/033_visualizer_html.html"

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(4000)

        result = await page.evaluate(
            """
() => {
  const out = {};
  if (typeof seriesMap === 'undefined') return out;
  for (const [key, series] of Object.entries(seriesMap)) {
    if (!series || typeof series.data !== 'function') continue;
    let arr = [];
    try { arr = series.data(); } catch (e) { out[key] = { error: e && e.message }; continue; }
    const len = arr.length;
    let nullCount = 0;
    for (const p of arr) {
      if (p == null || p.time == null || p.value == null) nullCount++;
    }
    out[key] = { len, nullCount };
  }
  return out;
}
"""
        )

        for key, info in result.items():
            print(key, "=>", info)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

