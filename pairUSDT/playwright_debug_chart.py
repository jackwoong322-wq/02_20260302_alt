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
  const LightweightChartsType = typeof LightweightCharts;
  const chartType = typeof chart;
  const seriesMapKeys = typeof seriesMap !== 'undefined' ? Object.keys(seriesMap) : [];
  let btcCoinId = null;
  try { btcCoinId = typeof BTC_COIN_ID !== 'undefined' ? BTC_COIN_ID : null; } catch(e) { btcCoinId = null; }
  let selected = null;
  try { selected = typeof selectedCoins !== 'undefined' ? selectedCoins : null; } catch(e) { selected = null; }
  let allSeriesCount = null;
  if (chart && typeof chart.getAllSeries === 'function') {
    allSeriesCount = chart.getAllSeries().length;
  }
  return { LightweightChartsType, chartType, seriesMapKeys, allSeriesCount, btcCoinId, selected };
}
"""
        )

        print("typeof LightweightCharts:", result["LightweightChartsType"])
        print("typeof chart:", result["chartType"])
        print("seriesMap keys:", result["seriesMapKeys"])
        print("chart.getAllSeries().length:", result["allSeriesCount"])
        print("BTC_COIN_ID (binding):", result["btcCoinId"])
        print("selectedCoins (binding):", result["selected"])

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

