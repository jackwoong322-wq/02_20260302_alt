import sys
import asyncio

from pathlib import Path


async def main() -> None:
    # loop index for file naming
    loop_idx = sys.argv[1] if len(sys.argv) > 1 else "1"

    # HTML 파일은 033_visualizer_html.py 실행 로그 기준으로
    #   E:\source\02_20260302_alt\pairUSDT\033_visualizer_html.html
    # 에 생성된다. 루트(02_20260302_alt)에서 http.server(8000)를 띄운 뒤,
    # 아래 URL 로 접근한다.
    url = "http://localhost:8000/pairUSDT/033_visualizer_html.html"

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        await page.goto(url, wait_until="networkidle")

        # 차트 렌더링을 위해 약간 추가 대기
        await page.wait_for_timeout(3000)

        out_path = Path(f"screenshot_loop{loop_idx}.png").resolve()
        await page.screenshot(path=str(out_path), full_page=True)
        print(f"[Playwright] Screenshot saved: {out_path}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())

