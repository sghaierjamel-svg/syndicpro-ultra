import asyncio
from playwright.async_api import async_playwright
from utils import extract_data

async def scrape_all(name, city):
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        sources = [
            ("google", f"https://www.google.com/maps/search/{name}+{city}"),
            ("duckduckgo", f"https://duckduckgo.com/?q={name}+{city}+contact"),
            ("pagesjaunes", f"https://www.pj.tn/search?what={name}&where={city}")
        ]

        for source, url in sources:
            try:
                await page.goto(url, timeout=10000)
                await page.wait_for_timeout(2000)

                html = await page.content()
                data = extract_data(html)
                data["source"] = source

                results.append(data)
            except:
                results.append({"source": source})

        await browser.close()

    return results
