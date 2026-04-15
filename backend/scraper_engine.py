"""
Moteur de scraping multi-sources pour syndicats tunisiens.
Sources : DuckDuckGo Lite, Bing, pj.tn, annuaire.com.tn, tayara.tn
"""

import requests
import time
import random
from urllib.parse import quote
from utils import extract_data

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
]


def get_headers(referer=None):
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,ar-TN;q=0.8,en-US;q=0.7,en;q=0.6",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    }
    if referer:
        h["Referer"] = referer
    return h


def fetch(url, retries=2, timeout=12, referer=None):
    for attempt in range(retries + 1):
        try:
            r = requests.get(
                url,
                headers=get_headers(referer),
                timeout=timeout,
                allow_redirects=True
            )
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                time.sleep(2 * (attempt + 1))
        except requests.exceptions.Timeout:
            pass
        except Exception:
            pass
        time.sleep(random.uniform(0.4, 0.9))
    return ""


# ── Sources de scraping ────────────────────────────────────────────────────────

def scrape_duckduckgo(name, city):
    """DuckDuckGo Lite — moins de JS, plus scraper-friendly."""
    query = quote(f"{name} syndic {city} contact téléphone email")
    url = f"https://lite.duckduckgo.com/lite/?q={query}"
    html = fetch(url)
    return extract_data(html), "duckduckgo"


def scrape_bing(name, city):
    """Bing — plus permissif que Google pour le scraping."""
    query = quote(f"{name} {city} syndic immeuble contact téléphone")
    url = f"https://www.bing.com/search?q={query}&setlang=fr&cc=TN"
    html = fetch(url, referer="https://www.bing.com/")
    return extract_data(html), "bing"


def scrape_pj_tn(name, city):
    """Pages Jaunes Tunisie — annuaire professionnel tunisien."""
    q_name = quote(name)
    q_city = quote(city)
    urls = [
        f"https://www.pj.tn/search?what={q_name}+syndic&where={q_city}",
        f"https://www.pj.tn/search?what={q_name}&where={q_city}",
    ]
    for url in urls:
        html = fetch(url, referer="https://www.pj.tn/")
        data = extract_data(html)
        if data["phones"] or data["emails"]:
            return data, "pagesjaunes"
    return extract_data(""), "pagesjaunes"


def scrape_annuaire_tn(name, city):
    """Annuaire professionnel tunisien annuaire.com.tn"""
    query = quote(f"{name} {city}")
    url = f"http://www.annuaire.com.tn/search?q={query}&cat=syndic"
    html = fetch(url, timeout=8)
    return extract_data(html), "annuaire_tn"


def scrape_google_cache(name, city):
    """Recherche via DuckDuckGo avec termes différents (contact direct)."""
    query = quote(f'"{name}" "{city}" syndic téléphone OR "tél" OR "contact"')
    url = f"https://lite.duckduckgo.com/lite/?q={query}"
    html = fetch(url)
    return extract_data(html), "ddg_contact"


def scrape_all(name, city):
    """Lance tous les scrapers et retourne la liste des résultats."""
    sources = [
        lambda: scrape_duckduckgo(name, city),
        lambda: scrape_bing(name, city),
        lambda: scrape_pj_tn(name, city),
        lambda: scrape_annuaire_tn(name, city),
        lambda: scrape_google_cache(name, city),
    ]

    results = []
    for fn in sources:
        try:
            data, source = fn()
            data["source"] = source
            results.append(data)
        except Exception:
            pass
        time.sleep(random.uniform(0.3, 0.6))

    return results
