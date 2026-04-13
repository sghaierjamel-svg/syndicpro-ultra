import requests
from utils import extract_data

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code == 200:
            return r.text
    except:
        return ""
    return ""

def scrape_all(name, city):
    results = []

    queries = [
        ("duckduckgo", f"https://duckduckgo.com/html/?q={name}+{city}+contact"),
        ("pagesjaunes", f"https://www.pj.tn/search?what={name}&where={city}")
    ]

    for source, url in queries:
        html = fetch(url)

        if html:
            data = extract_data(html)
        else:
            data = {"phones": [], "emails": []}

        data["source"] = source
        results.append(data)

    return results
