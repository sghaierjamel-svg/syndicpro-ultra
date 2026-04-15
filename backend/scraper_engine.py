"""
SyndicPro Scanner — Moteur de scraping ULTIME
16 sources en parallèle : Google, Bing, DDG, Facebook, LinkedIn,
Google Maps, pj.tn, annuaire.com.tn, RNE.tn, tayara.tn, mubawab.tn,
tunisie.com, requêtes arabe, contact page crawler, et plus.
"""

import requests
import time
import random
from urllib.parse import quote, urlparse
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from bs4 import BeautifulSoup
from utils import extract_data

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0",
]

NOISE_DOMAINS = {
    'google', 'bing', 'duckduckgo', 'yahoo', 'facebook',
    'twitter', 'youtube', 'wikipedia', 'linkedin', 'instagram',
    'tiktok', 'amazon', 'apple', 'microsoft',
}


def _headers(referer=None):
    h = {
        "User-Agent":              random.choice(USER_AGENTS),
        "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language":         "fr-FR,fr;q=0.9,ar-TN;q=0.8,en-US;q=0.7",
        "Accept-Encoding":         "gzip, deflate, br",
        "Connection":              "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT":                     "1",
        "Cache-Control":           "max-age=0",
    }
    if referer:
        h["Referer"] = referer
        h["Sec-Fetch-Site"] = "cross-site"
    return h


def fetch(url, timeout=8, retries=1, referer=None):
    """Récupère une URL avec retry limité — timeout court pour rester dans les 30s globales."""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(referer),
                             timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                time.sleep(1)
            elif r.status_code in (403, 404, 410):
                return ""
        except Exception:
            pass
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCES DE RECHERCHE
# ══════════════════════════════════════════════════════════════════════════════

def src_ddg_general(name, city):
    """DuckDuckGo Lite — recherche générale contact."""
    q = quote(f"{name} syndic {city} contact téléphone email")
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "ddg"


def src_ddg_contact(name, city):
    """DuckDuckGo — ciblé sur les numéros de téléphone."""
    q = quote(f'"{name}" "{city}" téléphone OR tél OR "04" OR "05" OR "07" OR "02"')
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "ddg_tel"


def src_ddg_arabic(name, city):
    """DuckDuckGo — requête en arabe (beaucoup de syndics tunisiens en arabe)."""
    q = quote(f"{name} {city} نقابة ملاك هاتف عنوان")
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "ddg_ar"


def src_ddg_facebook(name, city):
    """DuckDuckGo — recherche sur Facebook."""
    q = quote(f"site:facebook.com {name} {city}")
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "facebook"


def src_ddg_linkedin(name, city):
    """DuckDuckGo — recherche sur LinkedIn."""
    q = quote(f"site:linkedin.com {name} syndic {city} Tunisie contact")
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "linkedin"


def src_ddg_gmaps(name, city):
    """DuckDuckGo — extraire les numéros des résultats Google Maps indexés."""
    q = quote(f"site:maps.google.com {name} {city} OR \"{name}\" maps tunisie téléphone")
    return extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={q}")), "gmaps"


def src_bing_general(name, city):
    """Bing — recherche générale."""
    q = quote(f"{name} {city} syndic immeuble contact téléphone")
    return extract_data(fetch(
        f"https://www.bing.com/search?q={q}&setlang=fr&cc=TN",
        referer="https://www.bing.com/"
    )), "bing"


def src_bing_facebook(name, city):
    """Bing — recherche Facebook (Bing indexe mieux FB que DDG)."""
    q = quote(f"site:facebook.com \"{name}\" {city}")
    return extract_data(fetch(
        f"https://www.bing.com/search?q={q}",
        referer="https://www.bing.com/"
    )), "bing_fb"


def src_bing_local(name, city):
    """Bing Local — résultats d'entreprises locales."""
    q = quote(f"{name} {city} syndic Tunisia phone email")
    return extract_data(fetch(
        f"https://www.bing.com/search?q={q}&first=1&FORM=PERE",
        referer="https://www.bing.com/"
    )), "bing_local"


def src_google(name, city):
    """Google Search — avec session dédiée et cookies propres."""
    q = quote(f"{name} syndic {city} téléphone contact Tunisie")
    url = f"https://www.google.com/search?q={q}&hl=fr&gl=tn&num=10"
    try:
        r = requests.get(url, headers=_headers("https://www.google.com/"), timeout=8)
        if r.status_code == 200:
            return extract_data(r.text), "google"
    except Exception:
        pass
    return extract_data(""), "google"


def src_google_arabic(name, city):
    """Google Search — requête arabe."""
    q = quote(f"{name} {city} نقابة ملاك تونس هاتف")
    url = f"https://www.google.com/search?q={q}&hl=ar&gl=tn"
    try:
        r = requests.get(url, headers=_headers("https://www.google.com/"), timeout=10)
        if r.status_code == 200:
            return extract_data(r.text), "google_ar"
    except Exception:
        pass
    return extract_data(""), "google_ar"


def src_pj_tn(name, city):
    """Pages Jaunes Tunisie — annuaire professionnel."""
    qn, qc = quote(name), quote(city)
    for url in [
        f"https://www.pj.tn/search?what={qn}+syndic&where={qc}",
        f"https://www.pj.tn/search?what={qn}&where={qc}",
    ]:
        html = fetch(url, referer="https://www.pj.tn/")
        d = extract_data(html)
        if d["phones"] or d["emails"]:
            return d, "pagesjaunes"
    return extract_data(""), "pagesjaunes"


def src_annuaire_tn(name, city):
    """annuaire.com.tn — annuaire tunisien."""
    q = quote(f"{name} {city}")
    html = fetch(f"http://www.annuaire.com.tn/search?q={q}&cat=syndic", timeout=8)
    return extract_data(html), "annuaire_tn"


def src_tayara(name, city):
    """tayara.tn — petites annonces tunisiennes."""
    q = quote(f"{name} {city}")
    html = fetch(f"https://www.tayara.tn/search/?q={q}", referer="https://www.tayara.tn/")
    return extract_data(html), "tayara"


def src_mubawab(name, city):
    """mubawab.tn — immobilier tunisien."""
    q = quote(f"{name} {city}")
    html = fetch(f"https://www.mubawab.tn/fr/sc/immobilier-a-vendre:q:{q}",
                 referer="https://www.mubawab.tn/", timeout=10)
    return extract_data(html), "mubawab"


def src_rne(name, city, rne_id=""):
    """Registre National des Entreprises Tunisie."""
    if rne_id:
        html = fetch(f"https://www.registre.tn/fr/societe/{rne_id}", timeout=10)
        d = extract_data(html)
        if d["phones"] or d["emails"]:
            return d, "rne_direct"
    q = quote(f"{name} {city}")
    html = fetch(f"https://www.registre.tn/fr/recherche?q={q}",
                 referer="https://www.registre.tn/")
    return extract_data(html), "rne"


def src_contact_crawler(name, city):
    """
    Trouve le site officiel de la résidence, puis visite sa page /contact.
    Méthode la plus fiable quand elle aboutit.
    """
    q = quote(f'"{name}" "{city}" syndic')
    html = fetch(f"https://lite.duckduckgo.com/lite/?q={q}")
    if not html:
        return extract_data(""), "crawler"

    soup = BeautifulSoup(html, "html.parser")
    candidate_urls = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if not href.startswith('http'):
            continue
        parsed = urlparse(href)
        domain = parsed.netloc.lower().replace('www.', '')
        # Garder uniquement les sites tunisiens ou pertinents
        if any(n in domain for n in NOISE_DOMAINS):
            continue
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in candidate_urls:
            candidate_urls.append(base)

    merged = {"phones": [], "emails": [], "websites": []}
    seen_phones, seen_emails = set(), set()

    for base in candidate_urls[:4]:
        for path in ['/contact', '/nous-contacter', '/contactez-nous',
                     '/coordonnees', '/about', '/']:
            page_html = fetch(base + path, timeout=8, referer=base)
            if not page_html:
                continue
            d = extract_data(page_html)
            for p in d["phones"]:
                if p not in seen_phones:
                    seen_phones.add(p)
                    merged["phones"].append(p)
            for e in d["emails"]:
                if e not in seen_emails:
                    seen_emails.add(e)
                    merged["emails"].append(e)
            merged["websites"].extend(d["websites"])
            if d["phones"] or d["emails"]:
                break  # page contact trouvée pour ce domaine

    return merged, "crawler"


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATEUR PARALLÈLE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_all(name, city, rne_id=""):
    """
    Lance les 16 sources EN PARALLÈLE (max 8 workers).
    Timeout global 35 secondes.
    """
    tasks = {
        "ddg":          lambda: src_ddg_general(name, city),
        "ddg_tel":      lambda: src_ddg_contact(name, city),
        "ddg_ar":       lambda: src_ddg_arabic(name, city),
        "facebook":     lambda: src_ddg_facebook(name, city),
        "linkedin":     lambda: src_ddg_linkedin(name, city),
        "gmaps":        lambda: src_ddg_gmaps(name, city),
        "bing":         lambda: src_bing_general(name, city),
        "bing_fb":      lambda: src_bing_facebook(name, city),
        "bing_local":   lambda: src_bing_local(name, city),
        "google":       lambda: src_google(name, city),
        "google_ar":    lambda: src_google_arabic(name, city),
        "pagesjaunes":  lambda: src_pj_tn(name, city),
        "annuaire_tn":  lambda: src_annuaire_tn(name, city),
        "tayara":       lambda: src_tayara(name, city),
        "mubawab":      lambda: src_mubawab(name, city),
        "rne":          lambda: src_rne(name, city, rne_id),
        "crawler":      lambda: src_contact_crawler(name, city),
    }

    results = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {executor.submit(fn): key for key, fn in tasks.items()}

        # Attendre max 25 secondes — les futures non terminés sont ignorés
        done, not_done = wait(future_to_key.keys(), timeout=25,
                              return_when=ALL_COMPLETED)

        # Annuler les futures en attente (non démarrés)
        for f in not_done:
            f.cancel()

        # Récupérer les résultats des futures terminés
        for future in done:
            try:
                data, source = future.result(timeout=1)
                if data:
                    data["source"] = source
                    results.append(data)
            except Exception:
                pass

    return results
