"""
SyndicPro Scanner — Moteur de scraping ULTIME v3
Sources parallèles : DDG, Bing, Facebook Mobile, mbasic.facebook.com,
Google, pj.tn, annuaire.com.tn, RNE.tn, tayara.tn, + contact crawler.
Normalisation du nom RNE → nom court pour de meilleures recherches.
"""

import re
import requests
import random
from urllib.parse import quote, urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from bs4 import BeautifulSoup
from utils import extract_data

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0",
]

NOISE_DOMAINS = {
    'google', 'bing', 'duckduckgo', 'yahoo', 'youtube',
    'wikipedia', 'twitter', 'instagram', 'tiktok',
    'amazon', 'apple', 'microsoft', 'whatsapp',
}

# Préfixes juridiques à retirer du nom RNE pour obtenir le nom court
RNE_PREFIXES = [
    'SYNDIC RESIDENTIEL', 'SYNDICAT DES RESIDENTS DE LA',
    'SYNDICAT DES RESIDENTS', 'SYNDICAT DES PROPRIETAIRES',
    'SYNDICAT DE COPROPRIETE', 'SYNDICAT DE LA RESIDENCE',
    'SYNDIC DE LA RESIDENCE', 'SYNDIC DE RESIDENCE',
    'SOCIETE IMMOBILIERE', 'SOCIETE DE GESTION',
    'ASSOCIATION DES RESIDENTS', 'SOCIETE BOCHRA SYNDIC',
    'SOCIETE', 'SYNDIC', 'SYNDICAT', 'RESIDENCE', 'ASSOCIATION',
]


def short_name(name):
    """
    Extrait le nom court depuis le nom juridique RNE.
    Ex: 'SYNDIC RESIDENTIEL LES VIOLETTES' → 'Les Violettes'
    """
    n = name.upper().strip()
    for prefix in RNE_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):].strip()
            break
    # Retirer les mots parasites en début
    for word in ['DE LA', 'DE L\'', 'DES', 'DE', 'LA', 'LE', 'LES', 'AL', 'EL']:
        if n.startswith(word + ' '):
            pass  # garder — fait partie du nom
    return n.title() if n else name.title()


def _headers(referer=None):
    h = {
        "User-Agent":              random.choice(USER_AGENTS),
        "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":         "fr-FR,fr;q=0.9,ar-TN;q=0.8,en-US;q=0.7",
        "Accept-Encoding":         "gzip, deflate, br",
        "Connection":              "keep-alive",
        "DNT":                     "1",
        "Cache-Control":           "max-age=0",
    }
    if referer:
        h["Referer"] = referer
    return h


def fetch(url, timeout=8, retries=1, referer=None):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(referer),
                             timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                import time; import time as t; t.sleep(1)
            elif r.status_code in (403, 404, 410):
                return ""
        except Exception:
            pass
    return ""


def _extract_result_urls(html):
    """Extrait les URLs des résultats DDG/Bing depuis la page HTML."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        # DDG Lite redirige via //duckduckgo.com/l/?uddg=...
        if 'uddg=' in href:
            try:
                href = unquote(href.split('uddg=')[1].split('&')[0])
            except Exception:
                continue
        if not href.startswith('http'):
            continue
        parsed = urlparse(href)
        domain = parsed.netloc.lower().replace('www.', '')
        base_domain = domain.split('.')[0]
        if base_domain in NOISE_DOMAINS:
            continue
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in urls:
            urls.append(base)
    return urls[:6]


# ══════════════════════════════════════════════════════════════════════════════
#  SOURCES
# ══════════════════════════════════════════════════════════════════════════════

def src_ddg(name, city, short):
    """DDG Lite — nom complet et nom court."""
    results = {"phones": [], "emails": [], "websites": []}
    for q_str in [
        f"{short} {city} syndic téléphone contact",
        f'"{short}" {city} contact',
        f"{name} {city} syndic téléphone",
    ]:
        html = fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q_str)}")
        d = extract_data(html)
        results["phones"].extend(d["phones"])
        results["emails"].extend(d["emails"])
        results["websites"].extend(d["websites"])
    # Dédoublonner
    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "ddg"


def src_bing(name, city, short):
    """Bing — très bon pour Facebook et les annuaires."""
    results = {"phones": [], "emails": [], "websites": []}
    for q_str in [
        f"{short} {city} syndic téléphone",
        f"site:facebook.com {short} {city}",
        f'"{short}" {city} contact email',
    ]:
        html = fetch(f"https://www.bing.com/search?q={quote(q_str)}&cc=TN",
                     referer="https://www.bing.com/")
        d = extract_data(html)
        results["phones"].extend(d["phones"])
        results["emails"].extend(d["emails"])
        results["websites"].extend(d["websites"])
    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "bing"


def src_facebook_mobile(name, city, short):
    """
    mbasic.facebook.com — seule version de Facebook scrappable sans browser.
    Très efficace pour les syndics tunisiens qui n'ont que FB comme présence.
    """
    results = {"phones": [], "emails": [], "websites": []}
    for q_str in [
        f"{short} {city}",
        f"{short} syndic {city}",
        f"résidence {short} {city}",
    ]:
        url = f"https://mbasic.facebook.com/search/top/?q={quote(q_str)}"
        html = fetch(url, referer="https://mbasic.facebook.com/", timeout=10)
        d = extract_data(html)
        results["phones"].extend(d["phones"])
        results["emails"].extend(d["emails"])
        if d["phones"] or d["emails"]:
            break

    # Si on a trouvé une page, visiter les liens de résultat
    if not results["phones"]:
        q = quote(f"{short} syndic {city}")
        html = fetch(f"https://mbasic.facebook.com/search/pages/?q={q}",
                     referer="https://mbasic.facebook.com/")
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all('a', href=True)[:5]:
                href = a['href']
                if '/pages/' in href or ('facebook.com/' in href and 'search' not in href):
                    page_url = href if href.startswith('http') else f"https://mbasic.facebook.com{href}"
                    page_html = fetch(page_url, referer="https://mbasic.facebook.com/", timeout=8)
                    d = extract_data(page_html)
                    results["phones"].extend(d["phones"])
                    results["emails"].extend(d["emails"])
                    if d["phones"] or d["emails"]:
                        break

    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "facebook"


def src_google(name, city, short):
    """Google Search."""
    results = {"phones": [], "emails": [], "websites": []}
    for q_str in [
        f"{short} syndic {city} téléphone",
        f'"{short}" {city} contact',
    ]:
        try:
            r = requests.get(
                f"https://www.google.com/search?q={quote(q_str)}&hl=fr&gl=tn",
                headers=_headers("https://www.google.com/"), timeout=8
            )
            if r.status_code == 200:
                d = extract_data(r.text)
                results["phones"].extend(d["phones"])
                results["emails"].extend(d["emails"])
        except Exception:
            pass
    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "google"


def src_arabic(name, city, short):
    """Recherche en arabe — essentiel pour Tunisie."""
    results = {"phones": [], "emails": [], "websites": []}
    for q_str in [
        f"{short} {city} نقابة ملاك هاتف",
        f"{name} {city} تونس هاتف",
    ]:
        html = fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q_str)}")
        d = extract_data(html)
        results["phones"].extend(d["phones"])
        results["emails"].extend(d["emails"])
    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "arabic"


def src_pj_tn(name, city, short):
    """Pages Jaunes Tunisie."""
    for q in [short, name]:
        html = fetch(
            f"https://www.pj.tn/search?what={quote(q)}&where={quote(city)}",
            referer="https://www.pj.tn/"
        )
        d = extract_data(html)
        if d["phones"] or d["emails"]:
            return d, "pagesjaunes"
    return extract_data(""), "pagesjaunes"


def src_annuaire(name, city, short):
    """Annuaires tunisiens divers."""
    results = {"phones": [], "emails": [], "websites": []}
    urls = [
        f"http://www.annuaire.com.tn/search?q={quote(short)}+{quote(city)}",
        f"https://www.tayara.tn/search/?q={quote(short)}+{quote(city)}",
    ]
    for url in urls:
        d = extract_data(fetch(url, timeout=7))
        results["phones"].extend(d["phones"])
        results["emails"].extend(d["emails"])
    results["phones"] = list(dict.fromkeys(results["phones"]))
    results["emails"] = list(dict.fromkeys(results["emails"]))
    return results, "annuaires"


def src_rne(name, city, rne_id):
    """RNE.tn — accès direct si ID disponible."""
    if rne_id:
        html = fetch(f"https://www.registre.tn/fr/societe/{rne_id}", timeout=10)
        d = extract_data(html)
        if d["phones"] or d["emails"]:
            return d, "rne"
    q = quote(f"{name} {city}")
    html = fetch(f"https://www.registre.tn/fr/recherche?q={q}",
                 referer="https://www.registre.tn/")
    return extract_data(html), "rne"


RNE_BORNE_SEARCH = "https://www.registre-entreprises.tn/api/rne-api/front-office/shortEntites"
RNE_BORNE_ENTRIES = "https://www.registre-entreprises.tn/api/rne-borne-api/borne-entries"


def src_rne_borne(name, city, short):
    """
    RNE registre-entreprises.tn — API publique gratuite.
    Retourne le président, SG, trésorier, adresse officielle + ID RNE.
    Ces infos servent ensuite à enrichir les recherches de contact.
    """
    result = {"phones": [], "emails": [], "websites": [],
              "president": "", "address": "", "rne_id_found": ""}

    ref = "https://www.registre-entreprises.tn/"

    def _search(q):
        try:
            r = requests.get(RNE_BORNE_SEARCH, params={"denominationLatin": q},
                             headers=_headers(ref), timeout=10)
            if r.status_code == 200:
                return r.json().get("registres", [])
        except Exception:
            pass
        return []

    # Essayer nom court, puis nom complet
    registres = _search(short) or _search(name)
    if not registres:
        return result, "rne_borne"

    best = registres[0]
    id_unique = best.get("identifiantUnique", "")
    denom_latin = best.get("denominationLatin", short)
    result["rne_id_found"] = id_unique

    # Chercher l'entrée borne pour obtenir le nom du président
    try:
        r2 = requests.get(RNE_BORNE_ENTRIES, params={"denominationLatin": denom_latin},
                          headers=_headers(ref), timeout=10)
        if r2.status_code == 200:
            bornes = r2.json().get("bornes", [])
            # Préférer l'entrée avec le bon identifiantUnique
            borne_id = None
            for b in bornes:
                if b.get("identifiantUnique") == id_unique:
                    borne_id = b.get("id")
                    break
            if not borne_id and bornes:
                borne_id = bornes[0].get("id")

            if borne_id:
                r3 = requests.get(f"{RNE_BORNE_ENTRIES}/{borne_id}",
                                  headers=_headers(ref), timeout=10)
                if r3.status_code == 200:
                    det = r3.json()
                    president = (det.get("nomPresident") or
                                 det.get("nomResponsable") or
                                 det.get("nomSg") or
                                 det.get("nomTresorier") or "")
                    result["president"] = president.strip()
                    result["address"] = (det.get("adresse") or "").strip()
    except Exception:
        pass

    return result, "rne_borne"


def src_contact_crawler(name, city, short):
    """
    Trouve les URLs dans les résultats de recherche,
    puis visite leurs pages /contact pour extraire les coordonnées.
    """
    # Chercher les URLs avec DDG
    html = fetch(f"https://lite.duckduckgo.com/lite/?q={quote(short + ' ' + city + ' syndic')}")
    urls = _extract_result_urls(html)

    if not urls:
        html2 = fetch(f"https://lite.duckduckgo.com/lite/?q={quote(name + ' ' + city)}")
        urls = _extract_result_urls(html2)

    merged = {"phones": [], "emails": [], "websites": []}
    seen = set()

    for base_url in urls[:5]:
        for path in ['/contact', '/nous-contacter', '/contactez-nous',
                     '/coordonnees', '/a-propos', '/']:
            page = fetch(base_url + path, timeout=7, referer=base_url)
            if not page:
                continue
            d = extract_data(page)
            for p in d["phones"]:
                if p not in seen:
                    seen.add(p)
                    merged["phones"].append(p)
            for e in d["emails"]:
                if e not in seen:
                    seen.add(e)
                    merged["emails"].append(e)
            merged["websites"].extend(d["websites"])
            if d["phones"] or d["emails"]:
                break

    return merged, "crawler"


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATEUR PARALLÈLE
# ══════════════════════════════════════════════════════════════════════════════

def scrape_all(name, city, rne_id=""):
    """
    Lance toutes les sources EN PARALLÈLE.
    Normalise d'abord le nom pour de meilleures recherches.
    """
    sn = short_name(name)  # ex: "SYNDIC LES VIOLETTES" → "Les Violettes"

    tasks = {
        "ddg":       lambda: src_ddg(name, city, sn),
        "bing":      lambda: src_bing(name, city, sn),
        "facebook":  lambda: src_facebook_mobile(name, city, sn),
        "google":    lambda: src_google(name, city, sn),
        "arabic":    lambda: src_arabic(name, city, sn),
        "pj_tn":     lambda: src_pj_tn(name, city, sn),
        "annuaires": lambda: src_annuaire(name, city, sn),
        "rne":       lambda: src_rne(name, city, rne_id),
        "rne_borne": lambda: src_rne_borne(name, city, sn),
        "crawler":   lambda: src_contact_crawler(name, city, sn),
    }

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_map = {executor.submit(fn): key for key, fn in tasks.items()}
        done, not_done = wait(future_map.keys(), timeout=28, return_when=ALL_COMPLETED)
        for f in not_done:
            f.cancel()
        for future in done:
            try:
                data, source = future.result(timeout=1)
                if data:
                    data["source"] = source
                    results.append(data)
            except Exception:
                pass

    return results
