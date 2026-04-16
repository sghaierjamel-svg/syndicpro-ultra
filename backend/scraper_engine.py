"""
SyndicPro Scanner — Moteur de scraping ULTIME v4
────────────────────────────────────────────────
Sources Phase 1 (parallèles) :
  DDG · Bing · Facebook Mobile · Google · Arabe · PJ.tn · Yellow.tn
  Annuaires · RNE ancien · RNE Borne API · LinkedIn · Contact Crawler

Phase 2 (après RNE Borne) :
  Recherche par nom de chaque membre sur DDG + Facebook + Bing

Cache 24h intégré.
Fonctionne pour tout type de société tunisienne.
"""

import re
import requests
import random
import json
from urllib.parse import quote, urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from bs4 import BeautifulSoup
from utils import extract_data

# ─────────────────────────────────────────────────────────────────────────────
#  Constantes
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Android 14; Mobile; rv:125.0) Gecko/125.0 Firefox/125.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1",
]

NOISE_DOMAINS = {
    'google', 'bing', 'duckduckgo', 'yahoo', 'youtube', 'wikipedia',
    'twitter', 'instagram', 'tiktok', 'amazon', 'apple', 'microsoft',
    'whatsapp', 'linkedin', 'facebook', 'gstatic', 'googleapis',
}

# Préfixes à retirer du nom pour obtenir le nom court
SHORT_NAME_PREFIXES = [
    # Syndics
    'SYNDIC RESIDENTIEL', 'SYNDICAT DES RESIDENTS DE LA',
    'SYNDICAT DES COPROPRIETAIRES DE LA RESIDENCE',
    'SYNDICAT DES COPROPRIETAIRES DE LA',
    'SYNDICAT DES COPROPRIETAIRES',
    'SYNDICAT DES RESIDENTS', 'SYNDICAT DES PROPRIETAIRES',
    'SYNDICAT DE COPROPRIETE', 'SYNDICAT DE LA RESIDENCE',
    'SYNDIC DE LA RESIDENCE', 'SYNDIC DE RESIDENCE',
    'NIQABAT MUTASAKINI IQAMAT', 'NIQABAT MALIKI IQAMAT',
    # Sociétés
    'SOCIETE A RESPONSABILITE LIMITEE', 'SOCIETE ANONYME',
    'SOCIETE IMMOBILIERE', 'SOCIETE DE GESTION',
    'SOCIETE BOCHRA SYNDIC',
    'ENTREPRISE INDIVIDUELLE', 'ETABLISSEMENT',
    # Abréviations
    'SARL', 'EURL', 'SAS', 'SA ', 'STE ', 'SOC ',
    # Associations
    'ASSOCIATION DES RESIDENTS', 'ASSOCIATION DE',
    # Génériques
    'SOCIETE', 'SYNDIC', 'SYNDICAT', 'RESIDENCE', 'ASSOCIATION',
    'CABINET', 'CLINIQUE', 'HOTEL', 'RESTAURANT', 'AGENCE',
    'ECOLE', 'INSTITUT', 'CENTRE',
]

RNE_BORNE_SEARCH  = "https://www.registre-entreprises.tn/api/rne-api/front-office/shortEntites"
RNE_BORNE_ENTRIES = "https://www.registre-entreprises.tn/api/rne-borne-api/borne-entries"
RNE_REFERER       = "https://www.registre-entreprises.tn/"


# ─────────────────────────────────────────────────────────────────────────────
#  Utilitaires communs
# ─────────────────────────────────────────────────────────────────────────────

def short_name(name: str) -> str:
    """
    Retire les préfixes juridiques/génériques pour obtenir le nom utile.
    Ex: 'SYNDIC RESIDENTIEL LES VIOLETTES' → 'Les Violettes'
        'SARL TRANSPORT TAREK'             → 'Transport Tarek'
    """
    n = name.upper().strip()
    # Retirer préfixes de la liste (ordre important : du plus long au plus court)
    for prefix in sorted(SHORT_NAME_PREFIXES, key=len, reverse=True):
        pfx = prefix.upper().strip()
        if n.startswith(pfx + ' ') or n == pfx:
            n = n[len(pfx):].strip(' -–')
            break
    # Retirer articles résiduels en début
    for art in ['DE LA ', "DE L'", 'DES ', 'DE ', 'LA ', 'LE ', 'LES ', 'AL ', 'EL ']:
        if n.upper().startswith(art):
            n = n[len(art):]
            break
    return n.title() if n else name.title()


def _headers(referer=None):
    h = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,ar-TN;q=0.8,en-US;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "DNT":             "1",
    }
    if referer:
        h["Referer"] = referer
    return h


def fetch(url: str, timeout=8, retries=1, referer=None) -> str:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_headers(referer),
                             timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code in (429, 503):
                import time as _t; _t.sleep(1.5)
            elif r.status_code in (403, 404, 410):
                return ""
        except Exception:
            pass
    return ""


def _extract_result_urls(html: str) -> list[str]:
    """Extrait les URLs des résultats DDG/Bing depuis la page HTML."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    urls = []
    for a in soup.find_all('a', href=True):
        href = a['href']
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


def _merge(result: dict, d: dict, seen_phones: set, seen_emails: set):
    for p in d.get("phones", []):
        if p not in seen_phones:
            seen_phones.add(p)
            result["phones"].append(p)
    for e in d.get("emails", []):
        if e not in seen_emails:
            seen_emails.add(e)
            result["emails"].append(e)
    result["websites"].extend(d.get("websites", []))


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCES — Phase 1
# ─────────────────────────────────────────────────────────────────────────────

def src_ddg(name, city, short, context=""):
    ctx = f" {context}" if context else ""
    r   = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f"{short} {city}{ctx} téléphone contact",
        f'"{short}" {city} contact email',
        f"{name} {city} téléphone",
        f"{short} {city} email",
    ]:
        d = extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"))
        _merge(r, d, sp, se)
    return r, "ddg"


def src_bing(name, city, short, context=""):
    ctx = f" {context}" if context else ""
    r   = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f"{short} {city}{ctx} téléphone",
        f"site:facebook.com \"{short}\" {city}",
        f'"{short}" {city} contact email',
        f"{short} {city} email",
    ]:
        d = extract_data(fetch(
            f"https://www.bing.com/search?q={quote(q)}&cc=TN&setlang=fr",
            referer="https://www.bing.com/"
        ))
        _merge(r, d, sp, se)
    return r, "bing"


def src_facebook_mobile(name, city, short, context=""):
    """mbasic.facebook.com — version scrapable sans navigateur."""
    ctx = f" {context}" if context else ""
    r   = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()

    for q in [
        f"{short} {city}",
        f"{short}{ctx} {city}",
        f"{name} {city}",
    ]:
        html = fetch(f"https://mbasic.facebook.com/search/top/?q={quote(q)}",
                     referer="https://mbasic.facebook.com/", timeout=10)
        d = extract_data(html)
        _merge(r, d, sp, se)
        if r["phones"] or r["emails"]:
            break

    # Pages Facebook
    if not r["phones"] and not r["emails"]:
        q    = quote(f"{short}{ctx} {city}")
        html = fetch(f"https://mbasic.facebook.com/search/pages/?q={q}",
                     referer="https://mbasic.facebook.com/")
        if html:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all('a', href=True)[:6]:
                href = a['href']
                if '/pages/' in href or ('facebook.com/' in href and 'search' not in href):
                    page_url = href if href.startswith('http') else f"https://mbasic.facebook.com{href}"
                    d = extract_data(fetch(page_url, referer="https://mbasic.facebook.com/"))
                    _merge(r, d, sp, se)
                    if r["phones"] or r["emails"]:
                        break
    return r, "facebook"


def src_google(name, city, short, context=""):
    ctx = f" {context}" if context else ""
    r   = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f"{short}{ctx} {city} téléphone contact",
        f'"{short}" {city} email contact',
        f"{short} {city}",
    ]:
        try:
            resp = requests.get(
                f"https://www.google.com/search?q={quote(q)}&hl=fr&gl=tn&num=10",
                headers=_headers("https://www.google.com/"), timeout=8
            )
            if resp.status_code == 200:
                _merge(r, extract_data(resp.text), sp, se)
        except Exception:
            pass
        if r["phones"] or r["emails"]:
            break
    return r, "google"


def src_arabic(name, city, short, context=""):
    """Recherche en arabe + translittération."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f"{short} {city} هاتف",
        f"{name} {city} تونس هاتف بريد",
        f"{short} تونس هاتف",
    ]:
        d = extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"))
        _merge(r, d, sp, se)
    return r, "arabic"


def src_pj_tn(name, city, short, context=""):
    """Pages Jaunes Tunisie."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [short, name]:
        d = extract_data(fetch(
            f"https://www.pj.tn/search?what={quote(q)}&where={quote(city)}",
            referer="https://www.pj.tn/"
        ))
        _merge(r, d, sp, se)
        if r["phones"] or r["emails"]:
            break
    return r, "pagesjaunes"


def src_yellow_tn(name, city, short, context=""):
    """Yellow Pages Tunisia (yellow.tn)."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [short, name]:
        for url in [
            f"https://www.yellow.tn/en/search?term={quote(q)}&region={quote(city)}",
            f"https://www.yellow.tn/fr/search?term={quote(q)}&region={quote(city)}",
        ]:
            d = extract_data(fetch(url, referer="https://www.yellow.tn/"))
            _merge(r, d, sp, se)
            if r["phones"] or r["emails"]:
                return r, "yellow_tn"
    return r, "yellow_tn"


def src_annuaire(name, city, short, context=""):
    """Annuaires tunisiens divers."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for url in [
        f"http://www.annuaire.com.tn/search?q={quote(short)}+{quote(city)}",
        f"https://www.tayara.tn/search/?q={quote(short)}+{quote(city)}",
        f"https://tn.kompass.com/recherche/?search={quote(short)}&city={quote(city)}",
    ]:
        d = extract_data(fetch(url, timeout=7))
        _merge(r, d, sp, se)
    return r, "annuaires"


def src_linkedin(name, city, short, context=""):
    """LinkedIn via DDG — trouve les pages entreprises avec email de contact."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f'site:linkedin.com/company "{short}" {city}',
        f'site:linkedin.com "{short}" tunisie contact',
    ]:
        d = extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"))
        _merge(r, d, sp, se)
    return r, "linkedin"


def src_rne_old(name, city, rne_id):
    """RNE.tn (ancien site) — accès direct si ID disponible."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    if rne_id:
        d = extract_data(fetch(f"https://www.registre.tn/fr/societe/{rne_id}", timeout=10))
        _merge(r, d, sp, se)
        if r["phones"] or r["emails"]:
            return r, "rne"
    q    = quote(f"{name} {city}")
    d    = extract_data(fetch(f"https://www.registre.tn/fr/recherche?q={q}",
                              referer="https://www.registre.tn/"))
    _merge(r, d, sp, se)
    return r, "rne"


def src_contact_crawler(name, city, short, context=""):
    """
    Crawl contact — trouve les URLs de la société et visite /contact, /a-propos, etc.
    """
    ctx = f" {context}" if context else ""
    html = fetch(f"https://lite.duckduckgo.com/lite/?q={quote(short + ' ' + city + ctx)}")
    urls = _extract_result_urls(html) or \
           _extract_result_urls(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(name + ' ' + city)}"))

    r   = {"phones": [], "emails": [], "websites": []}
    seen = set()

    for base_url in urls[:5]:
        for path in ['/contact', '/nous-contacter', '/contactez-nous',
                     '/coordonnees', '/a-propos', '/about', '/']:
            page = fetch(base_url + path, timeout=7, referer=base_url)
            if not page:
                continue
            d = extract_data(page)
            r["websites"].extend(d.get("websites", []))
            for p in d["phones"]:
                if p not in seen:
                    seen.add(p); r["phones"].append(p)
            for e in d["emails"]:
                if e not in seen:
                    seen.add(e); r["emails"].append(e)
            if d["phones"] or d["emails"]:
                break

    return r, "crawler"


# ─────────────────────────────────────────────────────────────────────────────
#  RNE Borne API (Phase 1 + prépare la Phase 2)
# ─────────────────────────────────────────────────────────────────────────────

def _rne_member(nom, qualite):
    nom = (nom or "").strip()
    return {"nom": nom, "qualite": qualite} if nom else None


def _rne_score(candidate: dict, name: str, city: str) -> float:
    """
    Score how well a RNE entry matches our query (name + city).
    Higher = better match. Used to pick the right entry when multiple results exist.
    """
    denom = (candidate.get("denominationLatin") or "").upper()
    gov   = (candidate.get("gouvernorat") or candidate.get("ville") or
             candidate.get("region") or "").upper()
    n  = name.upper()
    cy = city.upper()

    # Remove all common prefixes to get the core name for comparison
    core_n = n
    for pfx in SHORT_NAME_PREFIXES:
        pfx_up = pfx.upper()
        if core_n.startswith(pfx_up + ' ') or core_n == pfx_up:
            core_n = core_n[len(pfx_up):].strip(' -–')
            break

    core_d = denom
    for pfx in SHORT_NAME_PREFIXES:
        pfx_up = pfx.upper()
        if core_d.startswith(pfx_up + ' ') or core_d == pfx_up:
            core_d = core_d[len(pfx_up):].strip(' -–')
            break

    score = 0.0

    # Exact core name match (highest priority)
    if core_n and core_n in core_d:
        score += 80
    elif core_n and core_d in core_n:
        score += 60

    # Word overlap between core names
    n_words = set(core_n.split()) - {'DE', 'LA', 'LE', 'LES', 'DES', 'AL', 'EL'}
    d_words = set(core_d.split()) - {'DE', 'LA', 'LE', 'LES', 'DES', 'AL', 'EL'}
    if n_words and d_words:
        overlap = n_words & d_words
        score += len(overlap) * 15

    # City match in gouvernorat/region field
    if cy and cy in gov:
        score += 40
    # City appears in denomination
    if cy and cy in denom:
        score += 20

    return score


def src_rne_borne(name, city, short):
    """
    API publique registre-entreprises.tn.
    Retourne tous les membres du bureau (Président, SG, Trésorier, Responsable),
    l'adresse officielle et l'identifiant RNE.
    Lance AUSSI une recherche contact par nom de chaque membre.
    """
    result = {
        "phones": [], "emails": [], "websites": [],
        "president": "", "address": "", "rne_id_found": "",
        "members": [], "denom_latin": "",
    }

    def _api_get(url, params=None, timeout=8):
        try:
            r = requests.get(url, params=params,
                             headers=_headers(RNE_REFERER), timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None

    # ── 1. Rechercher dans shortEntites — essayer plusieurs termes ────────────
    # Essayer d'abord le short name, puis le nom complet, puis juste les mots-clés
    registres = []
    for search_term in [short, name]:
        data = _api_get(RNE_BORNE_SEARCH, {"denominationLatin": search_term, "size": 20})
        if data:
            r = data.get("registres", [])
            if r:
                registres = r
                break

    if not registres:
        return result, "rne_borne"

    # ── 2. Choisir le meilleur résultat (scoring nom + ville) ─────────────────
    scored = [(entry, _rne_score(entry, name, city)) for entry in registres]
    scored.sort(key=lambda x: x[1], reverse=True)
    best = scored[0][0]

    id_unique    = best.get("identifiantUnique", "")
    denom_latin  = best.get("denominationLatin", short)
    result["rne_id_found"] = id_unique
    result["denom_latin"]  = denom_latin

    # ── 3. Trouver l'entrée borne ─────────────────────────────────────────────
    bornes_data = _api_get(RNE_BORNE_ENTRIES, {"denominationLatin": denom_latin, "size": 20})
    if not bornes_data:
        return result, "rne_borne"

    bornes = bornes_data.get("bornes", [])
    # Priorité : correspondance exacte sur identifiantUnique → sinon meilleur score
    borne_id = next(
        (b["id"] for b in bornes if b.get("identifiantUnique") == id_unique),
        None
    )
    if not borne_id and bornes:
        # Si pas de correspondance exacte, scorer aussi les bornes
        scored_b = [(b, _rne_score(b, name, city)) for b in bornes]
        scored_b.sort(key=lambda x: x[1], reverse=True)
        borne_id = scored_b[0][0]["id"]
    if not borne_id:
        return result, "rne_borne"

    # ── 4. Détails complets ────────────────────────────────────────────────────
    det = _api_get(f"{RNE_BORNE_ENTRIES}/{borne_id}", timeout=8)
    if not det:
        return result, "rne_borne"

    members = [m for m in [
        _rne_member(det.get("nomPresident"),   "Président"),
        _rne_member(det.get("nomSg"),          "Secrétaire Général"),
        _rne_member(det.get("nomTresorier"),   "Trésorier"),
        _rne_member(
            det.get("nomResponsable"),
            (det.get("qualiteResponsable") or "Responsable").strip()
        ),
        _rne_member(det.get("representantJuridFr"), "Représentant légal"),
    ] if m]
    # Dédupliquer par nom
    seen_noms = set()
    unique_members = []
    for m in members:
        if m["nom"] not in seen_noms:
            seen_noms.add(m["nom"])
            unique_members.append(m)

    result["members"]   = unique_members
    result["president"] = unique_members[0]["nom"] if unique_members else ""
    result["address"]   = (det.get("adresse") or "").strip()

    # ── 5. Chercher les contacts des membres ──────────────────────────────────
    sp, se = set(), set()
    for member in unique_members[:3]:
        nom = member["nom"]
        if not nom or len(nom) < 4:
            continue
        for q in [
            f'"{nom}" {city} email téléphone',
            f'"{nom}" "{denom_latin}"',
            f'"{nom}" {city} contact',
            f'"{nom}" email',
        ]:
            d = extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}", timeout=6))
            _merge(result, d, sp, se)
            if result["phones"] or result["emails"]:
                break

        # Facebook : chercher la page personnelle du membre
        fb = fetch(f"https://mbasic.facebook.com/search/people/?q={quote(nom)}",
                   referer="https://mbasic.facebook.com/", timeout=6)
        _merge(result, extract_data(fb), sp, se)

        # Bing : souvent meilleur pour les profils personnels
        bing_html = fetch(
            f"https://www.bing.com/search?q={quote(nom + ' ' + city + ' email téléphone')}&cc=TN",
            referer="https://www.bing.com/"
        )
        _merge(result, extract_data(bing_html), sp, se)

    return result, "rne_borne"


# ─────────────────────────────────────────────────────────────────────────────
#  ORCHESTRATEUR
# ─────────────────────────────────────────────────────────────────────────────

def scrape_all(name: str, city: str, rne_id: str = "", context: str = "") -> list:
    """
    Lance toutes les sources en parallèle.
    context : type d'activité optionnel ('syndic', 'restaurant', etc.)
    """
    # Vérifier le cache
    from db import get_cache, set_cache
    cached = get_cache(name, city)
    if cached is not None:
        cached["from_cache"] = True
        return [cached]

    sn = short_name(name)

    # ── Phase 1 : toutes sources en parallèle ────────────────────────────────
    phase1 = {
        "ddg":       lambda: src_ddg(name, city, sn, context),
        "bing":      lambda: src_bing(name, city, sn, context),
        "facebook":  lambda: src_facebook_mobile(name, city, sn, context),
        "google":    lambda: src_google(name, city, sn, context),
        "arabic":    lambda: src_arabic(name, city, sn, context),
        "pj_tn":     lambda: src_pj_tn(name, city, sn, context),
        "yellow_tn": lambda: src_yellow_tn(name, city, sn, context),
        "annuaires": lambda: src_annuaire(name, city, sn, context),
        "linkedin":  lambda: src_linkedin(name, city, sn, context),
        "rne_old":   lambda: src_rne_old(name, city, rne_id),
        "rne_borne": lambda: src_rne_borne(name, city, sn),
        "crawler":   lambda: src_contact_crawler(name, city, sn, context),
    }

    results      = []
    rne_borne_r  = None

    with ThreadPoolExecutor(max_workers=12) as ex:
        fmap = {ex.submit(fn): key for key, fn in phase1.items()}
        done, _ = wait(fmap.keys(), timeout=25, return_when=ALL_COMPLETED)
        for f in _:
            f.cancel()
        for future in done:
            try:
                data, source = future.result(timeout=1)
                if data:
                    data["source"] = source
                    results.append(data)
                    if source == "rne_borne":
                        rne_borne_r = data
            except Exception:
                pass

    # ── Phase 2 : recherche personnelle des membres si RNE a trouvé des gens ─
    if rne_borne_r and rne_borne_r.get("members"):
        members     = rne_borne_r["members"]
        denom_latin = rne_borne_r.get("denom_latin", sn)

        phase2_tasks = {}
        for i, member in enumerate(members[:4]):
            nom = member.get("nom", "")
            if not nom or len(nom) < 4:
                continue
            key = f"member_{i}"
            phase2_tasks[key] = lambda n=nom: _src_member_personal(n, city, denom_latin)

        if phase2_tasks:
            with ThreadPoolExecutor(max_workers=len(phase2_tasks)) as ex2:
                fmap2 = {ex2.submit(fn): key for key, fn in phase2_tasks.items()}
                done2, _ = wait(fmap2.keys(), timeout=8, return_when=ALL_COMPLETED)
                for f in _:
                    f.cancel()
                for future in done2:
                    try:
                        data, source = future.result(timeout=1)
                        if data and (data.get("phones") or data.get("emails")):
                            data["source"] = source
                            results.append(data)
                    except Exception:
                        pass

    return results


def _src_member_personal(nom: str, city: str, denom_latin: str):
    """
    Recherche intensive du contact personnel d'un membre :
    email perso (@gmail, @yahoo, @hotmail...), téléphone.
    """
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()

    queries = [
        # DDG — email personnel
        f'"{nom}" email gmail yahoo',
        f'"{nom}" "{denom_latin}" email',
        f'"{nom}" {city} email contact',
        # Bing — souvent meilleur pour profils personnels
        f'"{nom}" email téléphone tunisie',
    ]

    engines = [
        lambda q: fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"),
        lambda q: fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"),
        lambda q: fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"),
        lambda q: fetch(f"https://www.bing.com/search?q={quote(q)}&cc=TN",
                        referer="https://www.bing.com/"),
    ]

    for q, engine in zip(queries, engines):
        _merge(r, extract_data(engine(q)), sp, se)
        if r["phones"] or r["emails"]:
            break

    # Facebook people search
    fb = fetch(f"https://mbasic.facebook.com/search/people/?q={quote(nom)}",
               referer="https://mbasic.facebook.com/", timeout=7)
    _merge(r, extract_data(fb), sp, se)

    return r, f"member_contact"
