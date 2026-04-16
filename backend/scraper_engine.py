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
import time
import threading
import base64
import os
import logging
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
RNE_ENTITES       = "https://www.registre-entreprises.tn/api/rne-api/front-office/entites"
RNE_AUTH_URL      = "https://www.registre-entreprises.tn/api/rne-auth-api/oauth/token"
RNE_REFERER       = "https://www.registre-entreprises.tn/"

# ── Token RNE (chargé depuis l'env, rafraîchi automatiquement) ────────────────
_rne_token_lock  = threading.Lock()
_rne_token_cache = {"token": None, "expires_at": 0}


# ─────────────────────────────────────────────────────────────────────────────
#  Utilitaires communs
# ─────────────────────────────────────────────────────────────────────────────

def _deaccent(s: str) -> str:
    """Supprime les accents : résidence → residence, Méziàna → Meziana."""
    import unicodedata
    return unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('ascii')


# Translittération arabe → latin (noms tunisiens courants)
_AR_MAP = {
    'ا':'a','أ':'a','إ':'i','آ':'a','ب':'b','ت':'t','ث':'th','ج':'j',
    'ح':'h','خ':'kh','د':'d','ذ':'z','ر':'r','ز':'z','س':'s','ش':'sh',
    'ص':'s','ض':'d','ط':'t','ظ':'z','ع':'','غ':'gh','ف':'f','ق':'k',
    'ك':'k','ل':'l','م':'m','ن':'n','ه':'h','و':'ou','ي':'i','ى':'a',
    'ة':'a','ّ':'','َ':'a','ِ':'i','ُ':'ou','ً':'','ٍ':'','ٌ':'',
}

def _ar_to_latin(text: str) -> str:
    """
    Translittération arabe → latin pour les noms tunisiens.
    Ex: 'الاسعد الزيتوني' → 'El Assaad Zitouni'
        'محمد علي الجبري' → 'Mohamed Ali Jebri'
        'جمال الصغير'     → 'Jamal Saghir'
    """
    # Corrections de mots courants avant translittération
    _FIXES = {
        'بن': 'ben', 'بنت': 'bent', 'ابن': 'ibn',
        'محمد': 'Mohamed', 'أحمد': 'Ahmed', 'علي': 'Ali',
        'عمر': 'Omar', 'حسن': 'Hassen', 'حسين': 'Houcine',
        'يوسف': 'Youssef', 'خالد': 'Khaled', 'سمير': 'Samir',
        'كريم': 'Karim', 'نادر': 'Nader', 'هشام': 'Hichem',
        'فاطمة': 'Fatma', 'منى': 'Mona', 'سارة': 'Sara',
        'رضا': 'Ridha', 'نجيب': 'Nejib', 'توفيق': 'Taoufik',
    }
    result = []
    for word in text.split():
        if word in _FIXES:
            result.append(_FIXES[word])
            continue
        # Garder "El" si article défini ال
        prefix = ''
        w = word
        if w.startswith('ال'):
            prefix = 'El '
            w = w[2:]
        latin = ''.join(_AR_MAP.get(c, '') for c in w)
        latin = re.sub(r'[^\x00-\x7F]+', '', latin).strip()
        # Ajouter 'a' entre deux consonnes consécutives
        latin = re.sub(r'([bcdfghjklmnpqrstvwxz])([bcdfghjklmnpqrstvwxz])', r'\1a\2', latin)
        if latin:
            result.append((prefix + latin).strip().capitalize())
    return ' '.join(result)

def _is_arabic(s: str) -> bool:
    return bool(s) and any('\u0600' <= c <= '\u06ff' for c in s)


def short_name(name: str) -> str:
    """
    Retire les préfixes juridiques/génériques pour obtenir le nom utile.
    Ex: 'SYNDIC RESIDENTIEL LES VIOLETTES' → 'Les Violettes'
        'résidence meziana'                → 'Meziana'
        'SARL TRANSPORT TAREK'             → 'Transport Tarek'
    Gère les accents : 'RÉSIDENCE' matche le préfixe 'RESIDENCE'.
    """
    # Normaliser sans accents pour la comparaison avec les préfixes ASCII
    n = _deaccent(name).upper().strip()
    for prefix in sorted(SHORT_NAME_PREFIXES, key=len, reverse=True):
        pfx = _deaccent(prefix).upper().strip()
        if n.startswith(pfx + ' ') or n == pfx:
            n = n[len(pfx):].strip(' -–')
            break
    for art in ['DE LA ', "DE L'", 'DES ', 'DE ', 'LA ', 'LE ', 'LES ', 'AL ', 'EL ']:
        if n.startswith(art):
            n = n[len(art):]
            break
    return n.title() if n else _deaccent(name).title()


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


def fetch(url: str, timeout=6, retries=0, referer=None) -> str:
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


def src_google_maps(name, city, short, context=""):
    """Google local search — Knowledge Panel + Maps résultats contiennent souvent le numéro."""
    r   = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [
        f"{short} {city} tunisie",
        f"{name} {city} تونس",
    ]:
        # Recherche Google locale
        html = fetch(
            f"https://www.google.com/search?q={quote(q)}&hl=fr&gl=tn&num=5",
            referer="https://www.google.com/", timeout=7
        )
        if html:
            _merge(r, extract_data(html), sp, se)
            if r["phones"] or r["emails"]:
                break
    return r, "google_maps"


def src_tayara(name, city, short, context=""):
    """Tayara.tn — annonces immobilières mentionnent souvent le contact du syndic."""
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()
    for q in [short, f"{short} {city}"]:
        html = fetch(
            f"https://www.tayara.tn/ads/c/Immobilier/?q={quote(q)}",
            referer="https://www.tayara.tn/", timeout=7
        )
        d = extract_data(html)
        # Filtrer le numéro de Tayara lui-même (+216 95 256 096)
        d["phones"] = [p for p in d.get("phones", []) if p != "+21695256096"]
        _merge(r, d, sp, se)
        if r["phones"] or r["emails"]:
            break
    return r, "tayara"


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
    """Crée un membre RNE. Conserve le nom arabe et ajoute un flag."""
    nom = (nom or "").strip()
    if not nom:
        return None
    return {
        "nom":    nom,
        "nom_ar": nom if _is_arabic(nom) else "",
        "qualite": qualite,
    }


_STOP_WORDS = {'DE', 'LA', 'LE', 'LES', 'DES', 'AL', 'EL', 'ET', 'DU', 'AU', 'EN', 'UN', 'UNE'}


def _rne_score(denom: str, name: str, city: str) -> float:
    """
    Score une dénomination RNE contre notre requête (nom + ville).
    Applique _deaccent sur TOUT pour que "RÉSIDENCE" == "RESIDENCE".
    """
    # Normalisation : tout en ASCII majuscule sans accents
    d  = _deaccent(denom).upper()
    n  = _deaccent(name).upper()
    cy = _deaccent(city).upper()

    score = 0.0

    # ── Signal le plus fort : ville dans la dénomination ──────────────────
    if cy and cy in d:
        score += 100

    # ── Overlap mot par mot sur le nom complet (sans stop words) ──────────
    # On compare les mots du NOM DE REQUÊTE vs les mots de la DENOMINATION
    # Ex: "RESIDENCE MEZIANA" vs "SYNDIC RESIDENCE MEZIANA" → 2 mots en commun
    n_words = set(n.split()) - _STOP_WORDS
    d_words = set(d.split()) - _STOP_WORDS
    if n_words and d_words:
        overlap   = n_words & d_words
        n_covered = len(overlap) / len(n_words)   # % des mots de la requête trouvés
        score += len(overlap) * 20 + n_covered * 20

    return score


def _rne_fetch_details(rne_id: str, _api_get) -> dict | None:
    """
    Récupère les détails complets d'une entrée RNE à partir de son identifiantUnique.
    Utilise le lookup direct par identifiantUnique (fiable à 100%).
    """
    bornes_data = _api_get(RNE_BORNE_ENTRIES, {"identifiantUnique": rne_id, "size": 5})
    if not bornes_data:
        return None
    bornes = bornes_data.get("bornes", [])
    borne  = next((b for b in bornes if b.get("identifiantUnique") == rne_id),
                  bornes[0] if bornes else None)
    if not borne:
        return None
    return _api_get(f"{RNE_BORNE_ENTRIES}/{borne['id']}", timeout=8)


def get_rne_candidates(name: str, city: str) -> list:
    """
    Retourne tous les candidats RNE correspondant à un nom+ville.
    Utilisé pour la sélection manuelle dans le frontend.
    Chaque item : {rne_id, name_fr, name_ar, score}
    """
    sn = short_name(name)
    seen = {}
    headers_h = _headers(RNE_REFERER)

    for term in [f"{sn} {city}", sn, name]:
        try:
            r = requests.get(RNE_BORNE_SEARCH,
                             params={"denominationLatin": term, "size": 20},
                             headers=headers_h, timeout=8)
            if r.status_code == 200:
                for e in r.json().get("registres", []):
                    uid = e.get("identifiantUnique", "")
                    if uid and uid not in seen:
                        seen[uid] = {
                            "rne_id":  uid,
                            "name_fr": e.get("denominationLatin", ""),
                            "name_ar": e.get("denomination", ""),
                            "score":   _rne_score(e.get("denominationLatin", ""), name, city),
                        }
        except Exception:
            pass

    result = sorted(seen.values(), key=lambda x: x["score"], reverse=True)
    for item in result:
        item.pop("score", None)
    return result


def _get_rne_token() -> str:
    """
    Retourne un Bearer token valide pour l'API RNE.
    - Utilise RNE_TOKEN (env) directement si défini ET valide.
    - Sinon se connecte avec RNE_USERNAME / RNE_PASSWORD (OAuth2 password grant).
    - Cache le token en mémoire pour éviter de se reconnecter à chaque appel.
    """
    global _rne_token_cache

    with _rne_token_lock:
        now = time.time()
        # Token en cache encore valide (marge de 60s)
        if _rne_token_cache["token"] and _rne_token_cache["expires_at"] > now + 60:
            return _rne_token_cache["token"]

        # 1. Token statique depuis l'env (pratique pour le développement)
        static_token = os.environ.get("RNE_TOKEN", "").strip()
        if static_token:
            # On suppose qu'il est valide 8h (opaque token UUID RNE)
            _rne_token_cache = {"token": static_token, "expires_at": now + 8 * 3600}
            return static_token

        # 2. Connexion automatique via credentials
        username = os.environ.get("RNE_USERNAME", "").strip()
        password = os.environ.get("RNE_PASSWORD", "").strip()
        if not username or not password:
            return ""

        try:
            basic = base64.b64encode(b"recgmg:pin").decode()
            r = requests.post(
                RNE_AUTH_URL,
                data={
                    "grant_type": "password",
                    "username":   username,
                    "password":   password,
                },
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type":  "application/x-www-form-urlencoded",
                },
                timeout=10
            )
            if r.status_code == 200:
                d = r.json()
                token   = d.get("access_token", "")
                expires = d.get("expires_in", 3600)
                _rne_token_cache = {"token": token, "expires_at": now + expires}
                logging.info("[RNE] Token rafraîchi avec succès")
                return token
        except Exception as e:
            logging.warning(f"[RNE] Échec login: {e}")
        return ""


def src_rne_entite(rne_id: str):
    """
    API authentifiée registre-entreprises.tn — GET /front-office/entites/{id}
    Retourne l'email officiel de la société (adresseEmailSociete).
    Requiert RNE_TOKEN ou RNE_USERNAME+RNE_PASSWORD dans les variables d'env.
    """
    result = {"phones": [], "emails": [], "websites": []}
    if not rne_id:
        return result, "rne_entite"

    token = _get_rne_token()
    if not token:
        return result, "rne_entite"

    try:
        r = requests.get(
            f"{RNE_ENTITES}/{rne_id}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept":        "application/json",
                "Referer":       RNE_REFERER,
                "User-Agent":    random.choice(USER_AGENTS),
            },
            timeout=8
        )
        if r.status_code == 401:
            # Token expiré — forcer le refresh au prochain appel
            with _rne_token_lock:
                _rne_token_cache["expires_at"] = 0
            logging.warning("[RNE] Token expiré, sera rafraîchi au prochain appel")
            return result, "rne_entite"

        if r.status_code == 200 and r.text:
            d = r.json()
            email = (d.get("adresseEmailSociete") or d.get("email") or "").strip().lower()
            if email and "@" in email:
                result["emails"].append(email)
                logging.info(f"[RNE entite] {rne_id} → email: {email}")

    except Exception as e:
        logging.warning(f"[RNE entite] {rne_id}: {e}")

    return result, "rne_entite"


def src_rne_borne(name, city, short, rne_id=""):
    """
    API publique registre-entreprises.tn.
    - Si rne_id fourni : lookup direct par identifiantUnique (100% fiable)
    - Sinon : recherche par nom + scoring (meilleur candidat automatique)
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

    det          = None
    id_unique    = rne_id
    denom_latin  = short

    if rne_id:
        # ── Voie directe : identifiantUnique connu ─────────────────────────────
        det = _rne_fetch_details(rne_id, _api_get)
        if det:
            denom_latin = (det.get("denominationLatin") or short).strip()
    else:
        # ── Voie indirecte : recherche par nom + scoring ───────────────────────
        all_registres = {}
        for term in [f"{short} {city}", short, name]:
            data = _api_get(RNE_BORNE_SEARCH, {"denominationLatin": term, "size": 20})
            if data:
                for entry in data.get("registres", []):
                    uid = entry.get("identifiantUnique", "")
                    if uid and uid not in all_registres:
                        all_registres[uid] = entry
            # Arrêt anticipé si correspondance ville claire dès la première requête
            if all_registres and term == f"{short} {city}":
                city_matches = [
                    e for e in all_registres.values()
                    if _deaccent(city).upper() in _deaccent(e.get("denominationLatin","")).upper()
                ]
                if city_matches:
                    all_registres = {e["identifiantUnique"]: e for e in city_matches}
                    break

        if not all_registres:
            return result, "rne_borne"

        scored = [(e, _rne_score(e.get("denominationLatin",""), name, city))
                  for e in all_registres.values()]
        best_entry, best_score = max(scored, key=lambda x: x[1])

        # Seuil minimum : éviter de prendre une entité sans rapport
        # (ex: "El Yassamine" quand on cherche "Meziana" → score=30 car seul
        # "RESIDENCE" correspond — ce n'est pas assez)
        if best_score < 40:
            return result, "rne_borne"

        id_unique   = best_entry.get("identifiantUnique", "")
        denom_latin = best_entry.get("denominationLatin", short)
        det         = _rne_fetch_details(id_unique, _api_get)

    if not det:
        return result, "rne_borne"

    result["rne_id_found"] = id_unique
    result["denom_latin"]  = denom_latin

    # ── Extraction des membres ─────────────────────────────────────────────────
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

    seen_noms = set()
    unique_members = []
    for m in members:
        if m["nom"] not in seen_noms:
            seen_noms.add(m["nom"])
            unique_members.append(m)

    result["members"]   = unique_members
    result["president"] = unique_members[0]["nom"] if unique_members else ""
    result["address"]   = (det.get("adresse") or "").strip()
    # Note : la recherche de contacts des membres est faite en Phase 2 (scrape_all)
    # pour ne pas dépasser le timeout de la Phase 1.
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
        "linkedin":     lambda: src_linkedin(name, city, sn, context),
        "google_maps":  lambda: src_google_maps(name, city, sn, context),
        "tayara":       lambda: src_tayara(name, city, sn, context),
        "rne_old":      lambda: src_rne_old(name, city, rne_id),
        "rne_borne":    lambda: src_rne_borne(name, city, sn, rne_id),
        "crawler":      lambda: src_contact_crawler(name, city, sn, context),
    }

    results      = []
    rne_borne_r  = None

    with ThreadPoolExecutor(max_workers=14) as ex:
        fmap = {ex.submit(fn): key for key, fn in phase1.items()}
        done, _ = wait(fmap.keys(), timeout=20, return_when=ALL_COMPLETED)
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

    # ── Phase 1.5 : RNE entite (email officiel) — toujours séquentiel ────────────
    # Sorti de Phase 1 car le token OAuth2 + la requête HTTP dépassaient le timeout
    # de 20s quand le serveur RNE est lent. Ici pas de timeout — garanti d'exécuter.
    effective_rne_id = rne_id or (rne_borne_r.get("rne_id_found") if rne_borne_r else "")
    if effective_rne_id:
        try:
            entite_data, entite_src = src_rne_entite(effective_rne_id)
            if entite_data.get("emails"):
                entite_data["source"] = entite_src
                results.append(entite_data)
                logging.info(f"[Phase 1.5] rne_entite → {entite_data['emails']}")
        except Exception as e:
            logging.warning(f"[Phase 1.5] rne_entite échoué : {e}")

    # ── Phase 2 : recherche personnelle de TOUS les membres RNE ─────────────────
    # Président, Secrétaire Général, Trésorier, Responsable, Représentant légal
    # Chaque membre est cherché en parallèle sur DDG + Bing + Google + Facebook
    if rne_borne_r and rne_borne_r.get("members"):
        members     = rne_borne_r["members"]
        denom_latin = rne_borne_r.get("denom_latin", sn)

        phase2_tasks = {}
        for i, member in enumerate(members[:6]):   # tous les membres, max 6
            nom = member.get("nom", "")
            if not nom or len(nom) < 3:
                continue
            qualite = member.get("qualite", "")
            key = f"member_{i}_{qualite}"
            phase2_tasks[key] = lambda n=nom, q=qualite: _src_member_personal(
                n, city, denom_latin, q
            )

        if phase2_tasks:
            with ThreadPoolExecutor(max_workers=len(phase2_tasks)) as ex2:
                fmap2 = {ex2.submit(fn): key for key, fn in phase2_tasks.items()}
                done2, _ = wait(fmap2.keys(), timeout=15, return_when=ALL_COMPLETED)
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


def _src_member_personal(nom: str, city: str, denom_latin: str, qualite: str = ""):
    """
    Recherche intensive du contact d'un membre RNE.
    Si le nom est en arabe, on génère aussi la forme latine translittérée.
    4 sources en parallèle : DDG, Bing, Google, Facebook (personnes + pages).
    """
    r  = {"phones": [], "emails": [], "websites": []}
    sp, se = set(), set()

    # Forme latine : translittération si arabe, sinon nom tel quel
    nom_latin = _ar_to_latin(nom) if _is_arabic(nom) else nom
    # On cherche avec les deux formes pour maximiser les résultats
    noms = list(dict.fromkeys(filter(None, [nom, nom_latin])))

    def _ddg():
        out = {"phones": [], "emails": [], "websites": []}
        for n in noms:
            for q in [
                f'"{n}" "{denom_latin}" contact téléphone email',
                f'"{n}" {city} syndic email téléphone',
                f'{n} {city} syndic tunisie contact',
            ]:
                d = extract_data(fetch(f"https://lite.duckduckgo.com/lite/?q={quote(q)}"))
                for p in d.get("phones", []):
                    if p not in out["phones"]: out["phones"].append(p)
                for e in d.get("emails", []):
                    if e not in out["emails"]: out["emails"].append(e)
                if out["phones"] or out["emails"]:
                    return out
        return out

    def _bing():
        out = {"phones": [], "emails": [], "websites": []}
        for n in noms:
            for q in [
                f'"{n}" {city} téléphone email syndic',
                f'{n} syndic {denom_latin} contact',
            ]:
                d = extract_data(fetch(
                    f"https://www.bing.com/search?q={quote(q)}&cc=TN&setlang=fr",
                    referer="https://www.bing.com/"
                ))
                for p in d.get("phones", []):
                    if p not in out["phones"]: out["phones"].append(p)
                for e in d.get("emails", []):
                    if e not in out["emails"]: out["emails"].append(e)
                if out["phones"] or out["emails"]:
                    return out
        return out

    def _google():
        out = {"phones": [], "emails": [], "websites": []}
        for n in noms:
            for q in [
                f'"{n}" {city} email téléphone syndic',
                f'"{n}" "{denom_latin}" contact',
            ]:
                url = f"https://www.google.com/search?q={quote(q)}&hl=fr&gl=tn"
                d = extract_data(fetch(url, referer="https://www.google.com/"))
                for p in d.get("phones", []):
                    if p not in out["phones"]: out["phones"].append(p)
                for e in d.get("emails", []):
                    if e not in out["emails"]: out["emails"].append(e)
                if out["phones"] or out["emails"]:
                    return out
        return out

    def _facebook():
        out = {"phones": [], "emails": [], "websites": []}
        urls = []
        for n in noms:
            urls += [
                f"https://mbasic.facebook.com/search/people/?q={quote(n)}",
                f"https://mbasic.facebook.com/search/people/?q={quote(n + ' ' + city)}",
            ]
        # Page Facebook du syndic lui-même
        urls.append(
            f"https://mbasic.facebook.com/search/pages/?q={quote(denom_latin + ' ' + city)}"
        )
        for url in urls:
            d = extract_data(fetch(url, referer="https://mbasic.facebook.com/", timeout=7))
            for p in d.get("phones", []):
                if p not in out["phones"]: out["phones"].append(p)
            for e in d.get("emails", []):
                if e not in out["emails"]: out["emails"].append(e)
            if out["phones"] or out["emails"]:
                break
        return out

    # Lancer les 4 sources en parallèle
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {
            pool.submit(_ddg):      "ddg",
            pool.submit(_bing):     "bing",
            pool.submit(_google):   "google",
            pool.submit(_facebook): "facebook",
        }
        done_m, _ = wait(futures.keys(), timeout=10, return_when=ALL_COMPLETED)
        for f in _:
            f.cancel()
        for fut in done_m:
            try:
                _merge(r, fut.result(timeout=1), sp, se)
            except Exception:
                pass

    return r, "member_contact"
