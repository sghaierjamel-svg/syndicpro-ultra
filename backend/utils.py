"""
Extraction et normalisation des données de contact tunisiennes — v2
Améliorations : réduction des faux positifs, emails obfusqués, tel: links.
"""

import re
from bs4 import BeautifulSoup

# ── Téléphones ──────────────────────────────────────────────────────────────

# Format avec séparateurs : XX XXX XXX ou XX-XXX-XXX ou XX.XXX.XXX
RE_PHONE_SEP = re.compile(
    r'(?:(?:\+|00)216[\s.\-]?)?'
    r'\b([2-9]\d[\s.\-]\d{3}[\s.\-]\d{3})\b'
)

# Format compact (8 chiffres) — seulement après un mot-clé téléphonique
RE_PHONE_KEYWORD = re.compile(
    r'(?:tél?\.?|gsm|mob(?:ile)?\.?|fixe|num[ée]ro|tel\.?|contact|appel|'
    r'joindre|appeler|ligne|portable|cel\.?|هاتف|رقم|اتصل)\s*[:\s\-]*'
    r'(?:(?:\+|00)?216[\s.\-]?)?'
    r'([2-9]\d{7})',
    re.IGNORECASE | re.UNICODE
)

# Format +216XXXXXXXX ou 00216XXXXXXXX — très fiable
RE_PHONE_INTL = re.compile(
    r'(?:\+216|00216)[\s.\-]?([2-9]\d{7}|\d{2}[\s.\-]\d{3}[\s.\-]\d{3})'
)

# 8 chiffres bruts — seulement si entouré de séparateurs non-numériques
# et si le contexte proche contient un mot de contact
RE_PHONE_BARE = re.compile(r'(?<!\d)([2-9]\d{7})(?!\d)')
_PHONE_CTX = re.compile(
    r'(?:tél?|gsm|mob|fixe|contact|phone|هاتف|رقم|numéro|appel|'
    r'joindre|coordonnées|portable|ligne|'
    r'numTel|telSociete|telephoneSociete|numTelephone|telBureau|telFixe)',
    re.IGNORECASE | re.UNICODE
)

# ── Emails ──────────────────────────────────────────────────────────────────

RE_EMAIL = re.compile(
    r'[a-zA-Z0-9._%+\-]{2,}@[a-zA-Z0-9.\-]+\.(?:tn|com|fr|net|org|info|biz)',
    re.IGNORECASE
)

# Emails obfusqués : nom[at]domaine.tn  ou  nom (at) domaine.tn
RE_EMAIL_OBFUSCATED = re.compile(
    r'([a-zA-Z0-9._%+\-]{2,})\s*[\[\(]\s*at\s*[\]\)]\s*([a-zA-Z0-9.\-]+\.[a-zA-Z]{2,4})',
    re.IGNORECASE
)

# ── Sites web ───────────────────────────────────────────────────────────────

RE_WEBSITE = re.compile(
    r'https?://(?:www\.)?([a-zA-Z0-9\-]+\.(?:tn|com|fr|net))',
    re.IGNORECASE
)

NOISE_DOMAINS = {
    'google', 'bing', 'duckduckgo', 'yahoo', 'youtube', 'wikipedia',
    'twitter', 'instagram', 'tiktok', 'amazon', 'apple', 'microsoft',
    'whatsapp', 'linkedin', 'facebook', 'gstatic', 'googleapis',
}

# Mots qui indiquent qu'un nombre est une date / référence légale / ID
_DATE_CONTEXT = re.compile(
    r'(?:janvier|février|mars|avril|mai|juin|juillet|août|septembre|'
    r'octobre|novembre|décembre|article|décret|arrêté|loi|n°|num\.?\s*dossier|'
    r'réf(?:érence)?|code|ref\.|copie)',
    re.IGNORECASE
)


def normalize_phone(raw: str) -> str:
    """Normalise un numéro tunisien → +216XXXXXXXX."""
    d = re.sub(r'[^\d]', '', raw)
    if d.startswith('00216'):
        d = d[5:]
    elif d.startswith('216') and len(d) == 11:
        d = d[3:]
    if len(d) == 8 and d[0] in '23456789':
        return f"+216{d}"
    return ""


def extract_data(html: str) -> dict:
    """Extrait téléphones, emails et sites depuis du HTML brut."""
    if not html:
        return {"phones": [], "emails": [], "websites": []}

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(['script', 'style', 'noscript', 'meta', 'link', 'head']):
        tag.decompose()

    # ── 1. Liens tel: + wa.me + WhatsApp (sources directes, très fiables) ───────
    tel_phones: list[str] = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('tel:'):
            raw = re.sub(r'[^\d+]', '', href[4:])
            n   = normalize_phone(raw)
            if n:
                tel_phones.append(n)
        elif 'wa.me/' in href:
            m = re.search(r'wa\.me/(\d{8,15})', href)
            if m:
                n = normalize_phone(m.group(1))
                if n and n not in tel_phones:
                    tel_phones.append(n)
        elif 'whatsapp.com' in href:
            m = re.search(r'[?&]phone=(\d{8,15})', href)
            if m:
                n = normalize_phone(m.group(1))
                if n and n not in tel_phones:
                    tel_phones.append(n)

    text = soup.get_text(" ", strip=True)

    # ── 2. Téléphones dans le texte ───────────────────────────────────────────
    seen_phones: set[str] = set(tel_phones)
    phones: list[str]     = list(tel_phones)

    def _add_phone(raw_match):
        n = normalize_phone(re.sub(r'\s', '', raw_match))
        if n and n not in seen_phones:
            # Vérifier le contexte autour du nombre dans le texte
            idx = text.find(raw_match.replace(' ', ''))
            if idx > 0:
                ctx = text[max(0, idx-60):idx+20]
                if _DATE_CONTEXT.search(ctx):
                    return  # probablement un numéro légal/date
            seen_phones.add(n)
            phones.append(n)

    for m in RE_PHONE_INTL.finditer(text):
        _add_phone(m.group(1))

    for m in RE_PHONE_SEP.finditer(text):
        _add_phone(m.group(1))

    for m in RE_PHONE_KEYWORD.finditer(text):
        _add_phone(m.group(1))

    # 4. Numéros 8 chiffres bruts si contexte téléphonique proche
    for m in RE_PHONE_BARE.finditer(text):
        raw = m.group(1)
        idx = m.start()
        ctx = text[max(0, idx - 120):idx + 40]
        if _PHONE_CTX.search(ctx):
            _add_phone(raw)

    # ── 3. Emails ─────────────────────────────────────────────────────────────
    seen_emails: set[str] = set()
    emails: list[str]     = []

    for e in RE_EMAIL.findall(text):
        e = e.lower()
        if e not in seen_emails:
            seen_emails.add(e)
            emails.append(e)

    # Emails obfusqués
    for m in RE_EMAIL_OBFUSCATED.finditer(text):
        e = f"{m.group(1)}@{m.group(2)}".lower()
        if e not in seen_emails:
            seen_emails.add(e)
            emails.append(e)

    # Emails dans les liens mailto:
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('mailto:'):
            e = href[7:].split('?')[0].strip().lower()
            if RE_EMAIL.match(e) and e not in seen_emails:
                seen_emails.add(e)
                emails.append(e)

    # ── 4. Sites web ──────────────────────────────────────────────────────────
    websites = list({
        m for m in RE_WEBSITE.findall(text)
        if m.split('.')[0] not in NOISE_DOMAINS
    })[:5]

    return {"phones": phones, "emails": emails, "websites": websites}
