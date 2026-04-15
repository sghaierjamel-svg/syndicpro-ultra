"""
Extraction et normalisation des données de contact tunisiennes.
"""

import re
from bs4 import BeautifulSoup

# Numéros tunisiens : 8 chiffres commençant par 2-9, optionnellement précédés de +216 ou 216
RE_PHONE = re.compile(
    r'(?:(?:\+|00)216[\s.\-]?)?'
    r'(?:(?:\d{2}[\s.\-]\d{3}[\s.\-]\d{3})|(?:[2-9]\d{7}))',
    re.VERBOSE
)

RE_EMAIL = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.(?:tn|com|fr|net|org|info)',
    re.IGNORECASE
)

RE_WEBSITE = re.compile(
    r'https?://(?:www\.)?([a-zA-Z0-9\-]+\.(?:tn|com|fr|net))',
    re.IGNORECASE
)


def normalize_phone(raw):
    """Normalise un numéro tunisien au format +216XXXXXXXX."""
    # Garder uniquement chiffres et +
    d = re.sub(r'[^\d]', '', raw)

    if d.startswith('00216'):
        d = d[5:]
    elif d.startswith('216'):
        d = d[3:]

    # Doit faire exactement 8 chiffres, commencer par 2-9
    if len(d) == 8 and d[0] in '23456789':
        return f"+216{d}"
    return ""


def extract_data(html):
    """Extrait téléphones, emails et sites depuis du HTML brut."""
    if not html:
        return {"phones": [], "emails": [], "websites": []}

    soup = BeautifulSoup(html, "html.parser")

    # Supprimer les balises inutiles
    for tag in soup(['script', 'style', 'noscript', 'meta', 'link', 'head']):
        tag.decompose()

    # Extraire les liens tel: (très fiables)
    tel_phones = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('tel:'):
            raw = re.sub(r'[^\d+]', '', href.replace('tel:', ''))
            tel_phones.append(raw)

    text = soup.get_text(" ", strip=True)

    # Téléphones dans le texte + liens tel:
    raw_phones = RE_PHONE.findall(text) + tel_phones
    phones = []
    seen = set()
    for p in raw_phones:
        normalized = normalize_phone(p)
        if normalized and normalized not in seen:
            seen.add(normalized)
            phones.append(normalized)

    # Emails
    emails = list({e.lower() for e in RE_EMAIL.findall(text)})

    # Sites web (optionnel, pour enrichissement futur)
    websites = list({m for m in RE_WEBSITE.findall(text)
                     if not m.startswith(('bing.', 'google.', 'duckduckgo.', 'facebook.', 'twitter.', 'youtube.'))})

    return {
        "phones": phones,
        "emails": emails,
        "websites": websites[:5],
    }
