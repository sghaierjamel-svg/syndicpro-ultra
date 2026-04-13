import re
from bs4 import BeautifulSoup

RE_PHONE = re.compile(r'(?:\+216|0)?[2-9]\d{7}')
RE_EMAIL = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(?:tn|com|fr|net)', re.I)

def normalize_phone(p):
    d = re.sub(r'\D', '', p)
    if len(d) == 8:
        return "+216" + d
    if d.startswith("216"):
        return "+" + d
    return ""

def extract_data(html):
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ")

    phones = list(set(normalize_phone(p) for p in RE_PHONE.findall(text)))
    emails = list(set(RE_EMAIL.findall(text)))

    return {
        "phones": [p for p in phones if p],
        "emails": emails
    }
