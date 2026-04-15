"""
Moteur de scoring — confiance basée sur la répétition inter-sources.
Adapté pour 17 sources parallèles.
"""

# Sources considérées comme très fiables (bonus de confiance)
HIGH_TRUST_SOURCES = {"rne_direct", "pagesjaunes", "crawler", "facebook", "bing_fb"}


def compute_conformity(results):
    if not results:
        return _empty()

    total = len(results)
    phone_count   = {}
    email_count   = {}
    website_count = {}
    phone_sources  = {}   # phone → set of sources
    email_sources  = {}   # email → set of sources
    sources_with_data = []
    president    = ""
    address      = ""
    rne_id_found = ""
    members      = []

    for r in results:
        src = r.get("source", "?")
        has_data = False
        for p in r.get("phones", []):
            phone_count[p] = phone_count.get(p, 0) + 1
            phone_sources.setdefault(p, set()).add(src)
            has_data = True
        for e in r.get("emails", []):
            email_count[e] = email_count.get(e, 0) + 1
            email_sources.setdefault(e, set()).add(src)
            has_data = True
        for w in r.get("websites", []):
            website_count[w] = website_count.get(w, 0) + 1
        if has_data:
            sources_with_data.append(src)
        # Extraire les données RNE Borne
        if r.get("president") and not president:
            president = r["president"]
            if src not in sources_with_data:
                sources_with_data.append(src)
        if r.get("members") and not members:
            members = r["members"]
        if r.get("address") and not address:
            address = r["address"]
        if r.get("rne_id_found") and not rne_id_found:
            rne_id_found = r["rne_id_found"]

    def best(counts):
        if not counts:
            return "", 0
        b = max(counts, key=counts.get)
        # Score = (apparitions / total) * 100, plafonné à 95 avant bonus
        raw = min(95.0, round((counts[b] / total) * 100 * 3, 1))
        return b, raw

    phone,   p_conf = best(phone_count)
    email,   e_conf = best(email_count)
    website, _      = best(website_count)

    # Bonus multi-sources
    if phone:
        srcs = phone_sources.get(phone, set())
        if len(srcs) >= 3:
            p_conf = min(98.0, p_conf * 1.5)
        elif len(srcs) == 2:
            p_conf = min(95.0, p_conf * 1.3)
        # Bonus source fiable
        if srcs & HIGH_TRUST_SOURCES:
            p_conf = min(99.0, p_conf * 1.2)

    if email:
        srcs = email_sources.get(email, set())
        if len(srcs) >= 3:
            e_conf = min(98.0, e_conf * 1.5)
        elif len(srcs) == 2:
            e_conf = min(95.0, e_conf * 1.3)
        if srcs & HIGH_TRUST_SOURCES:
            e_conf = min(99.0, e_conf * 1.2)

    p_conf = round(p_conf, 1)
    e_conf = round(e_conf, 1)
    global_conf = round((p_conf + e_conf) / 2, 1) if (phone or email) else 0

    return {
        "phone":        phone,
        "email":        email,
        "website":      website,
        "phone_conf":   p_conf,
        "email_conf":   e_conf,
        "global_conf":  global_conf,
        "all_phones":   sorted(phone_count.keys()),
        "all_emails":   sorted(email_count.keys()),
        "sources_hit":  list(dict.fromkeys(sources_with_data)),
        "found":        bool(phone or email),
        "president":    president,
        "members":      members,
        "address":      address,
        "rne_id_found": rne_id_found,
    }


def _empty():
    return {
        "phone": "", "email": "", "website": "",
        "phone_conf": 0, "email_conf": 0, "global_conf": 0,
        "all_phones": [], "all_emails": [],
        "sources_hit": [], "found": False,
        "president": "", "members": [], "address": "", "rne_id_found": "",
    }
