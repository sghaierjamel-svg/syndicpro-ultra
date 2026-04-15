"""
Moteur de scoring : calcule la fiabilité des contacts trouvés.
Plus un numéro/email apparaît dans plusieurs sources indépendantes,
plus son score de confiance est élevé.
"""


def compute_conformity(results):
    total = len(results)

    if total == 0:
        return _empty()

    phone_count = {}
    email_count = {}
    website_count = {}
    sources_with_data = []

    for r in results:
        has_data = False
        for p in r.get("phones", []):
            phone_count[p] = phone_count.get(p, 0) + 1
            has_data = True
        for e in r.get("emails", []):
            email_count[e] = email_count.get(e, 0) + 1
            has_data = True
        for w in r.get("websites", []):
            website_count[w] = website_count.get(w, 0) + 1
        if has_data:
            sources_with_data.append(r.get("source", "?"))

    def best(counts):
        if not counts:
            return "", 0
        b = max(counts, key=counts.get)
        conf = round((counts[b] / total) * 100, 1)
        return b, conf

    phone, p_conf = best(phone_count)
    email, e_conf = best(email_count)
    website, _    = best(website_count)

    # Bonus si trouvé dans plusieurs sources
    if phone and phone_count.get(phone, 0) >= 2:
        p_conf = min(100.0, p_conf * 1.3)
    if email and email_count.get(email, 0) >= 2:
        e_conf = min(100.0, e_conf * 1.3)

    p_conf = round(p_conf, 1)
    e_conf = round(e_conf, 1)
    global_conf = round((p_conf + e_conf) / 2, 1)

    return {
        "phone":       phone,
        "email":       email,
        "website":     website,
        "phone_conf":  p_conf,
        "email_conf":  e_conf,
        "global_conf": global_conf,
        "all_phones":  sorted(phone_count.keys()),
        "all_emails":  sorted(email_count.keys()),
        "sources_hit": sources_with_data,
        "found":       bool(phone or email),
    }


def _empty():
    return {
        "phone": "", "email": "", "website": "",
        "phone_conf": 0, "email_conf": 0, "global_conf": 0,
        "all_phones": [], "all_emails": [],
        "sources_hit": [], "found": False,
    }
