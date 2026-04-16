"""
Moteur de scoring — confiance basée sur la répétition inter-sources v2
Corrections :
  - global_conf ne pénalise plus l'absence d'email si un téléphone est trouvé
  - bonus progressif par nombre de sources concordantes
  - sources haute confiance (RNE, facebook, crawler) boostent le score
"""

HIGH_TRUST_SOURCES = {"rne_borne", "rne_entite", "rne", "pagesjaunes", "yellow_tn", "crawler", "facebook"}


def compute_conformity(results):
    if not results:
        return _empty()

    # Gérer le cas cache
    if len(results) == 1 and results[0].get("from_cache"):
        r = results[0]
        r.pop("from_cache", None)
        r.pop("source", None)
        return r

    total = len(results)

    phone_count    = {}
    email_count    = {}
    website_count  = {}
    phone_sources  = {}
    email_sources  = {}
    sources_with_data = []
    president     = ""
    members       = []
    address       = ""
    rne_id_found  = ""

    for r in results:
        src      = r.get("source", "?")
        has_data = False

        for p in r.get("phones", []):
            phone_count[p]  = phone_count.get(p, 0) + 1
            phone_sources.setdefault(p, set()).add(src)
            has_data = True

        for e in r.get("emails", []):
            email_count[e]  = email_count.get(e, 0) + 1
            email_sources.setdefault(e, set()).add(src)
            has_data = True

        for w in r.get("websites", []):
            website_count[w] = website_count.get(w, 0) + 1

        if has_data and src not in sources_with_data:
            sources_with_data.append(src)

        # Données RNE
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

    def _best_score(counts, sources_map):
        if not counts:
            return "", 0.0
        best  = max(counts, key=counts.get)
        hits  = counts[best]
        srcs  = sources_map.get(best, set())
        nsrcs = len(srcs)

        # Score de base : fréquence sur le nombre de sources qui ont répondu
        raw = min(90.0, (hits / max(total, 1)) * 100 * 2.5)

        # Bonus concordance multi-sources
        if nsrcs >= 4:
            raw = min(98.0, raw * 1.6)
        elif nsrcs == 3:
            raw = min(97.0, raw * 1.4)
        elif nsrcs == 2:
            raw = min(95.0, raw * 1.2)

        # Bonus source haute confiance
        if srcs & HIGH_TRUST_SOURCES:
            raw = min(99.0, raw * 1.15)

        return best, round(raw, 1)

    phone,   p_conf = _best_score(phone_count,   phone_sources)
    email,   e_conf = _best_score(email_count,   email_sources)
    website, _      = _best_score(website_count, {})

    # ── Global confidence — ne pénalise pas l'absence de l'autre champ ───────
    if phone and email:
        global_conf = round((p_conf * 0.6 + e_conf * 0.4), 1)   # tél prime légèrement
    elif phone:
        global_conf = round(p_conf, 1)
    elif email:
        global_conf = round(e_conf, 1)
    else:
        global_conf = 0.0

    # Bonus léger si on a aussi le président RNE (données enrichies)
    if president and global_conf > 0:
        global_conf = min(99.0, global_conf * 1.05)
    global_conf = round(global_conf, 1)

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
        "found":        bool(phone or email or president or members),
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
