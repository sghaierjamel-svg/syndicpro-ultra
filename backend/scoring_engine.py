"""
Moteur de scoring — confiance basée sur la répétition inter-sources v3
Améliorations :
  - Votes pondérés par source (RNE=5, annuaires=3, DDG/Bing=1, Tayara=0.5)
  - Évite les faux positifs : un numéro DDG/Bing ne peut pas l'emporter sur RNE
  - Seuil minimum de poids pour afficher un résultat
"""

HIGH_TRUST_SOURCES = {"rne_borne", "rne_entite", "rne", "pagesjaunes", "yellow_tn", "crawler", "facebook", "11880", "truecaller", "mubawab"}

# Poids de vote par source — plus élevé = plus fiable
SOURCE_WEIGHTS = {
    "rne_borne":      5.0,
    "rne_entite":     5.0,
    "rne":            4.0,
    "pagesjaunes":    3.0,
    "yellow_tn":      3.0,
    "11880":          3.0,
    "truecaller":     3.0,
    "mubawab":        2.5,
    "crawler":        2.0,
    "facebook":       2.0,
    "google_maps":    1.5,
    "member_contact": 1.5,
    "ddg":            1.0,
    "bing":           1.0,
    "google":         1.0,
    "arabic":         0.8,
    "linkedin":       0.8,
    "annuaires":      0.5,
    "tayara":         0.3,
}


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

        w = SOURCE_WEIGHTS.get(src, 1.0)

        for p in r.get("phones", []):
            phone_count[p]  = phone_count.get(p, 0) + w
            phone_sources.setdefault(p, set()).add(src)
            has_data = True

        for e in r.get("emails", []):
            email_count[e]  = email_count.get(e, 0) + w
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

        # ── Source officielle RNE entite → confiance minimale garantie ──────
        # L'email déposé lors de l'immatriculation est plus fiable que
        # n'importe quelle combinaison de sources web.
        if "rne_entite" in srcs:
            raw = max(70.0, min(90.0, (hits / max(total, 1)) * 100 * 2.5))
        else:
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

    # ── Global confidence ─────────────────────────────────────────────────────
    if phone and email:
        global_conf = round((p_conf * 0.6 + e_conf * 0.4), 1)
    elif phone:
        global_conf = round(p_conf, 1)
    elif email:
        global_conf = round(e_conf, 1)
    elif president or members:
        # Données RNE (président/membres) sans contact → confiance partielle
        global_conf = 40.0
    else:
        global_conf = 0.0

    # Bonus si données RNE enrichissent un contact déjà trouvé
    if (president or members) and global_conf > 0 and (phone or email):
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
