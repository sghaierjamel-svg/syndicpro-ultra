def compute_conformity(results):
    total = len(results)

    phone_count = {}
    email_count = {}

    for r in results:
        for p in r.get("phones", []):
            phone_count[p] = phone_count.get(p, 0) + 1

        for e in r.get("emails", []):
            email_count[e] = email_count.get(e, 0) + 1

    def best(counts):
        if not counts:
            return "", 0, 0

        best = max(counts, key=counts.get)
        occ = counts[best]
        conf = (occ / total) * 100

        return best, occ, round(conf, 1)

    phone, p_occ, p_conf = best(phone_count)
    email, e_occ, e_conf = best(email_count)

    return {
        "phone": phone,
        "email": email,
        "phone_conf": p_conf,
        "email_conf": e_conf,
        "global_conf": round((p_conf + e_conf)/2, 1)
    }
