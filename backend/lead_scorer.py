"""
Lead Scorer — SyndicPro Scanner
Calcule un score 0-100 pour chaque prospect selon son potentiel commercial.
"""

import re
import logging

logger = logging.getLogger("lead_scorer")

# Villes prioritaires Tunisie (plus de syndics actifs)
_MAJOR_CITIES = {
    'tunis', 'ariana', 'la marsa', 'carthage', 'le bardo', 'la goulette',
    'sousse', 'sfax', 'nabeul', 'hammamet', 'bizerte', 'gabes', 'gafsa',
    'monastir', 'mahdia', 'kairouan', 'ben arous', 'hammam lif', 'rades',
    'soliman', 'grombalia', 'mornag', 'zaghouan', 'manouba', 'den den',
}

# Mots qui signalent que CE N'EST PAS un syndic résidentiel
_DISQUALIFY = [
    'TRANSPORT', 'TAXI', 'CAMION', 'AUTOBUS', 'MINIBUS', 'VEHICULE',
    'LEGER A SFAX', 'LEGER A TUNIS', 'LEGER A SOUSSE',
    'SOCIETE ANONYME', 'SARL ', ' SA ', 'EURL', 'SAS ',
    'AGRICOLE', 'COMMERCIAL', 'INDUSTRIEL', 'EXPORT', 'IMPORT',
    'CLINIQUE', 'MEDIC', 'HOTEL', 'RESTAURANT', 'CAFE',
]

# Mots qui confirment que c'est un bon prospect
_QUALIFY = [
    'RESIDENCE', 'COPROPRIET', 'COPROPRIETE', 'IMMEUBLE', 'APPARTEMENT',
    'BLOC', 'TOUR ', 'TOWER', 'PARK ', 'GARDEN', 'VILLA',
    'JARDIN', 'PARC ', 'CITE ', 'QUARTIER',
]


def score_contact(contact: dict) -> int:
    """
    Retourne un score 0-100 (ou -1 si disqualifié).
    Plus le score est élevé, plus le prospect est prioritaire.
    """
    name  = (contact.get("name") or "").upper()
    city  = (contact.get("city") or "").lower().strip()
    score = 0

    # ── Disqualification immédiate ────────────────────────────────────────────
    for kw in _DISQUALIFY:
        if kw in name:
            return -1

    # ── Qualification explicite : c'est bien un syndic résidentiel ───────────
    for kw in _QUALIFY:
        if kw in name:
            score += 12
            break

    # ── Données contact ───────────────────────────────────────────────────────
    if contact.get("email"):
        score += 30   # a un email → peut être contacté

    if contact.get("phone"):
        score += 20   # a un téléphone → peut être appelé

    # ── Personnalisation possible ─────────────────────────────────────────────
    if contact.get("president"):
        score += 10   # on peut personaliser l'email avec son nom

    # ── Confiance du scraping ─────────────────────────────────────────────────
    conf = float(contact.get("confidence") or 0)
    if conf >= 80:
        score += 15
    elif conf >= 50:
        score += 8
    elif conf >= 30:
        score += 3

    # ── Ville prioritaire ─────────────────────────────────────────────────────
    if any(c in city for c in _MAJOR_CITIES):
        score += 10

    # ── Pipeline status bonus ─────────────────────────────────────────────────
    status = contact.get("pipeline_status", "prospect")
    status_bonus = {
        "opened":     10,
        "interested": 20,
        "demo":       30,
    }
    score += status_bonus.get(status, 0)

    return min(100, max(0, score))


def run_scoring_all() -> dict:
    """
    Lance le scoring sur tous les contacts et met à jour la DB.
    Retourne un résumé: {scored, disqualified, avg_score}
    """
    from db import get_all_for_scoring, bulk_update_scores

    contacts = get_all_for_scoring(limit=10000)
    scores   = {}
    disq     = 0

    for c in contacts:
        s = score_contact(c)
        if s == -1:
            scores[c["id"]] = 0
            disq += 1
        else:
            scores[c["id"]] = s

    bulk_update_scores(scores)

    valid_scores = [v for v in scores.values() if v > 0]
    avg = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else 0

    logger.info(f"[Scorer] {len(contacts)} contacts scorés — {disq} disqualifiés — moyenne {avg}")
    return {
        "scored": len(contacts),
        "disqualified": disq,
        "avg_score": avg,
        "top_leads": len([s for s in valid_scores if s >= 50]),
    }
