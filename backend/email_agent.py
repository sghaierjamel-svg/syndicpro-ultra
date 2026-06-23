"""
SyndicPro Scanner — Agent Email v1
Envoie des emails de prospection personnalisés aux syndics scrapés.

Configuration (variables d'environnement) :
  RESEND_API_KEY    — Clé API Resend (recommandé)
  EMAIL_FROM        — Expéditeur (ex: contact@syndicpro.tn)
  EMAIL_FROM_NAME   — Nom affiché  (ex: SyndicPro)
  SMTP_HOST         — Serveur SMTP (fallback si pas de Resend)
  SMTP_PORT         — Port SMTP (défaut: 587)
  SMTP_USER         — Login SMTP
  SMTP_PASS         — Mot de passe SMTP
"""

import os
import json
import time
import smtplib
import logging
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import requests

logger = logging.getLogger("email_agent")

# ─── Config ───────────────────────────────────────────────────────────────────

RESEND_API_KEY  = os.environ.get("RESEND_API_KEY", "")
EMAIL_FROM      = os.environ.get("EMAIL_FROM", "contact@syndicpro.tn")
EMAIL_FROM_NAME = os.environ.get("EMAIL_FROM_NAME", "SyndicPro")
SMTP_HOST       = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT       = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER       = os.environ.get("SMTP_USER", "")
SMTP_PASS       = os.environ.get("SMTP_PASS", "")

DELAY_BETWEEN_EMAILS = float(os.environ.get("EMAIL_DELAY_SEC", "3"))  # secondes entre chaque envoi

# ─── Templates ────────────────────────────────────────────────────────────────

TEMPLATES = {
    "prospection": {
        "name": "Prospection standard",
        "subject": "Gérez votre résidence {name} en ligne — Essai gratuit 90 jours",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#1D4ED8,#3B82F6);padding:2rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.6rem">SyndicPro</h1>
    <p style="color:#bfdbfe;margin:.5rem 0 0;font-size:.95rem">La solution moderne pour les syndics tunisiens</p>
  </div>

  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p style="font-size:1rem;line-height:1.6">Bonjour,</p>

    <p style="font-size:1rem;line-height:1.6">
      Je me permets de vous contacter au sujet de la gestion de <strong>{name}</strong>{city_str}.
    </p>

    <p style="font-size:1rem;line-height:1.6">
      <strong>SyndicPro</strong> est une plateforme tunisienne conçue spécialement pour
      simplifier le travail des syndics de copropriété :
    </p>

    <ul style="line-height:2;font-size:.95rem;color:#374151">
      <li>✅ Suivi des encaissements et impayés en temps réel</li>
      <li>✅ Notifications WhatsApp automatiques aux résidents</li>
      <li>✅ États financiers et rapports PDF en 1 clic</li>
      <li>✅ Espace résident en ligne (solde, tickets, paiements)</li>
      <li>✅ Paiement en ligne intégré (Flouci / Konnect)</li>
    </ul>

    <div style="background:#fff;border:2px solid #3B82F6;border-radius:10px;padding:1.2rem;text-align:center;margin:1.5rem 0">
      <p style="font-size:1.1rem;font-weight:700;color:#1D4ED8;margin:0">
        🎁 Essai gratuit 90 jours — Sans carte bancaire
      </p>
      <p style="color:#64748b;font-size:.85rem;margin:.4rem 0 0">
        Aucun engagement. Configuration en moins d'une heure.
      </p>
    </div>

    <div style="text-align:center;margin:1.5rem 0">
      <a href="https://www.syndicpro.tn/register"
         style="background:linear-gradient(135deg,#1D4ED8,#3B82F6);color:#fff;text-decoration:none;
                padding:.9rem 2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block">
        Commencer gratuitement →
      </a>
    </div>

    <p style="font-size:.9rem;color:#64748b;line-height:1.6">
      Vous pouvez également consulter une démo complète en ligne :
      <a href="https://www.syndicpro.tn/demo" style="color:#3B82F6">www.syndicpro.tn/demo</a>
    </p>

    <hr style="border:none;border-top:1px solid #e2e8f0;margin:1.5rem 0">

    <p style="font-size:.85rem;color:#94a3b8;margin:0">
      Cordialement,<br>
      <strong style="color:#1e293b">{from_name}</strong><br>
      <a href="https://www.syndicpro.tn" style="color:#3B82F6">www.syndicpro.tn</a><br>
      <a href="mailto:{from_email}" style="color:#94a3b8">{from_email}</a>
    </p>

    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">
      Si vous ne souhaitez plus recevoir ces emails, répondez avec "DÉSABONNER".
    </p>
  </div>
</div>
""",
    },

    "relance": {
        "name": "Relance J+7",
        "subject": "Avez-vous eu le temps de regarder SyndicPro ? ({name})",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#0f172a,#1e293b);padding:1.5rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
  </div>

  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p style="font-size:1rem;line-height:1.6">Bonjour,</p>

    <p style="font-size:1rem;line-height:1.6">
      Je reviens vers vous suite à mon précédent message concernant <strong>{name}</strong>.
    </p>

    <p style="font-size:1rem;line-height:1.6">
      Notre essai gratuit de 90 jours est toujours disponible.
      Beaucoup de syndics tunisiens nous disent qu'ils hésitaient au début —
      mais après avoir testé SyndicPro une semaine, ils ne reviennent plus à Excel.
    </p>

    <blockquote style="border-left:4px solid #3B82F6;padding:.8rem 1rem;background:#eff6ff;border-radius:0 8px 8px 0;margin:1rem 0;font-style:italic;color:#374151">
      « Avant je passais 2 heures par mois à faire les calculs de charges.
      Maintenant ça prend 10 minutes. » — Syndic à Sfax
    </blockquote>

    <div style="text-align:center;margin:1.5rem 0">
      <a href="https://www.syndicpro.tn/register"
         style="background:#10B981;color:#fff;text-decoration:none;
                padding:.9rem 2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block">
        Tester maintenant — Gratuit 90 jours →
      </a>
    </div>

    <p style="font-size:.85rem;color:#64748b">
      Cordialement,<br>
      <strong style="color:#1e293b">{from_name}</strong> — SyndicPro<br>
      <a href="mailto:{from_email}" style="color:#3B82F6">{from_email}</a>
    </p>
  </div>
</div>
""",
    },
}

# ─── Envoi ────────────────────────────────────────────────────────────────────

def _send_via_resend(to_email: str, subject: str, html: str) -> bool:
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": f"{EMAIL_FROM_NAME} <{EMAIL_FROM}>",
            "to": [to_email],
            "subject": subject,
            "html": html,
        },
        timeout=15,
    )
    if resp.status_code in (200, 201):
        return True
    logger.error(f"[Resend] {resp.status_code} — {resp.text[:200]}")
    return False


def _send_via_smtp(to_email: str, subject: str, html: str) -> bool:
    if not SMTP_USER or not SMTP_PASS:
        logger.error("[SMTP] SMTP_USER ou SMTP_PASS non configuré")
        return False
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{EMAIL_FROM_NAME} <{EMAIL_FROM}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(EMAIL_FROM, to_email, msg.as_string())
        return True
    except Exception as e:
        logger.error(f"[SMTP] Erreur envoi : {e}")
        return False


def send_email(to_email: str, subject: str, html: str) -> bool:
    """Envoie via Resend si configuré, sinon SMTP."""
    if RESEND_API_KEY:
        return _send_via_resend(to_email, subject, html)
    return _send_via_smtp(to_email, subject, html)


def build_email(template_id: str, contact: dict) -> tuple[str, str]:
    """Retourne (subject, html) personnalisés pour ce contact."""
    tpl = TEMPLATES.get(template_id)
    if not tpl:
        raise ValueError(f"Template inconnu : {template_id}")

    name     = contact.get("name", "votre résidence")
    city     = contact.get("city", "")
    city_str = f" à {city}" if city else ""

    subject = tpl["subject"].format(name=name, city=city, city_str=city_str)
    html    = tpl["body_html"].format(
        name=name,
        city=city,
        city_str=city_str,
        from_name=EMAIL_FROM_NAME,
        from_email=EMAIL_FROM,
    )
    return subject, html


# ─── Campagne (bulk) ──────────────────────────────────────────────────────────

_campaign_lock = threading.Lock()
_active_campaign: Optional[dict] = None


def get_campaign_status() -> dict:
    with _campaign_lock:
        if _active_campaign is None:
            return {"running": False}
        return dict(_active_campaign)


def _run_campaign(contacts: list[dict], template_id: str, campaign_id: str):
    global _active_campaign
    total   = len(contacts)
    sent    = 0
    skipped = 0
    errors  = 0

    from db import get_conn
    conn = get_conn()

    for i, c in enumerate(contacts):
        with _campaign_lock:
            _active_campaign.update({
                "progress": i + 1,
                "total": total,
                "sent": sent,
                "errors": errors,
            })

        to_email = c.get("email", "").strip()
        if not to_email:
            skipped += 1
            continue

        # Vérifier si déjà envoyé
        row = conn.execute(
            "SELECT email_sent FROM results WHERE id=?", (c["id"],)
        ).fetchone()
        if row and row["email_sent"]:
            skipped += 1
            continue

        try:
            subject, html = build_email(template_id, c)
            ok = send_email(to_email, subject, html)
            if ok:
                conn.execute(
                    "UPDATE results SET email_sent=1, email_sent_at=CURRENT_TIMESTAMP, "
                    "email_template=?, email_status='sent' WHERE id=?",
                    (template_id, c["id"])
                )
                conn.commit()
                sent += 1
                logger.info(f"[Campaign] Envoyé à {to_email} ({c['name']})")
            else:
                conn.execute(
                    "UPDATE results SET email_status='error' WHERE id=?", (c["id"],)
                )
                conn.commit()
                errors += 1
        except Exception as e:
            logger.error(f"[Campaign] Erreur {c.get('name')} : {e}")
            errors += 1

        time.sleep(DELAY_BETWEEN_EMAILS)

    conn.close()
    with _campaign_lock:
        _active_campaign.update({
            "running": False,
            "done": True,
            "sent": sent,
            "skipped": skipped,
            "errors": errors,
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
    logger.info(f"[Campaign] Terminée — {sent} envoyés, {skipped} ignorés, {errors} erreurs")


def start_campaign(contacts: list[dict], template_id: str) -> dict:
    global _active_campaign
    with _campaign_lock:
        if _active_campaign and _active_campaign.get("running"):
            return {"error": "Une campagne est déjà en cours"}
        campaign_id = str(int(time.time()))
        _active_campaign = {
            "running": True,
            "done": False,
            "campaign_id": campaign_id,
            "template_id": template_id,
            "total": len(contacts),
            "progress": 0,
            "sent": 0,
            "skipped": 0,
            "errors": 0,
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    t = threading.Thread(
        target=_run_campaign,
        args=(contacts, template_id, campaign_id),
        daemon=True,
    )
    t.start()
    return {"campaign_id": campaign_id, "total": len(contacts)}


# ═══════════════════════════════════════════════════════════════════════════════
# SÉQUENCES AUTOMATIQUES (drip campaign 4 emails sur 14 jours)
# ═══════════════════════════════════════════════════════════════════════════════

SEQ_DELAYS = {1: 0, 2: 3 * 86400, 3: 7 * 86400, 4: 14 * 86400}  # secondes

_TRACKER_BASE = "https://www.syndicpro.tn"
_PIXEL = '<img src="' + _TRACKER_BASE + '/st/o/{track_id}" width="1" height="1" style="display:block" alt="">'

SEQ_TEMPLATES = {
    1: {
        "subject": "Gérez {name} en ligne — Essai gratuit 90 jours",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#1D4ED8,#3B82F6);padding:2rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.6rem">SyndicPro</h1>
    <p style="color:#bfdbfe;margin:.5rem 0 0;font-size:.9rem">La solution moderne pour les syndics tunisiens</p>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p>Je me permets de vous contacter au sujet de la gestion de <strong>{name}</strong>{city_str}.</p>
    <p><strong>SyndicPro</strong> est une plateforme 100% tunisienne qui simplifie le travail des syndics :</p>
    <ul style="line-height:2;font-size:.95rem">
      <li>✅ Suivi des encaissements et impayés en temps réel</li>
      <li>✅ Notifications WhatsApp automatiques aux résidents</li>
      <li>✅ États financiers et rapports PDF en 1 clic</li>
      <li>✅ Espace résident en ligne (solde, tickets, paiements)</li>
    </ul>
    <div style="background:#fff;border:2px solid #3B82F6;border-radius:10px;padding:1.2rem;text-align:center;margin:1.5rem 0">
      <p style="font-size:1.1rem;font-weight:700;color:#1D4ED8;margin:0">🎁 Essai gratuit 90 jours — Sans carte bancaire</p>
      <p style="color:#64748b;font-size:.85rem;margin:.4rem 0 0">Aucun engagement. Configuration en moins d'une heure.</p>
    </div>
    <div style="text-align:center;margin:1.5rem 0">
      <a href="https://www.syndicpro.tn/register" style="background:linear-gradient(135deg,#1D4ED8,#3B82F6);color:#fff;text-decoration:none;padding:.9rem 2rem;border-radius:8px;font-weight:700;display:inline-block">Commencer gratuitement →</a>
    </div>
    <hr style="border:none;border-top:1px solid #e2e8f0;margin:1.5rem 0">
    <p style="font-size:.85rem;color:#94a3b8">Cordialement,<br><strong style="color:#1e293b">{from_name}</strong><br>
    <a href="https://www.syndicpro.tn" style="color:#3B82F6">www.syndicpro.tn</a> · <a href="mailto:{from_email}" style="color:#94a3b8">{from_email}</a></p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    2: {
        "subject": "Re: {name} — une question rapide",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:#0f172a;padding:1.5rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p>Je fais suite à mon message de l'autre jour concernant <strong>{name}</strong>.</p>
    <p>Une question directe : <strong>combien de temps passez-vous chaque mois à gérer les charges et relancer les impayés ?</strong></p>
    <p>La plupart des syndics qu'on rencontre estiment perdre plusieurs heures par mois sur des tâches que SyndicPro automatise : relances, reçus PDF, tableaux de charges, suivi des impayés.</p>
    <p>Si vous êtes curieux, l'essai gratuit 3 mois est toujours ouvert — aucune carte bancaire requise.</p>
    <div style="text-align:center;margin:1.5rem 0">
      <a href="https://www.syndicpro.tn/register" style="background:#10B981;color:#fff;text-decoration:none;padding:.9rem 2rem;border-radius:8px;font-weight:700;display:inline-block">Essayer gratuitement →</a>
    </div>
    <p style="font-size:.85rem;color:#64748b">Cordialement,<br><strong style="color:#1e293b">{from_name}</strong> — SyndicPro</p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    3: {
        "subject": "{name} — comparatif Excel vs SyndicPro",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#7C3AED,#6D28D9);padding:1.5rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
    <p style="color:#ddd6fe;margin:.3rem 0 0;font-size:.85rem">Excel vs SyndicPro</p>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p>Voici une comparaison honnête pour la gestion de <strong>{name}</strong> :</p>
    <table style="width:100%;border-collapse:collapse;font-size:.9rem;margin:1rem 0">
      <tr style="background:#f1f5f9">
        <th style="padding:.6rem;text-align:left;border:1px solid #e2e8f0"></th>
        <th style="padding:.6rem;text-align:center;border:1px solid #e2e8f0">Excel</th>
        <th style="padding:.6rem;text-align:center;border:1px solid #e2e8f0;color:#1D4ED8">SyndicPro</th>
      </tr>
      <tr><td style="padding:.6rem;border:1px solid #e2e8f0">Suivi des impayés</td><td style="text-align:center;border:1px solid #e2e8f0">Manuel</td><td style="text-align:center;border:1px solid #e2e8f0;color:#10B981">✅ Automatique</td></tr>
      <tr style="background:#f8fafc"><td style="padding:.6rem;border:1px solid #e2e8f0">Relances résidents</td><td style="text-align:center;border:1px solid #e2e8f0">Téléphone</td><td style="text-align:center;border:1px solid #e2e8f0;color:#10B981">✅ WhatsApp auto</td></tr>
      <tr><td style="padding:.6rem;border:1px solid #e2e8f0">Rapports financiers</td><td style="text-align:center;border:1px solid #e2e8f0">2-3 heures</td><td style="text-align:center;border:1px solid #e2e8f0;color:#10B981">✅ 1 clic</td></tr>
      <tr style="background:#f8fafc"><td style="padding:.6rem;border:1px solid #e2e8f0">Accès résidents</td><td style="text-align:center;border:1px solid #e2e8f0">❌ Aucun</td><td style="text-align:center;border:1px solid #e2e8f0;color:#10B981">✅ Portail en ligne</td></tr>
      <tr><td style="padding:.6rem;border:1px solid #e2e8f0">Coût mensuel</td><td style="text-align:center;border:1px solid #e2e8f0">0 DT</td><td style="text-align:center;border:1px solid #e2e8f0;color:#1D4ED8">À partir de 29 DT</td></tr>
    </table>
    <p style="font-size:.9rem;color:#64748b">À partir de 29 DT/mois selon la taille de votre résidence.</p>
    <div style="text-align:center;margin:1.5rem 0">
      <a href="https://www.syndicpro.tn/register" style="background:#1D4ED8;color:#fff;text-decoration:none;padding:.9rem 2rem;border-radius:8px;font-weight:700;display:inline-block">Commencer l'essai gratuit →</a>
    </div>
    <p style="font-size:.85rem;color:#64748b">Cordialement,<br><strong style="color:#1e293b">{from_name}</strong> — SyndicPro</p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    4: {
        "subject": "Dernier message — {name}",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:#1e293b;padding:1.5rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p>C'est mon dernier message concernant <strong>{name}</strong>. Je ne veux pas encombrer votre boîte mail.</p>
    <p>Si le moment n'est pas venu, pas de problème — notre offre reste disponible quand vous le souhaitez :</p>
    <p style="text-align:center">
      <a href="https://www.syndicpro.tn" style="color:#3B82F6;font-weight:700">www.syndicpro.tn</a>
    </p>
    <p>Et si vous souhaitez simplement une démo de 15 minutes pour voir comment ça fonctionne concrètement pour votre résidence, je suis disponible :</p>
    <div style="text-align:center;margin:1.5rem 0">
      <a href="mailto:{from_email}?subject=Demande demo SyndicPro — {name}" style="background:#64748b;color:#fff;text-decoration:none;padding:.9rem 2rem;border-radius:8px;font-weight:700;display:inline-block">Demander une démo →</a>
    </div>
    <p style="font-size:.9rem;color:#64748b">À bientôt peut-être,<br><strong style="color:#1e293b">{from_name}</strong> — SyndicPro<br>
    <a href="mailto:{from_email}" style="color:#3B82F6">{from_email}</a></p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },
}


SEQ_MULTI_TEMPLATES = {
    1: {
        "subject": "Vos {count} résidences méritent mieux qu'Excel — SyndicPro",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#0f172a,#1e3a5f);padding:2rem;border-radius:12px 12px 0 0;text-align:center">
    <p style="color:#94a3b8;margin:0 0 .4rem;font-size:.75rem;letter-spacing:.15em;text-transform:uppercase">Gestionnaire multi-résidences</p>
    <h1 style="color:#fff;margin:0;font-size:1.6rem;letter-spacing:-.01em">SyndicPro</h1>
    <p style="color:#93c5fd;margin:.5rem 0 0;font-size:.9rem">Le même outil pour chacune de vos résidences</p>
  </div>

  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p style="font-size:1rem;line-height:1.7">Bonjour{president_str},</p>

    <p style="font-size:1rem;line-height:1.7">
      Vous gérez <strong>{count} résidences</strong>{residences_snippet}.
      C'est un volume qui dépasse largement ce qu'un simple fichier Excel peut gérer efficacement.
    </p>

    <div style="background:#eff6ff;border-left:4px solid #1D4ED8;padding:1rem 1.2rem;border-radius:0 8px 8px 0;margin:1.2rem 0">
      <p style="margin:0;font-size:.95rem;color:#1e3a8a;font-weight:600">
        Ce que ça représente concrètement pour {count} résidences sans outil dédié :
      </p>
      <ul style="margin:.6rem 0 0;padding-left:1.2rem;color:#1e3a8a;font-size:.9rem;line-height:1.9">
        <li>Relances impayés à gérer manuellement sur chaque résidence</li>
        <li>Rapports financiers à produire séparément pour chaque AG</li>
        <li>Zéro traçabilité commune entre vos résidences</li>
        <li>Risque d'oubli sur les délais légaux (assemblées, PV, mandats)</li>
      </ul>
    </div>

    <p style="font-size:1rem;line-height:1.7">
      <strong>SyndicPro</strong> vous permet de gérer chaque résidence avec le même outil,
      la même logique, la même interface — sans réapprendre ni jongler entre plusieurs solutions.
    </p>

    <table style="width:100%;border-collapse:collapse;font-size:.9rem;margin:1.2rem 0">
      <tr style="background:#1e293b;color:#fff">
        <th style="padding:.7rem 1rem;text-align:left;border-radius:6px 0 0 0">Fonctionnalité</th>
        <th style="padding:.7rem;text-align:center;border-radius:0 6px 0 0">Disponible par résidence</th>
      </tr>
      <tr style="background:#f8fafc"><td style="padding:.65rem 1rem;border-bottom:1px solid #e2e8f0">Encaissements et suivi des impayés</td><td style="text-align:center;border-bottom:1px solid #e2e8f0;color:#10B981">✅ Temps réel</td></tr>
      <tr><td style="padding:.65rem 1rem;border-bottom:1px solid #e2e8f0">Rapports financiers PDF</td><td style="text-align:center;border-bottom:1px solid #e2e8f0;color:#10B981">✅ 1 clic</td></tr>
      <tr style="background:#f8fafc"><td style="padding:.65rem 1rem;border-bottom:1px solid #e2e8f0">Portail résident en ligne</td><td style="text-align:center;border-bottom:1px solid #e2e8f0;color:#10B981">✅ Par résidence</td></tr>
      <tr><td style="padding:.65rem 1rem;border-bottom:1px solid #e2e8f0">Notifications WhatsApp</td><td style="text-align:center;border-bottom:1px solid #e2e8f0;color:#10B981">✅ Automatiques</td></tr>
      <tr style="background:#f8fafc"><td style="padding:.65rem 1rem">Assemblées générales & PV légaux</td><td style="text-align:center;color:#10B981">✅ Inclus</td></tr>
    </table>

    <div style="text-align:center;margin:1.8rem 0">
      <a href="https://www.syndicpro.tn/demo"
         style="background:linear-gradient(135deg,#1D4ED8,#1e40af);color:#fff;text-decoration:none;
                padding:1rem 2.2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block;letter-spacing:.01em">
        Demander une démo personnalisée →
      </a>
      <p style="font-size:.8rem;color:#94a3b8;margin:.6rem 0 0">30 minutes. Adapté à votre portefeuille. Sans engagement.</p>
    </div>

    <hr style="border:none;border-top:1px solid #e2e8f0;margin:1.5rem 0">

    <p style="font-size:.85rem;color:#94a3b8;margin:0">
      Cordialement,<br>
      <strong style="color:#1e293b">{from_name}</strong><br>
      <a href="https://www.syndicpro.tn" style="color:#3B82F6">www.syndicpro.tn</a> · <a href="mailto:{from_email}" style="color:#94a3b8">{from_email}</a>
    </p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    2: {
        "subject": "Gérer {count} résidences avec un seul outil — ce que ça change",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#065f46,#047857);padding:1.8rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
    <p style="color:#a7f3d0;margin:.4rem 0 0;font-size:.85rem">Analyse ROI — {count} résidences</p>
  </div>

  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p style="font-size:1rem;line-height:1.7">Bonjour{president_str},</p>

    <p style="font-size:1rem;line-height:1.7">
      Je fais suite à mon message de l'autre jour. Permettez-moi de vous présenter
      un calcul concret basé sur votre situation : <strong>{count} résidences à gérer</strong>.
    </p>

    <div style="background:#f0fdf4;border:1.5px solid #16a34a;border-radius:10px;padding:1.4rem;margin:1.2rem 0">
      <p style="font-size:1rem;font-weight:700;color:#065f46;margin:0 0 .6rem">📋 Ce que SyndicPro remplace pour chacune de vos {count} résidences</p>
      <ul style="margin:0;padding-left:1.2rem;color:#065f46;font-size:.9rem;line-height:2">
        <li>Fichiers Excel séparés par résidence</li>
        <li>Relances manuelles par téléphone ou WhatsApp</li>
        <li>Calculs et rapports financiers faits à la main pour chaque AG</li>
        <li>Aucune traçabilité des paiements ni des tickets résidents</li>
      </ul>
    </div>

    <p style="font-size:.95rem;line-height:1.7;color:#374151">
      Le gain réel varie selon votre organisation, mais le principe reste le même pour chaque résidence :
      moins de saisie manuelle, moins d'oublis, moins de temps perdu en fin de mois.
    </p>

    <div style="text-align:center;margin:1.8rem 0">
      <a href="https://www.syndicpro.tn/demo"
         style="background:#16a34a;color:#fff;text-decoration:none;
                padding:1rem 2.2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block">
        Voir la démo pour votre portefeuille →
      </a>
    </div>

    <p style="font-size:.85rem;color:#64748b">
      Cordialement,<br>
      <strong style="color:#1e293b">{from_name}</strong> — SyndicPro<br>
      <a href="mailto:{from_email}" style="color:#3B82F6">{from_email}</a>
    </p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    3: {
        "subject": "Les 3 risques réels d'un portefeuille de {count} résidences sans outil dédié",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#7C3AED,#6D28D9);padding:1.8rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
    <p style="color:#ddd6fe;margin:.3rem 0 0;font-size:.85rem">Gestion de portefeuille immobilier</p>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p style="font-size:1rem;line-height:1.7">
      Gérer {count} résidences en parallèle expose à des risques spécifiques que
      les gestionnaires mono-résidence ne rencontrent pas. En voici trois concrets :
    </p>

    <div style="margin:1.2rem 0">
      <div style="background:#fef2f2;border-left:4px solid #dc2626;padding:.9rem 1rem;border-radius:0 8px 8px 0;margin-bottom:.8rem">
        <p style="margin:0;font-weight:700;color:#991b1b">⚠ Risque 1 — Impayés croisés non détectés</p>
        <p style="margin:.4rem 0 0;font-size:.9rem;color:#7f1d1d">
          Un résident qui déménage d'une résidence à l'autre en laissant des dettes.
          Sans vue consolidée, chaque résidence ignore ce que sait l'autre.
        </p>
      </div>

      <div style="background:#fffbeb;border-left:4px solid #d97706;padding:.9rem 1rem;border-radius:0 8px 8px 0;margin-bottom:.8rem">
        <p style="margin:0;font-weight:700;color:#92400e">⚠ Risque 2 — Défaut de convocation d'assemblée</p>
        <p style="margin:.4rem 0 0;font-size:.9rem;color:#78350f">
          Avec {count} assemblées à organiser sur l'année, un oubli de PV ou de délai légal
          peut engager votre responsabilité personnelle.
        </p>
      </div>

      <div style="background:#f0f9ff;border-left:4px solid #0284c7;padding:.9rem 1rem;border-radius:0 8px 8px 0">
        <p style="margin:0;font-weight:700;color:#0c4a6e">⚠ Risque 3 — Charge mentale = erreurs</p>
        <p style="margin:.4rem 0 0;font-size:.9rem;color:#0c4a6e">
          Jongler entre {count} fichiers Excel, {count} WhatsApp groups et {count} cahiers
          de caisse augmente mécaniquement le taux d'erreur avec le volume.
        </p>
      </div>
    </div>

    <p style="font-size:.95rem;line-height:1.7">
      SyndicPro vous donne les mêmes outils sur chaque résidence — suivi des impayés,
      relances automatiques, rapports financiers, gestion des AG — avec la même interface,
      sans réapprendre à chaque fois.
    </p>

    <div style="text-align:center;margin:1.8rem 0">
      <a href="https://www.syndicpro.tn/demo"
         style="background:#7C3AED;color:#fff;text-decoration:none;
                padding:1rem 2.2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block">
        Planifier une démo →
      </a>
    </div>

    <p style="font-size:.85rem;color:#64748b">
      Cordialement,<br>
      <strong style="color:#1e293b">{from_name}</strong> — SyndicPro
    </p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },

    4: {
        "subject": "Proposition partenaire gestionnaire — {count} résidences",
        "body_html": """
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:#1e293b;padding:1.8rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">SyndicPro</h1>
    <p style="color:#94a3b8;margin:.3rem 0 0;font-size:.85rem">Offre gestionnaire professionnel</p>
  </div>
  <div style="background:#f8fafc;padding:2rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">
    <p>Bonjour{president_str},</p>
    <p style="font-size:1rem;line-height:1.7">
      C'est mon dernier message. Mais avant de clore ce sujet, je souhaitais vous faire
      une proposition adaptée à votre profil de gestionnaire multi-résidences.
    </p>

    <div style="background:#fff;border:2px solid #1D4ED8;border-radius:10px;padding:1.4rem;margin:1.2rem 0">
      <p style="font-size:1rem;font-weight:700;color:#1e40af;margin:0 0 .8rem">🤝 Ce que je vous propose</p>
      <p style="font-size:.95rem;color:#374151;line-height:1.7;margin:0">
        Une discussion de 30 minutes pour voir comment SyndicPro peut s'adapter
        à votre façon de travailler sur vos {count} résidences — sans engagement,
        à votre rythme. Si ça correspond, on déroule ensemble. Sinon, pas de pression.
      </p>
    </div>

    <p style="font-size:.95rem;line-height:1.7">
      Si ce n'est pas le bon moment, pas de problème — notre offre reste disponible.
      Mais si vous souhaitez en discuter, une réponse à cet email suffit.
    </p>

    <div style="text-align:center;margin:1.8rem 0">
      <a href="mailto:{from_email}?subject=Offre partenaire gestionnaire {count} résidences — SyndicPro"
         style="background:#1D4ED8;color:#fff;text-decoration:none;
                padding:1rem 2.2rem;border-radius:8px;font-weight:700;font-size:1rem;display:inline-block">
        Répondre à cette proposition →
      </a>
    </div>

    <p style="font-size:.9rem;color:#64748b">
      À bientôt peut-être,<br>
      <strong style="color:#1e293b">{from_name}</strong> — SyndicPro<br>
      <a href="mailto:{from_email}" style="color:#3B82F6">{from_email}</a>
    </p>
    <p style="font-size:.75rem;color:#cbd5e1;margin-top:1rem">Pour ne plus recevoir ces emails, répondez "DÉSABONNER".</p>
    {pixel}
  </div>
</div>""",
    },
}


def _extract_president_last_name(president_full: str) -> str:
    """Extrait le prénom/premier mot pour personnalisation."""
    if not president_full:
        return ""
    parts = president_full.strip().split()
    if not parts:
        return ""
    # Format RNE: NOM PRENOM → on prend le dernier mot (prénom)
    return parts[-1].capitalize()


def build_seq_email(step: int, contact: dict, track_id: str, base_url: str) -> tuple[str, str]:
    """Retourne (subject, html) pour l'étape step de la séquence."""
    is_multi = bool(contact.get("is_multi"))
    template_bank = SEQ_MULTI_TEMPLATES if is_multi else SEQ_TEMPLATES
    tpl = template_bank.get(step)
    if not tpl:
        raise ValueError(f"Étape de séquence invalide : {step}")

    name      = contact.get("name", "votre résidence")
    city      = contact.get("city", "")
    city_str  = f" à {city}" if city else ""
    pres_raw  = contact.get("president", "")
    pres_name = _extract_president_last_name(pres_raw)
    pres_str  = f" {pres_name}" if pres_name else ""
    pixel     = _PIXEL.format(track_id=track_id)

    if is_multi:
        count         = int(contact.get("multi_count") or 1)
        hours_lost    = count * 6
        total_mrr     = hours_lost * 20
        res_str       = contact.get("residences_str", "")
        res_names     = [r.strip() for r in res_str.split(",") if r.strip()][:3]
        if res_names:
            snippet = " (" + ", ".join(res_names) + ("…" if count > 3 else "") + ")"
        else:
            snippet = ""
        subject = tpl["subject"].format(count=count, hours_lost=hours_lost, total_mrr=total_mrr)
        html    = tpl["body_html"].format(
            count=count,
            hours_lost=hours_lost,
            total_mrr=total_mrr,
            residences_snippet=snippet,
            president_str=pres_str,
            from_name=EMAIL_FROM_NAME,
            from_email=EMAIL_FROM,
            pixel=pixel,
        )
    else:
        subject = tpl["subject"].format(name=name, city=city)
        html    = tpl["body_html"].format(
            name=name,
            city=city,
            city_str=city_str,
            president_str=pres_str,
            from_name=EMAIL_FROM_NAME,
            from_email=EMAIL_FROM,
            pixel=pixel,
        )
    return subject, html


def send_sequence_step(step: int, contact: dict, base_url: str) -> bool:
    """Envoie un email de séquence et met à jour la DB."""
    import uuid
    from db import log_seq_sent, advance_seq_step

    to_email = (contact.get("email") or "").strip()
    if not to_email:
        return False

    track_id = str(uuid.uuid4())
    try:
        subject, html = build_seq_email(step, contact, track_id, base_url)
        ok = send_email(to_email, subject, html)
        if ok:
            log_seq_sent(contact["id"], step, track_id, subject)
            advance_seq_step(contact["id"], step + 1)
            logger.info(f"[Seq] Étape {step} envoyée → {to_email} ({contact.get('name','')})")
        return ok
    except Exception as e:
        logger.error(f"[Seq] Erreur étape {step} pour {contact.get('name')}: {e}")
        return False


# ── Thread de fond: envoie automatiquement les emails dus ─────────────────────

_seq_worker_running = False
_seq_worker_lock    = threading.Lock()

BASE_URL = os.environ.get("BASE_URL", "http://localhost:10000")


def _seq_worker_loop():
    global _seq_worker_running
    from db import get_due_seq_emails
    logger.info("[Seq Worker] Démarré")
    while True:
        try:
            due = get_due_seq_emails()
            for c in due:
                step = c.get("seq_step", 1)
                send_sequence_step(step, c, BASE_URL)
                time.sleep(DELAY_BETWEEN_EMAILS)
        except Exception as e:
            logger.error(f"[Seq Worker] Erreur : {e}")
        time.sleep(300)  # vérifie toutes les 5 minutes


def start_seq_worker():
    global _seq_worker_running
    with _seq_worker_lock:
        if _seq_worker_running:
            return
        _seq_worker_running = True
    t = threading.Thread(target=_seq_worker_loop, daemon=True, name="seq-worker")
    t.start()
    logger.info("[Seq Worker] Thread lancé")
