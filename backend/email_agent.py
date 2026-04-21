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
    msg["From"]    = f"{EMAIL_FROM_NAME} <{SMTP_USER}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, to_email, msg.as_string())
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
