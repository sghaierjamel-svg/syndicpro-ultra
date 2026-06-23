"""
Email Watcher — surveillance IMAP contact@syndicpro.tn
- Vérifie les nouvelles réponses toutes les 5 minutes
- Détecte si l'expéditeur est un prospect dans la DB
- Met à jour le pipeline (→ interested)
- Envoie une alerte à sghaierjamel@gmail.com
- Notification bureau Linux (notify-send)
"""

import os
import imaplib
import email
import smtplib
import sqlite3
import logging
import time
import subprocess
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
IMAP_HOST   = "imap.zoho.com"
IMAP_PORT   = 993
SMTP_HOST   = os.environ.get("SMTP_HOST", "smtp.zoho.com")
SMTP_PORT   = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER   = os.environ.get("SMTP_USER", "contact@syndicpro.tn")
SMTP_PASS   = os.environ.get("SMTP_PASS", "")
ALERT_EMAIL = "sghaierjamel@gmail.com"
DB_PATH     = os.path.join(os.path.dirname(__file__), "data.db")
POLL_EVERY  = 300   # secondes (5 minutes)
LOG_FILE    = "/tmp/scanner_watcher.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [Watcher] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger("watcher")

# ── Helpers DB ────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def find_prospect_by_email(sender_email: str):
    """Cherche un prospect dans la DB par email (insensible à la casse)."""
    conn = get_conn()
    row = conn.execute(
        "SELECT id, name, city, pipeline_status FROM results WHERE LOWER(email) = LOWER(?)",
        (sender_email.strip(),)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def mark_as_interested(prospect_id: int):
    """Met le statut pipeline → interested et log la réponse."""
    conn = get_conn()
    conn.execute(
        "UPDATE results SET pipeline_status = 'interested' WHERE id = ? AND pipeline_status NOT IN ('demo','client')",
        (prospect_id,)
    )
    conn.commit()
    conn.close()


def mark_unsubscribed(prospect_id: int):
    """Marque le contact comme perdu (DÉSABONNER reçu)."""
    conn = get_conn()
    conn.execute(
        "UPDATE results SET pipeline_status = 'lost', seq_paused = 1 WHERE id = ?",
        (prospect_id,)
    )
    conn.commit()
    conn.close()

# ── Décodage email ────────────────────────────────────────────────────────────

def decode_str(value):
    if not value:
        return ""
    parts = decode_header(value)
    result = ""
    for part, enc in parts:
        if isinstance(part, bytes):
            result += part.decode(enc or "utf-8", errors="replace")
        else:
            result += part
    return result


def get_body(msg) -> str:
    """Extrait le corps texte de l'email."""
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            if ct == "text/plain" and "attachment" not in cd:
                try:
                    body = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace"
                    )
                    break
                except Exception:
                    pass
    else:
        try:
            body = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace"
            )
        except Exception:
            pass
    return body.strip()


def classify_reply(body: str) -> str:
    """Classifie la réponse du prospect."""
    body_lower = body.lower()
    if any(w in body_lower for w in ["désabonner", "desabonner", "arrêter", "stop", "ne plus recevoir", "supprimer"]):
        return "unsubscribe"
    if any(w in body_lower for w in ["intéressé", "interesse", "démo", "demo", "essai", "gratuit", "contact", "rendez-vous", "rdv", "rappel", "rappeler"]):
        return "interested"
    if any(w in body_lower for w in ["prix", "tarif", "coût", "cout", "abonnement", "combien", "forfait"]):
        return "price_question"
    if "?" in body:
        return "question"
    return "other"

# ── Notification ──────────────────────────────────────────────────────────────

def send_alert(prospect: dict, sender: str, subject: str, body: str, reply_type: str):
    """Envoie un email d'alerte à sghaierjamel@gmail.com."""

    type_labels = {
        "interested":    "🔥 INTÉRESSÉ — action requise",
        "price_question":"💰 Demande de prix",
        "question":      "❓ Question",
        "unsubscribe":   "🚫 Désabonnement",
        "other":         "📩 Réponse reçue",
    }
    label = type_labels.get(reply_type, "📩 Réponse reçue")

    html = f"""
<div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#1e293b">
  <div style="background:linear-gradient(135deg,#1D4ED8,#3B82F6);padding:1.5rem;border-radius:12px 12px 0 0;text-align:center">
    <h1 style="color:#fff;margin:0;font-size:1.4rem">📬 SyndicPro Scanner</h1>
    <p style="color:#bfdbfe;margin:.4rem 0 0;font-size:.9rem">Réponse d'un prospect détectée</p>
  </div>
  <div style="background:#f8fafc;padding:1.5rem;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px">

    <div style="background:#fff;border-left:4px solid {'#10b981' if reply_type=='interested' else '#3b82f6'};padding:1rem 1.2rem;border-radius:0 8px 8px 0;margin-bottom:1.2rem">
      <p style="font-size:1.1rem;font-weight:700;margin:0;color:{'#065f46' if reply_type=='interested' else '#1d4ed8'}">{label}</p>
    </div>

    <table style="width:100%;font-size:.9rem;margin-bottom:1.2rem">
      <tr><td style="color:#64748b;padding:.3rem 0;width:120px">Résidence</td><td style="font-weight:700">{prospect['name']}</td></tr>
      <tr><td style="color:#64748b;padding:.3rem 0">Ville</td><td>{prospect['city']}</td></tr>
      <tr><td style="color:#64748b;padding:.3rem 0">Email</td><td><a href="mailto:{sender}" style="color:#3b82f6">{sender}</a></td></tr>
      <tr><td style="color:#64748b;padding:.3rem 0">Objet</td><td>{subject}</td></tr>
      <tr><td style="color:#64748b;padding:.3rem 0">Reçu le</td><td>{datetime.now().strftime('%d/%m/%Y à %H:%M')}</td></tr>
    </table>

    <div style="background:#f1f5f9;border-radius:8px;padding:1rem;margin-bottom:1.2rem">
      <p style="font-size:.78rem;color:#64748b;margin:0 0 .5rem;font-weight:700;text-transform:uppercase">Message du prospect :</p>
      <p style="font-size:.9rem;color:#1e293b;margin:0;white-space:pre-wrap">{body[:800]}</p>
    </div>

    <div style="text-align:center">
      <a href="http://localhost:10000/pipeline"
         style="background:#1D4ED8;color:#fff;text-decoration:none;padding:.8rem 1.8rem;border-radius:8px;font-weight:700;display:inline-block">
        Voir le pipeline →
      </a>
    </div>

    <p style="font-size:.75rem;color:#94a3b8;margin-top:1.2rem;text-align:center">
      SyndicPro Scanner — Outil interne
    </p>
  </div>
</div>
"""

    subject_alert = f"[Scanner] {label} — {prospect['name']} ({prospect['city']})"

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject_alert
        msg["From"]    = f"SyndicPro Scanner <{SMTP_USER}>"
        msg["To"]      = ALERT_EMAIL
        msg.attach(MIMEText(html, "html", "utf-8"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, ALERT_EMAIL, msg.as_string())

        log.info(f"✅ Alerte envoyée à {ALERT_EMAIL} — {prospect['name']}")
    except Exception as e:
        log.error(f"❌ Erreur envoi alerte : {e}")


def desktop_notify(title: str, body: str):
    """Notification bureau Linux."""
    try:
        subprocess.Popen(
            ["notify-send", "-i", "mail-message-new", "-t", "10000", title, body],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except Exception:
        pass

# ── IMAP checker ──────────────────────────────────────────────────────────────

def check_inbox():
    """Se connecte à Zoho IMAP et traite les nouveaux emails."""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
        mail.login(SMTP_USER, SMTP_PASS)
        mail.select("INBOX")

        # Chercher emails non lus
        _, msg_ids = mail.search(None, "UNSEEN")
        ids = msg_ids[0].split()

        if not ids:
            log.info(f"Boîte vide — {len(ids)} nouveaux messages")
            mail.logout()
            return 0

        log.info(f"📬 {len(ids)} nouveaux email(s) à vérifier")
        processed = 0

        for msg_id in ids:
            try:
                _, data = mail.fetch(msg_id, "(RFC822)")
                raw = data[0][1]
                msg = email.message_from_bytes(raw)

                # Extraire infos
                from_header = decode_str(msg.get("From", ""))
                subject     = decode_str(msg.get("Subject", ""))
                body        = get_body(msg)

                # Extraire l'email de l'expéditeur
                sender_email = ""
                if "<" in from_header and ">" in from_header:
                    sender_email = from_header.split("<")[-1].rstrip(">").strip()
                else:
                    sender_email = from_header.strip()

                log.info(f"Email de : {sender_email} | Sujet : {subject[:50]}")

                # Chercher dans la DB
                prospect = find_prospect_by_email(sender_email)

                if prospect:
                    log.info(f"✅ Prospect identifié : {prospect['name']} ({prospect['city']})")

                    reply_type = classify_reply(body)
                    log.info(f"Classification : {reply_type}")

                    # Mettre à jour le pipeline
                    if reply_type == "unsubscribe":
                        mark_unsubscribed(prospect["id"])
                        log.info(f"🚫 {prospect['name']} → lost (désabonnement)")
                    else:
                        mark_as_interested(prospect["id"])
                        log.info(f"🔥 {prospect['name']} → interested")

                    # Envoyer alerte email + notif bureau
                    send_alert(prospect, sender_email, subject, body, reply_type)

                    type_labels = {
                        "interested":    "🔥 INTÉRESSÉ",
                        "price_question":"💰 Demande de prix",
                        "question":      "❓ Question",
                        "unsubscribe":   "🚫 Désabonnement",
                        "other":         "📩 Réponse",
                    }
                    desktop_notify(
                        f"Scanner Syndic — {type_labels.get(reply_type, 'Réponse')}",
                        f"{prospect['name']} ({prospect['city']}) a répondu !"
                    )
                    processed += 1
                else:
                    log.info(f"⚠  Expéditeur inconnu (pas dans la DB) : {sender_email}")

            except Exception as e:
                log.error(f"Erreur traitement email {msg_id} : {e}")

        mail.logout()
        return processed

    except imaplib.IMAP4.error as e:
        log.error(f"Erreur IMAP : {e}")
        return 0
    except Exception as e:
        log.error(f"Erreur connexion : {e}")
        return 0

# ── Boucle principale ─────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("SyndicPro Scanner — Email Watcher démarré")
    log.info(f"Surveillance : {SMTP_USER}")
    log.info(f"Alertes vers : {ALERT_EMAIL}")
    log.info(f"Polling toutes les {POLL_EVERY // 60} minutes")
    log.info("=" * 60)

    while True:
        try:
            processed = check_inbox()
            if processed > 0:
                log.info(f"✅ {processed} réponse(s) de prospect(s) traitée(s)")
        except Exception as e:
            log.error(f"Erreur boucle principale : {e}")

        time.sleep(POLL_EVERY)


if __name__ == "__main__":
    main()
