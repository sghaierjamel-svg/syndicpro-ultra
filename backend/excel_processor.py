"""
Enrichissement Excel — v3
- Colonnes de sortie : téléphone, email, site, président, membres, adresse, confiance, sources
- Callback de progression pour le traitement asynchrone
- context optionnel par ligne
"""

import io
import time
import json
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
from scraper_engine import scrape_all
from scoring_engine import compute_conformity


def enrich_excel(file_obj, progress_callback=None, context=""):
    """
    Enrichit un fichier Excel et retourne les bytes du fichier résultat.

    progress_callback(current, total) — appelé après chaque ligne traitée.
    """
    wb = openpyxl.load_workbook(file_obj)
    ws = wb.active

    # ── Cartographie des entêtes ──────────────────────────────────────────────
    headers = {}
    header_row = 1
    # Chercher la ligne d'entête dans les 5 premières lignes
    for ri in range(1, 6):
        row_vals = [str(ws.cell(ri, c).value or "").strip() for c in range(1, ws.max_column + 1)]
        if any(v for v in row_vals):
            for ci, v in enumerate(row_vals, 1):
                if v:
                    headers[v] = ci
            if headers:
                header_row = ri
                break

    def col_idx(fragments):
        """Trouve la colonne dont l'entête contient l'un des fragments."""
        for frag in fragments:
            for h, i in headers.items():
                if frag.lower() in h.lower():
                    return i
        return None

    def ensure_col(label):
        if label in headers:
            return headers[label]
        idx = ws.max_column + 1
        cell = ws.cell(header_row, idx, label)
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1E3A5F")
        cell.alignment = Alignment(horizontal="center", wrap_text=True)
        headers[label] = idx
        return idx

    name_col = col_idx(["Nom Résidence", "Nom Residence", "Dénomination", "Denomination", "Nom"]) or 1
    city_col = col_idx(["Ville", "Gouvernorat", "City"]) or 2
    ctx_col  = col_idx(["Type", "Activité", "Activite", "Context"])   # optionnel
    rne_col  = col_idx(["ID RNE", "RNE", "Identifiant", "rne_id"])    # optionnel

    # Colonnes résultat
    phone_col    = ensure_col("Téléphone")
    email_col    = ensure_col("Email")
    web_col      = ensure_col("Site Web")
    pres_col     = ensure_col("Président / Gérant")
    members_col  = ensure_col("Membres (bureau)")
    address_col  = ensure_col("Adresse officielle")
    conf_col     = ensure_col("Confiance (%)")
    src_col      = ensure_col("Sources")
    phones_col   = ensure_col("Tous les téléphones")
    emails_col   = ensure_col("Tous les emails")
    rne_out_col  = ensure_col("ID RNE")

    # Compter les lignes à traiter
    data_rows = [
        ri for ri in range(header_row + 1, ws.max_row + 1)
        if str(ws.cell(ri, name_col).value or "").strip()
    ]
    total = len(data_rows)

    # ── Traitement ligne par ligne ────────────────────────────────────────────
    for idx, row_num in enumerate(data_rows, 1):
        name   = str(ws.cell(row_num, name_col).value or "").strip()
        city   = str(ws.cell(row_num, city_col).value or "").strip()
        ctx    = str(ws.cell(row_num, ctx_col).value or "").strip() if ctx_col else context
        rne_id = str(ws.cell(row_num, rne_col).value or "").strip() if rne_col else ""
        if not name or not city:
            if progress_callback:
                progress_callback(idx, total)
            continue

        try:
            raw     = scrape_all(name, city, rne_id=rne_id, context=ctx)
            data    = compute_conformity(raw)

            members_str = " | ".join(
                f"{m['qualite']}: {m['nom']}"
                for m in data.get("members", [])
            )

            ws.cell(row_num, phone_col,   data.get("phone",    ""))
            ws.cell(row_num, email_col,   data.get("email",    ""))
            ws.cell(row_num, web_col,     data.get("website",  ""))
            ws.cell(row_num, pres_col,    data.get("president",""))
            ws.cell(row_num, members_col, members_str)
            ws.cell(row_num, address_col, data.get("address",  ""))
            ws.cell(row_num, conf_col,    data.get("global_conf", 0))
            ws.cell(row_num, src_col,     ", ".join(data.get("sources_hit", [])))
            ws.cell(row_num, phones_col,  ", ".join(data.get("all_phones",  [])))
            ws.cell(row_num, emails_col,  ", ".join(data.get("all_emails",  [])))
            ws.cell(row_num, rne_out_col, data.get("rne_id", "") or rne_id)

            # Colorier la ligne si trouvé
            if data.get("found"):
                for c in range(1, ws.max_column + 1):
                    ws.cell(row_num, c).fill = PatternFill("solid", fgColor="E8F5E9")

        except Exception as e:
            ws.cell(row_num, src_col, f"Erreur: {e}")

        if progress_callback:
            progress_callback(idx, total)

        # Politesse entre requêtes (évite le ban)
        time.sleep(0.3)

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()
