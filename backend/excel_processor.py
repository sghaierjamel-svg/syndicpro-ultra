"""
Enrichissement Excel — accepte un objet fichier upload Flask, retourne des bytes Excel.
"""
import io
import time
import openpyxl
from scraper_engine import scrape_all
from scoring_engine import compute_conformity


def enrich_excel(file_obj):
    """
    Prend un FileStorage Flask et retourne les bytes d'un Excel enrichi.
    Colonnes attendues : 'Nom Résidence (FR)' et 'Ville' (ou 'Nom' et 'Ville').
    """
    wb = openpyxl.load_workbook(file_obj)
    ws = wb.active

    # Cartographier les entêtes existants → numéro de colonne
    headers = {}
    for cell in ws[1]:
        if cell.value:
            headers[str(cell.value).strip()] = cell.column

    def ensure_col(name):
        """Crée la colonne de résultat si absente, retourne son index."""
        if name in headers:
            return headers[name]
        new_col = ws.max_column + 1
        ws.cell(row=1, column=new_col, value=name)
        headers[name] = new_col
        return new_col

    # Colonnes source (flexible)
    name_col = (headers.get("Nom Résidence (FR)")
                or headers.get("Nom Résidence")
                or headers.get("Nom")
                or 1)
    city_col = (headers.get("Ville")
                or headers.get("Gouvernorat")
                or 2)

    # Colonnes résultat à ajouter
    phone_col = ensure_col("Téléphone")
    email_col = ensure_col("Email")
    web_col   = ensure_col("Site Web")
    conf_col  = ensure_col("Confiance (%)")
    src_col   = ensure_col("Sources")
    phones_col = ensure_col("Tous les téléphones")
    emails_col = ensure_col("Tous les emails")

    # Traiter chaque ligne de données
    for row_num in range(2, ws.max_row + 1):
        name = str(ws.cell(row=row_num, column=name_col).value or "").strip()
        city = str(ws.cell(row=row_num, column=city_col).value or "").strip()
        if not name or not city:
            continue

        try:
            results = scrape_all(name, city)
            data = compute_conformity(results)

            ws.cell(row=row_num, column=phone_col,  value=data.get("phone", ""))
            ws.cell(row=row_num, column=email_col,  value=data.get("email", ""))
            ws.cell(row=row_num, column=web_col,    value=data.get("website", ""))
            ws.cell(row=row_num, column=conf_col,   value=data.get("global_conf", 0))
            ws.cell(row=row_num, column=src_col,
                    value=", ".join(data.get("sources_hit", [])))
            ws.cell(row=row_num, column=phones_col,
                    value=", ".join(data.get("all_phones", [])))
            ws.cell(row=row_num, column=emails_col,
                    value=", ".join(data.get("all_emails", [])))
        except Exception:
            pass

        time.sleep(0.5)  # politesse entre chaque scraping

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()
