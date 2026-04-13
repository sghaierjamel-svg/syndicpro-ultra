import pandas as pd
import asyncio
from scraper_engine import scrape_all
from scoring_engine import compute_conformity

async def enrich_excel(input_file, output_file):
    df = pd.read_excel(input_file)

    for i, row in df.iterrows():
        name = row.get("Nom Résidence (FR)", "")
        city = row.get("Ville", "")

        print(f"[{i}] {name}")

        results = await scrape_all(name, city)
        data = compute_conformity(results)

        df.at[i, "Téléphone"] = data["phone"]
        df.at[i, "Email"] = data["email"]
        df.at[i, "Confiance"] = data["global_conf"]

    df.to_excel(output_file, index=False)
    print("✅ Enrichissement terminé")
