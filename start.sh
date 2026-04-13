#!/bin/bash

echo "🚀 Lancement SyndicPro Ultra..."

# aller dans backend
cd backend

# activer environnement
source venv/bin/activate

# lancer backend en arrière-plan
echo "▶️ Backend en cours..."
python app.py &

sleep 3

# ouvrir frontend
echo "🌐 Ouverture interface..."
xdg-open ../frontend/index.html

echo "✅ Application lancée"
