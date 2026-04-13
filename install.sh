#!/bin/bash

echo "🔧 Installation en cours..."

cd backend

# environnement virtuel
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt

# playwright
playwright install

echo "✅ Installation terminée"
