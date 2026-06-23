#!/bin/bash

echo "🔧 Installation SyndicPro Scanner..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Vérifier Python 3 ─────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo "❌ Python 3 introuvable. Installez-le avec : sudo apt install python3 python3-venv"
    exit 1
fi

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "  Python $PYTHON_VER détecté"

# ── Vérifier curl (utilisé par start.sh pour détecter que Flask est prêt) ─────
if ! command -v curl &>/dev/null; then
    echo "  ⚠ curl non détecté — installation..."
    sudo apt-get install -y curl 2>/dev/null || echo "  ⚠ Installez curl manuellement : sudo apt install curl"
fi

# ── Créer le venv ─────────────────────────────────────────────────────────────
cd "$SCRIPT_DIR/backend"

if [ ! -d "venv" ]; then
    echo "  Création de l'environnement virtuel..."
    python3 -m venv venv
fi

# ── Activer et installer les dépendances ──────────────────────────────────────
# shellcheck disable=SC1091
source venv/bin/activate

echo "  Mise à jour pip..."
pip install --upgrade pip -q

echo "  Installation des dépendances..."
pip install -r requirements.txt -q

echo ""
echo "✅ Installation terminée !"
echo ""
echo "  Pour lancer le scanner :"
echo "    bash $SCRIPT_DIR/start.sh"
echo ""
echo "  Variables optionnelles (.env.local) :"
if [ -f "$SCRIPT_DIR/backend/.env.local.example" ]; then
    echo "    Copiez backend/.env.local.example → backend/.env.local"
    echo "    puis remplissez vos credentials."
fi
