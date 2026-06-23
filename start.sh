#!/bin/bash

PORT=${PORT:-10000}
LOGFILE="/tmp/scanner_syndic_backend.log"
TUNNEL_LOG="/tmp/scanner_syndic_tunnel.log"
PIDFILE="/tmp/scanner_syndic.pid"
TUNNEL_PIDFILE="/tmp/scanner_syndic_tunnel.pid"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "🚀 SyndicPro Scanner — Démarrage..."

# ── Variables d'environnement locales ────────────────────────────────────────
if [ -f "$SCRIPT_DIR/backend/.env.local" ]; then
    set -a
    source "$SCRIPT_DIR/backend/.env.local"
    set +a
    echo "  ✔ .env.local chargé"
fi

# ── Tuer les instances précédentes ───────────────────────────────────────────
if [ -f "$PIDFILE" ]; then
    OLD_PID=$(cat "$PIDFILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "  ⚠ Backend précédent (PID $OLD_PID) → arrêt..."
        kill "$OLD_PID" 2>/dev/null
        sleep 1
    fi
    rm -f "$PIDFILE"
fi
if [ -f "$TUNNEL_PIDFILE" ]; then
    OLD_TPID=$(cat "$TUNNEL_PIDFILE")
    if kill -0 "$OLD_TPID" 2>/dev/null; then
        kill "$OLD_TPID" 2>/dev/null
    fi
    rm -f "$TUNNEL_PIDFILE"
fi

# ── Vérifier que le venv existe ──────────────────────────────────────────────
if [ ! -f "$SCRIPT_DIR/backend/venv/bin/activate" ]; then
    echo "❌ Environnement virtuel introuvable. Lancez d'abord : bash install.sh"
    exit 1
fi

# ── Activer le venv et lancer Flask ──────────────────────────────────────────
cd "$SCRIPT_DIR/backend"
source venv/bin/activate

echo "  ▶ Démarrage backend sur le port $PORT..."
PORT=$PORT python app.py > "$LOGFILE" 2>&1 &
BACKEND_PID=$!
echo "$BACKEND_PID" > "$PIDFILE"

# ── Attendre que Flask réponde ───────────────────────────────────────────────
echo -n "  ⏳ En attente du serveur"
READY=0
for i in $(seq 1 25); do
    if curl -sf "http://localhost:$PORT/health" > /dev/null 2>&1; then
        READY=1
        break
    fi
    if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
        echo ""
        echo "❌ Le backend a planté. Logs :"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        tail -30 "$LOGFILE"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        exit 1
    fi
    printf "."
    sleep 1
done
echo ""

if [ $READY -eq 0 ]; then
    echo "❌ Timeout — Flask ne répond pas."
    tail -20 "$LOGFILE"
    exit 1
fi

echo "  ✅ Backend prêt → http://localhost:$PORT"

# ── Lancer le tunnel Cloudflare ──────────────────────────────────────────────
TUNNEL_URL=""
if command -v cloudflared &>/dev/null; then
    echo "  ☁  Démarrage du tunnel Cloudflare..."
    cloudflared tunnel --url "http://localhost:$PORT" --no-autoupdate > "$TUNNEL_LOG" 2>&1 &
    TUNNEL_PID=$!
    echo "$TUNNEL_PID" > "$TUNNEL_PIDFILE"

    # Attendre que le tunnel donne son URL (max 30s)
    echo -n "  ⏳ Connexion Cloudflare"
    for i in $(seq 1 30); do
        TUNNEL_URL=$(grep -o 'https://[a-zA-Z0-9\-]*\.trycloudflare\.com' "$TUNNEL_LOG" 2>/dev/null | head -1)
        if [ -n "$TUNNEL_URL" ]; then
            break
        fi
        printf "."
        sleep 1
    done
    echo ""

    if [ -n "$TUNNEL_URL" ]; then
        # Mettre à jour BASE_URL dans .env.local
        ENV_FILE="$SCRIPT_DIR/backend/.env.local"
        if grep -q "^BASE_URL=" "$ENV_FILE" 2>/dev/null; then
            sed -i "s|^BASE_URL=.*|BASE_URL=$TUNNEL_URL|" "$ENV_FILE"
        else
            echo "BASE_URL=$TUNNEL_URL" >> "$ENV_FILE"
        fi

        # Redémarrer Flask avec la nouvelle BASE_URL
        kill "$BACKEND_PID" 2>/dev/null
        sleep 1
        set -a; source "$ENV_FILE"; set +a
        PORT=$PORT python app.py > "$LOGFILE" 2>&1 &
        BACKEND_PID=$!
        echo "$BACKEND_PID" > "$PIDFILE"
        sleep 2

        echo "  ✅ Tunnel actif → $TUNNEL_URL"
        echo "  📧 Tracking emails activé"
    else
        echo "  ⚠  Tunnel Cloudflare indisponible — tracking désactivé"
    fi
else
    echo "  ⚠  cloudflared non installé — tracking désactivé"
fi

# ── Nettoyage à la fermeture ─────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "  🛑 Arrêt..."
    kill "$BACKEND_PID" 2>/dev/null || true
    kill "$TUNNEL_PID"  2>/dev/null || true
    rm -f "$PIDFILE" "$TUNNEL_PIDFILE"
}
trap cleanup EXIT INT TERM

# ── Ouvrir le navigateur ─────────────────────────────────────────────────────
xdg-open "http://localhost:$PORT" 2>/dev/null &

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SyndicPro Scanner  │  PID $BACKEND_PID  │  Port $PORT"
if [ -n "$TUNNEL_URL" ]; then
echo "  Tunnel public      │  $TUNNEL_URL"
fi
echo "  Logs  :  tail -f $LOGFILE"
echo "  Ctrl+C pour arrêter"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

wait "$BACKEND_PID"
