#!/bin/bash

echo "🚀 Démarrage du backend Flask..."
python backend/api.py &

echo "🛠  Build du frontend React (15-30 secondes)..."
cd frontend
npm run build

echo "✅ Lancement du serveur statique (ultra stable dans Codespaces)"
serve -s build -l 3000