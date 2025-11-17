from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Autorise React à appeler l'API

DB_PATH = "maintenance.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/api/matricules')
def get_matricules():
    conn = get_db_connection()
    matricules = conn.execute('SELECT * FROM Matricules').fetchall()
    conn.close()
    return jsonify([dict(row) for row in matricules])

@app.route('/api/planning/<int:annee>')
def get_planning(annee):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Union des 3 tables de planning
    cursor.execute(f"""
        SELECT 'Controle' as type, matricule, nom_entretien, date_prevue, statut 
        FROM Planning_Controle 
        WHERE strftime('%Y', date_prevue) = ?
        UNION ALL
        SELECT 'Nettoyage', matricule, nom_entretien, date_prevue, statut 
        FROM Planning_Nettoyage 
        WHERE strftime('%Y', date_prevue) = ?
        UNION ALL
        SELECT 'Changement', matricule, nom_entretien, date_prevue, statut 
        FROM Planning_Changement 
        WHERE strftime('%Y', date_prevue) = ?
        ORDER BY date_prevue
    """, (str(annee), str(annee), str(annee)))
    
    planning = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(planning)

@app.route('/api/sync-status')
def sync_status():
    conn = get_db_connection()
    logs = conn.execute("""
        SELECT type_sync, dernier_nbsi, date_sync, nb_lignes_ajoutees, statut 
        FROM Sync_Log 
        ORDER BY date_sync DESC LIMIT 5
    """).fetchall()
    conn.close()
    return jsonify([dict(row) for row in logs])

if __name__ == '__main__':
    print("API Flask lancée sur http://0.0.0.0:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)