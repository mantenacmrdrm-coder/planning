import sqlite3
import csv
from datetime import datetime, timedelta
from typing import List, Optional
import unicodedata
import os

# =============================================================================
# 1. DATABASE
# =============================================================================
class Database:
    def __init__(self, db_path: str = "maintenance.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()

    def initialize_schema(self):
        cursor = self.conn.cursor()
        # Tables
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Matricules (
            matricule TEXT PRIMARY KEY, designation TEXT NOT NULL, annee INTEGER,
            qte_vidange INTEGER, code_barre TEXT, marque TEXT, pneumatique TEXT, categorie TEXT NOT NULL
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Entretiens_Types (
            id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT UNIQUE NOT NULL, groupe TEXT
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Parametrage (
            id INTEGER PRIMARY KEY AUTOINCREMENT, entretien_nom TEXT NOT NULL,
            type_intervention TEXT NOT NULL, intervalle_jours INTEGER NOT NULL,
            CHECK (type_intervention IN ('C', 'N', 'CH'))
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Exclusions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, categorie TEXT NOT NULL,
            entretien_exclu TEXT NOT NULL, UNIQUE(categorie, entretien_exclu)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Historique_Preventif (
            id INTEGER PRIMARY KEY AUTOINCREMENT, matricule TEXT NOT NULL,
            nom_entretien TEXT NOT NULL, type_intervention TEXT, date_realisation DATE NOT NULL,
            compteur_km_h REAL, observations TEXT, source_fichier TEXT, nb_si TEXT,
            FOREIGN KEY (matricule) REFERENCES Matricules(matricule)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Historique_Curatif (
            id INTEGER PRIMARY KEY AUTOINCREMENT, matricule TEXT NOT NULL, categorie TEXT,
            designation TEXT, date_entree DATE, panne_declaree TEXT, situation_actuelle TEXT,
            pieces TEXT, date_sortie DATE, intervenant TEXT, affectation TEXT,
            nb_indisponibilite INTEGER, jour_ouvrable INTEGER, type_panne TEXT, nb_si TEXT,
            FOREIGN KEY (matricule) REFERENCES Matricules(matricule)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Planning_Controle (
            id INTEGER PRIMARY KEY AUTOINCREMENT, matricule TEXT NOT NULL,
            nom_entretien TEXT NOT NULL, date_prevue DATE NOT NULL, date_realisee DATE,
            statut TEXT DEFAULT 'a_faire', date_reference DATE, source_reference TEXT, observations TEXT,
            CHECK (statut IN ('a_faire', 'realise', 'reporte')),
            FOREIGN KEY (matricule) REFERENCES Matricules(matricule)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Planning_Nettoyage (
            id INTEGER PRIMARY KEY AUTOINCREMENT, matricule TEXT NOT NULL,
            nom_entretien TEXT NOT NULL, date_prevue DATE NOT NULL, date_realisee DATE,
            statut TEXT DEFAULT 'a_faire', date_reference DATE, source_reference TEXT, observations TEXT,
            CHECK (statut IN ('a_faire', 'realise', 'annule_curatif', 'reporte')),
            FOREIGN KEY (matricule) REFERENCES Matricules(matricule)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Planning_Changement (
            id INTEGER PRIMARY KEY AUTOINCREMENT, matricule TEXT NOT NULL,
            nom_entretien TEXT NOT NULL, date_prevue DATE NOT NULL, date_realisee DATE,
            statut TEXT DEFAULT 'a_faire', date_reference DATE, source_reference TEXT, observations TEXT,
            CHECK (statut IN ('a_faire', 'realise', 'annule_curatif', 'reporte')),
            FOREIGN KEY (matricule) REFERENCES Matricules(matricule)
        ) """)
        cursor.execute(""" CREATE TABLE IF NOT EXISTS Sync_Log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, type_sync TEXT NOT NULL,
            dernier_nbsi TEXT, date_sync DATETIME DEFAULT CURRENT_TIMESTAMP,
            nb_lignes_ajoutees INTEGER, statut TEXT, message TEXT
        ) """)
        # Index
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_hist_prev_matricule ON Historique_Preventif(matricule)",
            "CREATE INDEX IF NOT EXISTS idx_hist_prev_date ON Historique_Preventif(date_realisation)",
            "CREATE INDEX IF NOT EXISTS idx_hist_cur_matricule ON Historique_Curatif(matricule)",
            "CREATE INDEX IF NOT EXISTS idx_hist_cur_date ON Historique_Curatif(date_sortie)",
            "CREATE INDEX IF NOT EXISTS idx_exclusions ON Exclusions(categorie)",
            "CREATE INDEX IF NOT EXISTS idx_planning_c_matricule ON Planning_Controle(matricule)",
            "CREATE INDEX IF NOT EXISTS idx_planning_n_matricule ON Planning_Nettoyage(matricule)",
            "CREATE INDEX IF NOT EXISTS idx_planning_ch_matricule ON Planning_Changement(matricule)"
        ]:
            cursor.execute(idx)
        self.conn.commit()
        print("Schéma de base de données initialisé")

# =============================================================================
# 2. DATA IMPORTER
# =============================================================================
class DataImporter:
    def __init__(self, db: Database):
        self.db = db

    def safe_str(self, val):
        return val.strip() if val and str(val).strip() else ''

    def safe_int(self, val, default=0):
        try:
            return int(str(val).strip()) if val and str(val).strip() else default
        except:
            return default

    def safe_float(self, val):
        try:
            return float(str(val).strip()) if val and str(val).strip() else None
        except:
            return None

    def safe_date(self, val, fmt='%d/%m/%Y'):
        if not val or not str(val).strip():
            return None
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except:
            return None

    def normalize_header(self, header: str) -> str:
        if not header:
            return ''
        header = str(header).strip().lower()
        header = header.replace(' ', '_').replace('-', '_').replace('.', '')
        header = ''.join(c for c in unicodedata.normalize('NFD', header) if unicodedata.category(c) != 'Mn')
        return header

    def import_matrice(self, csv_path: str) -> int:
        if not os.path.exists(csv_path):
            print(f"FICHIER MANQUANT: {csv_path}")
            return 0
        cursor = self.db.conn.cursor()
        count = 0
        try:
            with open(csv_path, 'r', encoding='cp1252', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=';')  # ← AJOUT
                reader.fieldnames = [self.normalize_header(h) for h in reader.fieldnames]
                for row in reader:
                    row = {self.normalize_header(k): v for k, v in row.items()}
                    matricule = self.safe_str(row.get('matricule'))
                    if not matricule:
                        continue
                    cursor.execute("""
                        INSERT OR REPLACE INTO Matricules 
                        (matricule, designation, annee, qte_vidange, code_barre, marque, pneumatique, categorie)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        matricule,
                        self.safe_str(row.get('designation')),
                        self.safe_int(row.get('annee')),
                        self.safe_int(row.get('qte_vidange'), 0),
                        self.safe_str(row.get('code_barre')),
                        self.safe_str(row.get('marque')),
                        self.safe_str(row.get('pneumatique')),
                        self.safe_str(row.get('categorie'))
                    ))
                    count += 1
        except Exception as e:
            print(f"Erreur lecture {csv_path}: {e}")
        self.db.conn.commit()
        print(f"{count} matricules importés")
        return count

    def import_vidange(self, csv_path: str, incremental: bool = False) -> int:
        if not os.path.exists(csv_path):
            print(f"FICHIER MANQUANT: {csv_path}")
            return 0
        cursor = self.db.conn.cursor()
        count = 0
        dernier_nbsi = None
        if incremental:
            cursor.execute("SELECT dernier_nbsi FROM Sync_Log WHERE type_sync='VIDANGE' ORDER BY date_sync DESC LIMIT 1")
            result = cursor.fetchone()
            dernier_nbsi = result[0] if result else None
        try:
            with open(csv_path, 'r', encoding='cp1252', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=';')  # ← AJOUT
                reader.fieldnames = [self.normalize_header(h) for h in reader.fieldnames]
                for row in reader:
                    row = {self.normalize_header(k): v for k, v in row.items()}
                    nb_si = self.safe_str(row.get('nbsi'))
                    if not nb_si:
                        continue
                    if incremental and dernier_nbsi and nb_si <= dernier_nbsi:
                        continue
                    matricule = self.safe_str(row.get('matricule'))
                    if not matricule:
                        continue
                    date_realisation = self.safe_date(row.get('date'))
                    if not date_realisation:
                        continue
                    entretiens = []
                    if self.safe_str(row.get('entretien')) == 'VIDANGE,M':
                        entretiens.append('Vidanger le carter moteur')
                    if self.safe_str(row.get('f/h')) == '*':
                        entretiens.append('Filtre à huile')
                    if self.safe_str(row.get('f/g')) == '*':
                        entretiens.append('Filtre carburant')
                    if self.safe_str(row.get('f/air')) == '*':
                        entretiens.append('Filtre à air')
                    if self.safe_str(row.get('f/hyd')) == '*':
                        entretiens.append('Filtre hydraulique')
                    if self.safe_str(row.get('entretien')) == 'GR' or self.safe_float(row.get('gr')):
                        entretiens.append('Graissage général')
                    for e in entretiens:
                        cursor.execute("""
                            INSERT INTO Historique_Preventif 
                            (matricule, nom_entretien, type_intervention, date_realisation, 
                             compteur_km_h, observations, source_fichier, nb_si)
                            VALUES (?, ?, 'CH', ?, ?, ?, 'VIDANGE.csv', ?)
                        """, (
                            matricule, e, date_realisation,
                            self.safe_float(row.get('compteur_km/h')),
                            self.safe_str(row.get('obs')), nb_si
                        ))
                        count += 1
            if count > 0:
                cursor.execute("INSERT INTO Sync_Log (type_sync, dernier_nbsi, nb_lignes_ajoutees, statut, message) VALUES ('VIDANGE', ?, ?, 'SUCCESS', 'Import réussi')", (nb_si, count))
        except Exception as e:
            print(f"Erreur import VIDANGE: {e}")
        self.db.conn.commit()
        print(f"{count} entretiens préventifs importés")
        return count

    def import_suivi_curatif(self, csv_path: str, incremental: bool = False) -> int:
        if not os.path.exists(csv_path):
            print(f"FICHIER MANQUANT: {csv_path}")
            return 0
        cursor = self.db.conn.cursor()
        count = 0
        dernier_nbsi = None
        if incremental:
            cursor.execute("SELECT dernier_nbsi FROM Sync_Log WHERE type_sync='CURATIF' ORDER BY date_sync DESC LIMIT 1")
            result = cursor.fetchone()
            dernier_nbsi = result[0] if result else None
        try:
            with open(csv_path, 'r', encoding='cp1252', errors='replace') as f:
                reader = csv.DictReader(f, delimiter=';')  # ← AJOUT
                reader.fieldnames = [self.normalize_header(h) for h in reader.fieldnames]
                for row in reader:
                    row = {self.normalize_header(k): v for k, v in row.items()}
                    nb_si = self.safe_str(row.get('nbsi'))
                    if not nb_si:
                        continue
                    if incremental and dernier_nbsi and nb_si <= dernier_nbsi:
                        continue
                    matricule = self.safe_str(row.get('matricule'))
                    if not matricule:
                        continue
                    date_entree = self.safe_date(row.get('date_entree'))
                    date_sortie = self.safe_date(row.get('date_sortie'))
                    cursor.execute("""
                        INSERT INTO Historique_Curatif 
                        (matricule, categorie, designation, date_entree, panne_declaree, 
                         situation_actuelle, pieces, date_sortie, intervenant, affectation,
                         nb_indisponibilite, jour_ouvrable, type_panne, nb_si)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        matricule,
                        self.safe_str(row.get('categorie')),
                        self.safe_str(row.get('designation')),
                        date_entree,
                        self.safe_str(row.get('panne_declaree')),
                        self.safe_str(row.get('sit_actuelle')),
                        self.safe_str(row.get('pieces')),
                        date_sortie,
                        self.safe_str(row.get('intervenant')),
                        self.safe_str(row.get('affectation')),
                        self.safe_int(row.get('nbr_indisponibilite')),
                        self.safe_int(row.get('jour_ouvrable')),
                        self.safe_str(row.get('type_de_panne')),
                        nb_si
                    ))
                    count += 1
            if count > 0:
                cursor.execute("INSERT INTO Sync_Log (type_sync, dernier_nbsi, nb_lignes_ajoutees, statut, message) VALUES ('CURATIF', ?, ?, 'SUCCESS', 'Import réussi')", (nb_si, count))
        except Exception as e:
            print(f"Erreur import CURATIF: {e}")
        self.db.conn.commit()
        print(f"{count} entretiens curatifs importés")
        return count

    def initialize_exclusions(self):
        cursor = self.db.conn.cursor()
        exclusions_data = {
            "GEG": ["frein", "chaine", "pneu", "moyeu de roue", "graissage général", "boite de vitesse", "cardan", "embrayage", "circuit hydraulique", "pompe hydraulique", "filtre hydraulique", "réservoir hydraulique", "faisceaux électriques"],
            "AIR COMPRIME": ["frein", "chaine", "pneu", "moyeu de roue", "graissage général", "boite de vitesse", "cardan", "embrayage", "circuit hydraulique", "pompe hydraulique", "faisceaux électriques"],
            "LEGER": ["graissage général", "circuit hydraulique", "pompe hydraulique", "filtre hydraulique", "réservoir hydraulique", "faisceaux électriques"],
            "TRANS/MARCHANDISE 1": ["niveau d'huile du carter", "etanchéité des circuits", "courroie", "filtre à huile", "vidanger le carter moteur", "filtre à air", "filtre carburant", "chaine", "soupape", "boite de vitesse", "cardan", "embrayage", "circuit hydraulique", "pompe hydraulique", "filtre hydraulique", "réservoir hydraulique", "alternateur", "batterie", "faisceaux électriques"],
            "TRANS ET V, SPECIAUX 1": ["niveau d'huile du carter", "etanchéité des circuits", "courroie", "filtre à huile", "vidanger le carter moteur", "filtre à air", "filtre carburant", "chaine", "soupape", "boite de vitesse", "cardan", "embrayage", "circuit hydraulique", "pompe hydraulique", "filtre hydraulique", "réservoir hydraulique", "alternateur", "batterie", "faisceaux électriques"],
            "TRANS/PERSONNEL": ["niveau d'huile du carter", "circuit hydraulique", "pompe hydraulique", "filtre hydraulique", "réservoir hydraulique", "faisceaux électriques"],
            "TRANS/BENNE.R": ["embrayage", "chaine", "boite de vitesse", "alternateur", "faisceaux électriques"]
        }
        count = 0
        for categorie, exclusions in exclusions_data.items():
            for entretien in exclusions:
                cursor.execute("INSERT OR IGNORE INTO Exclusions (categorie, entretien_exclu) VALUES (?, ?)", (categorie, entretien))
                count += 1
        self.db.conn.commit()
        print(f"{count} exclusions initialisées")

# =============================================================================
# 3. PLANNING
# =============================================================================
class PlanningGenerator:
    def __init__(self, db: Database):
        self.db = db

    def generate_planning_for_year(self, annee: int):
        cursor = self.db.conn.cursor()
        cursor.execute("SELECT * FROM Matricules")
        matricules = cursor.fetchall()
        debut_annee = datetime(annee, 1, 1).date()
        fin_annee = datetime(annee, 12, 31).date()
        for table in ['Planning_Controle', 'Planning_Nettoyage', 'Planning_Changement']:
            cursor.execute(f"DELETE FROM {table} WHERE date_prevue BETWEEN ? AND ?", (debut_annee, fin_annee))
        total_count = 0
        entretiens_defaut = [
            "Niveau d'huile du carter", "Etanchéité de tous les circuits", "Frein", "courroie",
            "Filtre à huile", "Vidanger le carter moteur", "Filtre à air", "Filtre carburant",
            "chaine", "soupape", "Graissage général", "moyeu de roue", "pneu", "boite de vitesse",
            "cardan", "embrayage", "circuit hydraulique", "pompe hydraulique", "Filtre hydraulique",
            "Réservoir hydraulique", "alternateur", "batterie", "Faisceaux électriques"
        ]
        for e in entretiens_defaut:
            cursor.execute("INSERT OR IGNORE INTO Entretiens_Types (nom) VALUES (?)", (e,))
            cursor.execute("INSERT OR IGNORE INTO Parametrage (entretien_nom, type_intervention, intervalle_jours) VALUES (?, 'C', 30)", (e,))
            cursor.execute("INSERT OR IGNORE INTO Parametrage (entretien_nom, type_intervention, intervalle_jours) VALUES (?, 'N', 90)", (e,))
            cursor.execute("INSERT OR IGNORE INTO Parametrage (entretien_nom, type_intervention, intervalle_jours) VALUES (?, 'CH', 180)", (e,))
        for mat_row in matricules:
            mat = dict(mat_row)
            matricule_id = mat['matricule']
            categorie = mat['categorie']
            cursor.execute("SELECT entretien_exclu FROM Exclusions WHERE categorie = ?", (categorie,))
            exclusions = [row[0].lower() for row in cursor.fetchall()]
            for entretien in entretiens_defaut:
                if entretien.lower() in exclusions:
                    continue
                cursor.execute("SELECT type_intervention, intervalle_jours FROM Parametrage WHERE entretien_nom = ?", (entretien,))
                for param in cursor.fetchall():
                    type_interv, intervalle = param[0], param[1]
                    derniere_date = datetime(2010, 1, 1).date()
                    current_date = derniere_date + timedelta(days=intervalle)
                    while current_date <= fin_annee:
                        if current_date >= debut_annee:
                            table = {'C': 'Planning_Controle', 'N': 'Planning_Nettoyage', 'CH': 'Planning_Changement'}[type_interv]
                            cursor.execute(f"INSERT INTO {table} (matricule, nom_entretien, date_prevue, date_reference, source_reference) VALUES (?, ?, ?, ?, ?)", 
                                           (matricule_id, entretien, current_date, derniere_date, 'default'))
                            total_count += 1
                        current_date += timedelta(days=intervalle)
        self.db.conn.commit()
        print(f"{total_count} entretiens planifiés pour l'année {annee}")
        return total_count

# =============================================================================
# 4. MAIN
# =============================================================================
def main():
    print("Système de Gestion d'Entretiens - Initialisation")
    print("=" * 60)
    db = Database("maintenance.db")
    db.connect()
    db.initialize_schema()
    importer = DataImporter(db)
    print("\nInitialisation des exclusions...")
    importer.initialize_exclusions()
    print("\nImport des données...")
    importer.import_matrice("backend/data/MATRICE.csv")
    importer.import_vidange("backend/data/VIDANGE.csv", incremental=True)
    importer.import_suivi_curatif("dackend/data/SUIVI_CURATIF.csv", incremental=True)
    print("\nGénération du planning 2025...")
    PlanningGenerator(db).generate_planning_for_year(2025)
    print("\nInitialisation terminée!")
    print("Base de données: maintenance.db")
    db.close()

if __name__ == "__main__":
    main()