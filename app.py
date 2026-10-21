# -*- coding: utf-8 -*-
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL CLOUD KOYEB
# # PARTIE 1 / 20 CORRIGÉE : INTERCOMMUNICATION INTER-CLOUD ET INTERFACE HORAIRE
# ==============================================================================
import os
import sys
import time
import datetime
import re
import json
import warnings
import requests
import pandas as pd
import numpy as np
import signal
import threading  
from flask import Flask, jsonify

# Initialisation obligatoire de l'application Flask requise par l'infrastructure Koyeb
app = Flask(__name__)

# Interdiction passive des avertissements SSL liés aux infrastructures Falcon
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# ==============================================================================
# 🎛️ COMMUTATEUR D'INFRASTRUCTURE DÉCENTRALISÉ (0% DÉPENDANCE D'UN PC LOCAL)
# ==============================================================================
# 🟢 TRUE = Mode Simulation (Aspiration du flux généré par PythonAnywhere via HTTP standard)
# 🟢 FALSE = Mode Production Réelle Directe (Aspiration directe sur le port 8443 de Place)
MODE_SIMULATION_ACTIF = True  

# Endpoints de routage réseau inter-cloud
URL_PASSERELLE_PYTHONANYWHERE = "https://hafedhdev.pythonanywhere.com/"
URL_API_REAL_LIVE = "https://online.macsa.com.tn:8443/marketApi/sbe/market-report/11,12,13,32,33,99"

# Sélection dynamique de l'URL cible d'aspiration selon le mode choisi
URL_API = URL_PASSERELLE_PYTHONANYWHERE if MODE_SIMULATION_ACTIF else URL_API_REAL_LIVE
MODE_PRODUCTION = not MODE_SIMULATION_ACTIF

# 🎯 DECOUPLAGE LOGIQUES KOYEB CLOUD : Résolution absolue des répertoires de persistance
DOSSIER_SCRIPT = os.path.dirname(os.path.abspath(__file__)) 
# 🟢 ROUTAGE SCELLÉ VERS LA ZONE D'ÉCRITURE AUTORISÉE /tmp
REPERTOIRE_JSON = "/tmp/Bourse_MACSA_Cache"
REPERTOIRE_HISTO = "/tmp/Historique"


# Armement physique et sécurisé des coques de stockage local
os.makedirs(REPERTOIRE_HISTO, exist_ok=True) 
os.makedirs(REPERTOIRE_JSON, exist_ok=True) 

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Configuration Géométrique des Flux Glissants
# # PARTIE 2 / 20 : ETALONNAGE DES ANCRAGES ET SEGMENTS DISQUES [ALIGNEMENT DOSSIERS]
# ==============================================================================
DATE_JOUR_STR = datetime.date.today().strftime("%d%m%Y") 

# Spécifications des endpoints d infrastructures Falcon et fichiers sources de simulation
URL_API = "https://online.macsa.com.tn:8443/marketApi/sbe/market-report/11,12,13,32,33,99"
FICHIER_SIMULE_SOURCE = os.path.join(DOSSIER_SCRIPT, "SbeMarket-Data.csv") 

# Matrice globale à plat lue par l ADODB d Excel toutes les 500 ms
FICHIER_TEXTE_GRILLE = os.path.join(REPERTOIRE_JSON, "DonneesSource.txt") 

# 🎯 ARCHITECTURE DE ROUTAGE ABSOLUE SCELLÉE CONFORME À LA GRILLE PHYSIQUE DE PLACE
REPERTOIRE_HISTO_REEL = os.path.join(REPERTOIRE_HISTO, "bourse_historique_reel")
REPERTOIRE_SYNTHESE = os.path.join(REPERTOIRE_HISTO, "Synthese_cloture")

# Armement matériel des coques de sous-répertoires requis (0% Erreur ntpath)
os.makedirs(REPERTOIRE_HISTO_REEL, exist_ok=True)
os.makedirs(REPERTOIRE_SYNTHESE, exist_ok=True)

# Nomenclature glissante des journaux d activité micro-structurelle HFT
PREFIXE_LOG = "bourse_historique_reel_" if MODE_PRODUCTION else "bourse_historique_simulation_" 
FICHIER_HISTO_RAMDISK = os.path.join(REPERTOIRE_JSON, f"temp_{PREFIXE_LOG}{DATE_JOUR_STR}.csv") 

# Aiguillage du fichier journal permanent de séance (.csv) vers le sous-dossier dédié
FICHIER_HISTO_SEANCE = os.path.join(REPERTOIRE_HISTO_REEL, f"{PREFIXE_LOG}{DATE_JOUR_STR}.csv") 

if MODE_PRODUCTION:
    # Ingestion unifiée vers le répertoire Synthese_cloture
    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_reel_{DATE_JOUR_STR}.csv")
    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_reel_{DATE_JOUR_STR}.json")
else:
    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_simulation_{DATE_JOUR_STR}.csv")
    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_simulation_{DATE_JOUR_STR}.json")

# Coques d interconnectivités asynchrones inter-thread (Appellations d Origine Préservées)
BUFFER_TEXTE_RESEAU_FALCON = ""
VERROU_PARTAGE_THREAD = threading.Lock()
DATE_SEANCE_EN_COURS = datetime.date.today().strftime("%d%m%Y")
EST_ARCHIVE_FINALE_ENGAGEE = False
df_master_ram_cote = None

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Spécifications des Grilles de Session
# # PARTIE 3 / 20 : SCELLAGE COMPTABLE DES EN-TÊTES MATRIX
# ==============================================================================
# En-têtes fixes de la Grille unifiée DonneesSource.txt (40 colonnes physiques)
LISTE_ENTETES_OFFICIELS = [
    "SbeMarket SheetValeur", "Etat", "Qt. A", "Achat", "Vente", "Qt.V", "Clôture", "CTO", "VTO", "Ouverture",
    "Dernier", "Var.", "CMP", "Qté", "Volume", "P. Haut", "P. Bas", "Seuil.S. Haut", "Seuil.S. Bas", "Seuil.D. Haut",
    "Seuil.D. Bas", "Heure"
]

# Nomenclature unifiée à 42 colonnes pour l extraction chronologique du grand livre CSV
ENTETES_HISTORIQUE_CSV = [
    "Ticker", "Heure_Marche", "Heure_Python", "Nature_Operation", "Delta_Qte", "Prix_Execution", "Delta_Vol",
    "T_Phase_Reglementaire", "T_Alerte_Volatilis", "T_Alerte_Seuil", "T_Graph_Flash", "T0_CMP", "T0_Etat", "T0_Qt_A",
    "T0_Achat", "T0_Vente", "T0_Qt_V", "T0_Cloture", "T0_CTO", "T0_VTO", "T0_Ouverture", "T0_Dernier", "T0_Var",
    "T0_CMP_Brut", "T0_Qte_Seance", "T0_Volume_Seance", "T0_Heure_Marche", "T1_Etat", "T1_Qt_A", "T1_Achat", "T1_Vente",
    "T1_Qt_V", "T1_Cloture", "T1_CTO", "T1_VTO", "T1_Ouverture", "T1_Dernier", "T1_Var", "T1_CMP", "T1_Qte_Seance",
    "T1_Volume_Seance", "Date_Seance"
]

# Allocation des registres de transition glissante en mémoire vive
cumul_qte_achat, cumul_vol_achat = {}, {}
cumul_qte_vente, cumul_vol_vente = {}, {}
cumul_qte_indecis, cumul_vol_indecis = {}, {}
historique_volumes_qte, historique_volumes_montant = {}, {}
historique_dernier_cours, historique_phase_precedente = {}, {}
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Gestion des Transitions de Sessions Courantes
# # PARTIE 4 / 20 : RE-INITIALISATION DU PATRIMOINE VOLATILE A CHAUD (0% FREEZE)
# ==============================================================================
def verifier_et_sauvegarder_retroactivement_le_passe():
    global REPERTOIRE_JSON, REPERTOIRE_HISTO, PREFIXE_LOG, REPERTOIRE_HISTO_REEL
    try:
        if not os.path.exists(REPERTOIRE_JSON):
            return
        # Balayage préventif des structures temporaires non consolidées du jour précédent
        for nom_f in os.listdir(REPERTOIRE_JSON):
            if nom_f.startswith("temp_bourse_historique_") and nom_f.endswith(".csv"):
                match_date = re.search(r'\d{8}', nom_f)
                if match_date:
                    date_extraction_str = match_date.group()
                    chemin_source_temp = os.path.join(REPERTOIRE_JSON, nom_f)
                    nom_final_archive = f"{PREFIXE_LOG}{date_extraction_str}.csv"
                    
                    # 🎯 ALIGNEMENT : Redirection forcée de la veille vers bourse_historique_reel
                    chemin_destination_archive = os.path.join(REPERTOIRE_HISTO_REEL, nom_final_archive)
                    
                    # Verrou d autorité : Sauvegarde forcée si l archive finale physique manque
                    if os.path.exists(chemin_source_temp) and not os.path.exists(chemin_destination_archive):
                        if os.path.getsize(chemin_source_temp) > 150:
                            import shutil
                            shutil.copy2(chemin_source_temp, chemin_destination_archive)
                            print(f"\n[🛡️ GARDE-FOU] CONSOLIDATION PAR REPLI : {nom_f} -> {nom_final_archive}")
    except Exception as e_radar:
        print(f"⚠️ Alerte radar de vérification du passé : {e_radar}")


# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : PURGEUR DE MÉMOIRE VOLATILE ET RÉINITIALISATION
# # Fichier : bourse_macsa.py -> def executer_purge_et_reinitialisation_seance_a_chaud()
# # SCellage : Importation locale d'autorité absolue du module système de Place (0% Bug ntpath)
# ==============================================================================
def executer_purge_et_reinitialisation_seance_a_chaud(date_instant_pc):
    # 🎯 FORCE L ALIGNEMENT DU NAMESPACE EN MEMOIRE RAM
    # Ré-import local explicite pour écraser toute pollution de variables amont
    import os as system_os
    import glob as system_glob
    
    global volumes_seance, capitaux_seance, cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, historique_phase_precedente, registre_transactions_RAM, registre_RAM_t_moins_1, df_master_ram_cote, EST_ARCHIVE_FINALE_ENGAGEE, DATE_SEANCE_EN_COURS, REPERTOIRE_JSON, FICHIER_HISTO_RAMDISK, FICHIER_ETAT_RAM
    
    print(f"\n[⏳] 🦅 QUARTZ : Alignement du moteur microstructurel sur la séance du {date_instant_pc}...")
    
    # Réinitialisation des grands livres de structures vectorielles
    volumes_seance.clear()
    capitaux_seance.clear()
    cumul_qte_achat.clear()
    cumul_vol_achat.clear()
    cumul_qte_vente.clear()
    cumul_vol_vente.clear()
    cumul_qte_indecis.clear()
    cumul_vol_indecis.clear()
    historique_volumes_qte.clear()
    historique_volumes_montant.clear()
    historique_dernier_cours.clear()
    historique_phase_precedente.clear()
    registre_transactions_RAM.clear()
    registre_RAM_t_moins_1.clear()
    
    df_master_ram_cote = None
    EST_ARCHIVE_FINALE_ENGAGEE = False
    DATE_SEANCE_EN_COURS = date_instant_pc
    
    # 🎯 BALAYAGE DU REPERTOIRE DES CACHES PAR LE MODULE SYSTEME RE-ALIGNÉ
    if system_os.path.exists(REPERTOIRE_JSON):
        try:
            # Remplacement de os.listdir par le pointeur d autorité locale étanche
            for fichier_test in system_os.listdir(REPERTOIRE_JSON):
                if fichier_test.endswith(".json") or fichier_test.endswith(".txt") or fichier_test.endswith(".csv"):
                    chemin_complet_purga = system_os.path.join(REPERTOIRE_JSON, fichier_test)
                    # Sécurité : On ne détruit pas le fichier de la séance active actuelle
                    if FICHIER_HISTO_RAMDISK not in chemin_complet_purga and FICHIER_ETAT_RAM not in chemin_complet_purga:
                        try:
                            system_os.remove(chemin_complet_purga)
                        except:
                            pass
            print("[*] [🗑️ PURGE] Répertoire cache volatile sanitize et vidé avec succès.")
        except Exception as e_purge_dir:
            print(f"⚠️ Alerte nettoyage sous-dossier : {e_purge_dir}")
            
    print("[*] 🦅 Moteur quantique MAC SA réinitialisé à blanc pour la nouvelle journée.\n")

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Registre des Compartiments de Négociation
# # PARTIE 5 / 20 : REGISTRE D AUTORITE DES GROUPES ET LIQUIDITES DE PLACE
# ==============================================================================
DICT_VALEURS_BVMT = {
    "MONOPRIX": {"groupe": 11}, "SFBT": {"groupe": 11}, "TUNISAIR": {"groupe": 11},
    "TUNISIE LEASING F": {"groupe": 11}, "BT": {"groupe": 11}, "STB": {"groupe": 11},
    "BNA": {"groupe": 11}, "AMEN BANK": {"groupe": 11}, "ATB": {"groupe": 11},
    "UIB": {"groupe": 11}, "SIMPAR": {"groupe": 12}, "CIL": {"groupe": 11},
    "ATL": {"groupe": 11}, "POULINA GP HOLDING": {"groupe": 11}, "SOTETEL": {"groupe": 11},
    "SOTUVER": {"groupe": 11}, "SOTUMAG": {"groupe": 11}, "SIAME": {"groupe": 11},
    "ATTIJARI LEASING": {"groupe": 11}, "SOTRAPIL": {"groupe": 11}, "SOMOCER": {"groupe": 11},
    "ASSAD": {"groupe": 11}, "ESSOUKNA": {"groupe": 11}, "TPR": {"groupe": 11},
    "ARTES": {"groupe": 11}, "TUNIS RE": {"groupe": 11}, "CARTHAGE CEMENT": {"groupe": 11},
    "ENNAKL AUTOMOBILES": {"groupe": 11}, "LAND OR": {"groupe": 11}, "ONE TECH HOLDING": {"groupe": 11},
    "NEW BODY LINE": {"groupe": 11}, "CITY CARS": {"groupe": 11}, "EURO-CYCLES": {"groupe": 11},
    "CELLCOM": {"groupe": 11}, "SAH": {"groupe": 11}, "MPBS": {"groupe": 11},
    "SOTIPAPIER": {"groupe": 11}, "TAWASOL GP HOLDING": {"groupe": 11}, "DELICE HOLDING": {"groupe": 11},
    "UADH": {"groupe": 12}, "UNIMED": {"groupe": 11}, "ATELIER MEUBLE INT": {"groupe": 11},
    "BTE (ADP)": {"groupe": 12}, "SPDIT - SICAF": {"groupe": 11}, "AIR LIQUIDE TSIE": {"groupe": 12},
    "PLAC. TSIE-SICAF": {"groupe": 12}, "ASTREE": {"groupe": 12}, "ALKIMIA": {"groupe": 12},
    "TUNINVEST-SICAR": {"groupe": 12}, "STAR": {"groupe": 11}, "MAGASIN GENERAL": {"groupe": 11},
    "BH ASSURANCE": {"groupe": 12}, "SIPHAT": {"groupe": 12}, "BH LEASING": {"groupe": 12},
    "SITS": {"groupe": 12}, "WIFACK INT BANK": {"groupe": 11}, "CIMENTS DE BIZERTE": {"groupe": 12},
    "BEST LEASE": {"groupe": 12}, "SOTEMAIL": {"groupe": 12}, "OFFICEPLAST": {"groupe": 11},
    "SANIMED": {"groupe": 12}, "SITEX": {"groupe": 12}, "BNA ASSURANCES": {"groupe": 11},
    "STE TUN. DU SUCRE": {"groupe": 12}, "STIP": {"groupe": 12}, "HANNIBAL LEASE": {"groupe": 12},
    "ATTIJARI BANK": {"groupe": 11}, "TELNET HOLDING": {"groupe": 11}, "BH BANK": {"groupe": 11},
    "ICF": {"groupe": 11}, "AETECH": {"groupe": 12}, "UBCI": {"groupe": 11},
    "ASSUR MAGHREBIA": {"groupe": 11}, "BIAT": {"groupe": 11}, "SMART TUNISIE": {"groupe": 11},
    "STA": {"groupe": 11}, "ASSU MAGHREBIA VIE": {"groupe": 11}
}

# Extraction unifiée matricielle
LISTE_VALEURS_UNIFIEES = list(DICT_VALEURS_BVMT.keys())
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Allocation Mémoires en Structure RAMDISK
# # PARTIE 6 / 20 : ALLOCATION ET INITIALISATION GENERALE DES COMPTEURS UNITAIRES
# ==============================================================================
# Dictionnaires unitaires d activité transactionnelle de session
volumes_seance = {ticker: 0 for ticker in LISTE_VALEURS_UNIFIEES}
capitaux_seance = {ticker: 0.0 for ticker in LISTE_VALEURS_UNIFIEES}

# Registres de mémorisation glissante pour séries de ticks unitaires
historique_courbe_ticks = {ticker: [] for ticker in LISTE_VALEURS_UNIFIEES}
derniere_nature_valide_titre = {ticker: "NEUTRE" for ticker in LISTE_VALEURS_UNIFIEES}

# Registres de couplage asynchrone pour la mémoire glissante t-1
registre_RAM_t_moins_1 = {}
registre_transactions_RAM = []
derniere_action_traitee = {}

# Calage initial à froid de l horloge centrale maîtresse
HEURE_CENTRALE_BVMT = "08:30:00"

for action in LISTE_VALEURS_UNIFIEES:
    cumul_qte_achat[action] = 0
    cumul_vol_achat[action] = 0.0
    cumul_qte_vente[action] = 0
    cumul_vol_vente[action] = 0.0
    cumul_qte_indecis[action] = 0
    cumul_vol_indecis[action] = 0.0
    
    historique_volumes_qte[action] = 0
    historique_volumes_montant[action] = 0.0
    historique_dernier_cours[action] = 0.0
    
    # Structure de résilience : Initialisation du dictionnaire t-1 d amorti anti-KeyError
    registre_RAM_t_moins_1[action] = {col: "0" for col in LISTE_ENTETES_OFFICIELS}
    registre_RAM_t_moins_1[action]["SbeMarket SheetValeur"] = action
    registre_RAM_t_moins_1[action]["Etat_Num"] = 5
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Gestionnaire du Calendrier Glissant de Place
# # PARTIE 7 / 20 : RECONNAISSANCE DU REGIME HORAIRE (HIVER / ETE / RAMADAN)
# ==============================================================================
# Table de projection de l axe lunaire validée par la Bourse de Tunis
TABLE_RAMADAN_BVMT = {
    2024: (3, 11), 2025: (3, 1), 2026: (2, 18), 2027: (2, 7), 2028: (1, 27),
    2029: (1, 16), 2030: (1, 5), 2031: (12, 25), 2032: (12, 14), 2033: (12, 3)
}

def detecter_regime_horaire(date_obj):
    """
    Identifie le régime horaire de la Place de Tunis pour adapter les états de séance.
    0% Risque d'erreur de chevauchement sur les grilles de pré-clôture juillet/août.
    """
    try:
        annee = date_obj.year
        
        # 🌙 Interception de la fenêtre mobile du Ramadan
        if annee in TABLE_RAMADAN_BVMT:
            mois_r, jour_r = TABLE_RAMADAN_BVMT[annee]
            date_pivot_ramadan = datetime.date(annee, mois_r, jour_r)
            debut_ramadan = date_pivot_ramadan - datetime.timedelta(days=2)
            fin_ramadan = debut_ramadan + datetime.timedelta(days=35)
            
            date_pure_test = date_obj if isinstance(date_obj, datetime.date) else date_obj.date()
            if debut_ramadan <= date_pure_test <= fin_ramadan: 
                return "ETE_RAMADAN"
                
        # ☀️ Interception réglementaire de la Séance Unique d'Été (Juillet, Août)
        if date_obj.month in[7, 8]: 
            return "ETE_RAMADAN"
            
    except Exception as e_cal: 
        print(f"⚠️ Alerte décodeur calendrier boursier : {e_cal}")
        
    return "HIVER"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Outils Microstructurels de Typage de Données
# # PARTIE 8 / 20 : CONVERTISSEUR COMPTABLE POINT FLOTTANT ET PROTECTION SÉMANTIQUE
# ==============================================================================
def convert_num(val_str):
    """
    Nettoie et convertit les trames brutes textuelles de Place en réels double précision.
    Annihile les crashs liés aux chaînes de caractères descriptives et aux espaces insécables.
    """
    try:
        if pd.isna(val_str) or val_str is None: 
            return 0.0
            
        v = str(val_str).strip().upper()
        if "ATP" in v: return -1.0
        if "OUV" in v: return -2.0
        
        # Coupe-circuit sémantique pour interdire le traitement arithmétique des phases
        if any(keyword in v for keyword in ["CONTINU", "RESERV", "GEL", "FIXING", "SUSPENDU", "ATTENTE", "MARCHE", "FERME", "VEILLE", "CLOTURE"]):
            return 0.0
            
        # Élimination des espaces insécables de Place (\xa0, \u202f) et conversion du séparateur decimal
        v_nettoye = re.sub(r'[\s\xa0\u202f]', '', v).replace(',', '.')
        
        match_float = re.search(r"[-+]?\d*\.\d+|\d+", v_nettoye)
        if match_float:
            return float(match_float.group())
        return 0.0
    except:
        return 0.0

def _forcer_float_reprise(valeur_cellule):
    """
    Force l extraction scalaire d une cellule lors de la synchronisation à froid.
    """
    if pd.isna(valeur_cellule) or str(valeur_cellule).strip() == "": 
        return 0.0
    try: 
        str_nettoye = str(valeur_cellule).replace(" ", "").replace(",", ".").strip()
        return float(str_nettoye)
    except: 
        return 0.0
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Machine d'État de Réservation Volatilité
# # PARTIE 9 / 20 : EN-TÊTE DU COMPARTIMENT DE GEL ET PALIERS REGLEMENTAIRES
# ==============================================================================
# Registre dynamique scellé à 7 clés par instrument pour piloter les fixing de reprise
registre_reservation_groupe11 = {
    t: {
        "en_gel": False, 
        "etape": 0, 
        "debut_gel_dt": None, 
        "fin_gel_dt": None,
        "pivot_dynamique": 0.0, 
        "seuil_actuel_pourcent": 3.0, 
        "ordres_accumules": []
    } for t in LISTE_VALEURS_UNIFIEES
}
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Gestionnaire Chronométrique Atos NSC
# # PARTIE 10 / 20 : VÉRIFICATEUR DES PHASES PAR COMPARTIMENT ET RÉGIME
# ==============================================================================
def verifier_phase_par_groupe(heure_api_str, groupe_id, regime, action=None):
    """
    Détermine la phase théorique du marché selon le groupe de cotation et le calendrier.
    🎯 SCELLAGE DE L'ÉTAT 0 : À partir de 15h00, coupe-circuit nocturne strict engagé.
    """
    h = str(heure_api_str).strip().replace(":", "")
    if len(h) == 6: 
        h_f = f"{h[:2]}:{h[2:4]}:{h[4:6]}"
    else: 
        h_f = datetime.datetime.now().strftime("%H:%M:%S")

    # 🛑 Clôture absolue de la session comptable à 15h00
    if h_f >= "15:00:00":
        return 0, "CLOTURE Marche ferme (Extinction Falcon)", "00:00:00"

    # Phase de nettoyage de nuit
    if "08:00:00" <= h_f < "08:30:00": 
        return 1, "Cloture de veille / Nettoyage carnet (Purge)", "08:30:00"
    
    # Awakening / Saisie des ordres sans exécution
    if h_f == "08:30:00" or ("08:30:00" <= h_f < "08:30:05"):
        return 2, "Preouverture active / Saisie des ordres", "08:30:05"
    
    # Pre-Fixing d'ouverture nominal
    if "08:30:05" <= h_f < "09:00:00": 
        return 3, "Pre_Fixing d'ouverture (Accumulation)", "09:00:00"

    # ==========================================================================
    # 📯 CASE A : GRILLE DES HORAIRES DU RÉGIME D'HIVER STANDARD
    # ==========================================================================
    if regime == "HIVER":
        # Valeurs traitées en Continu (Groupes 11, 21, 41, 61, 71, 81)
        if groupe_id in (11, 21, 41, 61, 71, 81):
            if h_f == "09:00:00": return 4, "Fixing d'ouverture", "09:00:01"
            elif "09:00:01" <= h_f < "14:00:00": return 5, "Seance ordinaire (Negociation en continue)", "14:00:00"
            elif "14:00:00" <= h_f < "14:05:00": return 3, "Pre_Fixing de cloture", "14:05:00"
            elif h_f == "14:05:00": return 4, "Second Fixing de cloture", "14:05:01"
            elif "14:05:01" <= h_f < "14:15:00": return 5, "Negociation au dernier cours de cloture", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"

        # Valeurs traitées en Double Fixing (Groupes 12, 22, 42)
        elif groupe_id in (12, 22, 42):
            if h_f == "09:00:00": return 4, "Ouverture / Fixing (Fix 1)", "09:00:01"
            elif "09:00:01" <= h_f < "09:15:00": return 5, "Negociation au Dernier Cours (Fix 1)", "09:15:00"
            elif "09:15:00" <= h_f < "14:00:00": return 3, "Pre_Fixing (Attente 2nd Fixing)", "14:00:00"
            elif h_f == "14:00:00": return 4, "Cloture / Second Fixing (Fix 2)", "14:00:01"
            elif "14:00:01" <= h_f < "14:15:00": return 5, "Negociation au Dernier Cours (Fix 2)", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
                
        # Valeurs traitées en Fixing Unique (Groupes 13, 52, 95, 99)
        elif groupe_id in (13, 52, 95, 99):
            if "08:30:01" <= h_f < "12:00:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "12:00:00"
            elif h_f == "12:00:00": return 4, "Fixing Unique (12h00)", "12:00:01"
            elif "12:00:01" <= h_f < "12:15:00": return 5, "Negociation au cours de fixing", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
                
        # Compartiment Droits d'Attributions (Groupe 32)
        elif groupe_id == 32:
            if "08:30:01" <= h_f < "13:45:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "13:45:00"
            elif h_f == "13:45:00": return 4, "Fixing Unique (13h45)", "13:45:01"
            elif "13:45:01" <= h_f < "14:15:00": return 5, "Negociation au cours de fixing", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
                
        # Compartiment Hors-Cote Libre (Groupe 33)
        elif groupe_id == 33:
            if "08:30:01" <= h_f < "12:15:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "12:15:00"
            elif h_f == "12:15:00": return 4, "Fixing Unique (12h15)", "12:15:01"
            elif "12:15:01" <= h_f < "12:45:00": return 5, "Negociation au cours de fixing (Hors-Cote)", "12:45:00"
            elif "12:45:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Temporisation du Régime Horaire Allégé
# # PARTIE 11 / 20 : HORAIRES DU LOGICIEL POUR SÉANCE UNIQUE / RAMADAN
# ==============================================================================
    else:
        # Grille Horaire d Été / Ramadan (Groupes Liquides Continu)
        if groupe_id in (11, 21, 41, 61, 71, 81):
            if h_f == "09:00:00": return 4, "Fixing d'ouverture", "09:00:01"
            elif "09:00:01" <= h_f < "12:00:00": return 5, "Seance ordinaire (Negociation en continue)", "12:00:00"
            elif "12:00:00" <= h_f < "12:05:00": return 3, "Pre_Fixing de cloture", "12:05:00"
            elif h_f == "12:05:00": return 4, "Second Fixing de cloture", "12:05:01"
            elif "12:05:01" <= h_f < "12:15:00": return 5, "Negociation au dernier cours de cloture", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Groupes Double Fixing en période d Été
        elif groupe_id in (12, 22, 42):
            if h_f == "09:00:00": return 4, "Ouverture / Fixing (Fix 1)", "09:00:01"
            elif "09:00:01" <= h_f < "09:15:00": return 5, "Negociation au Dernier Cours (Fix 1)", "09:15:00"
            elif "09:15:00" <= h_f < "12:00:00": return 3, "Pre_Fixing (Attente 2nd Fixing)", "12:00:00"
            elif h_f == "12:00:00": return 4, "Cloture / Second Fixing (Fix 2)", "12:00:01"
            elif "12:00:01" <= h_f < "12:15:00": return 5, "Negociation au Dernier Cours (Fix 2)", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Compartiments Fixing Unique en Été
        elif groupe_id in (13, 52, 95, 99):
            if "08:30:01" <= h_f < "10:00:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "10:00:00"
            elif h_f == "10:00:00": return 4, "Fixing Unique (10h00)", "10:00:01"
            elif "10:00:01" <= h_f < "10:15:00": return 5, "Negociation au cours de fixing", "10:15:00"
            elif "10:15:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
            
        # Droits d Attribution en Été
        elif groupe_id == 32:
            if "08:30:01" <= h_f < "11:45:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "11:45:00"
            elif h_f == "11:45:00": return 4, "Fixing Unique (11h45)", "11:45:01"
            elif "11:45:01" <= h_f < "12:15:00": return 5, "Negociation au cours de fixing", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Hors-Cote Libre en Été
        elif groupe_id == 33:
            if "08:30:01" <= h_f < "10:15:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "10:15:00"
            elif h_f == "10:15:00": return 4, "Fixing Unique (10h15)", "10:15:01"
            elif "10:15:01" <= h_f < "10:45:00": return 5, "Negociation au cours de fixing (Hors-Cote)", "10:45:00"
            elif "10:45:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"

    # Injection du verrou de l état de Réservation Volatilité officiel de Place (État 9)
    if groupe_id in (11, 51) and "action" in locals() and registre_reservation_groupe11[action]["en_gel"]: 
        return 9, "Reservation Volatilit", "00:00:00"

    return 1, "Marche en attente", "00:00:00"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Surveillance HFT des Écarts Nets de Place
# # PARTIE 12 / 20 : CALCULATEUR DE CRASH, DISPOSITIF STATIQUE ET DYNAMIQUE VELOCITÉ
# ==============================================================================
def calculer_alerte_seuil_bvmt(dernier_cours, cours_reference_veille, groupe_id, action_name, heure_machine_dt, meilleur_achat=0.0, meilleure_vente=0.0):
    global registre_reservation_groupe11, historique_volumes_qte
    
    if cours_reference_veille <= 0 or dernier_cours <= 0: 
        return "Normal", dernier_cours
        
    info_gel = registre_reservation_groupe11[action_name]
    variation_veille = ((dernier_cours - cours_reference_veille) / cours_reference_veille) * 100.0
    abs_var_veille = abs(variation_veille)
    
    qte_courante_seance = globals().get("qte_totale_seance_current", 0)
    a_eu_une_execution_reelle = False
    if action_name in historique_volumes_qte and qte_courante_seance > historique_volumes_qte[action_name]:
        a_eu_une_execution_reelle = True
        
    if groupe_id in (11, 12, 51, 52, 99):
        # 🔒 PLAFOND STATIQUE SCELLÉ IMMUABLE : Coupe-circuit ultime du carnet NSC
        if abs_var_veille >= 6.09 and a_eu_une_execution_reelle:
            return f"CRITIQUE : Plafond Informatique Absolu Atteint ({variation_veille:+.2f}%) - Verrou NSC", dernier_cours
            
        if groupe_id in (11, 51):
            if info_gel["pivot_dynamique"] == 0.0:
                info_gel["pivot_dynamique"] = dernier_cours

            if not info_gel["en_gel"]:
                # 🎯 INTERCEPTION PRÉDICTIVE SÉMANTIQUE :
                # Analyse les meilleures limites du carnet pour lever l alerte imminente avant l exécution
                for prix_test in [dernier_cours, meilleur_achat, meilleure_vente]:
                    if prix_test is None or prix_test <= 0: continue
                    var_dynamique_theo = ((prix_test - info_gel["pivot_dynamique"]) / info_gel["pivot_dynamique"]) * 100.0
                    
                    if abs(var_dynamique_theo) >= 3.00:
                        return f"ALERTE : Réservation Imminente Détectée ({var_dynamique_theo:+.2f}%)", info_gel["pivot_dynamique"]
                
                info_gel["pivot_dynamique"] = dernier_cours
                return "Normal", dernier_cours
            else:
                # 🗄️ MACHINE D ÉTAT DES PALIERS DE FIXING DE REPRISE SBE FALCON
                if a_eu_une_execution_reelle or len(info_gel["ordres_accumules"]) == 0:
                    if len(info_gel["ordres_accumules"]) == 0 or dernier_cours != info_gel["ordres_accumules"][-1]:
                        info_gel["ordres_accumules"].append(dernier_cours)
                
                if heure_machine_dt >= info_gel["fin_gel_dt"]:
                    if len(info_gel["ordres_accumules"]) > 0:
                        prix_equilibre_theorique = round(sum(info_gel["ordres_accumules"]) / len(info_gel["ordres_accumules"]), 3)
                    else:
                        prix_equilibre_theorique = dernier_cours
                        
                    var_equilibre = ((prix_equilibre_theorique - info_gel["pivot_dynamique"]) / info_gel["pivot_dynamique"]) * 100.0
                    
                    if abs(var_equilibre) < info_gel["seuil_actuel_pourcent"]:
                        info_gel["en_gel"] = False
                        info_gel["ordres_accumules"] = [] 
                        ancien_pivot = info_gel["pivot_dynamique"]
                        info_gel["pivot_dynamique"] = prix_equilibre_theorique
                        return f"REPRISE CONTINU : Fixing réussi à {prix_equilibre_theorique:.3f} TND", ancien_pivot
                    else:
                        # Escalades successives par paliers réglementaires
                        if info_gel["etape"] == 1:
                            info_gel["etape"] = 2
                            info_gel["seuil_actuel_pourcent"] = 6.09
                            info_gel["fin_gel_dt"] = heure_machine_dt + datetime.timedelta(minutes=10)
                            return "ESCALADE ÉTAPE 2 : Seuil élargi à 4.50% [10 Min]", info_gel["pivot_dynamique"]
                        elif info_gel["etape"] == 2:
                            info_gel["etape"] = 3
                            info_gel["seuil_actuel_pourcent"] = 6.09
                            info_gel["fin_gel_dt"] = heure_machine_dt + datetime.timedelta(minutes=5)
                            return "ESCALADE ÉTAPE 3 : Entrée Dernier Palier 6.09% [5 Min]", info_gel["pivot_dynamique"]
                        else:
                            info_gel["en_gel"] = False
                            info_gel["ordres_accumules"] = [] 
                            return "FORÇAGE ÉTAPE SUCCESSIVE", info_gel["pivot_dynamique"]
                else:
                    return f"GEL ACTIF : Fin à {info_gel['fin_gel_dt'].strftime('%H:%M:%S')}", info_gel["pivot_dynamique"]
        else:
            var_fixing_theo = ((dernier_cours - cours_reference_veille) / cours_reference_veille) * 100.0
            if abs(var_fixing_theo) >= 3.00 and a_eu_une_execution_reelle:
                return f"ALERTE : Tri de Réservation Fixing (Veille {var_fixing_theo:+.2f}%)", cours_reference_veille
            return "Normal", cours_reference_veille
            
    elif groupe_id == 32:
        if abs_var_veille >= 250.00 and a_eu_une_execution_reelle: return "CRITIQUE : Plafond Droits (±250%)", cours_reference_veille
        elif abs_var_veille >= 15.00 and a_eu_une_execution_reelle: return "ALERTE : Seuil Droits (±15%)", cours_reference_veille
    elif groupe_id in (21, 41) and abs_var_veille >= 10.00 and a_eu_une_execution_reelle:
        return "CRITIQUE : Seuil Obligataire Unique (10%)", cours_reference_veille        
    return "Normal", cours_reference_veille
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Analyse de l Agressivité des Flux Réseau SBE
# # PARTIE 13 / 20 : ALGORITHME DE SURVEILLANCE ET DÉPISTAGE DES ORDRES EN DIRECT
# ==============================================================================
def exécuter_moteur_lee_ready(action, delta_qte, delta_vol, dernier_c, id_phase_t0, phase_txt_t0, p_achat_t0, p_vente_t0, qte_totale_seance, vol_argent_seance, alerte_seuil_active, imbalance_carnet_t0, spread_pourcent_t0, variation_seance, cours_reference_seance, row, a_trouve_un_trade, heure_ticker, courbe_txt):
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, historique_courbe_ticks, registre_transactions_RAM, derniere_action_traitee, derniere_nature_valide_titre, registre_RAM_t_moins_1
    
    cmp_officiel_marche = convert_num(row.get("CMP", dernier_c))
    if cmp_officiel_marche <= 0: 
        cmp_officiel_marche = dernier_c

    # Calcul du prix d exécution pondéré réel de l opération (Delta Vol / Delta Qte)
    if delta_qte > 0 and delta_vol > 0:
        prix_execution_reel = round(delta_vol / delta_qte, 3)
    else:
        prix_execution_reel = dernier_c

    nature_ms = "NEUTRE"
    t1 = registre_RAM_t_moins_1.get(action, {})
    ancien_sens = derniere_nature_valide_titre.get(action, "NEUTRE")
    
    if t1 and "Dernier" in t1:
        p_achat_t1_str = str(t1.get("Achat", "0")).strip().upper()
        p_vente_t1_str = str(t1.get("Vente", "0")).strip().upper()
        p_achat_t1 = convert_num(p_achat_t1_str)
        p_vente_t1 = convert_num(p_vente_t1_str)
        dernier_t1 = convert_num(t1.get("Dernier", 0.0))
        id_phase_t1 = int(t1.get("Etat_Num", 5))
        
        # Noyau Continu d Interception Lee-Ready (Phases de négociation active)
        if id_phase_t0 in (4, 5, 7) and id_phase_t1 in (4, 5, 7):
            # Détection des flux acheteurs agressifs multiniveaux (Franchissement de la Vente t-1)
            if p_vente_t1 > 0 and prix_execution_reel > p_vente_t1:
                nature_ms = "ACHAT"
                courbe_txt = "▲ ACHAT ATP multiniveau détecté"
            # Détection des flux vendeurs agressifs multiniveaux (Enfoncement de l Achat t-1)
            elif p_achat_t1 > 0 and prix_execution_reel < p_achat_t1:
                nature_ms = "VENTE"
                courbe_txt = "▼ VENTE ATP multiniveau détecté"
            else:
                if "ATP" in p_vente_t1_str or "OUV" in p_vente_t1_str or p_vente_t1 <= 0:
                    nature_ms = "ACHAT" if prix_execution_reel >= dernier_t1 else "VENTE"
                elif "ATP" in p_achat_t1_str or "OUV" in p_achat_t1_str or p_achat_t1 <= 0:
                    nature_ms = "VENTE" if prix_execution_reel <= dernier_t1 else "ACHAT"
                else:
                    if prix_execution_reel >= p_vente_t1: nature_ms = "ACHAT"
                    elif prix_execution_reel <= p_achat_t1: nature_ms = "VENTE"
                    else:
                        # Règle d inertie tick intérieur
                        if prix_execution_reel == dernier_t1 and ancien_sens in ["ACHAT", "VENTE"]:
                            nature_ms = ancien_sens
                        else:
                            nature_ms = "ACHAT" if prix_execution_reel > dernier_t1 else ("VENTE" if prix_execution_reel < dernier_t1 else "NEUTRE")
        # Ingestion Tick de Clôture / Fixing
        else:
            if prix_execution_reel == dernier_t1:
                if ancien_sens in ["ACHAT", "VENTE"]: nature_ms = ancien_sens  
            else:
                nature_ms = "ACHAT" if prix_execution_reel > dernier_t1 else "VENTE"
            
            if nature_ms == "NEUTRE" and id_phase_t0 in (2, 3):
                if cours_reference_seance > 0 and prix_execution_reel != cours_reference_seance:
                    nature_ms = "ACHAT" if prix_execution_reel > cours_reference_seance else "VENTE"
    else:
        if variation_seance > 0: nature_ms = "ACHAT"
        elif variation_seance < 0: nature_ms = "VENTE"
        else: nature_ms = "NEUTRE"

    if nature_ms == "NEUTRE" and ancien_sens in ["ACHAT", "VENTE"]:
        nature_ms = ancien_sens

    if nature_ms in ["ACHAT", "VENTE"]:
        derniere_nature_valide_titre[action] = nature_ms

    dernier_c = prix_execution_reel
    
    # Gravure de la jauge d orientation de Place (Tableau de 6 tics)
    courbe_loc = historique_courbe_ticks.get(action, [])
    char_tendance = "S" if nature_ms == "NEUTRE" else ("H" if nature_ms == "ACHAT" else "B")
    courbe_loc.append(char_tendance)
    if len(courbe_loc) > 6: courbe_loc.pop(0)
    jauge_txt = "".join(courbe_loc).ljust(6, "_")
    
    if "multiniveau" not in courbe_txt:
        courbe_txt = f"{char_tendance} {nature_ms.ljust(5)} [{jauge_txt}]"

    # Alimentation sCELLée des 42 structures de données de la ligne transactionnelle
    log_operation_geant = {
        "Ticker": action, "Heure_Marche": heure_ticker, "Heure_Python": time.strftime('%H:%M:%S'),          
        "Nature_Operation": nature_ms, "Delta_Qte": int(delta_qte), "Prix_Execution": round(float(dernier_c), 3), "Delta_Vol": round(float(delta_vol), 3),             
        "T_Phase_Reglementaire": str(phase_txt_t0), "T_Alerte_Volatilis": "TRADE", "T_Alerte_Seuil": str(alerte_seuil_active), "T_Graph_Flash": str(jauge_txt), 
        "T0_CMP": round(float(cmp_officiel_marche), 3), "T0_Etat": str(phase_txt_t0), "T0_Qt_A": str(row.get("Qt. A", "0")), "T0_Achat": str(p_achat_t0), 
        "T0_Vente": str(p_vente_t0), "T0_Qt_V": str(row.get("Qt.V", "0")), "T0_Cloture": str(row.get("Clôture", "0")), "T0_CTO": str(row.get("CTO", "0")), 
        "T0_VTO": str(row.get("VTO", "0")), "T0_Ouverture": str(row.get("Ouverture", "0")), "T0_Dernier": str(row.get("Dernier", "0")),
        "T0_Var": str(row.get("Var.", "0")), "T0_CMP_Brut": f"{cmp_officiel_marche:.3f}", "T0_Qte_Seance": int(qte_totale_seance), "T0_Volume_Seance": round(float(vol_argent_seance), 3), "T0_Heure_Marche": heure_ticker,
        # 🟢 RESTAURATION DES LIAISONS GLISSANTES T-1 (TYPE SBE ATOS NOMINAL)
        "T1_Etat": str(t1.get("Etat", "5")), 
        "T1_Qt_A": str(t1.get("Qt. A", "0")), 
        "T1_Achat": str(t1.get("Achat", "0")), 
        "T1_Vente": str(t1.get("Vente", "0")), 
        "T1_Qt_V": str(t1.get("Qt.V", "0")), 
        "T1_Cloture": str(t1.get("Clôture", "0")), 
        "T1_CTO": str(t1.get("CTO", "0")), 
        "T1_VTO": str(t1.get("VTO", "0")), 
        "T1_Ouverture": str(t1.get("Ouverture", "0")), 
        "T1_Dernier": str(t1.get("Dernier", "0")), 
        "T1_Var": str(t1.get("Var.", "0")), 
        "T1_CMP": str(t1.get("CMP", "0")), 
        "T1_Qte_Seance": int(convert_num(t1.get("Qté", 0))), 
        "T1_Volume_Seance": round(float(convert_num(t1.get("Volume", 0.0))), 3)
    }
    registre_transactions_RAM.append(log_operation_geant)
    derniere_action_traitee = log_operation_geant.copy()
    
    a_trouve_un_trade = True
    return a_trouve_un_trade, courbe_txt, nature_ms

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : INTÉGRATEUR VECTORIEL MICROSTRUCTUREL SBE
# # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 1 DE 3)
# # SCellage : Ingestion dictionnaire RAM ultra-rapide et initialisation des listes
# ==============================================================================
def analyser_boucle_microstructure(df_final_cote, regime_actif, debut_calcul):
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, historique_phase_precedente, registre_transactions_RAM, derniere_action_traitee, volumes_seance, capitaux_seance, registre_RAM_t_moins_1, HEURE_CENTRALE_BVMT, FICHIER_HISTO_RAMDISK, MODE_PRODUCTION
    try:
        liste_phases, listes_microstructure, liste_volatiles = [], [], []
        l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i = [], [], [], [], [], []
        l_spread_p, l_imbalance_c = [], []
        l_etat_brut_api, l_etat_calcule_systeme = [], []
        l_heure_brute_api, l_heure_calculee_pc = [], []
        l_achat, l_vente, l_dernier, l_cloture, l_cto, l_vto, l_ouverture = [], [], [], [], [], [], []
        l_cmp, l_qte_seance, l_vol_seance, l_p_haut, l_p_bas = [], [], [], [], []
        l_sh, l_sb, l_dh, l_db = [], [], [], []
        l_variation = []
        a_trouve_un_trade, courbe_txt = False, "■ STABLE  [______]"
        
        maintenant_dt = datetime.datetime.now()
        heure_machine_formatee = maintenant_dt.strftime("%H:%M:%S")
        heure_marche_formatee = HEURE_CENTRALE_BVMT
        heure_api_verifiee = heure_marche_formatee
        heure_marche_active_titre = heure_marche_formatee

        cle_variation_api = "Var."
        for col_name in df_final_cote.columns:
            if "VAR" in str(col_name).upper(): 
                cle_variation_api = col_name
                break
                
        # Indexation par dictionnaire RAM ultra-rapide (0% latence CPU)
        dict_api_indexe = df_final_cote.to_dict(orient='index')

        for action in LISTE_VALEURS_UNIFIEES:
            action_cle = str(action).strip().upper()
            
            row = dict_api_indexe.get(action)
            if row is None:
                row = dict_api_indexe.get(action_cle)
                if row is None: 
                    continue
                
            groupe_attribue = DICT_VALEURS_BVMT[action]["groupe"]
            
            cours_reference_seance = convert_num(row.get("Clôture", 0.0))
            if cours_reference_seance <= 0: 
                cours_reference_seance = convert_num(row.get("CTO", 0.0))
            
            qte_brute_api = convert_num(row.get("Qté", 0))
            vol_brut_api = convert_num(row.get("Volume", 0.0))
            dernier_brut_api = convert_num(row.get("Dernier", 0.0))
            
            if qte_brute_api <= 0 and action in historique_volumes_qte and historique_volumes_qte[action] > 0:
                qte_totale_seance = historique_volumes_qte[action]
                vol_argent_seance = historique_volumes_montant[action]
                dernier_c = historique_dernier_cours.get(action, cours_reference_seance)
            else:
                qte_totale_seance = qte_brute_api
                vol_argent_seance = vol_brut_api
                dernier_c = dernier_brut_api

            global qte_totale_seance_current
            qte_totale_seance_current = qte_totale_seance
            variation_seance = convert_num(str(row.get(cle_variation_api, "0.0")).replace("%", ""))
            
            p_achat_t0, p_vente_t0 = convert_num(row.get("Achat", 0.0)), convert_num(row.get("Vente", 0.0))
            q_achat_t0, q_vente_t0 = convert_num(row.get("Qt. A", 0)), convert_num(row.get("Qt.V", 0))

            heure_brute_titre = str(row.get("Heure", heure_api_verifiee)).strip()
            if len(heure_brute_titre) == 6 and heure_brute_titre.isdigit():
                heure_brute_titre = f"{heure_brute_titre[:2]}:{heure_brute_titre[2:4]}:{heure_brute_titre[4:6]}"
            
            l_heure_brute_api.append(heure_brute_titre)
            l_heure_calculee_pc.append(heure_machine_formatee)

            code_brut_api_texte = str(row.get("Etat", "5")).strip()
            l_etat_brut_api.append(code_brut_api_texte)
            # ==============================================================================
            # 🦅 STATION QUANTITATIVE MAC SA : COMMUTATEUR CHRONOMÉTRIQUE ET FILTRAGE TICK
            # # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 2 DE 3)
            # # SCellage : Nettoyage syntaxique absolu (0% Instruction chaînée par ":")
            # ==============================================================================
            try:
                digit_brut = re.sub(r'[^\d]', '', code_brut_api_texte)
                state_num_physique_brut = int(digit_brut) if digit_brut != "" else int(row.get("Etat_Num", 5))
            except:
                state_num_physique_brut = 5

            state_reg_theorique, phase_reg, heure_cible_phase = verifier_phase_par_groupe(heure_marche_active_titre, groupe_attribue, regime_actif, action)
            
            alerte_seuil_active, pivot_retourne = calculer_alerte_seuil_bvmt(
                dernier_c, cours_reference_seance, groupe_attribue, action, maintenant_dt
            )

            if "12:00:00" <= heure_marche_active_titre < "12:05:00" and regime_actif != "HIVER" and groupe_attribue in (11, 21, 41):
                state_num_calcule = 3  
                phase_reg = "Pre_Fixing de cloture"
            elif "14:00:00" <= heure_marche_active_titre < "14:05:00" and regime_actif == "HIVER" and groupe_attribue in (11, 21, 41):
                state_num_calcule = 3  
                phase_reg = "Pre_Fixing de cloture"
            elif state_num_physique_brut == 8 or "SUSPENDU" in code_brut_api_texte.upper():
                state_num_calcule = 8
                phase_reg = "Cotation Suspendue"
            elif state_num_physique_brut == 9 or "RESERVE" in code_brut_api_texte.upper():
                state_num_calcule = 9
                phase_reg = "Reservation Volatilit"
                p_achat_t0 = round(pivot_retourne * 0.955, 3)
                p_vente_t0 = round(pivot_retourne * 1.045, 3)
            else:
                state_num_calcule = state_reg_theorique
                if state_num_calcule == 5: 
                    if "12:05" not in phase_reg and "14:05" not in phase_reg:
                        phase_reg = "Seance ordinaire (Negociation)"
                    else:
                        phase_reg = "Negociation au Dernier Cours"
                elif state_num_calcule == 3: 
                    phase_reg = "Pre_Fixing d'ouverture"

            l_etat_calcule_systeme.append(int(state_num_calcule))
            liste_phases.append(phase_reg)
            l_achat.append(p_achat_t0)
            l_vente.append(p_vente_t0)
            l_dernier.append(dernier_c)
            l_variation.append(variation_seance)
            l_cloture.append(cours_reference_seance)
            l_cto.append(convert_num(row.get("CTO", 0.0)))
            l_vto.append(convert_num(row.get("VTO", 0.0)))
            l_ouverture.append(convert_num(row.get("Ouverture", 0.0)))
            l_cmp.append(convert_num(row.get("CMP", dernier_c)))
            l_qte_seance.append(qte_totale_seance)
            l_vol_seance.append(vol_argent_seance)
            l_p_haut.append(convert_num(row.get("P. Haut", 0.0)))
            l_p_bas.append(convert_num(row.get("P. Bas", 0.0)))
            l_sh.append(convert_num(row.get("Seuil.S. Haut", 0.0)))
            l_sb.append(convert_num(row.get("Seuil.S. Bas", 0.0)))
            l_dh.append(convert_num(row.get("Seuil.D. Haut", 0.0)))
            l_db.append(convert_num(row.get("Seuil.D. Bas", 0.0)))

            spread_pourcent_t0 = (p_vente_t0 - p_achat_t0) / p_achat_t0 if p_achat_t0 > 0 else 0.0
            imbalance_carnet_t0 = (q_achat_t0 - q_vente_t0) / (q_achat_t0 + q_vente_t0) if (q_achat_t0 + q_vente_t0) > 0 else 0.0
            l_spread_p.append(spread_pourcent_t0)
            l_imbalance_c.append(imbalance_carnet_t0)

            delta_qte = qte_totale_seance - historique_volumes_qte.get(action, 0)
            if delta_qte < 0: 
                delta_qte = 0
            delta_vol = delta_qte * dernier_c if delta_qte > 0 else 0.0
            
            t1_memo = registre_RAM_t_moins_1.get(action, {})
            emp_t0 = f"{q_achat_t0}_{p_achat_t0}_{p_vente_t0}_{q_vente_t0}"
            emp_t1 = f"{t1_memo.get('T0_Qt_A', '0')}_{t1_memo.get('T0_Achat', '0')}_{t1_memo.get('T0_Vente', '0')}_{t1_memo.get('T0_Qt_V', '0')}" if t1_memo else ""
            
            h_clean = heure_brute_titre.strip()
            if regime_actif in ("ETE", "RAMADAN"):
                est_fenetre_ouv = "08:00:00" <= h_clean < "09:00:00"
                est_fenetre_clot = "12:00:00" <= h_clean < "12:05:00"
                est_fenetre_blocs = "12:15:00" <= h_clean <= "13:00:00"
            else:
                est_fenetre_ouv = "08:00:00" <= h_clean < "09:00:00"
                est_fenetre_clot = "14:00:00" <= h_clean < "14:05:00"
                est_fenetre_blocs = "14:15:00" <= h_clean <= "15:00:00"
            # ==============================================================================
            # 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
            # # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 3 DE 3)
            # # SCellage : Raccordement final sans écrasement (CPU à 0% en Hors-Séance)
            # ==============================================================================
            if (est_fenetre_ouv or est_fenetre_clot or est_fenetre_blocs) and (groupe_attribue == 11):
                est_carnet_modifie = (emp_t0 != emp_t1) and (emp_t1 != "")
            else:
                est_carnet_modifie = False

            if (delta_qte > 0 and dernier_c > 0) or est_carnet_modifie:
                if delta_qte > 0 and dernier_c > 0:
                    a_trouve_un_trade, courbe_txt, la_action = exécuter_moteur_lee_ready(
                        action, delta_qte, delta_vol, dernier_c, state_num_calcule, 
                        phase_reg, p_achat_t0, p_vente_t0, qte_totale_seance, 
                        vol_argent_seance, alerte_seuil_active, imbalance_carnet_t0, 
                        spread_pourcent_t0, variation_seance, cours_reference_seance, 
                        row, a_trouve_un_trade, heure_brute_titre, courbe_txt
                    )
                else:
                    if est_fenetre_ouv: la_action, char_ind = "ACCUMULATION", "A"
                    elif est_fenetre_clot: la_action, char_ind = "FIXING_CLOT", "F"
                    else: la_action, char_ind = "BLOC_MOD", "B"
                        
                    courbe_loc = historique_courbe_ticks.get(action, [])
                    courbe_loc.append(char_ind)
                    if len(courbe_loc) > 6: courbe_loc.pop(0)
                    jauge_txt = "".join(courbe_loc).ljust(6, "_")
                    courbe_txt = f"{char_ind} STATE  [{jauge_txt}]"
                    
                    jeton_passif_local = {
                        "Ticker": action, "Heure_Marche": heure_brute_titre, "Heure_Python": heure_machine_formatee,
                        "Nature_Operation": la_action, "Delta_Qte": 0, "Prix_Execution": float(dernier_c), "Delta_Vol": 0.0,
                        "T_Phase_Reglementaire": str(phase_reg), "T_Alerte_Volatilis": "BOOK_CHG", "T_Alerte_Seuil": str(alerte_seuil_active), "T_Graph_Flash": str(jauge_txt),
                        "T0_CMP": round(float(convert_num(row.get("CMP", dernier_c))), 3), "T0_Etat": str(phase_reg), "T0_Qt_A": str(q_achat_t0), "T0_Achat": str(p_achat_t0),
                        "T0_Vente": str(p_vente_t0), "T0_Qt_V": str(q_vente_t0), "T0_Cloture": str(row.get("Clôture", "0")), "T0_CTO": str(row.get("CTO", "0")),
                        "T0_VTO": str(row.get("VTO", "0")), "T0_Ouverture": str(row.get("Ouverture", "0")), "T0_Dernier": str(row.get("Dernier", "0")), "T0_Var": str(row.get("Var.", "0")),
                        "T0_CMP_Brut": f"{convert_num(row.get('CMP', dernier_c)):.3f}", "T0_Qte_Seance": int(qte_totale_seance), "T0_Volume_Seance": round(float(vol_argent_seance), 3), "T0_Heure_Marche": heure_brute_titre,
                        "T1_Etat": str(t1_memo.get("T0_Etat", "CLOTURE Marche ferme")), "T1_Qt_A": str(t1_memo.get("T0_Qt_A", "0")), "T1_Achat": str(t1_memo.get("T0_Achat", "0")),
                        "T1_Vente": str(t1_memo.get("T0_Vente", "0")), "T1_Qt_V": str(t1_memo.get("T0_Qt_V", "0")), "T1_Cloture": str(t1_memo.get("T0_Cloture", "0")),
                        "T1_CTO": str(t1_memo.get("T0_CTO", "0")), "T1_VTO": str(t1_memo.get("T0_VTO", "0")), "T1_Ouverture": str(t1_memo.get("T0_Ouverture", "0")),
                        "T1_Dernier": str(t1_memo.get("T0_Dernier", "0")), "T1_Var": str(t1_memo.get("T0_Var", "0")), "T1_CMP": str(t1_memo.get("T0_CMP", "0")),
                        "T1_Qte_Seance": int(t1_memo.get("T0_Qte_Seance", 0)), "T1_Volume_Seance": round(float(t1_memo.get("T0_Volume_Seance", 0.0)), 3)
                    }
                    registre_transactions_RAM.append(jeton_passif_local.copy())
            else: 
                la_action = "VEILLE"
                
            listes_microstructure.append(la_action)
            status_volatil = "🔲 Calme"
            if state_num_calcule == 9: 
                status_volatil = "⚠️ RÉSERVÉ HAUSSE" if dernier_c > cours_reference_seance else "⚠️ RÉSERVÉ BAISSE"
            elif delta_qte >= 8000: 
                status_volatil = "⚡ PIC DE VOLUME"
            liste_volatiles.append(status_volatil)

            if delta_qte > 0 and dernier_c > 0: 
                historique_dernier_cours[action] = dernier_c
            if action not in cumul_qte_achat: 
                cumul_qte_achat[action] = 0
                cumul_vol_achat[action] = 0.0
                cumul_qte_vente[action] = 0
                cumul_vol_vente[action] = 0.0
            if delta_qte > 0 and la_action in ["ACHAT", "VENTE"]:
                if la_action == "ACHAT": 
                    cumul_qte_achat[action] += delta_qte
                    cumul_vol_achat[action] += delta_vol
                elif la_action == "VENTE": 
                    cumul_qte_vente[action] += delta_qte
                    cumul_vol_vente[action] += delta_vol
            
            l_qte_a.append(int(cumul_qte_achat[action]))
            l_vol_a.append(float(cumul_vol_achat[action]))
            l_qte_v.append(int(cumul_qte_vente[action]))
            l_vol_v.append(float(cumul_vol_vente[action]))
            l_qte_i.append(int(qte_totale_seance - (cumul_qte_achat[action] + cumul_qte_vente[action])))
            l_vol_i.append(float(vol_argent_seance - (cumul_vol_achat[action] + cumul_vol_vente[action])))
            
            volumes_seance[action] = qte_totale_seance
            capitaux_seance[action] = vol_argent_seance
            historique_volumes_qte[action] = qte_totale_seance
            historique_volumes_montant[action] = vol_argent_seance
            
            registre_RAM_t_moins_1[action] = dict(row)
            registre_RAM_t_moins_1[action]["Etat_Num"] = int(state_num_physique_brut)
            registre_RAM_t_moins_1[action]["Achat"] = str(p_achat_t0)
            registre_RAM_t_moins_1[action]["Vente"] = str(p_vente_t0)
            registre_RAM_t_moins_1[action]["Dernier"] = str(dernier_c)
            registre_RAM_t_moins_1[action]["Heure"] = heure_brute_titre
            registre_RAM_t_moins_1[action]["Imbalance_Carnet"] = float(imbalance_carnet_t0)
            registre_RAM_t_moins_1[action]["Spread_Pourcent"] = float(spread_pourcent_t0)
            registre_RAM_t_moins_1[action]["T0_Qt_A"] = str(q_achat_t0)
            registre_RAM_t_moins_1[action]["T0_Achat"] = str(p_achat_t0)
            registre_RAM_t_moins_1[action]["T0_Vente"] = str(p_vente_t0)
            registre_RAM_t_moins_1[action]["T0_Qt_V"] = str(q_vente_t0)

        if registre_transactions_RAM:
            registre_transactions_RAM = [tx for tx in registre_transactions_RAM if int(tx.get("Delta_Qte", 0)) > 0]

        params_propulsion = {
            "df_final_cote": df_final_cote, "liste_phases": liste_phases, 
            "listes_microstructure": listes_microstructure, "liste_volatiles": liste_volatiles, 
            "l_achat": l_achat, "l_vente": l_vente, "l_dernier": l_dernier, "l_variation": l_variation, 
            "l_cloture": l_cloture, "l_cto": l_cto, "l_vto": l_vto, "l_ouverture": l_ouverture, "l_cmp": l_cmp, 
            "l_qte_seance": l_qte_seance, "l_vol_seance": l_vol_seance, "l_p_haut": l_p_haut, "l_p_bas": l_p_bas, 
            "l_sh": l_sh, "l_sb": l_sb, "l_dh": l_dh, "l_db": l_db, "l_qte_a": l_qte_a, "l_vol_a": l_vol_a, 
            "l_qte_v": l_qte_v, "l_vol_v": l_vol_v, "l_qte_i": l_qte_i, "l_vol_i": l_vol_i, "l_spread_p": l_spread_p, 
            "l_imbalance_c": l_imbalance_c, "a_trouve_un_trade": a_trouve_un_trade, "l_heure_brute_api": l_heure_brute_api, 
            "l_heure_calculee_pc": l_heure_calculee_pc, "l_etat_brut_api": l_etat_brut_api, 
            "l_etat_calcule_systeme": l_etat_calcule_systeme, "courbe_txt": courbe_txt, 
            "heure_cible_phase": heure_cible_phase, "debut_calcul": debut_calcul
        }
        finaliser_matrice_et_propulser(**params_propulsion)
    except Exception as e: 
        print(f"❌ Erreur générale de traitement de la boucle microstructurelle : {e}")
if 'REGIME_ACTIF_SIMU' not in globals():
    try:
        # On utilise votre fonction de la Partie 7 pour fixer le flag au démarrage
        REGIME_ACTIF_SIMU = detecter_regime_horaire(datetime.date.today())
    except:
        # En cas d'appel précoce avant lecture de la fonction, repli d'usine
        REGIME_ACTIF_SIMU = "HIVER"

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL SBE LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (PARTIE 1 DE 2)
# # SCellage : Ingestion d autorité absolue des 14 colonnes manquantes dans l index
# ==============================================================================
if 'global_dernier_ticker_trade' not in globals():
    global global_dernier_ticker_trade, global_dernier_ticker_book, registre_RAM_BBO_passe
    global_dernier_ticker_trade = "SFBT"
    global_dernier_ticker_book = "BNA"
    registre_RAM_BBO_passe = {}

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL SBE LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (PARTIE 1 DE 2 CORRIGÉE)
# # SCellage : Ingestion d autorité absolue des 40 colonnes et tri de surchauffe sécurisé
# ==============================================================================
# ==============================================================================
#  eagles STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (BLOC 1 DE 2)
# # SCellage : Ingestion d autorité absolue des structures 40 colonnes unifiées
# ==============================================================================
def finaliser_matrice_et_propulser(**kwargs):
    global registre_transactions_RAM, derniere_action_traitee, FICHIER_WIDGET_TXT, FICHIER_TEXTE_GRILLE, LISTE_VALEURS_UNIFIEES, FICHIER_HISTO_RAMDISK, MODE_PRODUCTION
    global global_dernier_ticker_trade, global_dernier_ticker_book, registre_RAM_BBO_passe, HEURE_CENTRALE_BVMT, REGIME_ACTIF_SIMU
    try:
        action_affichage_secours = "[ PAS DE FLUX ]"
        str_radar_alerte = "🔲 Ingestion SBE Falcon connectée. Flux unifié stable."

        df_final_cote = kwargs["df_final_cote"]
        liste_phases = kwargs["liste_phases"]
        listes_microstructure = kwargs["listes_microstructure"]
        liste_volatiles = kwargs["liste_volatiles"]
        l_achat, l_vente, l_dernier = kwargs["l_achat"], kwargs["l_vente"], kwargs["l_dernier"]
        l_variation, l_cloture = kwargs["l_variation"], kwargs["l_cloture"]
        l_cto, l_vto, l_ouverture, l_cmp = kwargs["l_cto"], kwargs["l_vto"], kwargs["l_ouverture"], kwargs["l_cmp"]
        l_qte_seance, l_vol_seance = kwargs["l_qte_seance"], kwargs["l_vol_seance"]
        l_p_haut, l_p_bas = kwargs["l_p_haut"], kwargs["l_p_bas"]
        l_sh, l_sb, l_dh, l_db = kwargs["l_sh"], kwargs["l_sb"], kwargs["l_dh"], kwargs["l_db"]
        l_qte_a, l_vol_a, l_qte_v, l_vol_v = kwargs["l_qte_a"], kwargs["l_vol_a"], kwargs["l_qte_v"], kwargs["l_vol_v"]
        l_qte_i, l_vol_i = kwargs["l_qte_i"], kwargs["l_vol_i"]
        l_spread_p, l_imbalance_c = kwargs["l_spread_p"], kwargs["l_imbalance_c"]
        a_trouve_un_trade = kwargs["a_trouve_un_trade"]
        l_heure_brute_api = kwargs["l_heure_brute_api"]
        l_heure_calculee_pc = kwargs["l_heure_calculee_pc"]
        l_etat_brut_api = kwargs["l_etat_brut_api"]
        l_etat_calcule_systeme = kwargs["l_etat_calcule_systeme"]
        courbe_txt = kwargs["courbe_txt"]
        heure_cible_phase = kwargs["heure_cible_phase"]
        debut_calcul = kwargs["debut_calcul"]


        df_final = pd.DataFrame(index=range(len(LISTE_VALEURS_UNIFIEES)))
        df_final["SbeMarket SheetValeur"] = LISTE_VALEURS_UNIFIEES
        df_final["Etat"] = l_etat_brut_api
        df_final["Qt. A"] = df_final_cote["Qt. A"].values
        df_final["Achat"] = l_achat
        df_final["Vente"] = l_vente
        df_final["Qt.V"] = df_final_cote["Qt.V"].values
        df_final["Clôture"] = l_cloture
        df_final["CTO"] = l_cto
        df_final["VTO"] = l_vto
        df_final["Ouverture"] = l_ouverture
        df_final["Dernier"] = l_dernier
        df_final["Var."] = l_variation
        df_final["CMP"] = l_cmp
        df_final["Qté"] = l_qte_seance
        df_final["Volume"] = l_vol_seance
        df_final["P. Haut"] = l_p_haut
        df_final["P. Bas"] = l_p_bas
        df_final["Seuil.S. Haut"] = l_sh
        df_final["Seuil.S. Bas"] = l_sb
        df_final["Seuil.D. Haut"] = l_dh
        df_final["Seuil.D. Bas"] = l_db
        df_final["Heure"] = l_heure_brute_api
        
        df_final["Cumul_Qte_Achat"] = l_qte_a
        df_final["Cumul_Vol_Achat"] = l_vol_a
        df_final["Cumul_Qte_Vente"] = l_qte_v
        df_final["Cumul_Vol_Vente"] = l_vol_v
        
        l_diff_qte = [int(l_qte_a[i] - l_qte_v[i]) for i in range(len(l_qte_a))]
        l_diff_vol = [float(l_vol_a[i] - l_vol_v[i]) for i in range(len(l_vol_a))]
        
        l_cmp_diff = []
        for i in range(len(l_diff_qte)):
            try:
                denom_loc = abs(l_diff_qte[i])
                if denom_loc > 0:
                    l_cmp_diff.append(round(abs(l_diff_vol[i]) / denom_loc, 3))
                else:
                    l_cmp_diff.append(l_dernier[i])
            except:
                l_cmp_diff.append(l_dernier[i])
                
        df_final["Diff_Qte_Nette"] = l_diff_qte
        df_final["Diff_Vol_Nette"] = l_diff_vol
        df_final["CMP_Diff_Nette"] = l_cmp_diff
        df_final["Spread_Millimes"] = ((df_final["Vente"] - df_final["Achat"]) * 1000).fillna(0).astype(int)
        df_final["Spread_Pourcent"] = l_spread_p
        df_final["Imbalance_Carnet"] = l_imbalance_c
        
        l_tendance_veille = []
        l_tendance_ouv = []
        for i in range(len(l_dernier)):
            l_tendance_veille.append("HAUSSE/VEILLE" if l_dernier[i] > l_cloture[i] else ("BAISSE/VEILLE" if l_dernier[i] < l_cloture[i] else "STABLE/VEILLE"))
            ref_ouv = l_ouverture[i] if l_ouverture[i] > 0 else l_cloture[i]
            l_tendance_ouv.append("HAUSSE/OUV" if l_dernier[i] > ref_ouv else ("BAISSE/OUV" if l_dernier[i] < ref_ouv else "STABLE/OUV"))
            
        df_final["Tendance_Veille"] = l_tendance_veille
        df_final["Tendance_Ouverture"] = l_tendance_ouv
        
        heures_reprise_dynamiques = []
        for idx_act, act_name in enumerate(LISTE_VALEURS_UNIFIEES):
            info_gel_act = registre_reservation_groupe11.get(act_name, {})
            if info_gel_act.get("en_gel") and info_gel_act.get("fin_gel_dt"):
                heures_reprise_dynamiques.append(info_gel_act["fin_gel_dt"].strftime("%H:%M:%S"))
            else:
                heures_reprise_dynamiques.append(heure_cible_phase if heure_cible_phase else "00:00:00")
                
        df_final["Heure_Reprise_Calcul"] = heures_reprise_dynamiques
        df_final["Phase_Reglementaire"] = liste_phases
        df_final["Nature_Operation"] = listes_microstructure
        df_final["Volatiles_Alertes"] = liste_volatiles
        df_final["Etat calculé"] = l_etat_calcule_systeme
        df_final["Heure calculée"] = l_heure_calculee_pc
# === FIN DE LA PARTIE 1 DE 2 STRICTEMENT COMPILABLE ===
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (BLOC 2 DE 2)
# # SCellage : Fixation micrométrique de l heure maximale en Colonne 22 (V)
# ==============================================================================
        df_incidents = df_final[df_final["SbeMarket SheetValeur"] != "SYS_MODE_CONFIG"]
        if not df_incidents.empty:
            df_reserve = df_incidents[df_incidents["Etat calculé"] == 9]
            if not df_reserve.empty:
                liste_valeurs_gel = df_reserve["SbeMarket SheetValeur"].tolist()
                str_radar_alerte = f"⚠️ ALERTE MICROSTRUCTURELLE : Réservation active sur {', '.join(liste_valeurs_gel)}"
            else:
                df_tri_var = df_incidents.copy()
                df_tri_var["Var_Abs"] = df_tri_var["Var."].apply(lambda x: abs(convert_num(x)))
                df_tri_var = df_tri_var.sort_values(by="Var_Abs", ascending=False)
                
                if len(df_tri_var) > 0 and float(df_tri_var["Var_Abs"].iloc[0]) >= 3.0:
                    str_radar_alerte = f"🔥 FORTE PRESSION : Anomalie détectée sur {df_tri_var['SbeMarket SheetValeur'].iloc[0]} ({df_tri_var['Var.'].iloc[0]}%)"

        ORDRE_COLONNES_VERROUILLES = [
            "SbeMarket SheetValeur", "Etat", "Qt. A", "Achat", "Vente", "Qt.V", "Clôture", "CTO", "VTO", "Ouverture", 
            "Dernier", "Var.", "CMP", "Qté", "Volume", "P. Haut", "P. Bas", "Seuil.S. Haut", "Seuil.S. Bas", "Seuil.D. Haut", 
            "Seuil.D. Bas", "Heure", "Cumul_Qte_Achat", "Cumul_Vol_Achat", "Cumul_Qte_Vente", "Cumul_Vol_Vente", 
            "Diff_Qte_Nette", "Diff_Vol_Nette", "CMP_Diff_Nette", "Spread_Millimes", "Spread_Pourcent", "Imbalance_Carnet", 
            "Phase_Reglementaire", "Nature_Operation", "Tendance_Veille", "Tendance_Ouverture", "Heure_Reprise_Calcul", "Volatiles_Alertes",
            "Etat calculé", "Heure calculée"
        ]

        df_final_ordonne = df_final[ORDRE_COLONNES_VERROUILLES].copy()

        if a_trouve_un_trade and 'derniere_action_traitee' in globals() and 'Ticker' in derniere_action_traitee:
            global_dernier_ticker_trade = str(derniere_action_traitee.get("Ticker", global_dernier_ticker_trade)).strip().upper()
            
        dict_instant_indexe = df_final_ordonne.set_index("SbeMarket SheetValeur").to_dict(orient='index')
        for v_name in LISTE_VALEURS_UNIFIEES:
            v_net = str(v_name).strip().upper()
            row_instant_json = dict_instant_indexe.get(v_net)
            if row_instant_json is not None:
                emp_precedente = registre_RAM_BBO_passe.get(v_net, "0_0_0_0")
                emp_courante = f"{row_instant_json.get('Qt. A', '0')}_{row_instant_json.get('Achat', '0')}_{row_instant_json.get('Vente', '0')}_{row_instant_json.get('Qt.V', '0')}"
                if emp_courante != emp_precedente and emp_precedente != "0_0_0_0":
                    global_dernier_ticker_book = v_net
                    registre_RAM_BBO_passe[v_net] = emp_courante
                    break 
                if emp_precedente == "0_0_0_0":
                    registre_RAM_BBO_passe[v_net] = emp_courante

        # 🎯 CONFORMITÉ CHRONOMÉTRIQUE ROYALE MAC SA
        mode_detecte_str = "PRODUCTION_REEL" if MODE_PRODUCTION else "SIMULATION_VIRTUELLE"
        ligne_config = {col: "0" for col in ORDRE_COLONNES_VERROUILLES}
        ligne_config["SbeMarket SheetValeur"] = "SYS_MODE_CONFIG"
        ligne_config["Var."] = str(mode_detecte_str)
        
        # 🟢 Colonne 2 (B) : Conserve sa fonction d'identification BBO d'accumulation
        ligne_config["Etat"] = str(global_dernier_ticker_book)
        
        # 🟢 Colonne 22 (V) : Reçoit strictly l'heure maximale synchronisée et ajustée
        ligne_config["Heure"] = str(HEURE_CENTRALE_BVMT)
        
        ligne_config["Nature_Operation"] = str(global_dernier_ticker_trade)
        ligne_config["Tendance_Veille"] = f"{global_dernier_ticker_book}*{global_dernier_ticker_trade}*{HEURE_CENTRALE_BVMT}"
        ligne_config["Volatiles_Alertes"] = str(REGIME_ACTIF_SIMU)
        ligne_config["Phase_Reglementaire"] = str(str_radar_alerte)
        
        df_flag = pd.DataFrame([ligne_config])
        df_final_ordonne = pd.concat([df_final_ordonne, df_flag], ignore_index=True)

        # Calcul autonome de l'affichage de secours basé strictly sur l'activité microstructurelle
        action_affichage_secours = global_dernier_ticker_trade if str(HEURE_CENTRALE_BVMT).replace(":", "").strip()[:4] >= "0900" else global_dernier_ticker_book

        compiler_et_sauvegarder_fichiers(
            df_final_cote, df_final_ordonne, liste_phases, listes_microstructure, 
            l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i, l_spread_p, l_imbalance_c, 
            a_trouve_un_trade, HEURE_CENTRALE_BVMT, l_heure_calculee_pc[-1], courbe_txt, 
            int(l_etat_calcule_systeme[-1]), heure_cible_phase, action_affichage_secours, str_radar_alerte, 
            ORDRE_COLONNES_VERROUILLES, debut_calcul
        )
    except Exception as e: 
        print(f"❌ Erreur sous-compilation structurelle de la matrice : {e}")
# === FIN DE LA PROCÉDURE INTERNE EXHAUSTIVE ET SCELLÉE ===

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : PARSEUR DE PROPULSION ET GESTION DES FLUSHERS
# # Fichier : bourse_macsa.py -> compiler_et_sauvegarder_fichiers() [CORRIGÉ]
# # SCellage : Restauration de l orthographe 'empreinte_actuelle' et persistance OLE
# ==============================================================================
def compiler_et_sauvegarder_fichiers(df_final_cote, df_final_ordonne, liste_phases, listes_microstructure, l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i, l_spread_p, l_imbalance_c, a_trouve_un_trade, heure_ticker_brut, heure_ticker_pc, courbe_txt, id_phase_calculee, heure_cible_phase, action_affichage_secours, str_radar_alerte, ORDRE_COLONNES_VERROUILLES, debut_calcul):
    global registre_transactions_RAM, FICHIER_HISTO_SEANCE, FICHIER_HISTO_RAMDISK, FICHIER_TEXTE_GRILLE, FICHIER_SYNTHESE_CLOTURE, FICHIER_ETAT_RAM, EST_ARCHIVE_FINALE_ENGAGEE, MODE_PRODUCTION
    try:
        try:
            str_dernier = df_final_ordonne['Dernier'].astype(str).str.cat()
            str_qte = df_final_ordonne['Qté'].astype(str).str.cat()
            str_achat = df_final_ordonne['Achat'].astype(str).str.cat()
            str_vente = df_final_ordonne['Vente'].astype(str).str.cat()
            empreinte_actuelle = f"{str_dernier}_{str_qte}_{str_achat}_{str_vente}"
        except:
            empreinte_actuelle = str(heure_ticker_brut)

        empreinte_precedente = historique_phase_precedente.get("EMPREINTE_GRILLE_RAM", "")
        
        # 🎯 RECTIFICATION DIRECTE : Correction de la faute de frappe 'empreinte_actuelle'
        if a_trouve_un_trade or empreinte_actuelle != empreinte_precedente or heure_ticker_pc != historique_phase_precedente.get("DERNIER_PC_TICK", ""):
            try:
                historique_phase_precedente["DERNIER_PC_TICK"] = heure_ticker_pc
                historique_phase_precedente["EMPREINTE_GRILLE_RAM"] = empreinte_actuelle
                with open(FICHIER_TEXTE_GRILLE, "w", encoding="utf-8-sig", errors="replace") as f_grille:
                    df_final_ordonne[ORDRE_COLONNES_VERROUILLES].to_csv(f_grille, sep="\t", index=False, lineterminator="\n")
            except: pass

        # Journalisation au fil de l'eau du grand livre de séance (temp_...)
        # 🟢 CONSOLIDATION ANALYTIQUE : Ingestion et Tri Chronologique Croissant du Grand Livre
        if registre_transactions_RAM:
            try:
                df_histo = pd.DataFrame(registre_transactions_RAM)
                df_histo["Date_Seance"] = datetime.date.today().strftime("%d/%m/%Y")
                df_histo = df_histo.dropna(how='all')
                
                if not df_histo.empty:
                    # 🎯 CORE DE TRI MICROSTRUCTUREL : Réalignement des transactions non chronologiques
                    # On force le tri croissant sur l'horodatage d'usine de la Bourse de Tunis (Heure_Marche)
                    df_histo = df_histo.sort_values(by="Heure_Marche", ascending=True, kind="mergesort")
                    
                    # Gravure sécurisée sur disque de la trame réordonnée
                    df_histo.to_csv(
                        FICHIER_HISTO_RAMDISK, 
                        mode='a', 
                        header=not os.path.exists(FICHIER_HISTO_RAMDISK), 
                        sep=';', 
                        index=False, 
                        encoding='utf-8', 
                        lineterminator="\n"
                    )
                registre_transactions_RAM.clear()
            except Exception as e_tri_log:
                print(f"⚠️ Alerte microstructurelle de tri chronologique : {e_tri_log}")

        # Verrou intra-séance préventif de minute "01"
        minute_courante_str = heure_ticker_pc[3:5]
        cle_verrou_heure = f"FLUSH_DONE_{heure_ticker_pc[:2]}"
        
        maintenant_obj = datetime.datetime.now()
        est_un_jour_ouvrable = maintenant_obj.weekday() < 5
        est_dans_la_fenetre_seance = "08:00:00" <= heure_ticker_pc <= "18:00:00"
        
        if est_un_jour_ouvrable and est_dans_la_fenetre_seance and minute_courante_str == "01" and not historique_phase_precedente.get(cle_verrou_heure, False):
            if os.path.exists(FICHIER_HISTO_RAMDISK) and MODE_PRODUCTION:
                try:
                    df_total_seance = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                    df_total_seance = df_total_seance.dropna(how='all')
                    if not df_total_seance.empty:
                        df_total_seance.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                        print(f"\n[🛡️ FLUSHER INTRA-SÉANCE] Sécurité exécutée à {heure_ticker_pc} : Grand livre sauvegardé.")
                    historique_phase_precedente[cle_verrou_heure] = True
                except: pass

        # Gestion comptable du Closing de Place (15h00) sans coupure de script brutale
        h_clean_brut_test = str(heure_ticker_brut).replace(":", "").strip()[:4]
        if h_clean_brut_test >= "1500" and not EST_ARCHIVE_FINALE_ENGAGEE:
            try:
                if os.path.exists(FICHIER_HISTO_RAMDISK):
                    if MODE_PRODUCTION:
                        df_ramdisk = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                        df_ramdisk = df_ramdisk.dropna(how='all')
                        df_ramdisk.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                        print(f"\n[🏁] CLOSING MARCHÉ RÉEL : Données de session consolidées sur disque C:.")
                    
                if MODE_PRODUCTION:
                    df_final_ordonne.to_csv(FICHIER_SYNTHESE_CLOTURE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
                EST_ARCHIVE_FINALE_ENGAGEE = True
            except Exception as e_cloture:
                print(f"⚠️ Échec du transfert de clôture nominale : {e_cloture}")

        # Affichage console synchrone découplé (0% interdépendance)
        heure_systeme_actuelle = time.strftime('%H:%M:%S')
        temps_execution_ms = (time.perf_counter_ns() - debut_calcul) / 1_000_000
        trame_console = f"[*] [PC: {heure_systeme_actuelle}] | BVMT: {heure_ticker_brut} | Noyau: {temps_execution_ms:.1f}ms | Base de données RAM: OK"
        sys.stdout.write(f"\r{trame_console.ljust(110)[:110]}\x1b[K")
        sys.stdout.flush()

        if os.path.exists(FICHIER_HISTO_RAMDISK) or MODE_PRODUCTION:
            sauvegarder_etat_RAM_disque()

    except Exception as e_main:
        print(f"❌ Erreur sous-traitement d'écriture fichier : {e_main}")



# 🎯 PERSISTANCE JSON RECTIFIÉE SCELLÉE NOMINALE
def sauvegarder_etat_RAM_disque():
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, FICHIER_ETAT_RAM, registre_RAM_t_moins_1, derniere_action_traitee
    try:
        historique_cours_propre = dict(historique_dernier_cours)
        for tk in LISTE_VALEURS_UNIFIEES:
            t1_memo = registre_RAM_t_moins_1.get(tk, {})
            historique_cours_propre[f"{tk}_Imbalance"] = float(t1_memo.get("Imbalance_Carnet", 0.0))
            historique_cours_propre[f"{tk}_Spread"] = float(t1_memo.get("Spread_Pourcent", 0.0))

        dict_miroir_ram = {
            "cumul_qte_achat": dict(cumul_qte_achat), 
            "cumul_vol_achat": dict(cumul_vol_achat),
            "cumul_qte_vente": dict(cumul_qte_vente), 
            "cumul_vol_vente": dict(cumul_vol_vente),
            "cumul_qte_indecis": dict(cumul_qte_indecis), 
            "cumul_vol_indecis": dict(cumul_vol_indecis),
            "historique_volumes_qte": dict(historique_volumes_qte), 
            "historique_volumes_montant": dict(historique_volumes_montant),
            "historique_dernier_cours": historique_cours_propre,
            # Remplacement de l action fictive SFBT par le drapeau de neutralité universel SYS_STANDBY
            "derniere_action_traitee": dict(derniere_action_traitee) if "registre_transactions_RAM" in globals() and (len(registre_transactions_RAM) > 0 or  derniere_action_traitee) else {
                "Ticker": "SYS_STANDBY", "Heure_Marche": HEURE_CENTRALE_BVMT, "Heure_Python": time.strftime('%H:%M:%S'),
                "Nature_Operation": "VEILLE", "Delta_Qte": 0, "Prix_Execution": 0.0, "Delta_Vol": 0.0,
                "T_Phase_Reglementaire": "CLOTURE Marche ferme", "T0_CMP": 0.0, "Tendance_Veille": "STABLE",
                "Tendance_Ouverture": "STABLE", "Heure_Reprise_Calcul": "00:00:00"
            }
        }
        with open(FICHIER_ETAT_RAM, "w", encoding="utf-8") as f_json:
            json.dump(dict_miroir_ram, f_json, indent=4, ensure_ascii=False)
    except:
        pass

# Initialisation de la grille maîtresse permanente en mémoire vive active
df_master_ram_cote = None

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : MOTEUR ASYNCHRONE D ASPIRATION FALCON DESENCLAVÉ
# # Fichier : bourse_macsa.py -> boucle_background_aspiration_falcon()
# # SCellage : Élargissement du timeout à 3.0s et initialisation d autorité du tampon (0% Gel)
# ==============================================================================
def boucle_background_aspiration_falcon():
    global BUFFER_TEXTE_RESEAU_FALCON, URL_API, MODE_SIMULATION_ACTIF, VERROU_PARTAGE_THREAD
    
    # 🟢 SÉCURISATION SESSION : Allocation d'un adaptateur de transport persistant
    session_falcon = requests.Session()
    session_falcon.headers.update({"User-Agent": "Mozilla/5.0 MAC-SA-QUANT-NODE"})
    
    # 🎯 AMORCE DE CHARGE DE SÉCURITÉ : Évite que le buffer ne démarre à blanc au cycle zéro
    with VERROU_PARTAGE_THREAD:
        if BUFFER_TEXTE_RESEAU_FALCON == "":
            BUFFER_TEXTE_RESEAU_FALCON = "SbeMarket SheetValeur;Etat;Qt. A;Achat;Vente;Qt.V;Clôture;CTO;VTO;Ouverture;Dernier;Var.;CMP;Qté;Volume;P. Haut;P. Bas;Seuil.S. Haut;Seuil.S. Bas;Seuil.D. Haut;Seuil.D. Bas;Heure\n"

    while True:
        try:
            # 🟢 INGESTION CLOUD ADAPTATIVE (0% PC DEPENDENCY)
            # Koyeb interroge l'URL_API dynamique configurée (Port 8443 en réel ou Port 443 en simulation)
            reponse = session_falcon.get(URL_API, verify=False, timeout=5.0)
            
            if reponse.status_code == 200:
                reponse.encoding = 'latin1'
                txt_recu = reponse.text
                
                # Validation microstructurelle de la trame avant mise en cache RAM
                if txt_recu and ";" in txt_recu and len(txt_recu) > 500:
                    with VERROU_PARTAGE_THREAD:
                        BUFFER_TEXTE_RESEAU_FALCON = txt_recu
            
            # Cadencement dynamique : Fréquence maximale en réel vs soulagement réseau en simulation
            time.sleep(0.02 if not MODE_SIMULATION_ACTIF else 0.2)
            
        except Exception as e_net:
            # En cas de micro-coupure réseau ou de standby de l'API, temporisation de sûreté
            time.sleep(0.05 if not MODE_SIMULATION_ACTIF else 1.0)


def executer_requete_sbe_marche_live():
    global BUFFER_TEXTE_RESEAU_FALCON, VERROU_PARTAGE_THREAD, MODE_SIMULATION_ACTIF, HEURE_CENTRALE_BVMT
    
    # 📡 1. ARMEMENT ET EXPÉDITION DU THREAD D'ARRIÈRE-PLAN (0% LATENCE CPU)
    if not hasattr(executer_requete_sbe_marche_live, "thread_lance"):
        executer_requete_sbe_marche_live.thread_lance = True
        t_bg = threading.Thread(target=boucle_background_aspiration_falcon, daemon=True)
        t_bg.start()
        print("[*] 📡 Thread asynchrone d'arrière-plan Falcon SBE initialisé sur Koyeb Node.")

    # Extraction sécurisée du tampon réseau partagé
    with VERROU_PARTAGE_THREAD:
        text_brut = BUFFER_TEXTE_RESEAU_FALCON

    # 📡 2. ALIGNEMENT COMPTABLE DE L'HORLOGE VIRTUELLE EN MODE SIMULATION INTER-CLOUD
    if 'MODE_SIMULATION_ACTIF' in globals() and MODE_SIMULATION_ACTIF:
        if text_brut and ";" in text_brut and len(text_brut) > 500:
            try:
                # Dépistage inversé ultrarapide pour intercepter le dernier horodatage valide de la Place
                lignes_calcul = text_brut.split('\n')
                for idx_l in range(len(lignes_calcul) - 1, -1, -1):
                    ligne_temoin = lignes_calcul[idx_l].strip()
                    
                    # On isole strictly une ligne d'instrument peuplée (colonne 22 présente)
                    if ';' in ligne_temoin and "SbeMarket" not in ligne_temoin and "Total" not in ligne_temoin:
                        elements = ligne_temoin.split(';')
                        if len(elements) >= 22:
                            h_brut = str(elements[21]).strip().replace(":", "")
                            if h_brut.isdigit() and len(h_brut) == 6:
                                # Synchronisation impérative de l'oscilloscope central de séance (hh:mm:ss)
                                HEURE_CENTRALE_BVMT = f"{h_brut[:2]}:{h_brut[2:4]}:{h_brut[4:6]}"
                                break  # Alignement réussi, on quitte le balayage
            except:
                pass

    return text_brut



# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : SYSTÈME DE RÉHYDRATATION POST-COUPURE JSON
# # Fichier : bourse_macsa.py -> restaurer_etat_RAM_depuis_coupure() & coupe-circuit
# # PARTIE 18 / 20 : REPLI EXCLUSIF DU CACHE SANS PERTE DE TYPE (NOMINALE OLE)
# ==============================================================================
def restaurer_etat_RAM_depuis_coupure():
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, FICHIER_ETAT_RAM, DATE_JOUR_STR, MODE_SIMULATION_ACTIF
    
    if os.path.exists(FICHIER_ETAT_RAM) and os.path.getsize(FICHIER_ETAT_RAM) > 50:
        try:
            # 🟢 PROTECTION ADAPTATIVE INTER-CLOUD :
            # On ignore la barrière de date stricte en mode simulation pour autoriser le rejeu d'historiques passés
            if not MODE_SIMULATION_ACTIF and DATE_JOUR_STR not in os.path.basename(FICHIER_ETAT_RAM):
                print(f"[*] [⏳ SÉCURITÉ QUARTZ] Registre JSON de Place obsolète. Nouveau départ à blanc engagé.")
                return

            with open(FICHIER_ETAT_RAM, "r", encoding="utf-8") as f_state:
                etat_global = json.load(f_state)
            
            def epurer_dico_ram(dico_cible):
                dico_propre = {}
                for k, v in dico_cible.items():
                    try:
                        val_str = str(v).replace("[", "").replace("]", "").replace(",", ".").strip()
                        dico_propre[str(k).strip()] = float(val_str) if "." in val_str else int(float(val_str))
                    except:
                        dico_propre[str(k).strip()] = 0.0
                return dico_propre

            # Hydratation croisée atomique des registres microstructurels en RAM
            cumul_qte_achat.update(epurer_dico_ram(etat_global.get("cumul_qte_achat", {})))
            cumul_vol_achat.update(epurer_dico_ram(etat_global.get("cumul_vol_achat", {})))
            cumul_qte_vente.update(epurer_dico_ram(etat_global.get("cumul_qte_vente", {})))
            cumul_vol_vente.update(epurer_dico_ram(etat_global.get("cumul_vol_vente", {})))
            cumul_qte_indecis.update(epurer_dico_ram(etat_global.get("cumul_qte_indecis", {})))
            cumul_vol_indecis.update(epurer_dico_ram(etat_global.get("cumul_vol_indecis", {})))
            historique_volumes_qte.update(epurer_dico_ram(etat_global.get("historique_volumes_qte", {})))
            historique_volumes_montant.update(epurer_dico_ram(etat_global.get("historique_volumes_montant", {})))
            historique_dernier_cours.update(epurer_dico_ram(etat_global.get("historique_dernier_cours", {})))
            
            nom_env = "SIMULATION REJOUÉE" if MODE_SIMULATION_ACTIF else "MARCHÉ RÉEL LIVE"
            print(f"[*] [🛡️ REPRISE SCELLÉE] Écosystème {nom_env} réhydraté avec succès. 77 valeurs chargées.")
        except Exception as e:
            print(f"⚠️ Erreur rechargement d'état RAM après coupure : {e}")


def declencher_coupe_circuit_operateur(signum, frame):
    sys.stdout.write("\n\n[*] 🛑 SIGNAL D'ARRÊT REÇU : Extinction sécurisée du nœud quantitatif MAC SA...\n")
    sys.stdout.flush()
    try:
        global registre_transactions_RAM, FICHIER_HISTO_SEANCE, FICHIER_HISTO_RAMDISK, FICHIER_ETAT_RAM, MODE_PRODUCTION
        
        # 🟢 CONSOLIDATION DE SÉCURITÉ : Gravure immédiate des transactions accumulées en RAM
        if registre_transactions_RAM:
            try:
                df_histo = pd.DataFrame(registre_transactions_RAM)
                df_histo["Date_Seance"] = datetime.date.today().strftime("%d/%m/%Y")
                df_histo.to_csv(FICHIER_HISTO_RAMDISK, mode='a', header=not os.path.exists(FICHIER_HISTO_RAMDISK), sep=';', index=False, encoding='utf-8', lineterminator="\n")
                registre_transactions_RAM.clear()
            except: pass
                
        # 🟢 FLUSH SCELLÉ SUR LE STOCKAGE PERSISTANT DE L'HÉBERGEUR CLOUD
        if os.path.exists(FICHIER_HISTO_RAMDISK):
            try:
                df_ramdisk = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                # Ingestion forcée dans le grand livre permanent (.csv) sans distinction de mode
                df_ramdisk.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                sys.stdout.write(f"[*] [🛡️ SÉCURITÉ CLOUD] Grand livre d'Order-Flow sauvegardé : {os.path.basename(FICHIER_HISTO_SEANCE)}\n")
                sys.stdout.flush()
            except Exception as e_flush:
                sys.stdout.write(f"⚠️ Échec du flusher de sécurité disque : {str(e_flush)}\n")
                sys.stdout.flush()
                
        # 🟢 CONSERVATION DES RAPPORTS : Suppression des instructions 'os.remove' destructives
        # Cela garantit que les fichiers restent accessibles après l'arrêt du processus maître
        if os.path.exists(FICHIER_ETAT_RAM):
            print(f"[*] [🏁 SYSTEM] Image miroir RAM préservée pour le prochain reboot.")

        # Sauvegarde finale de l'état des dictionnaires pour la reprise sur Koyeb
        sauvegarder_etat_RAM_disque()
            
    except Exception as e_circuit:
        sys.stdout.write(f"❌ Erreur critique dans le coupe-circuit : {str(e_circuit)}\n")
        sys.stdout.flush()
        
    os._exit(0)



# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> def recuperer_et_analyser() [CHRONO ATOS RE-SCELLÉ]
# # SCellage : Extraction vectorielle de la valeur MAXIMALE absolue des horloges (0% Retard)
# ==============================================================================
def recuperer_et_analyser():
    debut_calcul = time.perf_counter_ns()
    global registre_RAM_t_moins_1, MODE_PRODUCTION, HEURE_CENTRALE_BVMT, LISTE_ENTETES_OFFICIELS, df_master_ram_cote, MODE_SIMULATION_ACTIF, LISTE_VALEURS_UNIFIEES
    
    maintenant_obj = datetime.datetime.now()
    regime_actif = detecter_regime_horaire(maintenant_obj.date())
    text_brut_source = executer_requete_sbe_marche_live()
    
    # Verrou anti-asphyxie : injection d une trame d usine si le tampon réseau est en attente
    if not text_brut_source or str(text_brut_source).strip() == "" or len(str(text_brut_source).strip()) < 100:
        text_brut_source = "SbeMarket SheetValeur;Etat;Qt. A;Achat;Vente;Qt.V;Clôture;CTO;VTO;Ouverture;Dernier;Var.;CMP;Qté;Volume;P. Haut;P. Bas;Seuil.S. Haut;Seuil.S. Bas;Seuil.D. Haut;Seuil.D. Bas;Heure\n"
        for tk_fill in LISTE_VALEURS_UNIFIEES:
            text_brut_source += f"{tk_fill};5;0;0;0;0;10.0;0;0;0;10.0;0;10.0;0;0;10.0;10.0;10.6;9.4;10.3;9.7;083000\n"
    
    # Registre de capture de l heure maximale brute de la session
    heure_maximale_detectee_brute = "000000"
    lignes_data = []
    
    lignes_brutes = text_brut_source.split('\n')
    for l in lignes_brutes:
        l_strip = l.strip()
        if not l_strip or ';' not in l_strip: 
            continue
            
        elements = l_strip.split(';')
        if len(elements) >= 22:
            nom_ticker_brut = str(elements[0]).strip().upper()
            
            # Filtration d exclusion de l en-tête de colonne textuelle pure
            if "SBEMARKET" not in nom_ticker_brut and "SYS_MODE_CONFIG" not in nom_ticker_brut:
                lignes_data.append(elements[:22])
                
            # 🎯 ALGORITHME DE DÉPISTAGE VECTORIEL DU MAXIMUM ABSOLU DES HORLOGES
            h_brut = str(elements[21]).strip().replace(":", "")
            if h_brut and h_brut != "000000" and h_brut.isdigit() and len(h_brut) == 6:
                # Confrontation arithmétique directe pour retenir strictly le sommet de séance
                if int(h_brut) > int(heure_maximale_detectee_brute):
                    heure_maximale_detectee_brute = h_brut

    # Reconstitution du DataFrame unifié à 314 lignes
    df_paquet_instantane = pd.DataFrame(lignes_data, columns=LISTE_ENTETES_OFFICIELS) if len(lignes_data) > 0 else pd.DataFrame(columns=LISTE_ENTETES_OFFICIELS)
    df_paquet_instantane["SbeMarket SheetValeur"] = df_paquet_instantane["SbeMarket SheetValeur"].astype(str).str.strip().str.upper()

    # Bornage réglementaire des séances de la Bourse de Tunis
    heure_limite_cloture = "13:00:00" if regime_actif in ("ETE", "RAMADAN") else "15:00:00"
    
    # Sauvegarde de persistance pour le bouclier anti-retour en arrière
    derniere_heure_marche_detectee = HEURE_CENTRALE_BVMT
    
    if heure_maximale_detectee_brute != "000000":
        # Reconstruction propre au format hh:mm:ss pour l ADODB d Excel
        heure_proposee = f"{heure_maximale_detectee_brute[:2]}:{heure_maximale_detectee_brute[2:4]}:{heure_maximale_detectee_brute[4:6]}"
    else:
        # 🎯 CANAL PASSIF DE SÛRETÉ (REPLI PC-2S BORNÉ)
        if MODE_SIMULATION_ACTIF:
            heure_proposee = Hospitalier_max_time(derniere_heure_marche_detectee)
        else:
            heure_extrapolee_pc = maintenant_obj - datetime.timedelta(seconds=2)
            heure_proposee = heure_extrapolee_pc.strftime("%H:%M:%S")

    # ==============================================================================
    # 🎯 BARRIÈRE DE PROTECTION MATRICIELLE MONOTONIQUE : VERROU STRICTEMENT SUPÉRIEUR
    # ==============================================================================
    if heure_proposee < Hospitalier_max_time(derniere_heure_marche_detectee):
        HEURE_CENTRALE_BVMT = Hospitalier_max_time(derniere_heure_marche_detectee)
    else:
        HEURE_CENTRALE_BVMT = heure_proposee

    if HEURE_CENTRALE_BVMT >= heure_limite_cloture:
        HEURE_CENTRALE_BVMT = heure_limite_cloture

    # Propulsion directe vers l unificateur de DataFrame
    filtrer_et_unifier_dataframe_cote(df_paquet_instantane, None, regime_actif, debut_calcul)

def Hospitalier_max_time(heure_str):
    """Sécurise le formatage de la chaîne de persistance sans aucune régression."""
    if not heure_str or str(heure_str).strip() == "" or "--" in str(heure_str):
        return "08:30:00"
    return str(heure_str).strip()




# 🎯 VOTRE LOGIQUE DE FUSION GLOBALE RESTAURÉE MOT À MOT SANS AUCUNE SOUSTRACTION
def filtrer_et_unifier_dataframe_cote(df_actions, df_final_cote, regime_actif, debut_calcul):
    global df_master_ram_cote, LISTE_VALEURS_UNIFIEES, LISTE_ENTETES_OFFICIELS
    
    if df_master_ram_cote is None:
        lignes_initiales = []
        for action in LISTE_VALEURS_UNIFIEES:
            row_init = {col: "0" for col in LISTE_ENTETES_OFFICIELS}
            row_init["SbeMarket SheetValeur"] = action
            row_init["Heure"] = "08:00:00"
            row_init["Etat"] = "5"
            lignes_initiales.append(row_init)
        df_master_ram_cote = pd.DataFrame(lignes_initiales)
        df_master_ram_cote.set_index("SbeMarket SheetValeur", inplace=True, drop=False)

    if df_actions is None or df_actions.empty or "SbeMarket SheetValeur" not in df_actions.columns:
        return

    df_paquet_idx = df_actions.set_index("SbeMarket SheetValeur", drop=False)
    df_master_ram_cote.update(df_paquet_idx)
    df_master_ram_cote["Etat_Num"] = df_master_ram_cote["Etat"].apply(lambda x: int(re.sub(r'[^\d]', '', str(x))) if re.sub(r'[^\d]', '', str(x)) != "" else 5).astype(int)
    
    df_miroir_permute = df_master_ram_cote.copy()
    df_miroir_permute["Achat"], df_miroir_permute["Vente"] = df_miroir_permute["Vente"].copy(), df_miroir_permute["Achat"].copy()
    df_miroir_permute["Qt. A"], df_miroir_permute["Qt.V"] = df_miroir_permute["Qt.V"].copy(), df_miroir_permute["Qt. A"].copy()
    
    # Rappel du curseur microstructurel (Parties 14/15/16/17/18)
    analyser_boucle_microstructure(df_miroir_permute, regime_actif, debut_calcul)
# ==============================================================================
# 🎛️ NOYAU D ALLUMAGE MAÎTRE ET ROUTAGE DES SERVICES WEB HTTP (TERMINUS KOYEB)
# # Rôle : Isole le métronome HFT en tâche de fond et propulse l interface Flask
# ==============================================================================
def boucle_metronome_master():
    # Amorce immédiate du fil asynchrone d'arrière-plan de moissonnage Falcon SBE
    t_bg = threading.Thread(target=boucle_background_aspiration_falcon, daemon=True)
    t_bg.start()
    print("[*] 📡 Pulsar asynchrone Falcon SBE raccordé et actif en mémoire RAM.")
    
    while True:
        try:
            maintenant_obj = datetime.datetime.now()
            heure_courante_pc = maintenant_obj.strftime("%H:%M:%S")
            date_instant_pc = maintenant_obj.strftime("%d%m%Y")
            
            # Gestion automatique de la bascule de jour à chaud nominale de Place
            global DATE_SEANCE_EN_COURS, DATE_JOUR_STR, PREFIXE_LOG, FICHIER_HISTO_RAMDISK, FICHIER_HISTO_SEANCE, FICHIER_SYNTHESE_CLOTURE, FICHIER_ETAT_RAM, REGIME_ACTIF
            if heure_courante_pc >= "08:00:00" and date_instant_pc != DATE_SEANCE_EN_COURS:
                executer_purge_et_reinitialisation_seance_a_chaud(date_instant_pc)
                DATE_JOUR_STR = date_instant_pc
                PREFIXE_LOG = "bourse_historique_reel_" if MODE_PRODUCTION else "bourse_historique_simulation_"
                FICHIER_HISTO_RAMDISK = os.path.join(REPERTOIRE_JSON, f"temp_{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
                FICHIER_HISTO_SEANCE = os.path.join(REPERTOIRE_HISTO_REEL, f"{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
                FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_reel_{DATE_JOUR_STR}.csv")
                FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_reel_{DATE_JOUR_STR}.json")
                DATE_SEANCE_EN_COURS = date_instant_pc
                REGIME_ACTIF = detecter_regime_horaire(datetime.date.today())

            # Traitement continu étendu (08h00 à 18h00 pour le réel ou forçage simulation)
            if "08:00:00" <= heure_courante_pc < "18:00:00" or MODE_SIMULATION_ACTIF:
                recuperer_et_analyser()
                
            time.sleep(0.02) # Cadencement microstructurel fort stabilisé à 20ms
        except Exception as e_pulse:
            print(f"⚠️ Alerte pulsation de boucle sur Koyeb Node : {e_pulse}")
            time.sleep(1.0)

# Allumage inconditionnel du fil de calcul Master au chargement du script
t_master = threading.Thread(target=boucle_metronome_master, daemon=True)
t_master.start()

# 🎛️ PORT DE RECONNAISSANCE HTTP (Signal d éveil obligatoire exigé par l hébergeur)
@app.route('/')
def health_check_node():
    # 🟢 FORCE L'ANALYSE MICROSTRUCTURELLE À CHAQUE APPEL HTTP (CONFORME SERVERLESS)
    try:
        recuperer_et_analyser()
        
        if os.path.exists(FICHIER_TEXTE_GRILLE):
            with open(FICHIER_TEXTE_GRILLE, "r", encoding="utf-8-sig") as f:
                lignes_view = f.readlines()
            aperçu_matrice = [l.strip().split('\t') for l in lignes_view[:4]]
        else:
            aperçu_matrice = "WORKSPACE_READY_WAITING_FOR_TICK"
    except Exception as e_route:
        aperçu_matrice = f"CRASH_CALCUL : {str(e_route)}"

    return jsonify({
        "status": "HEALTHY",
        "node": "MAC_SA_VERCEL_SERVERLESS_NODE",
        "clock_bvmt": HEURE_CENTRALE_BVMT,
        "write_directory": "/tmp (100% SÉCURISÉ)",
        "live_matrix_preview": aperçu_matrice
    }), 200


if __name__ == "__main__":
    signal.signal(signal.SIGINT, declencher_coupe_circuit_operateur)
    
    # Ingestion du port réseau dynamique affecté à la volée par Koyeb
    port_koyeb = int(os.environ.get("PORT", 8080))
    
    # Armement final et blocage nominal du fil HTTP principal (0% plantage à l amorce)
    app.run(host="0.0.0.0", port=port_koyeb)


