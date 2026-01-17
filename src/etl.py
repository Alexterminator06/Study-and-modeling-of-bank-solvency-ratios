import pandas as pd
import numpy as np
import os

# --- CONFIGURATION ROBUSTE ---
# On récupère le chemin exact où se trouve ce fichier (etl.py), c'est-à-dire le dossier /src
script_dir = os.path.dirname(os.path.abspath(__file__))

# On remonte d'un cran pour trouver la racine du projet (/Study-and-modeling-of-bank-solvency-ratios)
project_root = os.path.dirname(script_dir)

# On construit les chemins absolus vers les données
RAW_PATH = os.path.join(project_root, "data", "raw")
PROCESSED_PATH = os.path.join(project_root, "data", "processed")

# Vérification pour vous aider à débugger
print(f"Dossier du projet détecté : {project_root}")
print(f"Recherche des données dans : {RAW_PATH}")

def load_data():
    """
    Charge les fichiers CSV bruts.
    Le paramètre low_memory=False évite les warnings sur les types mixtes.
    """
    print("Chargement des données brutes...")
    try:
        # On force le LEI en string pour éviter la notation scientifique
        # Note : Ajustez le séparateur (sep=',' ou ';') selon votre fichier réel
        # Dans la fonction load_data()
        df_oth = pd.read_csv(os.path.join(RAW_PATH, "tr_oth.csv"), dtype={'LEI_Code': str}, sep=None, engine='python')
        df_cre = pd.read_csv(os.path.join(RAW_PATH, "tr_cre.csv"), dtype={'LEI_Code': str}, sep=None, engine='python')
        df_mrk = pd.read_csv(os.path.join(RAW_PATH, "tr_mrk.csv"), dtype={'LEI_Code': str}, sep=None, engine='python')
        
        print(f"Chargé: OTH {df_oth.shape}, CRE {df_cre.shape}, MRK {df_mrk.shape}")
        return df_oth, df_cre, df_mrk
    except FileNotFoundError as e:
        print(f"ERREUR CRITIQUE : Fichier introuvable. {e}")
        return None, None, None

def clean_and_merge(df_oth, df_cre, df_mrk, target_date="20250630"):
    print("Mapping et Nettoyage (Mode Pivot)...")
    
    # 1. Standardisation des Noms de Colonnes de base
    # On s'assure d'avoir 'LEI', 'Date', 'Item', 'Amount' partout
    for df in [df_oth, df_cre, df_mrk]:
        df.columns = [c.strip() for c in df.columns] # Nettoyage espaces
        
        # Renommage des colonnes techniques
        rename_map = {}
        for col in df.columns:
            if 'LEI' in col and 'Code' in col: rename_map[col] = 'LEI'
            elif 'Period' in col or 'Date' in col: rename_map[col] = 'Date'
            elif 'Item' in col: rename_map[col] = 'Item'
            elif 'Amount' in col or 'Carrying amount' in col: rename_map[col] = 'Amount'
            
        if rename_map:
            df.rename(columns=rename_map, inplace=True)
            
        # Standardisation Date
        if 'Date' in df.columns:
            df['Date'] = df['Date'].astype(str).str.strip()

    # 2. DEFINITION DU MAPPING (Codes EBA -> Vos variables)
    mapping_codes = {
        2520102: 'CET1_Capital',      # TR OTH
        2520133: 'Tier1_Capital',
        2520101: 'Total_Capital',
        2520138: 'RWA_Total',
        2521010: 'Total_Assets',
        2520333: 'Net_Income',
        2520903: 'Leverage_Exposure',
        2520603: 'Loans_Gross',       # TR CRE
        2520613: 'Provisions_Stock',
        2520710: 'NPL_Amount'
    }
    
    # Code NPL (parfois complexe, on prend un proxy simple si dispo)
    # Souvent le code 2520601 est utilisé pour les dettes, ou on filtre sur un statut.
    # Pour simplifier, on tente le code global s'il existe.
    
    # 3. FONCTION DE PIVOT GÉNÉRIQUE
    def pivot_eba_data(df, source_name):
        print(f"Pivot de {source_name}...")
        
        # Vérification des colonnes vitales
        if 'Item' not in df.columns or 'Amount' not in df.columns:
            print(f"⚠️ Attention : {source_name} ne semble pas être au format Long (Item/Amount manquants).")
            return df
            
        # On ne garde que les items qui nous intéressent pour alléger
        relevant_items = list(mapping_codes.keys())
        # On s'assure que Item est numérique pour le filtrage
        df['Item'] = pd.to_numeric(df['Item'], errors='coerce')
        df_filtered = df[df['Item'].isin(relevant_items)]
        
        if df_filtered.empty:
            print(f"⚠️ Aucun code pertinent trouvé dans {source_name}.")
            return pd.DataFrame(columns=['LEI', 'Date'])

        # PIVOT TABLE : La magie opère ici
        # On somme (sum) pour agréger les doublons (ex: plusieurs portefeuilles)
        df_pivot = pd.pivot_table(
            df_filtered,
            index=['LEI', 'Date'],
            columns='Item',
            values='Amount',
            aggfunc='sum'
        ).reset_index()
        
        # Renommage des colonnes (Codes -> Noms lisibles)
        df_pivot.rename(columns=mapping_codes, inplace=True)
        return df_pivot

    # 4. APPLICATION DU PIVOT
    df_oth_pivot = pivot_eba_data(df_oth, "tr_oth")
    df_cre_pivot = pivot_eba_data(df_cre, "tr_cre")
    
    # Pour le Market Risk, on prend juste le RWA s'il y est, sinon on ignore
    # Souvent le RWA total est déjà dans tr_oth (code 2520138), donc mrk est optionnel ici.

    # 5. FUSION DES DONNÉES PIVOTÉES
    print("Fusion des datasets pivotés...")
    master_df = pd.merge(df_oth_pivot, df_cre_pivot, on=['LEI', 'Date'], how='left')

    # 6. FILTRAGE DATE INTELLIGENT
    dates_dispo = sorted(master_df['Date'].unique())
    print(f"Dates disponibles : {dates_dispo}")

    target_str = str(target_date)
    if target_str in dates_dispo:
        master_df = master_df[master_df['Date'] == target_str]
    elif dates_dispo:
        last_date = dates_dispo[-1]
        print(f"⚠️ Date cible absente. Utilisation de la plus récente : {last_date}")
        master_df = master_df[master_df['Date'] == last_date]

    print(f"Dataset final généré : {master_df.shape}")
    return master_df

def quality_checks(df):
    """
    Vérifications de cohérence financière.
    """
    print("Exécution des Quality Checks...")
    
    # Check 1 : Actifs positifs
    if 'Total_Assets' in df.columns:
        neg_assets = df[df['Total_Assets'] < 0]
        if not neg_assets.empty:
            print(f"ATTENTION : {len(neg_assets)} banques avec Actifs Négatifs !")
        else:
            print("Check Actifs : OK")

    # Check 2 : RWA présents
    if 'RWA_Total' in df.columns:
        missing_rwa = df['RWA_Total'].isna().sum()
        print(f"Info : {missing_rwa} banques sans RWA (seront gérées par proxies).")

if __name__ == "__main__":
    # Pipeline d'exécution
    oth, cre, mrk = load_data()
    
    if oth is not None:
        final_df = clean_and_merge(oth, cre, mrk, target_date="20250630")
        quality_checks(final_df)
        
        # Sauvegarde
        output_file = os.path.join(PROCESSED_PATH, "master_dataset.csv")
        final_df.to_csv(output_file, index=False)
        print(f"✅ SUCCÈS : Fichier généré dans {output_file}")