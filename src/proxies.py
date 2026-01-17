import pandas as pd
import numpy as np

def calculate_proxies(df):
    """
    Applique les règles de gestion (Proxies) de manière sécurisée.
    Source: Rapport Technique Chapitre 3
    """
    print("Application des Proxies...")
    
    if df.empty:
        return df

    df = df.copy()

    # --- SÉCURISATION DES COLONNES MANQUANTES ---
    # On vérifie si les colonnes critiques existent, sinon on les crée à 0
    required_cols = ['Total_Assets', 'Exposure_Value', 'Loans_Gross', 'NPL_Amount', 'HQLA_Amount']
    
    for col in required_cols:
        if col not in df.columns:
            print(f"⚠️ ALERTE : Colonne '{col}' manquante. Création d'une colonne vide (0) pour éviter le crash.")
            df[col] = 0.0

    # --- 3.1 PROXY CREDIT RISK ---
    
    # 1. EAD (Exposure at Default)
    # [cite_start]Si Exposure_Value est vide (NaN), on utilise Total_Assets comme proxy [cite: 312]
    # On remplace les 0 par NaN pour que le fillna fonctionne bien
    df['EAD_Final'] = df['Exposure_Value'].replace(0, np.nan).fillna(df['Total_Assets'])

    # 2. PD (Probability of Default)
    # [cite_start]Formule : (NPL / Loans) * 1.2 [cite: 317]
    # On évite la division par zéro
    df['NPL_Ratio'] = np.where(df['Loans_Gross'] > 0, df['NPL_Amount'] / df['Loans_Gross'], 0)
    df['PD_Proxy'] = (df['NPL_Ratio'] * 1.2).clip(upper=1.0)
    
    # 3. LGD (Loss Given Default)
    # [cite_start]Valeur standard 45% [cite: 321]
    df['LGD_Proxy'] = 0.45

    # --- 3.2 PROXY LIQUIDITY ---
    # Si HQLA manque, on laisse à 0 (proxy simplifié déjà géré par l'initialisation ci-dessus)
    
    print("Proxies calculés avec succès.")
    return df