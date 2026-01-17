import pandas as pd
import numpy as np

def compute_rwa(df):
    """
    Calcul des Actifs Pondérés (RWA) - Version Sécurisée
    """
    print("Calcul des RWA (Waterfall)...")
    
    # SÉCURITÉ : Vérifier si les colonnes nécessaires existent
    # Si RWA_Total n'existe pas, on crée une colonne de NaN pour déclencher le modèle synthétique
    if 'RWA_Total' not in df.columns:
        print("⚠️ ALERTE : 'RWA_Total' manquant. Utilisation 100% Modèle Synthétique.")
        df['RWA_Total'] = np.nan

    # Vérification des ingrédients pour le modèle synthétique
    required_synthetic = ['EAD_Final', 'PD_Proxy', 'LGD_Proxy']
    for col in required_synthetic:
        if col not in df.columns:
            # Si un ingrédient manque (ex: à cause d'un échec proxy), on met 0
            df[col] = 0.0

    # 1. Calcul du RWA Synthétique (Modèle Interne Simplifié)
    # Formule Rapport Eq 4.1 : RWA_model = EAD * PD * LGD * 1.06
    df['RWA_Synthetic'] = (
        df['EAD_Final'] * df['PD_Proxy'] * df['LGD_Proxy'] * 1.06
    )

    # 2. Application de la Waterfall
    # On remplit les trous du RWA Réel par le RWA Synthétique
    df['RWA_Final'] = df['RWA_Total'].fillna(df['RWA_Synthetic'])
    
    return df

def compute_solvency_ratios(df):
    """
    Calcul des Ratios de Solvabilité Bâle III - Version Sécurisée
    """
    print("Calcul des Ratios de Solvabilité...")
    
    # Liste des colonnes de Capital nécessaires
    capital_cols = ['CET1_Capital', 'Total_Capital', 'Tier1_Capital', 'Leverage_Exposure']
    
    # Création des colonnes manquantes à NaN (pour ne pas afficher 0% faussement, mais plutôt "Donnée absente")
    for col in capital_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Sécurité division par zéro
    # Si RWA_Final est 0 ou NaN, on ne peut pas calculer de ratio
    df['RWA_Final'] = df['RWA_Final'].replace(0, np.nan)

    # 1. CET1 Ratio
    df['CET1_Ratio_Pct'] = (df['CET1_Capital'] / df['RWA_Final']) * 100

    # 2. Total Capital Ratio (TCR)
    df['TCR_Pct'] = (df['Total_Capital'] / df['RWA_Final']) * 100

    # 3. Leverage Ratio
    # Si leverage exposure manque, utiliser Total Assets (si dispo), sinon NaN
    if 'Total_Assets' in df.columns:
        exposure = df['Leverage_Exposure'].fillna(df['Total_Assets'])
    else:
        exposure = df['Leverage_Exposure']
        
    df['Leverage_Ratio_Pct'] = (df['Tier1_Capital'] / exposure) * 100

    return df

def run_engine(df):
    """
    Orchestration
    """
    if df.empty:
        return df
        
    df = compute_rwa(df)
    df = compute_solvency_ratios(df)
    
    # Texas Ratio (Indicateur d'alerte)
    # Besoin de NPL, Tier1 et Provisions
    # On sécurise l'accès
    npl = df['NPL_Amount'] if 'NPL_Amount' in df.columns else 0
    tier1 = df['Tier1_Capital'] if 'Tier1_Capital' in df.columns else 0
    prov = df['Provisions_Stock'] if 'Provisions_Stock' in df.columns else 0
    
    denominator = tier1 + prov
    # Éviter division par zéro
    df['Texas_Ratio_Pct'] = np.where(denominator > 0, (npl / denominator) * 100, np.nan)
    
    return df