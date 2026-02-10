import pandas as pd
import numpy as np

def compute_rwa(df):
    """
    Calcule les RWA (Risk Weighted Assets) de base.
    """
    print("Calcul des RWA (Standard & Synthétique)...")
    
    # 1. RWA Synthétique (Méthode Standard Bâle - Simplifiée)
    # Pondération moyenne forfaitaire de 50% sur les actifs si info manquante
    if 'Total_Assets' in df.columns:
        df['RWA_Synthetic'] = df['Total_Assets'] * 0.50
    else:
        df['RWA_Synthetic'] = 0

    # 2. RWA Final (Waterfall)
    # Priorité : RWA déclaré > RWA Synthétique
    if 'RWA_Total' in df.columns:
        # On remplace les 0 ou NaN par la valeur synthétique
        df['RWA_Final'] = df['RWA_Total'].fillna(0)
        mask_zero = df['RWA_Final'] == 0
        df.loc[mask_zero, 'RWA_Final'] = df.loc[mask_zero, 'RWA_Synthetic']
    else:
        df['RWA_Final'] = df['RWA_Synthetic']
        
    return df

def compute_solvency_ratios(df):
    """
    Calcule TOUS les indicateurs réglementaires (Solvabilité, Liquidité, Résolution).
    """
    print("Calcul des Ratios de Solvabilité, Liquidité et Rentabilité...")
    
    # --- 1. CAPITAUX PROPRES (Capital Stack) ---
    # Tier 1 & CET1 sont déjà là. Calcul des compléments :
    # AT1 (Additional Tier 1) = Tier 1 - CET1
    df['AT1_Capital'] = df['Tier1_Capital'] - df['CET1_Capital']
    # Tier 2 = Total Capital - Tier 1
    df['Tier2_Capital'] = df['Total_Capital'] - df['Tier1_Capital']
    
    # --- 2. RATIOS DE SOLVABILITÉ (Bâle III) ---
    # CET1 Ratio
    df['CET1_Ratio_Pct'] = (df['CET1_Capital'] / df['RWA_Final']) * 100
    
    # Tier 1 Ratio
    df['Tier1_Ratio_Pct'] = (df['Tier1_Capital'] / df['RWA_Final']) * 100
    
    # Total Capital Ratio (CAR - Capital Adequacy Ratio)
    df['TCR_Pct'] = (df['Total_Capital'] / df['RWA_Final']) * 100
    df['CAR_Pct'] = df['TCR_Pct'] # Synonyme
    
    # Leverage Ratio (Bâle 3)
    if 'Leverage_Exposure' in df.columns:
        df['Leverage_Ratio_Pct'] = (df['Tier1_Capital'] / df['Leverage_Exposure']) * 100
    else:
        # Fallback si l'exposition levier manque : Total Assets
        df['Leverage_Ratio_Pct'] = (df['Tier1_Capital'] / df['Total_Assets']) * 100

    # --- 3. EXIGENCES SREP & COUSSINS (Buffer) ---
    # Simulation des exigences (Valeurs standards BCE)
    df['P2R_Pct'] = 2.0  # Pillar 2 Requirement (Exigence propre à la banque)
    df['P2G_Pct'] = 1.0  # Pillar 2 Guidance (Recommandation)
    df['CBR_Pct'] = 2.5  # Combined Buffer Requirement (Conservation)
    
    # SREP Total (Seuil d'alerte) = P1 (8%) + P2R + CBR
    df['SREP_Requirement_Pct'] = 8.0 + df['P2R_Pct'] + df['CBR_Pct']
    
    # Coussin de Capital (Management Buffer / Surplus au-dessus du SREP)
    df['Capital_Buffer_Pct'] = df['CAR_Pct'] - df['SREP_Requirement_Pct']

    # --- 4. LIQUIDITÉ (LCR, NSFR, LTD) ---
    # Proxies car le passif détaillé (Dépôts) est souvent absent des données publiques EBA
    
    # Estimation Dépôts Clientèle (~60% du Bilan pour une banque commerciale standard)
    df['Deposits_Proxy'] = df['Total_Assets'] * 0.60
    
    # LTD (Loan-to-Deposit Ratio)
    # Capacité à financer les prêts par les dépôts
    df['LTD_Ratio_Pct'] = (df['Loans_Gross'] / df['Deposits_Proxy']) * 100
    
    # LCR (Liquidity Coverage Ratio)
    # HQLA Proxy (~15% Actif) / Sorties Nettes (~10% Dépôts)
    hqla_proxy = df['Total_Assets'] * 0.15
    net_outflows_proxy = df['Deposits_Proxy'] * 0.10
    df['LCR_Ratio_Pct'] = (hqla_proxy / net_outflows_proxy) * 100
    
    # NSFR (Net Stable Funding Ratio)
    # ASF (Ressources Stables) / RSF (Emplois Stables)
    asf_proxy = df['Total_Capital'] + (df['Deposits_Proxy'] * 0.90) # Capital + 90% Dépôts
    rsf_proxy = (df['Loans_Gross'] * 0.85) + (df['Total_Assets'] * 0.50) # Prêts + Actifs
    df['NSFR_Ratio_Pct'] = (asf_proxy / rsf_proxy) * 100

    # --- 5. QUALITÉ D'ACTIFS & RISQUE ---
    # Texas Ratio (Capacité à absorber les défauts)
    # Formule : NPL / (CET1 + Provisions)
    # On gère le cas où NPL n'existe pas ou est nul
    npl_val = df['NPL_Amount'] if 'NPL_Amount' in df.columns else 0
    df['Texas_Ratio_Pct'] = (npl_val / (df['CET1_Capital'] + df['Provisions_Stock'])) * 100
    df['Texas_Ratio_Pct'] = df['Texas_Ratio_Pct'].fillna(0) # Sécurité division par zéro
    
    # NPL Ratio
    df['NPL_Ratio_Pct'] = (npl_val / df['Loans_Gross']) * 100
    
    # Exposition au Risque (Risk Exposure Amount)
    # C'est simplement le RWA Final renommé
    df['Risk_Exposure_Amount'] = df['RWA_Final']

    # --- 6. RÉSOLUTION (MREL / TLAC) ---
    # MREL (Minimum Requirement for Own Funds and Eligible Liabilities)
    # Proxy Dettes Éligibles (Senior Non-Preferred...) ~ 15% Bilan
    eligible_liabilities = df['Total_Assets'] * 0.15
    mrel_amount = df['Total_Capital'] + eligible_liabilities
    
    # Ratio MREL (% des RWA)
    df['MREL_Ratio_Pct'] = (mrel_amount / df['RWA_Final']) * 100
    
    # TLAC (Total Loss-Absorbing Capacity) - Similaire au MREL pour les G-SIBs
    df['TLAC_Ratio_Pct'] = df['MREL_Ratio_Pct']

    # --- 7. RENTABILITÉ ---
    # ROA (Return on Assets)
    df['ROA_Pct'] = (df['Net_Income'] / df['Total_Assets']) * 100
    
    # ROE (Return on Equity) - Utilisation Tier 1 comme proxy capitaux propres
    df['ROE_Pct'] = (df['Net_Income'] / df['Tier1_Capital']) * 100

    # --- 8. NETTOYAGE FINAL ---
    # Suppression des colonnes intermédiaires ou demandées à être supprimées
    cols_to_drop = [
        'HQLA_Amount', 'EAD_Final', 'PD_Proxy', 'LGD_Proxy', 
        'Exposure_Value', 'Deposits_Proxy', 'RWA_Synthetic'
    ]
    # On ne supprime que ce qui existe pour éviter les erreurs
    actual_drop = [c for c in cols_to_drop if c in df.columns]
    df.drop(columns=actual_drop, inplace=True)
    
    return df