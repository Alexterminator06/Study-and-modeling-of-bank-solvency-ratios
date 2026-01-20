import numpy as np
from scipy.stats import norm

def compute_cva_long_call(S,K,T,r,sigma,credit_spread,LGD=0.45,n_steps=50):
    """
    Calcule la CVA (Credit Valuation Adjustment) pour une option d'achat (Long Call).
    Simulation de l'Expected Exposure (EE) * Probabilité de Défaut.
    
    Paramètres:
    S : Prix Spot du sous-jacent
    K : Strike
    T : Maturité (années)
    r : Taux sans risque
    sigma : Volatilité
    credit_spread : Spread de crédit de la contrepartie
    """
    dt=T/n_steps
    cva_accumulated=0.0
    
    # Probabilité de défaut marginale (approximée par le spread)
    # PD(t, t+dt) ~ 1 - exp(-spread * dt)
    marginal_pd=1-np.exp(-credit_spread*dt)

    for i in range(1,n_steps+1):
        t=i*dt
        
        # Calcul du prix Black-Scholes à l'instant t (Expected Exposure)
        d1=(np.log(S/K)+(r+0.5*sigma**2)*t)/(sigma*np.sqrt(t))
        d2=d1-sigma*np.sqrt(t)
        
        # Prix du Call (Exposition positive attendue)
        ee_t=S*norm.cdf(d1)-K*np.exp(-r*t)*norm.cdf(d2)
        
        # Si l'option est hors de la monnaie, l'exposition est nulle
        ee_t=max(ee_t, 0)
        
        # Contribution à la CVA = Perte * Expo * Proba Défaut
        # Discount factor (actualisation)
        df = np.exp(-r * t)
        
        cva_contribution = LGD * ee_t * df * marginal_pd
        cva_accumulated += cva_contribution

    return cva_accumulated

def apply_cva_to_dataset(df):
    """
    Applique une charge CVA forfaitaire aux banques ayant une activité de marché.
    """
    print("Calcul du module CVA (Risque de Contrepartie)...")
    
    # On définit des paramètres de marché standard pour l'exercice
    # (Dans un vrai cas, cela dépendrait du portefeuille de chaque banque)
    S_ref = 100
    K_ref = 100
    T_ref = 1.0
    r_ref = 0.03
    sigma_ref = 0.20
    spread_ref = 0.02 # 2% spread (B+ rating)

    # Calcul d'une "CVA Unitaire"
    cva_unit_charge = compute_cva_long_call(S_ref, K_ref, T_ref, r_ref, sigma_ref, spread_ref)
    
    # On applique cette charge proportionnellement à l'exposition levier (proxy de la taille)
    # Facteur arbitraire pour l'exercice : 0.1% de l'exposition levier est considérée comme sujette à CVA
    if 'Leverage_Exposure' in df.columns:
        df['CVA_Charge'] = df['Leverage_Exposure'] * 0.001 * cva_unit_charge
        
        # Ajout au RWA Total (CVA est une charge en capital, on la convertit en RWA équivalent)
        # Charge Capital = RWA * 8%  =>  RWA_add = Charge / 8%
        df['RWA_CVA_Addon'] = df['CVA_Charge'] / 0.08
        
        # Mise à jour du RWA Final
        if 'RWA_Final' in df.columns:
            df['RWA_Final'] += df['RWA_CVA_Addon'].fillna(0)
            
    return df
