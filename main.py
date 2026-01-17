import pandas as pd
import os
from src import etl, proxies, engine, cva_modules

# --- CONFIGURATION (C'est ici que ça manquait) ---
# On définit les chemins relatifs par rapport à la racine du projet
PROCESSED_PATH = "data/processed"
OUTPUT_FILE = "data/processed/final_results_2025.csv"

def main():
    print("=== Démarrage du Solvency Engine (ESILV 2026) ===")
    
    # 1. ETL : Charger ou créer le dataset
    # On vérifie si le dossier existe, sinon on le crée
    if not os.path.exists(PROCESSED_PATH):
        os.makedirs(PROCESSED_PATH)

    if os.path.exists(os.path.join(PROCESSED_PATH, "master_dataset.csv")):
        print("Chargement du Master Dataset existant...")
        df = pd.read_csv(os.path.join(PROCESSED_PATH, "master_dataset.csv"))
    else:
        print("Master Dataset introuvable. Lancement de l'ETL...")
        oth, cre, mrk = etl.load_data()
        
        # Sécurité : Si le chargement échoue
        if oth is None:
            print("❌ Erreur : Impossible de charger les données brutes.")
            return

        df = etl.clean_and_merge(oth, cre, mrk)
        
        if df.empty:
            print("❌ Erreur : Dataset vide après fusion. Vérifiez les dates.")
            return
            
        df.to_csv(os.path.join(PROCESSED_PATH, "master_dataset.csv"), index=False)

    # 2. PROXIES : Combler les trous
    # (EAD, PD, LGD manquants)
    df_proxied = proxies.calculate_proxies(df)

    # 3. ENGINE : Calculer les RWA de base
    # (Waterfall : RWA Réel vs Synthétique)
    df_calc = engine.compute_rwa(df_proxied)
    
    # 4. MODULE CVA (Avancé)
    # Ajoute la charge pour risque de contrepartie
    df_cva = cva_modules.apply_cva_to_dataset(df_calc)
    
    # 5. RATIOS FINAUX
    # (CET1, TCR, Leverage, Texas Ratio)
    df_final = engine.compute_solvency_ratios(df_cva)

    # 6. REPORTING : Sauvegarde
    df_final.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "="*50)
    print(f"✅ SUCCÈS : Calculs terminés.")
    print(f"📁 Résultats disponibles ici : {OUTPUT_FILE}")
    print("="*50)
    
    # Aperçu console pour vérifier les chiffres clés
    print("\n--- Aperçu des Résultats (5 premières banques) ---")
    cols_to_show = ['LEI', 'CET1_Ratio_Pct', 'RWA_Final', 'NPL_Amount', 'CVA_Charge']
    # On affiche uniquement les colonnes qui existent vraiment
    available_cols = [c for c in cols_to_show if c in df_final.columns]
    print(df_final[available_cols].head().to_string())

if __name__ == "__main__":
    main()