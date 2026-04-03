import pandas as pd
import numpy as np
import os

def load_data(dict_of_files):

    print("Chargement des données brutes en mémoire...")
    df_oth, df_cre, df_mrk, df_sdd = None, None, None, None
    
    try:
        if "SDD" in dict_of_files and os.path.exists(dict_of_files["SDD"]):
            df_sdd = pd.read_excel(dict_of_files["SDD"], engine='openpyxl')
            print(f"✅ SDD chargé: {df_sdd.shape}")

        if "oth" in dict_of_files and os.path.exists(dict_of_files["oth"]):
            df_oth = pd.read_csv(dict_of_files["oth"], dtype={'LEI_Code': str}, sep=None, engine='python')
            print(f"✅ OTH chargé: {df_oth.shape}")
            
        if "cre" in dict_of_files and os.path.exists(dict_of_files["cre"]):
            df_cre = pd.read_csv(dict_of_files["cre"], dtype={'LEI_Code': str}, sep=None, engine='python')
            print(f"✅ CRE chargé: {df_cre.shape}")
            
        if "mrk" in dict_of_files and os.path.exists(dict_of_files["mrk"]):
            df_mrk = pd.read_csv(dict_of_files["mrk"], dtype={'LEI_Code': str}, sep=None, engine='python')
            print(f"✅ MRK chargé: {df_mrk.shape}")
            
        return df_oth, df_cre, df_mrk, df_sdd
        
    except Exception as e:
        print(f"❌ ERREUR lors du chargement des fichiers : {e}")
        return None, None, None, None

def clean_and_merge(df_oth, df_cre, df_mrk):
    print("🧹 Mapping Intelligent et Nettoyage...")
    
    # 1. Standardisation rigoureuse des colonnes vitales
    dataframes = [df for df in [df_oth, df_cre, df_mrk] if df is not None]
    
    for df in dataframes:
        rename_map = {}
        for col in df.columns:
            c_low = str(col).lower().strip()
            if c_low == 'lei_code' or c_low == 'lei': rename_map[col] = 'LEI'
            elif c_low == 'period' or c_low == 'date': rename_map[col] = 'Date'
            elif c_low == 'item': rename_map[col] = 'Item'
            elif c_low == 'amount': rename_map[col] = 'Amount'
            
        df.rename(columns=rename_map, inplace=True)
        
        if 'Date' in df.columns:
            df['Date'] = df['Date'].astype(str).str.strip()

    suffix_mapping = {
        '0102': 'CET1_Capital',      
        '0133': 'Tier1_Capital',
        '0101': 'Total_Capital',
        '0138': 'RWA_Total',
        '1010': 'Total_Assets',
        '0333': 'Net_Income',
        '0603': 'Loans_Gross',       
        '0613': 'Provisions_Stock',
        '0710': 'NPL_Amount'
    }

    def pivot_eba_data(df, source_name):
        if df is None or df.empty or 'Item' not in df.columns or 'Amount' not in df.columns: 
            return pd.DataFrame(columns=['LEI', 'Date'])
            
        print(f"Transformation de {source_name}...")
        
        # On convertit les items en texte pour chercher la fin du code
        df['Item_Str'] = df['Item'].astype(str)
        
        # Fonction de mapping dynamique
        def map_code(item_str):
            for suffix, name in suffix_mapping.items():
                if item_str.endswith(suffix):
                    return name
            return None
            
        # Application du mapping
        df['Mapped_Item'] = df['Item_Str'].apply(map_code)
        
        # On filtre pour ne garder que ce qu'on a réussi à mapper
        df_filtered = df.dropna(subset=['Mapped_Item']).copy()
        
        if df_filtered.empty:
            print(f"⚠️ AVERTISSEMENT : Aucun item utile n'a été mappé dans {source_name}.")
            return pd.DataFrame(columns=['LEI', 'Date'])

        # On nettoie la colonne Amount (conversion en nombre)
        df_filtered['Amount'] = pd.to_numeric(df_filtered['Amount'], errors='coerce').fillna(0)

        df_pivot = pd.pivot_table(
            df_filtered, 
            index=['LEI', 'Date'], 
            columns='Mapped_Item', 
            values='Amount', 
            aggfunc='max'
        ).reset_index()
        
        print(f"✅ {source_name} transformé avec succès ! ({len(df_pivot)} lignes obtenues)")
        return df_pivot

    # 3. Exécution
    df_oth_pivot = pivot_eba_data(df_oth, "OTH")
    df_cre_pivot = pivot_eba_data(df_cre, "CRE")

    # 4. Fusion
    print("Fusion des datasets...")
    if not df_oth_pivot.empty and not df_cre_pivot.empty:
        master_df = pd.merge(df_oth_pivot, df_cre_pivot, on=['LEI', 'Date'], how='outer')
    elif not df_oth_pivot.empty:
        master_df = df_oth_pivot
    elif not df_cre_pivot.empty:
        master_df = df_cre_pivot
    else:
        print("❌ Les datasets fusionnés sont vides.")
        return None

    # 5. On garde le trimestre le plus récent
    dates_dispo = sorted(master_df['Date'].dropna().unique())
    if dates_dispo:
        last_date = dates_dispo[-1]
        print(f"Analyse conservée pour la période la plus récente : {last_date}")
        master_df = master_df[master_df['Date'] == last_date]
    
    master_df = master_df.fillna(0)
    print(f"✅ Dataset Final prêt : {master_df.shape}")
    return master_df

def run_etl(dict_of_files):
    print("\n" + "="*40)
    print("DÉBUT DU PIPELINE ETL")
    print("="*40)
    
    df_oth, df_cre, df_mrk, df_sdd = load_data(dict_of_files)
    
    if df_oth is not None or df_cre is not None:
        final_df = clean_and_merge(df_oth, df_cre, df_mrk)
        
        if final_df is not None and not final_df.empty:
            final_df.replace([np.inf, -np.inf], 0, inplace=True)
            print("\n✅ PIPELINE ETL TERMINÉ AVEC SUCCÈS")
            return final_df
            
    print("\n❌ L'ETL a échoué.")
    return None
