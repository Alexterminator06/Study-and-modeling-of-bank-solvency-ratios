import os
import requests

# --- DICTIONNAIRE DES SOURCES EBA (LIENS DIRECTS CSV) ---
# Remplacez ces URL fictives par les VRAIS liens directs vers les fichiers CSV de l'EBA.
# Faites un clic droit > "Copier le lien" sur chaque bouton de téléchargement du site.
EBA_SOURCES = {
    "2025": {
        "oth": "https://www.eba.europa.eu/assets/TE2025/Full_database/883401/tr_oth.csv",
        "cre": "https://www.eba.europa.eu/assets/TE2025/Full_database/883401/tr_cre.csv",
        "mrk": "https://www.eba.europa.eu/assets/TE2025/Full_database/883401/tr_mrk.csv",
        "SDD": "https://www.eba.europa.eu/assets/TE2025/Full_database/883401/SDD.xlsx"
    },
    "2024": {
        "oth": "https://www.eba.europa.eu/assets/TE2024/Full_database/256109/tr_oth.csv",
        "cre": "https://www.eba.europa.eu/assets/TE2024/Full_database/256109/tr_cre.csv",
        "mrk": "https://www.eba.europa.eu/assets/TE2024/Full_database/256109/tr_mrk.csv",
        "SDD": "https://www.eba.europa.eu/assets/TE2024/Full_database/256109/SDD.xlsx"
    },
    "2023": {
        "oth": "https://www.eba.europa.eu/assets/TE2023/Full_database/837203/tr_oth.csv",
        "cre": "https://www.eba.europa.eu/assets/TE2023/Full_database/837203/tr_cre.csv",
        "mrk": "https://www.eba.europa.eu/assets/TE2023/Full_database/837203/tr_mrk.csv",
        "SDD": "https://www.eba.europa.eu/assets/TE2023/Full_database/837203/SDD.xlsx"
    },
    "2022": {
        "oth": "https://www.eba.europa.eu/assets/TE2022/Full_database/tr_oth.csv",
        "cre": "https://www.eba.europa.eu/assets/TE2022/Full_database/tr_cre.csv",
        "mrk": "https://www.eba.europa.eu/assets/TE2022/Full_database/tr_mrk.csv",
        "SDD": "https://www.eba.europa.eu/assets/TE2022/Full_database/SDD.xlsx"
}}

def download_eba_data(period, output_dir="data/raw"):
    """
    Télécharge individuellement les fichiers de l'EBA pour une période donnée.
    Gère les .csv pour les données et les .xlsx pour le dictionnaire SDD.
    """
    print(f"🔄 Lancement du téléchargement pour la période : {period}...")
    
    if period not in EBA_SOURCES:
        print(f"❌ Erreur : L'année {period} n'est pas configurée dans le scraper.")
        return None
        
    os.makedirs(output_dir, exist_ok=True)
    downloaded_files_paths = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    for file_key, url in EBA_SOURCES[period].items():
        print(f"🌐 Téléchargement de [{file_key.upper()}] depuis : {url}")
        
        # --- GESTION DES EXTENSIONS ---
        extension = ".xlsx" if file_key == "SDD" else ".csv"
        local_filename = f"tr_{file_key}_{period}{extension}" if file_key != "SDD" else f"SDD_{period}{extension}"
        local_path = os.path.join(output_dir, local_filename)
        
        try:
            response = requests.get(url, headers=headers, stream=True, timeout=60)
            response.raise_for_status() 
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            print(f"✅ Fichier sauvegardé : {local_path}")
            downloaded_files_paths[file_key] = local_path
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Échec du téléchargement pour {file_key} : {e}")

    if downloaded_files_paths:
        print("🎉 Tous les téléchargements sont terminés !")
        return downloaded_files_paths
    else:
        print("❌ Échec total : Aucun fichier n'a pu être téléchargé.")
        return None