import pandas as pd

def find_npl_code():
    print("Recherche du code NPL...")
    try:
        # CORRECTION : header=1 pour sauter la ligne de texte inutile
        df = pd.read_csv("data/raw/SDD.csv", sep=";", encoding='cp1252', header=1)
        
        # Petit debug pour être sûr
        # print("Colonnes lues :", df.columns) 

        # Recherche stricte : "Non-performing" + "Loans" (ou Debt) + "Gross carrying amount"
        mask = (
            df['Label'].str.contains("Non-performing", case=False, na=False) &
            df['Label'].str.contains("Gross carrying amount", case=False, na=False) &
            df['CSV'].str.contains("tr_cre", case=False, na=False)
        )
        
        results = df[mask][['Item', 'Label']].drop_duplicates()
        
        print("\n--- CANDIDATS NPL ---")
        for idx, row in results.iterrows():
            print(f"Code : {row['Item']}  ->  {row['Label']}")

    except Exception as e:
        print(f"Erreur : {e}")

if __name__ == "__main__":
    find_npl_code()