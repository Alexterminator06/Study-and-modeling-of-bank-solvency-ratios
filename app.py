from time import time

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="Solvency Control Room",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Style : Réduire les marges pour que la jauge colle au chiffre
st.markdown("""
<style>
    .block-container { padding-top: 1rem; padding-bottom: 3rem; }
    /* Fond gris clair pour les cartes de métriques */
    div[data-testid="metric-container"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 10px;
        border-radius: 5px;
        color: black;
    }
</style>
""", unsafe_allow_html=True)

st.title("🏦 Solvency Control Room")

with st.expander("Mise à jour des données EBA (Scraping)", expanded=True): # mis sur "True" pour qu'il soit ouvert si c'est vide
    st.info("Aucune donnée ? Sélectionnez une période et lancez l'extraction.")
    
    c_sel, c_btn = st.columns([1, 2])
    with c_sel:
        selected_year = st.selectbox("Période cible :", ["2025", "2024", "2023", "2022"])
    with c_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Lancer l'extraction et le calcul"):
            with st.spinner(f"Extraction et calculs en cours pour {selected_year}..."):
                try:
                    from src import scraper, etl
                    
                    # 1. Scraping
                    raw_files_dict = scraper.download_eba_data(period=selected_year)
                    
                    if raw_files_dict:
                        st.write("Fichiers téléchargés. Lancement de l'ETL...")
                        
                        # 2. Nettoyage
                        df_clean = etl.run_etl(raw_files_dict) 
                        
                        if df_clean is not None and not df_clean.empty:
                            st.write("Harmonisation des données...")
                            
                            # --- CORRECTION ICI ---
                            # On harmonise le nom de la colonne RWA
                            if 'RWA_Total' in df_clean.columns:
                                df_clean.rename(columns={'RWA_Total': 'RWA_Final'}, inplace=True)
                            
                            # 3. Sauvegarde directe
                            # (Les ratios complexes sont calculés automatiquement à la volée par app.py !)
                            os.makedirs("data/processed", exist_ok=True)
                            df_clean.to_csv("data/processed/final_results_2025.csv", index=False)
                            st.success("✅ Base de données actualisée avec succès !")
                            
                            # 4. Recharge la page
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("❌ Échec lors du nettoyage des données (ETL).")
                    else:
                        st.error("❌ Échec du téléchargement.")
                except Exception as e:
                    st.error(f"❌ Erreur critique du système : {e}")

# --- 3. DONNÉES & CALCULS ---
@st.cache_data
def load_data():
    path = "data/processed/final_results_2025.csv"
    # Si le fichier n'existe pas, on renvoie tout de suite un dataframe vide
    if not os.path.exists(path): 
        return pd.DataFrame()
        
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()
    
    # Mapping Noms (Liste étendue des principales banques européennes EBA)
    lei_mapping = {
        # --- FRANCE ---
        "R0MUWSFPU8MPRO8K5P83":	"BNP Paribas",
        "549300FH0WJAPEHTIQ77":	"BofA Securities Europe SA",
        "969500STN7T9MRUMJ267":	"Bpifrance",
        "9695000CG7B84NLR5984":	"Confédération Nationale du Crédit Mutuel",
        "FR9695005MSX1OYEMGDF":	"Groupe BPCE",
        "FR969500TJ5KRTCJQWXH":	"Groupe Crédit Agricole",
        "F0HUI1NY1AZMJMD8LP67":	"HSBC Continental Europe",
        "96950066U5XAAIRCPA78":	"La Banque Postale",
        "96950001WI712W7PQG45":	"RCI Banque",
        "549300HFEHJOXGE4ZE63":	"SFIL S.A.",
        "O2RNE8IBXP4R0TD8PU41":	"Société générale S.A.",

        
        # --- ALLEMAGNE ---
        "254900RNFMDM0P11YR84":	"ATLANTIC LUX HOLDCO S.A R.L.",
        "VDYMYTQGZZ6DU0912C88":	"Bayerische Landesbank",
        "851WYGNLUQLFZBSYGB56":	"COMMERZBANK Aktiengesellschaft",
        "6TJCK1B7E7UTXP528Y04":	"Citigroup Global Markets Europe AG",
        "5299007S3UH5RKUYDA52":	"DEUTSCHE APOTHEKER- UND ÄRZTEBANK EG",
        "7LTWFZYICNSX8D621K86":	"DEUTSCHE BANK AKTIENGESELLSCHAFT",
        "529900HNOAA1KXQJUQ27":	"DZ BANK AG Deutsche Zentral-Genossenschaftsbank, Frankfurt am Main",
        "0W2PZJM8XOY22M4GG883":	"DekaBank Deutsche Girozentrale",
        "DZZ47B9A52ZJ6LT6VV95":	"Deutsche Pfandbriefbank AG",
        "391200EEGLNXBBCVKC73":	"Erwerbsgesellschaft der S-Finanzgruppe mbH & Co. KG",
        "8IBZUGJ7JPLH368JE346":	"Goldman Sachs Bank Europe SE",
        "529900JZTYE3W7WQH904":	"HASPA Finanzholding",
        "TUKDD90GPC79G1KOE162":	"Hamburg Commercial Bank AG",
        "549300ZK53CNGEEI6A29":	"J.P. Morgan SE",
        "B81CK4ESI35472RHJ606":	"Landesbank Baden-Württemberg",
        "DIZES5CFO5K3I5R58746":	"Landesbank Hessen-Thüringen Girozentrale",
        "549300C9KPZR0VZ16R05":	"Morgan Stanley Europe Holding SE",
        "529900GM944JT8YIRL63":	"Münchener Hypothekenbank eG",
        "DSNHHQ2B9X5N6OUJ1236":	"Norddeutsche Landesbank - Girozentrale -",
        "529900V3O1M5IHMOSF46":	"State Street Europe Holdings Germany S.a.r.l. & Co. KG",
        "5299007QVIQ7IO64NX37":	"UBS Europe SE",
        "529900SSGT49ZZSWYE62":	"Volkswagen Financial Services AG",
        "529900S1KHKOEQL5CK20":	"Wüstenrot Bausparkasse Aktiengesellschaft",

        
        # --- ESPAGNE ---
        "54930056IRBXK0Q1FP96":	"Abanca Corporacion Bancaria, S.A.",
        "K8MS7FD7N5Z2WQ51AZ71":	"Banco Bilbao Vizcaya Argentaria, S.A.",
        "5493006QMFDDMYWIAM13":	"Banco Santander, S.A.",
        "95980020140005881190":	"Banco de Crédito Social Cooperativo",
        "SI5RG2M0WQQLZCXKRM20":	"Banco de Sabadell, S.A.",
        "VWMYAEQSTOPNV0SUGU82":	"Bankinter, S.A.",
        "7CUNS533WID6K7DGFI87":	"CaixaBank, S.A.",
        "549300OLBL49CW8CT155":	"Ibercaja Banco, S.A.",
        "549300U4LIZV0REEQQ46":	"Kutxabank, S.A.",
        "5493007SJLLCTM6J6M37":	"Unicaja Banco, S.A.",
            
        # --- ITALIE ---
        "7LVZJ6XRIE7VNZ4UBX81":	"BANCA MEDIOLANUM S.P.A.",
        "J48C8PCSJVUBR8KCW529":	"BANCA POPOLARE DI SONDRIO SOCIETA' PER AZIONI",
        "815600E4E6DCD2D25E30":	"BANCO BPM SOCIETA' PER AZIONI",
        "N747OI7JINV7RUUH6190":	"BPER Banca S.p.A.",
        "J4CP7MHCXR8DAQMKIL78":	"Banca Monte dei Paschi di Siena S.p.A.",
        "LOO0AWXR8GF142JCO404":	"CASSA CENTRALE BANCA",
        "815600AD83B2B6317788":	"CREDITO EMILIANO HOLDING SOCIETA' PER AZIONI",
        "549300L7YCATGO57ZE10":	"FINECOBANK BANCA S.P.A.",
        "NNVPP80YIZGEY2314M97":	"ICCREA BANCA S.P.A.",
        "2W8N8UU78PMDQKZENC08":	"Intesa Sanpaolo S.p.A.",
        "PSNL19R2RXX5U3QWHI44":	"Mediobanca - Banca di Credito Finanziario S.p.A.",
        "549300TRUWO2CD2G5692":	"UNICREDIT, SOCIETA' PER AZIONI",


        # --- PAYS-BAS ---
        "BFXS5XCH7N0Y05NIXW11":	"ABN AMRO Bank N.V.",
        "529900GGYMNGRQTDOO93":	"BNG Bank N.V.",
        "DG3RU1DBUFHT4ZF9WN62":	"Coöperatieve Rabobank U.A.",
        "549300NYKK9MWM7GGW15":	"ING Groep N.V.",
        "JLP5FSPH9WPSHY3NIM24":	"Nederlandse Waterschapsbank N.V.",
        "724500JIWG886A9RRT57":	"RBS Holdings N.V.",
        "724500A1FNICHSDF2I11":	"de Volksbank N.V.",

        # --- AUTRICHE ---
        "529900S9YO2JHTIIDG38":	"BAWAG Group AG",
        "9ZHRYM6F437SQJ6OUG95":	"Raiffeisen Bank International AG",
        "529900SXEWPJ1MRRX537":	"Raiffeisen-Holding Niederösterreich-Wien",
        "529900XSTAE561178282":	"Raiffeisenbankengruppe OÖ Verbund eGen",
        "AT0000000000043000VB":	"VOLKSBANK WIEN AG VB",

        # --- BELGIQUE ---
        "A5GWLFH3KM7YV2SFQL84":	"Belfius Bank",
        "549300DYPOFMXOR7XM56":	"Crelan",
        "5493008QOCP58OLEN998":	"Investeringsmaatschappij Argenta",
        "213800X3Q9LSAKRUWY91":	"KBC Groep",
        "MMYX0N4ZEZ13Z4XCG897":	"The Bank of New York Mellon",

        # --- CHYPRE ---
        "635400L14KNHZXPUZM19":	"Bank of Cyprus Holdings Public Limited Company",

        # --- DANEMARK ---
        "MAES062Z21O4RZ2U7M96":	"Danske Bank A/S",
        "3M5E1GQGKL17HI6CPN30":	"Jyske Bank A/S",
        "LIU16F6VZJSD6UKHD557":	"Nykredit Realkredit A/S",

        # --- ESTONIE ---
        "529900JG015JC10LED24":	"AS LHV Group",

        # --- FINLANDE ---
        "529900HEKOENJHPNN480":	"Kuntarahoitus Oyj",
        "529900ODI3047E2LIV03":	"Nordea Bank Abp",
        "7437003B5WFBOIEFY714":	"OP Osuuskunta",

        # --- GRECE ---
        "213800DBQIB6VBNU5C64":	"Alpha Bank S.A.",
        "JEUVK5RWVJEN8W0C9M24":	"Eurobank Ergasias Services and Holdings S.A.",
        "5UMCZOEYKCVFAW8ZLO05":	"National Bank of Greece, S.A.",
        "M6AD1Y1KW32H8THQ6F76":	"Piraeus Financial Holdings",

        # --- HONGRIE ---
        "3H0Q3U74FVFED2SHZT16":	"MBH bankcsoport",
        "529900W3MOO00A18X956":	"OTP-csoport",

        # --- IRELANDE ---
        "635400AKJBGNS5WNQL34":	"AIB Group plc",
        "EQYXK86SF381Q21S3020":	"Bank of America Europe Designated Activity Company",
        "635400C8EK6DRI12LJ39":	"Bank of Ireland Group plc",
        "2G5BKIC2CB69PRJH1W31":	"Barclays Bank Ireland plc",
        "N1FBEDJ5J41VKZLO2475":	"Citibank Europe Plc",

        # --- LATIVIE ---
        "2138009Y59EAR7H1UO97":	"Akciju sabiedrība \"Citadele banka\"",

        # --- LIECHTENSTEIN ---
        "5493009EIBTCB1X12G89":	"LGT Group Foundation",
        "529900OE1FOAM50XLP72":	"Liechtensteinische Landesbank AG",

        # --- LITUANIE ---
        "549300TK038P6EV4YU51":	"Akcinė bendrovė Šiaulių bankas",
        "485100FX5Y9YLAQLNP12":	"Revolut Holdings Europe UAB",

        # --- LUXEMBOURG ---
        "9CZ7TVMR36CYD5TZBS50":	"Banque Internationale à Luxembourg",
        "R7CQUF1DQM73HUTV1078":	"Banque et Caisse d´Epargne de l´Etat, Luxembourg",

        # --- MALTE ---
        "529900RWC8ZYB066JF16":	"Bank of Valletta Plc",
        "213800TC9PZRBHMJW403":	"MDB Group Limited",

        # --- NORVEGE ---
        "549300GKFG0RYRRQ1414":	"DNB BANK ASA",
        "7V6Z97IO7R1SEAO84Q32":	"SpareBank 1 SMN",
        "549300Q3OIWRHQUQM052":	"SpareBank 1 Sør-Norge",

        # --- POLOGNE ---
        "5493000LKS7B3UTF7H35":	"Bank Polska Kasa Opieki S.A.",
        "P4GTT6GF1W40CVIMFR43":	"Powszechna Kasa Oszczednosci Bank Polski S.A.",

        # --- PORTUGAL ---
        "JU1U6S0DG9YLT7N8ZV32":	"Banco Comercial Português, SA",
        "TO822O0VT80V06K0FH57":	"Caixa Geral de Depósitos, S.A.",
        "222100K6QL2V4MLHWQ08":	"LSF NANI INVESTMENTS S.A R.L.",

        # --- ROUMANIE ---
        "549300RG3H390KEL8896":	"Banca Transilvania",
        "2138008AVF4W7FMW8W87":	"CEC BANK S.A.",

        # --- SLOVENIE ---
        "213800HDJ876ACJXXD05":	"AIKGROUP (CY) LIMITED",
        "5493001BABFV7P27OW30":	"Nova Ljubljanska Banka d.d., Ljubljana",

        # --- SUEDE ---
        "EV2XZWMLLXF2QRX0CD47":	"Kommuninvest - Grupp",
        "549300C6TUMDXNOVXS82":	"Länsförsäkringar Bank AB - gruppen",
        "H0YX5LBGKDVOWCXBZ594":	"SBAB Bank AB - Grupp",
        "F3JS33DEI6XQ4ZBPTN86":	"Skandinaviska Enskilda Banken - gruppen",
        "NHBDILHZTYCNBV5UYZ31":	"Svenska Handelsbanken - gruppen",
        "M312WZV08Y7LYUC71685":	"Swedbank - Grupp",



        # --- ROYAUME-UNI (Entités UE post-Brexit ou Groupes) ---
        "MP6I5ZYZBEU3UXPYFY54": "HSBC Holdings",
        "G5GSEF7VJP5I7OUK5573": "Barclays",
        "549300PPXHE2MFSCDA93": "Lloyds Banking Group",
        "2138005O9XJIJN4JPN90": "NatWest Group",
        "U4LOSYZ7YG4W3S5F2G91": "Standard Chartered",
        "XXXXXXXXXXXXXXXXXXXX": "All other banks",
    }

    df['Bank_Label'] = df.apply(lambda x: f"{lei_mapping.get(x['LEI'], 'Banque ' + str(x.get('NSA', 'EU')) + ' - ' + x['LEI'])}", axis=1)

    # --- Vérification des colonnes brutes ---
    cols_check = ['Total_Assets', 'RWA_Final', 'Tier1_Capital', 'CET1_Capital', 'Total_Capital',
                  'Net_Income', 'NPL_Amount', 'Loans_Gross', 'Provisions_Stock', 'Leverage_Exposure']
    for col in cols_check:
        if col not in df.columns: df[col] = 0.0

    if 'Date' not in df.columns: df['Date'] = "Inconnue"

    # ==========================================
    # CALCUL DES RATIOS DE SOLVABILITÉ BÂLE III
    # ==========================================
    # 1. Ratios de Capital
    df['CET1_Ratio_Pct'] = np.where(df['RWA_Final'] > 0, (df['CET1_Capital'] / df['RWA_Final']) * 100, 0)
    df['Tier1_Ratio_Pct'] = np.where(df['RWA_Final'] > 0, (df['Tier1_Capital'] / df['RWA_Final']) * 100, 0)
    df['TCR_Pct'] = np.where(df['RWA_Final'] > 0, (df['Total_Capital'] / df['RWA_Final']) * 100, 0)
    df['AT1_Ratio_Pct'] = df['Tier1_Ratio_Pct'] - df['CET1_Ratio_Pct']
    
    # 2. Ratio de Levier
    exposure = df['Leverage_Exposure'] if 'Leverage_Exposure' in df.columns else df['Total_Assets']
    df['Leverage_Ratio_Pct'] = np.where(exposure > 0, (df['Tier1_Capital'] / exposure) * 100, 0)

    # 3. Rentabilité et Densité
    df['ROE_Pct'] = df.apply(lambda x: (x['Net_Income']/x['Tier1_Capital']*100) if x['Tier1_Capital']>0 else 0, axis=1)
    df['Risk_Density_Pct'] = df.apply(lambda x: (x['RWA_Final']/x['Total_Assets']*100) if x['Total_Assets']>0 else 0, axis=1)
    
    # 4. Proxies de Liquidité (LCR, NSFR, LTD)
    df['Deposits_Proxy'] = df['Total_Assets'] * 0.60
    df['HQLA_Proxy'] = df['Total_Assets'] * 0.15
    
    asf = df['Total_Capital'] * 1.0 + df['Deposits_Proxy'] * 0.90
    other_assets = (df['Total_Assets'] - df['Loans_Gross'] - df['HQLA_Proxy']).clip(lower=0) 
    rsf = (df['Loans_Gross'] * 0.70) + (other_assets * 0.85)
    
    df['NSFR_Ratio_Pct'] = np.where(rsf > 0, (asf / rsf) * 100, 0)
    df['Net_Outflows_Proxy'] = df['Deposits_Proxy'] * 0.10
    df['LCR_Ratio_Pct'] = np.where(df['Net_Outflows_Proxy'] > 0, (df['HQLA_Proxy'] / df['Net_Outflows_Proxy']) * 100, 0)
    df['LTD_Ratio_Pct'] = np.where(df['Deposits_Proxy'] > 0, (df['Loans_Gross'] / df['Deposits_Proxy']) * 100, 0)

    # 5. Ratios de Risque (Texas, NPL, MREL)
    df['Texas_Ratio_Pct'] = np.where((df['CET1_Capital'] + df['Provisions_Stock']) > 0, 
                                     (df['NPL_Amount'] / (df['CET1_Capital'] + df['Provisions_Stock'])) * 100, 0)
    df['NPL_Ratio_Pct'] = np.where(df['Loans_Gross'] > 0, (df['NPL_Amount'] / df['Loans_Gross']) * 100, 0)
    df['MREL_Ratio_Pct'] = np.where(df['RWA_Final'] > 0, ((df['Total_Capital'] + (df['Total_Assets'] * 0.15)) / df['RWA_Final']) * 100, 0)

    # 6. SREP (Exigences Réglementaires)
    if 'P2R_Pct' not in df.columns: df['P2R_Pct'] = 2.0
    if 'P2G_Pct' not in df.columns: df['P2G_Pct'] = 1.0
    if 'CBR_Pct' not in df.columns: df['CBR_Pct'] = 2.5
    df['SREP_Requirement_Pct'] = 8.0 + df['P2R_Pct'] + df['CBR_Pct']
    
    df['Capital_Buffer_Pct'] = df['TCR_Pct'] - df['SREP_Requirement_Pct']

    return df.replace([np.inf, -np.inf], 0).fillna(0)

df = load_data()
if df.empty: st.stop()

market_means = df.mean(numeric_only=True)

# --- 3. MOTEUR GRAPHIQUE ---
def plot_gauge(value, min_th, max_th, inverse=False):
    if inverse: colors = [{'range': [0, min_th], 'color': "#2ecc71"}, {'range': [min_th, max_th], 'color': "#f1c40f"}, {'range': [max_th, max(value*1.2, max_th*1.5)], 'color': "#e74c3c"}]
    else: colors = [{'range': [0, min_th], 'color': "#e74c3c"}, {'range': [min_th, max_th], 'color': "#f1c40f"}, {'range': [max_th, max(100, max_th*1.5, value*1.2)], 'color': "#2ecc71"}]
        
    fig = go.Figure(go.Indicator(
        mode="gauge", value=value,
        gauge={'axis': {'range': [0, max(value*1.2, max_th*1.2)]}, 'bar': {'color': "#2c3e50"}, 'steps': colors, 'threshold': {'line': {'color': "black", 'width': 3}, 'thickness': 0.75, 'value': value}}
    ))
    fig.update_layout(height=100, margin=dict(l=10, r=10, t=0, b=0))
    return fig

def card(col, val, title, txt, min_b, min_g, inverse=False):
    with col:
        st.metric(label=title, value=f"{val:.2f}%", help=txt)
        st.plotly_chart(plot_gauge(val, min_b, min_g, inverse), use_container_width=True)

# --- 4. HEADER ---
c1, c2 = st.columns([3, 1])
with c2: 
    bank_list = sorted(df['Bank_Label'].unique())
    selected_label = st.selectbox("Sélectionner la Banque", bank_list)

bank = df[df['Bank_Label'] == selected_label].iloc[0]


# --- 5. INDICATEURS CLÉS ---
st.markdown("### 1. Solvabilité & Coussins")
c1, c2, c3, c4, c5 = st.columns(5)
card(c1, bank['CET1_Ratio_Pct'], "CET1 Ratio", "[Source : EBA Brute]\nCapital Dur (Actions). Le minimum réglementaire est de 4.5%.", 4.5, 9.0)
card(c2, bank['AT1_Ratio_Pct'], "Ratio AT1", "[Source : Calculé]\nAdditional Tier 1. Déduit par différence (Tier 1 - CET1).", 1.0, 1.5)
card(c3, bank['TCR_Pct'], "Total Capital", "[Source : EBA Brute]\nRatio Global de Solvabilité. Min 8%.", 8.0, 12.0)
card(c4, bank['Leverage_Ratio_Pct'], "Levier (LR)", "[Source : EBA Brute]\nRatio de Levier (Bâle III). Min 3.0%.", 3.0, 4.0)
card(c5, bank['Capital_Buffer_Pct'], "Coussin Dispo.", "[Source : Calculé vs SREP]\nExcédent de Capital réel au-dessus des exigences de la BCE.", 0.0, 2.0)

st.markdown("### 2. Exigences Réglementaires (SREP)")
s1, s2, s3, s4 = st.columns(4)
card(s1, bank['P2R_Pct'], "Pilier 2 (P2R)", "[Source : Proxy Modèle]\nDonnée confidentielle BCE. Proxy fixé à 2% pour le stress test.", 1.5, 2.5, inverse=True)
card(s2, bank['P2G_Pct'], "Guidance (P2G)", "[Source : Proxy Modèle]\nDonnée confidentielle BCE. Recommandation fixée à 1%.", 0.5, 1.5, inverse=True)
card(s3, bank['CBR_Pct'], "Buffer Combiné", "[Source : Proxy Modèle]\nCoussins macroprudentiels estimés à 2.5%.", 1.5, 3.5, inverse=False)
card(s4, bank['SREP_Requirement_Pct'], "Total SREP", "[Source : Calculé]\nP1 (8%) + P2R + CBR. Seuil de déclenchement des restrictions.", 9.0, 11.0, inverse=True)

st.markdown("### 3. Liquidité, Résolution & Risques")
l1, l2, l3, l4, l5 = st.columns(5)
card(l1, bank['LCR_Ratio_Pct'], "LCR (30j)", "[Source : Proxy Modèle]\nReconstruit via une estimation des HQLA et des sorties nettes. Min légal 100%.", 100, 110)
card(l2, bank['NSFR_Ratio_Pct'], "NSFR (1an)", "[Source : Proxy Modèle]\nPondérations Bâle III appliquées sur les encours bilanciels de l'EBA. Min 100%.", 100, 110)
card(l3, bank['MREL_Ratio_Pct'], "MREL / TLAC", "[Source : Proxy Modèle]\nCapacité d'absorption des pertes estimée à partir du bilan.", 18, 22)
card(l4, bank['Texas_Ratio_Pct'], "Texas Ratio", "[Source : Calculé]\nNPL / (CET1 + Provisions). Indicateur avancé de faillite.", 50, 90, inverse=True)
card(l5, bank['NPL_Ratio_Pct'], "Taux NPL", "[Source : Calculé]\nPart des crédits douteux bruts sur le total des prêts.", 3.0, 5.0, inverse=True)

st.divider()

# --- 6. GRAPHIQUES AVANCÉS ---
st.subheader("Analyse Comparative Visuelle")
g_left, g_right = st.columns(2)

with g_left:
    st.markdown("**1. Matrice Stratégique (Rentabilité vs Risque)**")
    color_opt = "NSA" if "NSA" in df.columns else None
    fig_scat = px.scatter(
        df, x="Risk_Density_Pct", y="ROE_Pct",
        color=color_opt, size="Total_Assets", hover_name="Bank_Label",
        labels={"Risk_Density_Pct": "Densité RWA (%)", "ROE_Pct": "ROE (%)"}
    )
    fig_scat.add_trace(go.Scatter(
        x=[bank['Risk_Density_Pct']], y=[bank['ROE_Pct']],
        mode='markers', marker=dict(color='#D32F2F', size=18, symbol='diamond', line=dict(width=2, color='white')),
        name="MA BANQUE"
    ))
    fig_scat.update_layout(title="Positionnement Concurrentiel")
    st.plotly_chart(fig_scat, use_container_width=True)

with g_right:
    st.markdown("**2. Benchmark Liquidité vs Moyenne Marché**")
    cats = ['LCR', 'NSFR', 'LTD']
    vals_b = [bank['LCR_Ratio_Pct'], bank['NSFR_Ratio_Pct'], bank['LTD_Ratio_Pct']]
    vals_m = [market_means['LCR_Ratio_Pct'], market_means['NSFR_Ratio_Pct'], market_means['LTD_Ratio_Pct']]
    
    fig_bar = go.Figure(data=[
        go.Bar(name='Ma Banque', x=cats, y=vals_b, marker_color='#2c3e50'),
        go.Bar(name='Moyenne Marché', x=cats, y=vals_m, marker_color='#95a5a6')
    ])
    fig_bar.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="Exigence BCE (100%)")
    fig_bar.update_layout(barmode='group', title="Écart de Liquidité")
    st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# --- 7. VERDICT DÉTAILLÉ (MÉTHODOLOGIE SREP) ---
st.subheader("Rapport d'Inspection : Synthèse des Risques")

st.markdown("""
Cette section génère automatiquement une opinion de crédit basée sur les 4 piliers de l'analyse bancaire (Solvabilité, Qualité des Actifs, Liquidité, Rentabilité).
""")

# Initialisation des compteurs d'alerte
alert_count = 0
warning_count = 0

p1, p2 = st.columns(2)
p3, p4 = st.columns(2)

# --- PILIER 1 : SOLVABILITÉ (Capital) ---
with p1:
    st.markdown("#### Pilier 1 : Solvabilité")
    if bank['Capital_Buffer_Pct'] < 0:
        st.error(f"**CRITIQUE** : La banque est en infraction réglementaire avec un déficit de capital de {bank['Capital_Buffer_Pct']:.2f}%. Risque de restrictions sur les dividendes (MDA).")
        alert_count += 1
    elif bank['Capital_Buffer_Pct'] < 1.5:
        st.warning(f"**TENDU** : Coussin de sécurité très fin ({bank['Capital_Buffer_Pct']:.2f}%). La banque a très peu de marge de manœuvre en cas de stress macroéconomique.")
        warning_count += 1
    else:
        st.success(f"**SOLIDE** : Excellente capitalisation. Le coussin de {bank['Capital_Buffer_Pct']:.2f}% permet d'absorber des chocs sévères tout en respectant le Pilier 2.")

# --- PILIER 2 : QUALITÉ DES ACTIFS (Asset Quality) ---
with p2:
    st.markdown("#### Pilier 2 : Qualité des Actifs")
    if bank['Texas_Ratio_Pct'] > 100 or bank['NPL_Ratio_Pct'] > 5.0:
        st.error(f"**CRITIQUE** : Texas Ratio alarmant ({bank['Texas_Ratio_Pct']:.0f}%) ou NPL trop lourd ({bank['NPL_Ratio_Pct']:.1f}%). Les créances douteuses menacent directement la viabilité de la banque.")
        alert_count += 1
    elif bank['Texas_Ratio_Pct'] > 50 or bank['NPL_Ratio_Pct'] > 3.0:
        st.warning(f"**VIGILANCE** : La détérioration du portefeuille (NPL à {bank['NPL_Ratio_Pct']:.1f}%) pèse sur le bilan. Un provisionnement supplémentaire pourrait être exigé par la BCE.")
        warning_count += 1
    else:
        st.success(f"**SAIN** : Portefeuille de crédits performant. Le taux de NPL ({bank['NPL_Ratio_Pct']:.1f}%) est maîtrisé et parfaitement couvert par les provisions et le capital.")

# --- PILIER 3 : LIQUIDITÉ (Liquidity) ---
with p3:
    st.markdown("#### Pilier 3 : Liquidité & Financement")
    if bank['LCR_Ratio_Pct'] < 100 or bank['NSFR_Ratio_Pct'] < 100:
        st.error(f"**CRITIQUE** : Non-conformité aux ratios Bâle III (LCR: {bank['LCR_Ratio_Pct']:.0f}%, NSFR: {bank['NSFR_Ratio_Pct']:.0f}%). Risque imminent d'illiquidité (Bank Run). *Note: calculé via Proxies.*")
        alert_count += 1
    elif bank['LCR_Ratio_Pct'] < 110 or bank['LTD_Ratio_Pct'] > 120:
        st.warning(f"**VIGILANCE** : Liquidité sous tension ou forte dépendance aux marchés de gros (LTD: {bank['LTD_Ratio_Pct']:.0f}%). Le profil de refinancement doit être sécurisé.")
        warning_count += 1
    else:
        st.success(f"**CONFORTABLE** : Profil de financement stable (NSFR: {bank['NSFR_Ratio_Pct']:.0f}%) et réserves de liquidité à court terme abondantes.")

# --- PILIER 4 : RENTABILITÉ (Profitability) ---
with p4:
    st.markdown("#### Pilier 4 : Rentabilité & Modèle")
    if bank['ROE_Pct'] < 0:
        st.error(f"**DESTRUCTION DE VALEUR** : La banque est en perte (ROE: {bank['ROE_Pct']:.1f}%), ce qui érode organiquement sa base de capital trimestre après trimestre.")
        alert_count += 1
    elif bank['ROE_Pct'] < 5.0:
        st.warning(f"**FAIBLE** : Rentabilité (ROE: {bank['ROE_Pct']:.1f}%) inférieure au coût du capital. Modèle économique sous pression, vulnérable aux taux d'intérêt.")
        warning_count += 1
    else:
        st.success(f"**PERFORMANT** : Génération organique de capital robuste (ROE: {bank['ROE_Pct']:.1f}%), justifiant le profil de risque (Densité RWA: {bank['Risk_Density_Pct']:.0f}%).")

st.markdown("---")

# --- SYNTHÈSE GLOBALE DIRECTIVE ---
st.markdown("### Conclusion (SREP)")

if alert_count >= 2:
    st.error(f"""
    **STATUT : BANQUE EN RÉSOLUTION / RESTRUCTURATION (DANGER SYSTÉMIQUE)**
    Cet établissement cumule {alert_count} défaillances critiques sur les piliers fondamentaux. Une intervention réglementaire immédiate est requise (injonction de recapitalisation, vente d'actifs, ou déclenchement du dispositif de résolution MREL/Bail-in).
    """)
elif alert_count == 1:
    st.warning(f"""
    **STATUT : SOUS SURVEILLANCE RENFORCÉE (EARLY WARNING)**
    L'établissement présente une faille majeure. Bien que les autres piliers puissent compenser temporairement, la banque est extrêmement vulnérable. Une mise sous tutelle partielle ou une augmentation des exigences P2R est recommandée.
    """)
elif warning_count >= 2:
    st.info(f"""
    **STATUT : PROFIL MODÉRÉ / VULNÉRABLE**
    Banque globalement solvable mais présentant {warning_count} points d'attention (marges faibles, NPL en hausse ou liquidité tendue). Aucun risque systémique immédiat, mais nécessite une optimisation du bilan à moyen terme.
    """)
else:
    st.success(f"""
    **STATUT : BANQUE CHAMPIONNE (INVESTMENT GRADE)**
    Fondamentaux excellents. L'établissement surperforme les exigences réglementaires de la BCE. Il dispose d'une forte capacité d'absorption des chocs macroéconomiques et d'un modèle d'affaires pérenne.
    """)

st.caption("Méthodologie : Rapport généré selon les données publiques de l'EBA. Les indicateurs Pilier 2 et Liquidité sont modélisés par des proxies respectant l'esprit des exigences CRR/CRD IV de la BCE.")