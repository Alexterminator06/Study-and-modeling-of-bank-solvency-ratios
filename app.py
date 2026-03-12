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

with st.expander("⚙️ Mise à jour des données EBA (Scraping)", expanded=True): # mis sur "True" pour qu'il soit ouvert si c'est vide
    st.info("Aucune donnée ? Sélectionnez une période et lancez l'extraction.")
    
    c_sel, c_btn = st.columns([1, 2])
    with c_sel:
        selected_year = st.selectbox("Période cible :", ["2024", "2023", "2022"])
    with c_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Lancer l'extraction et le calcul"):
            with st.spinner(f"Extraction et calculs en cours pour {selected_year}..."):
                try:
                    from src import scraper, etl
                    
                    # 1. Scraping
                    raw_files_dict = scraper.download_eba_data(period=selected_year)
                    
                    if raw_files_dict:
                        st.write("📥 Fichiers téléchargés. Lancement de l'ETL...")
                        
                        # 2. Nettoyage
                        df_clean = etl.run_etl(raw_files_dict) 
                        
                        if df_clean is not None and not df_clean.empty:
                            st.write("🧮 Harmonisation des données...")
                            
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
        "R0MUWSFPU8MPRO8K5P83": "BNP Paribas",
        "969500TKUY0GD4QE4754": "Société Générale",
        "969500TJ5KRTCJQWXH05": "Crédit Agricole Group",
        "9695005MSX1OYEMGDF46": "Groupe BPCE",
        "969500251306VVN14103": "La Banque Postale",
        "J4KT8KOXD2P13C9T3180": "Crédit Mutuel",
        "FOHUI1NY1AZSO6PB3W13": "HSBC Continental Europe",
        
        # --- ALLEMAGNE ---
        "7LTWFZYICNSX8D621K86": "Deutsche Bank",
        "851WYGNLUQLFZBSYGB56": "Commerzbank",
        "5299000OZYK41CNKN243": "Landesbank Baden-Württemberg (LBBW)",
        "DSNHHQ2B9X5N6OU08235": "Bayerische Landesbank (BayernLB)",
        "529900V3ZPEFWVKE4M63": "DZ BANK",
        "529900GGY5T66QCW0990": "Norddeutsche Landesbank (NordLB)",
        "O2RNE8IBXP4R0TD8PU41": "Helaba (Landesbank Hessen-Thüringen)",
        "2W8N8UU78PMDWI385219": "UniCredit Bank AG (HVB)",
        
        # --- ESPAGNE ---
        "5493006QMFDDMYWIAM13": "Banco Santander",
        "K8MS7FD7N5Z2WQ51AZ71": "BBVA",
        "SI5RG2M0WQQLZCXKRM20": "CaixaBank",
        "549300U4LTZ728KLKU68": "Banco de Sabadell",
        "5493006YMJAQ4E1Q9223": "Bankinter",
        
        # --- ITALIE ---
        "549300TRUWO2CD2G5692": "UniCredit SpA",
        "2W8N8UU78PMDWI385219": "Intesa Sanpaolo",
        "81560097964CDB924035": "Banco BPM",
        "549300Z91F73F5S89N52": "BPER Banca",
        "52990033C5FUCH75J479": "Mediobanca",
        "J4CP7MHCXR8DAQMKIL78": "Banca Monte dei Paschi di Siena",

        # --- PAYS-BAS ---
        "3TK20IVIUJ8J3ZU0QE75": "ING Groep",
        "724500PMK2A2M1SQQ228": "ABN AMRO Bank",
        "BFXS5XCH7N0Y05NIXW11": "Rabobank",
        "549300L70BSU59300D71": "De Volksbank",

        # --- ROYAUME-UNI (Entités UE post-Brexit ou Groupes) ---
        "MP6I5ZYZBEU3UXPYFY54": "HSBC Holdings",
        "G5GSEF7VJP5I7OUK5573": "Barclays",
        "549300PPXHE2MFSCDA93": "Lloyds Banking Group",
        "2138005O9XJIJN4JPN90": "NatWest Group",
        "U4LOSYZ7YG4W3S5F2G91": "Standard Chartered",

        # --- NORDIQUES (Suède, Danemark, Finlande, Norvège) ---
        "529900ODI3047E2LIV03": "Nordea",
        "MAES062Z21O4RZ2U7M96": "SEB (Skandinaviska Enskilda)",
        "M312WZV08Y7LYUC71685": "Danske Bank",
        "F7NTS631M5F18XQ26V13": "Swedbank",
        "549300D5I7C66WKN4B31": "Handelsbanken",
        "549300GKFG0RYRRQ1414": "DNB Bank",
        "549300K7MT8135596I23": "OP Financial Group",

        # --- AUTRES (Belgique, Autriche, Irlande, Portugal...) ---
        "21380041JRJ96B5X6K53": "KBC Group",
        "54930005F9312WJ9LG93": "Belfius Banque",
        "549300X3H0VKEF8XN891": "Erste Group",
        "9ZHRYM6F437SQJ6OUG95": "Raiffeisen Bank International",
        "635400L14KZEWXQ14Y53": "AIB Group (Allied Irish Banks)",
        "549300YPMVF137QRA232": "Bank of Ireland",
        "PTCGD0AM0009": "Caixa Geral de Depósitos"
    }
    df['Bank_Label'] = df.apply(lambda x: f"{lei_mapping.get(x['LEI'], 'Banque ' + str(x.get('NSA', 'EU')) + ' - ' + x['LEI'])}", axis=1)

    # --- Vérification des colonnes brutes ---
    cols_check = ['Total_Assets', 'RWA_Final', 'Tier1_Capital', 'CET1_Capital', 'Total_Capital',
                  'Net_Income', 'NPL_Amount', 'Loans_Gross', 'Provisions_Stock', 'Leverage_Exposure']
    for col in cols_check:
        if col not in df.columns: df[col] = 0.0

    if 'Date' not in df.columns: df['Date'] = "Inconnue"

    # ==========================================
    # 🧮 CALCUL DES RATIOS DE SOLVABILITÉ BÂLE III
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
st.subheader("📊 Analyse Comparative Visuelle")
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
st.subheader("🏁 Rapport d'Inspection : Synthèse des Risques")

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
    st.markdown("#### 🛡️ Pilier 1 : Solvabilité")
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
    st.markdown("#### 📉 Pilier 2 : Qualité des Actifs")
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
    st.markdown("#### 💧 Pilier 3 : Liquidité & Financement")
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
    st.markdown("#### 📈 Pilier 4 : Rentabilité & Modèle")
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
st.markdown("### 🏛️ Conclusion (SREP)")

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