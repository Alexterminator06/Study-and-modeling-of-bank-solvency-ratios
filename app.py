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

# --- 2. DONNÉES & CALCULS ---
@st.cache_data
def load_data():
    path = "data/processed/final_results_2025.csv"
    if not os.path.exists(path): return pd.DataFrame()
    df = pd.read_csv(path)
    
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

    # Sécurité colonnes
    cols_check = ['Total_Assets', 'RWA_Final', 'Tier1_Capital', 'CET1_Capital', 'Total_Capital',
                  'Net_Income', 'NPL_Amount', 'LCR_Ratio_Pct', 'NSFR_Ratio_Pct', 'LTD_Ratio_Pct', 
                  'Texas_Ratio_Pct', 'P2R_Pct', 'P2G_Pct', 'CBR_Pct', 'SREP_Requirement_Pct', 
                  'MREL_Ratio_Pct', 'Leverage_Ratio_Pct', 'Capital_Buffer_Pct']
    for col in cols_check:
        if col not in df.columns: df[col] = 0.0

    # Calculs à la volée
    if 'AT1_Ratio_Pct' not in df.columns:
        if 'Tier1_Ratio_Pct' in df.columns and 'CET1_Ratio_Pct' in df.columns:
            df['AT1_Ratio_Pct'] = df['Tier1_Ratio_Pct'] - df['CET1_Ratio_Pct']
        else:
            df['AT1_Ratio_Pct'] = 0.0
            
    df['ROE_Pct'] = df.apply(lambda x: (x['Net_Income']/x['Tier1_Capital']*100) if x['Tier1_Capital']>0 else 0, axis=1)
    df['Risk_Density_Pct'] = df.apply(lambda x: (x['RWA_Final']/x['Total_Assets']*100) if x['Total_Assets']>0 else 0, axis=1)

    return df.replace([np.inf, -np.inf], 0).fillna(0)

df = load_data()
if df.empty: st.stop()

market_means = df.mean(numeric_only=True)

# --- 3. MOTEUR GRAPHIQUE ---
def plot_gauge(value, min_th, max_th, inverse=False):
    """Crée une jauge compacte sans titre (géré par st.metric)"""
    if inverse: # Plus c'est bas, mieux c'est
        colors = [
            {'range': [0, min_th], 'color': "#2ecc71"},       # Vert
            {'range': [min_th, max_th], 'color': "#f1c40f"}, # Orange
            {'range': [max_th, max(value*1.2, max_th*1.5)], 'color': "#e74c3c"}   # Rouge
        ]
    else: # Plus c'est haut, mieux c'est
        colors = [
            {'range': [0, min_th], 'color': "#e74c3c"},       # Rouge
            {'range': [min_th, max_th], 'color': "#f1c40f"}, # Orange
            {'range': [max_th, max(100, max_th*1.5, value*1.2)], 'color': "#2ecc71"} # Vert
        ]
        
    fig = go.Figure(go.Indicator(
        mode="gauge", # On retire "number" car affiché au dessus
        value=value,
        gauge={
            'axis': {'range': [0, max(value*1.2, max_th*1.2)]},
            'bar': {'color': "#2c3e50"},
            'steps': colors,
            'threshold': {'line': {'color': "black", 'width': 3}, 'thickness': 0.75, 'value': value}
        }
    ))
    # Marges ultra-fines pour coller au texte du dessus
    fig.update_layout(height=100, margin=dict(l=10, r=10, t=0, b=0))
    return fig

def card(col, val, title, txt, min_b, min_g, inverse=False):
    with col:
        # 1. Le Titre + Valeur + Tooltip interactif (i)
        st.metric(label=title, value=f"{val:.2f}%", help=txt)
        
        # 2. La Jauge visuelle juste en dessous
        st.plotly_chart(plot_gauge(val, min_b, min_g, inverse), use_container_width=True)

# --- 4. HEADER ---
c1, c2 = st.columns([3, 1])
with c1: 
    st.title("🏦 Solvency Control Room")
    st.caption("Tableau de bord de surveillance prudentielle (Bâle III / CRR)")
with c2: 
    bank_list = sorted(df['Bank_Label'].unique())
    selected_label = st.selectbox("Sélectionner la Banque", bank_list)

bank = df[df['Bank_Label'] == selected_label].iloc[0]

# --- 5. SOLVABILITÉ & COUSSINS ---
st.markdown("### 1. Solvabilité & Coussins de Capital")
c1, c2, c3, c4, c5 = st.columns(5)

card(c1, bank['CET1_Ratio_Pct'], "CET1 Ratio", 
     "Capital Dur (Actions). Le minimum réglementaire est de 4.5%.", 4.5, 9.0)

card(c2, bank['AT1_Ratio_Pct'], "Ratio AT1", 
     "Additional Tier 1 (Dettes hybrides/CoCos). Cible ~1.5%.", 1.0, 1.5)

card(c3, bank['TCR_Pct'], "Total Capital", 
     "Ratio Global de Solvabilité (Fonds Propres Totaux / RWA). Min 8%.", 8.0, 12.0)

card(c4, bank['Leverage_Ratio_Pct'], "Levier (LR)", 
     "Ratio de Levier (Bâle III). Tier 1 / Total Actif. Min 3.0%.", 3.0, 4.0)

card(c5, bank['Capital_Buffer_Pct'], "Coussin Dispo.", 
     "Excédent de Capital au-dessus des exigences SREP. Si négatif, la banque est en danger.", 0.0, 2.0)

# --- 6. EXIGENCES SREP ---
st.markdown("### 2. Exigences Réglementaires (SREP)")
s1, s2, s3, s4 = st.columns(4)

card(s1, bank['P2R_Pct'], "Pilier 2 (P2R)", 
     "Exigence spécifique imposée par le superviseur pour les risques propres à la banque.", 1.5, 2.5, inverse=True)

card(s2, bank['P2G_Pct'], "Guidance (P2G)", 
     "Recommandation non contraignante de capital supplémentaire.", 0.5, 1.5, inverse=True)

card(s3, bank['CBR_Pct'], "Buffer Combiné", 
     "Total des coussins obligatoires (Conservation + CCyB + Systémique).", 1.5, 3.5, inverse=False)

card(s4, bank['SREP_Requirement_Pct'], "Total SREP", 
     "Ligne Rouge : Niveau total de capital requis (P1 + P2R + CBR).", 9.0, 11.0, inverse=True)

# --- 7. LIQUIDITÉ & RISQUES ---
st.markdown("### 3. Liquidité, Résolution & Risques")
l1, l2, l3, l4, l5 = st.columns(5)

card(l1, bank['LCR_Ratio_Pct'], "LCR (30j)", 
     "Liquidity Coverage Ratio. Couverture des sorties nettes à 30j. Min 100%.", 100, 110)

card(l2, bank['NSFR_Ratio_Pct'], "NSFR (1an)", 
     "Net Stable Funding Ratio. Stabilité du financement à long terme. Min 100%.", 100, 110)

card(l3, bank['MREL_Ratio_Pct'], "MREL / TLAC", 
     "Capacité d'absorption des pertes (Bail-in). Cible ~20-25% RWA.", 18, 22)

card(l4, bank['Texas_Ratio_Pct'], "Texas Ratio", 
     "Vulnérabilité aux faillites. NPL / (Capital + Réserves). >100% est critique.", 50, 90, inverse=True)

card(l5, bank['NPL_Ratio_Pct'], "Taux NPL", 
     "Part des crédits douteux dans le portefeuille total.", 3.0, 5.0, inverse=True)

st.divider()

# --- 8. ANALYSE COMPARATIVE (GRAPHIQUES) ---
st.subheader("📊 Analyse Comparative Visuelle")
g_left, g_right = st.columns(2)

with g_left:
    st.markdown("**1. Matrice Stratégique (Rentabilité vs Risque)**")
    
    color_opt = "NSA" if "NSA" in df.columns else None
    fig_scat = px.scatter(
        df, x="Risk_Density_Pct", y="ROE_Pct",
        color=color_opt, size="Total_Assets",
        hover_name="Bank_Label",
        labels={"Risk_Density_Pct": "Risque (RWA/Actif %)", "ROE_Pct": "Rentabilité (ROE %)"}
    )
    # DIAMANT PRO
    fig_scat.add_trace(go.Scatter(
        x=[bank['Risk_Density_Pct']], y=[bank['ROE_Pct']],
        mode='markers', 
        marker=dict(color='#D32F2F', size=18, symbol='diamond', line=dict(width=2, color='white')),
        name="MA BANQUE"
    ))
    fig_scat.update_layout(title="Positionnement Concurrentiel")
    st.plotly_chart(fig_scat, use_container_width=True)
    
    st.info("""
    **Interprétation :**
    * **En haut à gauche** (Haute Renta, Faible Risque) : Position idéale.
    * **En bas à droite** (Faible Renta, Haut Risque) : Zone de danger.
    * **Taille de la bulle** : Représente la taille du bilan (Total Actifs).
    """)

with g_right:
    st.markdown("**2. Benchmark Liquidité vs Moyenne Marché**")
    
    cats = ['LCR', 'NSFR', 'LTD']
    vals_b = [bank['LCR_Ratio_Pct'], bank['NSFR_Ratio_Pct'], bank['LTD_Ratio_Pct']]
    vals_m = [market_means['LCR_Ratio_Pct'], market_means['NSFR_Ratio_Pct'], market_means['LTD_Ratio_Pct']]
    
    fig_bar = go.Figure(data=[
        go.Bar(name='Ma Banque', x=cats, y=vals_b, marker_color='#2c3e50'),
        go.Bar(name='Moyenne Marché', x=cats, y=vals_m, marker_color='#95a5a6')
    ])
    fig_bar.add_hline(y=100, line_dash="dash", line_color="red", annotation_text="Seuil Réglementaire (100%)")
    fig_bar.update_layout(barmode='group', title="Écart de Liquidité")
    st.plotly_chart(fig_bar, use_container_width=True)
    
    st.info("""
    **Lecture du Benchmark :**
    * La ligne rouge (100%) est le minimum vital pour le LCR et NSFR.
    * Si les barres bleues sont **sous la ligne rouge**, la liquidité est insuffisante.
    * Le LTD (Loan-to-Deposit) doit idéalement être proche ou inférieur à 100%.
    """)

st.divider()

# --- 7. CONCLUSION AUTOMATISÉE ---
st.subheader("🏁 Verdict de Solvabilité & Risques")

# Logique de scoring
verdict_color = "green"
verdict_title = "SITUATION SOLIDE"
messages = []

# 1. Check Capital
if bank['Capital_Buffer_Pct'] < 0:
    verdict_color = "red"
    verdict_title = "ALERTE : INFRACTION RÉGLEMENTAIRE"
    messages.append("❌ La banque ne respecte pas ses exigences SREP (Coussin négatif). Augmentation de capital requise.")
elif bank['Capital_Buffer_Pct'] < 1.0:
    verdict_color = "orange"
    verdict_title = "SITUATION TENDUE"
    messages.append("⚠️ Le coussin de capital est faible (<1%). Marge de manœuvre limitée.")
else:
    messages.append("✅ La solvabilité est robuste avec un coussin confortable au-dessus des exigences.")

# 2. Check Risque Crédit (Texas Ratio)
if bank['Texas_Ratio_Pct'] > 100:
    verdict_color = "red" if verdict_color != "red" else "red"
    verdict_title = "RISQUE CRITIQUE D'INSOLVABILITÉ" if verdict_title != "ALERTE" else verdict_title
    messages.append("❌ **Texas Ratio > 100%** : Les créances douteuses absorbent la totalité du capital tangible. Risque de faillite élevé.")
elif bank['Texas_Ratio_Pct'] > 80:
    messages.append("⚠️ La qualité du portefeuille est dégradée (Texas Ratio élevé).")

# 3. Check Liquidité
if bank['LCR_Ratio_Pct'] < 100 or bank['NSFR_Ratio_Pct'] < 100:
    messages.append("⚠️ **Alerte Liquidité** : Certains ratios de liquidité sont sous le seuil réglementaire de 100%.")

# Affichage
if verdict_color == "green":
    st.success(f"### {verdict_title}")
elif verdict_color == "orange":
    st.warning(f"### {verdict_title}")
else:
    st.error(f"### {verdict_title}")

for msg in messages:
    st.markdown(f"- {msg}")

st.markdown("""
<small>Note : Ce verdict est généré automatiquement par un algorithme basé sur les seuils de Bâle III. Il ne constitue pas un conseil en investissement.</small>
""", unsafe_allow_html=True)