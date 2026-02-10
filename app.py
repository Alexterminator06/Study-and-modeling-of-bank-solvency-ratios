import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(
    page_title="Solvency Engine Dashboard",
    page_icon="🏦",
    layout="wide"
)

# --- CHARGEMENT DES DONNÉES ---
@st.cache_data
def load_data():
    path = "data/processed/final_results_2025.csv"
    if not os.path.exists(path):
        st.error("Le fichier de résultats est introuvable. Veuillez lancer main.py d'abord.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    return df

df = load_data()

if df.empty:
    st.stop()

# --- SIDEBAR (FILTRES) ---
st.sidebar.header("🔍 Sélection")

# Filtre par Pays (NSA) si disponible, sinon on saute
if 'NSA' in df.columns:
    countries = sorted(df['NSA'].unique())
    selected_country = st.sidebar.selectbox("Pays (NSA)", ["Tous"] + countries)
    if selected_country != "Tous":
        df_filtered = df[df['NSA'] == selected_country]
    else:
        df_filtered = df
else:
    df_filtered = df

# Filtre par Banque (LEI)
lei_list = df_filtered['LEI'].unique()
selected_lei = st.sidebar.selectbox("Choisir une Banque (LEI)", lei_list)

# Récupération des données de la banque sélectionnée
bank = df[df['LEI'] == selected_lei].iloc[0]

# --- TITRE PRINCIPAL ---
st.title("🏦 Analyse de Solvabilité & Risques")
st.markdown(f"**Code LEI :** `{selected_lei}` | **Date :** {bank['Date']}")

st.divider()

# --- 1. KPIs MAJEURS (LIGNE DU HAUT) ---
col1, col2, col3, col4 = st.columns(4)

# Fonction pour mettre de la couleur si le ratio est bon/mauvais
def delta_color(val, threshold):
    return "normal" if val >= threshold else "inverse"

with col1:
    st.metric(
        label="🏆 CET1 Ratio",
        value=f"{bank['CET1_Ratio_Pct']:.2f} %",
        delta=f"{bank['CET1_Ratio_Pct'] - 4.5:.2f} % vs Min (4.5%)",
        delta_color=delta_color(bank['CET1_Ratio_Pct'], 4.5)
    )

with col2:
    st.metric(
        label="🛡️ Total Capital Ratio",
        value=f"{bank['TCR_Pct']:.2f} %",
        delta=f"{bank['TCR_Pct'] - 8.0:.2f} % vs Min (8%)",
        delta_color=delta_color(bank['TCR_Pct'], 8.0)
    )

with col3:
    st.metric(
        label="⚖️ Leverage Ratio",
        value=f"{bank['Leverage_Ratio_Pct']:.2f} %",
        delta=f"{bank['Leverage_Ratio_Pct'] - 3.0:.2f} % vs Min (3%)",
        delta_color=delta_color(bank['Leverage_Ratio_Pct'], 3.0)
    )

with col4:
    # Texas Ratio : Plus c'est bas, mieux c'est. Donc logique inversée.
    # Si la colonne n'existe pas encore, on met 0 par défaut
    texas = bank.get('Texas_Ratio_Pct', 0)
    st.metric(
        label="🤠 Texas Ratio (Risque)",
        value=f"{texas:.2f} %",
        delta="⚠️ Attention" if texas > 100 else "✅ Sain",
        delta_color="off" # On gère le texte manuellement
    )

st.divider()

# --- 2. GRAPHIQUES DÉTAILLÉS ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("📊 Structure du Capital vs RWA")
    # Comparaison Capital vs RWA
    fig_cap = go.Figure(data=[
        go.Bar(name='CET1', x=['Capital'], y=[bank['CET1_Capital']], marker_color='#2ecc71'),
        go.Bar(name='AT1 + T2', x=['Capital'], y=[bank['Total_Capital'] - bank['CET1_Capital']], marker_color='#f1c40f'),
        go.Bar(name='RWA Total', x=['Exposition (RWA)'], y=[bank['RWA_Final']], marker_color='#e74c3c')
    ])
    fig_cap.update_layout(barmode='stack', title="Capitaux Propres vs Risques Pondérés")
    st.plotly_chart(fig_cap, use_container_width=True)

with c2:
    st.subheader("📉 Impact CVA & NPL")
    # Pie chart ou Bar chart pour montrer les composantes du risque
    
    # Données pour le graph
    rwa_base = bank['RWA_Final'] - (bank['CVA_Charge'] / 0.08) # On retrouve le RWA hors CVA
    rwa_cva = bank['CVA_Charge'] / 0.08
    
    values = [rwa_base, rwa_cva]
    labels = ["RWA Classique (Crédit/Marché)", "Surcharge CVA (Dérivés)"]
    
    fig_risk = px.pie(values=values, names=labels, hole=0.4, title="Impact du Module CVA sur les RWA")
    st.plotly_chart(fig_risk, use_container_width=True)

# --- 3. POSITIONNEMENT DE LA BANQUE (SCATTER PLOT) ---
# --- 3. POSITIONNEMENT DE LA BANQUE (SCATTER PLOT) ---
st.subheader("📍 Positionnement vs Marché")
st.markdown("Où se situe cette banque par rapport aux autres (Solvabilité vs Qualité d'actifs) ?")

# 1. On calcule le ratio pour TOUT le monde (dans df)
if 'Loans_Gross' in df.columns and 'NPL_Amount' in df.columns:
    # On évite la division par zéro
    df['Calc_NPL_Ratio'] = df.apply(lambda x: (x['NPL_Amount'] / x['Loans_Gross'] * 100) if x['Loans_Gross'] > 0 else 0, axis=1)
else:
    df['Calc_NPL_Ratio'] = 0

# 2. CRUCIAL : On récupère la valeur pour la banque sélectionnée APRES le calcul
# On va chercher la valeur directement dans le df mis à jour
bank_npl_val = df.loc[df['LEI'] == selected_lei, 'Calc_NPL_Ratio'].values[0]

fig_scatter = px.scatter(
    df, 
    x="Calc_NPL_Ratio", 
    y="CET1_Ratio_Pct",
    color="NSA" if "NSA" in df.columns else None,
    hover_data=['LEI', 'Total_Assets'],
    title="Carte des Risques : Solvabilité vs Crédits Douteux",
    labels={"Calc_NPL_Ratio": "Ratio NPL (%) (Plus bas est mieux)", "CET1_Ratio_Pct": "CET1 Ratio (%) (Plus haut est mieux)"}
)

# Mettre en évidence la banque sélectionnée
fig_scatter.add_trace(
    go.Scatter(
        x=[bank_npl_val],  # <--- ON UTILISE LA NOUVELLE VALEUR ICI
        y=[bank['CET1_Ratio_Pct']],
        mode='markers',
        marker=dict(color='red', size=15, symbol='star'),
        name=f"Banque Sélectionnée"
    )
)

st.plotly_chart(fig_scatter, use_container_width=True)