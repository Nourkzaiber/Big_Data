import pandas as pd
import streamlit as st
import plotly.colors as colors
import numpy as np
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from sklearn.cluster import KMeans
import plotly.express as px

# Streamlit-App initialisieren
st.set_page_config(layout="wide")
st.set_option('deprecation.showPyplotGlobalUse', False)  # Deaktiviere die Warnung für st.pyplot()

# Titel "Dashboard Gruppe B" auf der linken Seite anzeigen
st.sidebar.title("Dashboard Yelb dataset")

# Mitglieder anzeigen
st.sidebar.subheader("Mitglieder:")
st.sidebar.write("Nour-Eddine Kzaiber")

# Daten für den Plot
data = pd.read_csv('user_count_by_year.csv', header=None, names=['years', 'user_counts'])

# Vorhersagedaten
raw_data = pd.read_csv('2_result_prognose_für_nächste_5_Jahren.csv'
                       '', skiprows=range(0, 3), nrows=5,
                       encoding='ISO-8859-1', sep='|', header=None, names=['u', 'years', 'user_counts', 'v'])
prediction_data = raw_data[['years', 'user_counts']]

# Jahr auswählen
selected_data_year = st.sidebar.slider('Jahr auswählen (Daten)', min_value=min(data['years']),
                                       max_value=max(data['years']), value=min(data['years']), step=1)
selected_prediction_year = st.sidebar.slider('Jahr auswählen (Vorhersagen)',
                                             min_value=min(prediction_data['years']),
                                             max_value=max(prediction_data['years']),
                                             value=min(prediction_data['years']), step=1)

# Daten für den Plot filtern
data_plot_data = data[data['years'] <= selected_data_year]
prediction_plot_data = prediction_data[prediction_data['years'] <= selected_prediction_year]

# Erzeuge den Daten-Plot
data_fig = go.Figure()
data_fig.add_trace(
    go.Scatter(x=data_plot_data['years'], y=data_plot_data['user_counts'], mode='markers+lines', marker=dict(size=6)))
data_fig.update_layout(title='Nutzerzahlen pro Jahr (Daten)', xaxis_title='Jahr', yaxis_title='Anzahl der Nutzer')

# Erzeuge den Vorhersage-Plot
prediction_fig = go.Figure()
prediction_fig.add_trace(
    go.Scatter(x=prediction_plot_data['years'], y=prediction_plot_data['user_counts'], mode='markers+lines',
               marker=dict(size=6)))
prediction_fig.update_layout(title='Nutzerzahlen pro Jahr (Vorhersagen)', xaxis_title='Jahr',
                             yaxis_title='Anzahl der Nutzer')

# Plots nebeneinander anzeigen
col1, col2 = st.columns(2)
col1.plotly_chart(data_fig, use_container_width=True)
col2.plotly_chart(prediction_fig, use_container_width=True)

################################################################################
# Daten für den ersten Plot
raw_data = pd.read_csv('4_spark_result_best_bewertete_state.csv', skiprows=range(0, 3), nrows=14, encoding='ISO-8859-1', sep='|', header=None, names=['state', 'stars', 'count', 'u', ])
raw_data = raw_data.reset_index(drop=True)
data1 = raw_data[['state', 'stars']]

# Daten für den zweiten Plot
data2 = raw_data[['state', 'count']]

# Farben für die Balken
color_scale = colors.sequential.Viridis

# Erzeuge eine neue Abbildung mit angepasster Größe
plt.figure(figsize=(12, 4))

# Erzeuge den Linienplot für den ersten Plot
plt.subplot(1, 2, 1)
plt.plot(data1['state'], data1['stars'], marker='o', label='Linienplot')
plt.bar(data1['state'], data1['stars'], color=color_scale, alpha=0.5, label='Balken')
plt.xlabel('state')
plt.ylabel('Anzahl der stars')
plt.title('best bewertete state')
plt.legend()

# Erzeuge den Linienplot für den zweiten Plot
plt.subplot(1, 2, 2)
plt.plot(data2['state'], data2['count'], marker='o', label='Linienplot')
plt.bar(data2['state'], data2['count'], color=color_scale, alpha=0.5, label='Balken')
plt.xlabel('state')
plt.ylabel('Anzahl der count')
plt.title('best bewertete state')
plt.legend()

# Anzeige der Plots
st.pyplot(plt)

################################################################################

# Daten aus der CSV-Datei "4_spark_result_best_bewertete_state.csv" lesen
df = pd.read_csv('4_spark_result_best_bewertete_state.csv', skiprows=range(0, 3), nrows=14, encoding='ISO-8859-1',
                       sep='|', header=None, names=['state', 'stars', 'count', 'u', ])

# Seitenspalte erstellen
st.sidebar.title("Wähle deine Variablen aus")

# Dropdown-Auswahl für Variable A
a = st.sidebar.selectbox("Variable A", list(df.columns)[1:3])

# Dropdown-Auswahl für Variable B
b = st.sidebar.selectbox("Variable B", list(df.columns)[1:3], index=1)

# Slider für die Anzahl der Centroids
kmeans_slider = st.sidebar.slider(
    "Wähle die Anzahl der Centroids", min_value=2, max_value=8
)

# Warnung anzeigen, wenn Variable A und Variable B identisch sind
if a == b:
    st.sidebar.warning("Variablen sollten nicht identisch sein")

# K-Means-Clustering durchführen und Clusterzuordnung hinzufügen
df["cluster"] = KMeans(n_clusters=kmeans_slider, random_state=0).fit_predict(
    df.loc[:, (a, b)]
)

# Gruppengrößen berechnen
groupsize = df.groupby("cluster").size().reset_index().rename({0: "size"}, axis=1)

# Streamlit-Spalten erstellen
col1, col2 = st.columns(2)

# Erstellen einer benutzerdefinierten Farbsequenz für jeden Cluster
color_sequence = px.colors.qualitative.Pastel

# Scatterplot mit Plotly Express erstellen und alternative Farbpalette verwenden
fig = px.scatter(df, x=a, y=b, color="cluster", hover_data=['state'],
                 title='Cluster Analyse best bewertete state',
                 color_continuous_scale='Viridis')

# Plot im Streamlit anzeigen
col1.plotly_chart(fig, use_container_width=True)

#############################################################################

# Daten aus der CSV-Datei "6_result_hive_geschlossne_geschäfte.csv" lesen
dataset = pd.read_csv('6_result_hive_geschlossne_geschäfte.csv', header=None, names=['code', 'count'])

# Farbskala definieren
color_scale = colors.sequential.Viridis

# Erstellen des Daten-Objekts für den Choropleth-Plot mit der angepassten Farbskala
data = dict(
    type='choropleth',
    locations=dataset['code'],
    z=dataset['count'],
    locationmode='USA-states',
    text=dataset['code'],
    marker=dict(line=dict(color='rgb(0,0,0)')),
    colorscale=color_scale  # Verwendung der angepassten Farbskala
)
# Erstellen des Layout-Objekts für den Choropleth-Plot
layout = dict(
    title='Geschlossene Geschäfte in den USA',  # Titel für den Plot
    geo=dict(
        scope='usa',  # Festlegen des Umfangs auf USA für die Karte
        showlakes=True,  # Anzeigen der Seen auf der Karte
        lakecolor='rgb(85,173,240)'  # Festlegen der Farbe der Seen auf Hellblau
    )
)

# Erstellen des Choropleth-Plots mit den Daten und dem Layout
choromap = go.Figure(data=[data], layout=layout)

# Anzeigen des Plots in Streamlit
col2.plotly_chart(choromap, use_container_width=True)

#############################################################################

# Erstellen Sie eine Word Cloud
with open('9_result_mapreduce_top_50_worte.txt', 'r') as file:
    word_counts = file.readlines()

wordcloud_text = ' '.join(word_counts)
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(wordcloud_text)

plt.figure(figsize=(5, 3))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('top 50 worte')
st.pyplot(plt)

################################################################################

# Plot aus "3_result_hive_aktivsten_benutzer.csv" einfügen
# Daten aus der CSV-Datei lesen
data1 = pd.read_csv('top_aktivsten_Benutzer.csv', sep=',', names=['user_id', 'name', 'review_count'])

# Koordinaten für die Kreise generieren
data1['x'] = pd.Series(np.random.rand(len(data1)))
data1['y'] = pd.Series(np.random.rand(len(data1)))

# Plot erstellen
fig = px.scatter(data1, x='x', y='y', size='review_count', color='name',
                 color_discrete_sequence=px.colors.qualitative.T10,
                 hover_name='name', hover_data=['review_count'],
                 labels={'review_count': 'Review Count'},
                 title='Kreisdiagramm top aktivsten Benutzer',
                 size_max=50,  # Größe der Kreise anpassen
                 text='name')  # Name im Kreis anzeigen

# Hovertemplate anpassen
fig.update_traces(hovertemplate='<b>%{hovertext}</b><br>review_count: %{marker.size}')

# Achsenbeschriftungen entfernen
fig.update_layout(xaxis_visible=False, yaxis_visible=False)

# Plot im Streamlit anzeigen
st.plotly_chart(fig)

############################################################################################