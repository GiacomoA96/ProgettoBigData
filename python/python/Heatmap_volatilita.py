import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Carica i dati
df = pd.read_csv('volatilita.csv')

# Calcola la media per ogni coppia (Sector, Year)
df_grouped = df.groupby(['Sector', 'Year'], as_index=False)['Volatilita(StdDev)'].mean()

# Andiamo ad impostare i settori sulle righe e gli anni sulle colonne
heatmap_data = df_grouped.pivot(index='Sector', columns='Year', values='Volatilita(StdDev)')

plt.figure(figsize=(16, 10))
sns.heatmap(heatmap_data, cmap='YlGnBu', linewidths=0.5, linecolor='gray')
plt.title('Heatmap Volatilità (StdDev) per Settore e Anno')
plt.xlabel('Anno')
plt.ylabel('Settore')
plt.tight_layout()
plt.show()

df = pd.read_csv('trend_settoriale.csv')

# Se ci sono duplicati, calcola la media per ogni coppia (Sector, Year)
df_grouped = df.groupby(['Sector', 'Year'], as_index=False)['VariazionePercentuale'].mean()

# Pivot per avere settori sulle righe e anni sulle colonne
heatmap_data = df_grouped.pivot(index='Sector', columns='Year', values='VariazionePercentuale')

plt.figure(figsize=(16, 10))
sns.heatmap(heatmap_data, cmap='RdYlGn', linewidths=0.5, linecolor='gray')
plt.title('Heatmap Variazione Percentuale per Settore e Anno')
plt.xlabel('Anno')
plt.ylabel('Settore')
plt.tight_layout()
plt.show()

df = pd.read_csv('ticker_analisis.csv')
anno = 2009
top = df[df['Year'] == anno].sort_values('Variazione_percentuale', ascending=False).head(10)
plt.bar(top['Nome'], top['Variazione_percentuale'])
plt.xticks(rotation=90)
plt.title(f'Top 10 titoli per crescita percentuale nel {anno}')
plt.ylabel('Variazione percentuale')
plt.show()