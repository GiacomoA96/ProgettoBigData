import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('volatilita.csv')
for sector in df['Sector'].unique():
    data = df[df['Sector'] == sector]
    plt.plot(data['Year'], data['Volatilita(StdDev)'], label=sector)
plt.xlabel('Anno')
plt.ylabel('Volatilità')
plt.title('Volatilità per settore nel tempo')
plt.legend()
plt.show()