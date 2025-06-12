import pandas as pd

row = []
with open('ticker_analisis_new.txt', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        key, value = line.split('\t')
        ticker, year = key.split('#')
        fields = {'Ticker': ticker, 'Year': year}
        for campo in value.split(','):
            if '=' in campo:
                k, v = campo.split('=', 1)
                fields[k.strip()] = v.strip().replace('%', '')
            else:
                pass
        row.append(fields)

df = pd.DataFrame(row)
print(df[:10])
df.to_csv('ticker_analisis.csv', index=False, columns=['Ticker', 'Year', 'Nome', 'Variazione_percentuale', 'Prezzo_Iniziale', 'Prezzo_Finale'])