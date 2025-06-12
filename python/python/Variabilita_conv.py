import pandas as pd

row = []
with open('volatilita.txt', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        key, value = line.split('\t')
        sector, year = key.split('#')
        fields = {'Sector': sector, 'Year': year}
        for campo in value.split(','):
            if '=' in campo:
                k, v = campo.split('=', 1)
                fields[k.strip()] = v.strip().replace('%', '')
            else:
                pass
        row.append(fields)

df = pd.DataFrame(row)
print(df[:10])
df.to_csv('volatilita.csv', index=False, columns=['Sector', 'Year', 'Volatilita(StdDev)', 'PrezzoMedio'])