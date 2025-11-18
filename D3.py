import pandas as pd
import pandasql as psql

# Чтение CSV файлов
file_path = r"C:\Users\Admin\Downloads\prolongations.csv"
df = pd.read_csv(file_path)

file_path2 = r"C:\Users\Admin\Downloads\financial_data.csv"
df2 = pd.read_csv(file_path2)

def clean_number(x):
    if isinstance(x, str):
        # Заменяем пробелы и запятые для числового формата
        cleaned = x.replace(' ', '').replace(',', '.')
        try:
            return float(cleaned)
        except:
            return 0.0
    return x

df2['Ноябрь 2022'] = df2['Ноябрь 2022'].apply(clean_number)

query = """
SELECT
    sum(`Ноябрь 2022`), Account
FROM
    df2
group by account
"""

result = psql.sqldf(query)

print(result)


