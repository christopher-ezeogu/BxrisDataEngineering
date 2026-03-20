import pandas as pd

df = pd.read_csv("people.csv")

df_clean = df.dropna(subset=["age"])

print(df_clean)