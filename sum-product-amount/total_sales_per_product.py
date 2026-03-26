import pandas as pd

df = pd.read_csv("sales.csv")

result = df.groupby("product")["amount"].sum()

print(result)