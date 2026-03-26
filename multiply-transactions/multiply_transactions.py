import pandas as pd

df = pd.read_csv("transactions.csv")

df["total"] = df["price"] * df["quantity"]

print(df[["id", "total"]])