import pandas as pd
import os

file_path = os.getcwd()

REQUIRED_FIELD = ["service_date"]

df = pd.read_csv(file_path + "/claims.csv")

# convert to datetime
df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")

# remove missing dates
valid = df.dropna(subset=REQUIRED_FIELD)

# filter records from 2025
records_from_2025 = valid[valid["service_date"].dt.year == 2025]

print(records_from_2025.head(10))