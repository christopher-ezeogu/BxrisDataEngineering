"""
Return:
1. total amount per user
2. ignore negative values
3. sort users by total descending
"""

data =[
  {"user_id": 1, "amount": 100},
  {"user_id": 1, "amount": -50},
  {"user_id": 2, "amount": 200},
  {"user_id": 1, "amount": 100},
]

from collections import defaultdict

totals = defaultdict(int)
for row in data:
    if row["amount"] > 0:
        totals[row["user_id"]] += row["amount"]

result = sorted(totals.items(), key=lambda x: x[1], reverse=True)
#print(totals) # defaultdict(<class 'int'>, {1: 200, 2: 200})

print(result) # [(1, 200), (2, 200)]



########## using pandas ###################

import pandas as pd
import json

data = [
    {"user_id": 1, "amount": 100},
    {"user_id": 1, "amount": -50},
    {"user_id": 2, "amount": 200},
    {"user_id": 1, "amount": 100},
]

df = pd.DataFrame(data)
# print(df)

result = (
    df[df["amount"] > 0]                      # filter positive amounts
    .groupby("user_id", as_index=False)["amount"].sum()
    .sort_values(by="amount", ascending=False)
)
#df.groupby("user_id")["amount"].sum()
print(result)

# display result as json
json_str = result.to_json(orient="records", indent=2) 
print(json_str)

# to list of dict
dict_result = result.to_dict(orient="records")
print(dict_result)

# using json dumps
json_result = result.to_dict(orient="records")
print(json.dumps(json_result, indent=4))