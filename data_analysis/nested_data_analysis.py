data = [
    {
        "user_id": 1,
        "transactions": [
            {"amount": 100, "status": "completed"},
            {"amount": -50, "status": "failed"},
            {"amount": 200, "status": "completed"}
        ]
    },
    {
        "user_id": 2,
        "transactions": [
            {"amount": 300, "status": "completed"},
            {"amount": None, "status": "completed"}
        ]
    },
    {
        "user_id": 1,
        "transactions": [
            {"amount": 50, "status": "completed"}
        ]
    }
]


import pandas as pd

# 1. flatten the structure

rows = []

for entry in data:
    user_id = entry["user_id"]
    for txn in entry["transactions"]:
        rows.append({
            "user_id": user_id,
            "amount": txn["amount"],
            "status": txn["status"]
        })

df = pd.DataFrame(rows)
# print(df)

# 2. clean + filter ( remove negative amount and null values)
df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

df_clean = df[
    (df["status"] == "completed") &
    (df["amount"] > 0)
]
#print(df_clean)

# 3. aggregate and sort 
result = (
    df_clean
    .groupby("user_id", as_index=False)["amount"]
    .sum()
    .sort_values(by="amount", ascending=True)
)

print(result)