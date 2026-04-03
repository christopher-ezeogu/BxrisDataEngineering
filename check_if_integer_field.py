import re
import pandas as pd

def is_valid_integer(val):
    if pd.isna(val):
        return False
    return bool(re.fullmatch(r"\d+", str(val).strip()))

def check_valid_integer():

    mask_valid = df["provider_id"].apply(is_valid_integer)

    bad_rows = df[~mask_valid]
    clean_df = df[mask_valid].copy()

    clean_df["provider_id"] = clean_df["provider_id"].astype("Int64")

    # apply to all integer field
    int_columns = ["claim_id", "patient_id", "provider_id", "procedure_code"]

    for col in int_columns:
        mask_valid = df[col].astype(str).str.strip().str.match(r"^\d+$")

        bad_rows = pd.concat([bad_rows, df[~mask_valid]])
        df = df[mask_valid]

        df[col] = df[col].astype("Int64")