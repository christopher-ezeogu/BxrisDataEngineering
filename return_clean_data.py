import pandas as pd

REQUIRED_FIELDS = ["patient_id", "name", "dob"]

def process_patient_file(path):

    df = pd.read_csv(path)

    # Identify invalid records
    invalid = df[df[REQUIRED_FIELDS].isnull().any(axis=1)]

    # Keep only valid records
    valid = df.dropna(subset=REQUIRED_FIELDS)

    # Deduplicate by patient_id keeping most recent update
    valid = (
        valid.sort_values("updated_at")
        .drop_duplicates(subset="patient_id", keep="last")
    )
    print(valid.to_dict("records"), invalid.to_dict("records"))
    #return valid.to_dict("records"), invalid.to_dict("records")

#test code
process_patient_file("raw_data.csv")