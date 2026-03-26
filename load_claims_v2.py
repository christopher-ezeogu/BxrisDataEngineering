"""
claims_v2.csv file contains the following issues that should be cleaned before getting loaded 

| Issue injected                 | Purpose                                      |
| ------------------------------ | -------------------------------------------- |
| Duplicate `claim_id` (~2%)     | Test idempotent loads / primary key checks   |
| Negative `claim_amount` (~2%)  | Test financial validation rules              |
| Future `service_date` (~2%)    | Test date sanity validation                  |
| Invalid `procedure_code` (~2%) | Test reference code validation               |
| Valid rows                     | Ensure pipeline still processes correct data |


duplicates
invalid reference codes
future dates
negative financial values
schema drift


invalid_negative_amount = df[df["claim_amount"] < 0]

invalid_future_date = df[df["service_date"] > pd.Timestamp.today()]

duplicate_claims = df[df.duplicated("claim_id", keep=False)]

invalid_procedure = df[~df["procedure_code"].isin(valid_cpt_codes)]


"""
import pandas as pd

valid_cpt_codes = ['71020', '99213', '93000', '80050', '36415']

def load_claims_v2(filename):
    df = pd.read_csv(filename)
    #print(df)

    # invalid_future_date
    df["service_date"] = pd.to_datetime(df["service_date"], errors="coerce")
    invalid_future_date = df[df["service_date"] > pd.Timestamp.today()]
    print(len(invalid_future_date) , ' invalid futurist date')
    
    # fetch duplicate claims
    duplicate_claims = df[df.duplicated("claim_id", keep=False)]
    print(len(duplicate_claims) , ' duplicate claims')

    # invalid procedure
    invalid_procedure = df[~df["procedure_code"].isin(valid_cpt_codes)]
    print(len(invalid_procedure) , ' invalid procedure code')

load_claims_v2("claims_v2.csv")    