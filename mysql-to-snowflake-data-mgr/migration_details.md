# -- TEST DATA --
It includes 50,000 records with:
duplicate claim_id values
missing claim_id
missing patient_id
missing provider_id
missing diagnosis_code
valid, invalid, and missing procedure_code
negative amount
bad and future service_date values
One thing to note: your table has amount numeric(18,2) NOT NULL, so rows with missing amount were not included since that would fail immediately on insert.

# SQL Query - for data sorting and maintaining idempotency 
- sorts and persist bad records(invalid field details) - "etl.claims_rejects"
- sort and persist duplicate rocords - "etl.claims_rejects"
- upsert valid records -  valid claim amount, service_date & all required rows present
- audit records rejected & records upserted.

# Python Script
- define configuration
- logging
- connections
- helpers - generate load_id 
- process claims
    - process source data in chunks
    - normalize the column data types
    - split normalized data to account for valid and invalid data
    - 

Stronger version if you want this production-grade
You should also consider:
- chunked extraction from MySQL instead of pulling everything into memory
- quarantine of bad source rows before COPY
- schema validation before staging
- structured audit table for each load_id
- row count reconciliation between source, staging, and merged target

Your current code can work after these fixes, but it is still more of a small-to-medium batch script than a hardened production pipeline.
If you want, I can give you a production-standard version with chunked reads, quarantine handling, and row reconciliation.
