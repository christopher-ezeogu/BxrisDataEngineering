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