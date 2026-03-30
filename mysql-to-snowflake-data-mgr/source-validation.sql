-- elimating duplicate claims (fetch claims with latest "ingested_at" date)

WITH duplicate_claims AS (
    SELECT 
		claim_id,
    	patient_id,
    	provider_id,
    	diagnosis_code,
    	procedure_code,
    	amount,
    	service_date,
    	ingested_at,
	row_number() over (PARTITION BY claim_id ORDER BY ingested_at DESC) AS row_num
    FROM etl.stg_claims
)
SELECT
    claim_id,
    patient_id,
    provider_id,
    diagnosis_code,
    procedure_code,
    amount,
    service_date,
    ingested_at
FROM duplicate_claims
WHERE row_num = 1;


-- fetch duplicate claims for audit purpose