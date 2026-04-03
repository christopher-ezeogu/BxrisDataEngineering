CREATE TABLE IF NOT EXISTS etl.stg_claims
(
    load_id         varchar(50) NOT NULL,
    claim_id        varchar(50) NOT NULL,
    patient_id      integer,
    provider_id     integer,
    diagnosis_code  varchar(20),
    procedure_code  varchar(50),
    amount          numeric(18,2),
    service_date    date,
    ingested_at     timestamp without time zone NOT NULL DEFAULT now(),
    record_hash     text
);

CREATE TABLE IF NOT EXISTS etl.claims
(
    claim_id        varchar(50) PRIMARY KEY,
    patient_id      integer,
    provider_id     integer,
    diagnosis_code  varchar(20),
    procedure_code  varchar(50),
    amount          numeric(18,2),
    service_date    date,
    ingested_at     timestamp without time zone NOT NULL DEFAULT now(),
    record_hash     text NOT NULL
);

ALTER TABLE etl.stg_claims
ADD CONSTRAINT uq_stg_claims_load_claim
UNIQUE (load_id, claim_id);


CREATE OR REPLACE FUNCTION etl.merge_claims(i_load_id varchar)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_rows_upserted integer := 0;
BEGIN
    WITH ranked_rows AS (
        SELECT
            s.claim_id,
            s.patient_id,
            s.provider_id,
            s.diagnosis_code,
            s.procedure_code,
            s.amount,
            s.service_date,
            s.ingested_at,
            md5(
                concat_ws('|',
                    s.claim_id,
                    s.patient_id,
                    s.provider_id,
                    s.diagnosis_code,
                    s.procedure_code,
                    s.amount,
                    s.service_date
                )
            ) AS record_hash,
            ROW_NUMBER() OVER (
                PARTITION BY s.claim_id
                ORDER BY s.ingested_at DESC
            ) AS row_num
        FROM etl.stg_claims s
        WHERE s.load_id = i_load_id
          AND s.claim_id IS NOT NULL
          AND s.patient_id IS NOT NULL
          AND s.provider_id IS NOT NULL
          AND s.procedure_code IS NOT NULL
          AND s.amount IS NOT NULL
          AND s.amount >= 0
          AND s.service_date IS NOT NULL
          AND s.service_date <= CURRENT_DATE
    ),
    final_rows AS (
        SELECT
            claim_id,
            patient_id,
            provider_id,
            diagnosis_code,
            procedure_code,
            amount,
            service_date,
            ingested_at,
            record_hash
        FROM ranked_rows
        WHERE row_num = 1
    )
    INSERT INTO etl.claims (
        claim_id,
        patient_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        amount,
        service_date,
        ingested_at,
        record_hash
    )
    SELECT
        claim_id,
        patient_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        amount,
        service_date,
        CURRENT_TIMESTAMP,
        record_hash
    FROM final_rows
    ON CONFLICT (claim_id)
    DO UPDATE
    SET
        patient_id     = EXCLUDED.patient_id,
        provider_id    = EXCLUDED.provider_id,
        diagnosis_code = EXCLUDED.diagnosis_code,
        procedure_code = EXCLUDED.procedure_code,
        amount         = EXCLUDED.amount,
        service_date   = EXCLUDED.service_date,
        ingested_at    = CURRENT_TIMESTAMP,
        record_hash    = EXCLUDED.record_hash
    WHERE etl.claims.record_hash IS DISTINCT FROM EXCLUDED.record_hash;

    GET DIAGNOSTICS v_target_rows_upserted = ROW_COUNT;

    RETURN v_target_rows_upserted;
END;
$$;