-- CREATE SCHEMA internal;
-- CREATE SCHEMA pgcrypto;
-- CREATE SCHEMA staging;
-- CREATE SCHEMA etl;

CREATE USER appuser WITH PASSWORD 'testPwd@1';

CREATE ROLE app_dev_rw;

GRANT CONNECT ON DATABASE lucentis TO app_dev_rw;

GRANT USAGE, CREATE ON SCHEMA internal TO app_dev_rw;
GRANT USAGE, CREATE ON SCHEMA pgcrypto TO app_dev_rw;
GRANT USAGE, CREATE ON SCHEMA public TO app_dev_rw;
GRANT USAGE, CREATE ON SCHEMA staging TO app_dev_rw;
GRANT USAGE, CREATE ON SCHEMA etl TO app_dev_rw;


GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA internal TO app_dev_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgcrypto TO app_dev_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_dev_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO app_dev_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA etl TO app_dev_rw;

GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA internal, pgcrypto, public, staging, etl TO app_dev_rw;

GRANT app_dev_rw TO appuser;



SET ROLE app_dev_rw;

-- DROP TABLE IF EXISTS etl.stg_claims;

CREATE TABLE IF NOT EXISTS etl.stg_claims
(
    load_id character varying(50), 
	claim_id character varying(50) COLLATE pg_catalog."default",
    patient_id integer,
    provider_id integer,
    diagnosis_code character varying(20) COLLATE pg_catalog."default",
    procedure_code character varying(50) COLLATE pg_catalog."default",
    amount numeric(18,2),
    service_date character varying(64) COLLATE pg_catalog."default",
    ingested_at timestamp without time zone DEFAULT now(),
	record_hash UUID DEFAULT gen_random_uuid()
);

CREATE INDEX IF NOT EXISTS idx_stg_claims_load_id
    ON etl.stg_claims(load_id);

CREATE INDEX IF NOT EXISTS idx_stg_claims_claim_id
    ON etl.stg_claims(claim_id);

-- DROP TABLE IF EXISTS etl.claims;

CREATE TABLE IF NOT EXISTS etl.claims
(
    claim_id character varying(50) PRIMARY KEY COLLATE pg_catalog."default",
    patient_id integer,
    provider_id integer,
    diagnosis_code character varying(20) COLLATE pg_catalog."default",
    procedure_code character varying(50) COLLATE pg_catalog."default",
    amount numeric(18,2),
    service_date character varying(64) COLLATE pg_catalog."default",
    ingested_at timestamp without time zone DEFAULT now(),
	record_hash UUID DEFAULT gen_random_uuid()
);



-- verify load
SELECT * FROM staging.claims ORDER BY 1 ASC;

select gen_random_uuid() -- testing uuid generation

--load_id '20260329225932_5c6e3d09'

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
            s.record_hash,
            ROW_NUMBER() OVER (
                PARTITION BY s.claim_id
                ORDER BY s.ingested_at DESC, s.record_hash DESC
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
          AND s.service_date::date <= CURRENT_DATE
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
    WHERE etl.claims.record_hash <> EXCLUDED.record_hash;

    GET DIAGNOSTICS v_target_rows_upserted = ROW_COUNT;

    RETURN v_target_rows_upserted;
END;
$$;





