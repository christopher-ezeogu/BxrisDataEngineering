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
    claim_id character varying(50) COLLATE pg_catalog."default",
    patient_id integer,
    provider_id integer,
    diagnosis_code character varying(20) COLLATE pg_catalog."default",
    procedure_code character varying(50) COLLATE pg_catalog."default",
    amount numeric(18,2),
    service_date character varying(64) COLLATE pg_catalog."default",
    ingested_at timestamp without time zone DEFAULT now(),
	record_hash UUID DEFAULT gen_random_uuid()
)


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
)



-- verify load
SELECT * FROM staging.claims ORDER BY 1 ASC;

select gen_random_uuid() -- testing uuid generation

CREATE OR REPLACE FUNCTION etl.merge_claims(p_load_id varchar)
language plpgsql
as $$
BEGIN
	merge into prod.claims t
	using etl.stg_claims s
		on t.claim_id = s.claim_id
	when matched and t.record_hash <> s.record_hash then
	update set
		member_id = s.member_id,
		service_date = s.service_date,
		procedure_code = s.procedure_code,
		amount = s.amount,
		record_hash = s.record_hash,
		updated_at = getdate()
	when not matched then
	insert (
		claim_id,
		patient_id,
		provider_id,
		procedure_code,
		amount,
		service_date,
		ingested_at
	)
	values (
		s.claim_id,
		s.patient_id,
		s.service_date,
		s.procedure_code,
		s.amount,
		s.service_date,
		s.ingested_at
	);
end;
$$;





