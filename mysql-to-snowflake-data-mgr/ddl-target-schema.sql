CREATE SCHEMA IF NOT EXISTS etl;

-- SELECT * FROM etl.claims;

-- DROP TABLE IF EXISTS etl.claims;
CREATE TABLE IF NOT EXISTS etl.claims (
    claim_id        varchar(100) PRIMARY KEY,
    patient_id      varchar(100) NOT NULL,
    provider_id     varchar(100) NOT NULL,
    diagnosis_code  varchar(50),
    procedure_code  varchar(50) NOT NULL,
    amount          numeric(18,2) NOT NULL,
    service_date    date NOT NULL,
    ingested_at     timestamp NOT NULL DEFAULT current_timestamp,
    record_hash     varchar(64)
);


-- SELECT * FROM etl.stg_claims;

-- DROP TABLE IF EXISTS etl.stg_claims;
CREATE TABLE IF NOT EXISTS etl.stg_claims (
    load_id         varchar(100) NOT NULL,
    claim_id        varchar(100),
    patient_id      varchar(100),
    provider_id     varchar(100),
    diagnosis_code  varchar(50),
    procedure_code  varchar(50),
    amount          numeric(18,2),
    service_date    date,
    ingested_at     timestamp NOT NULL DEFAULT current_timestamp,
    record_hash     varchar(64)
);

CREATE INDEX IF NOT EXISTS idx_stg_claims_load_id
    ON etl.stg_claims(load_id);

CREATE INDEX IF NOT EXISTS idx_stg_claims_claim_id
    ON etl.stg_claims(claim_id);

ALTER TABLE etl.stg_claims
ADD COLUMN stg_claim_row_id bigserial;

ALTER TABLE etl.stg_claims
ADD CONSTRAINT stg_claims_pkey PRIMARY KEY (stg_claim_row_id);


-- SELECT * FROM etl.load_audit;

-- DROP TABLE IF EXISTS etl.load_audit;
CREATE TABLE IF NOT EXISTS etl.load_audit (
    audit_id                bigserial PRIMARY KEY,
    load_id                 varchar(100) NOT NULL,
    process_name            varchar(100) NOT NULL,
    source_rows_read        integer NOT NULL DEFAULT 0,
    source_rows_quarantined integer NOT NULL DEFAULT 0,
    staged_rows_loaded      integer NOT NULL DEFAULT 0,
    target_rows_rejected    integer NOT NULL DEFAULT 0,
    target_rows_upserted    integer NOT NULL DEFAULT 0,
    reconciliation_status   varchar(30) NOT NULL,
	status                  varchar(30) NOT NULL,
    error_message           text,
    updated_at              timestamp NOT NULL DEFAULT current_timestamp,
	started_at              timestamp NOT NULL DEFAULT current_timestamp,
    completed_at            timestamp
);

-- SELECT * FROM etl.source_quarantine;

-- DROP TABLE IF EXISTS etl.source_quarantine;
CREATE TABLE IF NOT EXISTS etl.source_quarantine (
	load_id           varchar(100) NOT NULL,
    claim_id          varchar(100),
    patient_id        varchar(100),
    provider_id       varchar(100),
    diagnosis_code    varchar(50),
    procedure_code    varchar(50),
    amount            numeric(18,2),
	service_date      date,
    rejection_reason  varchar(255) NOT NULL,
	source_row_payload text,
    quarantined_at    timestamp NOT NULL DEFAULT current_timestamp
);


-- SELECT * FROM etl.claims_rejects;

-- DROP TABLE IF EXISTS etl.claims_rejects;

CREATE TABLE IF NOT EXISTS etl.claims_rejects
(
    reject_id bigint NOT NULL DEFAULT nextval('etl.claims_rejects_reject_id_seq'::regclass),
    load_id character varying(100) COLLATE pg_catalog."default" NOT NULL,
    claim_id character varying(100) COLLATE pg_catalog."default",
    patient_id character varying(100) COLLATE pg_catalog."default",
    provider_id character varying(100) COLLATE pg_catalog."default",
    diagnosis_code character varying(50) COLLATE pg_catalog."default",
    procedure_code character varying(50) COLLATE pg_catalog."default",
    amount numeric(18,2),
    service_date date,
    ingested_at timestamp without time zone,
    record_hash character varying(64) COLLATE pg_catalog."default",
    rejection_reason character varying(255) COLLATE pg_catalog."default" NOT NULL,
    rejected_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT claims_rejects_pkey PRIMARY KEY (reject_id)
);

-- add column to ensure idempotency for rejected roles
ALTER TABLE etl.claims_rejects
ADD COLUMN stg_claim_row_id bigint;

-- Add a foreign key back to staging
ALTER TABLE etl.claims_rejects
ADD CONSTRAINT fk_claims_rejects_stg_claim_row
FOREIGN KEY (stg_claim_row_id)
REFERENCES etl.stg_claims (stg_claim_row_id);

-- Add unique constraint
ALTER TABLE etl.claims_rejects
ADD CONSTRAINT uq_claims_rejects_stage_reason
UNIQUE (stg_claim_row_id, rejection_reason);