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
