CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.claims (
    claim_id        varchar(100) PRIMARY KEY,
    patient_id      varchar(100) NOT NULL,
    provider_id     varchar(100) NOT NULL,
    diagnosis_code  varchar(50),
    procedure_code  varchar(50) NOT NULL,
    amount          numeric(18,2) NOT NULL,
    service_date    date NOT NULL,
    ingested_at     timestamp NOT NULL DEFAULT current_timestamp,
    record_hash     varchar(64) NOT NULL
);

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
    record_hash     varchar(64) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_claims_load_id
    ON etl.stg_claims(load_id);

CREATE INDEX IF NOT EXISTS idx_stg_claims_claim_id
    ON etl.stg_claims(claim_id);

CREATE TABLE IF NOT EXISTS etl.claims_rejects (
    reject_id         bigserial PRIMARY KEY,
    load_id           varchar(100) NOT NULL,
    claim_id          varchar(100),
    patient_id        varchar(100),
    provider_id       varchar(100),
    diagnosis_code    varchar(50),
    procedure_code    varchar(50),
    amount            numeric(18,2),
    service_date      date,
    ingested_at       timestamp,
    record_hash       varchar(64),
    rejection_reason  varchar(255) NOT NULL,
    rejected_at       timestamp NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS etl.load_audit (
    audit_id                bigserial PRIMARY KEY,
    load_id                 varchar(100) NOT NULL,
    process_name            varchar(100) NOT NULL,
    source_rows_read        integer NOT NULL DEFAULT 0,
    source_rows_rejected    integer NOT NULL DEFAULT 0,
    staged_rows_loaded      integer NOT NULL DEFAULT 0,
    target_rows_rejected    integer NOT NULL DEFAULT 0,
    target_rows_upserted    integer NOT NULL DEFAULT 0,
    status                  varchar(30) NOT NULL,
    error_message           text,
    started_at              timestamp NOT NULL DEFAULT current_timestamp,
    completed_at            timestamp
);