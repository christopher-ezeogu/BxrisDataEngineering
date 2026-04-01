-- DROP TABLE IF EXISTS lucentis.claims_backup;

CREATE TABLE IF NOT EXISTS lucentis.claims_backup (
    claim_id        integer,
    patient_id      integer,
    provider_id     integer,
    diagnosis_code  varchar(10),
    procedure_code  varchar(10),
    amount          numeric(18,2) NOT NULL,
    service_date    varchar(20)
);

SELECT * FROM lucentis.claims_backup;
