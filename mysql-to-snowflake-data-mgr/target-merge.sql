-- test function
-- SELECT * FROM etl.merge_claims('20260329225932_5c6e3d09');

-- DROP FUNCTION IF EXISTS etl.merge_claims(i_load_id varchar);

CREATE OR REPLACE FUNCTION etl.merge_claims(i_load_id varchar)
RETURNS TABLE(rejected_rows integer, upserted_rows integer)
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_rows_upserted integer := 0;
    v_target_rows_rejected integer := 0;
BEGIN
    -- 1. Reject invalid target-side rows
    INSERT INTO etl.claims_rejects (
        stg_claim_row_id,
        load_id,
        claim_id,
        patient_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        amount,
        service_date,
        ingested_at,
        record_hash,
        rejection_reason
    )
    SELECT
        s.stg_claim_row_id,
        s.load_id,
        s.claim_id,
        s.patient_id,
        s.provider_id,
        s.diagnosis_code,
        s.procedure_code,
        s.amount,
        s.service_date,
        s.ingested_at,
        s.record_hash,
        CASE
            WHEN s.claim_id IS NULL THEN 'missing claim_id'
            WHEN s.patient_id IS NULL THEN 'missing patient_id'
            WHEN s.provider_id IS NULL THEN 'missing provider_id'
            WHEN s.procedure_code IS NULL THEN 'missing procedure_code'
            WHEN s.procedure_code NOT IN ('J5502','99202','99203','99201') THEN 'invalid procedure_code'
            WHEN s.amount IS NULL THEN 'missing amount'
            WHEN s.amount < 0 THEN 'negative amount'
            WHEN s.service_date IS NULL THEN 'missing service_date'
            WHEN s.service_date::DATE > CURRENT_DATE THEN 'future service_date'
            ELSE 'unknown validation failure'
        END AS rejection_reason
    FROM etl.stg_claims s
    WHERE s.load_id = i_load_id
      AND (
            s.claim_id IS NULL
         OR s.patient_id IS NULL
         OR s.provider_id IS NULL
         OR s.procedure_code IS NULL
         OR s.amount IS NULL
         OR s.amount < 0
         OR s.service_date IS NULL
         OR s.service_date::DATE > CURRENT_DATE
      )
      ON CONFLICT (stg_claim_row_id, rejection_reason) DO NOTHING;

    GET DIAGNOSTICS v_target_rows_rejected = ROW_COUNT;

    -- 2. Reject duplicate rows within the stage load, keeping only latest row
    INSERT INTO etl.claims_rejects (
        stg_claim_row_id,
        load_id,
        claim_id,
        patient_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        amount,
        service_date,
        ingested_at,
        record_hash,
        rejection_reason
    )
    WITH valid_rows AS (
        SELECT
            s.*
        FROM etl.stg_claims s
        WHERE s.load_id = i_load_id
          AND s.claim_id IS NOT NULL
          AND s.patient_id IS NOT NULL
          AND s.provider_id IS NOT NULL
          AND s.procedure_code IS NOT NULL
          AND s.amount IS NOT NULL
          AND s.amount >= 0
          AND s.service_date IS NOT NULL
          AND s.service_date::DATE <= CURRENT_DATE
    ),
    ranked_rows AS (
        SELECT
            v.*,
            ROW_NUMBER() OVER (
                PARTITION BY v.claim_id
                ORDER BY v.ingested_at DESC, v.record_hash DESC
            ) AS row_num
        FROM valid_rows v
    )
    SELECT
        stg_claim_row_id,
        load_id,
        claim_id,
        patient_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        amount,
        service_date,
        ingested_at,
        record_hash,
        'duplicate claim_id in stage load'
    FROM ranked_rows
    WHERE row_num > 1
    ON CONFLICT (stg_claim_row_id, rejection_reason) DO NOTHING;

    -- 3. Upsert only the winning valid rows
    WITH valid_rows AS (
        SELECT
            s.*
        FROM etl.stg_claims s
        WHERE s.load_id = i_load_id
          AND s.claim_id IS NOT NULL
          AND s.patient_id IS NOT NULL
          AND s.provider_id IS NOT NULL
          AND s.procedure_code IS NOT NULL
          AND s.amount IS NOT NULL
          AND s.amount >= 0
          AND s.service_date IS NOT NULL
          AND s.service_date::DATE <= CURRENT_DATE
    ),
    ranked_rows AS (
        SELECT
            v.*,
            ROW_NUMBER() OVER (
                PARTITION BY v.claim_id
                ORDER BY v.ingested_at DESC, v.record_hash DESC
            ) AS row_num
        FROM valid_rows v
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
        current_timestamp,
        record_hash
    FROM final_rows
    ON CONFLICT (claim_id)
    DO UPDATE SET
        patient_id     = EXCLUDED.patient_id,
        provider_id    = EXCLUDED.provider_id,
        diagnosis_code = EXCLUDED.diagnosis_code,
        procedure_code = EXCLUDED.procedure_code,
        amount         = EXCLUDED.amount,
        service_date   = EXCLUDED.service_date,
        ingested_at    = current_timestamp,
        record_hash    = EXCLUDED.record_hash
    WHERE etl.claims.record_hash <> EXCLUDED.record_hash;

    GET DIAGNOSTICS v_target_rows_upserted = ROW_COUNT;

    -- 4. Update audit
    UPDATE etl.load_audit
    SET
        target_rows_rejected = (SELECT COUNT(*) FROM etl.claims_rejects r WHERE r.load_id = i_load_id),
        target_rows_upserted = v_target_rows_upserted,
        status = 'SUCCESS',
        completed_at = current_timestamp
    WHERE load_id = i_load_id;

    -- 5. return rejected and upserted records 
    RETURN QUERY
    SELECT v_target_rows_rejected,
           v_target_rows_upserted;

EXCEPTION
    WHEN OTHERS THEN
        UPDATE etl.load_audit
        SET
            status = 'FAILED',
            error_message = SQLERRM,
            completed_at = current_timestamp
        WHERE load_id = i_load_id;

        RAISE;
END;
$$;