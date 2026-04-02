# Expected data quality issues
Missing required fields
Duplicate business keys
Wrong data types
Invalid dates
Negative amounts where not allowed
Invalid diagnosis/procedure codes
Null foreign keys
Out-of-order columns in CSV
Extra/truncated/broken rows

- procedure codes should be in reference tables for better validation

# Bigger migration issues you are not covering yet
- A. Cross-system identity mismatch
    - same patient exists in MySQL, PostgreSQL, and CSV under different IDs
    - one patient accidentally merged with another
    - duplicate MRNs
    - different formatting of names, DOB, phone, address

- B. Referential integrity issues
    - claims referencing missing patients
    - appointments referencing missing providers
    - medications linked to encounters that do not exist
    - orphaned child rows

- C. Code-set and semantic mismatch
    - old EMR uses local procedure/diagnosis codes
    - target EMR expects standard ICD-10, CPT, RxNorm, LOINC, SNOMED, NDC, etc.
    - gender/status values differ: M/F, Male/Female, 1/2, Unknown
    - appointment statuses and billing statuses differ across systems

- D. History and slowly changing data
    - address changes over time
    - insurance effective dates overlap or conflict
    - medication status active in one system and discontinued in another
    - provider affiliations changed over time

- E. Timestamp/timezone issues
    - naive timestamps
    - UTC vs local conversion
    - date-only values becoming timestamp values
    - midnight shifts causing wrong service date

- F. Free-text and document issues
    - notes with bad encoding
    - scanned PDFs with no structured fields
    - multiline text breaking CSV exports
    - control characters, weird quotes, hidden delimiters
 
- G. PHI/compliance issues
    - audit trail required
    - row-level traceability back to source
    - quarantine files holding PHI must be protected
    - no hardcoded passwords in code

- H. Migration correctness issues
    - source count != staged count != target count
    - insert succeeded but merge logic altered rows unexpectedly
    - same file re-run causes duplicates
    - partial failure leaves target inconsistent