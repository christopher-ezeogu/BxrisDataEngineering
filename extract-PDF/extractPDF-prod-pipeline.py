"""
emr_pdf_pipeline/
├── incoming_pdfs/
├── output/
│   ├── parsed_json/
│   ├── quarantine/
│   ├── logs/
│   └── summaries/
└── emr_pdf_pipeline.py

"""

import os
import re
import csv
import json
import uuid
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from pypdf import PdfReader

# Optional OCR fallback
OCR_AVAILABLE = True
try:
    from pdf2image import convert_from_path
    import pytesseract
except Exception:
    OCR_AVAILABLE = False

# Optional PostgreSQL loading
POSTGRES_AVAILABLE = True
try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:
    POSTGRES_AVAILABLE = False


# ----------------------------
# Configuration
# ----------------------------

@dataclass
class PipelineConfig:
    input_dir: str = "incoming_pdfs"
    output_dir: str = "output"
    parsed_json_dir: str = "output/parsed_json"
    quarantine_dir: str = "output/quarantine"
    log_dir: str = "output/logs"
    summary_dir: str = "output/summaries"

    min_text_length_for_direct_success: int = 100
    low_confidence_threshold: float = 0.60
    enable_ocr_fallback: bool = True
    ocr_dpi: int = 300

    postgres_enabled: bool = False
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "emr"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    postgres_table: str = "etl.patient_document_staging"


# ----------------------------
# Data models
# ----------------------------

@dataclass
class ParsedDocument:
    run_id: str
    document_id: str
    file_name: str
    file_path: str
    processed_at: str
    extraction_method: str
    document_type: Optional[str]
    patient_name: Optional[str]
    mrn: Optional[str]
    dob: Optional[str]
    sex: Optional[str]
    admit_date: Optional[str]
    discharge_date: Optional[str]
    encounter_date: Optional[str]
    diagnosis: Optional[str]
    provider: Optional[str]
    confidence_score: float
    needs_review: bool
    raw_text_preview: str


# ----------------------------
# Utility setup
# ----------------------------

def ensure_directories(cfg: PipelineConfig) -> None:
    for path in [
        cfg.output_dir,
        cfg.parsed_json_dir,
        cfg.quarantine_dir,
        cfg.log_dir,
        cfg.summary_dir,
    ]:
        Path(path).mkdir(parents=True, exist_ok=True)


def setup_logging(cfg: PipelineConfig, run_id: str) -> logging.Logger:
    logger = logging.getLogger(f"emr_pdf_pipeline_{run_id}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = Path(cfg.log_dir) / f"run_{run_id}.log"
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def normalize_text(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def find_first(pattern: str, text: str, flags: int = re.IGNORECASE) -> Optional[str]:
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else None


def safe_json_write(path: Path, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


# ----------------------------
# PDF extraction
# ----------------------------

def extract_text_direct(pdf_path: str) -> str:
    reader = PdfReader(pdf_path)
    page_texts = []

    for page in reader.pages:
        page_texts.append(page.extract_text() or "")

    return "\n".join(page_texts).strip()


def extract_text_ocr(pdf_path: str, dpi: int = 300) -> str:
    if not OCR_AVAILABLE:
        raise RuntimeError("OCR fallback requested but OCR dependencies are not installed.")

    images = convert_from_path(pdf_path, dpi=dpi)
    texts = []
    for i, image in enumerate(images, start=1):
        page_text = pytesseract.image_to_string(image)
        texts.append(f"\n--- PAGE {i} ---\n{page_text}")

    return "\n".join(texts).strip()


def extract_text(pdf_path: str, cfg: PipelineConfig, logger: logging.Logger) -> Tuple[str, str]:
    direct_text = extract_text_direct(pdf_path)

    if len(direct_text.strip()) >= cfg.min_text_length_for_direct_success:
        return normalize_text(direct_text), "direct_pdf_text"

    if cfg.enable_ocr_fallback and OCR_AVAILABLE:
        logger.info("Direct extraction weak for %s. Falling back to OCR.", pdf_path)
        ocr_text = extract_text_ocr(pdf_path, dpi=cfg.ocr_dpi)
        return normalize_text(ocr_text), "ocr_fallback"

    logger.warning("Direct extraction weak and OCR unavailable for %s.", pdf_path)
    return normalize_text(direct_text), "direct_pdf_text_weak"


# ----------------------------
# Classification and parsing
# ----------------------------

DOCUMENT_PATTERNS = {
    "discharge_summary": [
        r"\bdischarge summary\b",
        r"\bhospital discharge\b",
    ],
    "consult_note": [
        r"\bconsult note\b",
        r"\bconsultation\b",
    ],
    "progress_note": [
        r"\bprogress note\b",
    ],
    "operative_report": [
        r"\boperative report\b",
        r"\boperation performed\b",
    ],
    "history_and_physical": [
        r"\bhistory and physical\b",
        r"\bH&P\b",
    ],
    "lab_report": [
        r"\blaboratory report\b",
        r"\blab results?\b",
    ],
    "radiology_report": [
        r"\bradiology report\b",
        r"\bimpression\b",
        r"\bfindings\b",
    ],
    "consent_form": [
        r"\bconsent\b",
        r"\binformed consent\b",
    ],
}


def classify_document(text: str) -> Optional[str]:
    lowered = text.lower()
    for doc_type, patterns in DOCUMENT_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, lowered):
                return doc_type
    return None


def extract_fields(text: str) -> Dict[str, Optional[str]]:
    return {
        "patient_name": (
            find_first(r"(?:Patient Name|Name)[:\s]+([A-Z][A-Za-z,\-.' ]+)", text)
        ),
        "mrn": (
            find_first(r"(?:MRN|Medical Record Number|Chart Number)[:\s#]+([A-Za-z0-9\-]+)", text)
        ),
        "dob": (
            find_first(r"(?:DOB|Date of Birth)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
        ),
        "sex": (
            find_first(r"(?:Sex|Gender)[:\s]+(Male|Female|M|F)", text)
        ),
        "admit_date": (
            find_first(r"(?:Admit Date|Admission Date)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
        ),
        "discharge_date": (
            find_first(r"(?:Discharge Date)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
        ),
        "encounter_date": (
            find_first(r"(?:Encounter Date|Visit Date|Date of Service)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
        ),
        "diagnosis": (
            find_first(r"(?:Primary Diagnosis|Diagnosis|Discharge Diagnosis)[:\s]+(.+)", text)
        ),
        "provider": (
            find_first(r"(?:Attending Physician|Provider|Author|Ordering Physician)[:\s]+([A-Z][A-Za-z,\-.' ]+)", text)
        ),
    }


def compute_confidence(document_type: Optional[str], fields: Dict[str, Optional[str]]) -> float:
    expected = [
        "patient_name", "mrn", "dob", "sex",
        "admit_date", "discharge_date", "encounter_date",
        "diagnosis", "provider"
    ]

    found_count = sum(1 for key in expected if fields.get(key))
    base_score = found_count / len(expected)

    if document_type:
        base_score += 0.10

    return round(min(base_score, 1.0), 2)


# ----------------------------
# Quarantine / persistence
# ----------------------------

def write_parsed_json(cfg: PipelineConfig, parsed: ParsedDocument, full_text: str) -> None:
    payload = asdict(parsed)
    payload["full_text"] = full_text
    output_file = Path(cfg.parsed_json_dir) / f"{parsed.document_id}.json"
    safe_json_write(output_file, payload)


def write_quarantine_record(
    cfg: PipelineConfig,
    file_name: str,
    reason: str,
    extracted_payload: Dict[str, Any]
) -> None:
    quarantine_file = Path(cfg.quarantine_dir) / f"{Path(file_name).stem}_quarantine.json"
    payload = {
        "file_name": file_name,
        "reason": reason,
        "quarantined_at": datetime.utcnow().isoformat(),
        "payload": extracted_payload,
    }
    safe_json_write(quarantine_file, payload)


def write_summary_csv(cfg: PipelineConfig, run_id: str, rows: List[ParsedDocument]) -> str:
    summary_file = Path(cfg.summary_dir) / f"run_{run_id}_summary.csv"

    fieldnames = [
        "run_id", "document_id", "file_name", "file_path", "processed_at",
        "extraction_method", "document_type", "patient_name", "mrn", "dob",
        "sex", "admit_date", "discharge_date", "encounter_date", "diagnosis",
        "provider", "confidence_score", "needs_review", "raw_text_preview"
    ]

    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))

    return str(summary_file)


# ----------------------------
# PostgreSQL loading
# ----------------------------

def load_to_postgres(cfg: PipelineConfig, rows: List[ParsedDocument], logger: logging.Logger) -> None:
    if not cfg.postgres_enabled:
        logger.info("PostgreSQL loading disabled.")
        return

    if not POSTGRES_AVAILABLE:
        raise RuntimeError("psycopg2 is not installed but postgres_enabled=True")

    conn = psycopg2.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        dbname=cfg.postgres_db,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
    )

    insert_sql = f"""
        INSERT INTO {cfg.postgres_table} (
            run_id,
            document_id,
            file_name,
            file_path,
            processed_at,
            extraction_method,
            document_type,
            patient_name,
            mrn,
            dob,
            sex,
            admit_date,
            discharge_date,
            encounter_date,
            diagnosis,
            provider,
            confidence_score,
            needs_review,
            raw_text_preview
        )
        VALUES %s
        ON CONFLICT (document_id) DO UPDATE
        SET
            run_id = EXCLUDED.run_id,
            file_name = EXCLUDED.file_name,
            file_path = EXCLUDED.file_path,
            processed_at = EXCLUDED.processed_at,
            extraction_method = EXCLUDED.extraction_method,
            document_type = EXCLUDED.document_type,
            patient_name = EXCLUDED.patient_name,
            mrn = EXCLUDED.mrn,
            dob = EXCLUDED.dob,
            sex = EXCLUDED.sex,
            admit_date = EXCLUDED.admit_date,
            discharge_date = EXCLUDED.discharge_date,
            encounter_date = EXCLUDED.encounter_date,
            diagnosis = EXCLUDED.diagnosis,
            provider = EXCLUDED.provider,
            confidence_score = EXCLUDED.confidence_score,
            needs_review = EXCLUDED.needs_review,
            raw_text_preview = EXCLUDED.raw_text_preview
    """

    values = [
        (
            row.run_id,
            row.document_id,
            row.file_name,
            row.file_path,
            row.processed_at,
            row.extraction_method,
            row.document_type,
            row.patient_name,
            row.mrn,
            row.dob,
            row.sex,
            row.admit_date,
            row.discharge_date,
            row.encounter_date,
            row.diagnosis,
            row.provider,
            row.confidence_score,
            row.needs_review,
            row.raw_text_preview,
        )
        for row in rows
    ]

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)

    conn.close()
    logger.info("Loaded %s records into PostgreSQL table %s", len(rows), cfg.postgres_table)


# ----------------------------
# Main processing
# ----------------------------

def process_pdf(pdf_path: Path, cfg: PipelineConfig, run_id: str, logger: logging.Logger) -> ParsedDocument:
    logger.info("Processing file: %s", pdf_path.name)

    full_text, extraction_method = extract_text(str(pdf_path), cfg, logger)
    document_type = classify_document(full_text)
    fields = extract_fields(full_text)
    confidence_score = compute_confidence(document_type, fields)
    needs_review = confidence_score < cfg.low_confidence_threshold

    parsed = ParsedDocument(
        run_id=run_id,
        document_id=str(uuid.uuid4()),
        file_name=pdf_path.name,
        file_path=str(pdf_path.resolve()),
        processed_at=datetime.utcnow().isoformat(),
        extraction_method=extraction_method,
        document_type=document_type,
        patient_name=fields["patient_name"],
        mrn=fields["mrn"],
        dob=fields["dob"],
        sex=fields["sex"],
        admit_date=fields["admit_date"],
        discharge_date=fields["discharge_date"],
        encounter_date=fields["encounter_date"],
        diagnosis=fields["diagnosis"],
        provider=fields["provider"],
        confidence_score=confidence_score,
        needs_review=needs_review,
        raw_text_preview=full_text[:1000],
    )

    write_parsed_json(cfg, parsed, full_text)

    if needs_review:
        write_quarantine_record(
            cfg,
            pdf_path.name,
            f"Low confidence score: {confidence_score}",
            {
                "parsed_document": asdict(parsed),
                "document_type": document_type,
                "extraction_method": extraction_method,
            }
        )
        logger.warning("File %s sent to review queue. Confidence=%s", pdf_path.name, confidence_score)

    return parsed


def run_pipeline(cfg: PipelineConfig) -> None:
    ensure_directories(cfg)
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    logger = setup_logging(cfg, run_id)

    input_path = Path(cfg.input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {cfg.input_dir}")

    pdf_files = sorted(input_path.glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s", cfg.input_dir)
        return

    logger.info("Starting run_id=%s with %s PDF files", run_id, len(pdf_files))

    results: List[ParsedDocument] = []
    failed_files = 0

    for pdf_file in pdf_files:
        try:
            parsed = process_pdf(pdf_file, cfg, run_id, logger)
            results.append(parsed)
        except Exception as exc:
            failed_files += 1
            logger.exception("Failed processing file %s: %s", pdf_file.name, exc)
            write_quarantine_record(
                cfg,
                pdf_file.name,
                f"Processing failure: {str(exc)}",
                {"file_path": str(pdf_file.resolve())}
            )

    summary_csv = write_summary_csv(cfg, run_id, results)
    logger.info("Summary CSV written to %s", summary_csv)

    if results:
        load_to_postgres(cfg, results, logger)

    logger.info(
        "Run complete. success=%s failed=%s review_needed=%s",
        len(results),
        failed_files,
        sum(1 for r in results if r.needs_review)
    )


if __name__ == "__main__":
    config = PipelineConfig(
        input_dir="incoming_pdfs",
        output_dir="output",
        parsed_json_dir="output/parsed_json",
        quarantine_dir="output/quarantine",
        log_dir="output/logs",
        summary_dir="output/summaries",
        min_text_length_for_direct_success=100,
        low_confidence_threshold=0.60,
        enable_ocr_fallback=True,
        ocr_dpi=300,

        # Turn on when ready
        postgres_enabled=False,
        postgres_host="localhost",
        postgres_port=5432,
        postgres_db="emr",
        postgres_user="postgres",
        postgres_password="postgres",
        postgres_table="etl.patient_document_staging"
    )

    run_pipeline(config)