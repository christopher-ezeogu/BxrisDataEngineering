import re
import json
from pathlib import Path
from typing import Dict, Any, Optional

# pip install pypdf pdf2image pytesseract pillow
# System requirements for OCR fallback:
# - poppler
# - tesseract

from pypdf import PdfReader

# OCR fallback imports
try:
    from pdf2image import convert_from_path
    import pytesseract
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False


def extract_text_from_pdf(pdf_path: str) -> str:
    """
    Try direct text extraction first.
    This works for digital PDFs that already contain selectable text.
    """
    reader = PdfReader(pdf_path)
    pages = []

    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(text)

    return "\n".join(pages).strip()


def extract_text_with_ocr(pdf_path: str, dpi: int = 300) -> str:
    """
    OCR fallback for scanned/image PDFs.
    Use only when the PDF has little or no extractable text.
    """
    if not OCR_AVAILABLE:
        raise RuntimeError(
            "OCR libraries are not available. Install pdf2image and pytesseract, "
            "and ensure poppler/tesseract are installed on the system."
        )

    images = convert_from_path(pdf_path, dpi=dpi)
    ocr_text = []

    for idx, image in enumerate(images, start=1):
        page_text = pytesseract.image_to_string(image)
        ocr_text.append(f"\n--- PAGE {idx} ---\n{page_text}")

    return "\n".join(ocr_text).strip()


def normalize_text(text: str) -> str:
    """
    Clean up text for easier regex matching.
    """
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def find_first(pattern: str, text: str, flags: int = re.IGNORECASE) -> Optional[str]:
    match = re.search(pattern, text, flags)
    if match:
        return match.group(1).strip()
    return None


def parse_patient_document(text: str) -> Dict[str, Any]:
    """
    Example extraction logic for common fields in a discharge summary / consult note.
    Adjust patterns to your document layout.
    """
    data = {
        "patient_name": None,
        "mrn": None,
        "dob": None,
        "sex": None,
        "admit_date": None,
        "discharge_date": None,
        "encounter_date": None,
        "document_type": None,
        "diagnosis": None,
        "provider": None,
        "raw_text_preview": text[:1000]
    }

    # Document type
    data["document_type"] = (
        find_first(r"Document Type[:\s]+([A-Za-z /-]+)", text) or
        find_first(r"^(Discharge Summary|Consult Note|Operative Report|Progress Note)$", text, flags=re.IGNORECASE | re.MULTILINE)
    )

    # Patient name
    data["patient_name"] = (
        find_first(r"Patient Name[:\s]+([A-Z][A-Za-z,\-.' ]+)", text) or
        find_first(r"Name[:\s]+([A-Z][A-Za-z,\-.' ]+)", text)
    )

    # MRN / Medical Record Number
    data["mrn"] = (
        find_first(r"(?:MRN|Medical Record Number)[:\s#]+([A-Za-z0-9\-]+)", text)
    )

    # DOB
    data["dob"] = (
        find_first(r"(?:DOB|Date of Birth)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
    )

    # Sex
    data["sex"] = (
        find_first(r"(?:Sex|Gender)[:\s]+(Male|Female|M|F)", text)
    )

    # Dates
    data["admit_date"] = (
        find_first(r"(?:Admit Date|Admission Date)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
    )

    data["discharge_date"] = (
        find_first(r"(?:Discharge Date)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
    )

    data["encounter_date"] = (
        find_first(r"(?:Encounter Date|Visit Date|Date of Service)[:\s]+([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4})", text)
    )

    # Diagnosis
    data["diagnosis"] = (
        find_first(r"(?:Primary Diagnosis|Diagnosis)[:\s]+(.+)", text)
    )

    # Provider / author
    data["provider"] = (
        find_first(r"(?:Attending Physician|Provider|Author)[:\s]+([A-Z][A-Za-z,\-.' ]+)", text)
    )

    return data


def process_patient_pdf(pdf_path: str) -> Dict[str, Any]:
    """
    End-to-end PDF processor:
    1. direct extraction
    2. OCR fallback if needed
    3. parse key fields
    """
    pdf_path = str(pdf_path)

    text = extract_text_from_pdf(pdf_path)

    extraction_method = "direct_pdf_text"

    # If almost no text came out, likely scanned PDF
    if len(text.strip()) < 100:
        if OCR_AVAILABLE:
            text = extract_text_with_ocr(pdf_path)
            extraction_method = "ocr_fallback"
        else:
            extraction_method = "direct_pdf_text_failed_no_ocr"

    text = normalize_text(text)
    parsed = parse_patient_document(text)

    return {
        "file_name": Path(pdf_path).name,
        "extraction_method": extraction_method,
        "parsed_fields": parsed,
        "full_text": text
    }


if __name__ == "__main__":
    pdf_file = "sample_discharge_summary.pdf"  # Replace with your file
    result = process_patient_pdf(pdf_file)

    print(json.dumps(result["parsed_fields"], indent=2))

    # Save full result for audit/review
    output_file = Path(pdf_file).with_suffix(".json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\nSaved structured output to: {output_file}")