from datetime import datetime, timezone
import hashlib
from pathlib import Path
import pandas as pd
from retailflow.config import REQUIRED_CSV_COLUMNS
from retailflow.db import Engine, text


class SchemaValidationError(ValueError):
    pass


def compute_row_hash(row: dict[str, str]) -> str:
    payload = "|".join(
        str(row.get(col, "")).strip()
        for col in REQUIRED_CSV_COLUMNS
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_csv_headers(columns: list[str]) -> list[str]:
    missing = [col for col in REQUIRED_CSV_COLUMNS if col not in columns]
    return missing


def _detect_csv_encoding(csv_path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"):
        try:
            with open(csv_path, "r", encoding=enc) as f:
                f.read(8192)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def ingest_csv_to_raw(
    csv_path: Path,
    batch_id: str,
    run_id: str,
    engine: Engine,
    chunk_size: int = 5000,
) -> int:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV file not found: {csv_path}")

    detected_encoding = _detect_csv_encoding(csv_path)

    header_sample = pd.read_csv(
        csv_path,
        nrows=0,
        encoding=detected_encoding,
        encoding_errors="replace",
    )

    missing_cols = validate_csv_headers(list(header_sample.columns))
    if missing_cols:
        raise SchemaValidationError(
            f"CSV schema validation failed for {csv_path.name}. Missing required columns: {', '.join(missing_cols)}"
        )

    now_iso = datetime.now(timezone.utc).isoformat()
    total_ingested = 0

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw_transactions WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )

    insert_sql = text(
        """
        INSERT INTO raw_transactions (
            raw_row_id, batch_id, run_id, source_file, source_row_number,
            row_hash, invoice_no, stock_code, description, quantity,
            invoice_date, unit_price, customer_id, country, ingested_at
        ) VALUES (
            :raw_row_id, :batch_id, :run_id, :source_file, :source_row_number,
            :row_hash, :invoice_no, :stock_code, :description, :quantity,
            :invoice_date, :unit_price, :customer_id, :country, :ingested_at
        )
        """
    )

    reader = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        chunksize=chunk_size,
        encoding=detected_encoding,
        encoding_errors="replace",
    )

    for chunk in reader:
        chunk_records: list[dict[str, object]] = []
        for row in chunk.to_dict(orient="records"):
            total_ingested += 1
            r_hash = compute_row_hash(row)
            raw_row_id = f"{batch_id}_R{total_ingested:07d}"
            chunk_records.append(
                {
                    "raw_row_id": raw_row_id,
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "source_file": csv_path.name,
                    "source_row_number": total_ingested,
                    "row_hash": r_hash,
                    "invoice_no": str(row.get("InvoiceNo", "")).strip(),
                    "stock_code": str(row.get("StockCode", "")).strip(),
                    "description": str(row.get("Description", "")).strip(),
                    "quantity": str(row.get("Quantity", "")).strip(),
                    "invoice_date": str(row.get("InvoiceDate", "")).strip(),
                    "unit_price": str(row.get("UnitPrice", "")).strip(),
                    "customer_id": str(row.get("CustomerID", "")).strip(),
                    "country": str(row.get("Country", "")).strip(),
                    "ingested_at": now_iso,
                }
            )

        if chunk_records:
            with engine.begin() as conn:
                conn.execute(insert_sql, chunk_records)

    return total_ingested
