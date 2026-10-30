from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from retailflow.config import (
    GUEST_CUSTOMER_ID,
    NON_MERCHANDISE_STOCK_CODES,
    RetailFlowConfig,
    get_config,
)
from retailflow.db import Engine, text


DATE_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d",
)


@dataclass(frozen=True)
class StagingResult:
    batch_id: str
    raw_count: int
    valid_count: int
    quarantined_count: int
    duplicate_count: int


def parse_flexible_datetime(raw_value: str) -> datetime | None:
    cleaned = raw_value.strip()
    if not cleaned:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        return None


def normalize_customer_id(raw_customer_id: str) -> tuple[str, int]:
    cleaned = raw_customer_id.strip()
    if not cleaned or cleaned.lower() in ("nan", "none", "null"):
        return GUEST_CUSTOMER_ID, 1
    if cleaned.endswith(".0"):
        cleaned = cleaned[:-2]
    return cleaned, 0


def transform_raw_to_staging(
    batch_id: str,
    run_id: str,
    engine: Engine,
    config: RetailFlowConfig | None = None,
) -> StagingResult:
    cfg = config or get_config()
    now_iso = datetime.now(timezone.utc).isoformat()

    with engine.connect() as conn:
        raw_df = conn.to_dataframe(
            text(
                """
                SELECT
                    raw_row_id, batch_id, run_id, source_file, source_row_number,
                    row_hash, invoice_no, stock_code, description, quantity,
                    invoice_date, unit_price, customer_id, country
                FROM raw_transactions
                WHERE batch_id = :batch_id
                ORDER BY source_row_number ASC
                """
            ),
            params={"batch_id": batch_id},
        )

    raw_count = len(raw_df)
    quarantine_rows: list[dict[str, object]] = []
    dedup_map: dict[str, dict[str, object]] = {}
    duplicate_count = 0

    for row in raw_df.to_dict(orient="records"):
        row_num = int(row["source_row_number"])
        inv_no = str(row["invoice_no"] or "").strip().upper()
        stock_code = str(row["stock_code"] or "").strip().upper()
        description = str(row["description"] or "").strip() or "UNSPECIFIED MERCHANDISE"
        qty_raw = str(row["quantity"] or "").strip()
        date_raw = str(row["invoice_date"] or "").strip()
        price_raw = str(row["unit_price"] or "").strip()
        cust_raw = str(row["customer_id"] or "").strip()
        country = str(row["country"] or "").strip() or "Unspecified"

        all_fields_empty = not any([
            inv_no,
            stock_code,
            str(row["description"] or "").strip(),
            qty_raw,
            date_raw,
            price_raw,
            cust_raw,
            str(row["country"] or "").strip(),
        ])

        rejection_code = None
        rejection_reason = None

        if all_fields_empty:
            rejection_code = "COMPLETELY_BLANK_ROW"
            rejection_reason = "Row contains no invoice, stock code, or financial data."
        elif not inv_no:
            rejection_code = "MISSING_INVOICE_NO"
            rejection_reason = "InvoiceNo is blank or missing."
        elif not stock_code:
            rejection_code = "MISSING_STOCK_CODE"
            rejection_reason = "StockCode is blank or missing."
        elif stock_code in NON_MERCHANDISE_STOCK_CODES:
            rejection_code = "NON_MERCHANDISE_CODE"
            rejection_reason = f"StockCode '{stock_code}' is a non-merchandise accounting/postage fee."

        parsed_qty: int = 0
        if rejection_code is None:
            try:
                parsed_qty = int(float(qty_raw))
                if parsed_qty == 0:
                    rejection_code = "ZERO_QUANTITY"
                    rejection_reason = "Quantity cannot be zero."
                elif abs(parsed_qty) > cfg.max_single_line_quantity:
                    rejection_code = "EXTREME_QUANTITY_OUTLIER"
                    rejection_reason = f"Absolute quantity {abs(parsed_qty)} exceeds limit {cfg.max_single_line_quantity}."
            except ValueError:
                rejection_code = "INVALID_QUANTITY_FORMAT"
                rejection_reason = f"Quantity '{qty_raw}' is not a valid integer."

        parsed_price: float = 0.0
        if rejection_code is None:
            try:
                parsed_price = round(float(price_raw), 4)
                if parsed_price <= 0.0:
                    rejection_code = "NON_POSITIVE_UNIT_PRICE"
                    rejection_reason = f"UnitPrice {parsed_price} must be strictly positive."
                elif parsed_price > cfg.max_unit_price:
                    rejection_code = "EXTREME_UNIT_PRICE"
                    rejection_reason = f"UnitPrice {parsed_price} exceeds maximum threshold {cfg.max_unit_price}."
            except ValueError:
                rejection_code = "INVALID_UNIT_PRICE_FORMAT"
                rejection_reason = f"UnitPrice '{price_raw}' is not numeric."

        parsed_dt: datetime | None = None
        if rejection_code is None:
            parsed_dt = parse_flexible_datetime(date_raw)
            if parsed_dt is None:
                rejection_code = "INVALID_INVOICE_DATE"
                rejection_reason = f"InvoiceDate '{date_raw}' could not be parsed."

        if rejection_code is not None or parsed_dt is None:
            quarantine_rows.append(
                {
                    "quarantine_id": f"{run_id}_Q{row_num:07d}",
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "source_row_number": row_num,
                    "invoice_no": inv_no,
                    "stock_code": stock_code,
                    "description": description,
                    "quantity_raw": qty_raw,
                    "invoice_date_raw": date_raw,
                    "unit_price_raw": price_raw,
                    "customer_id_raw": cust_raw,
                    "country_raw": country,
                    "rejection_code": rejection_code or "UNKNOWN_REJECTION",
                    "rejection_reason": rejection_reason or "Unspecified validation error",
                    "quarantined_at": now_iso,
                }
            )
            continue

        customer_id, is_guest = normalize_customer_id(cust_raw)
        is_cancelled = 1 if inv_no.startswith("C") else 0

        if is_cancelled and parsed_qty > 0:
            parsed_qty = -abs(parsed_qty)

        is_return = 1 if (is_cancelled == 1 or parsed_qty < 0) else 0
        if is_cancelled == 1:
            transaction_type = "CANCELLATION"
        elif parsed_qty < 0:
            transaction_type = "RETURN_ADJUSTMENT"
        else:
            transaction_type = "SALE"

        invoice_ts_str = parsed_dt.strftime("%Y-%m-%d %H:%M:%S")
        invoice_date_str = parsed_dt.strftime("%Y-%m-%d")
        date_key = int(parsed_dt.strftime("%Y%m%d"))

        line_signature = f"{inv_no}|{stock_code}|{invoice_ts_str}|{parsed_price:.4f}|{customer_id}|{parsed_qty}"
        stg_line_id = hashlib.sha256(line_signature.encode("utf-8")).hexdigest()[:32]

        if stg_line_id in dedup_map:
            duplicate_count += 1
            dedup_map[stg_line_id]["occurrence_count"] = int(dedup_map[stg_line_id]["occurrence_count"]) + 1
            continue

        line_value = round(abs(parsed_qty) * parsed_price, 2)
        if is_return == 1:
            gross_amount = 0.0
            return_amount = line_value
            net_amount = -line_value
        else:
            gross_amount = line_value
            return_amount = 0.0
            net_amount = line_value

        dedup_map[stg_line_id] = {
            "stg_line_id": stg_line_id,
            "batch_id": batch_id,
            "run_id": run_id,
            "invoice_no": inv_no,
            "stock_code": stock_code,
            "description": description,
            "quantity": parsed_qty,
            "invoice_timestamp": invoice_ts_str,
            "invoice_date": invoice_date_str,
            "date_key": date_key,
            "unit_price": parsed_price,
            "gross_amount": gross_amount,
            "return_amount": return_amount,
            "net_amount": net_amount,
            "customer_id": customer_id,
            "is_guest_customer": is_guest,
            "country": country,
            "transaction_type": transaction_type,
            "is_cancelled": is_cancelled,
            "is_return": is_return,
            "occurrence_count": 1,
            "cleaned_at": now_iso,
        }

    staging_rows = list(dedup_map.values())

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM stg_transactions WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        conn.execute(
            text("DELETE FROM ops_quarantine_records WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )

        if quarantine_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO ops_quarantine_records (
                        quarantine_id, run_id, batch_id, source_row_number,
                        invoice_no, stock_code, description, quantity_raw,
                        invoice_date_raw, unit_price_raw, customer_id_raw,
                        country_raw, rejection_code, rejection_reason, quarantined_at
                    ) VALUES (
                        :quarantine_id, :run_id, :batch_id, :source_row_number,
                        :invoice_no, :stock_code, :description, :quantity_raw,
                        :invoice_date_raw, :unit_price_raw, :customer_id_raw,
                        :country_raw, :rejection_code, :rejection_reason, :quarantined_at
                    )
                    """
                ),
                quarantine_rows,
            )

        if staging_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO stg_transactions (
                        stg_line_id, batch_id, run_id, invoice_no, stock_code,
                        description, quantity, invoice_timestamp, invoice_date,
                        date_key, unit_price, gross_amount, return_amount,
                        net_amount, customer_id, is_guest_customer, country,
                        transaction_type, is_cancelled, is_return, occurrence_count,
                        cleaned_at
                    ) VALUES (
                        :stg_line_id, :batch_id, :run_id, :invoice_no, :stock_code,
                        :description, :quantity, :invoice_timestamp, :invoice_date,
                        :date_key, :unit_price, :gross_amount, :return_amount,
                        :net_amount, :customer_id, :is_guest_customer, :country,
                        :transaction_type, :is_cancelled, :is_return, :occurrence_count,
                        :cleaned_at
                    )
                    """
                ),
                staging_rows,
            )

    return StagingResult(
        batch_id=batch_id,
        raw_count=raw_count,
        valid_count=len(staging_rows),
        quarantined_count=len(quarantine_rows),
        duplicate_count=duplicate_count,
    )
