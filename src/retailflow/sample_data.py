from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import pandas as pd


CATALOG_ITEMS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER", 2.95, "Home Decor"),
    ("22423", "REGENCY CAKESTAND 3 TIER", 12.75, "Dining & Kitchen"),
    ("85099B", "JUMBO BAG RED RETROSPOT", 2.08, "Bags & Storage"),
    ("47566", "PARTY BUNTING", 4.95, "Party & Seasonal"),
    ("84879", "ASSORTED COLOUR BIRD ORNAMENT", 1.69, "Home Decor"),
    ("20725", "LUNCH BAG RED RETROSPOT", 1.65, "Bags & Storage"),
    ("22720", "SET OF 3 CAKE TINS PANTRY DESIGN", 4.95, "Dining & Kitchen"),
    ("22197", "POPCORN HOLDER", 0.85, "Party & Seasonal"),
    ("21212", "PACK OF 72 RETROSPOT CAKE CASES", 0.55, "Dining & Kitchen"),
    ("22386", "JUMBO BAG PINK POLKADOT", 2.08, "Bags & Storage"),
    ("22960", "JAM MAKING SET WITH JARS", 4.25, "Dining & Kitchen"),
    ("23298", "SPOTTY BUNTING", 4.95, "Party & Seasonal"),
    ("22666", "RECIPE BOX PANTRY YELLOW DESIGN", 2.95, "Dining & Kitchen"),
    ("23084", "RABBIT NIGHT LIGHT", 2.08, "Lighting & Gifts"),
    ("22086", "PAPER CHAIN KIT 50'S CHRISTMAS", 2.95, "Party & Seasonal"),
    ("22469", "HEART OF WICKER SMALL", 1.65, "Home Decor"),
    ("23245", "SET OF 3 REGENCY CAKE TINS", 4.95, "Dining & Kitchen"),
    ("21754", "HOME BUILDING BLOCK WORD", 5.95, "Home Decor"),
    ("22727", "ALARM CLOCK BAKELIKE RED", 3.75, "Lighting & Gifts"),
    ("22726", "ALARM CLOCK BAKELIKE GREEN", 3.75, "Lighting & Gifts"),
    ("21931", "JUMBO STORAGE BAG SUKI", 2.08, "Bags & Storage"),
    ("22411", "JUMBO SHOPPER VINTAGE RED PAISLEY", 2.08, "Bags & Storage"),
    ("23203", "JUMBO BAG VINTAGE DOILY", 2.08, "Bags & Storage"),
    ("82482", "WOODEN PICTURE FRAME WHITE FINISH", 2.55, "Home Decor"),
    ("22910", "PAPER CHAIN KIT VINTAGE CHRISTMAS", 2.95, "Party & Seasonal"),
    ("22629", "SPACEBOY LUNCH BOX", 1.95, "Bags & Storage"),
    ("22630", "DOLLY GIRL LUNCH BOX", 1.95, "Bags & Storage"),
    ("22138", "BAKING SET 9 PIECE RETROSPOT", 4.95, "Dining & Kitchen"),
    ("21485", "RETROSPOT HEART HOT WATER BOTTLE", 4.95, "Home Decor"),
    ("22111", "SCOTTIE DOG HOT WATER BOTTLE", 4.95, "Home Decor"),
    ("23355", "HOT WATER BOTTLE KEEP CALM", 4.95, "Home Decor"),
    ("22558", "CLOTHES PEGS RETROSPOT PACK 24", 1.49, "Home Decor"),
]

COUNTRY_WEIGHTS = [
    ("United Kingdom", 0.72),
    ("Germany", 0.07),
    ("France", 0.06),
    ("EIRE", 0.05),
    ("Netherlands", 0.03),
    ("Spain", 0.025),
    ("Belgium", 0.02),
    ("Switzerland", 0.015),
    ("Australia", 0.01),
]


def infer_product_category(stock_code: str, description: str) -> str:
    for code, _, _, category in CATALOG_ITEMS:
        if code == stock_code:
            return category
    upper_desc = description.upper()
    if any(token in upper_desc for token in ("BAG", "LUNCH BOX", "SHOPPER", "STORAGE")):
        return "Bags & Storage"
    if any(token in upper_desc for token in ("CAKE", "JAM", "BAKING", "RECIPE", "PANTRY", "TEA", "MUG")):
        return "Dining & Kitchen"
    if any(token in upper_desc for token in ("BUNTING", "CHRISTMAS", "PARTY", "POPCORN", "PAPER CHAIN")):
        return "Party & Seasonal"
    if any(token in upper_desc for token in ("LIGHT", "CLOCK", "LAMP", "CANDLE")):
        return "Lighting & Gifts"
    return "Home Decor"


def generate_synthetic_retail_batch(
    output_path: Path,
    batch_name: str = "2026_10_batch_01",
    start_date: str = "2026-08-01",
    days: int = 90,
    num_invoices: int = 420,
    seed: int = 42,
) -> Path:
    rng = np.random.default_rng(seed)
    base_dt = datetime.strptime(start_date, "%Y-%m-%d")

    countries = [c[0] for c in COUNTRY_WEIGHTS]
    country_probs = [c[1] for c in COUNTRY_WEIGHTS]

    customer_pool = []
    for idx in range(12346, 12526):
        cust_country = rng.choice(countries, p=country_probs)
        is_vip = (idx % 11 == 0)
        customer_pool.append((str(idx), str(cust_country), is_vip))

    rows: list[dict[str, object]] = []
    invoice_counter = 560000 + (seed * 100)

    for inv_idx in range(num_invoices):
        invoice_counter += int(rng.integers(1, 5))
        day_offset = int(rng.integers(0, days))
        hour = int(rng.integers(8, 20))
        minute = int(rng.integers(0, 60))
        inv_dt = base_dt + timedelta(days=day_offset, hours=hour, minutes=minute)
        inv_date_str = inv_dt.strftime("%Y-%m-%d %H:%M:%S")

        is_guest = rng.random() < 0.08
        if is_guest:
            cust_id = ""
            country = "United Kingdom"
            is_vip = False
        else:
            cust_idx = int(rng.integers(0, len(customer_pool)))
            cust_id, country, is_vip = customer_pool[cust_idx]

        is_cancelled_order = rng.random() < 0.045
        inv_no = f"C{invoice_counter}" if is_cancelled_order else str(invoice_counter)

        num_lines = int(rng.integers(1, 7))
        selected_indices = rng.choice(len(CATALOG_ITEMS), size=num_lines, replace=False)

        for item_idx in selected_indices:
            stock_code, description, base_price, _ = CATALOG_ITEMS[int(item_idx)]
            qty_multiplier = 3 if is_vip else 1
            base_qty = int(rng.choice([1, 2, 3, 4, 6, 8, 12, 24], p=[0.20, 0.22, 0.15, 0.15, 0.12, 0.08, 0.05, 0.03])) * qty_multiplier

            if is_cancelled_order:
                quantity = -abs(base_qty)
            elif rng.random() < 0.015:
                quantity = -abs(int(rng.integers(1, 4)))
            else:
                quantity = base_qty

            price_jitter = round(float(base_price) * float(rng.choice([1.0, 1.0, 1.0, 0.92, 1.08])), 2)

            row_record = {
                "InvoiceNo": inv_no,
                "StockCode": stock_code,
                "Description": description,
                "Quantity": str(quantity),
                "InvoiceDate": inv_date_str,
                "UnitPrice": f"{price_jitter:.2f}",
                "CustomerID": cust_id,
                "Country": country,
            }
            rows.append(row_record)

            if rng.random() < 0.012:
                rows.append(dict(row_record))

    anomaly_records = [
        {
            "InvoiceNo": str(invoice_counter + 901),
            "StockCode": "POST",
            "Description": "POSTAGE",
            "Quantity": "1",
            "InvoiceDate": (base_dt + timedelta(days=5, hours=11)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "18.00",
            "CustomerID": "12352",
            "Country": "Germany",
        },
        {
            "InvoiceNo": str(invoice_counter + 902),
            "StockCode": "BANK CHARGES",
            "Description": "Bank Charges",
            "Quantity": "1",
            "InvoiceDate": (base_dt + timedelta(days=12, hours=14)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "15.00",
            "CustomerID": "12360",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": str(invoice_counter + 903),
            "StockCode": "85123A",
            "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
            "Quantity": "4",
            "InvoiceDate": (base_dt + timedelta(days=19, hours=9)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "0.00",
            "CustomerID": "12370",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": str(invoice_counter + 904),
            "StockCode": "22423",
            "Description": "REGENCY CAKESTAND 3 TIER",
            "Quantity": "2",
            "InvoiceDate": "NOT_A_VALID_DATE",
            "UnitPrice": "12.75",
            "CustomerID": "12375",
            "Country": "France",
        },
        {
            "InvoiceNo": str(invoice_counter + 905),
            "StockCode": "47566",
            "Description": "PARTY BUNTING",
            "Quantity": "0",
            "InvoiceDate": (base_dt + timedelta(days=27, hours=16)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "4.95",
            "CustomerID": "12380",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": str(invoice_counter + 906),
            "StockCode": "22720",
            "Description": "SET OF 3 CAKE TINS PANTRY DESIGN",
            "Quantity": "3",
            "InvoiceDate": (base_dt + timedelta(days=33, hours=10)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "-5.50",
            "CustomerID": "12388",
            "Country": "United Kingdom",
        },
        {
            "InvoiceNo": "",
            "StockCode": "20725",
            "Description": "LUNCH BAG RED RETROSPOT",
            "Quantity": "5",
            "InvoiceDate": (base_dt + timedelta(days=40, hours=13)).strftime("%Y-%m-%d %H:%M:%S"),
            "UnitPrice": "1.65",
            "CustomerID": "12392",
            "Country": "United Kingdom",
        },
    ]

    rows.extend(anomaly_records)
    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path
