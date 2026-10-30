import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

NON_MERCHANDISE_STOCK_CODES = frozenset({
    "POST",
    "D",
    "M",
    "BANK CHARGES",
    "PADS",
    "DOT",
    "CRUK",
    "AMAZONFEE",
    "B",
    "S",
})

REQUIRED_CSV_COLUMNS = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)

GUEST_CUSTOMER_ID = "GUEST_CHECKOUT"
GUEST_CUSTOMER_KEY = "CUST_GUEST_CHECKOUT"


@dataclass(frozen=True)
class RetailFlowConfig:
    env: str
    db_url: str
    project_root: Path
    data_dir: Path
    raw_dir: Path
    warehouse_dir: Path
    sql_dir: Path
    max_quarantine_rate: float = 0.25
    max_single_line_quantity: int = 100000
    max_unit_price: float = 25000.0


def get_config(custom_db_url: str | None = None) -> RetailFlowConfig:
    env = os.getenv("RETAILFLOW_ENV", "development")
    data_dir = PROJECT_ROOT / "data"
    raw_dir = data_dir / "raw"
    warehouse_dir = data_dir / "warehouse"
    sql_dir = PROJECT_ROOT / "sql"

    raw_dir.mkdir(parents=True, exist_ok=True)
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    default_sqlite_path = warehouse_dir / "retailflow.db"
    default_db_url = f"sqlite:///{default_sqlite_path.as_posix()}"
    db_url = custom_db_url or os.getenv("RETAILFLOW_DB_URL", default_db_url)

    return RetailFlowConfig(
        env=env,
        db_url=db_url,
        project_root=PROJECT_ROOT,
        data_dir=data_dir,
        raw_dir=raw_dir,
        warehouse_dir=warehouse_dir,
        sql_dir=sql_dir,
    )
