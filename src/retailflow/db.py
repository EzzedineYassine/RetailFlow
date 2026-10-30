from contextlib import contextmanager
from pathlib import Path
import re
import sqlite3
from typing import Any, Iterator
from urllib.parse import urlparse
import pandas as pd
from retailflow.config import RetailFlowConfig, get_config


_NAMED_PARAM_RE = re.compile(r"(?<!:):([a-zA-Z_][a-zA-Z0-9_]*)")


def text(sql_string: str) -> str:
    return sql_string


class ScalarResult:
    def __init__(self, rows: list[tuple[Any, ...]], columns: list[str]):
        self._rows = rows
        self._columns = columns

    def scalar(self) -> Any:
        if not self._rows or not self._rows[0]:
            return None
        return self._rows[0][0]

    def mappings(self) -> "MappingResult":
        return MappingResult(self._rows, self._columns)


class MappingResult:
    def __init__(self, rows: list[tuple[Any, ...]], columns: list[str]):
        self._rows = rows
        self._columns = columns

    def first(self) -> dict[str, Any] | None:
        if not self._rows:
            return None
        return dict(zip(self._columns, self._rows[0]))

    def all(self) -> list[dict[str, Any]]:
        return [dict(zip(self._columns, row)) for row in self._rows]


class WarehouseConnection:
    def __init__(self, raw_conn: Any, dialect: str):
        self.raw_conn = raw_conn
        self.dialect = dialect

    def _prepare_sql(self, sql_obj: Any, params: dict[str, Any] | list[dict[str, Any]] | None) -> tuple[str, Any]:
        sql_str = str(sql_obj)
        if self.dialect == "postgres":
            converted = _NAMED_PARAM_RE.sub(r"%(\1)s", sql_str)
            return converted, params
        return sql_str, params

    def execute(self, sql_obj: Any, params: dict[str, Any] | list[dict[str, Any]] | None = None) -> ScalarResult:
        sql_str, bound_params = self._prepare_sql(sql_obj, params)
        cursor = self.raw_conn.cursor()
        try:
            if isinstance(bound_params, list):
                cursor.executemany(sql_str, bound_params)
                return ScalarResult([], [])
            elif isinstance(bound_params, dict):
                cursor.execute(sql_str, bound_params)
            else:
                cursor.execute(sql_str)

            if cursor.description:
                cols = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                return ScalarResult(rows, cols)
            return ScalarResult([], [])
        finally:
            cursor.close()

    def to_dataframe(self, sql_obj: Any, params: dict[str, Any] | None = None) -> pd.DataFrame:
        sql_str, bound_params = self._prepare_sql(sql_obj, params)
        cursor = self.raw_conn.cursor()
        try:
            if bound_params:
                cursor.execute(sql_str, bound_params)
            else:
                cursor.execute(sql_str)
            cols = [col[0] for col in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            cursor.close()


class WarehouseEngine:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.dialect = "sqlite" if db_url.startswith("sqlite") else "postgres"

    def _open_raw_connection(self) -> Any:
        if self.dialect == "sqlite":
            raw_path = self.db_url.replace("sqlite:///", "")
            if raw_path != ":memory:":
                Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(raw_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys=ON")
            return conn
        else:
            import psycopg2
            parsed = urlparse(self.db_url.replace("postgresql+psycopg2://", "postgresql://"))
            return psycopg2.connect(
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                user=parsed.username,
                password=parsed.password,
                dbname=(parsed.path or "").lstrip("/"),
            )

    @contextmanager
    def begin(self) -> Iterator[WarehouseConnection]:
        raw_conn = self._open_raw_connection()
        wrapped = WarehouseConnection(raw_conn, self.dialect)
        try:
            yield wrapped
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

    @contextmanager
    def connect(self) -> Iterator[WarehouseConnection]:
        raw_conn = self._open_raw_connection()
        wrapped = WarehouseConnection(raw_conn, self.dialect)
        try:
            yield wrapped
        finally:
            raw_conn.close()


Engine = WarehouseEngine


def create_db_engine(db_url: str | None = None) -> WarehouseEngine:
    cfg = get_config(db_url)
    return WarehouseEngine(cfg.db_url)


def initialize_warehouse(engine: WarehouseEngine | None = None, config: RetailFlowConfig | None = None) -> None:
    cfg = config or get_config()
    eng = engine or create_db_engine(cfg.db_url)
    ddl_path = cfg.sql_dir / "ddl" / "01_schema.sql"
    raw_sql = ddl_path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in raw_sql.split(";") if stmt.strip()]

    with eng.begin() as conn:
        for statement in statements:
            conn.execute(statement)


def run_sql_query(query_text: str, engine: WarehouseEngine | None = None, params: dict[str, Any] | None = None) -> pd.DataFrame:
    eng = engine or create_db_engine()
    with eng.connect() as conn:
        return conn.to_dataframe(query_text, params=params)


def run_sql_file(sql_file_path: Path, engine: WarehouseEngine | None = None, params: dict[str, Any] | None = None) -> pd.DataFrame:
    query_text = sql_file_path.read_text(encoding="utf-8")
    return run_sql_query(query_text, engine=engine, params=params)
