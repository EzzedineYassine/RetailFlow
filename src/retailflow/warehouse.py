from dataclasses import dataclass
from datetime import datetime, timezone
from retailflow.db import Engine, text
from retailflow.sample_data import infer_product_category


@dataclass(frozen=True)
class WarehouseLoadResult:
    batch_id: str
    dates_upserted: int
    customers_upserted: int
    products_upserted: int
    facts_upserted: int


def classify_customer_segment(
    is_guest: int,
    lifetime_orders: int,
    lifetime_net_revenue: float,
) -> str:
    if is_guest == 1:
        return "Guest Checkout"
    if lifetime_net_revenue >= 750.0 or lifetime_orders >= 6:
        return "VIP Wholesale"
    if lifetime_net_revenue >= 250.0 or lifetime_orders >= 3:
        return "Loyal Repeat"
    if lifetime_orders >= 1 and lifetime_net_revenue > 0:
        return "Active Standard"
    return "At-Risk / Return Only"


def build_warehouse_star_schema(
    batch_id: str,
    run_id: str,
    engine: Engine,
) -> WarehouseLoadResult:
    now_iso = datetime.now(timezone.utc).isoformat()

    with engine.connect() as conn:
        batch_stg_df = conn.to_dataframe(
            text(
                """
                SELECT
                    stg_line_id, batch_id, run_id, invoice_no, stock_code,
                    description, quantity, invoice_timestamp, invoice_date,
                    date_key, unit_price, gross_amount, return_amount,
                    net_amount, customer_id, is_guest_customer, country,
                    transaction_type, is_cancelled, is_return
                FROM stg_transactions
                WHERE batch_id = :batch_id
                """
            ),
            params={"batch_id": batch_id},
        )

    if batch_stg_df.empty:
        return WarehouseLoadResult(
            batch_id=batch_id,
            dates_upserted=0,
            customers_upserted=0,
            products_upserted=0,
            facts_upserted=0,
        )

    fact_records: list[dict[str, object]] = []
    for row in batch_stg_df.to_dict(orient="records"):
        cust_key = f"CUST_{row['customer_id']}"
        prod_key = f"PROD_{row['stock_code']}"
        fact_records.append(
            {
                "sales_fact_id": str(row["stg_line_id"]),
                "invoice_no": str(row["invoice_no"]),
                "product_key": prod_key,
                "customer_key": cust_key,
                "date_key": int(row["date_key"]),
                "invoice_timestamp": str(row["invoice_timestamp"]),
                "country": str(row["country"]),
                "transaction_type": str(row["transaction_type"]),
                "is_cancelled": int(row["is_cancelled"]),
                "is_return": int(row["is_return"]),
                "quantity": int(row["quantity"]),
                "unit_price": float(row["unit_price"]),
                "gross_amount": float(row["gross_amount"]),
                "return_amount": float(row["return_amount"]),
                "net_amount": float(row["net_amount"]),
                "batch_id": batch_id,
                "last_run_id": run_id,
                "loaded_at": now_iso,
            }
        )

    with engine.connect() as conn:
        all_stg_df = conn.to_dataframe(
            text(
                """
                SELECT
                    stg_line_id, batch_id, invoice_no, stock_code, description,
                    quantity, invoice_date, date_key, unit_price, gross_amount,
                    return_amount, net_amount, customer_id, is_guest_customer,
                    country, is_cancelled, is_return
                FROM stg_transactions
                """
            ),
        )

    unique_dates = all_stg_df[["date_key", "invoice_date"]].drop_duplicates()
    date_records: list[dict[str, object]] = []
    for d_row in unique_dates.to_dict(orient="records"):
        dt_obj = datetime.strptime(str(d_row["invoice_date"]), "%Y-%m-%d")
        weekday_idx = dt_obj.isoweekday()
        date_records.append(
            {
                "date_key": int(d_row["date_key"]),
                "full_date": dt_obj.strftime("%Y-%m-%d"),
                "year": dt_obj.year,
                "quarter": (dt_obj.month - 1) // 3 + 1,
                "month": dt_obj.month,
                "month_name": dt_obj.strftime("%B"),
                "year_month": dt_obj.strftime("%Y-%m"),
                "week_of_year": int(dt_obj.isocalendar()[1]),
                "day_of_month": dt_obj.day,
                "day_of_week": weekday_idx,
                "day_name": dt_obj.strftime("%A"),
                "is_weekend": 1 if weekday_idx in (6, 7) else 0,
            }
        )

    customer_records: list[dict[str, object]] = []
    for cust_id, group in all_stg_df.groupby("customer_id", as_index=False):
        is_guest = int(group["is_guest_customer"].iloc[0])
        primary_country = str(group["country"].mode().iloc[0]) if not group["country"].empty else "Unspecified"
        first_date = str(group["invoice_date"].min())
        last_date = str(group["invoice_date"].max())

        completed_mask = (group["is_cancelled"] == 0) & (group["is_return"] == 0)
        cancelled_mask = (group["is_cancelled"] == 1) | (group["is_return"] == 1)

        lifetime_orders = int(group.loc[completed_mask, "invoice_no"].nunique())
        lifetime_cancelled_orders = int(group.loc[cancelled_mask, "invoice_no"].nunique())
        gross_rev = round(float(group["gross_amount"].sum()), 2)
        ret_amt = round(float(group["return_amount"].sum()), 2)
        net_rev = round(float(group["net_amount"].sum()), 2)

        segment = classify_customer_segment(is_guest, lifetime_orders, net_rev)

        customer_records.append(
            {
                "customer_key": f"CUST_{cust_id}",
                "customer_id": str(cust_id),
                "is_guest": is_guest,
                "primary_country": primary_country,
                "first_purchase_date": first_date,
                "last_purchase_date": last_date,
                "lifetime_orders": lifetime_orders,
                "lifetime_cancelled_orders": lifetime_cancelled_orders,
                "lifetime_gross_revenue": gross_rev,
                "lifetime_return_amount": ret_amt,
                "lifetime_net_revenue": net_rev,
                "customer_segment": segment,
                "updated_at": now_iso,
            }
        )

    product_records: list[dict[str, object]] = []
    for stock_code, group in all_stg_df.groupby("stock_code", as_index=False):
        canonical_desc = str(group["description"].mode().iloc[0]) if not group["description"].empty else "UNSPECIFIED"
        category = infer_product_category(str(stock_code), canonical_desc)
        avg_price = round(float(group["unit_price"].mean()), 2)
        min_price = round(float(group["unit_price"].min()), 2)
        max_price = round(float(group["unit_price"].max()), 2)

        sales_mask = (group["is_cancelled"] == 0) & (group["is_return"] == 0)
        return_mask = (group["is_cancelled"] == 1) | (group["is_return"] == 1)

        units_sold = int(group.loc[sales_mask, "quantity"].sum())
        units_returned = int(group.loc[return_mask, "quantity"].abs().sum())
        net_rev = round(float(group["net_amount"].sum()), 2)
        first_sold = str(group["invoice_date"].min())
        last_sold = str(group["invoice_date"].max())

        product_records.append(
            {
                "product_key": f"PROD_{stock_code}",
                "stock_code": str(stock_code),
                "canonical_description": canonical_desc,
                "product_category": category,
                "avg_unit_price": avg_price,
                "min_unit_price": min_price,
                "max_unit_price": max_price,
                "total_units_sold": units_sold,
                "total_units_returned": units_returned,
                "net_revenue": net_rev,
                "first_sold_date": first_sold,
                "last_sold_date": last_sold,
                "updated_at": now_iso,
            }
        )

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM fact_sales WHERE batch_id = :batch_id"),
            {"batch_id": batch_id},
        )
        for fact in fact_records:
            conn.execute(
                text("DELETE FROM fact_sales WHERE sales_fact_id = :sales_fact_id"),
                {"sales_fact_id": fact["sales_fact_id"]},
            )

        if date_records:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_date (
                        date_key, full_date, year, quarter, month, month_name,
                        year_month, week_of_year, day_of_month, day_of_week,
                        day_name, is_weekend
                    ) VALUES (
                        :date_key, :full_date, :year, :quarter, :month, :month_name,
                        :year_month, :week_of_year, :day_of_month, :day_of_week,
                        :day_name, :is_weekend
                    )
                    ON CONFLICT (date_key) DO UPDATE SET
                        full_date = EXCLUDED.full_date,
                        year = EXCLUDED.year,
                        quarter = EXCLUDED.quarter,
                        month = EXCLUDED.month,
                        month_name = EXCLUDED.month_name,
                        year_month = EXCLUDED.year_month,
                        week_of_year = EXCLUDED.week_of_year,
                        day_of_month = EXCLUDED.day_of_month,
                        day_of_week = EXCLUDED.day_of_week,
                        day_name = EXCLUDED.day_name,
                        is_weekend = EXCLUDED.is_weekend
                    """
                ),
                date_records,
            )

        if customer_records:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_customer (
                        customer_key, customer_id, is_guest, primary_country,
                        first_purchase_date, last_purchase_date, lifetime_orders,
                        lifetime_cancelled_orders, lifetime_gross_revenue,
                        lifetime_return_amount, lifetime_net_revenue,
                        customer_segment, updated_at
                    ) VALUES (
                        :customer_key, :customer_id, :is_guest, :primary_country,
                        :first_purchase_date, :last_purchase_date, :lifetime_orders,
                        :lifetime_cancelled_orders, :lifetime_gross_revenue,
                        :lifetime_return_amount, :lifetime_net_revenue,
                        :customer_segment, :updated_at
                    )
                    ON CONFLICT (customer_key) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        is_guest = EXCLUDED.is_guest,
                        primary_country = EXCLUDED.primary_country,
                        first_purchase_date = EXCLUDED.first_purchase_date,
                        last_purchase_date = EXCLUDED.last_purchase_date,
                        lifetime_orders = EXCLUDED.lifetime_orders,
                        lifetime_cancelled_orders = EXCLUDED.lifetime_cancelled_orders,
                        lifetime_gross_revenue = EXCLUDED.lifetime_gross_revenue,
                        lifetime_return_amount = EXCLUDED.lifetime_return_amount,
                        lifetime_net_revenue = EXCLUDED.lifetime_net_revenue,
                        customer_segment = EXCLUDED.customer_segment,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                customer_records,
            )

        if product_records:
            conn.execute(
                text(
                    """
                    INSERT INTO dim_product (
                        product_key, stock_code, canonical_description,
                        product_category, avg_unit_price, min_unit_price,
                        max_unit_price, total_units_sold, total_units_returned,
                        net_revenue, first_sold_date, last_sold_date, updated_at
                    ) VALUES (
                        :product_key, :stock_code, :canonical_description,
                        :product_category, :avg_unit_price, :min_unit_price,
                        :max_unit_price, :total_units_sold, :total_units_returned,
                        :net_revenue, :first_sold_date, :last_sold_date, :updated_at
                    )
                    ON CONFLICT (product_key) DO UPDATE SET
                        stock_code = EXCLUDED.stock_code,
                        canonical_description = EXCLUDED.canonical_description,
                        product_category = EXCLUDED.product_category,
                        avg_unit_price = EXCLUDED.avg_unit_price,
                        min_unit_price = EXCLUDED.min_unit_price,
                        max_unit_price = EXCLUDED.max_unit_price,
                        total_units_sold = EXCLUDED.total_units_sold,
                        total_units_returned = EXCLUDED.total_units_returned,
                        net_revenue = EXCLUDED.net_revenue,
                        first_sold_date = EXCLUDED.first_sold_date,
                        last_sold_date = EXCLUDED.last_sold_date,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                product_records,
            )

        if fact_records:
            conn.execute(
                text(
                    """
                    INSERT INTO fact_sales (
                        sales_fact_id, invoice_no, product_key, customer_key,
                        date_key, invoice_timestamp, country, transaction_type,
                        is_cancelled, is_return, quantity, unit_price,
                        gross_amount, return_amount, net_amount, batch_id,
                        last_run_id, loaded_at
                    ) VALUES (
                        :sales_fact_id, :invoice_no, :product_key, :customer_key,
                        :date_key, :invoice_timestamp, :country, :transaction_type,
                        :is_cancelled, :is_return, :quantity, :unit_price,
                        :gross_amount, :return_amount, :net_amount, :batch_id,
                        :last_run_id, :loaded_at
                    )
                    """
                ),
                fact_records,
            )

    return WarehouseLoadResult(
        batch_id=batch_id,
        dates_upserted=len(date_records),
        customers_upserted=len(customer_records),
        products_upserted=len(product_records),
        facts_upserted=len(fact_records),
    )
