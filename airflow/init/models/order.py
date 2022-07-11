from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Float,
    Identity,
    Integer,
    String,
    Table,
)
from utils.database import metadata

order = Table(
    "order",
    metadata,
    Column("id", Integer, Identity(start=1), primary_key=True),
    Column("order_id", Integer),
    Column("tax_amount", Float),
    Column("tax_excluded_amount", Float),
    Column("discount_amount", Float),
    Column("service_fee_amount", Float),
    Column("sales_amount", Float),
    Column("is_order_paid", Boolean),
    Column("order_created_at", TIMESTAMP),
    Column("order_updated_at", TIMESTAMP),
    Column("order_source", String),
    Column("order_option", String),
    Column("order_status", String),
    Column("is_valid_record", Boolean),
    Column("employee_key", String),
    Column("first_name", String),
    Column("last_name", String),
    Column("is_order_deleted", Boolean),
)
