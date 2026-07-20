"""Load transformed stock daily price data into PostgreSQL."""

import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import BigInteger, Column, Date, MetaData, Numeric, String, Table

from db import get_engine

logger = logging.getLogger(__name__)

metadata = MetaData()

stock_daily_price = Table(
    "stock_daily_price",
    metadata,
    Column("stock_id", String(20), nullable=False),
    Column("stock_name", String(100)),
    Column("trade_date", Date, nullable=False),
    Column("open_price", Numeric(12, 2)),
    Column("high_price", Numeric(12, 2)),
    Column("low_price", Numeric(12, 2)),
    Column("close_price", Numeric(12, 2)),
    Column("volume", BigInteger),
)


def load_stock_daily_price(df: pd.DataFrame) -> int:
    """Upsert stock daily price records into PostgreSQL."""
    if df.empty:
        logger.warning("No rows to load into stock_daily_price")
        return 0

    records = df.to_dict(orient="records")
    stmt = insert(stock_daily_price).values(records)
    insert_stmt = stmt.on_conflict_do_nothing(
        index_elements=["stock_id", "trade_date"],
    )

    engine = get_engine()
    with engine.begin() as connection:
        result = connection.execute(insert_stmt)

    row_count = result.rowcount or 0
    logger.info("Loaded %s rows into stock_daily_price", row_count)
    return row_count
