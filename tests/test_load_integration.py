import os
import sys
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from load import load_stock_daily_price


TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    (
        "postgresql+psycopg2://"
        "test_user:test_password@localhost:5433/stock_test_db"
    ),
)

test_engine = create_engine(TEST_DATABASE_URL)


@pytest.fixture(autouse=True)
def clean_stock_table():
    '''進行測試前清空 stock_daily_price 資料表，並在測試後再次清空資料表。'''
    with test_engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE stock_daily_price RESTART IDENTITY")
        )

    yield

    with test_engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE stock_daily_price RESTART IDENTITY")
        )


def create_sample_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stock_id": ["2330"],
            "stock_name": ["台積電"],
            "trade_date": [pd.Timestamp("2024-07-01")],
            "open_price": [960.0],
            "high_price": [970.0],
            "low_price": [955.0],
            "close_price": [968.0],
            "volume": [25000000],
        }
    )


def test_load_stock_daily_price_inserts_data():
    input_df = create_sample_dataframe()

    loaded_rows = load_stock_daily_price(
        input_df,
        test_engine,
    )

    assert loaded_rows == 1

    with test_engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT
                    stock_id,
                    stock_name,
                    trade_date,
                    close_price,
                    volume
                FROM stock_daily_price
                """
            )
        ).mappings().one()

    assert result["stock_id"] == "2330"
    assert result["stock_name"] == "台積電"
    assert str(result["trade_date"]) == "2024-07-01"
    assert float(result["close_price"]) == 968.0
    assert result["volume"] == 25000000


def test_load_same_data_twice_does_not_duplicate():
    input_df = create_sample_dataframe()

    first_loaded_rows = load_stock_daily_price(
        input_df,
        test_engine,
    )

    second_loaded_rows = load_stock_daily_price(
        input_df,
        test_engine,
    )

    with test_engine.connect() as connection:
        row_count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM stock_daily_price
                WHERE stock_id = '2330'
                AND trade_date = '2024-07-01'
                """
            )
        ).scalar_one()

    assert first_loaded_rows == 1
    assert second_loaded_rows == 0
    assert row_count == 1
