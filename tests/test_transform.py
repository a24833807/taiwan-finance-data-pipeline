import sys
from pathlib import Path

import pandas as pd
import pytest
from pandas.api.types import is_numeric_dtype

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transform import transform_twse_daily_price


@pytest.fixture
def raw_twse_df():
    return pd.DataFrame(
        {
            "證券代號": ["2330"],
            "證券名稱": ["台積電"],
            "成交股數": ["25,000,000"],
            "開盤價": ["960.00"],
            "最高價": ["970.00"],
            "最低價": ["955.00"],
            "收盤價": ["968.00"],
        }
    )


def test_transform_twse_columns_to_standard_schema(raw_twse_df):
    result = transform_twse_daily_price(raw_twse_df, "20240701")

    assert result.columns.tolist() == [
        "stock_id",
        "stock_name",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]
    assert result.loc[0, "stock_id"] == "2330"
    assert result.loc[0, "stock_name"] == "台積電"
    assert result.loc[0, "trade_date"] == pd.Timestamp("2024-07-01")


def test_transform_volume_removes_commas_and_uses_nullable_integer(raw_twse_df):
    result = transform_twse_daily_price(raw_twse_df, "20240701")

    assert result.loc[0, "volume"] == 123
    assert result["volume"].dtype == "Int64"


def test_transform_price_columns_to_numeric(raw_twse_df):
    result = transform_twse_daily_price(raw_twse_df, "20240701")

    price_columns = ["open_price", "high_price", "low_price", "close_price"]
    assert all(is_numeric_dtype(result[column]) for column in price_columns)
    assert result.loc[0, price_columns].tolist() == [960.0, 970.0, 955.0, 968.0]


def test_transform_double_dash_to_missing_value(raw_twse_df):
    raw_twse_df.loc[0, ["成交股數", "開盤價", "最高價", "最低價", "收盤價"]] = "--"

    result = transform_twse_daily_price(raw_twse_df, "20240701")

    columns = ["volume", "open_price", "high_price", "low_price", "close_price"]
    assert result.loc[0, columns].isna().all()


def test_transform_raises_value_error_when_required_column_is_missing(raw_twse_df):
    raw_twse_df = raw_twse_df.drop(columns=["收盤價"])

    with pytest.raises(ValueError, match="收盤價"):
        transform_twse_daily_price(raw_twse_df, "20240701")
