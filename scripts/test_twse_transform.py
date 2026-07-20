import ssl

import pandas as pd
import requests
from requests.adapters import HTTPAdapter


class SystemCertificateAdapter(HTTPAdapter):
    """Use the Windows system certificate store for HTTPS connections."""

    def init_poolmanager(
        self,
        connections,
        maxsize,
        block=False,
        **pool_kwargs,
    ):
        pool_kwargs["ssl_context"] = ssl.create_default_context()
        return super().init_poolmanager(
            connections,
            maxsize,
            block=block,
            **pool_kwargs,
        )

def fetch_twse_daily_price(trade_date: str) -> pd.DataFrame:
    """
    Fetch raw daily stock price data from TWSE.

    Args:
        trade_date: Trading date in YYYYMMDD format.

    Returns:
        Raw TWSE stock price DataFrame.
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"

    params = {
        "response": "json",
        "date": trade_date,
        "type": "ALLBUT0999",
    }

    with requests.Session() as session:
        session.mount("https://", SystemCertificateAdapter())
        response = session.get(url, params=params, timeout=10)
    response.raise_for_status()

    response_data = response.json()

    if response_data.get("stat") != "OK":
        raise ValueError(
            f"TWSE API returned unsuccessful status: "
            f"{response_data.get('stat')}"
        )

    fields = response_data.get("fields9")
    rows = response_data.get("data9")

    if not fields or not rows:
        required_fields = {
            "證券代號",
            "證券名稱",
            "成交股數",
            "開盤價",
            "最高價",
            "最低價",
            "收盤價",
        }

        for table in response_data.get("tables", []):
            table_fields = table.get("fields") or []
            table_rows = table.get("data") or []

            if required_fields.issubset(table_fields) and table_rows:
                fields = table_fields
                rows = table_rows
                break

    if not fields or not rows:
        raise ValueError(
            "TWSE API response did not contain the daily price table"
        )

    return pd.DataFrame(rows, columns=fields)


def transform_twse_daily_price(
    raw_df: pd.DataFrame,
    trade_date: str,
) -> pd.DataFrame:
    """
    Transform raw TWSE stock price data into the project's standard schema.

    Args:
        raw_df: Raw TWSE DataFrame.
        trade_date: Trading date in YYYYMMDD format.

    Returns:
        Standardized stock price DataFrame.
    """
    if raw_df.empty:
        return pd.DataFrame()

    column_mapping = {
        "證券代號": "stock_id",
        "證券名稱": "stock_name",
        "成交股數": "volume",
        "開盤價": "open_price",
        "最高價": "high_price",
        "最低價": "low_price",
        "收盤價": "close_price",
    }

    required_columns = list(column_mapping.keys())

    missing_columns = [
        column
        for column in required_columns
        if column not in raw_df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required TWSE columns: {missing_columns}"
        )

    transformed_df = raw_df[required_columns].copy()

    transformed_df = transformed_df.rename(
        columns=column_mapping
    )

    transformed_df["trade_date"] = pd.to_datetime(
        trade_date,
        format="%Y%m%d",
        errors="coerce",
    )

    transformed_df["stock_id"] = (
        transformed_df["stock_id"]
        .astype(str)
        .str.strip()
    )

    transformed_df["stock_name"] = (
        transformed_df["stock_name"]
        .astype(str)
        .str.strip()
    )

    price_columns = [
        "open_price",
        "high_price",
        "low_price",
        "close_price",
    ]

    for column in price_columns:
        transformed_df[column] = (
            transformed_df[column]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )

        transformed_df[column] = pd.to_numeric(
            transformed_df[column],
            errors="coerce",
        )

    transformed_df["volume"] = (
        transformed_df["volume"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
    )

    transformed_df["volume"] = pd.to_numeric(
        transformed_df["volume"],
        errors="coerce",
    ).astype("Int64")

    output_columns = [
        "stock_id",
        "stock_name",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
    ]

    return transformed_df[output_columns]


if __name__ == "__main__":
    trade_date = "20240701"

    raw_df = fetch_twse_daily_price(trade_date)

    print("Raw DataFrame shape:")
    print(raw_df.shape)

    print("\nRaw DataFrame preview:")
    print(raw_df.head())

    transformed_df = transform_twse_daily_price(
        raw_df,
        trade_date,
    )
    
    print("\nTransformed DataFrame shape:")
    print(transformed_df.shape)

    print("\nTransformed DataFrame columns:")
    print(transformed_df.columns.tolist())

    print("\nTransformed DataFrame data types:")
    print(transformed_df.dtypes)

    print("\nTransformed DataFrame preview:")
    print(transformed_df.head())

    print("\nNull count:")
    print(transformed_df.isna().sum())
