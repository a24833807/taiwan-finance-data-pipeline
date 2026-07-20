"""Transform raw stock daily price data into a normalized schema."""

from typing import Optional

import pandas as pd

OUTPUT_COLUMNS = [
    "stock_id",
    "stock_name",
    "trade_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
]

NUMERIC_COLUMNS = ["open_price", "high_price", "low_price", "close_price"]
REQUIRED_COLUMNS = OUTPUT_COLUMNS.copy()


def _clean_numeric_series(series: pd.Series) -> pd.Series:
    """Remove non-numeric characters and convert values to numeric."""
    cleaned = (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def transform_stock_daily_price(
    df: pd.DataFrame, trade_date: Optional[str]
) -> pd.DataFrame:
    """Clean raw stock daily price data into the PostgreSQL target schema.

    The function validates required columns, normalizes numeric fields,
    uses the provided trade date when present or the CSV trade_date column
    otherwise, removes rows with empty stock identifiers or names, and returns
    a DataFrame with a fixed column order.
    """
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {', '.join(missing_columns)}"
        )

    transformed = df.copy()

    # External trade_date has priority; otherwise keep the date from the CSV.
    if trade_date:
        transformed["trade_date"] = pd.to_datetime(
            trade_date, format="%Y%m%d", errors="coerce"
        ).date()
    else:
        transformed["trade_date"] = pd.to_datetime(
            transformed["trade_date"], errors="coerce"
        ).dt.date

    transformed["stock_id"] = (
        transformed["stock_id"].astype(str).str.strip().str.strip("'\"").str.strip()
    )
    transformed["stock_name"] = (
        transformed["stock_name"].astype(str).str.strip().str.strip("'\"").str.strip()
    )
    transformed = transformed[
        (transformed["stock_id"] != "")
        & (transformed["stock_id"].str.lower() != "nan")
        & (transformed["stock_name"] != "")
        & (transformed["stock_name"].str.lower() != "nan")
    ].copy()

    for column in NUMERIC_COLUMNS:
        transformed[column] = _clean_numeric_series(transformed[column])

    transformed["volume"] = _clean_numeric_series(transformed["volume"]).fillna(0).astype(
        "int64"
    )

    return transformed[OUTPUT_COLUMNS]


def transform_twse_daily_price(
    raw_df: pd.DataFrame,
    trade_date: str,
) -> pd.DataFrame:
    """
    Transform raw TWSE stock price data into the standard project schema.

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