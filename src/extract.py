"""Extract Taiwan stock daily price data from public data sources."""

import logging
import requests
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_stock_daily_price(trade_date: str) -> pd.DataFrame:
    """Read stock daily price data from the local CSV file.

    If trade_date is provided, only rows matching that YYYYMMDD date are
    returned.
    """
    logger.info("Extracting stock daily price data for trade_date=%s", trade_date)
    file_path = Path(__file__).resolve().parents[1] / "data/raw/stock_daily_price.csv"
    df = pd.read_csv(file_path)

    if trade_date:
        requested_date = pd.to_datetime(
            trade_date,
            format="%Y%m%d",
            errors="raise",
        )
        csv_dates = pd.to_datetime(df["trade_date"], errors="coerce")
        df = df.loc[csv_dates == requested_date].copy()

    return df


def extract_twse_daily_price(trade_date: str) -> pd.DataFrame:
    """
    Extract daily stock price data from the TWSE MI_INDEX endpoint.

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

    logger.info("Extracting TWSE daily price data for %s", trade_date)

    response = requests.get(
        url,
        params=params,
        timeout=10,
    )
    response.raise_for_status()

    response_data = response.json()

    if response_data.get("stat") != "OK":
        logger.warning(
            "TWSE returned unsuccessful status: %s",
            response_data.get("stat"),
        )
        return pd.DataFrame()

    required_fields = {
        "證券代號",
        "證券名稱",
        "成交股數",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
    }
    fields = None
    rows = None

    for table in response_data.get("tables", []):
        table_fields = table.get("fields") or []
        table_rows = table.get("data") or []

        if required_fields.issubset(table_fields) and table_rows:
            fields = table_fields
            rows = table_rows
            break

    if not fields or not rows:
        logger.warning(
            "TWSE returned no stock price data for %s",
            trade_date,
        )
        return pd.DataFrame()

    raw_df = pd.DataFrame(rows, columns=fields)

    logger.info("Extracted %s rows from TWSE", len(raw_df))

    return raw_df
