"""Validate transformed stock daily price data before loading."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

PRICE_COLUMNS = ["open_price", "high_price", "low_price", "close_price"]
REQUIRED_COLUMNS = ["stock_id", "trade_date", *PRICE_COLUMNS, "volume"]


def _is_present(series: pd.Series) -> pd.Series:
    """Return True when values are not null or blank strings."""
    stripped = series.astype(str).str.strip()
    return series.notna() & (stripped != "") & ~stripped.str.lower().isin(
        ["nan", "none", "nat"]
    )


def validate_stock_daily_price(df: pd.DataFrame) -> pd.DataFrame:
    """Filter invalid stock daily price rows and log validation warnings."""
    if df.empty:
        logger.warning("No rows to validate before loading stock_daily_price")
        return df.copy()

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing required columns for validation: {', '.join(missing_columns)}"
        )

    validated = df.copy()
    valid_mask = pd.Series(True, index=validated.index)

    missing_stock_id = ~_is_present(validated["stock_id"])
    if missing_stock_id.any():
        logger.warning("Filtered rows with empty stock_id: %s", int(missing_stock_id.sum()))
        valid_mask &= ~missing_stock_id

    missing_trade_date = ~_is_present(validated["trade_date"])
    if missing_trade_date.any():
        logger.warning(
            "Filtered rows with empty trade_date: %s", int(missing_trade_date.sum())
        )
        valid_mask &= ~missing_trade_date

    numeric_columns = PRICE_COLUMNS + ["volume"]
    numeric_values = validated[numeric_columns].apply(pd.to_numeric, errors="coerce")

    negative_prices = numeric_values[PRICE_COLUMNS].lt(0).any(axis=1)
    if negative_prices.any():
        logger.warning("Filtered rows with negative prices: %s", int(negative_prices.sum()))
        valid_mask &= ~negative_prices

    negative_volume = numeric_values["volume"].lt(0)
    if negative_volume.any():
        logger.warning("Filtered rows with negative volume: %s", int(negative_volume.sum()))
        valid_mask &= ~negative_volume

    invalid_price_range = numeric_values["high_price"].lt(numeric_values["low_price"])
    if invalid_price_range.any():
        logger.warning(
            "Filtered rows where high_price is lower than low_price: %s",
            int(invalid_price_range.sum()),
        )
        valid_mask &= ~invalid_price_range

    filtered_count = int((~valid_mask).sum())
    if filtered_count:
        logger.warning(
            "Filtered invalid stock_daily_price rows before loading: %s", filtered_count
        )

    return validated.loc[valid_mask].copy()
