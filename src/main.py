"""Command line entry point for the Taiwan finance ETL pipeline."""

import argparse
import logging
from datetime import datetime, timedelta
from extract import extract_twse_daily_price
from load import load_stock_daily_price
from transform import transform_twse_daily_price
from validate import validate_stock_daily_price


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run Taiwan stock daily price ETL")
    parser.add_argument(
        "--trade-date",
        type=validate_trade_date,
        help="Run ETL for one trade date in YYYYMMDD format.",
    )
    parser.add_argument(
        "--start-date",
        type=validate_trade_date,
        help="Start date for backfill in YYYYMMDD format.",
    )
    parser.add_argument(
        "--end-date",
        type=validate_trade_date,
        help="End date for backfill in YYYYMMDD format.",
    )
    args = parser.parse_args()

    has_date_range = args.start_date is not None or args.end_date is not None
    if args.trade_date and has_date_range:
        parser.error("--trade-date cannot be used with --start-date or --end-date")
    if (args.start_date is None) != (args.end_date is None):
        parser.error("--start-date and --end-date must be provided together")
    if not args.trade_date and not has_date_range:
        parser.error("provide --trade-date or both --start-date and --end-date")
    if args.start_date and args.start_date > args.end_date:
        parser.error("--start-date cannot be later than --end-date")

    return args


def configure_logging() -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def run_pipeline(trade_date: str) -> int:
    """Run the ETL workflow for one date and return the loaded row count."""
    logger = logging.getLogger(__name__)

    logger.info("Starting ETL pipeline for trade_date=%s", trade_date)
    raw_df = extract_twse_daily_price(trade_date)
    logger.info("Extracted rows=%s", len(raw_df))

    if raw_df.empty:
        logger.info("Transformed rows=0")
        logger.info("Validated rows=0")
        logger.info("Loaded rows=0")
        logger.info("ETL pipeline completed. loaded_rows=0")
        return 0

    transformed_df = transform_twse_daily_price(raw_df, trade_date)
    logger.info("Transformed rows=%s", len(transformed_df))
    validated_df = validate_stock_daily_price(transformed_df)
    logger.info("Validated rows=%s", len(validated_df))
    loaded_rows = load_stock_daily_price(validated_df)
    logger.info("Loaded rows=%s", loaded_rows)
    logger.info("ETL pipeline completed. loaded_rows=%s", loaded_rows)

    return loaded_rows


def iter_dates(start_date: str, end_date: str):
    """Yield every date from start_date through end_date in YYYYMMDD format."""
    current_date = datetime.strptime(start_date, "%Y%m%d")
    last_date = datetime.strptime(end_date, "%Y%m%d")

    while current_date <= last_date:
        yield current_date.strftime("%Y%m%d")
        current_date += timedelta(days=1)


def main(args: argparse.Namespace) -> None:
    """Run either a single-date ETL or an inclusive date-range backfill."""
    if args.trade_date:
        run_pipeline(args.trade_date)
        return

    logger = logging.getLogger(__name__)
    processed_dates = 0
    total_loaded_rows = 0

    for trade_date in iter_dates(args.start_date, args.end_date):
        total_loaded_rows += run_pipeline(trade_date)
        processed_dates += 1

    logger.info(
        "Backfill completed. processed_dates=%s total_loaded_rows=%s",
        processed_dates,
        total_loaded_rows,
    )


def validate_trade_date(value: str) -> str:
    """
    Validate that the trade date follows YYYYMMDD format.

    Args:
        value: Trade date string from command-line arguments.

    Returns:
        The validated trade date string.

    Raises:
        argparse.ArgumentTypeError: If the date format is invalid.
    """
    if len(value) != 8 or not value.isdigit():
        raise argparse.ArgumentTypeError(
            "trade date must be a valid date in YYYYMMDD format"
        )

    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "trade date must be a valid date in YYYYMMDD format"
        ) from error

    return value


if __name__ == "__main__":
    configure_logging()
    args = parse_args()
    main(args)
