import argparse
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from main import iter_dates, validate_trade_date


def test_validate_trade_date_with_valid_date():
    assert validate_trade_date("20240701") == "20240701"


def test_validate_trade_date_with_wrong_format():
    with pytest.raises(argparse.ArgumentTypeError):
        validate_trade_date("2024-07-01")


def test_validate_trade_date_with_invalid_date():
    with pytest.raises(argparse.ArgumentTypeError):
        validate_trade_date("20240230")


def test_iter_dates_includes_full_date_range():
    assert list(iter_dates("20240701", "20240703")) == [
        "20240701",
        "20240702",
        "20240703",
    ]


def test_iter_dates_with_same_start_and_end():
    assert list(iter_dates("20240701", "20240701")) == ["20240701"]
