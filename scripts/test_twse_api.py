import requests
import pandas as pd


def fetch_twse_daily_price(trade_date: str) -> pd.DataFrame:
    """
    Fetch TWSE daily closing price data by trade date.

    Args:
        trade_date: Date string in YYYYMMDD format, for example "20240701".

    Returns:
        A pandas DataFrame containing raw TWSE daily price data.
    """
    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"

    params = {
        "response": "json",
        "date": trade_date,
        "type": "ALLBUT0999",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    print("Response keys:")
    print(data.keys())

    print("\nResponse stat:")
    print(data.get("stat"))

    # TWSE MI_INDEX JSON structure may contain multiple tables.
    # We print keys first so we can inspect which data section contains stock price records.
    for key, value in data.items():
        if isinstance(value, list):
            print(f"\nKey: {key}, rows: {len(value)}")
            if len(value) > 0:
                print("First row:")
                print(value[0])

    # Support both older fields9/data9 responses and newer tables responses.
    fields = data.get("fields9")
    rows = data.get("data9")

    if not fields or not rows:
        for table in data.get("tables", []):
            table_fields = table.get("fields")
            table_rows = table.get("data")

            if (
                table_fields
                and table_rows
                and "證券代號" in table_fields
                and "證券名稱" in table_fields
            ):
                fields = table_fields
                rows = table_rows
                break

    if not fields or not rows:
        print("\nNo stock price table found. Please inspect the response structure above.")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=fields)
    print(df)
    return df


if __name__ == "__main__":
    # Use a past trading day first.
    # If the selected date is a weekend or holiday, TWSE may return no data.
    trade_date = "20240708"

    df = fetch_twse_daily_price(trade_date)

    print("\nDataFrame shape:")
    print(df.shape)

    print("\nDataFrame columns:")
    print(df.columns.tolist())

    print("\nDataFrame preview:")
    print(df.head())
