import pandas as pd

def oer_extract(raw_data: dict) -> pd.DataFrame:
    timestamp = raw_data["timestamp"] # UTC timestamp indicating the time the data was collected
    pandas_timestamp = pd.Timestamp(timestamp, unit="s")
    base_currency = raw_data["base"]
    rates = raw_data["rates"]
    df = pd.DataFrame.from_dict(rates, orient="index", columns=["rate"])
    df["timestamp"] = pandas_timestamp
    df["base_currency"] = base_currency
    df.reset_index(inplace=True)
    df.rename(columns={"index": "exchange_currency"}, inplace=True)
    return df