import json
import csv
import os
import datetime
import requests_cache as rc
from time import perf_counter, sleep
import pandas as pd

# Declare cached session:
session = rc.CachedSession(
    allowable_codes=(200,),
    cache_name="coinmarketcap_cache",
    backend='filesystem',
    ignored_parameters=["api_key"]  # Should not affect result
)

# Declare constants:
BASE_URL = "https://pro-api.coinmarketcap.com"
START_DATE, END_DATE = datetime.date(year=2015, month=1, day=1), datetime.date(year=2022, month=1, day=1)
STEP = datetime.timedelta(days=7)
REQUEST_LIMIT = 60  # requests/min
print(f"Reference range set from {START_DATE.isoformat()} to {END_DATE.isoformat()}")
print(f"Step is {STEP.days} days.")

# Read key from file (see https://coinmarketcap.com/api/documentation) for header shape:
try:
    with open("keys.json", 'r') as key_file:
        KEY_HEADER = json.loads(key_file.read())
        print(f"Key loaded successfully: {KEY_HEADER['X-CMC_PRO_API_KEY'][:5]}***")
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Key file 'keys.json' is missing or corrupted: {e}")
    exit()

# Form shared request headers:
HEADER = {
    **KEY_HEADER,
    "Accept": "application/json",
    "Accept-Encoding": 'deflate, gzip'
}


# Querry for data:
def get_data(url, params=None):
    ask = session.get(url, params=params, headers=HEADER)
    json_content = ask.json()
    if ask.status_code != 200 or not json_content["data"]:
        raise ValueError(f" Bad response! Code: {ask.status_code}; URL: {ask.url}")
    return json_content["data"]


# Script body:
if __name__ == '__main__':

    listing_endpoint = "/v1/cryptocurrency/listings/historical"
    current_date = START_DATE
    week_counter = 1

    # Requests are strictly limited n/min, so limit by sleeping here:
    def get_snapshot(date: datetime.date):
        request_start = perf_counter()
        data_dict = get_data(
            BASE_URL + listing_endpoint,
            params={
                "date": date.isoformat(),
                "limit": 5000,
                "start": 2
            }
        )
        request_end = perf_counter()
        sleep(max(0., (60/REQUEST_LIMIT - (request_end-request_start))))
        return data_dict

    # Prepare response for serialization in CSV:
    def flatten(coin_stats: dict):
        output_dict = dict()
        for key, value in coin_stats.items():
            # A dict of dicts (unique case):
            if key == "quote":
                for currency, details in value.items():
                    for detail_key, detail_value in details.items():
                        output_dict[currency + "_" + detail_key] = detail_value

            elif isinstance(value, dict):

                for nested_key, nested_value in value.items():
                    output_dict[key + '_' + nested_key] = nested_value

            elif isinstance(value, list):
                output_dict[key] = ','.join(value)

            else:
                output_dict[key] = value

        return output_dict

    while current_date < END_DATE:
        data = get_snapshot(current_date)
        serialized_coins = [flatten(coin) for coin in data]

        # Create directory for CSVs:
        os.makedirs("historical_CSV", exist_ok=True)

        # Dump CSV for current date:
        dataframe = pd.DataFrame(data=serialized_coins)
        dataframe.to_csv(
            os.path.join("historical_CSV", f"{current_date.isoformat()}.csv"),
            index=False,
            quoting=csv.QUOTE_NONNUMERIC
        )
        if week_counter == 1 or week_counter % 25 == 0:
            print(f"Processed {current_date.isoformat()}; Currently on step {week_counter}...")

        # Increment
        current_date += STEP
        week_counter += 1

    print(f"Done. Processed {week_counter} pages. Actual last date: {current_date}")
