import sys
from os.path import abspath, join

import pandas as pd

INPUT_FILE = "../raw_data/Motor_Vehicle_Collisions_-_Crashes.csv"
OUTPUT_FILE = "crash_data_normalized.pkl.gz"


def process(time_period_start, time_period_end):
    # Load raw data
    crash_df = pd.read_csv(
        INPUT_FILE, parse_dates=["CRASH DATE"], infer_datetime_format=True
    )

    # Filter for desired time period
    crash_df.query(
        '`CRASH DATE` >= @time_period_start & `CRASH DATE` <= @time_period_end',
        inplace=True,
    )

    # Filter for crashes that involved bicycles or pedestrians
    crash_df.query(
        "(`NUMBER OF PEDESTRIANS KILLED` > 0)"
        "| (`NUMBER OF PEDESTRIANS INJURED` > 0)"
        "| (`NUMBER OF CYCLIST KILLED` > 0)"
        "| (`NUMBER OF CYCLIST INJURED` > 0)"
        "|"
        + " | ".join(
            [
                f'`VEHICLE TYPE CODE {i+1}`.str.contains("Bike", case=False, na=False)'
                for i in range(5)
            ]
        ),
        inplace=True,
    )

    # Filter for crashes that have available location information
    crash_df.query(
        "~LATITUDE.isna() & ~LONGITUDE.isna() & LATITUDE != 0 & LONGITUDE != 0",
        inplace=True,
    )

    return crash_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('time_period_start')
    parser.add_argument('time_period_end')
    args = parser.parse_args()
    df = process(args.time_period_start, args.time_period_end)
    df.to_pickle(OUTPUT_FILE)
