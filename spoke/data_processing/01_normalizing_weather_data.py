from pathlib import Path

import pandas as pd

INPUT_FILE_PREFIX = "../raw_data/weather/"
INPUT_FILE_GLOB = "*.csv"
OUTPUT_FILE = "weather_data_normalized.pkl.gz"


def process(time_period_start, time_period_end):
    weather_df = pd.concat(
        (pd.read_csv(f) for f in Path(INPUT_FILE_PREFIX).glob(INPUT_FILE_GLOB))
    )

    weather_df_by_date = weather_df.groupby("DATE").mean()

    weather_df_by_date.drop(
        columns=[
            "DAPR",
            "MDPR",
            "DASF",
            "MDSF",
            "PSUN",
            "TSUN",
            "TAVG",
            "WESD",
            "WESF",
            "WSF2",
            "WSF5",
            "WDF2",
            "WDF5",
        ],
        inplace=True,
    )

    weather_df_by_date.fillna(0, inplace=True)

    weather_df_by_date.query('index >= @time_period_start & index < @time_period_end', inplace=True)

    return weather_df_by_date


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('time_period_start')
    parser.add_argument('time_period_end')
    args = parser.parse_args()
    df = process(args.time_period_start, args.time_period_end)
    df.to_pickle(OUTPUT_FILE)
