from pathlib import Path

import pandas as pd

TIME_PERIOD_START = "2019-01-01"
TIME_PERIOD_END = "2021-10-31"

INPUT_FILE_PREFIX = "pipeline_data/raw_data/weather/"
INPUT_FILE_GLOB = "*.csv"
OUTPUT_FILE = "pipeline_data/weather_data_normalized.pkl.gz"


def process():
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

    return weather_df_by_date


if __name__ == "__main__":
    df = process()
    df.to_pickle(OUTPUT_FILE)
