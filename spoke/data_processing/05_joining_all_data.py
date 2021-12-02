import pandas as pd
import osmnx as ox

INPUT_FILE_TRIPS = "trip_data_normalized.pkl.gz"
INPUT_FILE_CRASHES = "crash_data_normalized_with_node_graph.pkl.gz"
INPUT_FILE_WEATHER = "weather_data_normalized.pkl.gz"
OUTPUT_FILE = "unified_dataset.pkl.gz"


def normalize_trip_df(trip_df):
    time_df = trip_df["starttime"].str.split(" ", 1, expand=True)

    trip_df_norm = trip_df.drop(
        columns=[
            "TRIP_ID",
            "starttime",
            "stoptime",
            "start station id",
            "start station name",
            "start station latitude",
            "tripduration",
            "start station longitude",
            "end station id",
            "end station name",
            "end station latitude",
            "end station longitude",
            "bikeid",
            "usertype",
            "birth year",
            "gender",
        ]
    )

    trip_df_norm["NUMBER OF PERSONS INJURED"] = trip_df_norm[
        "NUMBER OF PERSONS KILLED"
    ] = 0
    trip_df_norm["NUMBER OF PEDESTRIANS INJURED"] = trip_df_norm[
        "NUMBER OF PEDESTRIANS KILLED"
    ] = 0
    trip_df_norm["NUMBER OF CYCLIST INJURED"] = trip_df_norm[
        "NUMBER OF CYCLIST KILLED"
    ] = 0
    trip_df_norm["NUMBER OF MOTORIST INJURED"] = trip_df_norm[
        "NUMBER OF MOTORIST KILLED"
    ] = 0
    trip_df_norm["EVENT_DIST_FROM_NODE"] = 0

    trip_df_norm["EVENT_DATE"] = time_df[0]
    trip_df_norm["EVENT_TIME"] = time_df[1]

    trip_df_norm["IS_CRASH"] = False

    return trip_df_norm


def normalize_crash_df(crash_df):
    crash_df_norm = (
        crash_df.drop(
            columns=[
                "LATITUDE",
                "LONGITUDE",
                "LOCATION",
                "ZIP CODE",
                "BOROUGH",
                *[f"VEHICLE TYPE CODE {i+1}" for i in range(5)],
                *[f"CONTRIBUTING FACTOR VEHICLE {i+1}" for i in range(5)],
                "ON STREET NAME",
                "CROSS STREET NAME",
                "OFF STREET NAME",
                "COLLISION_ID",
            ]
        )
        .rename(
            columns={
                "CRASH DATE": "EVENT_DATE",
                "CRASH TIME": "EVENT_TIME",
                "NODE_DIST_FROM_CRASH_M": "EVENT_DIST_FROM_NODE",
            }
        )
        .assign(
            EVENT_DATE=lambda df: pd.to_datetime(df["EVENT_DATE"]).dt.date.astype(str),
            EVENT_TIME=lambda df: df["EVENT_TIME"] + ":00.0000",
            IS_CRASH=lambda df: True,
        )
    )

    return crash_df_norm


def process():
    trip_df_norm = normalize_trip_df(pd.read_pickle(INPUT_FILE_TRIPS))
    crash_df_norm = normalize_crash_df(pd.read_pickle(INPUT_FILE_CRASHES))

    all_event_df = pd.concat((trip_df_norm, crash_df_norm)).reset_index(drop=True)

    weather_df = pd.read_pickle(INPUT_FILE_WEATHER)
    all_event_df_with_weather = pd.merge(
        all_event_df, weather_df, left_on="EVENT_DATE", right_on="DATE", how="left"
    )

    all_event_df_with_weather.dropna(inplace=True)

    return all_event_df_with_weather


if __name__ == "__main__":
    df = process()
    df.to_pickle(OUTPUT_FILE)
