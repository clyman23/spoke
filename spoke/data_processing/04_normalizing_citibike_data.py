from pathlib import Path
from zipfile import ZipFile

import dask.dataframe as dd
import numpy as np
import osmnx as ox
import pandas as pd
from dask.diagnostics import ProgressBar
from networkx import NetworkXNoPath
from tqdm import tqdm
from datetime import datetime

INPUT_FILE_GRAPH = "target_map.graphml"
INPUT_FILE_PREFIX = "../raw_data/citibike/"

OUTPUT_FILE = "trip_data_normalized.pkl.gz"

def load_input_data(time_period_start, time_period_end):
    start_year = datetime.strptime(time_period_start, '%Y-%m-%d').year
    end_year = datetime.strptime(time_period_end, '%Y-%m-%d').year

    trip_df = pd.DataFrame()
    for year in range(start_year, end_year + 1):
        glob = f'{year}*.csv.zip'
        for zipfile in Path(INPUT_FILE_PREFIX).glob(glob):
            with ZipFile(zipfile) as zf:
                for file in zf.infolist():
                    if file.filename.endswith(".csv") and not file.filename.startswith(
                        "__"
                    ):
                        print(file.filename)
                        trip_df = pd.concat((trip_df, pd.read_csv(zf.open(file.filename))))
    return trip_df.query('starttime >= @time_period_start & stoptime <= @time_period_end').reset_index()


def get_all_stations(trip_df):
    # Get a dataframe of all of the start station info
    start_stations = (
        trip_df[
            ["start station id", "start station latitude", "start station longitude"]
        ]
        .groupby("start station id")
        .first()
    )
    start_stations.columns = ["station_latitude", "station_longitude"]

    # Get a dataframe of all of the end station info
    end_stations = (
        trip_df[["end station id", "end station latitude", "end station longitude"]]
        .groupby("end station id")
        .first()
    )
    end_stations.columns = ["station_latitude", "station_longitude"]

    # Merge the two lists
    all_stations = pd.concat((start_stations, end_stations)).drop_duplicates()

    return all_stations


def pair_stations_with_nodes(all_stations, G, trip_threshold_dist_m):
    # This is the distance from a trip start or endpoint to its nearest node beyond which we assume
    # that the crash occurred outside of our graph and isn't really matched to that node.
    nearest_nodes = pd.DataFrame(
        columns=[
            "NODE_ID",
            "NODE_DIST_FROM_STATION_M",
            "NODE_LATITUDE",
            "NODE_LONGITUDE",
        ]
    )

    for i, station in tqdm(all_stations.iterrows(), total=all_stations.shape[0]):
        nn_id, dist = ox.distance.nearest_nodes(
            G, station.station_longitude, station.station_latitude, return_dist=True
        )
        if dist > trip_threshold_dist_m:
            continue
        nn = G.nodes[nn_id]
        nearest_nodes.loc[i] = {
            "NODE_ID": nn_id,
            "NODE_DIST_FROM_STATION_M": dist,
            "NODE_LATITUDE": nn["y"],
            "NODE_LONGITUDE": nn["x"],
        }

    stations_with_nodes = pd.concat((all_stations, nearest_nodes), join="inner", axis=1)

    return stations_with_nodes


def generate_node_events_for_trips(trip_df_in_area, stations_with_nodes, G, trip_sample_size):
    df = dd.from_pandas(
        trip_df_in_area.sample(n=trip_sample_size, random_state=42), chunksize=1000
    )

    trips_with_no_paths = []

    def associate_nodes(trip):
        start_node_id = stations_with_nodes.loc[trip["start station id"]].NODE_ID
        end_node_id = stations_with_nodes.loc[trip["end station id"]].NODE_ID

        try:
            route = ox.shortest_path(G, start_node_id, end_node_id)
        except NetworkXNoPath:
            trips_with_no_paths.append(trip.name)
            return

        # It looks like newer versions of NetworkX return None when there is no
        # route instead of throwing an error.
        if route is None:
            return

        return pd.DataFrame(
            {
                "TRIP_ID": trip.name,
                "NODE_ID": node_id,
                "NODE_LATITUDE": G.nodes[node_id]["y"],
                "NODE_LONGITUDE": G.nodes[node_id]["x"],
            }
            for node_id in route
        )

    with ProgressBar():
        result = df.apply(
            associate_nodes, axis=1, meta=pd.Series(dtype=object)
        ).values.compute()
    return pd.concat(result)


def process(time_period_start, time_period_end, trip_threshold_dist_m, trip_sample_size):
    trip_df = load_input_data(time_period_start, time_period_end)

    # Newer years (2021 onward) of data have some weird columns that only have
    # empty columns. Let's make sure we drop those. Note that we ignore errors
    # here because not all time periods are going to have these columns.
    trip_df.drop(columns=[
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
        "member_casual"
    ], inplace=True, errors='ignore')

    G = ox.io.load_graphml(INPUT_FILE_GRAPH)
    # Get the list of all unique Citibike stations referenced by the trip records we have
    all_stations = get_all_stations(trip_df)
    # Filter out all of the stations that don't have a corresponding node in the graph
    stations_with_nodes = pair_stations_with_nodes(all_stations, G, trip_threshold_dist_m)
    # Filter out all of the trips that didn't start or end at a station in the graph
    trip_df_in_area = trip_df[
        trip_df["end station id"].isin(stations_with_nodes.index)
        & trip_df["start station id"].isin(stations_with_nodes.index)
    ]

    trips_with_nodes = generate_node_events_for_trips(
        trip_df_in_area, stations_with_nodes, G, trip_sample_size
    )
    all_nodes_with_trip_info = pd.merge(
        trips_with_nodes, trip_df_in_area, left_on="TRIP_ID", right_index=True
    ).drop(columns=["index"])

    return all_nodes_with_trip_info


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('time_period_start')
    parser.add_argument('time_period_end')
    parser.add_argument('trip_threshold_dist_m', type=int)
    parser.add_argument('trip_sample_size', type=int)
    args = parser.parse_args()
    df = process(args.time_period_start, args.time_period_end, args.trip_threshold_dist_m, args.trip_sample_size)
    df.to_pickle(OUTPUT_FILE)
