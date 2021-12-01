import pandas as pd
import osmnx as ox
import numpy as np
from networkx import NetworkXNoPath
from tqdm import tqdm
from pathlib import Path
from zipfile import ZipFile
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

from os.path import join, abspath, dirname
import sys

DIR = sys.path[0] if __name__ == '__main__' else dirname(__loader__.path)

module_path = abspath(join(DIR, '../..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    
from spoke.datasets import load_mnh_below_34th

OUTPUT_FILE = join(DIR, '../../data/trip_data/trip_data_normalized.pkl.gz')

TRIP_SAMPLE_SIZE=10_000

def load_input_data():
    trip_df = pd.DataFrame()
    data_path = Path(abspath(join(DIR, '../../data/raw_data/citibike/')))
    for zipfile in data_path.glob('2019*.csv.zip'):
        with ZipFile(data_path / zipfile) as zf:
            for file in zf.infolist():
                if file.filename.endswith('.csv') and not file.filename.startswith('__'):
                    print(file.filename)
                    trip_df = pd.concat((trip_df, pd.read_csv(zf.open(file.filename))))
    return trip_df.reset_index()

def get_all_stations(trip_df):
    # Get a dataframe of all of the start station info
    start_stations = trip_df[['start station id', 'start station latitude', 'start station longitude']].groupby('start station id').first()
    start_stations.columns = ['station_latitude', 'station_longitude']

    # Get a dataframe of all of the end station info
    end_stations = trip_df[['end station id', 'end station latitude', 'end station longitude']].groupby('end station id').first()
    end_stations.columns = ['station_latitude', 'station_longitude']

    # Merge the two lists
    all_stations = pd.concat((start_stations, end_stations)).drop_duplicates()
    
    return all_stations

def pair_stations_with_nodes(all_stations, G):
    # This is the distance from a trip start or endpoint to its nearest node beyond which we assume
    # that the crash occurred outside of our graph and isn't really matched to that node.
    THRESHOLD_DIST_M = 100
    nearest_nodes = pd.DataFrame(columns=['NODE_ID', 'NODE_DIST_FROM_STATION_M', 'NODE_LATITUDE', 'NODE_LONGITUDE'])

    for i, station in tqdm(all_stations.iterrows(), total=all_stations.shape[0]):
        nn_id, dist = ox.distance.nearest_nodes(G, station.station_longitude, station.station_latitude, return_dist=True)
        if dist > THRESHOLD_DIST_M:
            continue
        nn = G.nodes[nn_id]
        nearest_nodes.loc[i] = {
            'NODE_ID': nn_id,
            'NODE_DIST_FROM_STATION_M': dist,
            'NODE_LATITUDE': nn['y'],
            'NODE_LONGITUDE': nn['x'],
        }

    stations_with_nodes = pd.concat((all_stations, nearest_nodes), join='inner', axis=1)
    
    return stations_with_nodes

def generate_node_events_for_trips(trip_df_in_area, stations_with_nodes, G):
    df = dd.from_pandas(trip_df_in_area.sample(n=TRIP_SAMPLE_SIZE, random_state=42), chunksize=1000)

    trips_with_no_paths = []

    def associate_nodes(trip):
        start_node_id = stations_with_nodes.loc[trip['start station id']].NODE_ID
        end_node_id = stations_with_nodes.loc[trip['end station id']].NODE_ID

        try:
            route = ox.shortest_path(G, start_node_id, end_node_id)
        except NetworkXNoPath:
            trips_with_no_paths.append(trip.name)
            return

        return pd.DataFrame(
            {
                'TRIP_ID': trip.name,
                'NODE_ID': node_id,
                'NODE_LATITUDE': G.nodes[node_id]['y'],
                'NODE_LONGITUDE': G.nodes[node_id]['x'],
            }
            for node_id in route
        )

    with ProgressBar():
        result = df.apply(associate_nodes, axis=1, meta=pd.Series(dtype=object)).values.compute()
    return pd.concat(result)

def process():
    trip_df = load_input_data()
    G = load_mnh_below_34th()
    # Get the list of all unique Citibike stations referenced by the trip records we have
    all_stations = get_all_stations(trip_df)
    # Filter out all of the stations that don't have a corresponding node in the graph
    stations_with_nodes = pair_stations_with_nodes(all_stations, G)
    # Filter out all of the trips that didn't start or end at a station in the graph
    trip_df_in_area = trip_df[trip_df['end station id'].isin(stations_with_nodes.index) & trip_df['start station id'].isin(stations_with_nodes.index)]
    
    trips_with_nodes = generate_node_events_for_trips(trip_df_in_area, stations_with_nodes, G)
    all_nodes_with_trip_info = pd.merge(trips_with_nodes, trip_df_in_area, left_on='TRIP_ID', right_index=True).drop(columns=['index'])
    
    return all_nodes_with_trip_info
                    
if __name__ == '__main__':
    df = process()
    df.to_pickle(OUTPUT_FILE)