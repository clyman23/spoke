import pandas as pd
import osmnx as ox
from collections import Counter

INPUT_FILE_TRIPS = "trip_data_normalized.pkl.gz"
INPUT_FILE_CRASHES = "crash_data_normalized_with_node_graph.pkl.gz"
INPUT_FILE_WEATHER = "weather_data_normalized.pkl.gz"
INPUT_FILE_GRAPH = "target_map.graphml"

OUTPUT_FILE = "unified_dataset.parquet"


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


def get_node_crash_attributes(crashes_df: pd.DataFrame, G):
    """
    Get the attributes of each node based on crashes and road types
    """
    num_node_crashes = get_num_node_crashes(G, crashes_df)

    for node, attrs in num_node_crashes.items():
        if node:
            for road_type, num_roads in count_road_types(G, node).items():
                attrs[road_type] = num_roads

    if 0 in num_node_crashes:
        _ = num_node_crashes.pop(0)

    return num_node_crashes


def count_road_types(_graph, node: int):
    """
    Count types of roads at a node

    Args:
        node (int): The node ID

    Returns:
        Dict[str, int]: Dictionary of road types and counts
    """
    roads = get_roads_at_node(_graph, node)

    # Need to unpack, because some returns from OSMnx can be a list of multiple road types
    flat_list_roads = []
    for sublist in roads:
        if type(sublist) == list:
            for item in sublist:
                flat_list_roads.append(item)
        else:
            flat_list_roads.append(sublist)

    return dict(Counter(flat_list_roads))


def get_roads_at_node(_graph, node: int):
    """
    Get all roads that are present (converge) at a given node in graph self._graph

    Args:
        node (int): The node ID

    Returns:
        List[str]: Road types for all edges that meet at the given node
    """
    all_edges = get_node_edges(_graph, node)

    roads = []

    for edge in all_edges:
        edge_roads = get_edge_road(_graph, edge)

        for road in edge_roads:
            roads.append(road)

    return roads


def get_edge_road(_graph, edge: tuple) -> str:
    """
    Get the road type of a given edge

    Args:
        edge (tuple): The tuple that defines an edge

    Returns:
        str: The type of road
    """
    roads = []
    edge_attrs = get_edge_attributes(_graph, edge)

    for i in edge_attrs.values():
        roads.append(i.get("highway", "N/A"))

    return roads


def get_edge_attributes(_graph, edge: tuple) -> dict:
    """
    Get the attributes of a given edge in graph self._graph

    Args:
        edge (tuple): The tuple that defines an edge

    Returns:
        dict: The attribute dictionary for a given edge. Example is:
        {0: {
                'osmid': [421853954, 421853949],
                'oneway': True,
                'lanes': '5',
                'name': '1st Avenue',
                'highway': 'primary',
                'maxspeed': '25 mph',
                'length': 81.28,
                'geometry': <shapely.geometry.linestring.LineString at 0x7fcd2b43a670>
        }}
    """
    return _graph.get_edge_data(edge[0], edge[1])


def get_node_edges(_graph, node: int):
    """
    Get all nodes at an edge in graph self._graph

    Args:
        node (int): The ID of a given node in the graph

    Returns:
        List[tuple]: List of tuples for each edge that meets at the node
    """
    in_edges = list(_graph.in_edges(node))
    out_edges = list(_graph.out_edges(node))

    # TODO: Could there be repeats between in- and out-edges?

    return in_edges + out_edges


def process():
    trip_df_norm = normalize_trip_df(pd.read_pickle(INPUT_FILE_TRIPS))
    crash_df_norm = normalize_crash_df(pd.read_pickle(INPUT_FILE_CRASHES))

    all_event_df = pd.concat((trip_df_norm, crash_df_norm)).reset_index(drop=True)

    weather_df = pd.read_pickle(INPUT_FILE_WEATHER)
    all_event_df_with_weather = pd.merge(
        all_event_df, weather_df, left_on="EVENT_DATE", right_on="DATE", how="left"
    )

    all_event_df_with_weather.dropna(inplace=True)

    # Now associate the road features from the graph
    G = ox.io.load_graphml(INPUT_FILE_GRAPH)

    node_road_types = {}

    for i in G.nodes:
        node_road_types[i] = count_road_types(G, i)

    node_road_types = pd.DataFrame.from_dict(node_road_types, orient="index").fillna(0)

    final_df = pd.merge(
        all_event_df_with_weather,
        node_road_types,
        how="left",
        left_on="NODE_ID",
        right_index=True,
    )

    return final_df


if __name__ == "__main__":
    df = process()
    df.to_parquet(OUTPUT_FILE)
