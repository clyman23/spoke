"""
Helper functions for analyzing crashes
"""
from typing import Dict, Tuple

from networkx import MultiDiGraph
import numpy as np
import osmnx as ox
import pandas as pd

from spoke.bike_route import BikeRoute


def get_crash_node(G: MultiDiGraph, crash: dict) -> int:
    """
    Returns the ID of the node in G closest to the crash
    """
    return ox.distance.nearest_nodes(G, crash["LONGITUDE"], crash["LATITUDE"])


def _get_nearest_crash_node(_x: pd.Series, _g: MultiDiGraph) -> int:
    """
    Function to apply to a df to get closest node in a graph, _g
    """
    if not (np.isnan(_x["LONGITUDE"]) and np.isnan(_x["LATITUDE"])):
        return ox.distance.nearest_nodes(_g, _x["LONGITUDE"], _x["LATITUDE"])
    return 0


def get_node_lat_lon(G: MultiDiGraph, node: int) -> Tuple[float, float]:
    """
    Returns tuple of lat and lon for a node in graph, G
    """
    return G.nodes[node]["y"], G.nodes[node]["x"]


def get_crashes(filepath: str) -> pd.DataFrame:
    """
    Get crashes from a csv and save to df

    Args:
        filepath (str): Filepath of input crash data

    Returns:
        pd.DataFrame: df of crashes
    """
    return pd.read_csv(filepath)


def get_borough_crashes(crashes_df: pd.DataFrame, borough: str) -> pd.DataFrame:
    """
    Return df of crashes for a single borough
    """
    return crashes_df[crashes_df["BOROUGH"] == borough.upper()]


def get_bike_crashes(crashes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return df of crashes only involving a bike
    """
    bike_ids = [
        "bike", "bicycle", "e-bike", "ebike", "e bike",
        "e bik", "e-bik", "bicyc", "minibike"
    ]

    bike = (
        crashes_df["VEHICLE TYPE CODE 1"].str.lower().isin(bike_ids)
        | crashes_df["VEHICLE TYPE CODE 2"].str.lower().isin(bike_ids)
        | crashes_df["VEHICLE TYPE CODE 3"].str.lower().isin(bike_ids)
        | crashes_df["VEHICLE TYPE CODE 4"].str.lower().isin(bike_ids)
        | crashes_df["VEHICLE TYPE CODE 5"].str.lower().isin(bike_ids)
    )

    return crashes_df[bike]


def get_crash_nodes(G: MultiDiGraph, crashes_df: pd.DataFrame) -> pd.Series:
    """
    Get a pandas Series of crash nodes in the graph, G
    """
    return crashes_df.apply(_get_nearest_crash_node, args=(G, ), axis=1)


def get_route_crashes(crashes_df: pd.DataFrame, route: list) -> pd.DataFrame:
    """
    Get only the crashes on a particular route
    """
    return crashes_df[crashes_df["crash_node"].isin(route)]


def get_num_node_crashes(crashes_df: pd.DataFrame) -> Dict[int, Dict[str, int]]:
    """
    Get the number of crashes per node in the crashes_df
    """
    return {
        node: {"num_crashes": i}
        for node, i in crashes_df["crash_node"].value_counts().iteritems()
    }


def get_node_crash_attributes(crashes_df: pd.DataFrame, bike_route: BikeRoute, G: MultiDiGraph, ):
    """
    Get the attributes of each node based on crashes and road types
    """
    num_node_crashes = get_num_node_crashes(crashes_df)

    for node, attrs in num_node_crashes.items():
        if node:
            for road_type, num_roads in bike_route.count_road_types(node).items():
                attrs[road_type] = num_roads

    if 0 in num_node_crashes:
        _ = num_node_crashes.pop(0)

    return num_node_crashes
