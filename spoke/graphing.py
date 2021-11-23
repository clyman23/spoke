"""
Contains functions and constants for generating OSMnx network graphs
"""
from typing import List, Tuple

from networkx import MultiDiGraph
import networkx
import osmnx as ox
from shapely.geometry import Polygon


SO_34_POLY = [
    (40.75739, -74.00591), # West 34th
    (40.7432, -73.97139), # East 34th
    (40.73549, -73.97182), # About East 23rd
    (40.7283, -73.9706), # About East 16th
    (40.7092, -73.976), # Navy Yard-ish
    (40.7047, -73.9987), # Brooklyn Bridge-ish
    (40.696, -74.0174), # The Battery
    (40.7272, -74.0249), # Holland Tunnel
    (40.75739, -74.00591), # West 34th
]


def get_graph_from_poly(
        poly_coords: List[Tuple[float, float]],
        network_type: str = "bike"
) -> MultiDiGraph:
    """
    Get a graph from OSMnx of a region defined by a list of lat/long coordinates

    Args:
        poly_coords (List[Tuple[float, float]]): List of tuples that define the outline of a polygon
        network_type (str): String that defines the network type for OSMnx graph_from_polygon()

    Returns:
        MultiDiGraph: The network graph from OSMnx
    """
    boundaries = Polygon(poly_coords)

    return ox.graph.graph_from_polygon(boundaries, network_type=network_type)
