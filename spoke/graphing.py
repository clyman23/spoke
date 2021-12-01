"""
Contains functions and constants for generating OSMnx network graphs
"""
from typing import List, Tuple

from networkx import MultiDiGraph
import networkx
import osmnx as ox
from shapely.geometry import Polygon

# Polygon defining a clipping mask for Manhattan below 34th St, in long/lat coordinates
SO_34_POLY = Polygon([
    (-74.00591, 40.75739), # West 34th
    (-73.97139, 40.7432), # East 34th
    (-73.97182, 40.73549), # About East 23rd
    (-73.9706, 40.7283), # About East 16th
    (-73.976, 40.7092), # Navy Yard-ish
    (-73.9987, 40.7047), # Brooklyn Bridge-ish
    (-74.0174, 40.696), # The Battery
    (-74.0249, 40.7272), # Holland Tunnel
    (-74.00591, 40.75739), # West 34th
])


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
