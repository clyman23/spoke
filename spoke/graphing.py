"""
Contains functions and constants for generating OSMnx network graphs
"""
from typing import List, Tuple

from networkx import MultiDiGraph
import networkx
import numpy as np
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

def clip_northern_edge(gdf, amount=0.0003):
    '''
    Function that will clip the northern nodes that fall within `meters` meters
    of the "northern" edge of the map. This is expected to be used on a map of
    Manhattan and so "north" actually means 30 degrees clockwise from true
    north. Useful for reducing edge effects on the northern edge of the map.
    '''
    from shapely.affinity import translate

    offset = -amount
    mnh_street_angle = np.pi / 6 # Manhattan's streets are about 30 degrees off of true north.
    clip_boundary = translate(SO_34_POLY, yoff=offset, xoff=np.sin(mnh_street_angle) * offset)
    
    return gdf.clip(clip_boundary)

# For a given series whose index is node IDs, fill all of the missing rows in
# with zeros for the nodes in the graph that are not in the series.
def saturate_nodes(G, s):
    for node_id in G.nodes:
        if node_id in s.index:
            continue
        else:
            s.loc[node_id] = 0
    return s

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
