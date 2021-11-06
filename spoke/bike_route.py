"""
Gets the bike route between two points
"""
from networkx import MultiDiGraph
import osmnx as ox


class BikeRoute:
    """
    Finds the bike route between two locations using OSMnx

    Args:
        graph (MultiDiGraph): A graph of the environment to be routed
        start_loc (tuple): Tuple of starting lat and lon
        end_loc (tuple): Tuple of ending lat and lon

    Attributes:
        None

    Methods:
        get_route (list): Gets the route on the graph
    """
    def __init__(self, graph: MultiDiGraph, start_loc: tuple, end_loc: tuple) -> None:
        self._graph: MultiDiGraph = graph
        self._start_loc: tuple = start_loc
        self._end_loc: tuple = end_loc

    def get_route(self) -> list:
        """
        Get route between start and end points on graph

        Args:
            None

        Returns:
            list: The route
        """
        orig = ox.distance.nearest_nodes(
            self._graph,
            self._start_loc[1],
            self._start_loc[0],
            return_dist=False
        )

        dest = ox.distance.nearest_nodes(
            self._graph,
            self._end_loc[1],
            self._end_loc[0],
            return_dist=False
        )

        return ox.shortest_path(
            self._graph,
            orig,
            dest,
            weight="travel_time"
        )
