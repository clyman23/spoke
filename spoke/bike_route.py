"""
Gets the bike route between two points
"""
from collections import Counter
from typing import Dict, List

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

    def get_node_edges(self, node: int) -> List[tuple]:
        """
        Get all nodes at an edge in graph self._graph

        Args:
            node (int): The ID of a given node in the graph

        Returns:
            List[tuple]: List of tuples for each edge that meets at the node
        """
        in_edges = list(self._graph.in_edges(node))
        out_edges = list(self._graph.out_edges(node))

        #TODO: Could there be repeats between in- and out-edges?

        return in_edges + out_edges

    def get_edge_attributes(self, edge: tuple) -> dict:
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
        return self._graph.get_edge_data(edge[0], edge[1])

    def get_edge_road(self, edge: tuple) -> str:
        """
        Get the road type of a given edge

        Args:
            edge (tuple): The tuple that defines an edge

        Returns:
            str: The type of road
        """
        return self.get_edge_attributes(edge)[0].get("highway", "N/A")

    def get_roads_at_node(self, node: int) -> List[str]:
        """
        Get all roads that are present (converge) at a given node in graph self._graph

        Args:
            node (int): The node ID

        Returns:
            List[str]: Road types for all edges that meet at the given node
        """
        all_edges = self.get_node_edges(node)

        return [self.get_edge_road(edge) for edge in all_edges]

    def count_road_types(self, node: int) -> Dict[str, int]:
        """
        Count types of roads at a node

        Args:
            node (int): The node ID

        Returns:
            Dict[str, int]: Dictionary of road types and counts
        """
        roads = self.get_roads_at_node(node)

        # Need to unpack, because some returns from OSMnx can be a list of multiple road types
        flat_list_roads = []
        for sublist in roads:
            if type(sublist)==list:
                for item in sublist:
                    flat_list_roads.append(item)
            else:
                flat_list_roads.append(sublist)
        
        return dict(Counter(flat_list_roads))
