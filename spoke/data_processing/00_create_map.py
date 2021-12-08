import osmnx as ox
import pandas as pd
from tqdm import tqdm
from functools import lru_cache

from spoke.graphing import SO_34_POLY

OUTPUT_FILE = "target_map.graphml"


@lru_cache(maxsize=None)
def create_mnh_poly():
    import geopandas as gpd

    return (
        gpd.read_file(gpd.datasets.get_path("nybb"))
        .query("BoroCode == 1")
        .to_crs('epsg:4326')
        .iloc[0]
        .geometry
    )


CLIPPING_BOUNDARIES = {"mnh": create_mnh_poly, "mnh_below_34th": lambda: SO_34_POLY}


def process(map_clipping_boundary):
    clip = CLIPPING_BOUNDARIES[map_clipping_boundary]()
    return ox.graph.graph_from_polygon(clip, network_type="bike")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("map_clipping_boundary")
    args = parser.parse_args()
    output = process(args.map_clipping_boundary)
    ox.io.save_graphml(output, filepath=OUTPUT_FILE)
