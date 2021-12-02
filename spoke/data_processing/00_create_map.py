import osmnx as ox
import pandas as pd
from tqdm import tqdm

from spoke.graphing import SO_34_POLY

OUTPUT_FILE = "pipeline_data/target_map.graphml"

CLIPPING_BOUNDARIES = {
    'mnh_below_34th': SO_34_POLY
}

def process(map_clipping_boundary):
    clip = CLIPPING_BOUNDARIES[map_clipping_boundary]
    return ox.graph.graph_from_polygon(clip, network_type="bike")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('map_clipping_boundary')
    args = parser.parse_args()
    output = process(args.map_clipping_boundary)
    ox.io.save_graphml(output, filepath=OUTPUT_FILE)
