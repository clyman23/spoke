import osmnx as ox
import pandas as pd
from tqdm import tqdm

from spoke.graphing import SO_34_POLY

OUTPUT_FILE = "pipeline_data/target_map.graphml"


def process():
    return ox.graph.graph_from_polygon(SO_34_POLY, network_type="bike")


if __name__ == "__main__":
    output = process()
    ox.io.save_graphml(output, filepath=OUTPUT_FILE)
