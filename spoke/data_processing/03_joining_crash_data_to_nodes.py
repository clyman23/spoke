import osmnx as ox
import pandas as pd
from tqdm import tqdm

from spoke.graphing import saturate_nodes

INPUT_FILE_GRAPH = "pipeline_data/target_map.graphml"
INPUT_FILE_CRASHES = "pipeline_data/crash_data_normalized.pkl.gz"
OUTPUT_FILE = "pipeline_data/crash_data_normalized_with_node_graph.pkl.gz"

def process(crash_threshold_dist_m):
    # We load the graph and the normalized set of crashes
    G = ox.io.load_graphml(INPUT_FILE_GRAPH)
    crash_df = pd.read_pickle(INPUT_FILE_CRASHES)

    # Now we create a dataframe with a row for each crash that contains the ID
    # of the nearest street network node.
    nearest_nodes = pd.DataFrame(
        columns=["NODE_ID", "NODE_DIST_FROM_CRASH_M", "NODE_LATITUDE", "NODE_LONGITUDE"]
    )

    for i, crash in tqdm(crash_df.iterrows(), total=crash_df.shape[0]):
        nn_id, dist = ox.distance.nearest_nodes(
            G, crash.LONGITUDE, crash.LATITUDE, return_dist=True
        )
        if dist > crash_threshold_dist_m:
            continue
        nn = G.nodes[nn_id]
        nearest_nodes.loc[i] = {
            "NODE_ID": nn_id,
            "NODE_DIST_FROM_CRASH_M": dist,
            "NODE_LATITUDE": nn["y"],
            "NODE_LONGITUDE": nn["x"],
        }

    crash_df_with_nodes = pd.concat((crash_df, nearest_nodes), join="inner", axis=1)

    return crash_df_with_nodes


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('crash_threshold_dist_m', type=int)
    args = parser.parse_args()
    df = process(args.crash_threshold_dist_m)
    df.to_pickle(OUTPUT_FILE)
