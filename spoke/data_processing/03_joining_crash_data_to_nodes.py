import pandas as pd
import osmnx as ox
from tqdm import tqdm

from os.path import join, abspath
import sys

DIR = sys.path[0]

module_path = abspath(join(DIR, '../..'))
if module_path not in sys.path:
    sys.path.append(module_path)
    
from spoke.datasets import load_mnh_below_34th, load_normalized_crash_gdf
from spoke.graphing import saturate_nodes

OUTPUT_FILE = join(DIR, '../../data/crash_data/crash_data_normalized_with_node_graph.pkl.gz')

def process():
    # We load the graph and the normalized set of crashes
    G = load_mnh_below_34th()
    crash_df = load_normalized_crash_gdf(with_nodes=False)

    # This is the distance from a crash to its nearest node beyond which we assume
    # that the crash occurred outside of our graph and isn't really matched to that node.
    THRESHOLD_DIST_M = 100
    
    # Now we create a dataframe with a row for each crash that contains the ID
    # of the nearest street network node.
    nearest_nodes = pd.DataFrame(columns=['NODE_ID', 'NODE_DIST_FROM_CRASH_M', 'NODE_LATITUDE', 'NODE_LONGITUDE'])

    for i, crash in tqdm(crash_df.iterrows(), total=crash_df.shape[0]):
        nn_id, dist = ox.distance.nearest_nodes(G, crash.LONGITUDE, crash.LATITUDE, return_dist=True)
        if dist > THRESHOLD_DIST_M:
            continue
        nn = G.nodes[nn_id]
        nearest_nodes.loc[i] = {
            'NODE_ID': nn_id,
            'NODE_DIST_FROM_CRASH_M': dist,
            'NODE_LATITUDE': nn['y'],
            'NODE_LONGITUDE': nn['x'],
        }
    
    crash_df_with_nodes = pd.concat((crash_df, nearest_nodes), join='inner', axis=1)
    
    return crash_df_with_nodes

if __name__ == '__main__':
    df = process()
    df.to_pickle(OUTPUT_FILE)