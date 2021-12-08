import pandas as pd
import osmnx as ox
import sklearn
from scipy.stats import mode
import re
import numpy as np
import networkx as nx

from spoke.graphing import saturate_nodes

INPUT_FILE_GRAPH = "target_map.graphml"
INPUT_FILE_CTA_LOOKUP = "node_id_census_tract_key.pkl.gz"
INPUT_FILE_DATASET = "unified_dataset.parquet"
OUTPUT_FILE_DANGER = "danger_by_node_id.pkl.gz"
OUTPUT_FILE_GRAPH = "target_map_consolidated.graphml"


def process():
    node_ids_crash_avg = pd.read_parquet(
        INPUT_FILE_DATASET
    , columns=['NODE_ID', 'IS_CRASH'])
    node_id_cta_associations = pd.read_pickle(INPUT_FILE_CTA_LOOKUP)
    G = ox.io.load_graphml(INPUT_FILE_GRAPH)

    # Compute the average number of crashes per node
    crash_avg_by_node = node_ids_crash_avg.groupby(by="NODE_ID").agg("mean").IS_CRASH.rename("CRASH_AVG")
    # Remove rows where we only had an example of a crash but no examples of trips
    crash_avg_by_node = crash_avg_by_node.loc[crash_avg_by_node < 1]
    crash_avg_by_node = saturate_nodes(G, crash_avg_by_node)

    scaler = sklearn.preprocessing.PowerTransformer(method="box-cox")
    df = pd.DataFrame(crash_avg_by_node)
    df_gt_0 = df[df.CRASH_AVG > 0]
    scaled_crash_avg_by_node = pd.Series(
        scaler.fit_transform(df_gt_0)[:, 0],
        index=df_gt_0.index
    )

    # Offset the scaled danger to be >0
    danger_by_node = scaled_crash_avg_by_node + np.abs(scaled_crash_avg_by_node.min())
    danger_by_node = saturate_nodes(G, danger_by_node)

    Gp = ox.simplification.consolidate_intersections(
        ox.projection.project_graph(G, to_crs=None), tolerance=25
    )

    def normalize_id_format(ids):
        if isinstance(ids, str):
            # If the mapping is one-to-many, then the old IDs come in as a stringified array. We convert it to a list of ints.
            return list(map(int, re.sub(r"[\[\]]", "", ids).split(", ")))
        else:
            # If the mapping is one-to-one, it comes in as a single int. We wrap it in a list.
            return [ids]

    def map_ids(new_node_id, old_node_ids):
        old_node_ids = normalize_id_format(old_node_ids)

        return {
            "DANGER": danger_by_node.loc[old_node_ids].max(),
            "CRASH_AVG": crash_avg_by_node[old_node_ids].max(),
            "OLD_NODE_IDS": old_node_ids,
            # We pick the mode of the census tract IDs from the old nodes
            "CENSUS_TRACT_ID": node_id_cta_associations.loc[mode(old_node_ids)[0][0]][
                0
            ],
        }

    consolidated_danger_df = pd.DataFrame(
        (
            map_ids(a, b)
            for a, b in nx.get_node_attributes(Gp, "osmid_original").items()
        ),
        index=Gp.nodes,
    )

    return consolidated_danger_df, Gp


if __name__ == "__main__":
    df, G = process()
    df.to_pickle(OUTPUT_FILE_DANGER)
    ox.io.save_graphml(G, OUTPUT_FILE_GRAPH)
