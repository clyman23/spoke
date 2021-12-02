import pandas as pd
import sys
import osmnx as ox
import sklearn
from scipy.stats import mode
import re
import numpy as np
import networkx as nx

from os.path import abspath, dirname, join

DIR = sys.path[0] if __name__ == '__main__' else dirname(__loader__.path)

module_path = abspath(join(DIR, '../..'))
if module_path not in sys.path:
    sys.path.append(module_path)


from spoke.graphing import saturate_nodes

INPUT_FILE_GRAPH = abspath(join(DIR, '../../pipeline_data/target_map.graphml'))
INPUT_FILE_CTA_LOOKUP = abspath(join(DIR, "../../pipeline_data/node_id_census_tract_key.pkl.gz"))
INPUT_FILE_DATASET = abspath(join(DIR, '../../pipeline_data/unified_dataset.pkl.gz'))
OUTPUT_FILE_DANGER = abspath(join(DIR, '../../pipeline_data/danger_by_node_id.pkl.gz'))
OUTPUT_FILE_GRAPH = abspath(join(DIR, '../../pipeline_data/target_map_consolidated.graphml'))

def process():
  # BEGIN Liz's code

  node_ids_crash_avg = pd.read_pickle(INPUT_FILE_DATASET) #, usecols=['NODE_ID', 'IS_CRASH'])
  # group by node ID and take average of IS_CRASH to have toy danger metric
  node_id_crash_df = node_ids_crash_avg.groupby(by='NODE_ID').agg('mean')

  node_id_census_tract = pd.read_pickle(INPUT_FILE_CTA_LOOKUP)
  
  # merge on node ids: census tracts and avg crash
  node_ids__crash_avg_census_tracts = node_id_crash_df.merge(node_id_census_tract, how='left', left_index=True, right_on='osmid')

  # rename columns to be clearer
  col_names = {'osmid':'NODE_ID','IS_CRASH':'CRASH_AVG','ct2010':'CENSUS_TRACT_ID'}
  node_ids__crash_avg_census_tracts.rename(columns=col_names, inplace=True)

  node_ids__crash_avg_census_tracts_clean = node_ids__crash_avg_census_tracts.loc[node_ids__crash_avg_census_tracts['CRASH_AVG']<1,:]

  # BEGIN Max's code
  crash_df = node_ids__crash_avg_census_tracts_clean
  G = ox.io.load_graphml(INPUT_FILE_GRAPH)

  crash_avg_by_node = crash_df['CRASH_AVG']

  crash_avg_by_node = saturate_nodes(G, crash_avg_by_node)

  scaler = sklearn.preprocessing.PowerTransformer(method='box-cox')
  df = pd.DataFrame(crash_avg_by_node)
  scaled_crash_avg_by_node = pd.Series(scaler.fit_transform(df[df.CRASH_AVG > 0])[:, 0])
  scaled_crash_avg_by_node.index = df[df.CRASH_AVG > 0].index

  danger_by_node = scaled_crash_avg_by_node + np.abs(scaled_crash_avg_by_node.min())
  danger_by_node = saturate_nodes(G, danger_by_node)

  Gp = ox.simplification.consolidate_intersections(ox.projection.project_graph(G, to_crs=None), tolerance=25)
  node_id_cta_associations = node_id_census_tract

  def normalize_id_format(ids):
    if isinstance(ids, str):
      # If the mapping is one-to-many, then the old IDs come in as a stringified array. We convert it to a list of ints.
      return list(map(int, re.sub(r'[\[\]]', '', ids).split(', ')))
    else:
      # If the mapping is one-to-one, it comes in as a single int. We wrap it in a list.
      return [ids]

  def map_ids(new_node_id, old_node_ids):
    old_node_ids = normalize_id_format(old_node_ids)
    
    return {
      'DANGER': danger_by_node.loc[old_node_ids].max(),
      'CRASH_AVG': crash_avg_by_node[old_node_ids].max(),
      'OLD_NODE_IDS': old_node_ids,
      # We pick the mode of the census tract IDs from the old nodes
      'CENSUS_TRACT_ID': node_id_cta_associations.loc[mode(old_node_ids)[0][0]][0]
    }

  consolidated_danger_df = pd.DataFrame((map_ids(a, b) for a, b in nx.get_node_attributes(Gp, 'osmid_original').items()), index=Gp.nodes)

  return consolidated_danger_df, Gp

if __name__ == '__main__':
  df, G = process()
  df.to_pickle(OUTPUT_FILE_DANGER)
  ox.io.save_graphml(G, OUTPUT_FILE_GRAPH)
