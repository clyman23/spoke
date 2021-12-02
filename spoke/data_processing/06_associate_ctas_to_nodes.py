import pandas as pd
import geopandas as gpd
import sys
import osmnx as ox
from os.path import abspath, dirname, join

DIR = sys.path[0] if __name__ == '__main__' else dirname(__loader__.path)

module_path = abspath(join(DIR, '../..'))
if module_path not in sys.path:
    sys.path.append(module_path)

INPUT_FILE_GRAPH = abspath(join(DIR, '../../pipeline_data/target_map.graphml'))
INPUT_FILE_CENSUS_SHAPEFILE = abspath(join(DIR, '../../pipeline_data/raw_data/2010_Census_Tracts/geo_export_85c202c5-6ec9-493e-b0ec-a13efa26758d.shp'))
INPUT_FILE_DATASET = abspath(join(DIR, '../../pipeline_data/unified_dataset.pkl.gz'))
OUTPUT_FILE = abspath(join(DIR, '../../pipeline_data/node_id_census_tract_key.pkl.gz'))

def process():
  G = ox.io.load_graphml(INPUT_FILE_GRAPH)
  tract_gdf = gpd.read_file(INPUT_FILE_CENSUS_SHAPEFILE)

  node_gdf = ox.utils_graph.graph_to_gdfs(G,nodes=True, edges=False, node_geometry=True)

  ny_county_tracts_gdf = tract_gdf.loc[tract_gdf['boro_name'] == 'Manhattan'].copy()

  nodes_in_tracts = gpd.sjoin(node_gdf, ny_county_tracts_gdf, predicate='intersects')

  node_id_census_tracts = nodes_in_tracts[['ct2010']].copy()

  return node_id_census_tracts

if __name__ == '__main__':
  output = process()
  output.to_pickle(OUTPUT_FILE)