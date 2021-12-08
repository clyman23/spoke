import pandas as pd
import geopandas as gpd
import sys
import osmnx as ox
from os.path import abspath, dirname, join

INPUT_FILE_GRAPH = "target_map.graphml"
INPUT_FILE_CENSUS_SHAPEFILE = "../raw_data/2010_Census_Tracts/geo_export_85c202c5-6ec9-493e-b0ec-a13efa26758d.shp"
OUTPUT_FILE = "node_id_census_tract_key.pkl.gz"


def process():
    G = ox.io.load_graphml(INPUT_FILE_GRAPH)
    tract_gdf = gpd.read_file(INPUT_FILE_CENSUS_SHAPEFILE)

    node_gdf = ox.utils_graph.graph_to_gdfs(
        G, nodes=True, edges=False, node_geometry=True
    )

    ny_county_tracts_gdf = tract_gdf.loc[tract_gdf["boro_name"] == "Manhattan"].to_crs(crs='epsg:4326')

    nodes_in_tracts = gpd.sjoin(node_gdf, ny_county_tracts_gdf, predicate="intersects")

    node_id_census_tracts = nodes_in_tracts[["ct2010"]]

    return node_id_census_tracts


if __name__ == "__main__":
    output = process()
    output.to_pickle(OUTPUT_FILE)
