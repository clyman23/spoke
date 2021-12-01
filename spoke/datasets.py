import pandas as pd
import geopandas as gpd
import osmnx as ox

from os.path import abspath, join

DIR = __loader__.path

def load_mnh_below_34th(show=False):
    G = ox.io.load_graphml(abspath(join(DIR, '../../data/mnh_below_34th.graphml')))
    if show:
        ox.plot.plot_graph(G)
    return G

def load_consolidated_mnh_below_34th(show=False):
    G = ox.io.load_graphml(abspath(join(DIR, '../../data/consolidated_mnh_below_34th.graphml')))
    if show:
        ox.plot.plot_graph(G)
    return G

def load_normalized_crash_gdf(with_nodes=True):
    if with_nodes:
        df = pd.read_csv(abspath(join(DIR, '../../data/crash_data_normalized_with_node_graph.csv')))
    else:
        df = pd.read_pickle(abspath(join(DIR, '../../data/crash_data/crash_data_normalized_2019-01-01-2021-11-01.pkl.gz')))
    return gpd.GeoDataFrame(data=df, geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE), crs='EPSG:4326')

def load_danger_df():
    return pd.read_pickle(abspath(join(DIR, '../../data/consolidated_danger_by_node_id.pkl.gz')))
