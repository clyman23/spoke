import osmnx as ox
import geoplot as gplt
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx

INPUT_FILE_DANGER = "pipeline_data/danger_by_node_id.pkl.gz"
INPUT_FILE_GRAPH = "pipeline_data/target_map_consolidated.graphml"

from spoke.graphing import clip_northern_edge

def make_heatmap(data, weights=None):
    import geoplot.crs as gcrs
    import contextily as cpr

    ax = gplt.kdeplot(
        data,
        weights=weights,
        bw_adjust=0.5,
        cmap="plasma",
        projection=gcrs.WebMercator(),
        fill=True,
        figsize=(12, 12),
        alpha=0.7,
    )
    gplt.webmap(data, provider=cpr.providers.Stamen.Toner, ax=ax)
    return ax


def process():
    danger_df = pd.read_pickle(INPUT_FILE_DANGER)
    G = ox.io.load_graphml(INPUT_FILE_GRAPH)

    # Now we set the node attributes to be this crash count
    nx.set_node_attributes(G, dict(danger_df.DANGER), "danger")
    # Now we map these to colors
    nc = ox.plot.get_node_colors_by_attr(G, "danger", cmap="plasma")

    # Note that I am clipping the top ~40 meters of the map off to help remove edge effects
    ns = clip_northern_edge(
        ox.utils_graph.graph_to_gdfs(G, edges=False).to_crs('EPSG:4326'),
        0.0003
    )

    hm = make_heatmap(ns, ns.danger)

    hm.figure.savefig("heatmap.png")

if __name__ == "__main__":
  process()