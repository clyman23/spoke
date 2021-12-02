import sys
from os.path import abspath, join

import osmnx as ox
import pandas as pd
from tqdm import tqdm

DIR = sys.path[0] if __name__ == '__main__' else dirname(__loader__.path)

module_path = abspath(join(DIR, '../..'))
if module_path not in sys.path:
    sys.path.append(module_path)

from spoke.graphing import SO_34_POLY

OUTPUT_FILE = abspath(join(DIR, '../../pipeline_data/target_map.graphml'))

print(DIR, OUTPUT_FILE)

def process():
  return ox.graph.graph_from_polygon(SO_34_POLY, network_type="bike")

if __name__ == '__main__':
  output = process()
  ox.io.save_graphml(output, filepath=OUTPUT_FILE)
