import sys
from os.path import abspath, join

import pandas as pd

TIME_PERIOD_START = '2019-01-01'
TIME_PERIOD_END = '2021-11-01'

DIR = sys.path[0] if __name__ == '__main__' else dirname(__loader__.path)

INPUT_FILE = abspath(join(DIR, '../../pipeline_data/raw_data/Motor_Vehicle_Collisions_-_Crashes.csv'))
OUTPUT_FILE = abspath(join(DIR, f'../../pipeline_data/crash_data_normalized.pkl.gz'))

def process():
    # Load raw data
    crash_df = pd.read_csv(INPUT_FILE, parse_dates=['CRASH DATE'], infer_datetime_format=True)
    
    # Filter for desired time period
    crash_df.query(f'`CRASH DATE` >= "{TIME_PERIOD_START}" & `CRASH DATE` <= "{TIME_PERIOD_END}"', inplace=True)
    
    # Filter for crashes that involved bicycles or pedestrians
    crash_df.query(
        '(`NUMBER OF PEDESTRIANS KILLED` > 0)'
        '| (`NUMBER OF PEDESTRIANS INJURED` > 0)'
        '| (`NUMBER OF CYCLIST KILLED` > 0)'
        '| (`NUMBER OF CYCLIST INJURED` > 0)'
        '|' + ' | '.join([f'`VEHICLE TYPE CODE {i+1}`.str.contains("Bike", case=False, na=False)' for i in range(5)]), inplace=True)
    
    # Filter for crashes that have available location information
    crash_df.query('~LATITUDE.isna() & ~LONGITUDE.isna() & LATITUDE != 0 & LONGITUDE != 0', inplace=True)
    
    return crash_df
    
if __name__ == '__main__':
    crash_df = process()
    crash_df.to_pickle(OUTPUT_FILE)
