from os.path import join
import sys
import pandas as pd

TIME_PERIOD_START = '2019-01-01'
TIME_PERIOD_END = '2021-11-01'

DIR = sys.path[0]
INPUT_FILE = join(DIR, '../../data/raw_data/Motor_Vehicle_Collisions_-_Crashes.csv')
OUTPUT_FILE = join(DIR, f'../../data/crash_data/crash_data_normalized_{TIME_PERIOD_START}-{TIME_PERIOD_END}.pkl.gz')



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