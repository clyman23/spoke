import pandas as pd

from os.path import join, abspath
import sys

TIME_PERIOD_START = '2019-01-01'
TIME_PERIOD_END = '2021-10-31'

DIR = sys.path[0]
INPUT_FILES = [
    abspath(join(DIR, p)) for p in (
    '../../data/raw_data/weather/weather_data_2013-01-01_2013-06-30.csv',
    '../../data/raw_data/weather/weather_data_2013-07-01_2015-12-31.csv',
    '../../data/raw_data/weather/weather_data_2016-01-01_2018-06-30.csv',
    '../../data/raw_data/weather/weather_data_2018-07-01_2021-10-31.csv')
]
OUTPUT_FILE = join(DIR, f'../../data/weather_data/weather_data_normalized_{TIME_PERIOD_START}-{TIME_PERIOD_END}.pkl.gz')

def process():
    weather_df = pd.concat((pd.read_csv(f) for f in INPUT_FILES))
    
    weather_df_by_date = weather_df.groupby('DATE').mean()
    
    weather_df_by_date.drop(columns=['DAPR', 'MDPR', 'DASF', 'MDSF', 'PSUN', 'TSUN', 'TAVG', 'WESD', 'WESF', 'WSF2', 'WSF5', 'WDF2', 'WDF5'], inplace=True)
    
    weather_df_by_date.fillna(0, inplace=True)
    
    return weather_df_by_date

if __name__ == '__main__':
    df = process()
    df.to_pickle(OUTPUT_FILE)