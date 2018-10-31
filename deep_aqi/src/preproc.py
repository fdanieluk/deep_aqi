#!/home/filip/miniconda3/envs/deep_aqi/bin/python


"""Load files from EPA, get rid of unimportant columns, clean up, drop sites
with bad coverage (less than 7888 records per year). Save to .parquet for
further analysis.
Look into jupyter/data_exploration-1.0.ipynb for details and explanations.
"""


from os.path import join, basename
from glob import glob
from dask import dataframe as dd
import pandas as pd
import dask
import logging
import atexit
import pickle
import re

from deep_aqi import ROOT
from deep_aqi.src.helper_functions import no_overlaping_files


pd.set_option('max_columns', 50)
pd.set_option('max_rows', 25)


def create_sitecode(df):
    """Create sitecode to get rid of too many non-unique fields.
    """
    logging.debug('Created SiteCode Column.')
    df['SiteCode'] = df['State Name'] + '_' + df['County Name'] + '_' + df['Site Num'].astype(str)
    return df

def only_predefined_sites(df):
    """Filter out all sites not contained in available_sites.csv
    """
    file_path = join(ROOT, 'available_sites.p')

    with open(file_path, 'rb') as file:
        available_sites = pickle.load(file)
    logging.info('Filtering out unimportant sites.')

    return df.loc[df.SiteCode.isin(available_sites), :]

def create_localdate(df):
    df['Local Date'] = df['Date Local'] + ' ' + df['Time Local']
    df['LocalDate'] = df['Local Date'].map_partitions(pd.to_datetime,
                                                      format='%Y/%m/%d %H:%M',
                                                      meta=('Local Date'))
    logging.debug('Created LocalDate columns.')
    return df

def average_instruments(df):
    """On multiplie sites parameters are measured by multiplie instruments,
    to not favour any of them I average across all of them for each site.
    """
    logging.info('Starting to average_instruments.')

    values = df.groupby(by=['SiteCode',
                            'LocalDate',
                            'Parameter Name'])['Sample Measurement'].mean()

    # mistake? not doing it throws an error with pivot_table though
    values = values.reset_index().compute()

    values = values.pivot_table(values='Sample Measurement',
                                index=['SiteCode', 'LocalDate'],
                                columns='Parameter Name')

    values.reset_index(inplace=True)
    return values

def ensure_coverage(df):
    """Make sure that records cover atleast 90% of the year.
    """
    logging.debug('Ensuring coverage.')
    site_count = df.SiteCode.value_counts()
    eligible_sites = site_count[site_count > 7888].index

    df = df.loc[df.SiteCode.isin(eligible_sites), :]
    df.reset_index(drop=True, inplace=True)
    return df

def save_file(df, source_path):
    logging.info('Saving file...')
    file_name = f'{os.path.splitext(os.path.basename(source_path))[0]}.parquet'
    save_path = os.path.join(INTERIM_DATA, file_name)

    df.to_parquet(save_path,
                  engine='fastparquet',
                  compression='SNAPPY')

    logging.info(f'Saved file under: {save_path}')

def concat_data():
    params = ['WIND', 'TEMP', 'PRESS', 'RH_DP',
              '42101', '88502', '42602', '44201',
              'SPEC', '88101', '81102', '42401']

    for param in params:
        print(param)
        files = glob(f'{INTERIM_DATA}/**.parquet', recursive=True)
        files = [file for file in files  if re.search(f'hourly_({param})', file)]
        tables = [dd.read_parquet(file) for file in files]
        df = dd.concat(tables)

        save_path = join(INTERIM_DATA, f'combined-{param}.parquet')
        df.to_parquet(save_path, engine='fastparquet', compression='SNAPPY')

def merge_weather():
    wind_path = join(INTERIM_DATA, 'combined-WIND.parquet')
    temp_path = join(INTERIM_DATA, 'combined-TEMP.parquet')
    press_path = join(INTERIM_DATA, 'combined-PRESS.parquet')
    rhdp_path = join(INTERIM_DATA, 'combined-RH_DP.parquet')

    wind = dd.read_parquet(wind_path)
    temp = dd.read_parquet(temp_path)
    press = dd.read_parquet(press_path)
    rhdp = dd.read_parquet(rhdp_path)

    weather = dd.merge(wind, temp, on=['SiteCode', 'LocalDate'])
    weather = dd.merge(weather, press, on=['SiteCode', 'LocalDate'])
    weather = dd.merge(weather, rhdp, on=['SiteCode', 'LocalDate'])


    # additional cleaning - data-review-1.0
    # almost no figures with Dew Point, also it's f(p, T)
    # missing values most likely caused by measurement 1 of 2 parameters at a Time
    # for wind speed and direction
    weather = weather.drop('Dew Point', axis=1)
    weather = weather.dropna()

    save_path = join(INTERIM_DATA, f'combined-WEATHER.parquet')
    weather.to_parquet(save_path, engine='fastparquet', compression='SNAPPY')

def create_dataset(param_name):
    weather_path = join(INTERIM_DATA, 'combined-WEATHER.parquet')
    weather_file = dd.read_parquet(weather_path)

    file_path = join(INTERIM_DATA, f'combined-{param_name}.parquet')
    df = dd.read_parquet(file_path)

    merged = dd.merge(weather_file,
                      df,
                      on=['SiteCode', 'LocalDate'],
                      how='right')

    save_path = join(PROCESSED_DATA, f'{param_name}.parquet')
    merged.to_parquet(save_path, engine='fastparquet', compression='SNAPPY')


# globals
RAW_DATA = join(ROOT, 'data', 'raw')
INTERIM_DATA = join(ROOT, 'data', 'interim')
PROCESSED_DATA = join(ROOT, 'data', 'processed')
LOG_PATH = join(ROOT, 'src', 'logs', 'preproc.log')
KEEP_COLS = ['SiteCode', 'LocalDate', 'Parameter Name', 'POC',
             'Sample Measurement', 'Units of Measure']
PARAMS = ['42101', '88502', '42602', '44201', 'SPEC', '88101', '81102', '42401']


logging.basicConfig(filename=LOG_PATH,
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S')


def main():
    logging.info('Reading files...')
    files = glob(f'{RAW_DATA}/*.csv', recursive=True)
    files = no_overlaping_files(files, dir_target=INTERIM_DATA)
    logging.info(files)

    for file in files:
        logging.info(f'Loading {os.path.basename(file)}')

        # something is messed up with Qualifier column
        data = dd.read_csv(file,
                           dtype={'Qualifier': 'object',
                                        'MDL': 'float64',
                                        'Date of Last Change': 'object'},
                           assume_missing=True)
        logging.info(f'File loaded successfully\n{data.info()}')

        data = create_sitecode(data)
        data = only_predefined_sites(data)
        data = create_localdate(data)
        data = average_instruments(data)
        data = ensure_coverage(data)

        save_file(data, file)

    concat_data()
    merge_weather()

    for param in PARAMS:
        create_dataset(param)

    atexit.register(logging.info, 'Closing...')


if __name__ == '__main__':
    main()
