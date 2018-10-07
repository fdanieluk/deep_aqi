#!/home/filip/miniconda3/envs/deep_aqi/bin/python


"""Load files from EPA, get rid of unimportant columns, clean up, drop sites
with bad coverage (less than 7888 records per year). Save to .parquet for
further analysis.
Look into jupyter/data_exploration-1.0.ipynb for details and explanations.
"""


import os.path
from glob import glob
from dask import dataframe as dd
import pandas as pd
import dask
import logging

from deep_aqi import ROOT


pd.set_option('max_columns', 50)
pd.set_option('max_rows', 25)


def create_sitecode(df):
    """Create sitecode to get rid of too many non-unique fields.
    """
    logging.debug('Created SiteCode Column.')
    df['SiteCode'] = df['State Name'] + '_' + df['County Name'] + '_' + df['Site Num'].astype(str)
    return df

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


# globals
RAW_DATA = os.path.join(ROOT, 'data', 'raw')
INTERIM_DATA = os.path.join(ROOT, 'data', 'interim')
LOG_PATH = os.path.join(ROOT, 'src', 'logs', 'preproc.log')
KEEP_COLS = ['SiteCode', 'LocalDate', 'Parameter Name', 'POC',
             'Sample Measurement', 'Units of Measure']

logging.basicConfig(filename=LOG_PATH,
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(asctime)s %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S')


if __name__ == '__main__':
    logging.info('Reading files...')
    files = glob(f'{RAW_DATA}/*.csv', recursive=True)
    logging.info(files)

    for file in files:
        logging.info(f'Loading {os.path.basename(file)}')

        # something is messed up with Qualifier column
        data = dd.read_csv(files[0], dtype={'Qualifier': 'object'})
        logging.info(f'File loaded successfully\n{data.info()}')

        data = create_sitecode(data)
        data = create_localdate(data)
        data = average_instruments(data)
        data = ensure_coverage(data)

        save_file(data, file)
