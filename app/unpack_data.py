from zipfile import ZipFile
from glob import glob
import pandas as pd


pd.set_option('max_columns', 50)
pd.set_option('expand_frame_repr', False)


def unzip(zip_file):
    fname = ZipFile(zip_file).namelist()
    df = pd.read_csv(ZipFile(zip_file).open(fname))
    return df

def main():
    zip_dir = r'./compressed_data'
    zip_files = glob(f'{zip_dir}/*', recursive=True)


    # TODO: create HDF Store
    for zip_file in zip_files:
        unzip(zip_file)

        # TODO: write to hdf store under same name as .csv name
        
