from zipfile import ZipFile
from glob import glob
from os.path import join

from deep_aqi import ROOT
from deep_aqi.src.helper_functions import no_overlaping_files


COMPRESSED_DATA = join(ROOT, 'data', 'compressed')
RAW_DATA = join(ROOT, 'data', 'raw')


def main():
    files_to_unpack = glob(f'{COMPRESSED_DATA}/*.zip', recursive=True)
    files_to_unpack = no_overlaping_files(files_to_unpack, dir_target=RAW_DATA)
    for file in files_to_unpack:
        ZipFile(file, 'r').extractall(RAW_DATA)


if __name__ == '__main__':
    main()
