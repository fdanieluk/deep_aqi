from zipfile import ZipFile
from glob import glob
from os.path import join

from deep_aqi import ROOT


COMPRESSED_DATA = join(ROOT, 'data', 'compressed')
RAW_DATA = join(ROOT, 'data', 'raw')


def main():
    files_to_unpack = glob(f'{COMPRESSED_DATA}/*.zip', recursive=True)
    for file in files_to_unpack:
        ZipFile(file, 'r').extractall(RAW_DATA)


if __name__ == '__main__':
    main()
