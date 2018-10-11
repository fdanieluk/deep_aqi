from deep_aqi.src.download_data import main as download
from deep_aqi.src.unpack_data import main as unpack
from deep_aqi.src.preproc import main as preproc


if __name__ == '__main__':
    download()
    unpack()
    preproc()
