from os.path import join, basename
from glob import glob


__all__ = ['no_overlaping_files']


def no_overlaping_files(files_source, dir_target):
    """Check if files in target directory already exist, return only files
    that do not exit in target directory.
    """
    files_target = glob(f'{dir_target}/*', recursive=True)

    # files_source_ = [basename(file).split('.')[0] for file in files_source]
    files_target = [basename(file).split('.')[0] for file in files_target]

    files_source = [file for file in files_source
                    if basename(file).split('.')[0] not in files_target]

    return files_source
