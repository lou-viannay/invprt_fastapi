#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>                                    
import logging
import os
import time
from datetime import datetime
from functools import lru_cache, wraps
from pathlib import Path
from typing import Union, Optional, Tuple

import pytz

logger = logging.getLogger(__name__)


def get_file_count(folder: Union[Path, str]) -> int:
    return sum(1 for entry in os.scandir(folder) if entry.is_file())


def find_oldest_file(directory_path: Union[Path, str]) -> Optional[Tuple[int, Path]]:
    """
    Finds the oldest file (based on creation time) in a given directory.

    Args:
        directory_path: The path to the directory to search.

    Returns:
        None if no files are found or a Tuple[int, Path] otherwise.
        - int number of files in the folder.
        - Path object representing the oldest file, or None if no files are found.
    """
    target_directory = Path(directory_path)

    if not target_directory.is_dir():
        logger.error(f"Error: '{directory_path}' is not a valid directory.")
        return None

    oldest_file = None
    oldest_time = float('inf')  # Initialize with a very large time
    count = 0
    for file_path in target_directory.iterdir():
        if file_path.is_file():
            count += 1
            try:
                creation_time = file_path.stat().st_ctime
                if creation_time == oldest_time:
                    if file_path < oldest_file:  # compare their file names to check which is 'smaller'
                        oldest_file = file_path
                        oldest_time = creation_time
                elif creation_time < oldest_time:
                    oldest_time = creation_time
                    oldest_file = file_path
            except OSError as e:
                logger.error(f"Could not get stats for {file_path}: {e}", exc_info=True)
                continue
    return count, oldest_file


def archive_name(fpath: Path) -> str:
    suffixes = fpath.suffixes
    stem = fpath.stem.strip().split('_')[0]
    timestamp = datetime.now(tz=pytz.UTC).strftime('%Y%m%d_%H%M%S_%f')
    return f'{stem}_{timestamp}' + ''.join(suffixes)


def cleanup_old_archives(archive_dir: Path, keep_files: Optional[int] = None):
    """
    Remove oldest archive files if count exceeds keep_files limit

    Args:
        archive_dir: Directory containing archived files for a branch
        keep_files: Number of files to keep
    """
    if keep_files is None or keep_files <= 0:
        return  # No limit, keep all files

    count, oldest = find_oldest_file(archive_dir)

    # Normally, this loop iterates only once, as it is called every time a new file is added
    while count > keep_files:
        try:
            oldest.unlink()
            logger.info(f"Removed old archive: {oldest.name}")
        except OSError as e:
            logger.error(f"Cleanup failed due to error while removing file {oldest}: {e!r}", exc_info=e)
            break

        count, oldest = find_oldest_file(archive_dir)


def lru_cache_ttl(ttl_seconds, maxsize=None):
    def decorator(func):
        @lru_cache(maxsize=maxsize)  # Use lru_cache for capacity management
        def wrapper(*args, __time_salt, **kwargs):
            return func(*args, **kwargs)

        @wraps(func)
        def wrapped(*args, **kwargs):
            # Calculate time_salt based on current time and TTL
            time_salt = int(time.time() / ttl_seconds)
            return wrapper(*args, __time_salt=time_salt, **kwargs)
        return wrapped
    return decorator


@lru_cache_ttl(5, 32)
def test_func(value: int):
    logger.debug("test_func executed.")
    time.sleep(1)
    return value * 2


def main():
    # count, oldest = find_oldest_file("/home/lou/Projects/BakeMark/invprt/archive/branch_001_None")
    count, oldest = find_oldest_file("../../files/archive/branch_001_None")
    print(f"There are {count} file(s), the oldest is {oldest}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
