#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
import asyncio
import logging
import time
from datetime import datetime
from ftplib import FTP
from pathlib import Path
from typing import Optional, Tuple, Iterator

import pytz
from libcommon.db.connect import DBConnection

from dibol_parser import DibolDataParser
from models import CallbackRecord, FTPRecord
from pgutil import PostgreSQLInvoiceLoader
from utils import find_oldest_file, archive_name, cleanup_old_archives
from semaphore import get_keyed_semaphore

logger = logging.getLogger(__name__)


def parse_files(schema: list, source_folder: Path) -> Iterator[Tuple[Path, dict]]:
    parser = DibolDataParser(schema)
    for f in source_folder.iterdir():
        if not f.is_file():
            logger.info(f"Not a file: {f}")
            continue
        yield f, parser.parse_file(f.as_posix())


def fetch_ftp_file(ftp: FTPRecord, save_folder: Path,
                   branch_no: int,
                   local_filename: Optional[str] = None,
                   timeout: Optional[int] = None):
    client = FTP(timeout=timeout or 30)
    logger.debug(f"FTP data: {ftp}")
    try:
        client.connect(ftp.ftp_host)
    except Exception as e:
        logger.error(f"Error connecting to FTP '{ftp.ftp_host}': {e!r}", exc_info=True)
        return
    try:
        client.login(ftp.ftp_username, ftp.ftp_password)
    except Exception as e:
        logger.error(f"Error logging into FTP '{ftp}': {e!r}", exc_info=True)
        return

    if local_filename:
        target = Path(local_filename)
    else:
        target = save_folder / Path(ftp.remote_filename)

    try:
        with target.open('wb') as f:
            client.retrbinary(f"RETR {ftp.remote_filename}", f.write)
        file_size = target.stat().st_size
        logger.info(f"Downloaded {file_size:,} bytes to {target}")
    except FileNotFoundError as e:
        logger.error(f"Remote file is missing (Branch# {branch_no} {ftp.ftp_host}:{ftp.remote_filename}): {e!r})")
    except Exception as e:
        logger.error(f"Error retrieving file from FTP (Branch# {branch_no} '{ftp.ftp_host}{ftp.remote_filename})': {e!r}", exc_info=True)
    finally:
        client.quit()


async def task_sync_branch(dibol_schema: list, ftp_rec: FTPRecord, cfg: dict, conn: DBConnection, branch_no: int, callback: CallbackRecord):
    import pprint
    logger.debug(f"Sync DB: {conn}")
    logger.debug(f"Sync Branch: {branch_no}")
    logger.debug(f"Callback: {callback}")

    save_folder = Path(cfg['sync']['save_folder']) / f'branch_{branch_no:03}'
    # ensure that the save folder exists
    save_folder.mkdir(exist_ok=True, parents=True)

    archive_path = Path(cfg['sync']['archive_folder'])

    archive_folder = archive_path / f'branch_{branch_no:03}'
    archive_count = cfg['sync']['max_archive_files']
    logger.debug(f"Sync Save Folder: {save_folder}")
    logger.debug(f"Sync Archive Folder: {archive_folder}")
    logger.debug(f"Sync Keep Count: {archive_count}")

    # ensure that only one request is running per branch
    _, semaphore = await get_keyed_semaphore(str(branch_no))
    if semaphore.locked():
        logger.error(f"Branch #{branch_no} is already locked, exiting.")
        return

    async with semaphore:
        cfg_db = cfg['databases'].get('local', None)
        if cfg_db is None:
            cfg_db = cfg['databases']['default']

        pg = PostgreSQLInvoiceLoader(cfg_db)

        # fetch files from FTP:
        fetch_ftp_file(ftp_rec, save_folder, branch_no)

        for f, data in parse_files(dibol_schema, save_folder):
            # Save parsed data to database
            # pg.load_headers(data['headers'], branch_no)
            # pg.load_details(data['details'], branch_no)

            # move file to archive and remove extra files if over keep count
            archive_file = archive_folder / archive_name(f)
            logger.info(f"Archive target: {archive_file}")
            # f.rename(archive_file)
            cleanup_old_archives(archive_folder, archive_count)

        # update db
        pg.update_last_processed(branch_no)

        # logger.debug("sleeping...")
        # await asyncio.sleep(60)

        logger.debug(f"Sync task for branch# {branch_no} done.")
