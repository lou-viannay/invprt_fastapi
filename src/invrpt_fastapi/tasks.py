#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
import logging
from datetime import datetime
from ftplib import FTP
from pathlib import Path
from typing import Optional, Tuple, Iterator

import pytz
from libcommon.db.connect import DBConnection

from dibol_parser import DibolDataParser
from models import CallbackRecord, FTPRecord
from pgutil import PostgreSQLInvoiceLoader
from semaphore import get_keyed_semaphore
from utils import archive_name, cleanup_old_archives

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
    msg = "OK"
    logger.debug(f"FTP data: {ftp}")
    try:
        client.connect(ftp.ftp_host)
    except Exception as e:
        msg = f"Error connecting to FTP '{ftp.ftp_host}': {e!r}"
        logger.error(msg, exc_info=True)
        return None, msg
    try:
        client.login(ftp.ftp_username, ftp.ftp_password)
    except Exception as e:
        msg = f"Error logging into FTP '{ftp}': {e!r}"
        logger.error(msg, exc_info=True)
        return None, msg

    if local_filename:
        target = Path(local_filename)
    else:
        target = save_folder / Path(ftp.remote_filename).name

    remote_file = Path(ftp.remote_filename)
    remote_dir = remote_file.parent
    remote_file = remote_file.name.lower()
    client.cwd(remote_dir.as_posix())
    file_list = client.nlst()
    fname = Path(remote_file).stem
    result = next((s for s in file_list if s.lower().startswith(fname)), None)
    logger.debug(f"String '{fname}' in list: {result}")

    if result is not None:
        remote_file = result
    logger.debug(f"current folder: {client.pwd()}")
    # remote_file = remote_file.with_suffix(remote_file.suffix.lower())
    try:
        command_str = f"RETR {remote_file}"
        logger.debug(f"ftp cmd: {command_str}")
        logger.debug(f"target: {target}")
        with target.open('wb') as f:
            client.retrbinary(command_str, f.write)
        file_size = target.stat().st_size
        logger.info(f"Downloaded {file_size:,} bytes to {target}")
        if file_size == 0:
            logger.info(f"Empty file {target}, deleting.")
            target.unlink()
            target = None
            msg = "Got an zero sized file."
    except FileNotFoundError as e:
        msg = f"Remote file is missing (Branch# {branch_no} {ftp.ftp_host}:{remote_dir}/{remote_file}): {e!r})"
        logger.error(msg, exc_info=True)
    except Exception as e:
        msg = f"Error retrieving file from FTP (Branch# {branch_no} '{ftp.ftp_host}/{remote_dir}{remote_file})': {e!r}"
        logger.error(msg, exc_info=True)
    finally:
        client.quit()

    return target, msg


async def task_sync_branch(dibol_schema: list, ftp_rec: FTPRecord, cfg: dict, conn: DBConnection, branch_no: int,
                           callback: Optional[CallbackRecord] = None):
    logger.debug(f"Sync DB: {conn}")
    logger.debug(f"Sync Branch: {branch_no}")
    logger.debug(f"Callback: {callback}")

    save_folder = Path(cfg['sync']['save_folder']) / f'branch_{branch_no:03}'
    archive_path = Path(cfg['sync']['archive_folder'])

    archive_folder = archive_path / f'branch_{branch_no:03}'
    # ensure that the folders exists
    save_folder.mkdir(exist_ok=True, parents=True)
    archive_folder.mkdir(exist_ok=True, parents=True)

    archive_count = cfg['sync']['max_archive_files']
    logger.debug(f"Sync Save Folder: {save_folder}")
    logger.debug(f"Sync Archive Folder: {archive_folder}")
    logger.debug(f"Sync Keep Count: {archive_count}")

    message_folder = save_folder / "msg"
    message_folder.mkdir(exist_ok=True, parents=True)
    message_target = message_folder / "last_message.txt"

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
        download_file, msg = fetch_ftp_file(ftp_rec, save_folder, branch_no)
        if download_file is None:
            with message_target.open('w') as f:
                f.write(f"\n{datetime.now(pytz.UTC).isoformat()}:{msg}")

        processed_count = 0
        for f, data in parse_files(dibol_schema, save_folder):
            processed_count += 1
            # Save parsed data to database
            pg.load_headers(data['headers'], branch_no)
            pg.load_details(data['details'], branch_no)

            # move file to archive and remove extra files if over keep count
            archive_file = archive_folder / archive_name(f)
            logger.info(f"Archive target: {archive_file}")
            # f.rename(archive_file)
            cleanup_old_archives(archive_folder, archive_count)

        # update db
        if processed_count:
            pg.update_last_processed(branch_no)

            with message_target.open('w') as f:
                f.write(f"\n{datetime.now(pytz.UTC).isoformat()}|OK")
        else:
            with message_target.open('w') as f:
                f.write(f"\n{datetime.now(pytz.UTC).isoformat()}|No files to process.")

        logger.debug(f"Sync task for branch# {branch_no} done.")
