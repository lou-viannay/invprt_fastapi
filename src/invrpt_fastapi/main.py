#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>                                    
import logging
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Optional, List, Union, Tuple
from datetime import timedelta

import jinja2
from fastapi import FastAPI, HTTPException, Depends, Body
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from jose.exceptions import JWTError
from libcommon import getConfig, setupLogging
from libcommon.db import get_connection
from libcommon.db.connect import DBConnection
from starlette import status
from fastapi import BackgroundTasks
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from typing_extensions import Annotated

from dibol_parser import DibolParser, DibolRecord
from models import BranchInfo, Token, TokenData, ResultMessage, CallbackRecord, ResultSyncStatus, FTPRecord
from semaphore import get_keyed_semaphore
from simple_user_db import User, SimpleUserDB
from tasks import task_sync_branch
from utils import lru_cache_ttl

ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

logger = logging.getLogger(__name__)


main_cfg: Optional[dict] = None
dibol_schema: Optional[list[DibolRecord]] = None


def load_config():
    global main_cfg

    if main_cfg is None:
        main_cfg = getConfig()

    return main_cfg


def load_dibol_schema():
    global dibol_schema
    schema_file = Path(main_cfg['dibol_schema'])
    dibol_parser = DibolParser()
    dibol_schema = dibol_parser.parse_file(str(schema_file))


def setup_logging():
    cfg = load_config()
    log_cfg = cfg["logging"]["config"]
    setupLogging(log_cfg, level=logging.DEBUG)


setup_logging()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def get_default_connection():
    return get_connection(main_cfg["databases"]["default"])


def get_local_connect():
    if 'local' in main_cfg['databases']:
        return get_connection(main_cfg["databases"]["local"])
    else:
        return get_default_connection()


# database setup
async def get_db_connection():
    conn = get_default_connection()
    try:
        yield conn
    finally:
        conn.commit()


async def get_local_connection():
    conn = get_local_connect()
    try:
        yield conn
    finally:
        conn.commit()


DBConnectionDep = Annotated[DBConnection, Depends(get_db_connection)]


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    user_db = main_cfg['user_db']
    try:
        payload = jwt.decode(token, user_db.secret, algorithms=[user_db.algorithm])
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = user_db.get_user(username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
        current_user: Annotated[User, Depends(get_current_user)]):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


async def get_external_user(current_user: Annotated[User, Depends(get_current_user)]):
    if not current_user.external:
        raise HTTPException(status_code=403, detail="User not allowed (internal)")
    return current_user


async def get_internal_user(current_user: Annotated[User, Depends(get_current_user)]):
    if current_user.external:
        raise HTTPException(status_code=403, detail="User not allowed (external)")
    return current_user


# noinspection PyUnusedLocal
@asynccontextmanager
async def lifespan(defapp: FastAPI):
    # create_db_and_tables()

    # noinspection PyShadowingNames
    main_cfg = load_config()
    load_dibol_schema()

    user_db = SimpleUserDB(main_cfg['user_db'], main_cfg['secret_key'], main_cfg['algorithm'])
    logger.info(f"users = {user_db}")
    main_cfg['user_db'] = user_db
    # templates = Jinja2Templates(directory=main_cfg["template_dir"])
    template_loader = jinja2.FileSystemLoader(main_cfg["template_dir"])
    environment = jinja2.Environment(loader=template_loader)
    templates = Jinja2Templates(env=environment)
    environment.globals['STATIC_PREFIX'] = '/static'
    main_cfg['templates'] = templates
    yield


app = FastAPI(lifespan=lifespan, title="BakeMark INVRPT API", version="0.1.1")

app.mount("/static", StaticFiles(directory=main_cfg['static_dir']), name="static")


@app.post("/token")
async def login_for_access_token(
        form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    user_db = main_cfg['user_db']
    user = user_db.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = user_db.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


@app.get("/users/me")
async def read_users_me(current_user: Annotated[User, Depends(get_current_user)]):
    return current_user


@app.get("/branch/list")
async def get_branch_list(conn: DBConnectionDep) -> List[BranchInfo]:
    SQL = "SELECT branch_id, branch_name FROM branch_ftp WHERE active=true "
    branches = []
    with conn as sess:
        res = sess.execute(SQL)
        for row in res:
            branches.append(
                BranchInfo(branch_no=row[0], name=row[1])
            )
    return branches


@app.get("/dibol/schema")
async def get_dibol_schema(
        current_user: Annotated[User, Depends(get_internal_user)],
) -> List[DibolRecord]:
    logger.info(f"current_user = {current_user}")
    # return [DibolRecord(**x) for x in dibol_schema]
    # return list(map(convert_to_dibol_schema, dibol_schema))
    return dibol_schema or []


@lru_cache_ttl(ttl_seconds=300, maxsize=30)
def valid_branch(conn: DBConnection, branch_no: int) -> Tuple[Optional[FTPRecord], str]:
    SQL = "SELECT active, ftp_host, ftp_username, ftp_password, ddf_filename FROM branch_ftp WHERE branch_id=:branch_id"

    logger.debug("executing SQL: %s", SQL)
    try:
        with conn as sess:
            res = sess.execute(SQL, params={"branch_id": branch_no}).fetchone()
            if res is None:
                return None, f"Branch# {branch_no} not found."
            logger.debug(f"res = {res}")
            ftp = FTPRecord(
                active=bool(res[0]),
                ftp_host=res[1],
                ftp_username=res[2],
                ftp_password=res[3],
                remote_filename=res[4]
            )
    except Exception as e:
        logger.error(f"While fetching branch state/ftp_host: {e!r}", exc_info=True)
        return None, repr(e) + str(e)

    return ftp, ""


@app.get("/dibol/sync/{branch_no}")
async def status_sync_branch(
        branch_no: Union[str, int],
) -> ResultSyncStatus:
    created, semaphore = await get_keyed_semaphore(str(branch_no))
    if semaphore.locked():
        return ResultSyncStatus(status="Pending", pending=True, completed=False)
    elif created:
        return ResultSyncStatus(status="Clear", pending=False,  completed=False)
    else:
        return ResultSyncStatus(status="Done", pending=False, completed=True)


@app.post("/dibol/sync/{branch_no}", status_code=status.HTTP_202_ACCEPTED)
async def sync_branch(
        current_user: Annotated[User, Depends(get_internal_user)],
        branch_no: Union[str, int],
        conn: DBConnectionDep,
        background_tasks: BackgroundTasks,
        # callback: Optional[Annotated[CallbackRecord, Body()]] = None,
) -> ResultMessage:
    logger.debug(f"current_user = {current_user}")
    # logger.debug(f"callback info = {callback}")
    try:
        br_no = int(branch_no)
    except ValueError:
        return ResultMessage(success=False, msg=f"{branch_no} is not a valid branch number.")

    ftp, msg = valid_branch(conn, br_no)
    logger.debug(f"branch #{br_no} active: {ftp.active} host: {ftp.ftp_host}")
    if ftp.active is None:
        return ResultMessage(success=False, msg=msg)
    elif ftp.active is False:
        return ResultMessage(success=False, msg=f"{branch_no} is not active.")
    elif not ftp.ftp_host:
        return ResultMessage(success=False, msg=f"{branch_no} has no valid FTP host.")

    _, semaphore = await get_keyed_semaphore(str(branch_no))
    if semaphore.locked():
        return ResultMessage(success=False, msg=f"{branch_no} is locked, "
                                                f"another request is already pending for this branch.")
    # task_sync_branch(main_cfg, conn, br_no, callback)
    logger.info(f"sending background task for branch #{branch_no}")
    # background_tasks.add_task(task_sync_branch, dibol_schema, ftp, main_cfg, conn, br_no, callback)
    background_tasks.add_task(task_sync_branch, dibol_schema, ftp, main_cfg, conn, br_no)
    logger.info(f"returning message queued for branch #{branch_no}")
    return ResultMessage(msg=f"Branch #{branch_no} is queued for sync from {ftp.ftp_host}")
