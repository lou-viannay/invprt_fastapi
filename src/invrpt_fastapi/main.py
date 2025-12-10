#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>                                    
import logging
from contextlib import asynccontextmanager
from typing import Optional
from datetime import timedelta

import jinja2
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from jose.exceptions import JWTError
from libcommon import getConfig, setupLogging
from starlette import status
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from typing_extensions import Annotated

from datadef import Token, TokenData
from simple_user_db import User, SimpleUserDB

ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7

logger = logging.getLogger(__name__)

main_cfg: Optional[dict] = None


def load_config():
    global main_cfg

    if main_cfg is None:
        main_cfg = getConfig()

    return main_cfg


def setup_logging():
    cfg = load_config()
    log_cfg = cfg["logging"]["config"]
    setupLogging(log_cfg, level=logging.DEBUG)

setup_logging()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


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


app = FastAPI(lifespan=lifespan, title="BakeMark Shipment API", version="0.1.1")

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
