#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from jose import jwt
from libcommon.misc.config import yml_loader
from pwdlib import PasswordHash
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class User(BaseModel):
    username: str
    external: bool = True
    full_name: Optional[str] = None
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


password_hash = PasswordHash.recommended()


def fake_hash_password(password: str) -> str:
    return f"fakehashed{password}"


def verify_password(plain_password, hashed_password):
    plain_hashed_password = password_hash.hash(plain_password)
    logger.debug(f"plain_password_hashed = {plain_hashed_password}")
    return password_hash.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    return password_hash.hash(password)


def load_db(user_db_path: Path) -> dict:
    logger.info("Loading user db from yml")
    return yml_loader(user_db_path)


class SimpleUserDB(object):
    def __init__(self, user_db_path: Path, secret: str, algorithm: str = "HS256"):
        self.__db = load_db(user_db_path)
        self.secret = secret
        self.algorithm = algorithm

    def fake_decode_token(self, token):
        user = self.get_user(token)
        return user

    def get_user(self, username: str) -> Optional[User]:
        if username in self.__db:
            user_dict = self.__db[username]
            logger.debug(f"user_dict = ({type(user_dict)}) {user_dict}")
            return UserInDB(**user_dict)

    def authenticate_user(self, username: str, password: str):
        user = self.get_user(username)
        if not user:
            logger.error("User not found")
            return False
        if not verify_password(password, user.hashed_password):
            logger.error("Password is invalid")
            return False
        return user

    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, self.secret, algorithm=self.algorithm)
        return encoded_jwt

    def __str__(self):
        return f"{self.__class__.__name__}: {self.__db.keys()}"
