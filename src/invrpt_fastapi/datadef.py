#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
from typing import Union

from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None

