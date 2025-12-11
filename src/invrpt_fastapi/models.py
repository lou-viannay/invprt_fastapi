#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
import datetime
from typing import List, Union, Annotated, Optional, Literal

from pydantic import BaseModel
from pydantic import Field, HttpUrl


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class FTPRecord(BaseModel):
    ftp_host: Optional[str] = None
    ftp_username: Optional[str] = None
    ftp_password: Optional[str] = None
    remote_filename: Optional[str] = None
    active: Optional[bool] = None


class BranchInfo(BaseModel):
    branch_no: int
    name: str


class DiBolField(BaseModel):
    field_name: str
    data_type: Annotated[str, Field(min_length=1, max_length=1)]
    length: int
    decimals: int
    start_pos: int
    end_pos: int
    comment: str


class DiBolRecord(BaseModel):
    record_name: str
    is_overlay: bool
    device_no: Optional[int]
    fields: List[DiBolField]


class ResultMessage(BaseModel):
    success: bool = True
    msg: str


class ResultSyncStatus(BaseModel):
    """Sync Status
    ## Status
    * Pending - There is a background process that is running
    * Clear - No background process was run since startup
    * Done - The background process finished
    """
    status: Literal["Pending", "Clear", "Done"] = "Clear"
    pending: bool = False
    completed: bool = False
    message: str = ""
    message_ts: str = ""


class CallbackRecord(BaseModel):
    """URL to 'callback' after background task is done.

    ## properties
    * method: string - request type, defaults to "GET"
    * url: string - Send the request to this URL

    ## request body
    The 'callback' will send a request body of:
    `{
        success: bool
        msg: str,
    }`

    """
    method: Optional[Literal["POST", "GET", "PUT"]] = "GET"
    url: HttpUrl
