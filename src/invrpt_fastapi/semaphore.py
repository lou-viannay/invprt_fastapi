#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>                                    
import asyncio
from typing import Tuple

# from fastapi import FastAPI, Depends, HTTPException
# from contextlib import asynccontextmanager

# Define a dictionary to hold the semaphores/locks for different keys
# In a real application, consider using a solution that handles cleanup of old keys.
keyed_semaphores: dict[str, asyncio.Semaphore] = {}
SEMAPHORE_LIMIT = 1  # Maximum concurrent operations per key


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # This might not be needed for simple dict implementation, but useful for other lifecycles
#     yield
#     # Cleanup might be needed here if using a more complex library
#
#
# app = FastAPI(lifespan=lifespan)
#

async def get_keyed_semaphore(key: str) -> Tuple[bool, asyncio.Semaphore]:
    """Retrieves or creates a semaphore for a specific key."""
    # Use a global lock to prevent race conditions when creating new semaphores
    global keyed_semaphores
    created = False
    if key not in keyed_semaphores:
        # Note: In a highly concurrent environment, a more sophisticated lock
        # mechanism for dictionary access might be needed, but this works for basic cases
        keyed_semaphores[key] = asyncio.Semaphore(SEMAPHORE_LIMIT)
        created = True
    return created, keyed_semaphores[key]


# @app.get("/process/{user_id}")
# async def process_user_data(user_id: str):
#     """
#     An endpoint that limits concurrent requests per user ID.
#     """
#     semaphore = await get_keyed_semaphore(user_id)
#
#     # Acquire the semaphore. If the limit for this user is reached, it waits.
#     async with semaphore:
#         print(f"Processing started for user {user_id} (active: {semaphore._value})")
#         try:
#             # Simulate an asynchronous I/O-bound task, like an external API call
#             await asyncio.sleep(2)
#         except Exception as e:
#             # Proper error handling
#             raise HTTPException(status_code=500, detail=f"Operation failed: {e}")
#         finally:
#             # The 'async with' statement automatically handles the release
#             print(f"Processing finished for user {user_id}")
#
#     return {"message": f"Data processed for user {user_id}"}
#
