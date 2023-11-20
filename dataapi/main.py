from contextlib import asynccontextmanager, contextmanager
import io
import os
from aiofile import async_open
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pathlib import Path
import psycopg
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from starlette import status
import time
from typing import Generator, IO, Any
from zstandard import ZstdDecompressor

ROOT_DIR = Path(os.getenv("ROOT_DIR", R"F:\poe-dev\inya"))
DATA_DIR = ROOT_DIR / "data"
INDEX_DIR = ROOT_DIR / "index"
INYA_DB_URI = os.getenv("INYA_DB_URI")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.async_pool = AsyncConnectionPool(conninfo=INYA_DB_URI)
    yield
    await app.async_pool.close()


app = FastAPI(lifespan=lifespan)


@asynccontextmanager
async def file_closer(file: IO[Any]):
    try:
        yield None
    finally:
        file.close()


async def file_streamer(file: IO[Any]):
    async with (
        file_closer(file),
        async_open(file) as fh,
    ):
        async for chunk in fh.iter_chunked(64 * 2**10):
            yield chunk


async def decompressing_file_streamer(file: IO[Any]):
    dctx = ZstdDecompressor()
    buf = io.BytesIO()
    async with (
        file_closer(file),
        async_open(file) as fh,
    ):
        dc = dctx.stream_writer(buf, write_return_read=False, closefd=False)
        async for chunk in fh.iter_chunked(64 * 2**10):
            write_count = dc.write(chunk)
            if write_count > 0:
                data = bytes(buf.getvalue()[:write_count])
                buf.seek(0)
                yield data


def filename_headers(filename: str, disp="attachment"):
    return {"Content-Disposition": f'{disp};filename="{filename}"'}


@app.get("/idxz/{depot}/{gid}/{kind}")
async def get_compressed_index(depot: int, gid: int, kind: str):
    if kind not in ["loose", "bundled"]:
        raise HTTPException(status_code=400)
    try:
        file_path = INDEX_DIR / str(depot) / f"{gid}-{kind}.ndjson.zst"
        fh = file_path.open(mode="rb")
        return StreamingResponse(
            file_streamer(fh),
            media_type="application/octet-stream",
            headers=filename_headers(file_path.name),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404)


@app.get("/idx/{depot}/{gid}/{kind}")
async def get_index(depot: int, gid: int, kind: str):
    if kind not in ["loose", "bundled"]:
        raise HTTPException(status_code=400)
    try:
        file_path = INDEX_DIR / str(depot) / f"{gid}-{kind}.ndjson.zst"
        fh = file_path.open(mode="rb")
        return StreamingResponse(
            decompressing_file_streamer(fh),
            media_type="application/x-ndjson",
            headers=filename_headers(file_path.with_suffix("").name),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404)


@app.get("/cadz/{hash}")
async def get_compressed_content_addressable_data(hash: str):
    if not all([x in "0123456789abcdefABCDEF" for x in hash]) or len(hash) != 64:
        raise HTTPException(status_code=400)
    hash_bytes = bytes.fromhex(hash)
    try:
        hash_text = hash_bytes.hex()
        file_path = DATA_DIR / hash_text[:2] / f"{hash_text}.bin.zst"
        fh = file_path.open(mode="rb")
        return StreamingResponse(
            file_streamer(fh),
            media_type="application/octet-stream",
            headers=filename_headers(file_path.name),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404)


@app.get("/cad/{hash}")
async def get_content_addressable_data(hash: str):
    if not all([x in "0123456789abcdefABCDEF" for x in hash]) or len(hash) != 64:
        raise HTTPException(status_code=400)
    hash_bytes = bytes.fromhex(hash)
    try:
        hash_text = hash_bytes.hex()
        file_path = DATA_DIR / hash_text[:2] / f"{hash_text}.bin.zst"
        fh = file_path.open(mode="rb")
        return StreamingResponse(
            decompressing_file_streamer(fh),
            media_type="application/octet-stream",
            headers=filename_headers(file_path.with_suffix("").name),
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404)
