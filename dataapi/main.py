import asyncio
from contextlib import asynccontextmanager, contextmanager
import io
import os
import sys
from aiofile import async_open
from fastapi import Body, FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pathlib import Path
import psycopg
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from starlette import status
import time
from typing import Generator, IO, Any, Optional
from zstandard import ZstdCompressor, ZstdDecompressor

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

ROOT_DIR = Path(os.getenv("ROOT_DIR", R"F:\poe-dev\inya"))
DATA_DIR = ROOT_DIR / "data"
INDEX_DIR = ROOT_DIR / "index"
INYA_DB_URI = os.getenv("INYA_DB_URI")

if INYA_DB_URI is None:
    raise RuntimeError("Environment variable INYA_DB_URI not set")


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


async def compress_zstd_gen(data: bytes):
    cctx = ZstdCompressor()
    for chunk in cctx.read_to_iter(data):
        yield chunk


async def decompress_zstd_gen(data: bytes):
    dctx = ZstdDecompressor()
    for chunk in dctx.read_to_iter(data):
        yield chunk


def filename_headers(filename: str, disp="attachment"):
    return {"Content-Disposition": f'{disp};filename="{filename}"'}


def compressed_response(data: bytes, compression: Optional[str], media_type: str, headers: dict[str, str]) -> Response:
    if compression is None:
        return StreamingResponse(
            compress_zstd_gen(data), media_type=media_type, headers=headers
        )
    return Response(content=data, media_type=media_type, headers=headers)


def uncompressed_response(data: bytes, compression: Optional[str], media_type: str, headers: dict[str, str]) -> Response:
    if compression == "zstd":
        return StreamingResponse(
            decompress_zstd_gen(data), media_type=media_type, headers=headers
        )
    return Response(content=data, media_type=media_type, headers=headers)


@app.get("/idxz/{depot}/{gid}/{kind}")
async def get_compressed_index(request: Request, depot: int, gid: int, kind: str):
    if kind not in ["loose", "bundled"]:
        raise HTTPException(status_code=400)
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    SELECT data, compression FROM index
                    WHERE gid = %s AND kind = %s
                    ORDER BY COMPRESSION NULLS LAST
                """,
                (str(gid), kind),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404)
            data, compression = row

    media_type = "application/octet-stream"
    headers = filename_headers(f"{gid}-{kind}.ndjson.zst")
    return compressed_response(data, compression, media_type, headers)


@app.get("/idx/{depot}/{gid}/{kind}")
async def get_index(request: Request, depot: int, gid: int, kind: str):
    if kind not in ["loose", "bundled"]:
        raise HTTPException(status_code=400)
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    SELECT data, compression FROM index
                    WHERE gid = %s AND kind = %s
                    ORDER BY COMPRESSION NULLS FIRST
                """,
                (str(gid), kind),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404)
            data, compression = row

    media_type = "application/x-ndjson"
    headers = filename_headers(f"{gid}-{kind}.ndjson")
    return uncompressed_response(data, compression, media_type, headers)


@app.get("/cadz/{hash}")
async def get_compressed_content_addressable_data(request: Request, hash: str):
    if not all([x in "0123456789abcdefABCDEF" for x in hash]) or len(hash) != 64:
        raise HTTPException(status_code=400)
    hash_bytes = bytes.fromhex(hash)
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    SELECT data, compression FROM data
                    WHERE content_hash = %s
                    ORDER BY compression NULLS LAST
                """,
                (hash_bytes,),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404)
            data, compression = row

            media_type = "application/octet-stream"
            headers = filename_headers(f"{hash_bytes.hex()}.bin.zst")
            return compressed_response(data, compression, media_type, headers)


@app.get("/cad/{hash}")
async def get_content_addressable_data(request: Request, hash: str):
    if not all([x in "0123456789abcdefABCDEF" for x in hash]) or len(hash) != 64:
        raise HTTPException(status_code=400)
    hash_bytes = bytes.fromhex(hash)
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                    SELECT data, compression FROM data
                    WHERE content_hash = %s
                    ORDER BY compression NULLS FIRST
                """,
                (hash_bytes,),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404)
            data, compression = row

            media_type = "application/octet-stream"
            headers = filename_headers(f"{hash_bytes.hex()}.bin")
            return uncompressed_response(data, compression, media_type, headers)
