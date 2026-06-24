"""
Counts how often each letter appears in total accross multiple documents
"""

from fastapi import FastAPI, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import PlainTextResponse
from starlette.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import threading
import uvicorn
import os
import ray
import signal
import time


# -- Request / Response -------------

class DUUIRequest(BaseModel):
    text: str

class DUUIResponse(BaseModel):
    counts: dict


class DUUIDocumentation(BaseModel):
    annotator_name: str
    version: str
    implementation_lang: str


# -- FastAPI app ---------------

app = FastAPI(
    docs_url="/api",
    redoc_url=None,
    title="DUUI Ray Letter Counter",
    description="Counts letter frequencies using Ray parallel workers",
    version="1.0.0",
    terms_of_service="https://www.texttechnologylab.org/legal_notice/",
    contact={
        "name": "Daniel Bundan",
        "url": "https://texttechnologylab.org",
        "email": "bundan@em.uni-frankfurt.de",
    },
    license_info={
        "name": "AGPL",
        "url": "http://www.gnu.org/licenses/agpl-3.0.en.html",
    },
)

# -- Communication layer (Lua script) ------------

_lua_path =  "communication.lua"
with open(_lua_path, "rb") as f:
    _communication_script = f.read().decode("utf-8")


# -- Ray initialisation -------------

_ray_initialized = False

# Stream mode buffer: one entry per document received via /v1/stream
_stream_lock = threading.Lock()
_streamed_texts: list[str] = []


def _ensure_ray_initialized():
    global _ray_initialized
    if _ray_initialized:
        return

    try:
        ray.init(address="auto", ignore_reinit_error=True)
        print("[LetterCounter] Connected to existing Ray cluster.")
    except Exception as e:
        print(f"[LetterCounter] Could not connect to Ray cluster ({e}); "
              "starting a local one instead.")
        ray.init(ignore_reinit_error=True)

    _ray_initialized = True


# -- Ray worker function ------------------------

@ray.remote
def _count_letters_chunk(chunk: str) -> dict:
    """
    Count letter occurrences in a single text chunk.
    Runs on a Ray worker — one remote call per chunk.
    """
    counts: dict[str, int] = {}
    for char in chunk.lower():
        if char.isalpha():
            counts[char] = counts.get(char, 0) + 1
    return counts


# -- Endpoints ------------------------

@app.get("/v1/communication_layer", response_class=PlainTextResponse)
def get_communication_layer() -> str:
    return _communication_script


@app.get("/v1/typesystem")
def get_typesystem() -> Response:
    # No custom UIMA types needed — results are stored as sofa data.
    empty_ts = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<typeSystemDescription xmlns="http://uima.apache.org/resourceSpecifier">'
        "</typeSystemDescription>"
    )
    return Response(content=empty_ts.encode("utf-8"), media_type="application/xml")


@app.get("/v1/details/input_output")
def get_input_output() -> JSONResponse:
    return JSONResponse(content=jsonable_encoder({
        "inputs":  [],
        "outputs": [],
    }))


@app.get("/v1/documentation")
def get_documentation() -> DUUIDocumentation:
    return DUUIDocumentation(
        annotator_name="DUUI Ray Letter Counter",
        version="1.0.0",
        implementation_lang="Python",
    )


@app.post("/v1/shutdown")
async def shutdown():
    if ray.is_initialized():
        ray.shutdown()


@app.post("/v1/stream")
async def stream_document(request: DUUIRequest) -> JSONResponse:
    """
    Stream mode: buffer one document per call.
    """
    with _stream_lock:
        _streamed_texts.append(request.text)
        count = len(_streamed_texts)
    print(f"[LetterCounter] Buffered document #{count} ({len(request.text)} chars)")
    return JSONResponse(content={"status": "ok", "received": count})


@app.post("/v1/finalize")
async def finalize() -> DUUIResponse:
    """
    End of stream: dispatch one Ray-worker per buffered document (one entire file per worker),
    merge the letter counts from all documents, and return the combined result.
    """
    global _streamed_texts

    with _stream_lock:
        docs = list(_streamed_texts)
        _streamed_texts = []

    if not docs:
        print("[LetterCounter] Finalize called with empty buffer")
        return DUUIResponse(counts={})

    _ensure_ray_initialized()

    print(f"[LetterCounter] Finalizing: dispatching {len(docs)} document(s), one worker each")

    # One remote task per document, each Ray-worker processes one entire file
    futures = [_count_letters_chunk.remote(text) for text in docs]
    partial_counts: list[dict] = ray.get(futures)

    # Merge letter counts across all documents
    total: dict[str, int] = {}
    for partial in partial_counts:
        for letter, count in partial.items():
            total[letter] = total.get(letter, 0) + count

    total = dict(sorted(total.items()))

    print(f"[LetterCounter] Finalize done: {len(docs)} documents, {len(total)} unique letters")
    return DUUIResponse(counts=total)

if __name__ == "__main__":
    uvicorn.run("letter_counter_stream:app", host="0.0.0.0", port=25591, workers=1)
