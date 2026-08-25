"""A minimal tapes cassette that returns a session's first user prompt."""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

MANIFEST: dict[str, Any] = {
    "kind": "cassette/v1alpha1",
    "cassette": {
        "name": "prompt",
        "version": "0.1.0",
        "display_name": "Prompt",
        "description": "Returns the first captured user prompt for a tapes session.",
        "license": "Apache-2.0",
        "homepage": "https://github.com/papercomputeco/prompt-cassette",
        "image": "tapes/prompt-cassette:0.1.0",
        "port": 9999,
    },
    "depends": {"core": "v1", "views": []},
    "api": {"health": "/ping", "openapi": "/openapi", "prefix_path": "api"},
    "config": [
        {
            "key": "tapes_base_url",
            "type": "string",
            "default": "http://127.0.0.1:8081",
            "description": "Base URL of the tapes core API.",
        }
    ],
}


class PromptRequest(BaseModel):
    session_id: UUID


def create_app(
    tapes_base_url: str | None = None, client: httpx.Client | None = None
) -> FastAPI:
    base_url = (
        tapes_base_url
        or os.getenv("CASSETTE_TAPES_BASE_URL")
        or "http://127.0.0.1:8081"
    ).rstrip("/")
    client = client or httpx.Client(timeout=10.0)
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

    @app.get("/ping")
    def ping() -> dict[str, str]:
        return {"status": "ok", "cassette": "prompt"}

    @app.get("/openapi")
    def openapi() -> JSONResponse:
        return JSONResponse(OPENAPI)

    def find_prompt(session_id: str) -> dict[str, str]:
        try:
            response = client.get(
                f"{base_url}/v1/traces", params={"session_id": session_id}
            )
        except httpx.HTTPError as error:
            raise HTTPException(status_code=502, detail="tapes is unavailable") from error
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="session not found")
        try:
            response.raise_for_status()
            items = response.json().get("items", [])
        except (httpx.HTTPError, ValueError) as error:
            raise HTTPException(status_code=502, detail="invalid response from tapes") from error
        for item in items:
            prompt = item.get("user_prompt", "")
            if isinstance(prompt, str) and prompt:
                return {"session_id": session_id, "prompt": prompt}
        raise HTTPException(status_code=404, detail="prompt not found")

    @app.get("/api/prompt/{session_id}")
    def get_prompt(session_id: UUID) -> dict[str, str]:
        return find_prompt(str(session_id))

    @app.post("/api/prompt/get")
    def get_prompt_tool(request: PromptRequest) -> dict[str, str]:
        return find_prompt(str(request.session_id))

    return app


OPENAPI: dict[str, Any] = {
    "openapi": "3.1.0",
    "info": {
        "title": "Prompt Cassette",
        "description": MANIFEST["cassette"]["description"],
        "version": MANIFEST["cassette"]["version"],
    },
    "x-tapes-cassette": MANIFEST,
    "paths": {
        "/api/prompt/get": {
            "post": {
                "operationId": "getPromptTool",
                "summary": "Get a session's first user prompt",
                "tags": ["prompt"],
                "x-tapes-mcp": {
                    "name": "get_prompt",
                    "annotations": {
                        "readOnlyHint": True,
                        "idempotentHint": True,
                        "openWorldHint": False,
                    },
                },
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["session_id"],
                                "properties": {
                                    "session_id": {
                                        "type": "string",
                                        "format": "uuid",
                                    }
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "The prompt",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["session_id", "prompt"],
                                    "properties": {
                                        "session_id": {"type": "string"},
                                        "prompt": {"type": "string"},
                                    },
                                }
                            }
                        },
                    },
                    "404": {"description": "Session or prompt not found"},
                    "502": {"description": "Tapes is unavailable"},
                },
            }
        },
        "/api/prompt/{session_id}": {
            "get": {
                "operationId": "getPrompt",
                "summary": "Get a session's first user prompt",
                "tags": ["prompt"],
                "parameters": [
                    {
                        "name": "session_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string", "format": "uuid"},
                    }
                ],
                "responses": {
                    "200": {
                        "description": "The prompt",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["session_id", "prompt"],
                                    "properties": {
                                        "session_id": {"type": "string"},
                                        "prompt": {"type": "string"},
                                    },
                                }
                            }
                        },
                    },
                    "422": {"description": "Invalid session id"},
                    "404": {"description": "Session or prompt not found"},
                    "502": {"description": "Tapes is unavailable"},
                },
            }
        }
    },
}


def main() -> None:
    host, _, port = os.getenv("CASSETTE_LISTEN", "127.0.0.1:9999").rpartition(":")
    import uvicorn

    uvicorn.run(create_app(), host=host or "127.0.0.1", port=int(port or 9999))


if __name__ == "__main__":
    main()
