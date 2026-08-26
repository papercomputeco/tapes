import httpx
import tomllib
from pathlib import Path

from fastapi.testclient import TestClient

from main import MANIFEST, create_app


def test_cassette_contract_and_prompt():
    session_id = "00000000-0000-0000-0000-000000000001"

    def tapes(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/traces"
        assert request.url.params["session_id"] == session_id
        return httpx.Response(
            200,
            json={"items": [{"user_prompt": "  Explain tapes.  "}]},
        )

    client = TestClient(
        create_app(client=httpx.Client(transport=httpx.MockTransport(tapes)))
    )
    assert client.get("/ping").json() == {"status": "ok", "cassette": "prompt"}
    document = client.get("/openapi").json()
    assert document["x-tapes-cassette"] == MANIFEST
    assert document["openapi"] == "3.1.0"
    assert document["paths"]["/api/prompt/get"]["post"]["x-tapes-mcp"] == {
        "name": "get_prompt",
        "annotations": {
            "readOnlyHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    }
    assert tomllib.loads(Path("cassette.toml").read_text()) == MANIFEST
    expected = {"session_id": session_id, "prompt": "  Explain tapes.  "}
    assert client.get(f"/api/prompt/{session_id}").json() == expected
    assert client.post("/api/prompt/get", json={"session_id": session_id}).json() == expected
    assert client.get("/api/prompt/not-a-uuid").status_code == 422
    assert client.post("/api/prompt/get", json={"session_id": "nope"}).status_code == 422
