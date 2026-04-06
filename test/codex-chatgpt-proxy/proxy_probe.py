#!/usr/bin/env python3
import argparse
import asyncio
import http.client
import json
import ssl
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import websockets


UPSTREAM_HOST = "chatgpt.com"
UPSTREAM_WS_SCHEME = "wss"
SKIP_REQ_HEADERS = {
    "host",
    "connection",
    "upgrade",
    "sec-websocket-key",
    "sec-websocket-version",
    "sec-websocket-extensions",
    "sec-websocket-accept",
}
RECENT_WS_ATTEMPTS = deque(maxlen=64)
STATE_LOCK = threading.Lock()
CONFIG = {"ingest_url": None}
SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
}


def log_event(event, **fields):
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "event": event,
        **fields,
    }
    sys.stdout.write(json.dumps(record, sort_keys=True) + "\n")
    sys.stdout.flush()


def redact_header_value(name, value):
    lower = name.lower()
    if lower in SENSITIVE_HEADERS:
        if lower == "authorization" and isinstance(value, str):
            parts = value.split(" ", 1)
            if len(parts) == 2:
                return parts[0] + " [redacted]"
        return "[redacted]"
    return value


def rewrite_path(path):
    split = urlsplit(path)
    clean_path = split.path or "/"
    if clean_path.startswith("/backend-api/"):
        upstream_path = clean_path
    elif clean_path == "/v1/responses":
        upstream_path = "/backend-api/codex/responses"
    elif clean_path.startswith("/v1/responses/"):
        upstream_path = "/backend-api/codex/responses" + clean_path[len("/v1/responses") :]
    elif clean_path.startswith("/api/codex/"):
        upstream_path = "/backend-api/codex/" + clean_path[len("/api/codex/") :]
    else:
        upstream_path = "/backend-api/codex" + clean_path
    if split.query:
        upstream_path += "?" + split.query
    return upstream_path


def interesting_headers(headers):
    keep = {}
    for key in (
        "Authorization",
        "Content-Type",
        "Accept",
        "User-Agent",
        "x-oai-request-id",
        "x-stainless-*",
        "openai-beta",
        "chatgpt-account-id",
        "Sec-WebSocket-Protocol",
        "Upgrade",
        "Connection",
    ):
        if "*" in key:
            prefix = key[:-1].lower()
            for actual, value in headers.items():
                if actual.lower().startswith(prefix):
                    keep[actual] = redact_header_value(actual, value)
        elif key in headers:
            keep[key] = redact_header_value(key, headers[key])
    return keep


def parse_event_shape(payload):
    try:
        data = json.loads(payload)
    except Exception:
        return {"shape": "non-json"}

    if isinstance(data, dict):
        shape = {"shape": "object", "keys": sorted(list(data.keys()))[:10]}
        for field in ("type", "event", "method", "status"):
            if field in data:
                shape[field] = data[field]
        return shape
    if isinstance(data, list):
        return {"shape": "array", "length": len(data)}
    return {"shape": type(data).__name__}


def event_type(event):
    if not isinstance(event, dict):
        return None
    return event.get("type") or event.get("event") or event.get("method")


def extract_text_parts(value):
    parts = []
    if isinstance(value, str):
        parts.append(value)
    elif isinstance(value, list):
        for item in value:
            parts.extend(extract_text_parts(item))
    elif isinstance(value, dict):
        if "text" in value and isinstance(value["text"], str):
            parts.append(value["text"])
        for key in ("content", "input", "message", "delta"):
            if key in value:
                parts.extend(extract_text_parts(value[key]))
    return parts


class TurnRecorder:
    def __init__(self, ingest_url=None):
        self.ingest_url = ingest_url
        self.current = None

    def observe(self, direction, message):
        if isinstance(message, bytes):
            return
        try:
            event = json.loads(message)
        except Exception:
            return

        kind = event_type(event)
        if kind == "response.created":
            self.current = {
                "request_event": None,
                "response_id": event.get("response", {}).get("id") or event.get("id"),
                "model": event.get("response", {}).get("model") or event.get("model"),
                "output_parts": [],
                "events": [],
            }

        if self.current is None and direction == "client_to_upstream":
            if "model" in event or "input" in event or "messages" in event:
                self.current = {
                    "request_event": event,
                    "response_id": None,
                    "model": event.get("model"),
                    "output_parts": [],
                    "events": [],
                }

        if self.current is None:
            return

        self.current["events"].append(event)
        if direction == "client_to_upstream" and self.current.get("request_event") is None:
            if "model" in event or "input" in event or "messages" in event:
                self.current["request_event"] = event
                self.current["model"] = self.current["model"] or event.get("model")

        if direction == "upstream_to_client":
            if kind in ("response.output_text.delta", "response.output_text.added", "output_text.delta"):
                delta = event.get("delta") or event.get("text") or ""
                if isinstance(delta, str):
                    self.current["output_parts"].append(delta)
            else:
                self.current["output_parts"].extend(extract_text_parts(event.get("delta")))

        if kind in ("response.done", "response.completed", "response.failed"):
            self._flush(event)
            self.current = None

    def _flush(self, final_event):
        raw_request, raw_response = self._build_turn_payload(final_event)
        if raw_request is None or raw_response is None:
            log_event("ingest_skipped", reason="unable to_reconstruct_turn")
            return

        log_event(
            "turn_reconstructed",
            request_model=raw_request.get("model"),
            response_id=raw_response.get("id"),
            response_chars=len(raw_response["choices"][0]["message"]["content"]),
        )

        if self.ingest_url:
            self._post_ingest(raw_request, raw_response)

    def _build_turn_payload(self, final_event):
        request_event = self.current.get("request_event") or {}
        model = self.current.get("model") or request_event.get("model") or final_event.get("response", {}).get("model")
        response_id = self.current.get("response_id") or final_event.get("response", {}).get("id") or final_event.get("id") or "codex-probe"

        input_text = "\n".join(part for part in extract_text_parts(request_event.get("input") or request_event.get("messages")) if part)
        if not input_text:
            input_text = "\n".join(part for part in extract_text_parts(request_event) if part)

        output_text = "".join(self.current.get("output_parts", []))
        if not output_text:
            output_text = "\n".join(part for part in extract_text_parts(final_event.get("response") or final_event) if part)

        if not model:
            return None, None

        raw_request = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": input_text or "[unparsed websocket request]",
                }
            ],
        }
        raw_response = {
            "id": response_id,
            "object": "chat.completion",
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": output_text or "[unparsed websocket response]",
                    },
                    "finish_reason": "stop" if event_type(final_event) != "response.failed" else "error",
                }
            ],
        }
        return raw_request, raw_response

    def _post_ingest(self, raw_request, raw_response):
        target = urlsplit(self.ingest_url)
        body = json.dumps(
            {
                "provider": "openai",
                "agent_name": "codex",
                "request": raw_request,
                "response": raw_response,
            }
        ).encode()
        conn_cls = http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(target.hostname, target.port, timeout=30)
        path = target.path or "/v1/ingest"
        if target.query:
            path += "?" + target.query
        try:
            conn.request("POST", path, body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            resp_body = resp.read()
            log_event(
                "ingest_result",
                ingest_url=self.ingest_url,
                status=resp.status,
                response_bytes=len(resp_body),
            )
        except Exception as exc:
            log_event("ingest_error", ingest_url=self.ingest_url, error=str(exc))
        finally:
            conn.close()


def parse_sse_events(body):
    events = []
    event_name = None
    data_lines = []
    for line in body.decode("utf-8", errors="replace").splitlines():
        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
        elif not line.strip():
            if data_lines:
                payload = "\n".join(data_lines)
                if payload != "[DONE]":
                    events.append((event_name, payload))
            event_name = None
            data_lines = []
    if data_lines:
        payload = "\n".join(data_lines)
        if payload != "[DONE]":
            events.append((event_name, payload))
    return events


def maybe_record_http_turn(handler, upstream_path, body, resp_body):
    if "responses" not in upstream_path:
        return

    request_payload = {}
    if body:
        try:
            request_payload = json.loads(body.decode("utf-8"))
        except Exception:
            request_payload = {}

    recorder = TurnRecorder(ingest_url=CONFIG["ingest_url"])
    if request_payload:
        recorder.observe("client_to_upstream", json.dumps(request_payload))

    recorded_events = 0
    for event_name, payload in parse_sse_events(resp_body):
        try:
            event = json.loads(payload)
        except Exception:
            continue
        if event_name and "type" not in event:
            event["type"] = event_name
        recorder.observe("upstream_to_client", json.dumps(event))
        recorded_events += 1

    log_event(
        "http_turn_probe",
        client=handler.client_address[0],
        path=handler.path,
        upstream_path=upstream_path,
        recorded_events=recorded_events,
        request_model=request_payload.get("model"),
    )


def note_ws_attempt(client_ip, path):
    with STATE_LOCK:
        RECENT_WS_ATTEMPTS.append((time.time(), client_ip, path))


def recent_ws_fallback_hint(client_ip, path):
    now = time.time()
    with STATE_LOCK:
        for ts, ip, ws_path in reversed(RECENT_WS_ATTEMPTS):
            if now - ts > 10:
                break
            if ip == client_ip and "responses" in ws_path and "responses" in path:
                return True
    return False


class ProxyHTTPRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_DELETE(self):
        self._proxy()

    def do_OPTIONS(self):
        self._proxy()

    def _proxy(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else b""
        upstream_path = rewrite_path(self.path)
        fallback_hint = recent_ws_fallback_hint(self.client_address[0], self.path)

        log_event(
            "http_request",
            client=self.client_address[0],
            method=self.command,
            path=self.path,
            upstream_path=upstream_path,
            body_bytes=len(body),
            fallback_hint=fallback_hint,
            headers=interesting_headers(self.headers),
        )

        req_headers = {}
        for key, value in self.headers.items():
            lower = key.lower()
            if lower in ("host", "content-length", "connection", "upgrade"):
                continue
            req_headers[key] = value
        req_headers["Host"] = UPSTREAM_HOST

        conn = http.client.HTTPSConnection(UPSTREAM_HOST, timeout=60, context=ssl.create_default_context())
        try:
            conn.request(self.command, upstream_path, body=body or None, headers=req_headers)
            resp = conn.getresponse()
            resp_body = resp.read()
        except Exception as exc:
            log_event(
                "http_upstream_error",
                client=self.client_address[0],
                method=self.command,
                path=self.path,
                upstream_path=upstream_path,
                error=str(exc),
            )
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            payload = json.dumps({"error": str(exc)}).encode()
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        finally:
            conn.close()

        log_event(
            "http_response",
            client=self.client_address[0],
            method=self.command,
            path=self.path,
            upstream_path=upstream_path,
            status=resp.status,
            response_bytes=len(resp_body),
            content_type=resp.getheader("Content-Type"),
        )

        maybe_record_http_turn(self, upstream_path, body, resp_body)

        self.send_response(resp.status, resp.reason)
        for key, value in resp.getheaders():
            lower = key.lower()
            if lower in ("content-length", "transfer-encoding", "connection", "content-encoding"):
                continue
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        if resp_body:
            self.wfile.write(resp_body)


async def relay_messages(ws_client, ws_upstream, direction, sequence):
    async for message in ws_client:
        if isinstance(message, bytes):
            summary = {"shape": "bytes", "bytes": len(message)}
            payload = message
        else:
            summary = parse_event_shape(message)
            payload = message
        sequence.append({"direction": direction, **summary})
        log_event(
            "ws_message",
            direction=direction,
            bytes=len(payload) if isinstance(payload, bytes) else len(payload.encode()),
            **summary,
        )
        recorder = sequence[0] if sequence else None
        if isinstance(recorder, TurnRecorder):
            recorder.observe(direction, payload)
        await ws_upstream.send(payload)


async def ws_handler(client_ws, path):
    client_ip = client_ws.remote_address[0] if client_ws.remote_address else "unknown"
    upstream_path = rewrite_path(path)
    upstream_url = f"{UPSTREAM_WS_SCHEME}://{UPSTREAM_HOST}{upstream_path}"
    note_ws_attempt(client_ip, path)

    request_headers = {
        key: value
        for key, value in client_ws.request_headers.raw_items()
        if key.lower() not in SKIP_REQ_HEADERS
    }

    log_event(
        "ws_upgrade",
        client=client_ip,
        path=path,
        upstream_url=upstream_url,
        headers=interesting_headers(client_ws.request_headers),
    )

    recorder = TurnRecorder(ingest_url=CONFIG["ingest_url"])
    sequence = [recorder]
    try:
        async with websockets.connect(
            upstream_url,
            extra_headers=request_headers,
            max_size=None,
            ping_interval=None,
        ) as upstream_ws:
            log_event("ws_connected", client=client_ip, path=path, upstream_url=upstream_url)
            await asyncio.gather(
                relay_messages(client_ws, upstream_ws, "client_to_upstream", sequence),
                relay_messages(upstream_ws, client_ws, "upstream_to_client", sequence),
            )
    except Exception as exc:
        log_event(
            "ws_error",
            client=client_ip,
            path=path,
            upstream_url=upstream_url,
            error=str(exc),
            turn_events=recorder.current["events"][:20] if recorder.current else [],
        )
        try:
            await client_ws.close(code=1011, reason="proxy upstream error")
        except Exception:
            pass
    else:
        log_event(
            "ws_closed",
            client=client_ip,
            path=path,
            upstream_url=upstream_url,
            turn_events=recorder.current["events"][:20] if recorder.current else [],
        )


def run_http_server(host, port):
    server = ThreadingHTTPServer((host, port), ProxyHTTPRequestHandler)
    log_event("http_server_started", listen=f"http://{host}:{port}")
    server.serve_forever()


async def run_ws_server(host, port):
    log_event("ws_server_started", listen=f"ws://{host}:{port}")
    async with websockets.serve(ws_handler, host, port, max_size=None, ping_interval=None):
        await asyncio.Future()


def main():
    parser = argparse.ArgumentParser(description="Probe Codex ChatGPT transport.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=8765)
    parser.add_argument("--ws-port", type=int, default=8766)
    parser.add_argument("--ingest-url", default=None, help="Optional Tapes ingest endpoint, e.g. http://127.0.0.1:8082/v1/ingest")
    args = parser.parse_args()
    CONFIG["ingest_url"] = args.ingest_url

    http_thread = threading.Thread(
        target=run_http_server,
        args=(args.host, args.http_port),
        daemon=True,
    )
    http_thread.start()

    try:
        asyncio.run(run_ws_server(args.host, args.ws_port))
    except KeyboardInterrupt:
        log_event("shutdown")


if __name__ == "__main__":
    main()
