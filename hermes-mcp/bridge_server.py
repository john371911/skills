#!/usr/bin/env python3
"""OpenClaw MCP HTTP Bridge v2 — stable, retry, auto-init.

Wraps `openclaw mcp serve` stdio bridge as HTTP at http://127.0.0.1:8765/mcp
so remote clients (Hermes, etc.) can reach OpenClaw over the network.

Improvements over v1:
  - Sends `notifications/initialized` after `initialize` response
  - Auto-retry subprocess on crash (up to 3 attempts)
  - Background stderr collector (avoids pipe buffer deadlock)
  - Session idle timeout + cleanup
  - Proper error responses with JSON-RPC format
"""
import os, sys, json, uuid, asyncio, logging, time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("openclaw-mcp-bridge")

MCP_CMD = [
    "/home/liao/.npm-global/bin/openclaw",
    "mcp",
    "serve",
    "--url", "ws://127.0.0.1:18789",
    "--password-file", "/tmp/openclaw_gateway.pwd",
    "--claude-channel-mode", "auto",
]
MAX_RETRIES = 3
SESSION_IDLE_SEC = 600  # reap sessions unused for 10 min
CLEANUP_INTERVAL_SEC = 120

sessions: dict[str, dict] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("OpenClaw MCP bridge v2 starting on :8765")
    # Periodic session cleanup
    async def _cleanup_loop():
        while True:
            await asyncio.sleep(CLEANUP_INTERVAL_SEC)
            now = time.monotonic()
            stale = [sid for sid, s in list(sessions.items())
                     if now - s["last_used"] > SESSION_IDLE_SEC]
            for sid in stale:
                s = sessions.pop(sid, None)
                if s:
                    _kill_proc(s.get("proc"))
                    logger.info("Reaped idle session %s", sid)
    task = asyncio.create_task(_cleanup_loop())
    yield
    task.cancel()
    for s in list(sessions.values()):
        _kill_proc(s.get("proc"))


def _kill_proc(proc):
    if proc and proc.returncode is None:
        try:
            proc.kill()
        except Exception:
            pass


def _make_error(code: int, msg: str, req_id=None):
    d = {"jsonrpc": "2.0", "error": {"code": code, "message": msg}}
    if req_id is not None:
        d["id"] = req_id
    return d


app = FastAPI(title="OpenClaw MCP HTTP Bridge v2", lifespan=lifespan)


async def _spawn_proc() -> tuple:
    """Spawn openclaw mcp serve subprocess; returns (proc, retry_count)."""
    retries = 0
    last_err = ""
    while retries < MAX_RETRIES:
        logger.info("Spawning openclaw mcp serve (attempt %d/%d)", retries + 1, MAX_RETRIES)
        proc = await asyncio.create_subprocess_exec(
            *MCP_CMD,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # Give it a moment to crash early
        await asyncio.sleep(0.3)
        if proc.returncode is not None:
            # Read stderr for diagnostics
            try:
                err_data = await asyncio.wait_for(proc.stderr.read(), timeout=2)
                last_err = err_data.decode(errors="replace")[:200]
            except asyncio.TimeoutError:
                last_err = "(stderr read timeout)"
            logger.warning("Subprocess died early (rc=%d): %s", proc.returncode, last_err)
            retries += 1
            continue
        # Start background stderr collector
        async def _drain_stderr(p=proc):
            try:
                while True:
                    chunk = await p.stderr.read(4096)
                    if not chunk:
                        break
                    logger.debug("mcp-stderr: %s", chunk.decode(errors="replace").strip())
            except Exception:
                pass
        asyncio.create_task(_drain_stderr())
        return proc
    raise RuntimeError(f"Subprocess failed after {MAX_RETRIES} retries. Last error: {last_err}")


async def get_or_create_session(session_id: str) -> dict:
    now = time.monotonic()
    if session_id in sessions:
        s = sessions[session_id]
        s["last_used"] = now
        proc = s["proc"]
        if proc.returncode is None:
            return s
        else:
            logger.info("Session %s process exited rc=%s; recycling", session_id, proc.returncode)
            del sessions[session_id]

    session_id_input = session_id or str(uuid.uuid4())
    proc = await _spawn_proc()
    s = {
        "id": session_id_input,
        "proc": proc,
        "lock": asyncio.Lock(),
        "initialized": False,
        "last_used": now,
    }
    sessions[session_id_input] = s
    return s


@app.get("/health")
async def health():
    return JSONResponse({"ok": True, "sessions": len(sessions)})


@app.post("/mcp")
async def mcp_handler(request: Request):
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(_make_error(-32700, f"Parse error: {e}"), status_code=400)

    session_id = request.headers.get("X-MCP-Session") or request.query_params.get("session", "")
    if not session_id:
        session_id = str(uuid.uuid4())

    try:
        s = await get_or_create_session(session_id)
    except RuntimeError as e:
        return JSONResponse(_make_error(-32000, str(e), body.get("id")), status_code=502)

    proc = s["proc"]
    lock = s["lock"]

    async with lock:
        s["last_used"] = time.monotonic()
        if proc.returncode is not None:
            return JSONResponse(
                _make_error(-32000, "session process died", body.get("id")),
                status_code=502,
            )

        method = body.get("method", "")

        # Write request to subprocess stdin
        payload = (json.dumps(body) + "\n").encode()
        try:
            proc.stdin.write(payload)
            await proc.stdin.drain()
        except Exception as e:
            logger.error("stdin error: %s", e)
            return JSONResponse(
                _make_error(-32000, f"stdin: {e}", body.get("id")),
                status_code=502,
            )

        # Read response(s) until we find matching id
        response = None
        raw = ""
        try:
            while True:
                line = await asyncio.wait_for(proc.stdout.readline(), timeout=35)
                if not line:
                    return JSONResponse(
                        _make_error(-32000, "session closed", body.get("id")),
                        status_code=502,
                    )
                raw += line.decode("utf-8", errors="replace")
                try:
                    candidate = json.loads(raw)
                    if isinstance(candidate, dict) and candidate.get("id") == body.get("id"):
                        response = candidate
                        break
                    # It's some other message (e.g. notification); discard and continue
                    raw = ""
                except json.JSONDecodeError:
                    continue
        except asyncio.TimeoutError:
            return JSONResponse(
                _make_error(-32000, "response timeout", body.get("id")),
                status_code=504,
            )

        # After successful initialize, send notifications/initialized to the subprocess
        if method == "initialize" and not s["initialized"]:
            s["initialized"] = True
            init_notification = json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n"
            try:
                proc.stdin.write(init_notification.encode())
                await proc.stdin.drain()
            except Exception as e:
                logger.warning("Could not send initialized notification: %s", e)

        headers = {"X-MCP-Session": s["id"], "Content-Type": "application/json"}
        return JSONResponse(content=response, headers=headers)


@app.delete("/mcp")
async def mcp_close(request: Request):
    session_id = request.headers.get("X-MCP-Session") or request.query_params.get("session", "")
    if session_id in sessions:
        s = sessions.pop(session_id)
        _kill_proc(s.get("proc"))
        logger.info("Closed session %s", session_id)
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")
