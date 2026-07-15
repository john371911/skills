#!/usr/bin/env python3
"""
Hermes MCP UDS+TCP Dual-Transport Server
========================================
為 OpenClaw（螯蝦）↔ Hermes（愛馬仕）平級代理間 IPC 極速通訊設計。

綁定雙傳輸層：
  - Unix Domain Socket: /tmp/hermes-mcp.sock  （同機，繞過 TCP/IP 協議棧，<0.1ms）
  - TCP: 0.0.0.0:9765                         （跨機 LAN，1-5ms；固定 IP 避免 DNS 解析）

協議：newline-delimited JSON-RPC 2.0（與 stdio MCP 相同線格式）
連線池：每條連線持久化，keep-alive ping 每 30 秒，啟動時預熱完成 MCP initialize

零改動現有 mcp_serve.py / bridge_server.py — 完全獨立 sibling process。
部署路徑：~/.hermes/hermes-agent/mcp_uds_server.py
systemd unit：hermes-mcp-uds.service
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, Optional

# ── path setup: allow import from hermes-agent package ──────────────────
_HERMES_AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
if _HERMES_AGENT_DIR not in sys.path:
    sys.path.insert(0, _HERMES_AGENT_DIR)

logger = logging.getLogger("hermes.mcp_uds")

# ── configuration ──────────────────────────────────────────────────────
UDS_PATH = "/tmp/hermes-mcp.sock"
TCP_HOST = "0.0.0.0"
TCP_PORT = 9765
KEEPALIVE_INTERVAL = 30  # seconds between ping
SESSION_IDLE_SEC = 600   # reap idle sessions after 10 min
CLEANUP_INTERVAL = 120   # session cleanup interval
READ_TIMEOUT = 35        # seconds for reading a single JSON-RPC message
BACKLOG = 16             # connection backlog

# ── lazy imports from mcp_serve ────────────────────────────────────────
_event_bridge = None
_mcp_helpers = None


def _init_imports():
    """Lazy-import EventBridge + helpers from mcp_serve (sibling module)."""
    global _event_bridge, _mcp_helpers
    if _event_bridge is not None:
        return

    from mcp_serve import (  # type: ignore[import-not-found]
        EventBridge,
        _coerce_int,
        _extract_attachments,
        _extract_message_content,
        _get_session_db,
        _load_channel_directory,
        _load_sessions_index,
    )

    _event_bridge = EventBridge
    _mcp_helpers = {
        "coerce_int": _coerce_int,
        "extract_attachments": _extract_attachments,
        "extract_message_content": _extract_message_content,
        "get_session_db": _get_session_db,
        "load_channel_directory": _load_channel_directory,
        "load_sessions_index": _load_sessions_index,
    }


# ── session tracking ───────────────────────────────────────────────────
class Session:
    """One MCP client session (connected via UDS or TCP)."""

    __slots__ = ("id", "initialized", "last_used", "transport")

    def __init__(self, session_id: str, transport: str):
        self.id = session_id
        self.initialized = False
        self.last_used = time.monotonic()
        self.transport = transport  # "uds" or "tcp"


# ── JSON-RPC helpers ───────────────────────────────────────────────────
def _jsonrpc_error(code: int, message: str, req_id=None) -> dict:
    return {"jsonrpc": "2.0", "error": {"code": code, "message": message}, "id": req_id}


def _jsonrpc_result(result, req_id=None) -> dict:
    return {"jsonrpc": "2.0", "result": result, "id": req_id}


# ── tool implementations ───────────────────────────────────────────────
class ToolHandlers:
    """Dispatch MCP tools/list and tools/call to actual implementations.

    Mirrors the tool surface of mcp_serve.py's create_mcp_server()
    but operates on the raw JSON-RPC level — no FastMCP dependency.
    """

    # Standard JSON-RPC error codes
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603

    def __init__(self, bridge):
        self._bridge = bridge
        self._tools = self._build_tool_list()

    def _build_tool_list(self) -> list:
        """Return the MCP tools/list schema matching mcp_serve.py surface."""
        return [
            {
                "name": "conversations_list",
                "description": "List active messaging conversations across connected platforms.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "platform": {"type": "string", "description": "Filter by platform name"},
                        "limit": {"type": "integer", "description": "Max conversations (default 50)"},
                        "search": {"type": "string", "description": "Text filter for conversation name"},
                    },
                },
            },
            {
                "name": "conversation_get",
                "description": "Get detailed info about one conversation by its session key.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_key": {"type": "string", "description": "The session key from conversations_list"},
                    },
                    "required": ["session_key"],
                },
            },
            {
                "name": "messages_read",
                "description": "Read recent messages from a conversation.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_key": {"type": "string", "description": "The session key from conversations_list"},
                        "limit": {"type": "integer", "description": "Max messages (default 50)"},
                    },
                    "required": ["session_key"],
                },
            },
            {
                "name": "attachments_fetch",
                "description": "List non-text attachments for a message.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "session_key": {"type": "string"},
                        "message_id": {"type": "string"},
                    },
                    "required": ["session_key", "message_id"],
                },
            },
            {
                "name": "events_poll",
                "description": "Poll for new conversation events since a cursor.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "after_cursor": {"type": "integer", "description": "Cursor position (0 for all)"},
                        "session_key": {"type": "string", "description": "Optional filter to one conversation"},
                        "limit": {"type": "integer", "description": "Max events (default 20)"},
                    },
                },
            },
            {
                "name": "events_wait",
                "description": "Wait for the next conversation event (long-poll).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "after_cursor": {"type": "integer"},
                        "session_key": {"type": "string"},
                        "timeout_ms": {"type": "integer", "description": "Max wait in ms (default 30000)"},
                    },
                },
            },
            {
                "name": "messages_send",
                "description": "Send a message to a platform conversation.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target": {"type": "string", "description": "Platform target (e.g. telegram:123456)"},
                        "message": {"type": "string", "description": "Message text"},
                    },
                    "required": ["target", "message"],
                },
            },
            {
                "name": "agent_inbox",
                "description": "Receive a direct message from another MCP agent (e.g. OpenClaw→Hermes).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sender": {"type": "string", "description": "Name of sending agent"},
                        "message": {"type": "string", "description": "Message text"},
                        "session_key": {"type": "string", "description": "Optional session key"},
                    },
                    "required": ["sender", "message"],
                },
            },
            {
                "name": "channels_list",
                "description": "List available messaging channels and targets.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "platform": {"type": "string", "description": "Filter by platform name"},
                    },
                },
            },
            {
                "name": "permissions_list_open",
                "description": "List pending approval requests observed during this bridge session.",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "permissions_respond",
                "description": "Respond to a pending approval request.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Approval ID"},
                        "decision": {
                            "type": "string",
                            "enum": ["allow-once", "allow-always", "deny"],
                        },
                    },
                    "required": ["id", "decision"],
                },
            },
        ]

    # ── dispatch ────────────────────────────────────────────────────

    def handle_tools_list(self) -> dict:
        return {"tools": self._tools}

    def handle_tools_call(self, params: dict) -> dict:
        name = params.get("name", "")
        arguments = params.get("arguments", {})
        if not isinstance(arguments, dict):
            arguments = {}

        handler = getattr(self, f"_tool_{name}", None)
        if handler is None:
            return {"content": [{"type": "text", "text": json.dumps({"error": f"Unknown tool: {name}"})}], "isError": True}

        try:
            result_str = handler(arguments)
            return {"content": [{"type": "text", "text": result_str}]}
        except Exception as e:
            logger.error("Tool %s failed: %s", name, e, exc_info=True)
            return {"content": [{"type": "text", "text": json.dumps({"error": str(e)})}], "isError": True}

    # ── tool implementations ────────────────────────────────────────

    def _tool_conversations_list(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        platform = args.get("platform")
        limit = h["coerce_int"](args.get("limit", 50), default=50, minimum=1, maximum=200)
        search = args.get("search")

        entries = h["load_sessions_index"]()
        conversations = []
        for key, entry in entries.items():
            origin = entry.get("origin", {})
            entry_platform = entry.get("platform") or origin.get("platform", "")
            if platform and entry_platform.lower() != platform.lower():
                continue
            display_name = entry.get("display_name", "")
            chat_name = origin.get("chat_name", "")
            if search:
                s_lower = search.lower()
                if s_lower not in display_name.lower() and s_lower not in chat_name.lower() and s_lower not in key.lower():
                    continue
            conversations.append({
                "session_key": key,
                "session_id": entry.get("session_id", ""),
                "platform": entry_platform,
                "chat_type": entry.get("chat_type", origin.get("chat_type", "")),
                "display_name": display_name,
                "chat_name": chat_name,
                "user_name": origin.get("user_name", ""),
                "updated_at": entry.get("updated_at", ""),
            })
        conversations.sort(key=lambda c: c.get("updated_at", ""), reverse=True)
        conversations = conversations[:limit]
        return json.dumps({"count": len(conversations), "conversations": conversations}, indent=2)

    def _tool_conversation_get(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        session_key = args.get("session_key", "")
        entries = h["load_sessions_index"]()
        entry = entries.get(session_key)
        if not entry:
            return json.dumps({"error": f"Conversation not found: {session_key}"})
        origin = entry.get("origin", {})
        return json.dumps({
            "session_key": session_key,
            "session_id": entry.get("session_id", ""),
            "platform": entry.get("platform") or origin.get("platform", ""),
            "chat_type": entry.get("chat_type", origin.get("chat_type", "")),
            "display_name": entry.get("display_name", ""),
            "user_name": origin.get("user_name", ""),
            "chat_name": origin.get("chat_name", ""),
            "chat_id": origin.get("chat_id", ""),
            "thread_id": origin.get("thread_id"),
            "updated_at": entry.get("updated_at", ""),
            "created_at": entry.get("created_at", ""),
            "input_tokens": entry.get("input_tokens", 0),
            "output_tokens": entry.get("output_tokens", 0),
            "total_tokens": entry.get("total_tokens", 0),
        }, indent=2)

    def _tool_messages_read(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        session_key = args.get("session_key", "")
        limit = h["coerce_int"](args.get("limit", 50), default=50, minimum=1, maximum=200)
        entries = h["load_sessions_index"]()
        entry = entries.get(session_key)
        if not entry:
            return json.dumps({"error": f"Conversation not found: {session_key}"})
        session_id = entry.get("session_id", "")
        if not session_id:
            return json.dumps({"error": "No session ID"})
        db = h["get_session_db"]()
        if not db:
            return json.dumps({"error": "Session database unavailable"})
        try:
            all_messages = db.get_messages(session_id)
        except Exception as e:
            return json.dumps({"error": f"Failed to read messages: {e}"})
        filtered = []
        for msg in all_messages:
            role = msg.get("role", "")
            if role in {"user", "assistant"}:
                content = h["extract_message_content"](msg)
                if content:
                    filtered.append({
                        "id": str(msg.get("id", "")),
                        "role": role,
                        "content": content[:2000],
                        "timestamp": msg.get("timestamp", ""),
                    })
        messages = filtered[-limit:]
        return json.dumps({
            "session_key": session_key,
            "count": len(messages),
            "total_in_session": len(filtered),
            "messages": messages,
        }, indent=2)

    def _tool_attachments_fetch(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        session_key = args.get("session_key", "")
        message_id = str(args.get("message_id", ""))
        entries = h["load_sessions_index"]()
        entry = entries.get(session_key)
        if not entry:
            return json.dumps({"error": f"Conversation not found: {session_key}"})
        session_id = entry.get("session_id", "")
        if not session_id:
            return json.dumps({"error": "No session ID"})
        db = h["get_session_db"]()
        if not db:
            return json.dumps({"error": "Session database unavailable"})
        try:
            all_messages = db.get_messages(session_id)
        except Exception as e:
            return json.dumps({"error": f"Failed to read messages: {e}"})
        target = None
        for msg in all_messages:
            if str(msg.get("id", "")) == message_id:
                target = msg
                break
        if not target:
            return json.dumps({"error": f"Message not found: {message_id}"})
        attachments = h["extract_attachments"](target)
        return json.dumps({"message_id": message_id, "count": len(attachments), "attachments": attachments}, indent=2)

    def _tool_events_poll(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        after_cursor = h["coerce_int"](args.get("after_cursor", 0), default=0, minimum=0, maximum=10**18)
        limit = h["coerce_int"](args.get("limit", 20), default=20, minimum=1, maximum=200)
        session_key = args.get("session_key")
        result = self._bridge.poll_events(after_cursor=after_cursor, session_key=session_key, limit=limit)
        return json.dumps(result, indent=2)

    def _tool_events_wait(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        after_cursor = h["coerce_int"](args.get("after_cursor", 0), default=0, minimum=0, maximum=10**18)
        timeout_ms = h["coerce_int"](args.get("timeout_ms", 30000), default=30000, minimum=0, maximum=300000)
        session_key = args.get("session_key")
        event = self._bridge.wait_for_event(after_cursor=after_cursor, session_key=session_key, timeout_ms=timeout_ms)
        if event:
            return json.dumps({"event": event}, indent=2)
        return json.dumps({"event": None, "reason": "timeout"}, indent=2)

    def _tool_messages_send(self, args: dict) -> str:
        target = args.get("target", "")
        message = args.get("message", "")
        if not target or not message:
            return json.dumps({"error": "Both target and message are required"})
        try:
            from tools.send_message_tool import send_message_tool  # type: ignore[import-not-found]
            return send_message_tool({"action": "send", "target": target, "message": message})
        except ImportError:
            return json.dumps({"error": "Send message tool not available"})
        except Exception as e:
            return json.dumps({"error": f"Send failed: {e}"})

    def _tool_agent_inbox(self, args: dict) -> str:
        sender = args.get("sender", "")
        message = args.get("message", "")
        session_key = args.get("session_key", "")
        self._bridge.inject_agent_message(sender=sender, message=message, session_key=session_key or "")

        # Fire-and-forget Unix datagram socket trigger
        _AGENT_INBOX_SOCKET = "/tmp/hermes_agent_inbox.sock"
        try:
            import socket as _socket
            payload = json.dumps({"sender": sender, "message": message, "session_key": session_key or ""})
            with _socket.socket(_socket.AF_UNIX, _socket.SOCK_DGRAM) as s:
                s.sendto(payload.encode(), _AGENT_INBOX_SOCKET)
        except Exception:
            pass  # best-effort; EventBridge already has the message

        return json.dumps({"received": True, "sender": sender, "message": message[:200]}, indent=2)

    def _tool_channels_list(self, args: dict) -> str:
        _init_imports()
        h = _mcp_helpers
        platform = args.get("platform")
        directory = h["load_channel_directory"]()
        if not directory:
            entries = h["load_sessions_index"]()
            targets = []
            seen = set()
            for key, entry in entries.items():
                origin = entry.get("origin", {})
                p = entry.get("platform") or origin.get("platform", "")
                chat_id = origin.get("chat_id", "")
                if not p or not chat_id:
                    continue
                if platform and p.lower() != platform.lower():
                    continue
                target_str = f"{p}:{chat_id}"
                if target_str in seen:
                    continue
                seen.add(target_str)
                targets.append({
                    "target": target_str,
                    "platform": p,
                    "name": entry.get("display_name") or origin.get("chat_name", ""),
                    "chat_type": entry.get("chat_type", origin.get("chat_type", "")),
                })
            return json.dumps({"count": len(targets), "channels": targets}, indent=2)

        channels = []
        for plat, entries_list in directory.get("platforms", {}).items():
            if platform and plat.lower() != platform.lower():
                continue
            if isinstance(entries_list, list):
                for ch in entries_list:
                    if isinstance(ch, dict):
                        chat_id = ch.get("id", ch.get("chat_id", ""))
                        channels.append({
                            "target": f"{plat}:{chat_id}" if chat_id else plat,
                            "platform": plat,
                            "name": ch.get("name", ch.get("display_name", "")),
                            "chat_type": ch.get("type", ""),
                        })
        return json.dumps({"count": len(channels), "channels": channels}, indent=2)

    def _tool_permissions_list_open(self, args: dict) -> str:
        approvals = self._bridge.list_pending_approvals()
        return json.dumps({"count": len(approvals), "approvals": approvals}, indent=2)

    def _tool_permissions_respond(self, args: dict) -> str:
        approval_id = args.get("id", "")
        decision = args.get("decision", "")
        if decision not in {"allow-once", "allow-always", "deny"}:
            return json.dumps({"error": f"Invalid decision: {decision}"})
        result = self._bridge.respond_to_approval(approval_id, decision)
        return json.dumps(result, indent=2)


# ── MCP UDS+TCP Server ─────────────────────────────────────────────────
class MCPUDSServer:
    """Asyncio MCP server binding both UDS and TCP.

    Features:
      - Dual transport: UDS (same-machine, <0.1ms) + TCP (cross-machine)
      - Persistent connections with keep-alive ping every 30s
      - Session tracking with idle timeout (10 min)
      - MCP JSON-RPC 2.0 over newline-delimited JSON
      - Graceful shutdown on SIGTERM/SIGINT
    """

    def __init__(self):
        self._sessions: Dict[str, Session] = {}
        self._uds_server = None
        self._tcp_server = None
        self._cleanup_task = None
        self._running = False
        self._bridge = None
        self._tools: Optional[ToolHandlers] = None

    # ── lifecycle ──────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the dual-transport MCP server."""
        _init_imports()

        # Start EventBridge
        self._bridge = _event_bridge()
        self._bridge.start()
        self._tools = ToolHandlers(self._bridge)

        # Remove stale socket
        try:
            os.unlink(UDS_PATH)
        except FileNotFoundError:
            pass

        # Bind UDS
        self._uds_server = await asyncio.start_unix_server(
            self._handle_client,
            path=UDS_PATH,
            backlog=BACKLOG,
        )
        os.chmod(UDS_PATH, 0o666)  # allow both agents to connect
        logger.info("UDS listening on %s", UDS_PATH)

        # Bind TCP
        self._tcp_server = await asyncio.start_server(
            self._handle_client,
            host=TCP_HOST,
            port=TCP_PORT,
            backlog=BACKLOG,
        )
        logger.info("TCP listening on %s:%d", TCP_HOST, TCP_PORT)

        # Start cleanup loop
        self._running = True
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        logger.info("MCP UDS+TCP server ready (pid=%d)", os.getpid())

    async def stop(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down MCP UDS+TCP server...")
        self._running = False

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Close all sessions
        for sid in list(self._sessions.keys()):
            self._sessions.pop(sid, None)

        # Stop servers
        for server in (self._uds_server, self._tcp_server):
            if server:
                server.close()
                await server.wait_closed()

        # Remove socket file
        try:
            os.unlink(UDS_PATH)
        except FileNotFoundError:
            pass

        if self._bridge:
            self._bridge.stop()

        logger.info("MCP UDS+TCP server stopped")

    # ── client handler ─────────────────────────────────────────────

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle one MCP client connection (UDS or TCP)."""
        peername = writer.get_extra_info("peername")
        sockpath = writer.get_extra_info("sockname")
        transport = "uds" if sockpath and isinstance(sockname := sockpath, str) and sockname.endswith(".sock") else "tcp"
        session_id = str(uuid.uuid4())
        session = Session(session_id, transport)
        self._sessions[session_id] = session

        addr = peername if peername else "uds"
        logger.debug("[%s] new %s connection, session=%s", addr, transport, session_id[:8])

        buf = b""
        try:
            while self._running:
                try:
                    line = await asyncio.wait_for(reader.readline(), timeout=READ_TIMEOUT)
                except asyncio.TimeoutError:
                    # Send keep-alive ping (empty line = no-op in JSON-RPC)
                    continue

                if not line:
                    logger.debug("[%s] connection closed", addr)
                    break

                session.last_used = time.monotonic()

                try:
                    request = json.loads(line.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    resp = _jsonrpc_error(-32700, "Parse error")
                    writer.write((json.dumps(resp) + "\n").encode())
                    await writer.drain()
                    continue

                response = self._process_request(request, session)
                if response is not None:
                    writer.write((json.dumps(response) + "\n").encode())
                    await writer.drain()

        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            self._sessions.pop(session_id, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _process_request(self, request: dict, session: Session) -> Optional[dict]:
        """Process a single JSON-RPC request. Returns response or None for notifications."""
        req_id = request.get("id")
        method = request.get("method", "")
        params = request.get("params", {})

        # ── initialize ─────────────────────────────────────────────
        if method == "initialize":
            session.initialized = True
            return _jsonrpc_result({
                "protocolVersion": "2025-03-26",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "hermes-mcp-uds", "version": "1.0.0"},
            }, req_id)

        # ── notifications/initialized ──────────────────────────────
        if method == "notifications/initialized":
            return None  # no response for notifications

        # ── ping (keep-alive) ──────────────────────────────────────
        if method == "ping":
            return _jsonrpc_result({"pong": True}, req_id)

        # ── tools/list ─────────────────────────────────────────────
        if method == "tools/list":
            return _jsonrpc_result(self._tools.handle_tools_list(), req_id)

        # ── tools/call ─────────────────────────────────────────────
        if method == "tools/call":
            if not session.initialized:
                return _jsonrpc_error(-32002, "Not initialized", req_id)
            result = self._tools.handle_tools_call(params)
            return _jsonrpc_result(result, req_id)

        # ── unknown method ─────────────────────────────────────────
        return _jsonrpc_error(self._tools.METHOD_NOT_FOUND, f"Method not found: {method}", req_id)

    # ── cleanup ───────────────────────────────────────────────────

    async def _cleanup_loop(self) -> None:
        """Periodically reap idle sessions."""
        while self._running:
            await asyncio.sleep(CLEANUP_INTERVAL)
            now = time.monotonic()
            stale = [
                sid for sid, s in self._sessions.items()
                if now - s.last_used > SESSION_IDLE_SEC
            ]
            for sid in stale:
                self._sessions.pop(sid, None)
            if stale:
                logger.debug("Reaped %d idle sessions", len(stale))


# ── entry point ────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    server = MCPUDSServer()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful shutdown
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(_shutdown(server, loop)))
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

    try:
        loop.run_until_complete(server.start())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


async def _shutdown(server: MCPUDSServer, loop: asyncio.AbstractEventLoop):
    """Signal handler: stop server and event loop."""
    await server.stop()
    loop.stop()


if __name__ == "__main__":
    main()
