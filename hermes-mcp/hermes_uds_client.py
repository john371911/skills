#!/usr/bin/env python3
"""
Hermes UDS Client — 極速 MCP 客戶端（給 OpenClaw 螯蝦使用）
============================================================

為 OpenClaw（螯蝦）↔ Hermes（愛馬仕）平級代理間 IPC 設計的高效能客戶端。

特性：
  - UDS 優先（同機 <0.1ms），TCP 自動 fallback（跨機 LAN 1-5ms）
  - 連線池：持久化單一連線，避免每次重新握手
  - 自動 MCP initialize：首次呼叫自動完成握手
  - keep-alive ping：每 30 秒心跳，維持連線活性
  - 預熱（warmup）：啟動時完成 initialize + tools/list，首次呼叫即熱
  - 同步 + 非同步雙 API

使用範例：
    from hermes_uds_client import HermesClient

    client = HermesClient()          # auto UDS, TCP fallback
    client.warmup()                  # pre-initialize (optional, auto on first call)

    result = client.call_tool("agent_inbox", {
        "sender": "螯蝦",
        "message": "你好愛馬仕！"
    })

    result = client.call_tool("conversations_list", {"limit": 10})

部署路徑：~/.openclaw/scripts/hermes_uds_client.py
"""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("hermes_uds_client")

# ── configuration ──────────────────────────────────────────────────────
UDS_PATH = "/tmp/hermes-mcp.sock"
TCP_HOST = "127.0.0.1"  # fixed IP, no DNS resolution
TCP_PORT = 9765
CONNECT_TIMEOUT = 0.01   # 10ms for UDS probe
TCP_CONNECT_TIMEOUT = 1.0  # 1s for TCP
READ_TIMEOUT = 30.0       # 30s for response
KEEPALIVE_INTERVAL = 30   # seconds between ping
WARMUP_TIMEOUT = 5.0      # seconds for warmup initialize


# ── connection pool ────────────────────────────────────────────────────
class _Connection:
    """A persistent MCP connection to Hermes (UDS or TCP socket).

    Maintains a single connection per HermesClient instance.
    Thread-safe: all I/O is guarded by a lock.
    """

    def __init__(self):
        self._sock: Optional[socket.socket] = None
        self._file: Any = None  # socket.makefile() for line reading
        self._lock = threading.Lock()
        self._transport: str = ""  # "uds" or "tcp"
        self._initialized = False
        self._req_id = 0
        self._last_ping = 0.0
        self._connected_at = 0.0

    @property
    def transport(self) -> str:
        return self._transport

    @property
    def is_connected(self) -> bool:
        return self._sock is not None

    def connect(self) -> bool:
        """Connect to Hermes MCP server. UDS first, TCP fallback."""
        with self._lock:
            return self._connect_locked()

    def _connect_locked(self) -> bool:
        if self._sock is not None:
            return True

        # ── try UDS first ─────────────────────────────────────────
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(CONNECT_TIMEOUT)
            sock.connect(UDS_PATH)
            sock.settimeout(READ_TIMEOUT)
            self._sock = sock
            self._file = sock.makefile("r", buffering=1, encoding="utf-8", errors="replace")
            self._transport = "uds"
            self._connected_at = time.monotonic()
            logger.debug("Connected via UDS: %s (%.2fms)", UDS_PATH, (time.monotonic() - self._connected_at) * 1000)
            return True
        except (FileNotFoundError, ConnectionRefusedError, socket.timeout, OSError) as e:
            logger.debug("UDS connect failed (%s), trying TCP fallback...", e)

        # ── TCP fallback ──────────────────────────────────────────
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(TCP_CONNECT_TIMEOUT)
            sock.connect((TCP_HOST, TCP_PORT))
            sock.settimeout(READ_TIMEOUT)
            self._sock = sock
            self._file = sock.makefile("r", buffering=1, encoding="utf-8", errors="replace")
            self._transport = "tcp"
            self._connected_at = time.monotonic()
            logger.debug("Connected via TCP: %s:%d", TCP_HOST, TCP_PORT)
            return True
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            logger.error("TCP connect also failed: %s", e)
            return False

    def disconnect(self) -> None:
        """Close the connection."""
        with self._lock:
            self._disconnect_locked()

    def _disconnect_locked(self) -> None:
        if self._file:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None
        self._initialized = False
        self._transport = ""

    def warmup(self) -> bool:
        """Pre-warm: connect + initialize + tools/list in one shot."""
        if not self.is_connected:
            if not self.connect():
                return False

        t0 = time.monotonic()
        try:
            # MCP initialize
            resp = self._rpc_call("initialize", {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "openclaw-hermes-uds-client", "version": "1.0.0"},
            }, timeout=WARMUP_TIMEOUT)
            if not resp:
                logger.warning("Warmup: initialize failed")
                return False

            with self._lock:
                self._initialized = True

            # Send initialized notification
            self._rpc_notify("notifications/initialized", {})

            # Fetch tools list
            tools_resp = self._rpc_call("tools/list", {}, timeout=WARMUP_TIMEOUT)
            elapsed = (time.monotonic() - t0) * 1000
            tool_count = len(tools_resp.get("tools", [])) if tools_resp else 0
            logger.info("Warmup complete via %s: %d tools loaded in %.1fms", self._transport, tool_count, elapsed)
            return True
        except Exception as e:
            logger.error("Warmup failed: %s", e)
            with self._lock:
                self._disconnect_locked()
            return False

    def ping(self) -> bool:
        """Send keep-alive ping. Returns True if connection is healthy."""
        if not self.is_connected:
            return False
        try:
            resp = self._rpc_call("ping", {}, timeout=5.0)
            return resp is not None and resp.get("pong") is True
        except Exception:
            return False

    def call_tool(self, name: str, arguments: Dict[str, Any], timeout: float = READ_TIMEOUT) -> Optional[Dict[str, Any]]:
        """Call an MCP tool on Hermes. Returns the parsed result dict, or None on failure.

        Auto-initializes on first call if not yet warmed up.
        """
        if not self.is_connected:
            if not self.connect():
                return None

        # Auto-initialize on first call
        if not self._initialized:
            if not self.warmup():
                return None

        # Keep-alive check
        if time.monotonic() - self._last_ping > KEEPALIVE_INTERVAL:
            if not self.ping():
                logger.warning("Keep-alive ping failed, reconnecting...")
                with self._lock:
                    self._disconnect_locked()
                if not self.connect() or not self.warmup():
                    return None
            self._last_ping = time.monotonic()

        try:
            result = self._rpc_call("tools/call", {
                "name": name,
                "arguments": arguments,
            }, timeout=timeout)
            return result
        except Exception as e:
            logger.error("call_tool('%s') failed: %s", name, e)
            # Reconnect on failure
            with self._lock:
                self._disconnect_locked()
            return None

    # ── internal JSON-RPC ──────────────────────────────────────────

    def _next_id(self) -> int:
        with self._lock:
            self._req_id += 1
            return self._req_id

    def _rpc_call(self, method: str, params: dict, timeout: float = READ_TIMEOUT) -> Optional[dict]:
        """Send a JSON-RPC request and wait for the matching response."""
        req_id = self._next_id()
        request = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}

        with self._lock:
            if self._sock is None:
                return None
            try:
                self._sock.settimeout(timeout)
                payload = json.dumps(request) + "\n"
                self._sock.sendall(payload.encode())

                # Read response line
                line = self._file.readline()
                if not line:
                    raise ConnectionError("Connection closed by server")
                response = json.loads(line.strip())
            except (socket.timeout, TimeoutError):
                logger.warning("RPC call '%s' timed out after %.1fs", method, timeout)
                return None
            except (ConnectionResetError, BrokenPipeError, ConnectionError) as e:
                logger.warning("RPC call '%s' connection error: %s", method, e)
                self._disconnect_locked()
                return None

        if "error" in response:
            err = response["error"]
            logger.warning("RPC error on '%s': code=%s message=%s", method, err.get("code"), err.get("message"))
            return None

        return response.get("result")

    def _rpc_notify(self, method: str, params: dict) -> None:
        """Send a JSON-RPC notification (no response expected)."""
        request = {"jsonrpc": "2.0", "method": method, "params": params}
        with self._lock:
            if self._sock is None:
                return
            try:
                self._sock.sendall((json.dumps(request) + "\n").encode())
            except Exception:
                pass


# ── public API ─────────────────────────────────────────────────────────
class HermesClient:
    """High-level Hermes MCP client for OpenClaw.

    Usage:
        client = HermesClient()
        client.warmup()  # optional, auto on first call

        # Call any MCP tool
        result = client.call_tool("agent_inbox", {"sender": "螯蝦", "message": "hello"})
        result = client.call_tool("conversations_list", {"limit": 5})
        result = client.call_tool("messages_read", {"session_key": "..."})
    """

    def __init__(self, uds_path: str = UDS_PATH, tcp_host: str = TCP_HOST, tcp_port: int = TCP_PORT):
        self._conn = _Connection()

    @property
    def transport(self) -> str:
        """Returns 'uds', 'tcp', or '' (not connected)."""
        return self._conn.transport

    @property
    def is_connected(self) -> bool:
        return self._conn.is_connected

    def warmup(self) -> bool:
        """Pre-warm the connection: connect + MCP initialize + fetch tools.

        Call this at startup to ensure the first tool call is instant.
        Returns True if warmup succeeded.
        """
        return self._conn.warmup()

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = READ_TIMEOUT) -> Optional[Dict[str, Any]]:
        """Call an MCP tool on Hermes.

        Args:
            name: Tool name (e.g. 'agent_inbox', 'conversations_list')
            arguments: Tool arguments dict
            timeout: Response timeout in seconds (default 30)

        Returns:
            Parsed result dict, or None on failure.
        """
        return self._conn.call_tool(name, arguments or {}, timeout=timeout)

    def disconnect(self) -> None:
        """Close the connection."""
        self._conn.disconnect()

    # ── convenience methods ───────────────────────────────────────

    def send_agent_message(self, sender: str, message: str, session_key: str = "") -> Optional[dict]:
        """Send a direct agent-to-agent message to Hermes (agent_inbox)."""
        return self.call_tool("agent_inbox", {
            "sender": sender,
            "message": message,
            "session_key": session_key,
        })

    def list_conversations(self, platform: str = None, limit: int = 50) -> Optional[dict]:
        """List active conversations."""
        args = {"limit": limit}
        if platform:
            args["platform"] = platform
        return self.call_tool("conversations_list", args)

    def read_messages(self, session_key: str, limit: int = 50) -> Optional[dict]:
        """Read messages from a conversation."""
        return self.call_tool("messages_read", {"session_key": session_key, "limit": limit})

    def send_message(self, target: str, message: str) -> Optional[dict]:
        """Send a message to a platform target."""
        return self.call_tool("messages_send", {"target": target, "message": message})


# ── CLI test entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    client = HermesClient()
    print(f"Connecting to Hermes MCP...")

    if not client.warmup():
        print("ERROR: Failed to connect/warmup. Is hermes-mcp-uds.service running?")
        sys.exit(1)

    print(f"Connected via {client.transport.upper()} ✓")

    # Test: list conversations
    result = client.call_tool("conversations_list", {"limit": 3})
    if result:
        data = json.loads(result.get("content", [{}])[0].get("text", "{}"))
        print(f"Conversations: {data.get('count', 0)}")

    # Test: agent inbox
    result = client.call_tool("agent_inbox", {"sender": "螯蝦-test", "message": "UDS client smoke test"})
    if result:
        data = json.loads(result.get("content", [{}])[0].get("text", "{}"))
        print(f"Agent inbox: {data.get('received', False)}")

    print("All tests passed ✓")
    client.disconnect()
