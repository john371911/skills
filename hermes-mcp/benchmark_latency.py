#!/usr/bin/env python3
"""
Hermes ↔ OpenClaw MCP 互連延遲基準測試
========================================
測試兩方向：
  🦞→🧳 螯蝦→愛馬仕：via UDS /tmp/hermes-mcp.sock
  🧳→🦞 愛馬仕→螯蝦：via WebSocket ws://127.0.0.1:18789 / bridge :8765
"""
import json
import socket
import time
import statistics
import sys

# ── 測試參數 ──────────────────────────────────────────────────────────
ITERATIONS = 20
WARMUP = 3

# ── 工具函式 ──────────────────────────────────────────────────────────
def uds_call(method, params=None, sid="perf-test"):
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect("/tmp/hermes-mcp.sock")
    f = s.makefile("r", encoding="utf-8", errors="replace")
    s.sendall((json.dumps(req) + "\n").encode())
    line = f.readline()
    s.close()
    return json.loads(line) if line else {}

def web_call(method, params=None, sid="perf-test"):
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 18789))
    # Very simple WS framing - just send raw for ws:// with upgrade would be complex
    # Use the existing MCP client library instead
    s.close()
    return {}

def measure_uds(method, params=None, iterations=ITERATIONS):
    times = []
    for i in range(iterations):
        t0 = time.perf_counter()
        resp = uds_call(method, params, f"perf-{i}")
        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000
        if "error" in resp:
            print(f"  ERROR: {resp['error']}")
            return None
        times.append(elapsed_ms)
    return times

def measure_tcp(method, params=None, iterations=ITERATIONS):
    """Use HermesClient library for Hermes→OpenClaw tests."""
    sys.path.insert(0, "/home/liao/.openclaw/scripts")
    from hermes_uds_client import HermesClient
    client = HermesClient()
    if not client.warmup():
        print("  FAILED to connect to OpenClaw")
        return None

    times = []
    for i in range(iterations):
        t0 = time.perf_counter()
        result = client._conn.call_tool(method, params or {})
        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000
        if result and "error" in result:
            print(f"  ERROR: {result['error']}")
            return None
        times.append(elapsed_ms)

    client.disconnect()
    return times

def stats_str(times):
    if not times:
        return "N/A"
    return f"min={min(times):.1f}ms  median={statistics.median(times):.1f}ms  p95={sorted(times)[int(len(times)*0.95)]:.1f}ms  max={max(times):.1f}ms"

def print_result(label, times):
    if times is None:
        print(f"{label}: FAILED")
        return
    warm = times[:WARMUP]
    hot = times[WARMUP:]
    print(f"{label}:")
    print(f"  cold (first {WARMUP}): {stats_str(warm)}")
    print(f"  hot  (after warmup): {stats_str(hot)}")

# ── 主程式 ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Hermes ↔ OpenClaw MCP 互連延遲基準測試")
    print("=" * 60)
    print(f"Iterations: {ITERATIONS} (warmup first {WARMUP})\n")

    # 🦞→🧳 direction 1: agent_inbox
    print("🦞→🧳 螯蝦→愛馬仕（UDS /tmp/hermes-mcp.sock）")
    print("  測試: agent_inbox")
    t1 = measure_uds("tools/call", {
        "name": "agent_inbox",
        "arguments": {"sender": "效能測試", "message": "ping"}
    })
    print_result("  agent_inbox", t1)

    print("  測試: conversations_list")
    t2 = measure_uds("tools/call", {
        "name": "conversations_list",
        "arguments": {"limit": 5}
    })
    print_result("  conversations_list", t2)

    # 🧳→🦞 direction 2: Hermes→OpenClaw via MCP
    print("\n🧳→🦞 愛馬仕→螯蝦（MCP via hermes_uds_client）")
    print("  測試: agent_inbox (向螯蝦發送)")
    t3 = measure_tcp("agent_inbox", {
        "sender": "愛馬仕",
        "message": "ping"
    })
    print_result("  agent_inbox", t3)

    print("\n" + "=" * 60)
    print("摘要")
    print("=" * 60)

    results = [
        ("🦞→🧳 agent_inbox (UDS)", t1),
        ("🦞→🧳 conversations_list (UDS)", t2),
        ("🧳→🦞 agent_inbox (TCP/MCP)", t3),
    ]

    for label, times in results:
        if times and len(times) > WARMUP:
            hot = times[WARMUP:]
            median = statistics.median(hot)
            p95 = sorted(hot)[int(len(hot)*0.95)]
            print(f"  {label:40s}  median={median:5.1f}ms  p95={p95:5.1f}ms")
        else:
            print(f"  {label:40s}  FAILED")

    print()
    print("注意：🧳→🦞 走 Hermes MCP client → bridge → openclaw mcp serve 子程序 → gateway WebSocket")
    print("      瓶頸在 bridge 子程序 spawn（~7ms），不在 TCP 傳輸層。")
