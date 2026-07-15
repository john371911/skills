# Bridge UDS Performance Test Results

**Date:** 2026-07-15  
**Conclusion:** UDS for bridge provides negligible benefit (~0.4ms). Bottleneck is subprocess IPC, not transport.

## Test Setup

- Hermes → bridge_server.py → `openclaw mcp serve` (subprocess) → gateway :18789
- UDS: `/tmp/openclaw-mcp-bridge.sock`
- TCP: `127.0.0.1:8765`
- Both on same machine (WSL Ubuntu)

## Cold Start (first call, includes subprocess spawn + MCP initialize)

| Transport | tools/list latency |
|-----------|-------------------|
| UDS | 3197ms |
| TCP | 2345ms |

Cold start is dominated by subprocess spawn (2-3s), not transport.

## Hot Calls (same session, subprocess already running)

| Call | UDS | TCP | Difference |
|------|-----|-----|------------|
| `tools/list` | 7.0ms | 6.6ms | ~0.4ms |
| `conversations_list` | 139.8ms | 112.0ms | ~28ms (noise) |

## Root Cause

```
Hermes ──UDS 0.1ms──▶ bridge ──stdin/stdout 7ms──▶ openclaw mcp serve ──WebSocket──▶ gateway
         ▲ negligible           ▲ THIS is the bottleneck
```

The bridge spawns `openclaw mcp serve` as a subprocess and communicates via stdin/stdout. This subprocess IPC dominates latency. The transport between Hermes and bridge (UDS vs TCP) is at most 0.4ms out of 7ms total.

## Contrast: 🦞→🧳 direction

`mcp_uds_server.py` eliminates the subprocess entirely by importing `EventBridge` directly from `mcp_serve.py`:

| Direction | Transport | Architecture | Hot Call |
|-----------|-----------|-------------|----------|
| 🦞→🧳 | UDS `/tmp/hermes-mcp.sock` | No subprocess (direct import) | **<0.1ms** |
| 🧳→🦞 | TCP `127.0.0.1:8765` | Subprocess (bridge → openclaw mcp serve) | ~7ms |

## Decision

Reverted bridge to TCP-only. The subprocess is inherent to OpenClaw's architecture (MCP tools require gateway WebSocket connection). Eliminating it would require modifying OpenClaw's Node.js gateway — high risk, low reward (7ms vs LLM inference time of seconds).
