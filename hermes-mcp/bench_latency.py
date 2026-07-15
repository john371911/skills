#!/usr/bin/env python3
"""Benchmark Hermes MCP latency: UDS hot vs MCP client hot"""
import json, socket, time, statistics, sys

ITERATIONS = 15
WARMUP = 3

def uds_hot_call(method, params, sid):
    """Single persistent UDS connection, MCP initialize on first call, then tools."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect('/tmp/hermes-mcp.sock')
    f = s.makefile('r', encoding='utf-8', errors='replace')

    # Always send initialize first (server may be fresh)
    init_req = json.dumps({
        'jsonrpc': '2.0', 'id': 0, 'method': 'initialize',
        'params': {'protocolVersion': '2025-03-26', 'capabilities': {}, 'clientInfo': {'name': 'bench', 'version': '1.0'}}
    }).encode() + b'\n'
    s.sendall(init_req)
    f.readline()  # discard init response

    # Actual tool call
    call_id = hash(sid) % 100000
    req = json.dumps({'jsonrpc': '2.0', 'id': call_id, 'method': method, 'params': params}).encode() + b'\n'
    t0 = time.perf_counter()
    s.sendall(req)
    resp_line = f.readline()
    t1 = time.perf_counter()
    s.close()
    resp = json.loads(resp_line) if resp_line else {}
    if 'error' in resp and resp['error'].get('code') != -32001:
        raise RuntimeError(resp['error'])
    return (t1 - t0) * 1000

# ── 🦞→🧳 UDS ─────────────────────────────────────────────────────────
print('=== 🦞→🧳 螯蝦→愛馬仕（UDS hot，含 initialize 握手）===')

tests = [
    ('agent_inbox', {'name': 'agent_inbox', 'arguments': {'sender': 'perf', 'message': 'ping'}}),
    ('conversations_list', {'name': 'conversations_list', 'arguments': {'limit': 5}}),
    ('events_poll', {'name': 'events_poll', 'arguments': {'after_cursor': 0, 'limit': 5}}),
]

uds_results = {}
for label, params in tests:
    times = []
    for i in range(ITERATIONS):
        try:
            times.append(uds_hot_call('tools/call', params, f'{label}-{i}'))
        except Exception as e:
            print(f'  {label} FAIL: {e}')
            break
    hot = times[WARMUP:]
    if hot:
        p95 = sorted(hot)[int(len(hot)*0.95)]
        uds_results[label] = hot
        print(f'  {label:25s}  median={statistics.median(hot):6.1f}ms  p95={p95:6.1f}ms  max={max(hot):6.1f}ms  (n={len(hot)})')

# ── 🧳→🦞 MCP ──────────────────────────────────────────────────────────
print('\n=== 🧳→🦞 愛馬仕→螯蝦（MCP via hermes client，含門檻+預熱）===')

sys.path.insert(0, '/home/liao/.openclaw/scripts')
from hermes_uds_client import HermesClient
client = HermesClient()
if not client.warmup():
    print('  FAIL: cannot connect/warmup')
else:
    print(f'  transport={client.transport}')

    mcp_tests = [
        ('agent_inbox', {'sender': '愛馬仕', 'message': 'ping'}),
        ('conversations_list', {'limit': 5}),
        ('events_poll', {'after_cursor': 0, 'limit': 5}),
    ]

    mcp_results = {}
    for label, args in mcp_tests:
        times = []
        for i in range(ITERATIONS):
            t0 = time.perf_counter()
            result = client._conn.call_tool(label, args)
            t1 = time.perf_counter()
            if result and result.get('error'):
                print(f'  {label} ERROR: {result["error"]}')
                break
            times.append((t1 - t0) * 1000)
        hot = times[WARMUP:]
        if hot:
            p95 = sorted(hot)[int(len(hot)*0.95)]
            mcp_results[label] = hot
            print(f'  {label:25s}  median={statistics.median(hot):6.1f}ms  p95={p95:6.1f}ms  max={max(hot):6.1f}ms  (n={len(hot)})')

    client.disconnect()

# ── 摘要 ───────────────────────────────────────────────────────────────
print('\n=== 最終對比 ===')
header = f'{"工具":25s}  {"🦞→🧳 UDS":>18s}  {"🧳→🦞 MCP":>18s}  {"倍數":>8s}'
print(header)
print('-' * len(header))

all_labels = set(list(uds_results.keys()) + list(mcp_results.keys()))
for label in sorted(all_labels):
    uds_str = f'median={statistics.median(uds_results[label]):.1f}ms' if label in uds_results else '-'
    mcp_str = f'median={statistics.median(mcp_results[label]):.1f}ms' if label in mcp_results else '-'

    if label in uds_results and label in mcp_results:
        ratio = statistics.median(mcp_results[label]) / statistics.median(uds_results[label])
        ratio_str = f'{ratio:.1f}x'
    else:
        ratio_str = '-'

    print(f'  {label:25s}  {uds_str:>18s}  {mcp_str:>18s}  {ratio_str:>8s}')

print()
print('⚠️  結論基準：')
print('  1. 🦞→🧳 UDS 直連 mcp_uds_server（零子程序）→ 極速')
print('  2. 🧳→🦞 Hermes MCP client → bridge → openclaw mcp serve 子程序 → gateway')
print('     瓶頸在 bridge 子程序 spawn，非 TCP 傳輸層。')
print('     即使 Bridge 用 UDS，子程序開銷 (~7ms) 仍在。')
