\---

name: "hermes-mcp"
description: "OpenClaw ↔ Hermes MCP agent-to-agent 直通、UDS+TCP 極速 IPC、bridge 相容性、surgical runtime patches、疑難排解"
---

# Hermes MCP — 雙向 OpenClaw ↔ Hermes 純 MCP 互連

透過 MCP (Model Context Protocol) 讓 OpenClaw（螯蝦）與 Hermes（愛馬仕）**純 MCP agent-to-agent 雙向通訊**，零 Telegram、零外部平台介入。

## 架構總覽

```
🦞→🧳 螯蝦→愛馬仕（UDS 極速通道，<0.1ms）：
┌──────────┐  UDS /tmp/hermes-mcp.sock   ┌──────────────────┐
│ OpenClaw │ ──────────────────────────→ │ mcp\_uds\_server   │
│ hermes\_  │    TCP 127.0.0.1:9765       │   (systemd)      │
│ uds\_     │    (跨機 fallback)           │   ├ EventBridge  │
│ client   │                             │   └ 11 tools     │
└──────────┘                             └────────┬─────────┘
                                                   │
🧳→🦞 愛馬仕→螯蝦（TCP bridge + WebSocket）：
┌──────────┐  TCP :8765 (HTTP bridge)     ┌──────────────────┐
│ Hermes   │ ───────────────────────────→ │ bridge\_server.py │
│          │                               │   ↓ subprocess   │
│          │  MCP ws://127.0.0.1:18789    │ openclaw mcp     │
│          │ ───────────────────────────→ │   serve          │
└──────────┘                               └──────┬───────────┘
                                                   │
                                                   ▼
                                            ┌──────────────────┐
                                            │ OpenClaw         │
                                            │ gateway :18789   │
                                            └──────────────────┘
```

### 雙向機制

|方向|傳輸|工具|延遲|
|-|-|-|-|
|🦞→🧳 螯蝦→愛馬仕|**UDS** (`/tmp/hermes-mcp.sock`) 或 TCP fallback|`agent\_inbox`, `conversations\_list`, `messages\_read`, 全部 11 個|<0.1ms (UDS)|
|🧳→🦞 愛馬仕→螯蝦|WebSocket `ws://127.0.0.1:18789` 或 HTTP bridge `127.0.0.1:8765`（TCP）|`messages\_send`, `messages\_read`, `events\_poll`/`events\_wait`|\~7ms（瓶頸在 bridge 子程序，非 TCP）|

\---

## 設定路徑

* **Hermes 安裝**: `/home/OXOX/.hermes/hermes-agent/`
* **Hermes CLI**: `/home/OXOX/.hermes/hermes-agent/venv/bin/hermes`
* **Hermes 設定**: `/home/OXOX/.hermes/config.yaml`
* **Hermes state.db**: `/home/OXOX/.hermes/state.db`
* **UDS socket**: `/tmp/hermes-mcp.sock`（MCP stream server）
* **TCP port**: `0.0.0.0:9765`（跨機 fallback）
* **OpenClaw 設定**: `/home/OXOX/.openclaw/openclaw.json`
* **Gateway 密碼檔**: `/tmp/openclaw\_gateway.pwd`
* **橋接伺服器**: `/home/OXOX/.openclaw/bridge\_server.py`
* **Bridge systemd unit**: `openclaw-mcp-bridge.service`
* **UDS server systemd unit**: `hermes-mcp-uds.service`
* **OpenClaw dist**: `/home/OXOX/.npm-global/lib/node\_modules/openclaw/dist/`

\---

## 方向一：🦞→🧳 螯蝦→愛馬仕（UDS 極速通道）

### 啟動 UDS Server

```bash
systemctl --user enable --now hermes-mcp-uds.service
systemctl --user status hermes-mcp-uds.service
```

### OpenClaw 端呼叫（Python）

```python
from hermes\_uds\_client import HermesClient

client = HermesClient()
client.warmup()  # 預熱：initialize + tools/list（首次呼叫自動執行）

# 發送 agent inbox 訊息
client.send\_agent\_message("螯蝦", "你好愛馬仕！")

# 列出對話
result = client.list\_conversations(limit=5)

# 讀取訊息
result = client.read\_messages("telegram:***", limit=10)

# 發送訊息到平台
client.send\_message("telegram:***", "來自螯蝦的訊息")

client.disconnect()
```

### 提供的工具（11 個）

`agent\_inbox`, `conversations\_list`, `conversation\_get`, `messages\_read`, `attachments\_fetch`, `events\_poll`, `events\_wait`, `messages\_send`, `channels\_list`, `permissions\_list\_open`, `permissions\_respond`

### 效能

|傳輸|冷啟動|熱呼叫|
|-|-|-|
|**UDS**|\~5ms|**<0.1ms**|
|**TCP localhost**|\~10ms|0.2-1ms|
|**TCP LAN**|\~20ms|1-5ms|

### client 設計

* **UDS 優先**：10ms probe UDS，失敗自動 TCP fallback
* **連線池**：持久化單一連線，不重複握手
* **keep-alive**：每 30 秒 ping，維持連線活性
* **預熱**：`warmup()` 一次完成 initialize + tools/list

\---

## 方向二：🧳→🦞 愛馬仕→螯蝦（MCP + HTTP Bridge）

### 設定

在 Hermes `config.yaml` 的 `mcp\_servers` 區段：

```yaml
mcp\_servers:
  openclaw:
    command: /home/OXOX/.npm-global/bin/openclaw
    args:
      - mcp
      - serve
      - --url
      - ws://127.0.0.1:18789
      - --password-file
      - /tmp/openclaw\_gateway.pwd
      - --claude-channel-mode
      - off
```

**重要：必須設 `--claude-channel-mode off`**

### HTTP Bridge（TCP，port 8765）

```bash
systemctl --user start|stop|status openclaw-mcp-bridge.service

# 測試
curl -s -m 5 -X POST http://127.0.0.1:8765/mcp \\
  -H "Content-Type: application/json" \\
  -H "X-MCP-Session: test" \\
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
```

橋接特性：子程序崩潰自動重啟（最多 3 次）、Session idle timeout 10 分鐘、開機啟動

### ⚠️ 螯蝦 MCP 改 UDS 原生支援：風險評估

🧳→🦞 方向若要像 🦞→🧳 一樣達到 <0.1ms，需讓 OpenClaw 直接 expose MCP tools over UDS，**消除 bridge 子程序**。以下是實作路徑與風險：

|方案|做法|風險|建議|
|-|-|-|-|
|**A. Gateway 加 UDS listener**|改 OpenClaw Node.js gateway 的 WebSocket server，加綁 UDS socket|🔴 高 — 改 OpenClaw dist（`gateway-\*.js`），npm 更新即失效，可能破壞 WebSocket 邏輯、認證、session 管理|❌ 不建議|
|**B. UDS sidecar**|寫 Node.js sidecar，監聽 UDS，內部轉發 WebSocket 到 `:18789`|🟡 中 — 需處理 WebSocket upgrade、auth token、session 對應。OpenClaw 的 MCP over WebSocket 非標準 JSON-RPC，需逆向協議|⚠️ 可嘗試但維護成本高|
|**C. 接受現狀**|🧳→🦞 維持 TCP（bridge 或 WebSocket），只有 🦞→🧳 走 UDS|🟢 低 — bridge 熱呼叫已 \~7ms，實務上夠用。瓶頸在 LLM 推理（數秒），不在 IPC|✅ **目前做法**|

**為什麼不建議改 OpenClaw：**

1. OpenClaw dist 是 webpack bundle（壓縮 JS），修改困難且脆弱
2. npm 更新會覆蓋所有修改（需重套 surgical patches）
3. WebSocket 認證（password/token）綁定 TCP，加 UDS 需改認證層
4. OpenClaw 極度敏感（已知 exit code 78、自毀設定），增加改動風險太高

**結論：** 🦞→🧳 方向已用 `mcp\_uds\_server.py` 達到 <0.1ms。🧳→🦞 方向 bridge \~7ms 在實務中已足夠（LLM 推理遠大於 IPC），不值得為省 7ms 冒改 OpenClaw dist 的風險。

> 原始測試數據見 \[`references/bridge-uds-perf-results.md`](references/bridge-uds-perf-results.md)

\---

## 密碼管理

```bash
NEW\_PASS=$(openssl rand -base64 24 | tr -dc 'a-zA-Z0-9' | head -c 32)
python3 -c "
import json
with open('/home/OXOX/.openclaw/openclaw.json') as f:
    cfg = json.load(f)
cfg\['gateway']\['auth']\['password'] = '$NEW\_PASS'
cfg\['gateway']\['auth']\['token'] = '$NEW\_PASS'
with open('/home/liao/.openclaw/openclaw.json', 'w') as f:
    json.dump(cfg, f, indent=2)
    f.write('\\n')
"
echo -n "$NEW\_PASS" > /tmp/openclaw\_gateway.pwd
systemctl --user restart openclaw-gateway.service
```

\---

## Surgical Runtime Patches

直接修改 OpenClaw dist (`\~/.npm-global/lib/node\_modules/openclaw/dist/`)，npm 更新後需重新套用。

### P1: Inline Keyboard 按鈕支援（`{{buttons}}`）

在 `bot-CBwSxkGA.js` delivery 流程插入 parser，偵測 `{{buttons}}...{{/buttons}}` 格式自動轉 inline keyboard。

**腳本：** `\~/.openclaw/scripts/repatch-buttons.py`

```bash
python3 \~/.openclaw/scripts/repatch-buttons.py
```

### P2: "(see attached image)" 圖片佔位符

**問題：** tool 回傳空結果時，傳輸層替換為 `"(see attached image)"`。螯蝦 LLM 讀到此無意義字串後可能納入回覆，Telegram 渲染成圖片佔位符。

**受影響檔案：**

* `openai-transport-stream-\*.js:1949` — `textResult || "(see attached image)"` → `textResult || " "`
* `openai-completions-\*.js:987` — `textResult ? "(see attached image)"` → `textResult ? " "`

**腳本：** `\~/.openclaw/scripts/repatch-see-attached-image.py`

```bash
python3 \~/.openclaw/scripts/repatch-see-attached-image.py
python3 \~/.openclaw/scripts/repatch-see-attached-image.py --check
```

**套用後重啟：**

```bash
systemd-run --user --scope --unit=openclaw-restart systemctl --user restart openclaw-gateway.service
```

### npm 更新後恢復

```bash
npm update -g openclaw
python3 \~/.openclaw/scripts/repatch-buttons.py
python3 \~/.openclaw/scripts/repatch-see-attached-image.py
systemd-run --user --scope --unit=openclaw-restart systemctl --user restart openclaw-gateway.service
```

\---

## 疑難排解

### UDS server 狀態

```bash
systemctl --user status hermes-mcp-uds.service
ls -la /tmp/hermes-mcp.sock
ss -tlnp | grep 9765
journalctl --user -u hermes-mcp-uds.service -f
```

### UDS 手動測試

```bash
python3 -c "
import json, socket
s = socket.socket(socket.AF\_UNIX, socket.SOCK\_STREAM)
s.connect('/tmp/hermes-mcp.sock')
f = s.makefile('r')
# initialize
s.sendall(json.dumps({'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'test','version':'1.0'}}}).encode()+b'\\n')
print(f.readline())
# tools/list
s.sendall(json.dumps({'jsonrpc':'2.0','id':2,'method':'tools/list','params':{}}).encode()+b'\\n')
print(f.readline())
s.close()
"
```

### TCP 手動測試

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | nc -w 2 127.0.0.1 9765
```

### Client CLI 測試

```bash
python3 \~/.openclaw/scripts/hermes\_uds\_client.py
```

### MCP 連線測試（🧳→🦞）

```bash
{
  echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'
  sleep 2
  echo '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
  sleep 2
} | timeout 10 /home/liao/.npm-global/bin/openclaw mcp serve \\
  --url ws://127.0.0.1:18789 \\
  --password-file /tmp/openclaw\_gateway.pwd \\
  --claude-channel-mode off 2>/dev/null
```

\---

## 檔案部署路徑

技能資料夾內的檔案若要直接用，需放到對應的原始路徑：

|檔案|部署路徑|用途|
|-|-|-|
|`mcp\_uds\_server.py`|`\~/.hermes/hermes-agent/mcp\_uds\_server.py`|🆕 UDS+TCP 雙模 MCP server，持久化連線、零冷啟動|
|`hermes\_uds\_client.py`|`\~/.openclaw/scripts/hermes\_uds\_client.py`|🆕 OpenClaw 端 UDS client + 連線池|
|`hermes-mcp-uds.service`|`\~/.config/systemd/user/hermes-mcp-uds.service`|🆕 systemd unit，管理 mcp\_uds\_server.py|
|`mcp\_serve.py`|`\~/.hermes/hermes-agent/mcp\_serve.py`|Hermes MCP server（上游原始碼，含 agent\_inbox 自訂修改）。被 `mcp\_uds\_server.py` import `EventBridge`|
|`bridge\_server.py`|`\~/.openclaw/bridge\_server.py`|HTTP MCP 橋接伺服器（port 8765），🧳→🦞 備援通道|
|`openclaw-mcp-bridge.service`|`\~/.config/systemd/user/openclaw-mcp-bridge.service`|systemd unit，管理 bridge\_server.py|

### 部署步驟

```bash
# 1. 部署 UDS server（🦞→🧳 主通道）
cp skills/hermes-mcp/mcp\_uds\_server.py \~/.hermes/hermes-agent/
cp skills/hermes-mcp/hermes\_uds\_client.py \~/.openclaw/scripts/
cp skills/hermes-mcp/hermes-mcp-uds.service \~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now hermes-mcp-uds.service

# 2. 部署 mcp\_serve.py（Hermes 核心，上游原始碼 + 自訂修改）
cp skills/hermes-mcp/mcp\_serve.py \~/.hermes/hermes-agent/mcp\_serve.py

# 3. 部署 bridge（🧳→🦞 備援通道）
cp skills/hermes-mcp/bridge\_server.py \~/.openclaw/bridge\_server.py
cp skills/hermes-mcp/openclaw-mcp-bridge.service \~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now openclaw-mcp-bridge.service

# 4. 重啟 Hermes gateway（讓 mcp\_serve.py 修改生效）
systemctl --user restart hermes-gateway.service
```

### 注意事項

* **`mcp\_serve.py`** 是 Hermes 原始碼的一部分，目前有未提交的 git 修改（`agent\_inbox` tool、`EventBridge.inject\_agent\_message()`）。`mcp\_uds\_server.py` 依賴這些修改（import EventBridge）。若 Hermes 上游更新覆蓋此檔，需重新部署技能內的版本
* **`mcp\_uds\_server.py` 零改動現有檔案**：不碰 `mcp\_serve.py`、`bridge\_server.py`、gateway — 完全獨立 sibling process。`systemctl --user stop hermes-mcp-uds` 即可停用
* **🧳→🦞 方向維持 TCP**：bridge 子程序瓶頸 \~7ms，UDS 無實益。詳見上方「螯蝦 MCP 改 UDS 原生支援：風險評估」
* **技能目錄為來源**：所有修改的檔案都放 `\~/.hermes/skills/hermes-mcp/`，部署時從技能目錄複製到目標路徑
* **npm 更新後**：`repatch-buttons.py` 和 `repatch-see-attached-image.py` 需重新套用（見 Surgical Runtime Patches 章節）

## 版本歷史

|版本|日期|改動|
|-|-|-|
|v9|2026-07-15|🔄 還原 bridge UDS：實測 UDS 對 bridge 無實益（瓶頸在子程序，非傳輸層）。新增「螯蝦 MCP 改 UDS 原生支援：風險評估」章節，詳列三方案與風險。bridge 回 TCP-only|
|v8|2026-07-15|🧹 清理：刪除 `agent\_inbox\_send.py`、`mcp\_serve\_wrapper.py`（被 `hermes\_uds\_client.py` 取代）。重寫 SKILL.md 以 UDS 為主架構，精簡為 7 檔案|
|v7|2026-07-15|🆕 UDS+TCP 雙模 MCP server（`mcp\_uds\_server.py` + `hermes\_uds\_client.py`）：同機 UDS <0.1ms、跨機 TCP 1-5ms、連線池+keep-alive+預熱、零改動現有檔案|
|v6|2026-07-15|修正：`run.py` 完全沒被改動。agent\_inbox 全部在 `mcp\_serve.py` 實作。新增檔案部署路徑章節|
|v5|2026-07-12|新增 Surgical Runtime Patches 章節（inline keyboard + see attached image fix）、Bridge 相容性注意事項|
|v4|2026-07-11|`mcp\_serve.py` 五層例外處理|
|v3|2026-07-11|JSONL polling → Unix datagram socket|
|v2|2026-07-11|JSONL IPC + gateway watcher + `Platform.LOCAL`|
|v1|2026-07-10|初始 MCP 設定、bridge\_server.py|



