# AstrBot Factorio 告警锚插件

[![Version](https://img.shields.io/badge/version-1.0.1-blue.svg)](CHANGELOG.md)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

通过会话订阅接收 Factorio 游戏服务器的告警推送，结合 LLM 智能决策并主动推送给用户。

## ✨ 功能特性

- 🚨 **智能告警推送**：接收游戏告警，携带会话历史让 LLM 分析并推送建议
- 🤖 **Agent 模式**：LLM 可自主调用 MCP 工具查询状态、下发控制指令
- 📝 **历史记录**：告警和响应自动存入会话历史，便于后续对话
- 🎯 **灵活配置**：支持三种历史携带模式（all/recent/relevant）

## 🚀 快速开始

### 1. 安装插件

```bash
# 将插件放入 AstrBot 插件目录
cd AstrBot/data/plugins
git clone <your-repo-url> astrbot_plugin_factorio_anchor

# 安装依赖
cd astrbot_plugin_factorio_anchor
pip install -r requirements.txt
```

### 2. 配置插件

在 AstrBot 管理面板中配置：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| alarm_port | 8765 | 告警监听端口 |
| enable_agent_mode | false | 是否启用 Agent 模式（MCP 工具调用） |
| record_alert_history | true | 是否记录告警到会话历史 |
| history_mode | all | 会话历史模式：all/recent/relevant |
| history_count | 0 | 历史条数限制（0=不限制） |

### 3. 订阅告警

在目标会话中发送：

```
/alarm_subscribe
```

### 4. 推送告警

游戏服务器向插件发送 POST 请求：

```bash
curl -X POST http://<host>:8765/alert \
  -H "Content-Type: application/json" \
  -d '{
    "alert_name": "铁板库存告警",
    "severity": "warning",
    "source": "熔炉区",
    "message": "库存 5200 超过阈值 5000"
  }'
```

## 🎮 Agent 模式（MCP 工具）

启用 `enable_agent_mode` 后，LLM 可以调用 MCP 工具：

- `get_current_signals` - 获取实时信号
- `get_probe_config` - 查看探针配置
- `send_command` - 下发控制指令
- `get_history_records` - 查询历史记录
- 更多工具...

**配置 MCP 服务器**（在 AstrBot 主配置中）：

```json
{
  "mcp_servers": {
    "factorio": {
      "transport": "sse",
      "url": "http://<game-server>:8081/sse"
    }
  }
}
```

## 📋 指令列表

| 指令 | 说明 |
|------|------|
| `/alarm_subscribe` | 绑定当前会话为告警目标 |
| `/alarm_unsubscribe` | 取消订阅 |
| `/alarm_subscribe_status` | 查询绑定状态 |
| `/alarm_test` | 发送测试告警 ⭐ |
| `/alarm_debug` | 查看配置和工具状态 ⭐ |

## 🔧 故障排查

### 快速诊断

```bash
# 1. 查看配置和工具状态
/alarm_debug

# 2. 发送测试告警
/alarm_test

# 3. 查看日志
# 应该看到：
# ✓ 全局工具数量: 9
# ✓ 已将工具列表转换为 ToolSet
# ✓ 成功记录告警和响应到会话历史
```

### 常见问题

**Q: Agent 不调用工具？**
- 确认 `enable_agent_mode` 为 `true`
- 运行 `/alarm_debug` 查看工具数量是否 > 0
- 检查 MCP 服务器是否在 AstrBot 主配置中正确配置

**Q: 告警未推送？**
- 确认已运行 `/alarm_subscribe`
- 测试端点：`curl http://<host>:<port>/health`

**Q: 历史未保存？**
- 确认 `record_alert_history` 为 `true`
- 查看日志中是否有 "成功记录告警和响应"

## 📦 项目结构

```
astrbot_plugin_factorio_anchor/
├── main.py                 # 插件主文件
├── mcp_server.py          # MCP 网关（游戏服务器端）
├── metadata.yaml          # 插件元数据
├── _conf_schema.json      # 配置模板
├── requirements.txt       # Python 依赖
├── README.md             # 本文档
├── CHANGELOG.md          # 更新历史
└── docs/                 # AstrBot 开发文档
```

## 🎯 工作流程

```
游戏服务器
    ↓ 信号文件
MCP 网关
    ↓ 阈值检查
    ↓ HTTP POST
AstrBot 插件
    ↓ 携带历史
    ↓ LLM 分析
    ↓ 工具调用
推送到用户
```

## 📝 更新日志

查看 [CHANGELOG.md](CHANGELOG.md) 了解版本历史。

**最新版本 v1.0.1 (2026-06-21)**：
- ✅ 修复 LLM 响应存储问题
- ✅ 修复 Agent 工具调用问题
- ✅ 新增测试和调试命令
- ✅ 增强日志输出

## 📄 许可证

[MIT License](LICENSE)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

**开发者**: 您的名字  
**文档版本**: v1.0.1  
**更新时间**: 2026-06-21
