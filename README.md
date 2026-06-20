# Factorio 告警锚 插件

Factorio 告警锚插件用于接收游戏服务器推送的告警，并把告警内容传递给 AstrBot 会话对应的 LLM，由模型判断并给出处理建议，最终通过消息平台主动推送至订阅用户。

## 功能说明

- 启动时自动创建 HTTP 接口 `POST /alert`，默认监听端口：`8765`
- 支持客户端通过会话命令 `/alarm_subscribe` 绑定当前会话为告警接收目标
- 支持 `/alarm_unsubscribe` 取消绑定
- 支持 `/alarm_subscribe_status` 查询当前绑定状态
- 告警会携带订阅会话的 `unified_msg_origin` 触发 AstrBot LLM 提供商，从而命中会话上下文并减少无关开销

## 快速上手

1. 将插件放入 AstrBot 插件目录
2. 在目标消息会话中发送：`/alarm_subscribe`
3. 在游戏服务器中向插件所在主机发送告警请求：

```bash
curl -X POST http://<host>:8765/alert \
  -H "Content-Type: application/json" \
  -d '{"alert_name":"Factorio 服务器异常","severity":"critical","message":"游戏服务器异常退出","source":"factorio-01","timestamp":"2026-06-20T12:34:56Z"}'
```

4. 目标会话会收到包含告警信息与 LLM 建议的主动消息

## 接口说明

### `POST /alert`

JSON 请求体示例：

```json
{
  "alert_name": "游戏服务器崩溃",
  "severity": "critical",
  "message": "Factorio 服务器出现致命错误，正在重启",
  "source": "factorio-server-01",
  "timestamp": "2026-06-20T12:34:56Z"
}
```

如果当前插件未绑定会话，则返回错误。

## 插件配置

可在 AstrBot 插件配置中设置：

```yaml
alarm_port: 8765
```
可选配置项：

```yaml
alarm_port: 8765
history_mode: all         # all / recent / relevant
history_count: 0         # 限制历史条数，0 表示不限制
record_alert_history: true
```

配置说明：

- `alarm_port`：告警接收 HTTP 服务监听端口。
- `history_mode`：历史上下文选择策略，
  - `all`：全部历史；
  - `recent`：只取最近若干条；
  - `relevant`：优先取与当前告警关键词相关的消息。
- `history_count`：在 `recent` 或 `relevant` 模式下限制历史条数；0 表示不限制。
- `record_alert_history`：是否将本次告警内容写入当前会话历史，以便后续对话能够引用。

## 依赖

- aiohttp

## 安装依赖

```bash
pip install -r requirements.txt
```
