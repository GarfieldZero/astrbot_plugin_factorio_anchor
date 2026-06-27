import os
import json
import time
import sqlite3
import asyncio
import argparse
import struct
from datetime import datetime
from typing import Dict, List, Any, Optional
import aiohttp
from fastmcp import FastMCP, Context
from mcp import types
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ============ 配置 ============
WATCH_DIR = os.path.expandvars(r"/home/factorio/factorio/script-output")
TARGET_FILE = "ai_signals.json"
CONFIG_FILE = "ai_config.json"
DB_FILE = "factorio_signals.db"
SAVE_INTERVAL = 30

# ============ 全局状态 ============
current_signals: List[Dict[str, Any]] = []
current_config: Dict[str, Any] = {}
last_update_time: float = 0
observer: Observer = None

# ============ 告警推送状态 ============
# key: "{probe_id}:{signal_type}:{signal_name}" -> 上次推送时间戳
_alert_last_sent: Dict[str, float] = {}
_alert_url: str = ""          # AstrBot 插件的 /alert 端点
_alert_cooldown: float = 1800  # 默认30分钟冷却

# ============ 数据库初始化 ============
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS signal_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp REAL NOT NULL,
            unit_number INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            signal_name TEXT NOT NULL,
            value INTEGER NOT NULL
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ts_unit ON signal_history (timestamp, unit_number)')
    conn.commit()
    conn.close()
    print("✅ 数据库初始化完成")

# ============ 配置管理 ============
def load_config():
    global current_config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                current_config = json.load(f)
            print(f"✅ 加载了 {len(current_config)} 个探针配置")
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            current_config = {}
    else:
        current_config = {}

def save_config():
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(current_config, f, ensure_ascii=False, indent=2)

# ============ 阈值检查与故障推送 ============
_OPS = {
    ">":   lambda v, t: v > t,
    "<":   lambda v, t: v < t,
    ">=":  lambda v, t: v >= t,
    "<=":  lambda v, t: v <= t,
    "==":  lambda v, t: v == t,
    "gt":  lambda v, t: v > t,
    "lt":  lambda v, t: v < t,
    "gte": lambda v, t: v >= t,
    "lte": lambda v, t: v <= t,
    "eq":  lambda v, t: v == t,
}

def _resolve_op(threshold: Dict[str, Any]) -> str:
    """从 threshold 中读取运算符，兼容 'op' 和 'operator' 两种字段名。"""
    return threshold.get("op") or threshold.get("operator", ">")

def _check_threshold(value: int, threshold: Dict[str, Any]) -> bool:
    op = _resolve_op(threshold)
    limit = threshold.get("value")
    if limit is None:
        return False
    fn = _OPS.get(op)
    if fn is None:
        return False
    try:
        return fn(int(value), int(limit))
    except (TypeError, ValueError):
        return False

def _is_cooldown_ok(alert_key: str) -> bool:
    """返回 True 表示冷却已过，可以推送。"""
    last = _alert_last_sent.get(alert_key, 0.0)
    return (time.time() - last) >= _alert_cooldown

def _mark_sent(alert_key: str) -> None:
    _alert_last_sent[alert_key] = time.time()

async def _push_alert(probe_name: str, probe_id: str, signal_type: str,
                      signal_name: str, value: int, threshold: Dict[str, Any]) -> None:
    """向 AstrBot 插件的 /alert 端点发送 HTTP POST 告警。"""
    if not _alert_url:
        return

    severity = threshold.get("severity", "warning")
    custom_msg = threshold.get("message", "")
    op = threshold.get("op", ">")
    limit = threshold.get("value", "?")

    payload = {
        "alert_name": f"{probe_name} - {signal_name} 阈值触发",
        "severity": severity,
        "source": f"探针 {probe_name} (unit={probe_id}, 信号回路={signal_type})",
        "timestamp": datetime.now().isoformat(),
        "message": (
            f"信号 [{signal_name}] 当前值 {value} {op} 阈值 {limit}。"
            + (f" {custom_msg}" if custom_msg else "")
        ),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(_alert_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                status = resp.status
                print(f"📤 告警推送 [{signal_name}={value}] -> {_alert_url}  HTTP {status}")
    except Exception as e:
        print(f"❌ 告警推送失败: {e}")

def check_thresholds_and_push(signals: List[Dict[str, Any]]) -> None:
    """对所有探针的信号值做阈值检查，触发时异步推送告警（带冷却）。"""
    if not current_config:
        return

    # 建立 unit_number -> (probe_id, probe_cfg) 的快速索引
    unit_map: Dict[int, tuple] = {}
    for pid, pcfg in current_config.items():
        un = pcfg.get("unit_number")
        if un is not None:
            unit_map[int(un)] = (pid, pcfg)

    for node in signals:
        unit = node.get("unit_number")
        if unit is None:
            continue
        if unit not in unit_map:
            continue

        probe_id, probe_cfg = unit_map[unit]
        probe_name = probe_cfg.get("name", str(probe_id))
        thresholds: Dict[str, Any] = probe_cfg.get("thresholds", {})
        if not thresholds:
            continue

        for sig_type in ("red_signals", "green_signals"):
            sig_label = "red" if sig_type == "red_signals" else "green"
            for sig_name, sig_value in node.get(sig_type, {}).items():
                # 阈值 key 支持两种形式:
                #   1. 直接用信号名:  "iron-plate"
                #   2. 带回路前缀:    "red:iron-plate" / "green:energy"
                rule = thresholds.get(f"{sig_label}:{sig_name}") or thresholds.get(sig_name)
                if rule is None:
                    continue

                if not _check_threshold(sig_value, rule):
                    continue

                alert_key = f"{probe_id}:{sig_label}:{sig_name}"
                if not _is_cooldown_ok(alert_key):
                    remaining = int(_alert_cooldown - (time.time() - _alert_last_sent.get(alert_key, 0)))
                    print(f"⏳ 冷却中，跳过推送 [{probe_name}/{sig_name}={sig_value}]，剩余 {remaining}s")
                    continue

                _mark_sent(alert_key)
                print(f"🚨 阈值触发: {probe_name} / {sig_name} = {sig_value}，准备推送告警")
                asyncio.run(
                    _push_alert(probe_name, probe_id, sig_label, sig_name, sig_value, rule)
                )

# ============ 历史数据保存 ============
def save_history_data():
    if not current_signals:
        return
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        current_time = time.time()
        for node in current_signals:
            for name, value in node.get('red_signals', {}).items():
                cursor.execute('''
                    INSERT INTO signal_history (timestamp, unit_number, signal_type, signal_name, value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (current_time, node['unit_number'], 'red', name, value))
            for name, value in node.get('green_signals', {}).items():
                cursor.execute('''
                    INSERT INTO signal_history (timestamp, unit_number, signal_type, signal_name, value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (current_time, node['unit_number'], 'green', name, value))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"保存历史数据错误: {e}")

# ============ 文件监控 ============
class FactorioSignalHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory and os.path.basename(event.src_path) == TARGET_FILE:
            self.process_signal(event.src_path)
    
    def process_signal(self, file_path):
        global current_signals, last_update_time
        time.sleep(0.05)
        for attempt in range(3):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if not content.strip():
                        return
                    current_signals = json.loads(content)
                    last_update_time = time.time()
                    # print(f"✅ 加载了 {len(current_signals)} 个探针信号")
                    check_thresholds_and_push(current_signals)
                break
            except Exception as e:
                if attempt == 2:
                    print(f"读取文件错误: {e}")
                time.sleep(0.02)

def initial_file_load():
    global current_signals
    file_path = os.path.join(WATCH_DIR, TARGET_FILE)
    loaded = False
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    current_signals = json.loads(content)
                    print(f"✅ 初始加载了 {len(current_signals)} 个探针")
                    loaded = True
        except Exception as e:
            print(f"初始加载文件失败: {e}")
    
    if not loaded:
        print("⚠️ 使用测试数据")
        current_signals = [
            {"unit_number": 39, "red_signals": {"iron-plate": 500, "I": 101}, "green_signals": {}},
            {"unit_number": 677, "red_signals": {"copper-plate": 200}, "green_signals": {"energy": 100}},
            {"unit_number": 679, "red_signals": {}, "green_signals": {"steel": 50}}
        ]

def start_file_watchdog():
    global observer
    if not os.path.exists(WATCH_DIR):
        print(f"⚠️ 警告: 找不到监控目录: {WATCH_DIR}")
        return
    
    try:
        event_handler = FactorioSignalHandler()
        observer = Observer()
        observer.schedule(event_handler, path=WATCH_DIR, recursive=False)
        observer.start()
        print("🚀 文件监控已启动")
    except Exception as e:
        print(f"❌ 启动文件监控失败: {e}")

# ============ 主函数 ============
def main():
    parser = argparse.ArgumentParser(description='Factorio AI Gateway MCP Server')
    parser.add_argument('--host', type=str, default='0.0.0.0',
                        help='SSE 服务监听地址 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8081,
                        help='SSE 服务监听端口 (默认: 8081)')
    parser.add_argument('--alert-url', type=str, default='',
                        help='AstrBot 插件告警端点，如 http://127.0.0.1:8765/alert')
    parser.add_argument('--alert-cooldown', type=float, default=1800,
                        help='同一告警最小推送间隔（秒），默认 1800（30分钟）')

    args = parser.parse_args()

    global _alert_url, _alert_cooldown
    _alert_url = args.alert_url
    _alert_cooldown = args.alert_cooldown

    print("🚀 启动 Factorio AI Gateway MCP 服务器...")
    print(f"🌐 SSE 服务将运行在: http://{args.host}:{args.port}")
    print(f"🔗 SSE 端点: http://{args.host}:{args.port}/sse")
    if _alert_url:
        print(f"📤 告警推送端点: {_alert_url}  冷却: {int(_alert_cooldown)}s")
    else:
        print("⚠️  未配置告警推送端点（--alert-url），阈值触发时将只打印日志")
    
    # 初始化
    init_db()
    load_config()
    initial_file_load()
    
    # 启动文件监控
    start_file_watchdog()
    
    # 现在创建 MCP 服务器 - 因为需要 host 和 port 参数
    mcp = FastMCP("Factorio AI Gateway")
    
    # ============ MCP 工具 ============
    @mcp.tool()
    def get_current_signals() -> str:
        """获取 Factorio 游戏中所有电路网络探针的实时信号快照。

        返回每个探针的红色回路信号和绿色回路信号，信号值为整数（Factorio 原生单位）。
        信号名称使用 Factorio 内部 item/fluid/signal 名称，例如 "iron-plate"、"copper-plate"、"electric-power"。

        典型工作流：
          1. 先调用本工具获取当前所有信号值；
          2. 再调用 get_probe_config 确认对应探针的阈值配置；
          3. 如需干预，调用 send_command 向目标探针下发控制信号。

        返回格式（每个探针一段，以 --- 分隔）：
          探针: <名称> (ID: <unit_number>)
          红色信号: {"iron-plate": 500, ...}
          绿色信号: {"energy": 100, ...}
        """
        if not current_signals:
            return "当前没有信号数据"
        
        result = []
        for node in current_signals:
            probe_id = str(node.get('unit_number', 'unknown'))
            probe_name = probe_id
            
            # 查找探针名称
            for k, v in current_config.items():
                if v.get('unit_number') == node.get('unit_number'):
                    probe_name = v.get('name', probe_id)
                    break
            
            result.append(f"""
探针: {probe_name} (ID: {node.get('unit_number')})
红色信号: {json.dumps(node.get('red_signals', {}), ensure_ascii=False)}
绿色信号: {json.dumps(node.get('green_signals', {}), ensure_ascii=False)}
""")
        
        return "\n---\n".join(result)

    @mcp.tool()
    def get_probe_config() -> str:
        """获取所有探针的配置信息，包括阈值规则。

        返回每个探针的名称、描述、对应的游戏 unit_number，以及阈值告警规则。

        阈值规则格式说明：
          键名支持两种形式：
            - 纯信号名：  "iron-plate"          （匹配红/绿任意回路）
            - 带前缀：    "red:iron-plate"       （仅匹配红色回路）
                         "green:energy"          （仅匹配绿色回路）
          值为规则对象：
            {
              "op":       ">",           // 比较运算符，支持 > < >= <= ==
              "value":    5000,          // 阈值（整数）
              "severity": "warning",     // 告警等级，建议 info / warning / critical
              "message":  "铁板库存过高"  // 可选，附加说明
            }

        如需修改阈值，请调用 update_probe_config。
        """
        if not current_config:
            return "当前没有配置数据"
        
        result = []
        for probe_id, config in current_config.items():
            result.append(f"""
探针ID: {probe_id}
名称: {config.get('name', '')}
描述: {config.get('description', '')}
Unit Number: {config.get('unit_number', '')}
阈值配置: {json.dumps(config.get('thresholds', {}), ensure_ascii=False, indent=2)}
""")
        
        return "\n---\n".join(result)
        
    @mcp.tool()
    async def test_sampling(ctx: Context) -> str:
        """测试 AstrBot 是否支持 MCP Sampling (反向请求大模型生成回复) 机制"""
        
        print("➡️ 收到工具调用，正在向 AstrBot 发起反向 Sampling 请求...")
        
        try:
            # 1. 构造一条想让大模型去思考/生成的测试消息
            message = types.SamplingMessage(
                role="user",
                content=types.TextContent(
                    type="text", 
                    text="这是一条来自异星工场后端的反向采样测试。请你明确回复：'我已成功收到采样请求！'"
                )
            )
            
            # 2. 调用 session 发起 sampling/createMessage 请求
            response = await ctx.session.create_message(
                messages=[message],
                max_tokens=100,
                system_prompt="你是一个异星工场的系统监控底层 AI。"
            )
            
            # 3. 如果没报错，说明客户端完美支持！
            print(f"✅ Sampling 成功！客户端大模型生成了回复: {response}")
            return f"🎉 测试大成功！AstrBot 的 MCP 客户端支持 Sampling 功能。刚才模型内部采样的回复是：{response}"
            
        except Exception as e:
            # 如果抛出 MethodNotFound 等异常，说明 AstrBot 还没实现这个功能
            print(f"❌ Sampling 测试失败: {e}")
            return f"⚠️ 测试失败。AstrBot 当前版本的 MCP 客户端大概率还不支持 Sampling 特性。报错详情: {str(e)}"
    
    @mcp.tool()
    async def send_command(unit_number: int, signals: List[Dict[str, Any]]) -> str:
        """通过 RCON 向 Factorio 游戏内的指定探针写入控制信号。

        这是唯一能直接影响游戏运行状态的工具，调用前请确认：
          1. 目标探针的 unit_number（可从 get_current_signals 或 get_probe_config 获取）；
          2. 信号名称使用 Factorio 内部名称（如 "iron-plate"、"signal-red"）；
          3. count 为整数，负值也合法（Factorio 电路信号支持负数）。

        Args:
            unit_number: 目标探针的游戏内唯一编号（unit_number），不是配置文件中的 probe_id 字符串。
            signals: 要写入的信号列表，每项格式为：
                       {"type": "item"|"fluid"|"virtual", "name": "<信号名>", "count": <整数>}
                     示例：
                       [{"type": "item", "name": "iron-plate", "count": -1000}]
                       [{"type": "virtual", "name": "signal-red", "count": 1}]

        Returns:
            RCON 执行结果文本；若连接失败或认证错误会返回具体错误信息。
        """
        try:
            json_str = json.dumps(signals)
            lua_command = f'/silent-command remote.call("ai_bridge", "set_signals", {unit_number}, \'{json_str}\')'
            
            print(f"📡 正在发送 RCON 命令: {lua_command}")
            
            # 使用我们自己写的原生异步 RCON 客户端
            response = await async_rcon_command("127.0.0.1", 27015, "aibrain", lua_command)
            
            print(f"✅ RCON 返回: {repr(response)}")
            return f"指令发送成功！响应: {response}"
            
        except Exception as e:
            print(f"❌ RCON 错误: {e}")
            return f"指令发送失败: {str(e)}"

    @mcp.tool()
    def get_history_summary() -> str:
        """查询本地 SQLite 数据库中的历史信号记录总条数。

        适合在分析趋势前先了解数据量规模，再决定查询参数（limit/offset）。
        若总数为 0，说明数据库尚未收到任何探针数据，请检查文件监控是否正常运行。
        """
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM signal_history")
            total_count = cursor.fetchone()[0]
            conn.close()
            
            return f"历史记录总数: {total_count}"
        except Exception as e:
            print(f"历史概览错误: {e}")
            return "获取历史数据失败"

    @mcp.tool()
    def get_history_records(
        unit_number: Optional[int] = None,
        signal_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> str:
        """从本地数据库查询历史信号记录，支持按探针和信号名过滤。

        历史记录每隔约 30 秒自动写入一次，可用于分析信号趋势、判断告警是否持续。

        Args:
            unit_number: 按探针的游戏 unit_number 过滤（可选）。
                         不填则返回所有探针的记录。
            signal_name: 按信号名精确匹配过滤（可选），如 "iron-plate"。
                         不填则返回所有信号。
            limit:  每次最多返回条数，默认 100，建议不超过 500 避免响应过大。
            offset: 分页偏移量，默认 0（最新记录在前）。

        返回 JSON 数组，每条记录结构：
          {
            "id":          <自增主键>,
            "timestamp":   <Unix 时间戳（秒）>,
            "datetime":    "<ISO 8601 时间字符串>",
            "unit_number": <探针 unit_number>,
            "signal_type": "red" | "green",
            "signal_name": "<信号名>",
            "value":       <整数值>
          }
        """
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            
            query = "SELECT id, timestamp, unit_number, signal_type, signal_name, value FROM signal_history WHERE 1=1"
            params = []
            
            if unit_number is not None:
                query += " AND unit_number = ?"
                params.append(unit_number)
            
            if signal_name is not None:
                query += " AND signal_name = ?"
                params.append(signal_name)
            
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                result.append({
                    "id": row[0],
                    "timestamp": row[1],
                    "datetime": datetime.fromtimestamp(row[1]).isoformat() if row[1] else None,
                    "unit_number": row[2],
                    "signal_type": row[3],
                    "signal_name": row[4],
                    "value": row[5]
                })
            
            conn.close()
            
            if not result:
                return "没有找到匹配的历史记录"
            
            return json.dumps(result, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"历史记录查询错误: {e}")
            return f"查询失败: {str(e)}"

    @mcp.tool()
    def update_probe_config(probe_id: str, config_updates: Dict[str, Any]) -> str:
        """新建或更新一个探针的配置，并立即持久化到 ai_config.json。

        若 probe_id 不存在，会自动创建一条空白配置后再写入。

        Args:
            probe_id: 配置文件中的探针标识符（字符串 key），自定义命名，如 "furnace_01"。
                      与游戏内 unit_number 不同——unit_number 是游戏分配的数字，
                      probe_id 是你在配置文件里给这个探针起的别名。
            config_updates: 要更新的字段，支持以下 key（只需传需要修改的字段）：
                "name"        (str)  — 探针显示名称，用于告警推送时的可读标识
                "description" (str)  — 备注说明
                "unit_number" (int)  — 与游戏内探针绑定的 unit_number
                "thresholds"  (dict) — 阈值规则字典，格式见 get_probe_config 的说明。
                                       传入会整体替换旧的 thresholds，请带上全部规则。

        示例——为 furnace_01 设置铁板库存上限告警：
          probe_id = "furnace_01"
          config_updates = {
            "name": "熔炉区探针",
            "unit_number": 39,
            "thresholds": {
              "iron-plate": {"op": ">", "value": 5000, "severity": "warning", "message": "铁板堆积，请检查下游"},
              "red:signal-red": {"op": "==", "value": 1, "severity": "critical", "message": "紧急停机信号"}
            }
          }

        Returns:
            操作结果文本；配置写入失败时返回错误详情。
        """
        try:
            if probe_id not in current_config:
                current_config[probe_id] = {
                    "name": "",
                    "description": "",
                    "unit_number": 0,
                    "thresholds": {}
                }
            
            # 更新配置
            for key, value in config_updates.items():
                if key in current_config[probe_id]:
                    current_config[probe_id][key] = value
            
            save_config()
            return f"成功更新探针 {probe_id} 的配置"
        except Exception as e:
            print(f"更新配置错误: {e}")
            return f"更新配置失败: {str(e)}"

    @mcp.tool()
    def get_system_status() -> str:
        """获取 MCP 网关服务的运行状态概览。

        返回以下信息：
          - 服务运行状态（始终为"运行中"，因为能收到工具调用说明服务正常）
          - 最后一次成功读取信号文件的时间（若长时间未更新，说明文件监控或游戏端可能异常）
          - 当前内存中持有的探针信号数（对应游戏中正在上报数据的探针数量）
          - 已配置阈值规则的探针数（已在 ai_config.json 中登记的探针数量）

        建议在处理告警前先调用本工具，确认数据是最新的。
        若"最后更新时间"距今超过 5 分钟，优先排查游戏端脚本或文件监控是否正常。
        """
        return f"""
系统状态: 运行中
最后更新时间: {datetime.fromtimestamp(last_update_time).isoformat() if last_update_time else '无'}
当前连接探针数: {len(current_signals)}
配置探针数: {len(current_config)}
"""

    @mcp.tool()
    def delete_probe_config(probe_id: str) -> str:
        """删除指定探针的配置，并立即持久化到 ai_config.json。

        此工具用于清理不再使用的探针配置，保持配置文件整洁。
        删除后，该探针将不再进行阈值检查和告警推送（即使游戏中仍在上报数据）。

        注意：
          - 删除操作不可逆，但不会影响历史数据库中已保存的记录
          - 若探针在游戏中仍然存在并上报信号，可通过 get_current_signals 查看实时数据
          - 若需恢复配置，需手动调用 update_probe_config 重新创建

        Args:
            probe_id: 要删除的探针配置 ID（配置文件中的 key，如 "furnace_01"）

        Returns:
            操作结果文本；若 probe_id 不存在则返回提示信息。
        """
        try:
            if probe_id not in current_config:
                return f"探针 {probe_id} 不存在，无需删除"

            # 删除配置
            probe_name = current_config[probe_id].get("name", probe_id)
            del current_config[probe_id]

            # 持久化到文件
            save_config()

            return f"成功删除探针配置: {probe_id} ({probe_name})"
        except Exception as e:
            print(f"删除配置错误: {e}")
            return f"删除配置失败: {str(e)}"
    
    @mcp.tool()
    def list_known_signals(from_db: bool = True) -> str:
        """列出所有已知的信号名称及其所属探针，用于发现当前有哪些可监控的信号。

        Args:
            from_db: True（默认）从历史数据库查询，覆盖更全；False 仅从当前内存信号查。

        返回每行一个信号：信号名、回路颜色、所属探针名称/unit_number。
        """
        try:
            def probe_name(unit):
                for v in current_config.values():
                    if v.get('unit_number') == unit:
                        return f"{v.get('name', unit)} (unit={unit})"
                return f"unit={unit}"

            if from_db:
                conn = sqlite3.connect(DB_FILE)
                rows = conn.execute(
                    "SELECT DISTINCT signal_name, signal_type, unit_number FROM signal_history ORDER BY unit_number, signal_type, signal_name"
                ).fetchall()
                conn.close()
                if not rows:
                    return "数据库中尚无信号记录"
                return "\n".join(f"{r[0]}  [{r[1]}回路]  探针: {probe_name(r[2])}" for r in rows)
            else:
                lines = []
                for node in current_signals:
                    unit = node.get('unit_number')
                    pn = probe_name(unit)
                    for sig in node.get('red_signals', {}):
                        lines.append(f"{sig}  [red回路]  探针: {pn}")
                    for sig in node.get('green_signals', {}):
                        lines.append(f"{sig}  [green回路]  探针: {pn}")
                return "\n".join(lines) if lines else "当前没有信号数据"
        except Exception as e:
            return f"查询失败: {str(e)}"

    @mcp.tool()
    def get_signal_value(signal_name: str, unit_number: Optional[int] = None) -> str:
        """查询指定信号的当前实时值，比 get_current_signals 更精确。

        Args:
            signal_name: 信号名称，如 "iron-plate"、"signal-red"。
            unit_number: 可选，限定在某个探针内查找；不填则搜索所有探针。

        返回匹配到的所有探针上该信号的当前值。
        """
        results = []
        for node in current_signals:
            unit = node.get('unit_number')
            if unit_number is not None and unit != unit_number:
                continue
            probe_name = str(unit)
            for v in current_config.values():
                if v.get('unit_number') == unit:
                    probe_name = v.get('name', str(unit))
                    break
            for circuit, label in (('red_signals', 'red'), ('green_signals', 'green')):
                val = node.get(circuit, {}).get(signal_name)
                if val is not None:
                    results.append(f"探针: {probe_name} (unit={unit})  [{label}回路]  {signal_name} = {val}")
        return "\n".join(results) if results else f"未找到信号: {signal_name}"

    @mcp.tool()
    def search_probe_config(name_keyword: str) -> str:
        """按名称关键字模糊搜索探针配置（不区分大小写）。

        当 agent 知道探针的名称关键字但不知道 probe_id 时使用，
        比逐条翻阅 get_probe_config 更高效。

        Args:
            name_keyword: 名称关键字，如 "熔炉"、"furnace"。
        """
        kw = name_keyword.lower()
        matches = {k: v for k, v in current_config.items()
                   if kw in v.get('name', '').lower() or kw in k.lower()}
        if not matches:
            return f"未找到名称包含 '{name_keyword}' 的探针"
        result = []
        for pid, cfg in matches.items():
            result.append(
                f"探针ID: {pid}\n名称: {cfg.get('name','')}\n描述: {cfg.get('description','')}\n"
                f"Unit Number: {cfg.get('unit_number','')}\n"
                f"阈值配置: {json.dumps(cfg.get('thresholds', {}), ensure_ascii=False)}"
            )
        return "\n---\n".join(result)

    @mcp.tool()
    def get_history_stats(
        signal_name: str,
        unit_number: Optional[int] = None,
        hours: float = 1.0
    ) -> str:
        """查询指定信号在最近 N 小时内的统计数据（最小/最大/平均/最新值）。

        适合快速判断趋势，无需拉取大量原始记录。

        Args:
            signal_name: 信号名称，如 "iron-plate"。
            unit_number: 可选，限定探针；不填则统计所有探针上该信号的合并结果。
            hours:       时间窗口（小时），默认 1.0。
        """
        try:
            since = time.time() - hours * 3600
            conn = sqlite3.connect(DB_FILE)
            if unit_number is not None:
                row = conn.execute(
                    "SELECT MIN(value),MAX(value),AVG(value),COUNT(*) FROM signal_history WHERE signal_name=? AND unit_number=? AND timestamp>=?",
                    (signal_name, unit_number, since)
                ).fetchone()
                latest = conn.execute(
                    "SELECT value FROM signal_history WHERE signal_name=? AND unit_number=? ORDER BY timestamp DESC LIMIT 1",
                    (signal_name, unit_number)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT MIN(value),MAX(value),AVG(value),COUNT(*) FROM signal_history WHERE signal_name=? AND timestamp>=?",
                    (signal_name, since)
                ).fetchone()
                latest = conn.execute(
                    "SELECT value FROM signal_history WHERE signal_name=? ORDER BY timestamp DESC LIMIT 1",
                    (signal_name,)
                ).fetchone()
            conn.close()
            if not row or row[3] == 0:
                return f"最近 {hours} 小时内没有 '{signal_name}' 的记录"
            probe_info = f"  探针 unit={unit_number}" if unit_number else ""
            return (f"信号: {signal_name}{probe_info}\n时间窗口: 最近 {hours} 小时\n"
                    f"最小值: {row[0]}\n最大值: {row[1]}\n平均值: {row[2]:.1f}\n"
                    f"记录数: {row[3]}\n最新值: {latest[0] if latest else 'N/A'}")
        except Exception as e:
            return f"查询失败: {str(e)}"

    @mcp.tool()
    def delete_history_records(
        unit_number: Optional[int] = None,
        signal_name: Optional[str] = None,
        before_days: Optional[float] = None
    ) -> str:
        """从历史数据库删除记录，至少需要提供一个过滤条件。

        Args:
            unit_number:  按探针 unit_number 过滤（可选）。
            signal_name:  按信号名精确过滤（可选）。
            before_days:  删除 N 天前的记录（可选），如 7.0 表示清理 7 天前的数据。

        Returns:
            删除的记录条数。
        """
        if unit_number is None and signal_name is None and before_days is None:
            return "错误：至少需要提供一个过滤条件（unit_number / signal_name / before_days）"
        try:
            conn = sqlite3.connect(DB_FILE)
            query = "DELETE FROM signal_history WHERE 1=1"
            params = []
            if unit_number is not None:
                query += " AND unit_number=?"
                params.append(unit_number)
            if signal_name is not None:
                query += " AND signal_name=?"
                params.append(signal_name)
            if before_days is not None:
                query += " AND timestamp<?"
                params.append(time.time() - before_days * 86400)
            conn.execute(query, params)
            deleted = conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()
            conn.close()
            return f"已删除 {deleted} 条历史记录"
        except Exception as e:
            return f"删除失败: {str(e)}"

    print("✅ MCP服务器已准备就绪")
    
    # 使用官方原生支持的 SSE 模式启动服务器
    mcp.run(transport="sse", host=args.host, port=args.port)


# ============ 异步 RCON 客户端 ============
async def async_rcon_command(host: str, port: int, password: str, command: str) -> str:
    """原生 asyncio 实现的 RCON 客户端，彻底避开多线程与信号冲突"""
    reader, writer = await asyncio.open_connection(host, port)
    
    try:
        # 内部发包辅助函数
        async def send_packet(req_id: int, pkt_type: int, body: str):
            payload = body.encode('utf-8')
            # 协议格式: ReqID(4字节) + Type(4字节) + Payload + Null(2字节)
            packet = struct.pack('<ii', req_id, pkt_type) + payload + b'\x00\x00'
            writer.write(struct.pack('<i', len(packet)) + packet)
            await writer.drain()

        # 内部收包辅助函数
        async def read_packet():
            length_bytes = await reader.readexactly(4)
            length = struct.unpack('<i', length_bytes)[0]
            data = await reader.readexactly(length)
            req_id, pkt_type = struct.unpack('<ii', data[:8])
            payload = data[8:-2].decode('utf-8')
            return req_id, pkt_type, payload

        # 1. 发送 RCON 认证 (Type 3)
        await send_packet(1, 3, password)
        auth_id, _, _ = await read_packet()
        if auth_id == -1:
            raise Exception("RCON 认证失败，请检查密码！")

        # 2. 发送执行命令 (Type 2)
        await send_packet(2, 2, command)
        _, _, response = await read_packet()
        
        return response
        
    finally:
        writer.close()
        await writer.wait_closed()

if __name__ == "__main__":
    main()