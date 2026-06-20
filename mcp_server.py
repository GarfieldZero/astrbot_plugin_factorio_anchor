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
    ">":  lambda v, t: v > t,
    "<":  lambda v, t: v < t,
    ">=": lambda v, t: v >= t,
    "<=": lambda v, t: v <= t,
    "==": lambda v, t: v == t,
}

def _check_threshold(value: int, threshold: Dict[str, Any]) -> bool:
    """判断单条信号值是否触发阈值。

    threshold 格式示例:
      {"op": ">", "value": 100, "severity": "warning", "message": "库存过高"}
    op 支持: > < >= <= ==
    """
    op = threshold.get("op", ">")
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

    loop = asyncio.get_event_loop()

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
                if loop.is_running():
                    asyncio.ensure_future(
                        _push_alert(probe_name, probe_id, sig_label, sig_name, sig_value, rule)
                    )
                else:
                    loop.run_until_complete(
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
                    print(f"✅ 加载了 {len(current_signals)} 个探针信号")
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
    
    # ============ MCP 工具 - 获取信号 ============
    @mcp.tool()
    def get_current_signals() -> str:
        """获取当前所有探针的信号数据
        
        返回所有探针的红/绿信号信息
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
        """获取所有探针的配置信息
        
        返回探针的名称、描述、阈值配置等
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
        """向 Factorio 游戏发送控制指令
        
        Args:
            unit_number: 目标探针的 unit number
            signals: 要发送的信号列表，每个信号包含 type, name, count
        
        Returns:
            发送结果信息
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
        """获取历史数据总览
        
        Returns:
            历史数据统计信息
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
        """获取历史数据记录
        
        Args:
            unit_number: 探针ID (可选)
            signal_name: 信号名称 (可选)
            limit: 返回数量限制，默认 100
            offset: 偏移量，默认 0
        
        Returns:
            历史记录列表
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
        """更新探针配置
        
        Args:
            probe_id: 探针ID
            config_updates: 要更新的配置字段，可以包含 name, description, unit_number, thresholds
        
        Returns:
            更新结果信息
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
        """获取系统状态信息
        
        Returns:
            系统健康状态和统计信息
        """
        return f"""
系统状态: 运行中
最后更新时间: {datetime.fromtimestamp(last_update_time).isoformat() if last_update_time else '无'}
当前连接探针数: {len(current_signals)}
配置探针数: {len(current_config)}
"""
    
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