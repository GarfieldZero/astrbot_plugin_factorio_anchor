import asyncio
import json
import re
from typing import Any, Dict, List, Optional

import aiohttp.web as web

from astrbot.api.event import MessageChain, AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.core.platform.astr_message_event import AstrMessageEvent as _AstrMessageEvent
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.platform.message_type import MessageType

@register(
    "astrbot_plugin_factorio_anchor",
    "Factorio 告警锚",
    "通过会话订阅接收游戏告警，并由 AstrBot LLM 决策后主动推送",
    "1.0.0",
)
class FactorioAlarmAnchorPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self._subscribed_umo: Optional[str] = None
        self._server_runner: Optional[web.AppRunner] = None
        self._server_task: Optional[asyncio.Task[Any]] = None

        self._alarm_port: int = int(config.get("alarm_port", 8765))
        history_mode = str(config.get("history_mode", "all")).lower()
        self._history_mode: str = history_mode if history_mode in {"all", "recent", "relevant"} else "all"
        self._history_count: int = int(config.get("history_count", 0))
        self._record_alert_history: bool = bool(config.get("record_alert_history", True))
        self._enable_agent_mode: bool = bool(config.get("enable_agent_mode", False))

    async def initialize(self):
        """初始化时启动告警接收服务。"""
        self._server_task = asyncio.create_task(self._start_alert_server())

    async def terminate(self):
        """插件被卸载或停用时关闭告警服务。"""
        if self._server_runner:
            try:
                await self._server_runner.cleanup()
                logger.info("FactorioAlarmAnchor: 告警服务已关闭")
            except Exception as exc:
                logger.exception(f"FactorioAlarmAnchor: 关闭告警服务失败：{exc}")
        if self._server_task and not self._server_task.done():
            self._server_task.cancel()

    async def _start_alert_server(self) -> None:
        app = web.Application()
        app.router.add_post("/alert", self._handle_alert)
        app.router.add_get("/health", self._handle_health)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._alarm_port)
        await site.start()

        self._server_runner = runner
        logger.info(f"FactorioAlarmAnchor: 告警服务已启动，监听 0.0.0.0:{self._alarm_port}")

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "status": "running"})

    async def _handle_alert(self, request: web.Request) -> web.Response:
        logger.info(f"FactorioAlarmAnchor: 收到告警请求，来源 IP: {request.remote}")

        if not self._subscribed_umo:
            logger.warning("FactorioAlarmAnchor: 告警被拒绝 - 当前未绑定会话")
            return web.json_response(
                {"ok": False, "error": "当前未绑定会话，请先在目标会话中发送 /alarm_subscribe"},
                status=400,
            )

        try:
            payload = await request.json()
            logger.debug(f"FactorioAlarmAnchor: 收到告警 payload: {payload}")
        except json.JSONDecodeError:
            logger.error("FactorioAlarmAnchor: 无效的 JSON 请求体")
            return web.json_response({"ok": False, "error": "无效的 JSON 请求体"}, status=400)
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: 解析请求体时出错：{exc}")
            return web.json_response({"ok": False, "error": str(exc)}, status=400)

        alert = self._normalize_alert(payload)
        if not alert:
            logger.warning("FactorioAlarmAnchor: 告警数据缺失或格式不正确")
            return web.json_response({"ok": False, "error": "告警数据缺失或格式不正确"}, status=400)

        logger.info(f"FactorioAlarmAnchor: 告警规范化完成 - {alert['alert_name']}")

        try:
            result_text = await self._process_alert(alert)
            logger.info(f"FactorioAlarmAnchor: 告警处理成功，返回结果长度: {len(result_text)} 字符")
            return web.json_response({"ok": True, "result": result_text})
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: 处理告警时出错：{exc}")
            return web.json_response({"ok": False, "error": f"处理告警时出错: {str(exc)}"}, status=500)

    def _normalize_alert(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None

        alert_name = payload.get("alert_name") or payload.get("name") or payload.get("title")
        severity = payload.get("severity") or payload.get("level") or "unknown"
        message = payload.get("message") or payload.get("content") or payload.get("text")
        source = payload.get("source") or payload.get("origin") or payload.get("instance")
        timestamp = payload.get("timestamp") or payload.get("time") or payload.get("date")

        if not alert_name or not message:
            return None

        return {
            "alert_name": str(alert_name),
            "severity": str(severity),
            "message": str(message),
            "source": str(source) if source is not None else "unknown",
            "timestamp": str(timestamp) if timestamp is not None else "unknown",
        }

    async def _process_alert(self, alert: Dict[str, str]) -> str:
        logger.info(f"FactorioAlarmAnchor: 开始处理告警 - {alert['alert_name']} (等级: {alert['severity']})")
        history_text = await self._get_session_history(self._subscribed_umo, alert)
        logger.debug(f"FactorioAlarmAnchor: 获取到会话历史，长度: {len(history_text)} 字符")

        llm_text = None

        if self._enable_agent_mode:
            logger.info("FactorioAlarmAnchor: 使用 Agent 模式处理告警")
            llm_text = await self._process_alert_agent(alert, history_text)
        else:
            logger.info("FactorioAlarmAnchor: 使用文本对话模式处理告警")
            llm_text = await self._process_alert_text_chat(alert, history_text)

        logger.info(f"FactorioAlarmAnchor: LLM 处理完成，响应长度: {len(llm_text) if llm_text else 0} 字符")

        message = self._build_push_message(alert, llm_text)
        await self._send_to_subscribed_session(message)

        if self._record_alert_history:
            logger.debug("FactorioAlarmAnchor: 记录告警和响应到会话历史")
            await self._append_alert_to_history(alert, llm_text)

        logger.info("FactorioAlarmAnchor: 告警处理完成")
        return message

    async def _process_alert_text_chat(self, alert: Dict[str, str], history_text: str) -> Optional[str]:
        provider = self.context.get_using_provider(umo=self._subscribed_umo)
        if not provider:
            logger.warning("FactorioAlarmAnchor: 获取当前会话 LLM 提供商失败，将直接发送告警摘要")
            return None
        try:
            prompt = self._build_llm_prompt(alert, history_text)
            logger.debug(f"FactorioAlarmAnchor: 构建 LLM 提示词，长度: {len(prompt)} 字符")

            response = await provider.text_chat(
                prompt=prompt,
                context=[],
                system_prompt="你是一个运维告警助手，收到游戏告警后要优先判断是否需要提醒用户以及建议下一步操作。请依据告警信息和当前会话历史给出简明结论和建议，返回适合直接发送给用户的中文内容。",
            )

            logger.debug(f"FactorioAlarmAnchor: LLM 响应对象类型: {type(response)}")

            llm_text = getattr(response, "completion_text", None)
            if not llm_text:
                llm_text = getattr(response, "text", None)
            if not llm_text and response is not None:
                llm_text = str(response)

            logger.info(f"FactorioAlarmAnchor: 文本对话模式 LLM 返回结果: {llm_text[:100] if llm_text else 'None'}...")
            return llm_text
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: 调用 LLM 失败：{exc}")
            return None

    async def _process_alert_agent(self, alert: Dict[str, str], history_text: str) -> Optional[str]:
        try:
            provider_id = await self.context.get_current_chat_provider_id(self._subscribed_umo)
            logger.info(f"FactorioAlarmAnchor: 获取到 provider_id: {provider_id}")
        except Exception as exc:
            logger.warning(f"FactorioAlarmAnchor: 获取 provider_id 失败，退回 text_chat：{exc}")
            return await self._process_alert_text_chat(alert, history_text)

        try:
            synthetic_event = self._make_synthetic_event(self._subscribed_umo)
            prompt = self._build_llm_prompt(alert, history_text)
            logger.debug(f"FactorioAlarmAnchor: 构建 Agent 提示词，长度: {len(prompt)} 字符")
            logger.info("FactorioAlarmAnchor: 开始调用 tool_loop_agent，max_steps=10，timeout=30")

            response = await self.context.tool_loop_agent(
                event=synthetic_event,
                chat_provider_id=provider_id,
                prompt=prompt,
                system_prompt="你是一个 Factorio 游戏服务器运维助手。收到告警后，先判断严重程度，再决定是否需要调用 MCP 工具向游戏服务器下发命令（如踢人、暂停、广播消息等）。最终给出简明中文结论和已执行的操作摘要。",
                max_steps=10,
                tool_call_timeout=30,
            )

            logger.debug(f"FactorioAlarmAnchor: tool_loop_agent 响应对象类型: {type(response)}")

            llm_text = getattr(response, "completion_text", None)
            if not llm_text and response is not None:
                llm_text = str(response)

            logger.info(f"FactorioAlarmAnchor: Agent 模式 LLM 返回结果: {llm_text[:100] if llm_text else 'None'}...")
            return llm_text
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: Agent 调用失败：{exc}")
            return None

    def _make_synthetic_event(self, unified_msg_origin: str) -> _AstrMessageEvent:
        # Parse platform_name:message_type:session_id from the stored umo string.
        parts = unified_msg_origin.split(":", 2)
        platform_name = parts[0] if len(parts) > 0 else "unknown"
        session_id = parts[2] if len(parts) > 2 else unified_msg_origin

        platform_meta = PlatformMetadata(
            name=platform_name,
            description="synthetic event for alarm agent",
            id=platform_name,
        )

        msg_obj = AstrBotMessage()
        msg_obj.type = MessageType.FRIEND_MESSAGE
        msg_obj.self_id = "alarm_anchor"
        msg_obj.session_id = session_id
        msg_obj.message_id = "alarm_anchor_synthetic"
        msg_obj.message = []
        msg_obj.message_str = ""
        msg_obj.raw_message = None
        sender = MessageMember(user_id=session_id, nickname="alarm_anchor")
        msg_obj.sender = sender

        class _SyntheticEvent(_AstrMessageEvent):
            def get_platform_name(self_):
                return platform_name
            def get_message_outline(self_) -> str:
                return ""
            def get_group_id(self_) -> str:
                return ""
            def get_sender_id(self_) -> str:
                return session_id
            def get_sender_name(self_) -> str:
                return "alarm_anchor"

        event = _SyntheticEvent(
            message_str="",
            message_obj=msg_obj,
            platform_meta=platform_meta,
            session_id=session_id,
        )
        # Override session to preserve the exact original umo string.
        event.unified_msg_origin = unified_msg_origin
        return event

    async def _get_session_history(self, unified_msg_origin: Optional[str], alert: Dict[str, str]) -> str:
        if not unified_msg_origin:
            return ""

        try:
            conv_mgr = self.context.conversation_manager
            curr_cid = await conv_mgr.get_curr_conversation_id(unified_msg_origin)
            if curr_cid and hasattr(conv_mgr, "get_human_readable_context"):
                try:
                    pages, _ = await conv_mgr.get_human_readable_context(unified_msg_origin, curr_cid)
                    if pages:
                        lines = [line for line in "\n".join(pages).splitlines() if line.strip()]
                        return "\n".join(self._filter_history_by_scope(lines, alert))
                except Exception:
                    pass

            if curr_cid:
                conversation = await conv_mgr.get_conversation(unified_msg_origin, curr_cid)
                if conversation and getattr(conversation, "history", None):
                    history = conversation.history
                    if isinstance(history, str):
                        try:
                            parsed = json.loads(history)
                            if isinstance(parsed, list):
                                lines = []
                                for item in parsed[-20:]:
                                    if isinstance(item, dict):
                                        role = item.get("role") or item.get("type") or ""
                                        content = item.get("content") or item.get("text") or ""
                                        if role or content:
                                            lines.append(f"{role}: {content}".strip())
                                    else:
                                        lines.append(str(item))
                                return "\n".join(self._filter_history_by_scope(lines, alert))
                        except json.JSONDecodeError:
                            lines = [line for line in history[-2000:].splitlines() if line.strip()]
                            return "\n".join(self._filter_history_by_scope(lines, alert))
                    return str(history)
        except Exception as exc:
            logger.warning(f"FactorioAlarmAnchor: 读取会话历史失败：{exc}")
        return ""

    def _filter_history_by_scope(self, lines: List[str], alert: Dict[str, str]) -> List[str]:
        if self._history_mode == "recent":
            return lines[-self._history_count :] if self._history_count > 0 else lines

        if self._history_mode == "relevant":
            keywords = set(re.findall(r"\w+", " ".join([alert.get("alert_name", ""), alert.get("message", ""), alert.get("source", "")]).lower()))
            scored: List[tuple[int, int, str]] = []
            for idx, line in enumerate(lines):
                score = sum(1 for kw in keywords if kw and kw in line.lower())
                scored.append((score, idx, line))
            scored.sort(key=lambda item: (-item[0], -item[1]))
            selected = scored[: self._history_count] if self._history_count > 0 else [item for item in scored if item[0] > 0]
            if not selected:
                selected = scored[: self._history_count] if self._history_count > 0 else scored
            selected.sort(key=lambda item: item[1])
            return [item[2] for item in selected]

        return lines

    async def _append_alert_to_history(self, alert: Dict[str, str], llm_response: Optional[str] = None) -> None:
        if not self._subscribed_umo:
            return
        try:
            conv_mgr = self.context.conversation_manager
            curr_cid = await conv_mgr.get_curr_conversation_id(self._subscribed_umo)
            if not curr_cid:
                logger.warning("FactorioAlarmAnchor: 未找到当前会话 ID，无法记录告警")
                return

            conversation = await conv_mgr.get_conversation(self._subscribed_umo, curr_cid)
            if conversation is None:
                logger.warning("FactorioAlarmAnchor: 未找到会话对象，无法记录告警")
                return

            history_list: List[Dict[str, Any]] = []
            if isinstance(conversation.history, str) and conversation.history:
                try:
                    parsed = json.loads(conversation.history)
                    if isinstance(parsed, list):
                        history_list = parsed
                except json.JSONDecodeError:
                    history_list = [{"role": "system", "content": conversation.history}]

            # 添加告警消息（作为 user 角色）
            alert_entry = {
                "role": "user",
                "content": f"Factorio 告警：{alert['alert_name']}，等级：{alert['severity']}，来源：{alert['source']}，时间：{alert['timestamp']}，内容：{alert['message']}",
            }
            history_list.append(alert_entry)
            logger.debug(f"FactorioAlarmAnchor: 添加告警到历史 - {alert['alert_name']}")

            # 如果有 LLM 响应，也添加到历史（作为 assistant 角色）
            if llm_response:
                assistant_entry = {
                    "role": "assistant",
                    "content": llm_response,
                }
                history_list.append(assistant_entry)
                logger.debug(f"FactorioAlarmAnchor: 添加 LLM 响应到历史，长度: {len(llm_response)} 字符")

            await conv_mgr.update_conversation(
                unified_msg_origin=self._subscribed_umo,
                conversation_id=curr_cid,
                history=history_list,
            )
            logger.info("FactorioAlarmAnchor: 成功记录告警和响应到会话历史")
        except Exception as exc:
            logger.warning(f"FactorioAlarmAnchor: 记录告警到会话历史失败：{exc}")

    def _build_llm_prompt(self, alert: Dict[str, str], history_text: str = "") -> str:
        prompt = ""
        if history_text:
            prompt += (
                "当前会话历史：\n"
                f"{history_text}\n\n"
                "请结合上述会话历史和告警信息，判断是否需要立即处理，并给出一个简明优先级建议。\n\n"
            )
        prompt += (
            f"告警名称：{alert['alert_name']}\n"
            f"告警等级：{alert['severity']}\n"
            f"告警来源：{alert['source']}\n"
            f"告警时间：{alert['timestamp']}\n"
            f"告警内容：{alert['message']}\n\n"
            "请直接返回一句简短的中文结论，说明是否需要立即处理，并给出一个优先级建议。"
        )
        return prompt

    def _build_push_message(self, alert: Dict[str, str], llm_text: Optional[str]) -> str:
        lines = [
            "【Factorio 告警】",
            f"告警：{alert['alert_name']}",
            f"等级：{alert['severity']}",
            f"建议：{llm_text.strip() if llm_text else '未获取到建议'}",
        ]
        if alert.get("source"):
            lines.insert(3, f"来源：{alert['source']}")
        return "\n".join(lines)

    async def _send_to_subscribed_session(self, message: str) -> None:
        if not self._subscribed_umo:
            logger.warning("FactorioAlarmAnchor: 未绑定会话，无法推送告警")
            return

        logger.info(f"FactorioAlarmAnchor: 准备推送告警消息到会话: {self._subscribed_umo}")
        logger.debug(f"FactorioAlarmAnchor: 推送消息内容: {message}")

        chain = MessageChain().message(message)
        try:
            await self.context.send_message(self._subscribed_umo, chain)
            logger.info("FactorioAlarmAnchor: 告警消息推送成功")
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: 发送告警消息失败：{exc}")

    @filter.command("alarm_subscribe")
    async def alarm_subscribe(self, event: AstrMessageEvent):
        """将当前会话绑定为告警推送目标。"""
        self._subscribed_umo = event.unified_msg_origin
        logger.info(f"FactorioAlarmAnchor: 订阅成功，绑定会话 {self._subscribed_umo}")
        yield event.plain_result(
            "已将当前会话绑定为告警推送目标。\n"
            f"请使用 POST /alert 向插件发送告警数据，服务地址: http://<host>:{self._alarm_port}/alert"
        )

    @filter.command("alarm_unsubscribe")
    async def alarm_unsubscribe(self, event: AstrMessageEvent):
        """取消当前会话的告警推送绑定。"""
        if event.unified_msg_origin == self._subscribed_umo:
            self._subscribed_umo = None
            logger.info("FactorioAlarmAnchor: 取消绑定会话")
            yield event.plain_result("已取消告警推送订阅。")
        else:
            yield event.plain_result("当前会话未绑定为告警推送目标，无法取消。")

    @filter.command("alarm_subscribe_status")
    async def alarm_subscribe_status(self, event: AstrMessageEvent):
        """查询当前告警推送绑定状态。"""
        if self._subscribed_umo:
            yield event.plain_result(
                f"当前已绑定告警目标会话：{self._subscribed_umo}\n"
                f"告警推送地址：http://<host>:{self._alarm_port}/alert"
            )
        else:
            yield event.plain_result("当前未绑定任何告警推送会话。请在目标会话中发送 /alarm_subscribe 进行绑定。")

    @filter.command("alarm_test")
    async def alarm_test(self, event: AstrMessageEvent):
        """发送一个测试告警以验证插件功能。"""
        if not self._subscribed_umo:
            yield event.plain_result("当前未绑定会话，请先使用 /alarm_subscribe 绑定当前会话")
            return

        if event.unified_msg_origin != self._subscribed_umo:
            yield event.plain_result(f"当前会话未绑定为告警目标。已绑定会话: {self._subscribed_umo}")
            return

        logger.info("FactorioAlarmAnchor: 收到测试告警指令")
        yield event.plain_result("正在发送测试告警...")

        # 构造测试告警
        test_alert = {
            "alert_name": "测试告警",
            "severity": "info",
            "source": "test_command",
            "timestamp": "2026-06-21 12:00:00",
            "message": "这是一个测试告警消息，用于验证插件功能是否正常。",
        }

        try:
            result = await self._process_alert(test_alert)
            logger.info("FactorioAlarmAnchor: 测试告警处理完成")
            yield event.plain_result(f"测试告警已处理完成\n\n{result}")
        except Exception as exc:
            logger.exception(f"FactorioAlarmAnchor: 测试告警处理失败：{exc}")
            yield event.plain_result(f"测试告警处理失败：{exc}")

    @filter.command("alarm_debug")
    async def alarm_debug(self, event: AstrMessageEvent):
        """显示插件调试信息。"""
        provider = self.context.get_using_provider(umo=event.unified_msg_origin)
        provider_name = provider.__class__.__name__ if provider else "未配置"

        try:
            provider_id = await self.context.get_current_chat_provider_id(event.unified_msg_origin)
        except:
            provider_id = "获取失败"

        conv_mgr = self.context.conversation_manager
        try:
            curr_cid = await conv_mgr.get_curr_conversation_id(event.unified_msg_origin)
            conversation = await conv_mgr.get_conversation(event.unified_msg_origin, curr_cid)
            history_count = 0
            if conversation and conversation.history:
                try:
                    parsed = json.loads(conversation.history)
                    if isinstance(parsed, list):
                        history_count = len(parsed)
                except:
                    history_count = "解析失败"
        except:
            curr_cid = "获取失败"
            history_count = "获取失败"

        debug_info = (
            f"【插件调试信息】\n"
            f"订阅会话: {self._subscribed_umo or '未绑定'}\n"
            f"当前会话: {event.unified_msg_origin}\n"
            f"监听端口: {self._alarm_port}\n"
            f"Agent 模式: {'启用' if self._enable_agent_mode else '禁用'}\n"
            f"记录历史: {'启用' if self._record_alert_history else '禁用'}\n"
            f"历史模式: {self._history_mode}\n"
            f"历史条数: {self._history_count}\n"
            f"LLM 提供商: {provider_name}\n"
            f"Provider ID: {provider_id}\n"
            f"对话 ID: {curr_cid}\n"
            f"历史记录数: {history_count}\n"
        )
        yield event.plain_result(debug_info)
