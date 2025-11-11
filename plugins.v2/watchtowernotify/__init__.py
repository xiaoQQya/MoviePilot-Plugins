from typing import Any, List, Dict, Tuple, Optional

from fastapi import Request

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import Response, NotificationType


class WatchtowerNotify(_PluginBase):
    # 插件名称
    plugin_name = "Watchtower通知"
    # 插件描述
    plugin_desc = "接收 Watchtower 通知并推送。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/xiaoQQya/MoviePilot-Plugins/refs/heads/main/icons/watchtower.png"
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "xiaoQQya"
    # 作者主页
    author_url = "https://github.com/xiaoQQya"
    # 插件配置项ID前缀
    plugin_config_prefix = "watchtowernotify_"
    # 加载顺序
    plugin_order = 100
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _msgtype = None

    def init_plugin(self, config: Optional[dict] = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._msgtype = config.get("msgtype")

    def get_state(self) -> bool:
        return self._enabled

    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "auth: "apikey",  # 鉴权类型：apikey/bear
            "summary": "API名称",
            "description": "API说明"
        }]
        """
        return [
            {
                "path": "/webhook",
                "endpoint": self.send_notify,
                "methods": ["POST"],
                "auth": "apikey",
                "summary": "Watchtower通知",
                "description": "接收 Watchtower 通知并推送"
            }
        ]

    async def send_notify(self, request: Request) -> Response:
        """
        推送通知
        """
        body = await request.body()
        text = body.decode("utf-8")
        logger.info(f"收到 Watchtower 通知：\n{text}")

        if self._enabled:
            mtype = NotificationType.Manual
            if self._msgtype:
                mtype = NotificationType.__getitem__(str(self._msgtype)) or NotificationType.Manual
            if text and len(text.strip()) > 0:
                self.post_message(title="🚀 Watchtower 通知",
                                  mtype=mtype,
                                  text=text)
                logger.info(f"推送 Watchtower 通知成功：\n{text}")

        return Response(success=True, message="发送成功")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': False,
                                            'chips': True,
                                            'model': 'msgtype',
                                            'label': '消息类型',
                                            'items': [{"title": item.value, "value": item.name}
                                                      for item in NotificationType]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': 'Watchtower 环境变量 WATCHTOWER_NOTIFICATION_URL 配置为 generic+http://HOST:PORT/api/v1/plugin/WatchtowerNotify/webhook?apikey=APIKEY，其中 HOST 为 MoviePilot 服务地址，PORT 为 MoviePilot 服务端口（默认 3001），APIKEY 为 MoviePilot API Token。'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '如安装完插件后，Watchtower 发送消息提示 404，重启 MoviePilot 即可。'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "msgtype": "Manual"
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        插件详情页面使用Vuetify组件拼装，参考：https://vuetifyjs.com/
        :return: 页面配置（vuetify模式）或 None（vue模式）
        """
        pass

    def stop_service(self):
        """
        退出插件
        """
        pass
