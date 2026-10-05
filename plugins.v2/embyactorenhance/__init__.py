import re
import time
import pytz
import json
import queue
import base64
import difflib
import threading
from dateutil.parser import isoparse
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional, Callable

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.background import BackgroundScheduler

from app.log import logger
from app.core.cache import Cache
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.plugins import _PluginBase
from app.utils.string import StringUtils
from app.modules.douban import DoubanApi
from app.modules.themoviedb import TmdbApi
from app.schemas import WebhookEventInfo, ServiceInfo
from app.schemas.types import EventType, MediaType
from app.helper.mediaserver import MediaServerHelper
from app.sdk.network import RequestUtils


# 只有出演类人物才有角色名；导演、编剧、制片人等类型在 Emby 里按类型分区展示，
# 带上 Role 只会显示成重复的职务名或从别处串进来的角色名
_ROLE_PERSON_TYPES = ("Actor", "GuestStar")

# 单集标题里出现「第 N 集」说明标题被命名污染，需要刷新元数据
_EPISODE_TITLE_PATTERN = re.compile(r'第\s*([0-9]|[十|一|二|三|四|五|六|七|八|九|零])+\s*集')

# 片名相似度比较前，把中文数字归一成阿拉伯数字
_ZH_NUMBER_MAP = {
    "零": "0", "一": "1", "二": "2", "两": "2", "三": "3", "四": "4",
    "五": "5", "六": "6", "七": "7", "八": "8", "九": "9", "十": "10",
}


class EmbyActorEnhance(_PluginBase):
    # 插件名称
    plugin_name = "Emby演职人员增强"
    # 插件描述
    plugin_desc = "媒体元数据刷新，演职人员角色中文，导入季/集演职人员，更新节目系列演职人员为各季合并。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/xiaoQQya/MoviePilot-Plugins/refs/heads/main/icons/actor.png"
    # 插件版本
    plugin_version = "1.1.0"
    # 插件作者
    plugin_author = "xiaoQQya"
    # 作者主页
    author_url = "https://github.com/xiaoQQya"
    # 插件配置项ID前缀
    plugin_config_prefix = "embyactorenhance_"
    # 加载顺序
    plugin_order = 100
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _enabled = False
    _clearcache = False
    _onlyonce = False
    _mediaservers = None
    _num = None
    _cron = None

    _scheduler = None
    _tmdbapi = TmdbApi()
    _doubanapi = DoubanApi()
    _cache = Cache("ttl", 2000, 7 * 24 * 60 * 60)
    # 队列、消费线程和停止标志都由 init_plugin 按实例重建，插件分身之间不共享
    _queue = queue.Queue()
    _stop_event: Optional["threading.Event"] = None
    _consumer: Optional["threading.Thread"] = None

    @property
    def _region(self) -> str:
        """插件专属缓存区，与插件配置项前缀保持一致"""
        return self.plugin_config_prefix.rstrip("_")

    def _cache_get(self, key: str, default: Any = None) -> Any:
        """
        读缓存，未命中或条目刚好过期时返回默认值

        exists() 与 get() 是两次独立查询，中间条目可能过期，get 会返回 None；
        统一在这里兜底，调用方不必各自判空。
        :param key: 缓存键
        :param default: 未命中时返回的值
        :return: 缓存值或默认值
        """
        value = self._cache.get(key, self._region)
        return default if value is None else value

    def init_plugin(self, config: Optional[dict] = None):
        self.stop_service()

        if config:
            self._enabled = config.get("enabled")
            self._clearcache = config.get("clearcache")
            self._onlyonce = config.get("onlyonce")
            self._mediaservers = config.get("mediaservers") or []
            self._num = config.get("num")
            self._cron = config.get("cron")
            
        if self._clearcache:
            logger.info("Emby 演职人员缓存清除")
            self._cache.clear(self._region)
            self._clearcache = False

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        self._scheduler.start()
        # webhook 消费是永不返回的常驻循环，交给守护线程；挂在调度器上会永久占用一个
        # worker，且 shutdown() 默认 wait=True，保存配置时会被它拖住
        self._start_hook_consumer()

        if self._onlyonce:
            logger.info("Emby 演职人员增强服务启动，立即运行一次")
            self._scheduler.add_job(func=self.run, trigger="date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=1),
                                    name="Emby 演职人员增强")
            self._onlyonce = False
            
        self.update_config({
            "enabled": self._enabled,
            "clearcache": False,
            "onlyonce": False,
            "mediaservers": self._mediaservers,
            "num": self._num,
            "cron": self._cron
        })

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [{
                "id": "EmbyActorEnhance",
                "name": self.plugin_name,
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run,
                "kwargs": {}
            }]
        return []

    @eventmanager.register(EventType.WebhookMessage)
    def hook(self, event: Event):
        """
        监听媒体入库事件
        """
        if not self._enabled:
            return

        event_info: WebhookEventInfo = event.event_data
        if not event_info:
            return
        
        if "emby" != event_info.channel:
            return
        
        if "library.new" != event_info.event:
            return
        
        mediaserver: ServiceInfo = self.service_infos.get(event_info.server_name)
        if not mediaserver:
            return
        
        media = (event_info.json_object or {}).get("Item")
        if not media:
            logger.warning(f"媒体服务器 {event_info.server_name} 的入库事件缺少 Item 数据，跳过处理")
            return

        self._queue.put((mediaserver, media))

    def _start_hook_consumer(self) -> None:
        """
        启动 webhook 事件的常驻消费线程

        队列、停止标志和线程都在这里按实例重建，插件分身各自消费自己的事件。
        """
        self._queue = queue.Queue()
        self._stop_event = threading.Event()
        self._consumer = threading.Thread(
            target=self.handle_hook,
            daemon=True,
            name="EmbyActorEnhance-consumer",
        )
        self._consumer.start()

    def handle_hook(self):
        """
        处理媒体入库事件

        单条事件处理失败只记录日志，不能让消费循环退出：线程退出后没有任何机制重新拉起，
        队列会持续堆积到插件重载为止。轮询带上超时是为了能在停止标志置位后及时退出，
        不必依赖往队列里塞哨兵。
        """
        logger.info("媒体入库事件 webhook 处理启动")
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            if item is None:
                break

            mediaserver, media = item
            try:
                self._handle_media(mediaserver, media)
            except Exception as err:  # pylint: disable=broad-except
                logger.error(f"处理媒体入库事件失败：{err}", exc_info=True)
        logger.info("媒体入库事件 webhook 处理停止")

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        if not self._mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return {}

        services = MediaServerHelper().get_services(name_filters=self._mediaservers)
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return {}

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return {}

        return active_services

    def run(self) -> None:
        # service_infos 每次求值都会重建 MediaServerHelper 并重新枚举运行模块，只取一次
        services = self.service_infos or {}
        if not services:
            return

        for name, service in services.items():
            logger.info(f"开始获取媒体服务器 {name} 最近 {self._num} 天的媒体数据")
            medias = self._get_latest_medias(service)
            logger.info(f"获取媒体服务器 {name} 最近 {self._num} 天的媒体数据共 {len(medias)} 条")

            for media in medias:
                self._handle_media(service, media)
            
            logger.info(f"媒体服务器 {name} 演职人员增强完成")

    def _handle_media(self, mediaserver: ServiceInfo, media: dict):
        item_type = media.get("Type")
        # library.new 事件还会带上 Series、Season 等条目类型，这些类型没有可增强的演职人员
        # 层级；未登记的类型一律跳过，避免把剧集或季的 ID 当成电影 ID 处理
        media_type = {"Movie": MediaType.MOVIE, "Episode": MediaType.TV}.get(item_type)
        if not media_type:
            logger.info(f"<{media.get('Name')}> 条目类型 {item_type} 无需演职人员增强，跳过处理")
            return

        series_id = media.get("SeriesId") if media_type == MediaType.TV else media.get("Id")
        if not series_id:
            logger.warning(f"<{media.get('Name')}> 未获取到媒体条目 ID，跳过处理")
            return

        season_id = media.get("SeasonId", None)
        series_name = media.get("SeriesName") if media_type == MediaType.TV else media.get("Name")
        season_name = media.get("SeasonName", None)
        media_name = f"{series_name}-{season_name}-{media.get('Name')}" if media_type == MediaType.TV else f"{series_name}"

        # 刷新媒体元信息
        self._auto_refresh_item(mediaserver, media, media_type)

        # 单集有自己的分集演职人员表，必须排在季级缓存判断之前：
        # 同一季的首集会写入季级缓存，后续单集若先判缓存就会被整体跳过
        cached_series = cached_season = None
        if media_type == MediaType.TV:
            cached_series, cached_season = self._update_episode_credits(
                mediaserver, media, series_id, season_id)

        # 处理缓存信息
        key = f"{mediaserver.name}:handled_medias"
        handled_medias = self._cache_get(key, [])
        if (season_id or series_id) in handled_medias:
            logger.info(f"<{media_name}> 媒体演职人员信息已更新，跳过更新")
            return

        # 获取系列元信息（单集路径已取过就直接复用）
        series_info = cached_series or self._get_item_info(mediaserver, series_id)
        if not series_info:
            logger.warning(f"<{series_name}> 获取系列元信息失败，请检查配置")
            return

        # 获取季元信息（同上）
        season_info = None
        if media_type == MediaType.TV and season_id:
            season_info = cached_season or self._get_item_info(mediaserver, season_id)
            if not season_info:
                logger.warning(f"<{series_name}-{season_name}> 获取季元信息失败，请检查配置")
                return

            # 更新季演职人员信息
            season_info = self._update_season_credits(mediaserver, series_info, season_info)
            if not season_info:
                return

        # 演职人员角色信息中文
        if not self._update_chinese_role(mediaserver, media_type, series_info, season_info):
            return

        # 更新系列演职人员信息
        if media_type == MediaType.TV and season_info:
            series_info = self._update_tv_credits(mediaserver, series_info, season_info)

        # 缓存处理信息
        self._remember_handled(key, season_id or series_id)

        time.sleep(3)

    @staticmethod
    def _parse_item_date(value: Optional[str]) -> Optional[datetime]:
        """
        解析 Emby 条目的入库时间，无法解析时返回 None

        DateCreated 可能缺失、为空串或格式异常；Emby 也可能返回不带时区的时间，
        这类值必须补齐 UTC 时区，否则与带时区的时间窗相减会直接抛 TypeError。
        """
        if not value:
            return None
        try:
            item_date = isoparse(value)
        except (TypeError, ValueError, OverflowError) as err:
            logger.warning(f"解析入库时间 {value} 失败：{err}")
            return None
        if item_date.tzinfo is None:
            item_date = item_date.replace(tzinfo=pytz.utc)
        return item_date

    def _get_latest_medias(self, mediaserver: ServiceInfo):
        """
        获取最新媒体数据

        Items 按入库时间倒序返回，遇到早于时间窗的条目即可停止遍历；缺少入库时间的
        条目无法判断归属，跳过该条但不中断，避免一条脏数据让整轮刷新结果为空。
        """
        try:
            days = int(self._num)
        except (TypeError, ValueError):
            days = 3
            logger.warning(f"最新入库天数配置无效（{self._num}），按 {days} 天处理")

        url = "[HOST]emby/Users/[USER]/Items?Limit=1000&api_key=[APIKEY]&SortBy=DateCreated,SortName&SortOrder=Descending&IncludeItemTypes=Episode,Movie&Recursive=true&Fields=DateCreated,Overview,PrimaryImageAspectRatio,ProductionYear"
        res = mediaserver.instance.get_data(url=url)
        if not (res and res.status_code == 200):
            return []

        items = res.json().get("Items", [])
        medias = []
        update_date = datetime.now(tz=pytz.utc) - timedelta(days=days)
        for item in items:
            item_date = self._parse_item_date(item.get("DateCreated"))
            if item_date is None:
                logger.warning(f"<{item.get('Name')}> 缺少可用的入库时间，跳过该条")
                continue
            if item_date > update_date:
                medias.append(item)
            else:
                break
        return medias

    def _get_item_info(self, mediaserver: ServiceInfo, item_id: int):
        """
        获取单个项目详情
        """
        url = f"[HOST]emby/Users/[USER]/Items/{item_id}?X-Emby-Token=[APIKEY]&Fields=ChannelMappingInfo&ExcludeFields=Chapters,MediaSources,MediaStreams,Subviews"
        res = mediaserver.instance.get_data(url=url)
        if res and res.status_code == 200:
            return res.json()
        return None

    @staticmethod
    def _get_tmdb_provider_id(item_info: dict) -> Optional[str]:
        """
        读取条目的 TMDB ProviderId

        Emby 返回的 ProviderIds 可能整体缺失或为 null，链式取值会抛 AttributeError。
        """
        provider_ids = item_info.get("ProviderIds")
        if not isinstance(provider_ids, dict):
            return None
        return provider_ids.get("Tmdb")

    @staticmethod
    def _set_tmdb_provider_id(item_info: dict, tmdb_id: Optional[str]) -> None:
        """
        写入条目的 TMDB ProviderId，ProviderIds 缺失或非法时按空字典补建
        """
        provider_ids = item_info.get("ProviderIds")
        if not isinstance(provider_ids, dict):
            provider_ids = {}
            item_info["ProviderIds"] = provider_ids
        provider_ids["Tmdb"] = tmdb_id

    @staticmethod
    def _lock_cast_field(item_info: dict) -> None:
        """
        把 Cast 加入条目的锁定字段

        LockedFields 缺失或被置空时必须补建，否则 Emby 后续刷新会用在线元数据
        覆盖已写入的演职人员。
        """
        locked_fields = item_info.get("LockedFields")
        if not isinstance(locked_fields, list):
            locked_fields = []
            item_info["LockedFields"] = locked_fields
        if "Cast" not in locked_fields:
            locked_fields.append("Cast")

    @staticmethod
    def _merge_people_roles(peoples: List[dict]) -> List[dict]:
        """
        按「名称 + 类型」合并演职人员，同一人物的多个角色用「 / 」拼接并去重

        Emby 会把一人分饰两角或身兼数职的同一个人拆成多条；这里按名称合并角色，
        同时保留类型维度，让演员、导演、编剧各自独立，不因合并丢失类型信息。
        名称缺失的条目按其 Id 单独成组，不会被静默丢弃。
        :param peoples: 条目上的演职人员列表
        :return: 合并后的演职人员列表，保持各组首次出现的顺序
        """
        owners: Dict[Tuple[str, str], dict] = {}
        roles: Dict[Tuple[str, str], List[str]] = {}
        for people in peoples:
            name = people.get("Name") or f"#{people.get('Id')}"
            key = (name, people.get("Type") or "")
            if key not in owners:
                owners[key] = people
                roles[key] = []
            bucket = roles[key]
            # 旧值可能已经是「A / B」形式，先拆开再合并才能正确去重
            for role in (people.get("Role") or "").split("/"):
                role = role.strip()
                if role and role not in bucket:
                    bucket.append(role)

        merged = []
        # owners 是普通 dict，插入顺序就是各组首次出现的顺序
        for key, people in owners.items():
            # 只有出演类人物保留角色；其它类型连空 Role 也不留。
            # 多角色沿用 Emby/TMDb 既有的「 / 」分隔形式
            if key[1] in _ROLE_PERSON_TYPES and roles[key]:
                people["Role"] = " / ".join(roles[key])
            else:
                people.pop("Role", None)
            merged.append(people)
        return merged

    def _load_tmdb_casts(
        self,
        media_type: MediaType,
        series_info: dict,
        season_info: Optional[dict] = None,
        episode_info: Optional[dict] = None,
    ) -> Dict[str, str]:
        """
        从当前作品的 TMDb credits 建立「人物名称 -> TMDb 人物 ID」映射

        人物在具体作品里的出演记录是唯一的，不像按名称搜索那样存在同名歧义，因此这是
        给人物实体补 TMDb ID 时可靠的来源。取不到时返回空映射，调用方按无法锚定处理。
        :param media_type: 媒体类型
        :param series_info: 系列或电影条目
        :param season_info: 季条目，仅电视剧有
        :param episode_info: 单集条目，传入时取分集演职员表
        :return: 人物名称到 TMDb 人物 ID 的映射
        """
        tmdb_id = self._get_tmdb_provider_id(series_info)
        if not tmdb_id:
            return {}
        try:
            if media_type == MediaType.TV and season_info and episode_info:
                # 分集演职员表与季的不同，单集的锚定必须用单集自己的 credits
                credits = self._tmdbapi.episode_obj.credits(
                    tv_id=int(tmdb_id),
                    season_num=season_info.get("IndexNumber"),
                    episode_num=episode_info.get("IndexNumber"),
                )
            elif media_type == MediaType.TV and season_info:
                credits = self._tmdbapi.season_obj.credits(
                    tv_id=int(tmdb_id),
                    season_num=season_info.get("IndexNumber"),
                )
            elif media_type == MediaType.TV:
                credits = self._tmdbapi.tv.credits(tv_id=int(tmdb_id))
            else:
                credits = self._tmdbapi.movie.credits(movie_id=int(tmdb_id))
        except Exception as err:  # pylint: disable=broad-except
            logger.warning(f"获取 TMDb 演职人员信息失败：{err}")
            return {}

        credits = credits or {}
        casts: Dict[str, str] = {}
        for group in (credits.get("cast"), credits.get("crew")):
            for person in group or []:
                name, person_id = person.get("name"), person.get("id")
                if name and person_id:
                    casts.setdefault(name, str(person_id))
        return casts

    def _get_person_aliases(self, person_id: str) -> List[str]:
        """
        取 TMDb 人物的别名列表，按人物 ID 缓存

        人物的别名几乎不变，而同一人物会出现在同季的多个单集里；不缓存就会逐集重复
        请求 TMDb，既慢又容易触发限流。
        :param person_id: TMDb 人物 ID
        :return: 别名列表，取不到时返回空列表
        """
        key = f"person_aliases:{person_id}"
        cached = self._cache_get(key)
        if cached is not None:
            return cached

        detail = self._tmdbapi.get_person_detail(person_id)
        aliases = (detail or {}).get("also_known_as") or []
        # 空结果也缓存，避免没有别名的人物每次都重查 TMDb
        self._cache.set(key, aliases, None, self._region)
        return aliases

    def _prepare_person_entity(
        self,
        mediaserver: ServiceInfo,
        person_id: Optional[str],
        person_name: Optional[str],
        new_name: Optional[str],
        tmdb_casts: Dict[str, str],
        people_info: Optional[dict] = None,
    ) -> Optional[str]:
        """
        改条目之前先把人物实体调整到位

        实体必须比条目先改名：Emby 按名称匹配人物实体，条目里写了库里不存在的名称时，
        它会新建一个没有任何元数据的空壳，原实体的头像、简介和 ProviderIds 就此与条目
        脱钩（实测过 `Zhu Ran` 被条目改成 `朱然` 后分裂成两个实体的案例）。实体先改名，
        名称匹配才能命中，条目改完仍指向同一个 Id。

        同时补齐缺失的 TMDb ID：实体没有外部 ID 时 Emby 抓不到这个人物的头像和简介。
        :param mediaserver: 媒体服务器
        :param person_id: 人物实体 ID
        :param person_name: 人物当前名称，用于在 credits 中定位
        :param new_name: 目标名称，与实体当前名称相同时只做 ID 补齐
        :param tmdb_casts: 当前作品的人物名称到 TMDb 人物 ID 映射
        :param people_info: 已取到的人物实体，传入时不再重复请求
        :return: 人物实体的 TMDb ID；无法确定时返回 None
        """
        if not person_id:
            return None

        if people_info is None:
            people_info = self._get_item_info(mediaserver, person_id)
        if not people_info:
            logger.warning(f"人员 <{person_name}> 人物实体获取失败，改名后条目可能与其脱钩")
            return None

        changed = False
        if new_name and new_name != people_info.get("Name"):
            logger.info(f"人员 <{person_name}> 实体改名为 <{new_name}>，条目将沿用同一人物实体")
            people_info["Name"] = new_name
            changed = True

        tmdb_id = self._get_tmdb_provider_id(people_info)
        if not tmdb_id:
            resolved = tmdb_casts.get(person_name)
            if resolved:
                self._set_tmdb_provider_id(people_info, resolved)
                tmdb_id = resolved
                changed = True
            else:
                logger.warning(f"人员 <{person_name}> 不在当前作品的 TMDb 演职人员中，无法补写 TMDb ID")

        if changed and not self._update_item_info(mediaserver, person_id, people_info):
            logger.warning(f"人员 <{person_name}> 人物实体更新失败")
            return None
        return tmdb_id

    @staticmethod
    def _douban_avatar_url(douban_actor: dict) -> Optional[str]:
        """
        取豆瓣演员的头像地址

        豆瓣默认返回 webp，这里把 imageView2 的 format 参数换成 jpg，接口会返回真正的
        JPEG，避免部分 Emby 版本对 webp 支持不一致。地址缺失时返回 None。
        :param douban_actor: 豆瓣演职人员条目
        :return: 可直接下载的头像地址
        """
        avatar = douban_actor.get("avatar")
        if not isinstance(avatar, dict):
            return None
        url = avatar.get("large") or avatar.get("normal")
        if not url:
            return None
        # imageView2 的 format 参数改成 jpg，接口返回真正的 JPEG，
        # 避免部分 Emby 版本对 webp 支持不一致
        return url.replace("format/webp", "format/jpg")

    def _apply_douban_avatar(
        self,
        mediaserver: ServiceInfo,
        people: dict,
        douban_actor: dict,
        media_name: str,
    ) -> Optional[str]:
        """
        用豆瓣头像补全没有头像的演员

        条目侧的演职人员条目用 PrimaryImageTag 表示是否已有头像，缺该字段即表示 Emby
        既没有本地图片也没有抓到远程图片。头像由插件自行下载后以 base64 提交，不依赖
        Emby 主机能否访问豆瓣图床。
        :param mediaserver: 媒体服务器
        :param people: 条目上的演职人员条目
        :param douban_actor: 豆瓣演职人员条目
        :param media_name: 用于日志的媒体名称
        :return: 写入成功返回新的头像标记，否则返回 None
        """
        person_id = people.get("Id")
        person_name = people.get("Name")
        if not person_id or not douban_actor:
            return None

        image_url = self._douban_avatar_url(douban_actor)
        if not image_url:
            return None

        # 豆瓣图床对直链校验 Referer，缺失时返回 418
        res = RequestUtils(headers={"Referer": "https://movie.douban.com/"}, timeout=30).get_res(image_url)
        if not res or res.status_code != 200 or not res.content:
            logger.warning(f"<{media_name}> 演员 <{person_name}> 的豆瓣头像下载失败")
            return None

        # Emby 按 Content-Type 推断图片扩展名，必须传真实图片类型，
        # 传 application/octet-stream 会被拒绝；body 则必须是该图片的 base64
        content_type = (res.headers.get("Content-Type") or "").split(";")[0].strip() or "image/jpeg"
        if not content_type.startswith("image/"):
            logger.warning(f"<{media_name}> 演员 <{person_name}> 的豆瓣头像返回了非图片内容（{content_type}）")
            return None

        url = f"[HOST]emby/Items/{person_id}/Images/Primary?api_key=[APIKEY]"
        uploaded = mediaserver.instance.post_data(
            url=url,
            data=base64.b64encode(res.content).decode(),
            headers={"Content-Type": content_type},
        )
        if not uploaded or uploaded.status_code not in [200, 204]:
            logger.warning(f"<{media_name}> 演员 <{person_name}> 的豆瓣头像写入失败")
            return None

        # 头像标记由 Emby 生成，回读实体拿到真实值，保证随后的条目写回不会把它丢掉
        updated_info = self._get_item_info(mediaserver, person_id)
        image_tag = ((updated_info or {}).get("ImageTags") or {}).get("Primary")
        logger.info(f"<{media_name}> 演员 <{person_name}> 已用豆瓣头像补全")
        return image_tag

    def _get_items_info(self, mediaserver: ServiceInfo, item_ids: List[Any]) -> Dict[str, dict]:
        """
        批量获取条目详情

        逐条 GET 是本插件最大的请求来源，一个 50 人的季要打 50 次；Emby 的列表接口支持
        Ids 批量查询，按批取回即可。请求必须带 IncludeItemTypes=Person，否则人物实体不在
        用户可见的条目列表里，会返回空结果。
        :param mediaserver: 媒体服务器
        :param item_ids: 条目 ID 列表，空值会被忽略
        :return: 条目 ID 到详情的映射，取不到的条目不出现在结果中
        """
        ids = [str(item_id) for item_id in item_ids if item_id]
        if not ids:
            return {}

        result: Dict[str, dict] = {}
        # 分批防止 Ids 过长导致 URL 超限
        for start in range(0, len(ids), 100):
            batch = ids[start:start + 100]
            url = (
                "[HOST]emby/Users/[USER]/Items"
                f"?Ids={','.join(batch)}&IncludeItemTypes=Person&Recursive=true"
                "&X-Emby-Token=[APIKEY]&Fields=ProviderIds,Overview,ImageTags"
            )
            res = mediaserver.instance.get_data(url=url)
            if not res or res.status_code != 200:
                continue
            for item in res.json().get("Items") or []:
                if item.get("Id"):
                    result[str(item["Id"])] = item
        return result

    def _auto_refresh_item(self, mediaserver: ServiceInfo, media: dict, media_type: MediaType):
        """
        自动刷新单个项目信息
        """
        item_id = media.get("Id")
        is_tv = media_type == MediaType.TV
        series_name = media.get("SeriesName") if is_tv else media.get("Name")
        season_name = media.get("SeasonName", None)
        episode_name = media.get("Name") or ""
        media_name = f"{series_name}-{season_name}-{episode_name}" if is_tv else f"{series_name}-{episode_name}"
        overview = media.get("Overview")
        image = (media.get("ImageTags") or {}).get("Primary")

        refresh_meta = bool(_EPISODE_TITLE_PATTERN.search(episode_name)) or not overview or not StringUtils.is_chinese(episode_name) or not StringUtils.is_chinese(overview)
        refresh_image = not image

        if refresh_meta or refresh_image:
            if self._refresh_item_info(mediaserver, item_id, refresh_meta, refresh_image):
                logger.info(f"<{media_name}> 媒体元信息刷新成功")
            else:
                logger.warning(f"<{media_name}> 媒体元信息刷新失败，请检查配置")
        else:
            logger.info(f"<{media_name}> 媒体元信息无需刷新")

    def _refresh_item_info(self, mediaserver: ServiceInfo, item_id: int, refresh_meta: bool = True, refresh_image: bool = True):
        """
        刷新单个项目信息
        """
        url = f"[HOST]emby/Items/{item_id}/Refresh?Recursive=true&MetadataRefreshMode=FullRefresh&ImageRefreshMode=FullRefresh&ReplaceAllMetadata={refresh_meta}&ReplaceAllImages={refresh_image}&ReplaceThumbnailImages=false&api_key=[APIKEY]"
        res = mediaserver.instance.post_data(url=url)
        if res and res.status_code in [200, 204]:
            return True
        return False

    def _update_season_credits(self, mediaserver: ServiceInfo, series_info: dict, season_info: dict):
        """
        更新季演职人员
        """
        item_id = season_info["Id"]
        series_name = series_info["Name"]
        season_name = season_info["Name"]
        media_name = f"{series_name}-{season_name}"
        if season_info.get("People"):
            logger.info(f"<{media_name}> 季演职人员已存在，跳过更新演职人员")
            return season_info

        tmdb_id = self._get_tmdb_provider_id(series_info)
        season = season_info.get("IndexNumber")
        if not tmdb_id:
            logger.warning(f"<{media_name}> 媒体未获取到 TMDB ID，跳过更新演职人员")
            return None
        credits = self._tmdbapi.season_obj.credits(tv_id=tmdb_id, season_num=season)
        if not credits or len(credits.get("cast", [])) == 0:
            logger.warning(f"<{media_name}> 媒体未找到季演职人员信息，跳过更新演职人员")
            return None

        peoples = []
        for cast in credits.get("cast", []):
            people = {"Name": cast.get("name"), "Role": cast.get("character")}
            if cast.get("known_for_department") == "Acting":
                people["Type"] = "Actor"
            elif cast.get("known_for_department") == "Directing":
                people["Type"] = "Director"
            elif cast.get("known_for_department") == "Writing":
                people["Type"] = "Writer"
            else:
                continue
            peoples.append(people)
        if self._write_people(mediaserver, season_info, peoples, media_name, "季"):
            logger.info(f"<{media_name}> 季演职人员信息更新成功")
        else:
            logger.warning(f"<{media_name}> 季演职人员信息更新失败")
            return None

        # 刮削演职人员信息
        casts = {cast.get("name"): cast.get("id") for cast in credits.get("cast", [])}
        updated_season_info = self._get_item_info(mediaserver, item_id)
        if not updated_season_info:
            logger.warning(f"<{media_name}> 季演职人员信息刷新失败")
            return None
        peoples = updated_season_info.get("People", [])
        # 逐人取实体是本流程最大的请求来源，一次批量取回
        people_infos = self._get_items_info(mediaserver, [p.get("Id") for p in peoples])
        for people in peoples:
            people_id = people.get("Id", None)
            people_name = people.get("Name", None)
            people_info = people_infos.get(str(people_id))
            if not people_info:
                logger.warning(f"<{media_name}> 季演职人员 {people_name} 信息刷新失败")
                continue

            people_tmdb_id = self._get_tmdb_provider_id(people_info)
            people_overview = people_info.get("Overview")
            people_image = (people_info.get("ImageTags") or {}).get("Primary")
            if not people_overview or not people_image:
                if not people_tmdb_id:
                    self._set_tmdb_provider_id(people_info, casts.get(people_name))
                    self._update_item_info(mediaserver, people_id, people_info)
                if self._refresh_item_info(mediaserver, people_id, not people_overview, not people_image):
                    logger.info(f"<{media_name}> 季演职人员 <{people_name}> 信息刷新成功")
                else:
                    logger.warning(f"<{media_name}> 季演职人员 <{people_name}> 信息刷新失败")

        return updated_season_info

    def _update_tv_credits(self, mediaserver: ServiceInfo, series_info: dict, season_info: dict):
        """
        更新系列演职人员
        """
        series_name = series_info["Name"]

        series_peoples = {(people.get("Name"), people.get("Type") or ""): people
                          for people in series_info.get("People") or [] if people.get("Name")}
        season_peoples = {(people.get("Name"), people.get("Type") or ""): people
                          for people in season_info.get("People") or [] if people.get("Name")}
        updated_series_peoples = []
        for key, series_people in series_peoples.items():
            # 同一个人的多个类型各自独立，不跨类型互相覆盖角色
            series_people["Role"] = season_peoples.get(
                key, {}).get("Role") or series_people.get("Role")
            updated_series_peoples.append(series_people)
        for key, season_people in season_peoples.items():
            if key not in series_peoples:
                updated_series_peoples.append(season_people)

        if self._write_people(mediaserver, series_info, updated_series_peoples, series_name, "系列"):
            logger.info(f"<{series_name}> 系列演职人员信息更新成功")
            return series_info
        else:
            logger.warning(f"<{series_name}> 系列演职人员信息更新失败")
            return None

    def _update_item_info(self, mediaserver: ServiceInfo, item_id: int, item_info: dict):
        """
        更新媒体信息
        """
        url = f"[HOST]emby/Items/{item_id}?reqformat=json&api_key=[APIKEY]"
        headers = {"Content-Type": "application/json"}
        res = mediaserver.instance.post_data(url=url, data=json.dumps(item_info), headers=headers)
        if res and res.status_code in [200, 204]:
            return True
        return False

    def _load_douban_peoples(
        self,
        media_type: MediaType,
        series_info: dict,
        season_info: Optional[dict],
    ) -> Optional[Dict[str, dict]]:
        """
        取当前媒体的豆瓣演职人员映射

        同一季的多个单集共用同一份豆瓣数据，按季（无季时按系列）缓存，避免逐集重复查询
        豆瓣触发限流。返回 None 表示豆瓣媒体信息获取失败，与「有媒体但没有演员」区分。
        :param media_type: 媒体类型
        :param series_info: 系列或电影条目
        :param season_info: 季条目，仅电视剧有
        :return: 人物名称到豆瓣人物条目的映射；获取失败时返回 None
        """
        cache_key = f"douban_peoples:{(season_info or series_info).get('Id')}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        douban_info = self._get_douban_info(media_type, series_info, season_info)
        if not douban_info:
            return None

        douban_peoples: Dict[str, dict] = {}
        for actor in douban_info.get("actors", []):
            actor_name = actor.get("name")
            if not actor_name:
                continue
            douban_peoples[actor_name] = actor
            if actor.get("latin_name"):
                douban_peoples[actor["latin_name"]] = actor

        # 空结果也缓存，避免豆瓣匹配不上的剧每一集都重走一遍搜索与打分
        self._cache.set(cache_key, douban_peoples, None, self._region)
        return douban_peoples

    def _apply_chinese_people(
        self,
        mediaserver: ServiceInfo,
        peoples: List[dict],
        douban_peoples: Dict[str, dict],
        media_name: str,
        casts_loader: Callable[[], Dict[str, str]],
    ) -> None:
        """
        就地改写演职人员条目的名称、角色与头像

        改名时先调整人物实体再改条目，这是 Emby 能否沿用同一人物的关键，详见
        `_prepare_person_entity`。
        :param mediaserver: 媒体服务器
        :param peoples: 条目上的演职人员列表，函数就地修改
        :param douban_peoples: 豆瓣人物名称映射
        :param media_name: 用于日志的媒体名称
        :param casts_loader: 惰性获取 TMDb 演职人员的回调，仅在确实要改名时调用
        """
        # TMDb 演职人员只在确实要改名时才拉取，避免给纯角色更新的场景白白增加一次请求
        tmdb_casts: Optional[Dict[str, str]] = None
        # 需要读人物实体才能判断改名的条目先集中批量取回：逐条 GET 是本流程最大的请求源，
        # 一季几十人、一集十几人，全靠这一处收敛
        people_infos = self._get_items_info(mediaserver, [
            people.get("Id") for people in peoples if self._needs_person_entity(people, douban_peoples)
        ])
        for people in peoples:
            original_name = people.get("Name")
            # 豆瓣条目同时用于改名和头像补全，因此在外层解析，名称与角色都已中文时也能补头像
            douban_actor = douban_peoples.get(original_name)
            if not StringUtils.is_chinese(original_name) or not StringUtils.is_chinese(people.get("Role")):
                if douban_actor:
                    new_name = douban_actor.get("name")
                    # 名称实际变化时才动实体：中文名互转不会让 Emby 失去人物匹配
                    if new_name and new_name != original_name:
                        if tmdb_casts is None:
                            tmdb_casts = casts_loader()
                        self._prepare_person_entity(
                            mediaserver, people.get("Id"), original_name, new_name, tmdb_casts
                        )
                    people["Name"] = new_name
                    people["Role"] = douban_actor.get("character")
                else:
                    people_info = people_infos.get(str(people.get("Id")))
                    people_tmdb_id = self._get_tmdb_provider_id(people_info) if people_info else None
                    if people_tmdb_id:
                        also_known_as = self._get_person_aliases(people_tmdb_id)
                        for alias in also_known_as:
                            matched_actor = douban_peoples.get(alias)
                            if matched_actor:
                                alias_name = matched_actor.get("name")
                                if alias_name and alias_name != original_name:
                                    if tmdb_casts is None:
                                        tmdb_casts = casts_loader()
                                    self._prepare_person_entity(
                                        mediaserver, people.get("Id"), original_name,
                                        alias_name, tmdb_casts, people_info,
                                    )
                                people["Name"] = alias_name
                                people["Role"] = matched_actor.get("character")
                                douban_actor = matched_actor
                                break
                        else:
                            # 别名与豆瓣演职人员表都对不上，既不改名也不换角色
                            logger.info(
                                f"人员 <{original_name}> 的 TMDb 别名与豆瓣演职人员表无交集，保持原名"
                            )
                    else:
                        logger.warning(f"人员 <{original_name}> 未获取到 tmdbid，跳过更新演职人员角色中文")
                # Role 只对出演类人物有意义，其它类型直接清掉，避免写回冗余的职务名
                if (people.get("Type") or "") in _ROLE_PERSON_TYPES:
                    people["Role"] = self._clean_role(people.get("Role"))
                else:
                    people.pop("Role", None)

            # 头像与名称是否变化无关：条目上没有头像标记就说明该演员缺图，用豆瓣头像补全
            if douban_actor and not people.get("PrimaryImageTag"):
                image_tag = self._apply_douban_avatar(mediaserver, people, douban_actor, media_name)
                if image_tag:
                    people["PrimaryImageTag"] = image_tag

    @staticmethod
    def _needs_person_entity(people: dict, douban_peoples: Dict[str, dict]) -> Optional[str]:
        """
        判断一条演职人员是否会走到「读人物实体」的分支，是则返回其实体 ID

        豆瓣表里直接命中名称的条目不需要实体，只有靠 TMDb 别名兜底的那些才要读实体拿
        ProviderIds。提前算出来就能一次批量取回，不必逐条 GET。
        :param people: 条目上的演职人员条目
        :param douban_peoples: 豆瓣人物名称映射
        :return: 需要读取的人物实体 ID，不需要时返回 None
        """
        if not people.get("Id"):
            return None
        if douban_peoples.get(people.get("Name")):
            return None
        if StringUtils.is_chinese(people.get("Name")) and StringUtils.is_chinese(people.get("Role")):
            return None
        return str(people["Id"])

    @staticmethod
    def _clean_role(role: Optional[str]) -> str:
        """
        清洗角色名里的中文标注与英文占位词

        豆瓣与 TMDb 的角色串常见「饰 某某」「某某(voice)」这类写法，统一归一成中文形式。
        :param role: 原始角色名，可能缺失或为 None
        :return: 清洗后的角色名
        """
        role = role or ""
        role = re.sub(r"饰\s+", "", role)
        role = re.sub(r"饰演\s+", "", role)
        role = re.sub(r"配\s+", "（配音）", role)
        role = re.sub(r"配音\s+", "（配音）", role)
        role = re.sub(r"演员", "", role)
        role = re.sub(r"自己", "", role)
        role = re.sub(r"\s*[（(]?\s*\bvoice\b\s*[）)]?\s*", "（配音）", role, flags=re.IGNORECASE)
        role = re.sub(r"\s*[（(]?\s*\bdirector\b\s*[）)]?\s*", "（导演）", role, flags=re.IGNORECASE)
        return role

    def _write_people(
        self,
        mediaserver: ServiceInfo,
        item_info: dict,
        peoples: List[dict],
        media_name: str,
        label: str,
    ) -> bool:
        """
        演职人员写回条目的唯一出口

        固定顺序：按名称与类型合并（合并时顺带按类型决定 Role 去留）→ 锁定 Cast → 写回。
        合并对已合并的数据幂等，季、系列、单集、电影四条路径因此可以共用同一条。
        :param mediaserver: 媒体服务器
        :param item_info: 待写回的条目
        :param peoples: 演职人员列表
        :param media_name: 用于日志的媒体名称
        :param label: 日志中描述该条目的名称
        :return: 写回是否成功
        """
        if peoples:
            merged_peoples = self._merge_people_roles(peoples)
            if len(merged_peoples) != len(peoples):
                logger.info(f"<{media_name}> {label}演职人员按名称与类型合并：{len(peoples)} 条 -> {len(merged_peoples)} 条")
            item_info["People"] = merged_peoples

        self._lock_cast_field(item_info)
        return self._update_item_info(mediaserver, item_info["Id"], item_info)

    def _update_chinese_role(
        self,
        mediaserver: ServiceInfo,
        media_type: MediaType,
        series_info: dict,
        season_info: Optional[dict],
    ) -> bool:
        """
        更新演职人员角色中文

        就地改写传入的系列或季条目，不替换对象本身。
        :return: 是否更新成功
        """
        if media_type == MediaType.TV and season_info:
            target_info = season_info
            media_name = f"{series_info.get('Name')}-{season_info.get('Name')}"
        else:
            target_info = series_info
            media_name = series_info.get("Name")

        douban_peoples = self._load_douban_peoples(media_type, series_info, season_info)
        if douban_peoples is None:
            logger.warning(f"<{media_name}> 获取豆瓣媒体信息失败，请检查配置")
            return False

        peoples = target_info.get("People") or []
        self._apply_chinese_people(
            mediaserver,
            peoples,
            douban_peoples,
            media_name,
            lambda: self._load_tmdb_casts(media_type, series_info, season_info),
        )

        if self._write_people(mediaserver, target_info, peoples, media_name, ""):
            logger.info(f"<{media_name}> 媒体演职人员角色中文更新成功")
            return True

        logger.warning(f"<{media_name}> 媒体演职人员角色中文更新失败")
        return False

    def _update_episode_credits(
        self,
        mediaserver: ServiceInfo,
        media: dict,
        series_id: str,
        season_id: Optional[str],
    ) -> Tuple[Optional[dict], Optional[dict]]:
        """
        更新单集自己的演职人员中文

        分集演职人员表与季、系列的都不是同一份数据，必须单独处理。调用点必须排在季级
        缓存判断之前，否则同一季的后续单集会因为季已处理而被整体跳过。单集单独记为
        已处理，重复触发时不再重复请求。
        :param mediaserver: 媒体服务器
        :param media: webhook 或列表返回的单集条目
        :param series_id: 所属系列 ID
        :param season_id: 所属季 ID
        :return: 顺带取到的（系列条目, 季条目），供调用方复用，免去同一轮里的重复拉取；
            未走到取条目那一步时返回 (None, None)
        """
        episode_id = media.get("Id")
        if not episode_id:
            return None, None

        key = f"{mediaserver.name}:handled_episodes"
        if episode_id in self._cache_get(key, []):
            return None, None

        episode_info = self._get_item_info(mediaserver, episode_id)
        if not episode_info:
            logger.warning(f"<{media.get('Name')}> 获取单集元信息失败，跳过单集演职人员更新")
            return None, None

        peoples = episode_info.get("People") or []
        if not peoples:
            self._remember_handled(key, episode_id)
            return None, None

        series_info = self._get_item_info(mediaserver, series_id)
        if not series_info:
            logger.warning(f"<{media.get('Name')}> 获取系列元信息失败，跳过单集演职人员更新")
            return None, None

        season_info = self._get_item_info(mediaserver, season_id) if season_id else None
        media_name = f"{series_info.get('Name')}-{media.get('Name')}"

        # 豆瓣数据按季缓存，同季各集复用同一份，不会逐集重复查询豆瓣
        douban_peoples = self._load_douban_peoples(MediaType.TV, series_info, season_info)
        if douban_peoples is None:
            logger.warning(f"<{media_name}> 获取豆瓣媒体信息失败，跳过单集演职人员更新")
            return series_info, season_info

        self._apply_chinese_people(
            mediaserver,
            peoples,
            douban_peoples,
            media_name,
            lambda: self._load_tmdb_casts(MediaType.TV, series_info, season_info, episode_info),
        )

        if self._write_people(mediaserver, episode_info, peoples, media_name, "单集"):
            logger.info(f"<{media_name}> 单集演职人员角色中文更新成功")
            self._remember_handled(key, episode_id)
        else:
            logger.warning(f"<{media_name}> 单集演职人员角色中文更新失败")
        return series_info, season_info

    def _remember_handled(self, key: str, item_id: str) -> None:
        """
        把条目记入「已处理」缓存列表，避免重复触发时重复请求

        :param key: 缓存键
        :param item_id: 条目 ID
        """
        handled = list(self._cache_get(key, []))
        if item_id not in handled:
            handled.append(item_id)
            self._cache.set(key, handled, None, self._region)

    def _get_douban_info(self, media_type: MediaType, series_info: dict, season_info: Optional[dict]):
        """
        匹配豆瓣媒体信息

        PremiereDate 可能缺失、为空或不是字符串，取值前先归一；拿不到年份时跳过
        年份校验，只按片名相似度匹配，避免整条豆瓣信息匹配失败。
        """
        series_name = series_info.get("Name")
        if not series_name:
            logger.warning("媒体条目缺少名称，无法匹配豆瓣信息")
            return None

        season_name = (season_info or {}).get("Name") or ""
        premiere_date = (season_info or series_info).get("PremiereDate") or ""
        year = str(premiere_date)[:4] if premiere_date else ""
        if not year:
            logger.info(f"<{series_name}> 未获取到首播日期，按片名相似度匹配豆瓣信息")

        result = self._doubanapi.search(series_name)
        if not result or not result.get("items"):
            return None

        douban_id = None
        for item in result.get("items") or []:
            if item.get("type_name") != media_type.value:
                continue
            target = item.get("target") or {}
            if year and target.get("year") != year:
                continue

            item_name = target.get("title")
            if not item_name:
                continue
            score_series = self.sequence_matcher(item_name, series_name)
            score_season = self.sequence_matcher(item_name, season_name)
            score_all = self.sequence_matcher(item_name, series_name + season_name)
            score = max(score_series, score_season, score_all)
            if score < 0.8:
                continue

            douban_id = target.get("id")
            break

        if not douban_id:
            return None

        douban_info = self.chain.douban_info(douban_id, media_type)
        if not douban_info:
            return None
        return douban_info

    @staticmethod
    def sequence_matcher(s1: str, s2: str) -> float:
        def normalize(text):
            if text is None:
                return ""
            text = text.lower().replace(" ", "")
            for zh, num in _ZH_NUMBER_MAP.items():
                text = text.replace(zh, num)
            return text

        return difflib.SequenceMatcher(None, normalize(s1), normalize(s2)).ratio()

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
        return []

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
                                    'md': 4
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
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clearcache',
                                            'label': '清除缓存后运行',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'mediaservers',
                                            'label': '媒体服务器',
                                            'items': [{"title": config.name, "value": config.name}
                                                      for config in MediaServerHelper().get_configs().values()
                                                      if config.type == "emby"]
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
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'num',
                                            'label': '最新入库天数',
                                            'placeholder': '更新多少天之内的入库记录（天）'
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
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '0 1 * * *'
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
                                            'text': '支持媒体服务器 Webhook 实时更新演职人员信息，需要设置媒体服务器 Webhook 地址为 http://HOST:PORT/api/v1/webhook?token=API_TOKEN&source=SERVER_NAME，其中 HOST 为 MoviePilot 服务地址，PORT 为 MoviePilot 服务端口（默认 3001），API_TOKEN 为 MoviePilot API Token，SERVER_NAME 为发送 Webhook 的媒体服务器在 MoviePilot 中的名称。'
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
            "clearcache": False,
            "onlyonce": False,
            "mediaservers": [],
            "num": 3,
            "cron": "0 1 * * *"
        }

    def get_page(self) -> Optional[List[dict]]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        插件详情页面使用Vuetify组件拼装，参考：https://vuetifyjs.com/
        :return: 页面配置（vuetify模式）或 None（vue模式）
        """
        # 本插件没有详情页面
        return None

    def stop_service(self):
        """
        退出插件

        先置停止标志让消费线程自行退出，再做有界等待：线程可能正卡在一次媒体处理里，
        无限等下去会拖住保存配置。调度器此时已没有常驻任务，可以正常收敛。
        """
        if self._stop_event:
            self._stop_event.set()
        if self._consumer and self._consumer.is_alive():
            self._consumer.join(timeout=30)

        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown()

        self._queue = queue.Queue()
        self._stop_event = None
        self._consumer = None
        self._scheduler = None
