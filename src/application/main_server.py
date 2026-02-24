import asyncio
from asyncio.subprocess import PIPE
from ipaddress import ip_address
from mimetypes import guess_type
import re
from contextlib import suppress
from datetime import datetime, time, timedelta
from hashlib import sha256
import json
from pathlib import Path
from shutil import which
from textwrap import dedent
from typing import TYPE_CHECKING
import time as time_module
from urllib.parse import quote, urljoin, urlparse, urlencode

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pyperclip import paste
from uvicorn import Config, Server

from ..custom import (
    __VERSION__,
    REPOSITORY,
    SERVER_HOST,
    SERVER_PORT,
    VERSION_BETA,
    VIDEO_INDEX,
    is_valid_token,
)
from ..models import (
    Account as AccountPayload,
    AccountLive,
    AccountTiktok,
    Comment,
    DataResponse,
    Detail,
    DetailTikTok,
    DouyinCookie,
    DouyinCookieBrowserCreate,
    DouyinCookieClipboardCreate,
    DouyinCookieCreate,
    DouyinUser,
    DouyinUserCreate,
    DouyinUserPage,
    DouyinUserSettingsUpdate,
    DouyinWork,
    DouyinWorkPage,
    DouyinDailyWorkPage,
    DouyinWorkListPage,
    DouyinClientFeedItem,
    DouyinClientFeedPage,
    DouyinPlaylistCreate,
    DouyinPlaylist,
    DouyinPlaylistPage,
    DouyinPlaylistImport,
    DouyinScheduleSetting,
    GeneralSearch,
    Live,
    LiveSearch,
    LiveTikTok,
    Mix,
    MixTikTok,
    Reply,
    Settings,
    ShortUrl,
    UrlResponse,
    UserSearch,
    VideoSearch,
)
from ..interface import Account as AccountFetcher, Detail as DetailFetcher
from ..module import Cookie, DouyinLiveRecorder
from ..tools import Browser, cookie_dict_to_str
from ..translation import _
from .main_terminal import TikTok

if TYPE_CHECKING:
    from ..config import Parameter
    from ..manager import Database

__all__ = ["APIServer"]


def token_dependency(token: str = Header(None)):
    if not is_valid_token(token):
        raise HTTPException(
            status_code=403,
            detail=_("验证失败！"),
        )


class APIServer(TikTok):
    USER_FETCH_TIMEOUT = 20
    DEFAULT_SCHEDULE_TIMES = ("09:30", "15:30", "21:00")
    REFRESH_QUEUE_SIZE = 200
    REFRESH_CONCURRENCY = 2
    AUTO_PENDING_SCAN_LIMIT = 300
    MEDIA_PROBE_TIMEOUT = 8
    MEDIA_PROBE_CONCURRENCY = 2
    USER_ID_PATTERN = re.compile(r"^MS4wL[0-9A-Za-z_-]+$")
    USER_ID_SCAN_PATTERN = re.compile(r"MS4wL[0-9A-Za-z_-]+")
    STREAM_CACHE_MAX_ITEMS = 240
    STREAM_CACHE_MAX_BYTES = 2 * 1024 * 1024
    STREAM_CACHE_TTL_M3U8 = 5
    STREAM_CACHE_TTL_SEGMENT = 12
    STREAM_LIVE_PREFIX_TTL = 90
    CLIENT_FEED_WORK_TYPES = ("video", "note", "live")
    DOWNLOADABLE_WORK_TYPES = ("video", "note")
    USER_FULL_SYNC_PAGE_COUNT = 50
    USER_FULL_SYNC_MAX_PAGES = 500
    AUTO_COMPENSATE_INTERVAL_SECONDS = 120
    AUTO_ZOMBIE_TIMEOUT_MINUTES = 120
    AUTO_ZOMBIE_RESET_LIMIT = 500
    AUTO_FAILED_RETRY_INTERVAL_MINUTES = 15

    def __init__(
        self,
        parameter: "Parameter",
        database: "Database",
        server_mode: bool = True,
    ):
        super().__init__(
            parameter,
            database,
            server_mode,
        )
        self.server = None
        self._schedule_task = None
        self._schedule_last_key = ""
        self._douyin_live_cache = {}
        self._debug_account_dumped = set()
        self._refresh_queue = asyncio.Queue(maxsize=self.REFRESH_QUEUE_SIZE)
        self._refresh_workers = []
        self._refresh_pending = set()
        self._orphan_cleanup_at = None
        self._auto_compensate_at = None
        self._feed_subscribers = set()
        self._stream_cache = {}
        self._live_stream_prefixes = {}
        self._live_monitor_task = None
        self._live_refreshing = set()
        self._auto_downloading = set()
        self._user_full_syncing = set()
        self._user_full_sync_progress = {}
        self._auto_compensation_status = {
            "last_run_at": "",
            "last_force": False,
            "reset_count": 0,
            "users_total": 0,
            "users_pending": 0,
            "works_pending": 0,
            "users_processed": 0,
            "running_downloading": 0,
            "error": "",
        }
        self.live_recorder = DouyinLiveRecorder(parameter, database)

    @staticmethod
    def _hash_cookie(cookie: str) -> str:
        return sha256(cookie.encode("utf-8")).hexdigest()

    @staticmethod
    def _mask_cookie(cookie: str) -> str:
        if not cookie:
            return ""
        if len(cookie) <= 12:
            return "*" * len(cookie)
        return f"{cookie[:6]}...{cookie[-4:]}"

    @classmethod
    def _extract_first_url(cls, value) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            url_list = value.get("url_list")
            if isinstance(url_list, list) and url_list:
                return str(url_list[0])
            url = value.get("url")
            if isinstance(url, str):
                return url
        if isinstance(value, list) and value:
            return cls._extract_first_url(value[0])
        return ""

    @classmethod
    def _extract_author_profile(cls, item: dict) -> dict:
        author = item.get("author") if isinstance(item, dict) else None
        if not isinstance(author, dict):
            return {"uid": "", "nickname": "", "avatar": "", "cover": ""}
        avatar = ""
        for key in ("avatar_larger", "avatar_medium", "avatar_thumb"):
            avatar = cls._extract_first_url(author.get(key))
            if avatar:
                break
        return {
            "uid": author.get("uid") or author.get("id") or "",
            "nickname": author.get("nickname", ""),
            "avatar": avatar,
            "cover": cls._extract_first_url(author.get("cover_url")),
        }

    @classmethod
    def _extract_image_list(cls, item: dict) -> list:
        if not isinstance(item, dict):
            return []
        for key in ("images", "image_infos"):
            images = item.get(key)
            if isinstance(images, list):
                return images
        image_post_info = item.get("image_post_info")
        if isinstance(image_post_info, dict):
            images = image_post_info.get("images") or image_post_info.get("image_list")
            if isinstance(images, list):
                return images
        return []

    @classmethod
    def _extract_first_image_url(cls, item: dict) -> str:
        images = cls._extract_image_list(item)
        for image in images:
            if isinstance(image, dict):
                for key in ("url_list", "download_url_list", "download_url"):
                    url = cls._extract_first_url(image.get(key))
                    if url:
                        return url
            url = cls._extract_first_url(image)
            if url:
                return url
        return ""

    @classmethod
    def _extract_image_size(cls, item: dict) -> tuple[int, int]:
        images = cls._extract_image_list(item)
        for image in images:
            if not isinstance(image, dict):
                continue
            width = int(
                image.get("width")
                or image.get("image_width")
                or image.get("img_width")
                or 0
            )
            height = int(
                image.get("height")
                or image.get("image_height")
                or image.get("img_height")
                or 0
            )
            if width and height:
                return width, height
        return 0, 0

    @classmethod
    def _extract_work_cover(cls, item: dict) -> str:
        video = item.get("video") if isinstance(item, dict) else None
        if not isinstance(video, dict):
            return cls._extract_first_image_url(item)
        for key in ("cover", "origin_cover", "dynamic_cover"):
            url = cls._extract_first_url(video.get(key))
            if url:
                return url
        return cls._extract_first_image_url(item)

    @staticmethod
    def _extract_play_count(item: dict) -> int:
        stats = item.get("statistics") if isinstance(item, dict) else None
        if not isinstance(stats, dict):
            return 0
        value = stats.get("play_count") or stats.get("playCount") or 0
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _extract_video_size(cls, video: dict) -> tuple[int, int]:
        if not isinstance(video, dict):
            return 0, 0
        width = int(video.get("width") or 0)
        height = int(video.get("height") or 0)
        if width and height:
            return width, height
        bit_rate = video.get("bit_rate")
        if isinstance(bit_rate, list) and bit_rate:
            sizes = []
            for item in bit_rate:
                if not isinstance(item, dict):
                    continue
                play_addr = item.get("play_addr")
                if not isinstance(play_addr, dict):
                    continue
                size_w = int(play_addr.get("width") or 0)
                size_h = int(play_addr.get("height") or 0)
                if size_w and size_h:
                    sizes.append((max(size_w, size_h), size_w, size_h))
            if sizes:
                sizes.sort(key=lambda x: x[0])
                return sizes[-1][1], sizes[-1][2]
        for key in ("play_addr", "play_addr_h264", "play_addr_bytevc1"):
            value = video.get(key)
            if not isinstance(value, dict):
                continue
            size_w = int(value.get("width") or 0)
            size_h = int(value.get("height") or 0)
            if size_w and size_h:
                return size_w, size_h
        return 0, 0

    @classmethod
    def _extract_work_play_url(cls, item: dict) -> str:
        video = item.get("video") if isinstance(item, dict) else None
        if not isinstance(video, dict):
            return ""
        bit_rate = video.get("bit_rate")
        if isinstance(bit_rate, list) and bit_rate:
            try:
                items = []
                for item in bit_rate:
                    if not isinstance(item, dict):
                        continue
                    play_addr = (
                        item.get("play_addr")
                        if isinstance(item.get("play_addr"), dict)
                        else {}
                    )
                    url_list = play_addr.get("url_list") or []
                    items.append(
                        (
                            int(item.get("FPS") or 0),
                            int(item.get("bit_rate") or 0),
                            int(play_addr.get("data_size") or 0),
                            int(play_addr.get("height") or 0),
                            int(play_addr.get("width") or 0),
                            url_list,
                        )
                    )
                items.sort(
                    key=lambda x: (
                        max(x[3], x[4]),
                        x[0],
                        x[1],
                        x[2],
                    )
                )
                if items:
                    url_list = items[-1][-1]
                    if isinstance(url_list, list) and url_list:
                        return str(url_list[VIDEO_INDEX])
            except Exception:
                pass
        for key in ("play_addr", "play_addr_h264", "play_addr_bytevc1", "play_url"):
            url = cls._extract_first_url(video.get(key))
            if url:
                return url
        return ""

    @classmethod
    def _is_video_item(cls, item: dict) -> bool:
        if not isinstance(item, dict):
            return False
        if (
            item.get("images")
            or item.get("image_infos")
            or item.get("image_post_info")
            or item.get("is_image")
        ):
            return False
        video = item.get("video")
        if not isinstance(video, dict):
            return False
        if cls._extract_first_url(video.get("play_addr")):
            return True
        if cls._extract_first_url(video.get("play_addr_h264")):
            return True
        if cls._extract_first_url(video.get("play_addr_bytevc1")):
            return True
        if cls._extract_first_url(video.get("play_url")):
            return True
        return False

    @classmethod
    def _is_note_item(cls, item: dict) -> bool:
        if not isinstance(item, dict):
            return False
        return bool(
            item.get("images")
            or item.get("image_infos")
            or item.get("image_post_info")
            or item.get("is_image")
        )

    @classmethod
    def _is_work_item(cls, item: dict) -> bool:
        return cls._is_video_item(item) or cls._is_note_item(item)

    @classmethod
    def _normalize_input_url(cls, value: str) -> str:
        if not value:
            return ""
        if value.startswith(("http://", "https://")):
            return value
        prefixes = (
            "www.douyin.com/",
            "douyin.com/",
            "live.douyin.com/",
            "www.iesdouyin.com/",
            "iesdouyin.com/",
            "webcast.amemv.com/",
        )
        if value.startswith(prefixes):
            return f"https://{value}"
        return value

    def _extract_sec_user_id_from_live(self, data: dict) -> str:
        if not isinstance(data, dict):
            return ""
        try:
            obj = self.extractor.generate_data_object(data)
        except Exception:
            return ""
        candidates = (
            "data.data[0].owner.sec_uid",
            "data.data[0].owner.secUid",
            "data.data[0].owner.sec_user_id",
            "data.room.owner.sec_uid",
            "data.room.owner.secUid",
            "data.room.owner.sec_user_id",
            "data.owner.sec_uid",
            "data.owner.secUid",
        )
        for path in candidates:
            value = self.extractor.safe_extract(obj, path, "")
            if value:
                return str(value)
        return ""

    @staticmethod
    def _extract_sec_user_id_from_detail_data(data: dict) -> str:
        if not isinstance(data, dict):
            return ""
        if "aweme_detail" in data and isinstance(data.get("aweme_detail"), dict):
            data = data.get("aweme_detail") or {}
        if "aweme_detail_list" in data:
            detail_list = data.get("aweme_detail_list") or []
            if detail_list and isinstance(detail_list[0], dict):
                data = detail_list[0]
        author = data.get("author") if isinstance(data.get("author"), dict) else {}
        sec_user_id = (
            author.get("sec_uid")
            or author.get("secUid")
            or author.get("sec_user_id")
            or ""
        )
        return str(sec_user_id) if sec_user_id else ""

    async def _resolve_sec_user_id_from_detail(self, detail_id: str) -> str:
        if not detail_id:
            return ""
        try:
            data = await self.handle_detail_single(
                DetailFetcher,
                "",
                None,
                detail_id,
            )
        except Exception:
            return ""
        return self._extract_sec_user_id_from_detail_data(data)

    async def _resolve_sec_user_id(self, value: str) -> str:
        text = (value or "").strip()
        if not text:
            return ""
        if self.USER_ID_PATTERN.match(text):
            return text
        if "MS4wL" in text:
            match = self.USER_ID_SCAN_PATTERN.search(text)
            if match:
                return match.group(0)
        normalized = self._normalize_input_url(text)
        try:
            links = await self.links.run(normalized, "user")
        except Exception:
            links = []
        if links:
            return links[0]
        try:
            detail_ids = await self.links.run(normalized, "detail")
        except Exception:
            detail_ids = []
        for detail_id in detail_ids:
            sec_user_id = await self._resolve_sec_user_id_from_detail(detail_id)
            if sec_user_id:
                return sec_user_id
        try:
            live_ids = await self.links.run(normalized, type_="live")
        except Exception:
            live_ids = []
        for web_rid in live_ids:
            try:
                live_data = await self.get_live_data(web_rid=web_rid)
            except Exception:
                continue
            sec_user_id = self._extract_sec_user_id_from_live(live_data)
            if sec_user_id:
                return sec_user_id
        return ""

    def _debug_dump_account_data(self, sec_user_id: str, data: list[dict]) -> None:
        if not sec_user_id or not data:
            return
        if sec_user_id in self._debug_account_dumped:
            return
        self._debug_account_dumped.add(sec_user_id)
        cache_dir = Path(__file__).resolve().parent.parent.parent.joinpath("Cache")
        cache_dir.mkdir(exist_ok=True)
        payload = {
            "sec_user_id": sec_user_id,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(data),
            "sample": data[0],
        }
        path = cache_dir.joinpath("admin_account_sample.json")
        try:
            path.write_text(
                json.dumps(payload, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
        except OSError:
            return

    @staticmethod
    def _normalize_user_row(row: dict) -> dict:
        if not row:
            return {}
        return {
            "id": row.get("id", 0),
            "sec_user_id": row.get("sec_user_id", ""),
            "uid": row.get("uid", ""),
            "nickname": row.get("nickname", ""),
            "avatar": row.get("avatar", ""),
            "cover": row.get("cover", ""),
            "has_works": bool(row.get("has_works", 0)),
            "status": row.get("status", "unknown"),
            "is_live": bool(row.get("is_live", 0)),
            "has_new_today": bool(row.get("has_new_today", 0)),
            "auto_update": bool(row.get("auto_update", 0)),
            "update_window_start": row.get("update_window_start", ""),
            "update_window_end": row.get("update_window_end", ""),
            "last_live_at": row.get("last_live_at", ""),
            "last_new_at": row.get("last_new_at", ""),
            "last_fetch_at": row.get("last_fetch_at", ""),
            "created_at": row.get("created_at", ""),
            "updated_at": row.get("updated_at", ""),
            "next_auto_update_at": row.get("next_auto_update_at", ""),
        }

    def _normalize_cookie_row(self, row: dict) -> dict:
        if not row:
            return {}
        return {
            "id": row.get("id", 0),
            "account": row.get("account", ""),
            "cookie_masked": self._mask_cookie(row.get("cookie", "")),
            "status": row.get("status", "active"),
            "fail_count": row.get("fail_count", 0),
            "last_used_at": row.get("last_used_at", ""),
            "last_failed_at": row.get("last_failed_at", ""),
            "created_at": row.get("created_at", ""),
            "updated_at": row.get("updated_at", ""),
        }

    @staticmethod
    def _read_clipboard_cookie() -> str:
        try:
            return (paste() or "").strip()
        except Exception:
            return ""

    def _read_browser_cookie(self, browser: str) -> dict[str, str]:
        reader = Browser(self.parameter, self.parameter.cookie_object)
        return reader.get(browser, Browser.PLATFORM[False].domain)

    async def _save_douyin_cookie(self, account: str, cookie: str) -> DouyinCookie:
        cookie_value = (cookie or "").strip()
        if not Cookie.validate_cookie_minimal(cookie_value):
            raise HTTPException(status_code=400, detail=_("Cookie 格式无效"))
        record = await self.database.upsert_douyin_cookie(
            account,
            cookie_value,
            self._hash_cookie(cookie_value),
        )
        return DouyinCookie(**self._normalize_cookie_row(record))

    @staticmethod
    def _format_timestamp(ts: int) -> str:
        if not ts:
            return ""
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _today_str() -> str:
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _parse_time(value: str) -> time | None:
        if not value:
            return None
        parts = value.split(":", 1)
        if len(parts) != 2:
            return None
        try:
            return time(int(parts[0]), int(parts[1]))
        except ValueError:
            return None

    @classmethod
    def _within_window(cls, start: str, end: str, now_time: time) -> bool:
        start_time = cls._parse_time(start)
        end_time = cls._parse_time(end)
        if not start_time or not end_time:
            return True
        if start_time <= end_time:
            return start_time <= now_time <= end_time
        return now_time >= start_time or now_time <= end_time

    @staticmethod
    def _format_schedule_times(times: list[str]) -> str:
        if not times:
            return ""
        return ", ".join(times)

    @classmethod
    def _normalize_schedule_text(cls, value: str) -> str:
        if not value:
            return ""
        text = value.strip()
        text = text.replace("，", ",")
        text = re.sub(r"\s*-\s*", "-", text)
        text = re.sub(r"\s*/\s*", "/", text)
        text = re.sub(
            r"(\d{1,2}:\d{2}-\d{1,2}:\d{2})\s*每", r"\1每", text
        )
        return text

    @classmethod
    def _parse_schedule_times_text(cls, value: str) -> list[str]:
        text = cls._normalize_schedule_text(value)
        if not text:
            return []
        parts = [p for p in re.split(r"[,\s]+", text) if p]
        times = []
        seen = set()
        for part in parts:
            range_match = re.match(
                r"^(\d{1,2}:\d{2})-(\d{1,2}:\d{2})(?:/(\d+))?$", part
            )
            interval = None
            if range_match:
                start_raw, end_raw, interval_raw = range_match.groups()
                interval = int(interval_raw) if interval_raw else 1
            else:
                range_match = re.match(
                    r"^(\d{1,2}:\d{2})-(\d{1,2}:\d{2})每(\d+)?小时$",
                    part,
                )
                if range_match:
                    start_raw, end_raw, interval_raw = range_match.groups()
                    interval = int(interval_raw) if interval_raw else 1
            if range_match:
                start_time = cls._parse_time(start_raw)
                end_time = cls._parse_time(end_raw)
                if not start_time or not end_time:
                    continue
                if interval <= 0:
                    continue
                if start_time > end_time:
                    continue
                start_minutes = start_time.hour * 60 + start_time.minute
                end_minutes = end_time.hour * 60 + end_time.minute
                step = interval * 60
                current = start_minutes
                while current <= end_minutes:
                    key = f"{current // 60:02d}:{current % 60:02d}"
                    if key not in seen:
                        seen.add(key)
                        times.append(key)
                    current += step
                continue
            time_match = re.match(r"^(\d{1,2})\s*:\s*(\d{2})$", part)
            if not time_match:
                continue
            hour_str, minute_str = time_match.groups()
            try:
                hour = int(hour_str)
                minute = int(minute_str)
            except ValueError:
                continue
            if hour < 0 or hour > 23 or minute < 0 or minute > 59:
                continue
            key = f"{hour:02d}:{minute:02d}"
            if key in seen:
                continue
            seen.add(key)
            times.append(key)
        times.sort(key=lambda item: (int(item.split(":")[0]), int(item.split(":")[1])))
        return times

    @staticmethod
    def _build_schedule_expression(times: list[str]) -> str:
        if not times:
            return ""
        groups: dict[int, list[int]] = {}
        for item in times:
            hour_str, minute_str = item.split(":", 1)
            groups.setdefault(int(minute_str), []).append(int(hour_str))
        parts = []
        for minute in sorted(groups):
            hours = ",".join(str(h) for h in sorted(set(groups[minute])))
            parts.append(f"{minute} {hours} * * *")
        return " | ".join(parts)

    def _resolve_schedule_setting(self, data: dict) -> dict:
        enabled = True if not data else bool(data.get("enabled", 1))
        raw_text = self._normalize_schedule_text(data.get("times_text") or "")
        times = self._parse_schedule_times_text(raw_text)
        if not times:
            times = list(self.DEFAULT_SCHEDULE_TIMES)
            times_text = self._format_schedule_times(times)
        else:
            times_text = raw_text if raw_text else self._format_schedule_times(times)
        expression = self._build_schedule_expression(times)
        return {
            "enabled": enabled,
            "times": times,
            "times_text": times_text,
            "expression": expression,
        }

    async def _compute_next_auto_update_at(self, row: dict) -> str:
        if not row:
            return ""
        setting = self._resolve_schedule_setting(
            await self.database.get_douyin_schedule()
        )
        if not setting.get("enabled"):
            return "已停用"
        times = setting.get("times") or []
        if not times:
            return "-"
        now = datetime.now()
        now_floor = now.replace(second=0, microsecond=0)
        start = row.get("update_window_start", "")
        end = row.get("update_window_end", "")
        for day_offset in range(2):
            day = now.date() + timedelta(days=day_offset)
            for time_str in times:
                time_obj = self._parse_time(time_str)
                if not time_obj:
                    continue
                candidate = datetime.combine(day, time_obj)
                if day_offset == 0 and candidate < now_floor:
                    continue
                if not self._within_window(start, end, candidate.time()):
                    continue
                return candidate.strftime("%Y-%m-%d %H:%M:%S")
        return "-"

    @classmethod
    def _extract_work_brief(cls, item: dict, fallback_sec_user_id: str) -> dict:
        aweme_id = item.get("aweme_id", "")
        desc = item.get("desc", "") or aweme_id
        create_ts = int(item.get("create_time") or 0)
        create_date = (
            datetime.fromtimestamp(create_ts).strftime("%Y-%m-%d")
            if create_ts
            else ""
        )
        author = item.get("author") or {}
        work_type = "note" if cls._is_note_item(item) else "video"
        video = item.get("video") if isinstance(item, dict) else None
        if work_type == "note":
            width, height = cls._extract_image_size(item)
            play_url = ""
        else:
            width, height = cls._extract_video_size(video)
            play_url = cls._extract_work_play_url(item)
        return {
            "type": work_type,
            "sec_user_id": author.get("sec_uid", "") or fallback_sec_user_id,
            "aweme_id": aweme_id,
            "desc": desc,
            "create_ts": create_ts,
            "create_time": cls._format_timestamp(create_ts),
            "create_date": create_date,
            "nickname": author.get("nickname", ""),
            "cover": cls._extract_work_cover(item),
            "play_count": cls._extract_play_count(item),
            "width": width,
            "height": height,
            "play_url": play_url,
        }

    def _build_work_from_row(self, row: dict) -> DouyinWork:
        create_ts = int(row.get("create_ts") or 0)
        return DouyinWork(
            type=row.get("work_type") or "video",
            sec_user_id=row.get("sec_user_id", ""),
            aweme_id=row.get("aweme_id", ""),
            desc=row.get("desc", ""),
            create_ts=create_ts,
            create_time=self._format_timestamp(create_ts),
            create_date=row.get("create_date", ""),
            nickname=row.get("nickname", ""),
            cover=row.get("cover", ""),
            play_count=int(row.get("play_count") or 0),
            width=int(row.get("width") or 0),
            height=int(row.get("height") or 0),
            upload_status=row.get("upload_status") or "pending",
            upload_provider=row.get("upload_provider") or "",
            upload_destination=row.get("upload_destination") or "",
            upload_origin_destination=row.get("upload_origin_destination") or "",
            upload_message=row.get("upload_message") or "",
            local_path=row.get("local_path") or "",
            downloaded_at=row.get("downloaded_at") or "",
            uploaded_at=row.get("uploaded_at") or "",
        )

    @staticmethod
    def _parse_datetime_ts(value: str) -> int:
        if not value:
            return 0
        try:
            return int(datetime.strptime(value, "%Y-%m-%d %H:%M:%S").timestamp())
        except ValueError:
            return 0

    @classmethod
    def _is_status_stale(cls, status_updated_at: str, timeout_minutes: int) -> bool:
        timeout_minutes = int(timeout_minutes or 0)
        if timeout_minutes <= 0:
            return True
        status_ts = cls._parse_datetime_ts(status_updated_at)
        if not status_ts:
            return True
        return (datetime.now().timestamp() - status_ts) >= timeout_minutes * 60

    @staticmethod
    def _resolve_client_ip(request: Request | None) -> str:
        if not request:
            return ""
        forwarded = str(request.headers.get("x-forwarded-for", "") or "").strip()
        if forwarded:
            first = forwarded.split(",")[0].strip()
            if first:
                return first
        if not request.client:
            return ""
        return str(request.client.host or "").strip()

    @staticmethod
    def _is_lan_ip(value: str) -> bool:
        if not value:
            return False
        try:
            ip = ip_address(value)
        except ValueError:
            return False
        return bool(ip.is_private or ip.is_loopback or ip.is_link_local)

    @staticmethod
    def _build_live_url(web_rid: str, room_id: str) -> str:
        base = web_rid or room_id
        if not base:
            return ""
        params = {
            "action_type": "click",
            "enter_from_merge": "web_others_homepage",
            "enter_method": "web_homepage_head",
            "enter_method_temai": "web_video_head",
            "group_id": "undefined",
            "is_livehead_preview_mini_window_show": "",
            "is_replaced_live": "0",
            "live_position": "undefined",
            "mini_window_show_type": "",
            "request_id": "undefined",
            "room_id": room_id or "undefined",
            "search_tab": "undefined",
            "web_card_rank": "",
            "web_live_page": "",
        }
        return f"https://live.douyin.com/{base}?{urlencode(params)}"

    def _build_work_feed_item(self, row: dict) -> tuple[int, DouyinClientFeedItem]:
        create_ts = int(row.get("create_ts") or 0)
        aweme_id = row.get("aweme_id", "")
        work_type = row.get("work_type") or "video"
        if work_type == "live":
            feed_type = "live_record"
            share_url = ""
        elif work_type == "note":
            feed_type = "note"
            share_url = f"https://www.douyin.com/note/{aweme_id}" if aweme_id else ""
        else:
            feed_type = "video"
            share_url = f"https://www.douyin.com/video/{aweme_id}" if aweme_id else ""
        item = DouyinClientFeedItem(
            type=feed_type,
            sec_user_id=row.get("sec_user_id") or "",
            uid=row.get("uid") or "",
            nickname=row.get("nickname") or "",
            avatar=row.get("avatar") or "",
            title=row.get("desc", "") or aweme_id,
            cover=row.get("cover", ""),
            sort_time=self._format_timestamp(create_ts),
            aweme_id=aweme_id,
            play_count=int(row.get("play_count") or 0),
            video_url=share_url,
            width=int(row.get("width") or 0),
            height=int(row.get("height") or 0),
        )
        return create_ts, item

    def _build_live_feed_item(self, row: dict) -> tuple[int, DouyinClientFeedItem]:
        sec_user_id = row.get("sec_user_id", "")
        live_info = self._get_cached_live_info(sec_user_id) or {}
        room = live_info.get("room") if isinstance(live_info, dict) else None
        room = room if isinstance(room, dict) else {}
        web_rid = live_info.get("web_rid", "") if isinstance(live_info, dict) else ""
        room_id = live_info.get("room_id", "") if isinstance(live_info, dict) else ""
        cover = room.get("cover") or row.get("cover", "")
        title = room.get("title") or live_info.get("title", "") or "直播中"
        live_width = int(room.get("width") or row.get("live_width") or 0)
        live_height = int(room.get("height") or row.get("live_height") or 0)
        item = DouyinClientFeedItem(
            type="live",
            sec_user_id=sec_user_id,
            uid=row.get("uid") or "",
            nickname=row.get("nickname") or "",
            avatar=row.get("avatar") or "",
            title=title,
            cover=cover,
            sort_time=row.get("last_live_at", ""),
            room_id=str(room_id) if room_id else "",
            web_rid=str(web_rid) if web_rid else "",
            live_url=self._build_live_url(str(web_rid), str(room_id)),
            last_live_at=row.get("last_live_at", ""),
            flv_pull_url=room.get("flv_pull_url") or {},
            hls_pull_url_map=room.get("hls_pull_url_map") or {},
            width=live_width,
            height=live_height,
        )
        sort_ts = self._parse_datetime_ts(row.get("last_live_at", ""))
        return sort_ts, item

    @staticmethod
    def _unwrap_detail_data(data: dict) -> dict:
        if not isinstance(data, dict):
            return {}
        if "aweme_detail" in data and isinstance(data.get("aweme_detail"), dict):
            return data.get("aweme_detail") or {}
        if "aweme_detail_list" in data:
            detail_list = data.get("aweme_detail_list") or []
            if detail_list and isinstance(detail_list[0], dict):
                return detail_list[0]
        return data

    @classmethod
    def _extract_detail_cover(cls, data: dict) -> str:
        detail = cls._unwrap_detail_data(data)
        video = detail.get("video") if isinstance(detail, dict) else None
        if not isinstance(video, dict):
            return cls._extract_first_image_url(detail)
        for key in ("cover", "origin_cover", "dynamic_cover"):
            url = cls._extract_first_url(video.get(key))
            if url:
                return url
        return cls._extract_first_image_url(detail)

    @classmethod
    def _extract_detail_video_url(cls, data: dict) -> str:
        detail = cls._unwrap_detail_data(data)
        video = detail.get("video") if isinstance(detail, dict) else None
        if not isinstance(video, dict):
            return ""
        bit_rate = video.get("bit_rate")
        if isinstance(bit_rate, list) and bit_rate:
            try:
                items = []
                for item in bit_rate:
                    if not isinstance(item, dict):
                        continue
                    play_addr = item.get("play_addr") if isinstance(item.get("play_addr"), dict) else {}
                    url_list = play_addr.get("url_list") or []
                    items.append(
                        (
                            int(item.get("FPS") or 0),
                            int(item.get("bit_rate") or 0),
                            int(play_addr.get("data_size") or 0),
                            int(play_addr.get("height") or 0),
                            int(play_addr.get("width") or 0),
                            url_list,
                        )
                    )
                items.sort(
                    key=lambda x: (
                        max(x[3], x[4]),
                        x[0],
                        x[1],
                        x[2],
                    )
                )
                if items:
                    url_list = items[-1][-1]
                    if isinstance(url_list, list) and url_list:
                        return str(url_list[VIDEO_INDEX])
            except Exception:
                pass
        for key in ("play_addr", "play_addr_h264", "play_addr_bytevc1"):
            url = cls._extract_first_url(video.get(key))
            if url:
                return url
        return ""

    @classmethod
    def _extract_detail_audio_url(cls, data: dict) -> str:
        detail = cls._unwrap_detail_data(data)
        if not isinstance(detail, dict):
            return ""
        for key in ("music", "music_info"):
            music = detail.get(key)
            if not isinstance(music, dict):
                continue
            for play_key in ("play_url", "playUrl", "play_url_h264", "play_url_bytevc1"):
                url = cls._extract_first_url(music.get(play_key))
                if url:
                    return url
        return ""

    @classmethod
    def _extract_detail_size(cls, data: dict) -> tuple[int, int]:
        detail = cls._unwrap_detail_data(data)
        video = detail.get("video") if isinstance(detail, dict) else None
        width, height = cls._extract_video_size(video)
        if width and height:
            return width, height
        return cls._extract_image_size(detail)

    @staticmethod
    def _normalize_detail_url(value: str) -> str:
        return str(value or "").strip()

    def _upload_channel_enabled(self) -> bool:
        upload = self.parameter.upload if isinstance(self.parameter.upload, dict) else {}
        if not upload.get("enabled"):
            return False
        webdav = upload.get("webdav", {})
        if not isinstance(webdav, dict):
            return False
        return bool(webdav.get("enabled") and str(webdav.get("base_url", "")).strip())

    @staticmethod
    def _build_local_stream_source_url(aweme_id: str) -> str:
        return f"/client/douyin/local-stream?aweme_id={quote(str(aweme_id or ''), safe='')}"

    @staticmethod
    def _is_path_within(path: Path, parent: Path) -> bool:
        try:
            path.relative_to(parent)
            return True
        except Exception:
            return False

    def _resolve_local_cache_path(self, local_path: str) -> Path | None:
        text = str(local_path or "").strip()
        if not text:
            return None
        root = self.parameter.root.expanduser().resolve()
        project_root_raw = getattr(self.parameter, "ROOT", root)
        project_root = (
            project_root_raw.expanduser().resolve()
            if isinstance(project_root_raw, Path)
            else root
        )
        volume_root = (
            root if root.name.lower() == "volume" else project_root.joinpath("Volume")
        )
        allow_roots = [root, project_root, volume_root]
        if root.name.lower() == "volume":
            allow_roots.append(root.parent)

        normalized = text.replace("\\", "/")
        candidates: list[Path] = []
        raw = Path(text).expanduser()
        if raw.is_absolute():
            candidates.append(raw)
        else:
            candidates.append(root.joinpath(raw))
            candidates.append(project_root.joinpath(raw))

        def append_relative(base: Path, relative_text: str) -> None:
            relative_text = relative_text.strip("/")
            if not relative_text:
                return
            candidates.append(base.joinpath(Path(relative_text)))

        if normalized.startswith("/app/Volume/"):
            relative = normalized.split("/app/Volume/", 1)[1]
            append_relative(volume_root, relative)
            append_relative(root, relative)
        elif normalized.startswith("/app/"):
            relative = normalized.split("/app/", 1)[1]
            append_relative(project_root, relative)
            append_relative(root, relative)

        if "/Volume/" in normalized:
            relative = normalized.split("/Volume/", 1)[1]
            append_relative(volume_root, relative)

        seen: set[str] = set()
        for candidate in candidates:
            try:
                target = candidate.resolve()
            except Exception:
                continue
            key = str(target)
            if key in seen:
                continue
            seen.add(key)
            if not target.is_file():
                continue
            if any(self._is_path_within(target, base) for base in allow_roots):
                return target
        return None

    async def _resolve_work_local_file(self, aweme_id: str, work_row: dict) -> Path | None:
        raw_local_path = str((work_row or {}).get("local_path", "")).strip()
        local_file = self._resolve_local_cache_path(raw_local_path)
        if local_file:
            return local_file
        work_type = str((work_row or {}).get("work_type", "")).strip().lower()
        is_live = work_type == "live" or str(aweme_id).startswith("live_")
        if not is_live:
            return None
        fallback_path = await self.database.get_latest_douyin_live_record_output(aweme_id)
        if not fallback_path:
            return None
        local_file = self._resolve_local_cache_path(fallback_path)
        if not local_file:
            return None
        work_row["local_path"] = str(local_file)
        await self.database.set_douyin_work_local_path(aweme_id, str(local_file))
        return local_file

    @classmethod
    def _build_detail_video_sources(
        cls,
        douyin_url: str,
        uploaded_url: str,
        uploaded_origin_url: str,
        local_cache_url: str,
        prefer_origin: bool,
        include_upload_sources: bool = True,
    ) -> tuple[list[dict], str]:
        sources: list[dict] = []
        seen: set[str] = set()

        def add_source(source_id: str, label: str, url: str) -> None:
            target = cls._normalize_detail_url(url)
            if not target:
                return
            key = target.lower()
            if key in seen:
                return
            seen.add(key)
            source = {
                "id": source_id,
                "label": label,
                "url": target,
            }
            if source_id == "nas_origin":
                source["need_auth"] = True
            sources.append(source)

        add_source("local_cache", "本地缓存", local_cache_url)

        if include_upload_sources:
            upload_candidates = (
                (
                    ("nas_origin", "NAS(局域网)", uploaded_origin_url),
                    ("nas_proxy", "NAS(代理)", uploaded_url),
                )
                if prefer_origin
                else (
                    ("nas_proxy", "NAS(代理)", uploaded_url),
                    ("nas_origin", "NAS(局域网)", uploaded_origin_url),
                )
            )
            for source_id, label, url in upload_candidates:
                add_source(source_id, label, url)

        add_source("douyin", "抖音", douyin_url)
        default_source = sources[0]["id"] if sources else ""
        return sources, default_source

    @staticmethod
    def _is_m3u8_resource(url: str, content_type: str = "") -> bool:
        if content_type and "mpegurl" in content_type.lower():
            return True
        return url.lower().split("?")[0].endswith(".m3u8")

    @staticmethod
    def _proxy_stream_url(url: str, live: bool = False) -> str:
        path = "/client/douyin/stream-live" if live else "/client/douyin/stream"
        return f"{path}?url={quote(url, safe='')}"

    @staticmethod
    def _parse_range_length(range_header: str | None) -> int:
        if not range_header:
            return 0
        match = re.match(r"bytes=(\d+)-(\d+)?", range_header.strip())
        if not match:
            return 0
        start = int(match.group(1) or 0)
        end = match.group(2)
        if end is None:
            return 0
        try:
            end_value = int(end)
        except ValueError:
            return 0
        if end_value < start:
            return 0
        return end_value - start + 1

    @staticmethod
    def _build_stream_cache_key(url: str, range_header: str | None) -> str:
        return f"{url}|{range_header or ''}"

    @staticmethod
    def _normalize_stream_prefix(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        base_path = parsed.path.rsplit("/", 1)[0]
        return f"{parsed.scheme}://{parsed.netloc}{base_path}/"

    def _mark_live_prefix(self, url: str) -> None:
        prefix = self._normalize_stream_prefix(url)
        if not prefix:
            return
        self._live_stream_prefixes[prefix] = time_module.monotonic() + self.STREAM_LIVE_PREFIX_TTL

    def _is_live_prefix(self, url: str) -> bool:
        if not self._live_stream_prefixes:
            return False
        now = time_module.monotonic()
        expired = [key for key, value in self._live_stream_prefixes.items() if value <= now]
        for key in expired:
            self._live_stream_prefixes.pop(key, None)
        for prefix in self._live_stream_prefixes.keys():
            if url.startswith(prefix):
                return True
        return False

    @staticmethod
    def _is_live_playlist(text: str) -> bool:
        if not text:
            return True
        upper = text.upper()
        if "#EXT-X-ENDLIST" in upper:
            return False
        if "#EXT-X-PLAYLIST-TYPE:VOD" in upper:
            return False
        return True

    def _get_stream_cache(self, key: str) -> dict | None:
        item = self._stream_cache.get(key)
        if not item:
            return None
        if item.get("expires_at", 0) <= time_module.monotonic():
            self._stream_cache.pop(key, None)
            return None
        return item

    def _set_stream_cache(
        self,
        key: str,
        body: bytes,
        content_type: str,
        headers: dict,
        status_code: int,
        ttl_seconds: int,
    ) -> None:
        if not body or len(body) > self.STREAM_CACHE_MAX_BYTES:
            return
        now = time_module.monotonic()
        self._stream_cache[key] = {
            "expires_at": now + ttl_seconds,
            "stored_at": now,
            "body": body,
            "content_type": content_type,
            "headers": headers,
            "status_code": status_code,
        }
        self._prune_stream_cache()

    def _prune_stream_cache(self) -> None:
        now = time_module.monotonic()
        expired = [key for key, item in self._stream_cache.items() if item["expires_at"] <= now]
        for key in expired:
            self._stream_cache.pop(key, None)
        if len(self._stream_cache) <= self.STREAM_CACHE_MAX_ITEMS:
            return
        items = sorted(
            self._stream_cache.items(),
            key=lambda pair: pair[1].get("stored_at", 0),
        )
        excess = len(items) - self.STREAM_CACHE_MAX_ITEMS
        for index in range(excess):
            self._stream_cache.pop(items[index][0], None)

    def _should_cache_stream(
        self,
        url: str,
        content_type: str,
        range_header: str | None,
        content_length: str | None,
    ) -> bool:
        if self._is_live_prefix(url):
            return False
        if self._is_m3u8_resource(url, content_type):
            return False
        if range_header:
            requested = self._parse_range_length(range_header)
            return 0 < requested <= self.STREAM_CACHE_MAX_BYTES
        if content_length:
            try:
                length = int(content_length)
            except (TypeError, ValueError):
                length = 0
            if 0 < length <= self.STREAM_CACHE_MAX_BYTES:
                return True
        return False

    @classmethod
    def _rewrite_m3u8(cls, content: str, base_url: str, live: bool = False) -> str:
        if not content:
            return ""
        lines = []
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped:
                lines.append("")
                continue
            if stripped.startswith("#"):
                if "URI=" in stripped:
                    line = re.sub(
                        r'URI="([^"]+)"',
                        lambda match: f'URI="{cls._proxy_stream_url(urljoin(base_url, match.group(1)), live)}"',
                        line,
                    )
                    line = re.sub(
                        r"URI='([^']+)'",
                        lambda match: f"URI='{cls._proxy_stream_url(urljoin(base_url, match.group(1)), live)}'",
                        line,
                    )
                lines.append(line)
                continue
            lines.append(cls._proxy_stream_url(urljoin(base_url, stripped), live))
        return "\n".join(lines)

    def _build_stream_headers(self, url: str, range_header: str | None) -> dict:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if "live.douyin.com" in host:
            origin = "https://live.douyin.com"
            referer = "https://live.douyin.com/"
        else:
            origin = "https://www.douyin.com"
            referer = "https://www.douyin.com/"
        headers = {
            "User-Agent": self.parameter.headers.get("User-Agent", ""),
            "Referer": referer,
            "Origin": origin,
        }
        if range_header:
            headers["Range"] = range_header
        return headers

    def _build_probe_headers(self, url: str) -> str:
        headers = self._build_stream_headers(url, None)
        lines = [f"{key}: {value}" for key, value in headers.items() if value]
        if not lines:
            return ""
        return "\r\n".join(lines) + "\r\n"

    @staticmethod
    def _parse_probe_size(stdout: bytes | str) -> tuple[int, int]:
        text = (
            stdout.decode("utf-8", errors="ignore")
            if isinstance(stdout, bytes)
            else str(stdout or "")
        ).strip()
        match = re.search(r"(\d+)x(\d+)", text)
        if not match:
            return 0, 0
        return int(match.group(1)), int(match.group(2))

    async def _probe_media_size(self, url: str) -> tuple[int, int]:
        if not url:
            return 0, 0
        ffprobe_path = which("ffprobe")
        if not ffprobe_path:
            return 0, 0
        header_text = self._build_probe_headers(url)
        user_agent = self.parameter.headers.get("User-Agent", "")
        command = [
            ffprobe_path,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0:s=x",
        ]
        if header_text:
            command += ["-headers", header_text]
        if user_agent:
            command += ["-user_agent", user_agent]
        command += [
            "-rw_timeout",
            str(self.MEDIA_PROBE_TIMEOUT * 1_000_000),
            url,
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=PIPE,
                stderr=PIPE,
            )
        except OSError:
            return 0, 0
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(),
                timeout=self.MEDIA_PROBE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            process.kill()
            with suppress(Exception):
                await process.wait()
            return 0, 0
        return self._parse_probe_size(stdout)

    async def _probe_local_media_size(self, file_path: Path) -> tuple[int, int]:
        if not isinstance(file_path, Path) or not file_path.is_file():
            return 0, 0
        ffprobe_path = which("ffprobe")
        if not ffprobe_path:
            return 0, 0
        command = [
            ffprobe_path,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height",
            "-of",
            "csv=p=0:s=x",
            str(file_path),
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=PIPE,
                stderr=PIPE,
            )
        except OSError:
            return 0, 0
        try:
            stdout, _ = await asyncio.wait_for(
                process.communicate(),
                timeout=self.MEDIA_PROBE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            process.kill()
            with suppress(Exception):
                await process.wait()
            return 0, 0
        return self._parse_probe_size(stdout)

    @staticmethod
    def _pick_live_stream_url(room: dict) -> str:
        if not isinstance(room, dict):
            return ""
        hls_map = room.get("hls_pull_url_map") or {}
        flv_map = room.get("flv_pull_url") or {}
        hls_values = hls_map.values() if isinstance(hls_map, dict) else []
        flv_values = flv_map.values() if isinstance(flv_map, dict) else []
        for value in hls_values:
            if value:
                return str(value)
        for value in flv_values:
            if value:
                return str(value)
        return ""

    async def _fetch_douyin_detail(
        self,
        detail_id: str,
        cookie: str,
        proxy: str = None,
    ) -> dict:
        return await self.handle_detail_single(
            DetailFetcher,
            cookie,
            proxy,
            detail_id,
        )

    async def _fetch_douyin_detail_with_pool(
        self,
        detail_id: str,
        proxy: str = None,
    ) -> tuple[dict | None, int | None]:
        cookies = await self.database.list_douyin_cookies(status="active")
        if not cookies:
            data = await self._fetch_douyin_detail(detail_id, "", proxy=proxy)
            return data, None
        for item in cookies:
            cookie_value = item.get("cookie", "")
            try:
                data = await asyncio.wait_for(
                    self._fetch_douyin_detail(
                        detail_id,
                        cookie_value,
                        proxy=proxy,
                    ),
                    timeout=self.USER_FETCH_TIMEOUT,
                )
            except asyncio.TimeoutError:
                await self.database.mark_douyin_cookie_expired(item.get("id", 0))
                continue
            if data:
                return data, item.get("id", 0)
        data = await self._fetch_douyin_detail(detail_id, "", proxy=proxy)
        return data, None

    async def _cleanup_orphan_works(self, force: bool = False) -> None:
        now = datetime.now()
        if (
            not force
            and self._orphan_cleanup_at
            and (now - self._orphan_cleanup_at).total_seconds() < 600
        ):
            return
        self._orphan_cleanup_at = now
        try:
            removed = await self.database.delete_orphan_douyin_works()
            if removed:
                self.logger.info(_("已清理孤儿作品: %s") % removed)
                self._notify_feed_update(
                    "delete",
                    {"reason": "cleanup", "works_removed": removed},
                )
        except Exception:
            self.logger.error(_("清理孤儿作品失败"))

    def _notify_feed_update(self, reason: str, payload: dict | None = None) -> None:
        if not self._feed_subscribers:
            return
        data = {
            "reason": reason,
            "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if payload:
            data.update(payload)
        event = {"type": "feed", "data": data}
        for queue in list(self._feed_subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                continue

    async def _build_daily_feed_page(
        self,
        page: int,
        page_size: int,
        sec_user_id: str = "",
    ) -> DouyinClientFeedPage:
        await self._cleanup_orphan_works()
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        today = self._today_str()
        sec_user_id = sec_user_id.strip()
        if sec_user_id:
            video_total = await self.database.count_douyin_user_works(
                sec_user_id,
                work_types=self.CLIENT_FEED_WORK_TYPES,
            )
            video_rows = await self.database.list_douyin_user_works(
                sec_user_id,
                page,
                page_size,
                work_types=self.CLIENT_FEED_WORK_TYPES,
            )
            items = [self._build_work_feed_item(row)[1] for row in video_rows]
            return DouyinClientFeedPage(
                total=video_total,
                video_total=video_total,
                live_total=0,
                items=items,
            )
        video_total = await self.database.count_douyin_works_today(
            today,
            work_types=self.CLIENT_FEED_WORK_TYPES,
        )
        live_total = await self.database.count_douyin_live_today(today)
        fetch_size = page * page_size
        video_rows = await self.database.list_douyin_works_today(
            today,
            1,
            fetch_size,
            work_types=self.CLIENT_FEED_WORK_TYPES,
        )
        live_rows = await self.database.list_douyin_live_today(
            today,
            1,
            fetch_size,
        )
        items_with_sort = [
            self._build_work_feed_item(row) for row in video_rows
        ] + [self._build_live_feed_item(row) for row in live_rows]
        items_with_sort.sort(key=lambda item: item[0], reverse=True)
        start = (page - 1) * page_size
        end = start + page_size
        items = [item for _, item in items_with_sort[start:end]]
        return DouyinClientFeedPage(
            total=video_total + live_total,
            video_total=video_total,
            live_total=live_total,
            items=items,
        )

    async def _build_playlist_feed_page(
        self,
        playlist_id: int,
        page: int,
        page_size: int,
    ) -> DouyinClientFeedPage:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        total = await self.database.count_douyin_playlist_items(playlist_id)
        rows = await self.database.list_douyin_playlist_items(
            playlist_id,
            page,
            page_size,
        )
        items = [self._build_work_feed_item(row)[1] for row in rows]
        return DouyinClientFeedPage(
            total=total,
            video_total=total,
            live_total=0,
            items=items,
        )

    async def _fetch_douyin_account_page(
        self,
        sec_user_id: str,
        cookie: str,
        cursor: int = 0,
        count: int = 18,
        proxy: str = None,
    ) -> tuple[list[dict], int, bool, bool, bool]:
        account = AccountFetcher(
            self.parameter,
            cookie,
            proxy,
            sec_user_id,
            "post",
            "",
            "",
            pages=1,
            cursor=cursor,
            count=count,
        )
        data = await account.run(single_page=True)
        if data:
            self._debug_dump_account_data(sec_user_id, data)
        return (
            data or [],
            int(account.cursor or 0),
            not account.finished,
            account.cookie_invalid,
            account.empty_data,
        )

    async def _fetch_douyin_account_page_with_pool(
        self,
        sec_user_id: str,
        cursor: int = 0,
        count: int = 18,
        proxy: str = None,
    ) -> tuple[list[dict], int, bool, int | None, bool, bool]:
        cookies = await self.database.list_douyin_cookies(status="active")
        if not cookies:
            data, next_cursor, has_more, cookie_invalid, empty_data = (
                await self._fetch_douyin_account_page(
                    sec_user_id,
                    "",
                    cursor=cursor,
                    count=count,
                    proxy=proxy,
                )
            )
            return data, next_cursor, has_more, None, cookie_invalid, empty_data
        for item in cookies:
            cookie_value = item.get("cookie", "")
            try:
                data, next_cursor, has_more, cookie_invalid, empty_data = (
                    await asyncio.wait_for(
                        self._fetch_douyin_account_page(
                            sec_user_id,
                            cookie_value,
                            cursor=cursor,
                            count=count,
                            proxy=proxy,
                        ),
                        timeout=self.USER_FETCH_TIMEOUT,
                    )
                )
            except asyncio.TimeoutError:
                await self.database.mark_douyin_cookie_expired(item.get("id", 0))
                continue
            if cookie_invalid:
                await self.database.mark_douyin_cookie_expired(item.get("id", 0))
                continue
            if data or empty_data:
                return (
                    data,
                    next_cursor,
                    has_more,
                    item.get("id", 0),
                    cookie_invalid,
                    empty_data,
                )
        return [], 0, False, None, True, False

    async def _fetch_douyin_account_data(
        self,
        extract: AccountPayload,
    ) -> tuple[list[dict] | None, dict, int | None]:
        if extract.cookie:
            data, meta = await self.deal_account_detail(
                0,
                extract.sec_user_id,
                tab=extract.tab,
                earliest=extract.earliest,
                latest=extract.latest,
                pages=extract.pages,
                api=True,
                source=extract.source,
                cookie=extract.cookie,
                proxy=extract.proxy,
                tiktok=False,
                cursor=extract.cursor,
                count=extract.count,
                return_meta=True,
            )
            return data, meta, None
        cookies = await self.database.list_douyin_cookies(status="active")
        if not cookies:
            data, meta = await self.deal_account_detail(
                0,
                extract.sec_user_id,
                tab=extract.tab,
                earliest=extract.earliest,
                latest=extract.latest,
                pages=extract.pages,
                api=True,
                source=extract.source,
                cookie=extract.cookie,
                proxy=extract.proxy,
                tiktok=False,
                cursor=extract.cursor,
                count=extract.count,
                return_meta=True,
            )
            return data, meta, None
        for item in cookies:
            data, meta = await self.deal_account_detail(
                0,
                extract.sec_user_id,
                tab=extract.tab,
                earliest=extract.earliest,
                latest=extract.latest,
                pages=extract.pages,
                api=True,
                source=extract.source,
                cookie=item.get("cookie", ""),
                proxy=extract.proxy,
                tiktok=False,
                cursor=extract.cursor,
                count=extract.count,
                return_meta=True,
            )
            if meta.get("cookie_invalid"):
                await self.database.mark_douyin_cookie_expired(item.get("id", 0))
                continue
            return data, meta, item.get("id", 0)
        return None, {"cookie_invalid": True, "empty_data": False}, None

    async def _fetch_douyin_account_live(
        self,
        extract: AccountLive,
    ) -> tuple[dict | None, int | None, str]:
        if extract.cookie:
            live_info = await self.get_account_live_status(
                extract.sec_user_id,
                cookie=extract.cookie,
                proxy=extract.proxy,
                dump_html=extract.dump_html,
            )
            return live_info, None, extract.cookie
        cookies = await self.database.list_douyin_cookies(status="active")
        if not cookies:
            live_info = await self.get_account_live_status(
                extract.sec_user_id,
                cookie=extract.cookie,
                proxy=extract.proxy,
                dump_html=extract.dump_html,
            )
            return live_info, None, extract.cookie
        for item in cookies:
            cookie_value = item.get("cookie", "")
            live_info = await self.get_account_live_status(
                extract.sec_user_id,
                cookie=cookie_value,
                proxy=extract.proxy,
                dump_html=extract.dump_html,
            )
            if not live_info:
                await self.database.mark_douyin_cookie_expired(item.get("id", 0))
                continue
            return live_info, item.get("id", 0), cookie_value
        return None, None, ""

    def _cache_live_info(self, sec_user_id: str, live_info: dict) -> None:
        if not sec_user_id or not live_info:
            return
        self._douyin_live_cache[sec_user_id] = live_info

    def _get_cached_live_info(self, sec_user_id: str) -> dict | None:
        return self._douyin_live_cache.get(sec_user_id)

    def _clear_live_cache(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        self._douyin_live_cache.pop(sec_user_id, None)

    async def _build_live_info(
        self,
        extract: AccountLive,
    ) -> dict | None:
        live_info, cookie_id, cookie_value = await self._fetch_douyin_account_live(
            extract
        )
        if not live_info:
            return None
        if cookie_id:
            await self.database.touch_douyin_cookie(cookie_id)
        web_rid = live_info.get("web_rid") or None
        room_id = live_info.get("room_id") or None
        if not live_info.get("live_status") or (not room_id and not web_rid):
            live_info["room"] = None
            return live_info
        room_data = await self.get_live_data(
            web_rid=web_rid,
            room_id=room_id,
            sec_user_id=extract.sec_user_id,
            cookie=cookie_value or extract.cookie,
            proxy=extract.proxy,
        )
        if not room_data and room_id and web_rid:
            room_data = await self.get_live_data(
                room_id=room_id,
                sec_user_id=extract.sec_user_id,
                cookie=cookie_value or extract.cookie,
                proxy=extract.proxy,
            )
        if not room_data:
            live_info["room"] = None
            return live_info
        if extract.source:
            live_info["room"] = room_data
            return live_info
        room_list = await self.extractor.run(
            [room_data],
            None,
            "live",
        )
        live_info["room"] = room_list[0] if room_list else None
        return live_info

    async def _fill_work_sizes(self, works: list[dict]) -> None:
        if not works:
            return
        semaphore = asyncio.Semaphore(self.MEDIA_PROBE_CONCURRENCY)

        async def probe(item: dict) -> None:
            work_type = item.get("type") or item.get("work_type") or "video"
            if work_type != "video":
                item.pop("play_url", None)
                return
            if item.get("width") and item.get("height"):
                item.pop("play_url", None)
                return
            url = item.get("play_url") or ""
            if not url:
                item.pop("play_url", None)
                return
            async with semaphore:
                width, height = await self._probe_media_size(url)
            if width and height:
                item["width"] = width
                item["height"] = height
            item.pop("play_url", None)

        await asyncio.gather(*(probe(item) for item in works))

    async def _store_account_work_items(
        self,
        sec_user_id: str,
        work_items: list[dict],
    ) -> tuple[int, int]:
        if not sec_user_id or not work_items:
            return 0, 0
        works = [self._extract_work_brief(item, sec_user_id) for item in work_items]
        await self._fill_work_sizes(works)
        stored = await self.database.insert_douyin_works(works)
        if any(item.get("create_date") == self._today_str() for item in works):
            await self.database.update_douyin_user_new(sec_user_id, True)
        return len(works), int(stored or 0)

    def _init_user_full_sync_progress(self, sec_user_id: str) -> dict:
        sec_user_id = str(sec_user_id or "").strip()
        if not sec_user_id:
            return {}
        progress = self._user_full_sync_progress.get(sec_user_id)
        if not progress:
            progress = {
                "sec_user_id": sec_user_id,
                "status": "idle",
                "started_at": "",
                "updated_at": "",
                "finished_at": "",
                "pages": 0,
                "works": 0,
                "stored": 0,
                "has_more": False,
                "error": "",
            }
            self._user_full_sync_progress[sec_user_id] = progress
        return progress

    def _update_user_full_sync_progress(self, sec_user_id: str, **updates) -> dict:
        progress = self._init_user_full_sync_progress(sec_user_id)
        if not progress:
            return {}
        progress.update(updates)
        progress["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return progress

    async def _sync_user_all_works(
        self,
        sec_user_id: str,
        seed_data: list[dict] | None = None,
        seed_next_cursor: int = 0,
        seed_has_more: bool = False,
    ) -> None:
        sec_user_id = str(sec_user_id or "").strip()
        if not sec_user_id:
            return
        if sec_user_id in self._user_full_syncing:
            return
        self._user_full_syncing.add(sec_user_id)
        self._update_user_full_sync_progress(
            sec_user_id,
            status="running",
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            finished_at="",
            pages=0,
            works=0,
            stored=0,
            has_more=bool(seed_has_more),
            error="",
        )
        total_pages = 0
        total_works = 0
        total_saved = 0
        auto_update_enabled = False
        try:
            seed_items = [item for item in (seed_data or []) if self._is_work_item(item)]
            count, saved = await self._store_account_work_items(sec_user_id, seed_items)
            total_works += count
            total_saved += saved
            if seed_data is not None:
                total_pages = 1

            cursor = int(seed_next_cursor or 0)
            has_more = bool(seed_has_more)
            if seed_data is None:
                cursor = 0
                has_more = True
            self._update_user_full_sync_progress(
                sec_user_id,
                pages=total_pages,
                works=total_works,
                stored=total_saved,
                has_more=has_more,
            )
            while has_more and total_pages < self.USER_FULL_SYNC_MAX_PAGES:
                (
                    data,
                    next_cursor,
                    has_more,
                    cookie_id,
                    _cookie_invalid,
                    empty_data,
                ) = await self._fetch_douyin_account_page_with_pool(
                    sec_user_id,
                    cursor=cursor,
                    count=self.USER_FULL_SYNC_PAGE_COUNT,
                )
                if cookie_id and (data or empty_data):
                    await self.database.touch_douyin_cookie(cookie_id)

                work_items = [item for item in data if self._is_work_item(item)]
                count, saved = await self._store_account_work_items(sec_user_id, work_items)
                total_works += count
                total_saved += saved
                total_pages += 1
                self._update_user_full_sync_progress(
                    sec_user_id,
                    pages=total_pages,
                    works=total_works,
                    stored=total_saved,
                    has_more=has_more,
                )

                next_cursor = int(next_cursor or 0)
                if not data and (empty_data or next_cursor == cursor):
                    break
                cursor = next_cursor

            user_row = await self.database.get_douyin_user(sec_user_id)
            if user_row:
                auto_update_enabled = bool(user_row.get("auto_update", 0))
                has_works = bool(total_works > 0)
                status = "active" if has_works else "no_works"
                if (
                    bool(user_row.get("has_works", 0)) != has_works
                    or str(user_row.get("status", "")).strip() != status
                ):
                    await self.database.upsert_douyin_user(
                        sec_user_id=sec_user_id,
                        uid=str(user_row.get("uid", "")),
                        nickname=str(user_row.get("nickname", "")),
                        avatar=str(user_row.get("avatar", "")),
                        cover=str(user_row.get("cover", "")),
                        has_works=has_works,
                        status=status,
                    )
            await self.database.update_douyin_user_fetch_time(sec_user_id)
            self.logger.info(
                _(
                    "新增用户全量作品同步完成: sec_user_id={sec_user_id}, total_works={total_works}, stored={stored}, pages={pages}"
                ).format(
                    sec_user_id=sec_user_id,
                    total_works=total_works,
                    stored=total_saved,
                    pages=total_pages,
                )
            )
            self._update_user_full_sync_progress(
                sec_user_id,
                status="done",
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                has_more=bool(has_more),
            )
            # 历史全量同步是异步落库，若用户已开启自动下载，需在同步完成后再触发一次。
            if auto_update_enabled:
                self._trigger_user_auto_update_now(sec_user_id)
        except Exception as exc:
            error_text = str(exc)
            if len(error_text) > 200:
                error_text = f"{error_text[:200]}..."
            self._update_user_full_sync_progress(
                sec_user_id,
                status="failed",
                finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                error=error_text,
            )
            self.logger.error(_("新增用户全量作品同步异常"), exc_info=True)
        finally:
            self._user_full_syncing.discard(sec_user_id)

    def _trigger_user_full_sync(
        self,
        sec_user_id: str,
        seed_data: list[dict] | None = None,
        seed_next_cursor: int = 0,
        seed_has_more: bool = False,
    ) -> None:
        sec_user_id = str(sec_user_id or "").strip()
        if not sec_user_id:
            return
        if sec_user_id in self._user_full_syncing:
            self._update_user_full_sync_progress(sec_user_id, status="running")
            return
        self._update_user_full_sync_progress(
            sec_user_id,
            status="queued",
            started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            finished_at="",
            pages=0,
            works=0,
            stored=0,
            has_more=bool(seed_has_more),
            error="",
        )
        asyncio.create_task(
            self._sync_user_all_works(
                sec_user_id=sec_user_id,
                seed_data=seed_data,
                seed_next_cursor=seed_next_cursor,
                seed_has_more=seed_has_more,
            )
        )

    async def _refresh_user_latest(self, sec_user_id: str) -> dict:
        data, next_cursor, has_more, cookie_id, cookie_invalid, empty_data = await self._fetch_douyin_account_page_with_pool(
            sec_user_id,
            cursor=0,
            count=18,
        )
        if cookie_id and (data or empty_data):
            await self.database.touch_douyin_cookie(cookie_id)
        work_items = [item for item in data if self._is_work_item(item)]
        profile_source = work_items[0] if work_items else (data[0] if data else None)
        if profile_source:
            profile = self._extract_author_profile(profile_source)
            await self.database.update_douyin_user_profile(
                sec_user_id,
                profile.get("uid", ""),
                profile.get("nickname", ""),
                profile.get("avatar", ""),
                profile.get("cover", ""),
            )
        await self.database.update_douyin_user_fetch_time(sec_user_id)
        works = [self._extract_work_brief(item, sec_user_id) for item in work_items]
        today = self._today_str()
        today_works = [item for item in works if item.get("create_date") == today]
        await self._fill_work_sizes(today_works)
        inserted = await self.database.insert_douyin_works(today_works)
        if today_works:
            await self.database.update_douyin_user_new(sec_user_id, True)
        if inserted:
            self._notify_feed_update(
                "video",
                {"sec_user_id": sec_user_id, "count": inserted},
            )
        return {
            "items": today_works,
            "inserted": inserted,
            "total": len(today_works),
        }

    async def _list_user_pending_auto_download_works(
        self,
        sec_user_id: str,
    ) -> list[dict]:
        rows = await self.database.list_douyin_user_pending_works(
            sec_user_id,
            limit=self.AUTO_PENDING_SCAN_LIMIT,
        )
        works = []
        for row in rows:
            aweme_id = str(row.get("aweme_id", "")).strip()
            if not aweme_id:
                continue
            status = str(row.get("upload_status") or "").strip().lower()
            status_updated_at = str(row.get("status_updated_at") or "").strip()
            if status == "failed" and not self._is_status_stale(
                status_updated_at,
                self.AUTO_FAILED_RETRY_INTERVAL_MINUTES,
            ):
                continue
            work_type = row.get("work_type") or "video"
            if work_type not in self.DOWNLOADABLE_WORK_TYPES:
                continue
            works.append(
                {
                    "aweme_id": aweme_id,
                    "type": work_type,
                }
            )
        return works

    async def _collect_auto_download_works(
        self,
        sec_user_id: str,
        latest_items: list[dict] | None = None,
    ) -> list[dict]:
        candidates: dict[str, dict] = {}

        for item in latest_items or []:
            aweme_id = str(item.get("aweme_id", "")).strip()
            if not aweme_id:
                continue
            work_type = item.get("type") or item.get("work_type") or "video"
            if work_type not in self.DOWNLOADABLE_WORK_TYPES:
                continue
            candidates[aweme_id] = {
                "aweme_id": aweme_id,
                "type": work_type,
            }

        pending = await self._list_user_pending_auto_download_works(sec_user_id)
        for item in pending:
            aweme_id = str(item.get("aweme_id", "")).strip()
            if not aweme_id:
                continue
            if aweme_id in candidates:
                continue
            candidates[aweme_id] = item

        return list(candidates.values())

    async def _reconcile_work_status_from_download_record(
        self,
        aweme_ids: list[str],
    ) -> int:
        updated = 0
        for aweme_id in aweme_ids:
            if not aweme_id:
                continue
            row = await self.database.get_douyin_work(aweme_id)
            status = str(row.get("upload_status") or "").strip().lower()
            if status in ("downloaded", "uploaded", "uploading"):
                continue
            if not await self.recorder.has_id(aweme_id):
                continue
            raw_local_path = str(row.get("local_path", "")).strip()
            local_file = self._resolve_local_cache_path(raw_local_path)
            local_path = (
                str(local_file) if local_file and local_file.is_file() else raw_local_path
            )
            await self.database.update_douyin_work_upload(
                aweme_id=aweme_id,
                status="downloaded",
                local_path=local_path,
                message="自动补偿: 检测到历史下载记录，已标记为已下载",
                mark_downloaded=True,
            )
            updated += 1
        return updated

    async def _auto_download_user_works(self, sec_user_id: str, works: list[dict]) -> None:
        if not sec_user_id or not works:
            return
        target_ids = []
        status_map: dict[str, str] = {}
        upload_enabled = self._upload_channel_enabled()
        for item in works:
            aweme_id = str(item.get("aweme_id", "")).strip()
            if not aweme_id:
                continue
            work_type = item.get("type") or item.get("work_type") or "video"
            if work_type not in self.DOWNLOADABLE_WORK_TYPES:
                continue
            row = await self.database.get_douyin_work(aweme_id)
            current_status = (row.get("upload_status") or "").lower()
            if current_status == "uploaded":
                continue
            if current_status in ("pending", "failed"):
                if await self.recorder.has_id(aweme_id):
                    await self.database.update_douyin_work_upload(
                        aweme_id=aweme_id,
                        status="downloaded",
                        message="自动补偿: 检测到历史下载记录，已标记为已下载",
                        mark_downloaded=True,
                    )
                    continue
            if not upload_enabled and current_status == "failed":
                raw_local_path = str(row.get("local_path", "")).strip()
                local_file = self._resolve_local_cache_path(raw_local_path)
                if local_file and local_file.is_file():
                    await self.database.update_douyin_work_upload(
                        aweme_id=aweme_id,
                        status="downloaded",
                        local_path=str(local_file),
                        message="自动补偿: 上传未启用，检测到本地文件，已标记为已下载",
                        mark_downloaded=True,
                    )
                    continue
            if not upload_enabled and current_status in ("uploading", "downloaded"):
                if current_status == "uploading":
                    await self.database.update_douyin_work_upload(
                        aweme_id=aweme_id,
                        status="downloaded",
                        message="自动补偿: 上传未启用，已标记为已下载",
                        mark_downloaded=True,
                    )
                continue
            if aweme_id in self._auto_downloading:
                continue
            if current_status in ("downloading", "uploading"):
                status_updated_at = str(row.get("status_updated_at") or "").strip()
                if not self._is_status_stale(
                    status_updated_at,
                    self.AUTO_ZOMBIE_TIMEOUT_MINUTES,
                ):
                    continue
                await self.database.update_douyin_work_upload(
                    aweme_id=aweme_id,
                    status="pending",
                    message="自动补偿: 检测到超时僵尸任务，已重置",
                )
                current_status = "pending"
            status_map[aweme_id] = current_status
            self._auto_downloading.add(aweme_id)
            target_ids.append(aweme_id)

        if not target_ids:
            return
        try:
            download_enabled = bool(getattr(self.downloader, "download", True))
            for aweme_id in target_ids:
                if status_map.get(aweme_id) not in (
                    "downloading",
                    "downloaded",
                    "uploading",
                ):
                    await self.database.update_douyin_work_upload(
                        aweme_id=aweme_id,
                        status="downloading" if download_enabled else "pending",
                        message="自动下载任务处理中" if download_enabled else "",
                    )

            details = []
            for aweme_id in target_ids:
                data, cookie_id = await self._fetch_douyin_detail_with_pool(aweme_id)
                if cookie_id and data:
                    await self.database.touch_douyin_cookie(cookie_id)
                if not data:
                    await self.database.update_douyin_work_upload(
                        aweme_id=aweme_id,
                        status="failed",
                        message="作品详情获取失败",
                    )
                    continue
                details.append(data)
            if not details:
                return

            root, params, logger = self.record.run(self.parameter)
            async with logger(root, console=self.console, **params) as record:
                detail_data = await self.extractor.run(
                    details,
                    record,
                    tiktok=False,
                )
            if detail_data:
                await self.downloader.run(detail_data, "detail", tiktok=False)
                await self._reconcile_work_status_from_download_record(target_ids)
        finally:
            for aweme_id in target_ids:
                self._auto_downloading.discard(aweme_id)

    async def _run_auto_download_compensation(self, force: bool = False) -> None:
        now = datetime.now()
        if (
            not force
            and self._auto_compensate_at
            and (
                now - self._auto_compensate_at
            ).total_seconds() < self.AUTO_COMPENSATE_INTERVAL_SECONDS
        ):
            return
        self._auto_compensate_at = now

        users_total = 0
        users_pending = 0
        works_pending = 0
        users_processed = 0
        reset_count = 0
        error_text = ""
        stale_before = (
            now - timedelta(minutes=self.AUTO_ZOMBIE_TIMEOUT_MINUTES)
        ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            reset_count = await self.database.reset_stale_douyin_work_status(
                stale_before=stale_before,
                limit=self.AUTO_ZOMBIE_RESET_LIMIT,
            )
            if reset_count:
                self.logger.info(_("已自动重置僵尸下载任务: %s") % reset_count)

            users = await self.database.list_douyin_users_auto_update()
            users_total = len(users)
            # 自动补偿的目标是“排空积压任务”，不受时间窗口限制。
            for user in users:
                sec_user_id = str(user.get("sec_user_id", "")).strip()
                if not sec_user_id:
                    continue
                pending = await self._list_user_pending_auto_download_works(sec_user_id)
                if not pending:
                    continue
                users_pending += 1
                works_pending += len(pending)
                await self._auto_download_user_works(sec_user_id, pending)
                users_processed += 1
        except Exception as exc:
            error_text = str(exc)
            raise
        finally:
            self._auto_compensation_status = {
                "last_run_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "last_force": bool(force),
                "reset_count": int(reset_count or 0),
                "users_total": int(users_total or 0),
                "users_pending": int(users_pending or 0),
                "works_pending": int(works_pending or 0),
                "users_processed": int(users_processed or 0),
                "running_downloading": len(self._auto_downloading),
                "error": error_text,
            }

    async def _run_user_auto_update_now(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        latest_items = []
        try:
            latest = await self._refresh_user_latest(sec_user_id)
            latest_items = latest.get("items", [])
        except Exception:
            # 最新页拉取失败时，仍需继续处理历史 pending，避免自动下载整体失效。
            self.logger.error(_("拉取用户最新作品失败，继续处理历史待下载作品"), exc_info=True)
        candidates = await self._collect_auto_download_works(
            sec_user_id,
            latest_items,
        )
        await self._auto_download_user_works(
            sec_user_id,
            candidates,
        )
        try:
            await self._refresh_user_live(sec_user_id)
        except Exception:
            self.logger.error(_("拉取直播状态失败"), exc_info=True)

    async def _run_user_auto_update_now_background(self, sec_user_id: str) -> None:
        try:
            await self._run_user_auto_update_now(sec_user_id)
        except Exception:
            self.logger.error(_("立即自动下载任务执行异常"), exc_info=True)

    def _trigger_user_auto_update_now(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        asyncio.create_task(
            self._run_user_auto_update_now_background(sec_user_id),
        )

    def _trigger_refresh_latest(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        if sec_user_id in self._refresh_pending:
            return
        try:
            self._refresh_queue.put_nowait(sec_user_id)
            self._refresh_pending.add(sec_user_id)
        except asyncio.QueueFull:
            self.logger.warning(_("自动拉取队列已满，忽略请求"))

    async def _bootstrap_auto_update_users(self) -> None:
        try:
            users = await self.database.list_douyin_users_auto_update()
        except Exception:
            self.logger.error(_("初始化自动下载用户任务失败"), exc_info=True)
            return
        for row in users:
            sec_user_id = str(row.get("sec_user_id", "")).strip()
            if not sec_user_id:
                continue
            self._trigger_user_auto_update_now(sec_user_id)

    async def _refresh_user_live_background(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        try:
            await self._refresh_user_live(sec_user_id)
        except Exception:
            self.logger.error(_("拉取直播状态失败"), exc_info=True)

    def _trigger_refresh_live(self, sec_user_id: str) -> None:
        if not sec_user_id:
            return
        asyncio.create_task(self._refresh_user_live_background(sec_user_id))

    async def _refresh_latest_worker(self, worker_id: int) -> None:
        while True:
            sec_user_id = await self._refresh_queue.get()
            try:
                await self._refresh_user_latest(sec_user_id)
            except Exception:
                self.logger.error(_("自动拉取任务执行异常"), exc_info=True)
            finally:
                self._refresh_pending.discard(sec_user_id)
                self._refresh_queue.task_done()

    async def _refresh_user_live(self, sec_user_id: str) -> dict:
        if not sec_user_id:
            return {}
        if sec_user_id in self._live_refreshing:
            return self._get_cached_live_info(sec_user_id) or {}
        self._live_refreshing.add(sec_user_id)
        extract = AccountLive(
            sec_user_id=sec_user_id,
            dump_html=False,
        )
        try:
            live_info = await self._build_live_info(extract)
            is_live = bool(live_info and live_info.get("live_status"))
            await self.database.update_douyin_user_live(sec_user_id, is_live)
            if live_info and is_live:
                room = live_info.get("room") if isinstance(live_info, dict) else None
                if isinstance(room, dict):
                    width = int(room.get("width") or 0)
                    height = int(room.get("height") or 0)
                    if not width or not height:
                        stream_url = self._pick_live_stream_url(room)
                        if stream_url:
                            width, height = await self._probe_media_size(stream_url)
                    if width and height:
                        room["width"] = width
                        room["height"] = height
                        await self.database.update_douyin_user_live_size(
                            sec_user_id, width, height
                        )
                self._notify_feed_update(
                    "live",
                    {
                        "sec_user_id": sec_user_id,
                        "web_rid": live_info.get("web_rid")
                        if isinstance(live_info, dict)
                        else "",
                        "room_id": live_info.get("room_id")
                        if isinstance(live_info, dict)
                        else "",
                    },
                )
            if live_info:
                self._cache_live_info(sec_user_id, live_info)

            if self.live_recorder.enabled:
                if live_info and is_live and isinstance(live_info.get("room"), dict):
                    await self.live_recorder.ensure_recording(sec_user_id, live_info)
                else:
                    await self.live_recorder.mark_offline(sec_user_id)
            return live_info or {}
        finally:
            self._live_refreshing.discard(sec_user_id)

    async def _schedule_tick(self) -> None:
        setting = self._resolve_schedule_setting(
            await self.database.get_douyin_schedule()
        )
        if not setting.get("enabled"):
            return
        await self._cleanup_orphan_works()
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        if current_time not in set(setting.get("times", [])):
            return
        current_key = now.strftime("%Y-%m-%d %H:%M")
        if self._schedule_last_key == current_key:
            return
        self._schedule_last_key = current_key
        users = await self.database.list_douyin_users_auto_update()
        for user in users:
            start = user.get("update_window_start", "")
            end = user.get("update_window_end", "")
            if not self._within_window(start, end, now.time()):
                continue
            sec_user_id = user.get("sec_user_id", "")
            latest = await self._refresh_user_latest(sec_user_id)
            candidates = await self._collect_auto_download_works(
                sec_user_id,
                latest.get("items", []),
            )
            await self._auto_download_user_works(
                sec_user_id,
                candidates,
            )
            await self._refresh_user_live(sec_user_id)

    async def _run_schedule_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            try:
                await self._run_auto_download_compensation()
            except Exception:
                self.logger.error(_("自动下载补偿任务执行异常"), exc_info=True)
            try:
                await self._schedule_tick()
            except Exception:
                self.logger.error(_("计划任务执行异常"), exc_info=True)

    async def _live_monitor_tick(self) -> None:
        if not self.live_recorder.enabled:
            return
        users = await self.database.list_douyin_users_auto_update()
        now = datetime.now().time()
        active_users = set()
        for user in users:
            sec_user_id = str(user.get("sec_user_id", "")).strip()
            if not sec_user_id:
                continue
            start = user.get("update_window_start", "")
            end = user.get("update_window_end", "")
            if not self._within_window(start, end, now):
                continue
            active_users.add(sec_user_id)
            await self._refresh_user_live(sec_user_id)
        await self.live_recorder.prune_sessions(active_users)

    async def _run_live_monitor_loop(self) -> None:
        if not self.live_recorder.enabled:
            return
        while True:
            await asyncio.sleep(self.live_recorder.monitor_interval)
            try:
                await self._live_monitor_tick()
            except Exception:
                self.logger.error(_("直播监听任务执行异常"), exc_info=True)

    async def handle_redirect(self, text: str, proxy: str = None) -> str:
        return await self.links.run(
            text,
            "",
            proxy,
        )

    async def handle_redirect_tiktok(self, text: str, proxy: str = None) -> str:
        return await self.links_tiktok.run(
            text,
            "",
            proxy,
        )

    async def run_server(
        self,
        host=SERVER_HOST,
        port=SERVER_PORT,
        log_level="info",
    ):
        self.server = FastAPI(
            debug=VERSION_BETA,
            title="DouK-Downloader",
            version=__VERSION__,
        )
        self.setup_routes()
        config = Config(
            self.server,
            host=host,
            port=port,
            log_level=log_level,
        )
        server = Server(config)
        await server.serve()

    def setup_routes(self):
        admin_root = (
            Path(__file__).resolve().parent.parent.parent.joinpath("static", "admin")
        )
        if admin_root.exists():
            self.server.mount(
                "/admin-ui",
                StaticFiles(directory=admin_root, html=True),
                name="admin-ui",
            )
        client_root = (
            Path(__file__).resolve().parent.parent.parent.joinpath("static", "client")
        )
        if client_root.exists():
            self.server.mount(
                "/client-ui",
                StaticFiles(directory=client_root, html=True),
                name="client-ui",
            )

        @self.server.on_event("startup")
        async def startup_schedule():
            await self.database.mark_running_live_records_interrupted()
            if not self._schedule_task:
                self._schedule_task = asyncio.create_task(self._run_schedule_loop())
            if not self._refresh_workers:
                self._refresh_workers = [
                    asyncio.create_task(self._refresh_latest_worker(index))
                    for index in range(self.REFRESH_CONCURRENCY)
                ]
            if self.live_recorder.enabled and not self._live_monitor_task:
                self._live_monitor_task = asyncio.create_task(
                    self._run_live_monitor_loop()
                )
                await self._live_monitor_tick()
            asyncio.create_task(self._bootstrap_auto_update_users())
            asyncio.create_task(self._run_auto_download_compensation(force=True))

        @self.server.on_event("shutdown")
        async def shutdown_schedule():
            if self._schedule_task:
                self._schedule_task.cancel()
                self._schedule_task = None
            if self._refresh_workers:
                for task in self._refresh_workers:
                    task.cancel()
                self._refresh_workers = []
            if self._live_monitor_task:
                self._live_monitor_task.cancel()
                self._live_monitor_task = None
            await self.live_recorder.shutdown()

        @self.server.get(
            "/",
            summary=_("访问项目 GitHub 仓库"),
            description=_("重定向至项目 GitHub 仓库主页"),
            tags=[_("项目")],
        )
        async def index():
            return RedirectResponse(url=REPOSITORY)

        @self.server.get(
            "/admin/douyin/media",
            summary=_("代理获取抖音图片资源"),
            tags=[_("管理")],
            response_class=Response,
        )
        async def proxy_douyin_media(
            url: str = Query(..., min_length=8),
            token: str = Depends(token_dependency),
        ):
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise HTTPException(status_code=400, detail=_("无效资源地址"))
            headers = self._build_stream_headers(url, None)
            try:
                resp = await self.parameter.client.get(url, headers=headers)
                resp.raise_for_status()
            except Exception:
                raise HTTPException(status_code=502, detail=_("图片获取失败"))
            content_type = resp.headers.get("Content-Type") or "image/jpeg"
            return Response(content=resp.content, media_type=content_type)

        @self.server.get(
            "/client/douyin/media",
            summary=_("代理获取抖音图片资源"),
            tags=[_("客户端")],
            response_class=Response,
        )
        async def proxy_douyin_media_client(
            url: str = Query(..., min_length=8),
        ):
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise HTTPException(status_code=400, detail=_("无效资源地址"))
            headers = self._build_stream_headers(url, None)
            try:
                resp = await self.parameter.client.get(url, headers=headers)
                resp.raise_for_status()
            except Exception:
                raise HTTPException(status_code=502, detail=_("图片获取失败"))
            content_type = resp.headers.get("Content-Type") or "image/jpeg"
            return Response(content=resp.content, media_type=content_type)

        @self.server.get(
            "/client/douyin/local-stream",
            summary=_("播放本地缓存媒体"),
            tags=[_("客户端")],
            response_class=FileResponse,
        )
        async def stream_douyin_local_cache_client(
            aweme_id: str = Query(..., min_length=6),
        ):
            aweme_id = str(aweme_id or "").strip()
            work_row = await self.database.get_douyin_work(aweme_id)
            if not work_row:
                raise HTTPException(status_code=404, detail=_("未找到作品记录"))
            local_file = await self._resolve_work_local_file(aweme_id, work_row)
            if not local_file:
                raise HTTPException(status_code=404, detail=_("本地缓存不存在"))
            media_type = guess_type(local_file.name)[0] or "application/octet-stream"
            return FileResponse(
                path=str(local_file),
                media_type=media_type,
                filename=local_file.name,
            )

        @self.server.get(
            "/client/douyin/stream",
            summary=_("代理获取抖音媒体资源"),
            tags=[_("客户端")],
            response_class=Response,
        )
        async def proxy_douyin_stream_client(
            request: Request,
            url: str = Query(..., min_length=8),
        ):
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise HTTPException(status_code=400, detail=_("无效资源地址"))
            range_header = request.headers.get("range") if request else None
            cache_key = self._build_stream_cache_key(url, range_header)
            cached = self._get_stream_cache(cache_key)
            if cached:
                return Response(
                    content=cached["body"],
                    status_code=cached.get("status_code", 200),
                    headers=cached.get("headers") or {},
                    media_type=cached.get("content_type") or "application/octet-stream",
                )
            headers = self._build_stream_headers(url, range_header)
            stream = self.parameter.client.stream("GET", url, headers=headers)
            try:
                resp = await stream.__aenter__()
            except Exception:
                raise HTTPException(status_code=502, detail=_("资源获取失败"))
            if resp.status_code >= 400:
                await stream.__aexit__(None, None, None)
                raise HTTPException(status_code=502, detail=_("资源获取失败"))
            content_type = resp.headers.get("Content-Type") or "application/octet-stream"
            is_live_prefix = self._is_live_prefix(url)
            if self._is_m3u8_resource(url, content_type):
                body = await resp.aread()
                await stream.__aexit__(None, None, None)
                text = body.decode("utf-8", errors="ignore")
                is_live = self._is_live_playlist(text)
                if is_live:
                    self._mark_live_prefix(url)
                rewritten = self._rewrite_m3u8(text, url, live=is_live).encode("utf-8")
                cache_headers = {"Content-Length": str(len(rewritten))}
                if is_live:
                    cache_headers["Cache-Control"] = "no-store"
                    cache_headers["X-Accel-Buffering"] = "no"
                else:
                    cache_headers["Cache-Control"] = "public, max-age=300"
                if not is_live:
                    self._set_stream_cache(
                        cache_key,
                        rewritten,
                        "application/vnd.apple.mpegurl",
                        cache_headers,
                        resp.status_code,
                        self.STREAM_CACHE_TTL_M3U8,
                    )
                return Response(
                    content=rewritten,
                    headers=cache_headers,
                    media_type="application/vnd.apple.mpegurl",
                )
            response_headers = {}
            for key in ("Content-Length", "Content-Range", "Accept-Ranges"):
                if resp.headers.get(key):
                    response_headers[key] = resp.headers.get(key)
            if is_live_prefix:
                response_headers["Cache-Control"] = "no-store"
                response_headers["X-Accel-Buffering"] = "no"
            else:
                response_headers["Cache-Control"] = "public, max-age=300"
            cacheable = self._should_cache_stream(
                url,
                content_type,
                range_header,
                resp.headers.get("Content-Length"),
            )
            if cacheable:
                body = await resp.aread()
                await stream.__aexit__(None, None, None)
                response_headers["Content-Length"] = str(len(body))
                self._set_stream_cache(
                    cache_key,
                    body,
                    content_type,
                    response_headers,
                    resp.status_code,
                    self.STREAM_CACHE_TTL_SEGMENT,
                )
                return Response(
                    content=body,
                    status_code=resp.status_code,
                    headers=response_headers,
                    media_type=content_type,
                )

            async def iterator():
                try:
                    async for chunk in resp.aiter_bytes():
                        yield chunk
                finally:
                    await stream.__aexit__(None, None, None)

            return StreamingResponse(
                iterator(),
                status_code=resp.status_code,
                headers=response_headers,
                media_type=content_type,
            )

        @self.server.get(
            "/token",
            summary=_("测试令牌有效性"),
            description=_(
                dedent("""
                项目默认无需令牌；公开部署时，建议设置令牌以防止恶意请求！
                
                令牌设置位置：`src/custom/function.py` - `is_valid_token()`
                """)
            ),
            tags=[_("项目")],
            response_model=DataResponse,
        )
        async def handle_test(token: str = Depends(token_dependency)):
            return DataResponse(
                message=_("验证成功！"),
                data=None,
                params=None,
            )

        @self.server.post(
            "/admin/settings",
            include_in_schema=False,
            response_model=Settings,
        )
        @self.server.post(
            "/settings",
            summary=_("更新项目全局配置"),
            description=_(
                dedent("""
                更新项目配置文件 settings.json
                
                仅需传入需要更新的配置参数
                
                返回更新后的全部配置参数
                """)
            ),
            tags=[_("配置")],
            response_model=Settings,
        )
        async def handle_settings(
            extract: Settings, token: str = Depends(token_dependency)
        ):
            await self.parameter.set_settings_data(extract.model_dump())
            return Settings(**self.parameter.get_settings_data())

        @self.server.get(
            "/admin/settings",
            include_in_schema=False,
            response_model=Settings,
        )
        @self.server.get(
            "/settings",
            summary=_("获取项目全局配置"),
            description=_("返回项目全部配置参数"),
            tags=[_("配置")],
            response_model=Settings,
        )
        async def get_settings(token: str = Depends(token_dependency)):
            return Settings(**self.parameter.get_settings_data())

        @self.server.get(
            "/admin/douyin/users",
            summary=_("获取抖音用户列表"),
            tags=[_("管理")],
            response_model=list[DouyinUser],
        )
        async def list_douyin_users(token: str = Depends(token_dependency)):
            rows = await self.database.list_douyin_users()
            return [DouyinUser(**self._normalize_user_row(i)) for i in rows]

        @self.server.get(
            "/admin/douyin/users/paged",
            summary=_("分页获取抖音用户列表"),
            tags=[_("管理")],
            response_model=DouyinUserPage,
        )
        async def list_douyin_users_paged(
            page: int = 1,
            page_size: int = 20,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 100)
            total = await self.database.count_douyin_users()
            rows = await self.database.list_douyin_users_paged(page, page_size)
            return DouyinUserPage(
                total=total,
                items=[DouyinUser(**self._normalize_user_row(i)) for i in rows],
            )

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}",
            summary=_("查询抖音用户"),
            tags=[_("管理")],
            response_model=DouyinUser,
        )
        async def get_douyin_user(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            row = await self.database.get_douyin_user(sec_user_id)
            if not row:
                raise HTTPException(status_code=404, detail=_("抖音用户不存在"))
            row["next_auto_update_at"] = await self._compute_next_auto_update_at(row)
            return DouyinUser(**self._normalize_user_row(row))

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/full-sync",
            summary=_("获取抖音用户全量同步进度"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def get_douyin_user_full_sync(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            row = await self.database.get_douyin_user(sec_user_id)
            if not row:
                raise HTTPException(status_code=404, detail=_("抖音用户不存在"))
            progress = self._init_user_full_sync_progress(sec_user_id)
            return DataResponse(
                message=_("请求成功"),
                data=progress,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.post(
            "/admin/douyin/users/{sec_user_id}/full-sync",
            summary=_("触发抖音用户全量同步"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def trigger_douyin_user_full_sync(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            row = await self.database.get_douyin_user(sec_user_id)
            if not row:
                raise HTTPException(status_code=404, detail=_("抖音用户不存在"))
            self._trigger_user_full_sync(sec_user_id)
            progress = self._init_user_full_sync_progress(sec_user_id)
            return DataResponse(
                message=_("已触发同步"),
                data=progress,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.post(
            "/admin/douyin/users",
            summary=_("新增抖音用户并拉取信息"),
            tags=[_("管理")],
            response_model=DouyinUser,
        )
        async def create_douyin_user(
            payload: DouyinUserCreate, token: str = Depends(token_dependency)
        ):
            sec_user_id = await self._resolve_sec_user_id(payload.sec_user_id)
            if not sec_user_id:
                raise HTTPException(status_code=400, detail=_("无法识别用户标识或链接"))
            if await self.database.get_douyin_user(sec_user_id):
                raise HTTPException(status_code=409, detail=_("抖音用户已存在"))
            data, next_cursor, has_more, cookie_id, cookie_invalid, empty_data = (
                await self._fetch_douyin_account_page_with_pool(
                    sec_user_id,
                    cursor=0,
                    count=18,
                )
            )
            if cookie_id and (data or empty_data):
                await self.database.touch_douyin_cookie(cookie_id)
            if data:
                profile_source = data[0] if data else None
                profile = (
                    self._extract_author_profile(profile_source)
                    if profile_source
                    else {"uid": "", "nickname": "", "avatar": "", "cover": ""}
                )
                work_items = [item for item in data if self._is_work_item(item)]
                video_items = [item for item in data if self._is_video_item(item)]
                if video_items:
                    uid, nickname, _ = self.extractor.preprocessing_data(
                        video_items,
                        False,
                        "post",
                        user_id=sec_user_id,
                    )
                    record = await self.database.upsert_douyin_user(
                        sec_user_id=sec_user_id,
                        uid=uid or profile.get("uid", ""),
                        nickname=nickname or profile.get("nickname", ""),
                        avatar=profile.get("avatar", ""),
                        cover=profile.get("cover", ""),
                        has_works=True,
                        status="active",
                    )
                    self._trigger_user_full_sync(
                        sec_user_id,
                        seed_data=data,
                        seed_next_cursor=next_cursor,
                        seed_has_more=has_more,
                    )
                    self._trigger_refresh_live(sec_user_id)
                    return DouyinUser(**self._normalize_user_row(record))
                if work_items:
                    record = await self.database.upsert_douyin_user(
                        sec_user_id=sec_user_id,
                        uid=profile.get("uid", ""),
                        nickname=profile.get("nickname", ""),
                        avatar=profile.get("avatar", ""),
                        cover=profile.get("cover", ""),
                        has_works=True,
                        status="active",
                    )
                    self._trigger_user_full_sync(
                        sec_user_id,
                        seed_data=data,
                        seed_next_cursor=next_cursor,
                        seed_has_more=has_more,
                    )
                    self._trigger_refresh_live(sec_user_id)
                    return DouyinUser(**self._normalize_user_row(record))
                record = await self.database.upsert_douyin_user(
                    sec_user_id=sec_user_id,
                    uid=profile.get("uid", ""),
                    nickname=profile.get("nickname", ""),
                    avatar=profile.get("avatar", ""),
                    cover=profile.get("cover", ""),
                    has_works=False,
                    status="no_works",
                )
                self._trigger_user_full_sync(
                    sec_user_id,
                    seed_data=data,
                    seed_next_cursor=next_cursor,
                    seed_has_more=has_more,
                )
                self._trigger_refresh_live(sec_user_id)
                return DouyinUser(**self._normalize_user_row(record))
            record = await self.database.upsert_douyin_user(
                sec_user_id=sec_user_id,
                uid="",
                nickname="",
                avatar="",
                cover="",
                has_works=False,
                status="no_works",
            )
            self._trigger_refresh_latest(sec_user_id)
            self._trigger_refresh_live(sec_user_id)
            return DouyinUser(**self._normalize_user_row(record))

        @self.server.get(
            "/admin/douyin/playlists",
            summary=_("获取播放列表"),
            tags=[_("管理")],
            response_model=DouyinPlaylistPage,
        )
        async def list_douyin_playlists(
            page: int = 1,
            page_size: int = 20,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 50)
            total = await self.database.count_douyin_playlists()
            rows = await self.database.list_douyin_playlists(page, page_size)
            items = [DouyinPlaylist(**row) for row in rows]
            return DouyinPlaylistPage(total=total, items=items)

        @self.server.post(
            "/admin/douyin/playlists",
            summary=_("创建播放列表"),
            tags=[_("管理")],
            response_model=DouyinPlaylist,
        )
        async def create_douyin_playlist(
            payload: DouyinPlaylistCreate,
            token: str = Depends(token_dependency),
        ):
            name = payload.name.strip()
            if not name:
                raise HTTPException(status_code=400, detail=_("名称不能为空"))
            record = await self.database.create_douyin_playlist(name)
            return DouyinPlaylist(**record)

        @self.server.get(
            "/admin/douyin/playlists/{playlist_id}",
            summary=_("获取播放列表详情"),
            tags=[_("管理")],
            response_model=DouyinPlaylist,
        )
        async def get_douyin_playlist(
            playlist_id: int,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            return DouyinPlaylist(**record)

        @self.server.delete(
            "/admin/douyin/playlists/{playlist_id}",
            summary=_("删除播放列表"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def delete_douyin_playlist(
            playlist_id: int,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            await self.database.delete_douyin_playlist(playlist_id)
            return DataResponse(
                message=_("删除成功"),
                data={"playlist_id": playlist_id},
                params={"playlist_id": playlist_id},
            )

        @self.server.post(
            "/admin/douyin/playlists/{playlist_id}/clear",
            summary=_("清空播放列表"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def clear_douyin_playlist(
            playlist_id: int,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            removed = await self.database.clear_douyin_playlist(playlist_id)
            return DataResponse(
                message=_("清空成功"),
                data={"playlist_id": playlist_id, "removed": removed},
                params={"playlist_id": playlist_id},
            )

        @self.server.get(
            "/admin/douyin/playlists/{playlist_id}/items",
            summary=_("获取播放列表内容"),
            tags=[_("管理")],
            response_model=DouyinWorkListPage,
        )
        async def list_douyin_playlist_items(
            playlist_id: int,
            page: int = 1,
            page_size: int = 12,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 50)
            total = await self.database.count_douyin_playlist_items(playlist_id)
            rows = await self.database.list_douyin_playlist_items(
                playlist_id,
                page,
                page_size,
            )
            items = [self._build_work_from_row(row) for row in rows]
            return DouyinWorkListPage(total=total, items=items)

        @self.server.post(
            "/admin/douyin/playlists/{playlist_id}/items/import",
            summary=_("导入作品到播放列表"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def import_douyin_playlist_items(
            playlist_id: int,
            payload: DouyinPlaylistImport,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            inserted = await self.database.insert_douyin_playlist_items(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("导入成功"),
                data={"playlist_id": playlist_id, "inserted": inserted},
                params={"playlist_id": playlist_id},
            )

        @self.server.post(
            "/admin/douyin/playlists/{playlist_id}/items/check",
            summary=_("检查播放列表作品"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def check_douyin_playlist_items(
            playlist_id: int,
            payload: DouyinPlaylistImport,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            exists = await self.database.list_douyin_playlist_item_ids(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("查询成功"),
                data={"exists": exists},
                params={"playlist_id": playlist_id},
            )

        @self.server.post(
            "/admin/douyin/playlists/{playlist_id}/items/remove",
            summary=_("移除播放列表作品"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def remove_douyin_playlist_items(
            playlist_id: int,
            payload: DouyinPlaylistImport,
            token: str = Depends(token_dependency),
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            removed = await self.database.delete_douyin_playlist_items(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("移除成功"),
                data={"playlist_id": playlist_id, "removed": removed},
                params={"playlist_id": playlist_id},
            )

        @self.server.get(
            "/admin/douyin/works/stored",
            summary=_("获取全部作品库"),
            tags=[_("管理")],
            response_model=DouyinWorkListPage,
        )
        async def list_douyin_works_stored(
            page: int = 1,
            page_size: int = 20,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 50)
            total = await self.database.count_douyin_works_all()
            rows = await self.database.list_douyin_works_all(
                page=page,
                page_size=page_size,
            )
            items = [self._build_work_from_row(row) for row in rows]
            return DouyinWorkListPage(total=total, items=items)

        @self.server.put(
            "/admin/douyin/users/{sec_user_id}/settings",
            summary=_("更新抖音用户设置"),
            tags=[_("管理")],
            response_model=DouyinUser,
        )
        async def update_douyin_user_settings(
            sec_user_id: str,
            payload: DouyinUserSettingsUpdate,
            token: str = Depends(token_dependency),
        ):
            await self.database.update_douyin_user_settings(
                sec_user_id,
                payload.auto_update,
                payload.update_window_start,
                payload.update_window_end,
            )
            row = await self.database.get_douyin_user(sec_user_id)
            if not row:
                raise HTTPException(status_code=404, detail=_("抖音用户不存在"))
            if payload.auto_update:
                # 开启自动下载后立即触发一次扫描与下载，避免必须等待下个计划时间点。
                self._trigger_user_auto_update_now(sec_user_id)
            row["next_auto_update_at"] = await self._compute_next_auto_update_at(row)
            return DouyinUser(**self._normalize_user_row(row))

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/works",
            summary=_("获取抖音用户作品列表"),
            tags=[_("管理")],
            response_model=DouyinWorkPage,
        )
        async def list_douyin_user_works(
            sec_user_id: str,
            cursor: int = 0,
            count: int = 18,
            token: str = Depends(token_dependency),
        ):
            count = min(max(count, 1), 50)
            data, next_cursor, has_more, cookie_id, cookie_invalid, empty_data = (
                await self._fetch_douyin_account_page_with_pool(
                    sec_user_id,
                    cursor=cursor,
                    count=count,
                )
            )
            if cookie_id and (data or empty_data):
                await self.database.touch_douyin_cookie(cookie_id)
            await self.database.update_douyin_user_fetch_time(sec_user_id)
            await self.database.clear_douyin_user_new(sec_user_id)
            work_items = [item for item in data if self._is_work_item(item)]
            items = [
                DouyinWork(**self._extract_work_brief(i, sec_user_id))
                for i in work_items
            ]
            return DouyinWorkPage(items=items, next_cursor=next_cursor, has_more=has_more)

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/works/stored",
            summary=_("获取抖音用户作品库"),
            tags=[_("管理")],
            response_model=DouyinWorkListPage,
        )
        async def list_douyin_user_works_stored(
            sec_user_id: str,
            page: int = 1,
            page_size: int = 12,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 50)
            total = await self.database.count_douyin_user_works(sec_user_id)
            rows = await self.database.list_douyin_user_works(
                sec_user_id,
                page=page,
                page_size=page_size,
            )
            items = [self._build_work_from_row(row) for row in rows]
            return DouyinWorkListPage(total=total, items=items)

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/works/stats",
            summary=_("获取抖音用户作品状态统计"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def get_douyin_user_works_stats(
            sec_user_id: str,
            token: str = Depends(token_dependency),
        ):
            stats = await self.database.summarize_douyin_user_work_status(sec_user_id)
            return DataResponse(
                message=_("请求成功"),
                data=stats,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/latest",
            summary=_("获取抖音用户当日作品"),
            tags=[_("管理")],
            response_model=DouyinDailyWorkPage,
        )
        async def list_douyin_user_latest(
            sec_user_id: str,
            page: int = 1,
            page_size: int = 12,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 50)
            today = self._today_str()
            total = await self.database.count_douyin_user_works_today(
                sec_user_id,
                today,
            )
            rows = await self.database.list_douyin_user_works_today(
                sec_user_id,
                today,
                page=page,
                page_size=page_size,
            )
            items = [self._build_work_from_row(row) for row in rows]
            return DouyinDailyWorkPage(total=total, items=items)

        @self.server.post(
            "/admin/douyin/users/{sec_user_id}/latest",
            summary=_("获取抖音用户最新作品"),
            tags=[_("管理")],
            response_model=DouyinDailyWorkPage,
        )
        async def fetch_douyin_user_latest(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            result = await self._refresh_user_latest(sec_user_id)
            row = await self.database.get_douyin_user(sec_user_id)
            if row and bool(row.get("auto_update", 0)):
                self._trigger_user_auto_update_now(sec_user_id)
            items = [DouyinWork(**i) for i in result.get("items", [])]
            return DouyinDailyWorkPage(total=len(items), items=items)

        @self.server.get(
            "/admin/douyin/users/{sec_user_id}/live",
            summary=_("获取抖音用户直播缓存"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def get_douyin_user_live_cache(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            cached = self._get_cached_live_info(sec_user_id)
            message = _("请求成功") if cached else _("暂无缓存")
            return DataResponse(
                message=message,
                data=cached,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.post(
            "/admin/douyin/users/{sec_user_id}/live",
            summary=_("获取抖音用户直播状态"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def fetch_douyin_user_live(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            live_info = await self._refresh_user_live(sec_user_id)
            return DataResponse(
                message=_("请求成功"),
                data=live_info,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.delete(
            "/admin/douyin/users/{sec_user_id}",
            summary=_("删除抖音用户"),
            tags=[_("管理")],
            status_code=204,
        )
        async def delete_douyin_user(
            sec_user_id: str, token: str = Depends(token_dependency)
        ):
            removed = await self.database.delete_douyin_user_with_works(sec_user_id)
            self._clear_live_cache(sec_user_id)
            self._notify_feed_update(
                "delete",
                {"sec_user_id": sec_user_id, "works_removed": removed},
            )

        @self.server.get(
            "/admin/douyin/cookies",
            summary=_("获取抖音 Cookie 列表"),
            tags=[_("管理")],
            response_model=list[DouyinCookie],
        )
        async def list_douyin_cookies(token: str = Depends(token_dependency)):
            rows = await self.database.list_douyin_cookies()
            return [DouyinCookie(**self._normalize_cookie_row(i)) for i in rows]

        @self.server.post(
            "/admin/douyin/cookies",
            summary=_("新增抖音 Cookie"),
            tags=[_("管理")],
            response_model=DouyinCookie,
        )
        async def create_douyin_cookie(
            payload: DouyinCookieCreate, token: str = Depends(token_dependency)
        ):
            return await self._save_douyin_cookie(payload.account, payload.cookie)

        @self.server.post(
            "/admin/douyin/cookies/clipboard",
            summary=_("从剪贴板新增抖音 Cookie"),
            tags=[_("管理")],
            response_model=DouyinCookie,
        )
        async def create_douyin_cookie_from_clipboard(
            payload: DouyinCookieClipboardCreate,
            token: str = Depends(token_dependency),
        ):
            cookie_value = self._read_clipboard_cookie()
            if not cookie_value:
                raise HTTPException(status_code=400, detail=_("剪贴板未读取到 Cookie"))
            return await self._save_douyin_cookie(payload.account, cookie_value)

        @self.server.post(
            "/admin/douyin/cookies/browser",
            summary=_("从浏览器新增抖音 Cookie"),
            tags=[_("管理")],
            response_model=DouyinCookie,
        )
        async def create_douyin_cookie_from_browser(
            payload: DouyinCookieBrowserCreate,
            token: str = Depends(token_dependency),
        ):
            cookie_dict = self._read_browser_cookie(payload.browser)
            if not cookie_dict:
                raise HTTPException(status_code=400, detail=_("未读取到 Cookie 数据"))
            return await self._save_douyin_cookie(
                payload.account,
                cookie_dict_to_str(cookie_dict),
            )

        @self.server.delete(
            "/admin/douyin/cookies/{cookie_id}",
            summary=_("删除抖音 Cookie"),
            tags=[_("管理")],
            status_code=204,
        )
        async def delete_douyin_cookie(
            cookie_id: int, token: str = Depends(token_dependency)
        ):
            await self.database.delete_douyin_cookie(cookie_id)

        @self.server.get(
            "/admin/douyin/auto-download/status",
            summary=_("获取自动下载补偿状态"),
            tags=[_("管理")],
            response_model=DataResponse,
        )
        async def get_auto_download_status(token: str = Depends(token_dependency)):
            data = dict(self._auto_compensation_status)
            data["download_enabled"] = bool(getattr(self.downloader, "download", True))
            data["upload_enabled"] = self._upload_channel_enabled()
            return DataResponse(
                message=_("请求成功"),
                data=data,
                params=None,
            )

        @self.server.get(
            "/admin/douyin/schedule",
            summary=_("获取全局计划任务设置"),
            tags=[_("管理")],
            response_model=DouyinScheduleSetting,
        )
        async def get_douyin_schedule(token: str = Depends(token_dependency)):
            setting = self._resolve_schedule_setting(
                await self.database.get_douyin_schedule()
            )
            return DouyinScheduleSetting(
                enabled=bool(setting.get("enabled")),
                times=setting.get("times_text", ""),
                expression=setting.get("expression", ""),
            )

        @self.server.post(
            "/admin/douyin/schedule",
            summary=_("更新全局计划任务设置"),
            tags=[_("管理")],
            response_model=DouyinScheduleSetting,
        )
        async def update_douyin_schedule(
            payload: DouyinScheduleSetting, token: str = Depends(token_dependency)
        ):
            raw_times = self._normalize_schedule_text(payload.times or "")
            times = self._parse_schedule_times_text(raw_times)
            if payload.enabled and not times:
                raise HTTPException(status_code=400, detail=_("未识别计划时间"))
            record = await self.database.upsert_douyin_schedule(
                payload.enabled,
                raw_times,
            )
            setting = self._resolve_schedule_setting(record)
            return DouyinScheduleSetting(
                enabled=bool(setting.get("enabled")),
                times=setting.get("times_text", ""),
                expression=setting.get("expression", ""),
            )

        @self.server.get(
            "/admin/douyin/daily/works",
            summary=_("获取当天新增作品列表"),
            tags=[_("管理")],
            response_model=DouyinDailyWorkPage,
        )
        async def list_douyin_daily_works(
            page: int = 1,
            page_size: int = 20,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 100)
            today = self._today_str()
            total = await self.database.count_douyin_works_today(today)
            rows = await self.database.list_douyin_works_today(
                today,
                page,
                page_size,
            )
            items = []
            for row in rows:
                items.append(self._build_work_from_row(row))
            return DouyinDailyWorkPage(total=total, items=items)

        @self.server.get(
            "/admin/douyin/daily/live",
            summary=_("获取当天直播用户列表"),
            tags=[_("管理")],
            response_model=DouyinUserPage,
        )
        async def list_douyin_daily_live(
            page: int = 1,
            page_size: int = 20,
            token: str = Depends(token_dependency),
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 100)
            today = self._today_str()
            total = await self.database.count_douyin_live_today(today)
            rows = await self.database.list_douyin_live_today(
                today,
                page,
                page_size,
            )
            return DouyinUserPage(
                total=total,
                items=[DouyinUser(**self._normalize_user_row(i)) for i in rows],
            )

        @self.server.get(
            "/admin/douyin/daily/feed",
            summary=_("获取当天作品与直播播放列表"),
            tags=[_("管理")],
            response_model=DouyinClientFeedPage,
        )
        async def list_douyin_daily_feed(
            page: int = 1,
            page_size: int = 30,
            token: str = Depends(token_dependency),
        ):
            return await self._build_daily_feed_page(page, page_size)

        @self.server.get(
            "/client/douyin/feed/stream",
            summary=_("订阅播放列表更新"),
            tags=[_("客户端")],
        )
        async def stream_douyin_feed(
            request: Request,
        ):
            queue: asyncio.Queue = asyncio.Queue(maxsize=10)
            self._feed_subscribers.add(queue)

            async def event_generator():
                try:
                    yield "event: ready\ndata: ok\n\n"
                    while True:
                        if await request.is_disconnected():
                            break
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=15)
                            event_type = event.get("type") or "feed"
                            payload = json.dumps(
                                event.get("data") or {},
                                ensure_ascii=False,
                            )
                            yield f"event: {event_type}\ndata: {payload}\n\n"
                        except asyncio.TimeoutError:
                            yield "event: ping\ndata: {}\n\n"
                finally:
                    self._feed_subscribers.discard(queue)

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        @self.server.get(
            "/client/douyin/daily/feed",
            summary=_("获取当天作品与直播播放列表"),
            tags=[_("客户端")],
            response_model=DouyinClientFeedPage,
        )
        async def list_douyin_daily_feed_client(
            page: int = 1,
            page_size: int = 30,
            sec_user_id: str = "",
        ):
            return await self._build_daily_feed_page(page, page_size, sec_user_id)

        @self.server.get(
            "/client/douyin/playlists",
            summary=_("获取播放列表"),
            tags=[_("客户端")],
            response_model=DouyinPlaylistPage,
        )
        async def list_douyin_playlists_client(
            page: int = 1,
            page_size: int = 50,
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 100)
            total = await self.database.count_douyin_playlists()
            rows = await self.database.list_douyin_playlists(page, page_size)
            items = [DouyinPlaylist(**row) for row in rows]
            return DouyinPlaylistPage(total=total, items=items)

        @self.server.get(
            "/client/douyin/users/with-works",
            summary=_("获取有作品的用户"),
            tags=[_("客户端")],
            response_model=DouyinUserPage,
        )
        async def list_douyin_users_with_works_client(
            page: int = 1,
            page_size: int = 200,
        ):
            page = max(page, 1)
            page_size = min(max(page_size, 1), 500)
            total = await self.database.count_douyin_users_with_works()
            rows = await self.database.list_douyin_users_with_works(page, page_size)
            items = [DouyinUser(**self._normalize_user_row(i)) for i in rows]
            return DouyinUserPage(total=total, items=items)

        @self.server.get(
            "/client/douyin/playlists/{playlist_id}/feed",
            summary=_("获取播放列表播放内容"),
            tags=[_("客户端")],
            response_model=DouyinClientFeedPage,
        )
        async def list_douyin_playlist_feed_client(
            playlist_id: int,
            page: int = 1,
            page_size: int = 30,
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            return await self._build_playlist_feed_page(
                playlist_id,
                page,
                page_size,
            )

        @self.server.post(
            "/client/douyin/playlists/{playlist_id}/items",
            summary=_("添加作品到播放列表"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def add_douyin_playlist_item_client(
            playlist_id: int,
            payload: DouyinPlaylistImport,
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            inserted = await self.database.insert_douyin_playlist_items(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("添加成功"),
                data={"playlist_id": playlist_id, "inserted": inserted},
                params={"playlist_id": playlist_id},
            )

        @self.server.post(
            "/client/douyin/playlists/{playlist_id}/items/check",
            summary=_("检查播放列表作品"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def check_douyin_playlist_items_client(
            playlist_id: int,
            payload: DouyinPlaylistImport,
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            exists = await self.database.list_douyin_playlist_item_ids(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("查询成功"),
                data={"exists": exists},
                params={"playlist_id": playlist_id},
            )

        @self.server.post(
            "/client/douyin/playlists/{playlist_id}/items/remove",
            summary=_("移除播放列表作品"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def remove_douyin_playlist_items_client(
            playlist_id: int,
            payload: DouyinPlaylistImport,
        ):
            record = await self.database.get_douyin_playlist(playlist_id)
            if not record:
                raise HTTPException(status_code=404, detail=_("播放列表不存在"))
            removed = await self.database.delete_douyin_playlist_items(
                playlist_id,
                payload.aweme_ids,
            )
            return DataResponse(
                message=_("移除成功"),
                data={"playlist_id": playlist_id, "removed": removed},
                params={"playlist_id": playlist_id},
            )

        @self.server.get(
            "/client/network",
            summary=_("获取客户端网络信息"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def get_client_network_info(request: Request):
            client_ip = self._resolve_client_ip(request)
            webdav_config = (
                self.parameter.upload.get("webdav", {})
                if isinstance(self.parameter.upload, dict)
                else {}
            )
            origin_base_url = str(webdav_config.get("origin_base_url", "")).strip()
            base_url = str(webdav_config.get("base_url", "")).strip()
            if not origin_base_url:
                origin_base_url = base_url
            return DataResponse(
                message=_("请求成功"),
                data={
                    "ip": client_ip,
                    "is_lan": self._is_lan_ip(client_ip),
                    "webdav_base_url": base_url,
                    "webdav_origin_base_url": origin_base_url,
                },
                params=None,
            )

        @self.server.get(
            "/client/douyin/detail",
            summary=_("获取单个作品播放信息"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def get_client_detail(
            request: Request,
            aweme_id: str = Query(..., min_length=6),
        ):
            aweme_id = str(aweme_id or "").strip()
            work_row = await self.database.get_douyin_work(aweme_id)
            row_work_type = str(work_row.get("work_type") or "").strip().lower()
            data = None
            cookie_id = 0
            if row_work_type != "live":
                data, cookie_id = await self._fetch_douyin_detail_with_pool(aweme_id)
                if cookie_id and data:
                    await self.database.touch_douyin_cookie(cookie_id)
            detail = self._unwrap_detail_data(data) if data else {}
            if not detail and not work_row:
                raise HTTPException(status_code=404, detail=_("未获取到作品信息"))

            author = detail.get("author") if isinstance(detail, dict) else None
            author = author if isinstance(author, dict) else {}
            is_note = self._is_note_item(detail) if detail else row_work_type == "note"
            payload_type = "note" if is_note else "video"
            raw_video_url = self._extract_detail_video_url(detail) if detail else ""
            sec_user_id = (
                author.get("sec_uid")
                or author.get("secUid")
                or author.get("sec_user_id")
                or work_row.get("sec_user_id", "")
            )
            user_row = (
                await self.database.get_douyin_user(sec_user_id)
                if sec_user_id and not author.get("nickname")
                else {}
            )
            local_file = await self._resolve_work_local_file(aweme_id, work_row)
            local_cache_url = (
                self._build_local_stream_source_url(aweme_id) if local_file else ""
            )
            upload_enabled = self._upload_channel_enabled()

            payload = {
                "aweme_id": aweme_id,
                "title": (
                    detail.get("desc", "")
                    if isinstance(detail, dict)
                    else work_row.get("desc", "") or aweme_id
                ),
                "cover": (
                    self._extract_detail_cover(detail)
                    if detail
                    else str(work_row.get("cover", ""))
                ),
                "video_url": raw_video_url,
                "audio_url": self._extract_detail_audio_url(detail) if is_note else "",
                "type": payload_type,
                "sec_user_id": sec_user_id,
                "nickname": author.get("nickname")
                or user_row.get("nickname", "")
                or work_row.get("sec_user_id", ""),
                "avatar": self._extract_first_url(author.get("avatar_larger"))
                or self._extract_first_url(author.get("avatar_medium"))
                or self._extract_first_url(author.get("avatar_thumb"))
                or user_row.get("avatar", ""),
                "video_urls": [],
                "default_video_source": "",
                "local_path": str(local_file) if local_file else "",
                "upload_enabled": upload_enabled,
            }
            if not payload["cover"]:
                payload["cover"] = user_row.get("cover", "")
            if not payload["title"] or payload["title"] == aweme_id:
                payload["title"] = work_row.get("desc", "") or aweme_id
            if row_work_type == "live" and (
                not payload["title"] or payload["title"] == aweme_id
            ):
                payload["title"] = work_row.get("desc", "") or "直播回放"

            if detail:
                width, height = self._extract_detail_size(detail)
            else:
                width = int(work_row.get("width") or 0)
                height = int(work_row.get("height") or 0)
            if (not width or not height) and payload.get("type") == "video":
                if local_file:
                    width, height = await self._probe_local_media_size(local_file)
                if (not width or not height) and row_work_type == "live":
                    live_width = int(user_row.get("live_width") or 0)
                    live_height = int(user_row.get("live_height") or 0)
                    if live_width and live_height:
                        width, height = live_width, live_height
                if not width or not height:
                    video_url = payload.get("video_url") or ""
                    if video_url:
                        width, height = await self._probe_media_size(video_url)
            payload["width"] = width
            payload["height"] = height
            if width and height:
                await self.database.update_douyin_work_size(aweme_id, width, height)

            upload_status = work_row.get("upload_status", "")
            uploaded_url = str(work_row.get("upload_destination", "")).strip()
            uploaded_origin_url = str(
                work_row.get("upload_origin_destination", "")
            ).strip()
            payload["upload_status"] = upload_status
            payload["uploaded_url"] = uploaded_url
            payload["uploaded_origin_url"] = uploaded_origin_url
            payload["upload_destination"] = uploaded_url
            payload["upload_origin_destination"] = uploaded_origin_url

            client_ip = self._resolve_client_ip(request)
            prefer_origin = self._is_lan_ip(client_ip)
            if payload.get("type") == "video":
                video_sources, default_source = self._build_detail_video_sources(
                    douyin_url=raw_video_url,
                    uploaded_url=uploaded_url,
                    uploaded_origin_url=uploaded_origin_url,
                    local_cache_url=local_cache_url,
                    prefer_origin=prefer_origin,
                    include_upload_sources=upload_enabled,
                )
                payload["video_urls"] = video_sources
                payload["default_video_source"] = default_source
                if video_sources:
                    payload["video_url"] = video_sources[0].get("url", "")
            return DataResponse(
                message=_("请求成功"),
                data=payload,
                params={"aweme_id": aweme_id},
            )

        @self.server.post(
            "/client/douyin/users/{sec_user_id}/live",
            summary=_("获取抖音用户直播状态"),
            tags=[_("客户端")],
            response_model=DataResponse,
        )
        async def fetch_douyin_user_live_client(
            sec_user_id: str,
        ):
            live_info = await self._refresh_user_live(sec_user_id)
            return DataResponse(
                message=_("请求成功"),
                data=live_info,
                params={"sec_user_id": sec_user_id},
            )

        @self.server.post(
            "/douyin/share",
            summary=_("获取分享链接重定向的完整链接"),
            description=_(
                dedent("""
                **参数**:
                
                - **text**: 包含分享链接的字符串；必需参数
                - **proxy**: 代理；可选参数
                """)
            ),
            tags=[_("抖音")],
            response_model=UrlResponse,
        )
        async def handle_share(
            extract: ShortUrl, token: str = Depends(token_dependency)
        ):
            if url := await self.handle_redirect(extract.text, extract.proxy):
                return UrlResponse(
                    message=_("请求链接成功！"),
                    url=url,
                    params=extract.model_dump(),
                )
            return UrlResponse(
                message=_("请求链接失败！"),
                url=None,
                params=extract.model_dump(),
            )

        @self.server.post(
            "/douyin/detail",
            summary=_("获取单个作品数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **detail_id**: 抖音作品 ID；必需参数
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_detail(
            extract: Detail, token: str = Depends(token_dependency)
        ):
            return await self.handle_detail(extract, False)

        @self.server.post(
            "/douyin/account",
            summary=_("获取账号作品数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **sec_user_id**: 抖音账号 sec_uid；必需参数
                - **tab**: 账号页面类型；可选参数，默认值：`post`
                - **earliest**: 作品最早发布日期；可选参数
                - **latest**: 作品最晚发布日期；可选参数
                - **pages**: 最大请求次数，仅对请求账号喜欢页数据有效；可选参数
                - **cursor**: 可选参数
                - **count**: 可选参数
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_account(
            extract: AccountPayload, token: str = Depends(token_dependency)
        ):
            return await self.handle_account(extract, False)

        @self.server.post(
            "/douyin/account/live",
            summary=_("查询账号直播状态与直播间数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **dump_html**: 是否保存原始 HTML 到本地；可选参数，默认值：True
                - **sec_user_id**: 抖音账号 sec_uid；必需参数
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_account_live(
            extract: AccountLive, token: str = Depends(token_dependency)
        ):
            return await self.handle_account_live(extract)

        @self.server.post(
            "/douyin/mix",
            summary=_("获取合集作品数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **mix_id**: 抖音合集 ID
                - **detail_id**: 属于合集的抖音作品 ID
                - **cursor**: 可选参数
                - **count**: 可选参数
                
                **`mix_id` 和 `detail_id` 二选一，只需传入其中之一即可**
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_mix(extract: Mix, token: str = Depends(token_dependency)):
            is_mix, id_ = self.generate_mix_params(
                extract.mix_id,
                extract.detail_id,
            )
            if not isinstance(is_mix, bool):
                return DataResponse(
                    message=_("参数错误！"),
                    data=None,
                    params=extract.model_dump(),
                )
            if data := await self.deal_mix_detail(
                is_mix,
                id_,
                api=True,
                source=extract.source,
                cookie=extract.cookie,
                proxy=extract.proxy,
                cursor=extract.cursor,
                count=extract.count,
            ):
                return self.success_response(extract, data)
            return self.failed_response(extract)

        @self.server.post(
            "/douyin/live",
            summary=_("获取直播数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **web_rid**: 抖音直播 web_rid
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_live(extract: Live, token: str = Depends(token_dependency)):
            # if self.check_live_params(
            #     extract.web_rid,
            #     extract.room_id,
            #     extract.sec_user_id,
            # ):
            #     if data := await self.handle_live(
            #         extract,
            #     ):
            #         return self.success_response(extract, data[0])
            #     return self.failed_response(extract)
            # return DataResponse(
            #     message=_("参数错误！"),
            #     data=None,
            #     params=extract.model_dump(),
            # )
            if data := await self.handle_live(
                extract,
            ):
                return self.success_response(extract, data[0])
            return self.failed_response(extract)

        @self.server.post(
            "/douyin/comment",
            summary=_("获取作品评论数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **detail_id**: 抖音作品 ID；必需参数
                - **pages**: 最大请求次数；可选参数
                - **cursor**: 可选参数
                - **count**: 可选参数
                - **count_reply**: 可选参数
                - **reply**: 可选参数，默认值：False
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_comment(
            extract: Comment, token: str = Depends(token_dependency)
        ):
            if data := await self.comment_handle_single(
                extract.detail_id,
                cookie=extract.cookie,
                proxy=extract.proxy,
                source=extract.source,
                pages=extract.pages,
                cursor=extract.cursor,
                count=extract.count,
                count_reply=extract.count_reply,
                reply=extract.reply,
            ):
                return self.success_response(extract, data)
            return self.failed_response(extract)

        @self.server.post(
            "/douyin/reply",
            summary=_("获取评论回复数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **detail_id**: 抖音作品 ID；必需参数
                - **comment_id**: 评论 ID；必需参数
                - **pages**: 最大请求次数；可选参数
                - **cursor**: 可选参数
                - **count**: 可选参数
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_reply(extract: Reply, token: str = Depends(token_dependency)):
            if data := await self.reply_handle(
                extract.detail_id,
                extract.comment_id,
                cookie=extract.cookie,
                proxy=extract.proxy,
                pages=extract.pages,
                cursor=extract.cursor,
                count=extract.count,
                source=extract.source,
            ):
                return self.success_response(extract, data)
            return self.failed_response(extract)

        @self.server.post(
            "/douyin/search/general",
            summary=_("获取综合搜索数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **keyword**: 关键词；必需参数
                - **offset**: 起始页码；可选参数
                - **count**: 数据数量；可选参数
                - **pages**: 总页数；可选参数
                - **sort_type**: 排序依据；可选参数
                - **publish_time**: 发布时间；可选参数
                - **duration**: 视频时长；可选参数
                - **search_range**: 搜索范围；可选参数
                - **content_type**: 内容形式；可选参数
                
                **部分参数传入规则请查阅文档**: [参数含义](https://github.com/JoeanAmier/TikTokDownloader/wiki/Documentation#%E9%87%87%E9%9B%86%E6%90%9C%E7%B4%A2%E7%BB%93%E6%9E%9C%E6%95%B0%E6%8D%AE%E6%8A%96%E9%9F%B3)
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_search_general(
            extract: GeneralSearch, token: str = Depends(token_dependency)
        ):
            return await self.handle_search(extract)

        @self.server.post(
            "/douyin/search/video",
            summary=_("获取视频搜索数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **keyword**: 关键词；必需参数
                - **offset**: 起始页码；可选参数
                - **count**: 数据数量；可选参数
                - **pages**: 总页数；可选参数
                - **sort_type**: 排序依据；可选参数
                - **publish_time**: 发布时间；可选参数
                - **duration**: 视频时长；可选参数
                - **search_range**: 搜索范围；可选参数
                
                **部分参数传入规则请查阅文档**: [参数含义](https://github.com/JoeanAmier/TikTokDownloader/wiki/Documentation#%E9%87%87%E9%9B%86%E6%90%9C%E7%B4%A2%E7%BB%93%E6%9E%9C%E6%95%B0%E6%8D%AE%E6%8A%96%E9%9F%B3)
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_search_video(
            extract: VideoSearch, token: str = Depends(token_dependency)
        ):
            return await self.handle_search(extract)

        @self.server.post(
            "/douyin/search/user",
            summary=_("获取用户搜索数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **keyword**: 关键词；必需参数
                - **offset**: 起始页码；可选参数
                - **count**: 数据数量；可选参数
                - **pages**: 总页数；可选参数
                - **douyin_user_fans**: 粉丝数量；可选参数
                - **douyin_user_type**: 用户类型；可选参数
                
                **部分参数传入规则请查阅文档**: [参数含义](https://github.com/JoeanAmier/TikTokDownloader/wiki/Documentation#%E9%87%87%E9%9B%86%E6%90%9C%E7%B4%A2%E7%BB%93%E6%9E%9C%E6%95%B0%E6%8D%AE%E6%8A%96%E9%9F%B3)
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_search_user(
            extract: UserSearch, token: str = Depends(token_dependency)
        ):
            return await self.handle_search(extract)

        @self.server.post(
            "/douyin/search/live",
            summary=_("获取直播搜索数据"),
            description=_(
                dedent("""
                **参数**:
                
                - **cookie**: 抖音 Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **keyword**: 关键词；必需参数
                - **offset**: 起始页码；可选参数
                - **count**: 数据数量；可选参数
                - **pages**: 总页数；可选参数
                """)
            ),
            tags=[_("抖音")],
            response_model=DataResponse,
        )
        async def handle_search_live(
            extract: LiveSearch, token: str = Depends(token_dependency)
        ):
            return await self.handle_search(extract)

        @self.server.post(
            "/tiktok/share",
            summary=_("获取分享链接重定向的完整链接"),
            description=_(
                dedent("""
            **参数**:

            - **text**: 包含分享链接的字符串；必需参数
            - **proxy**: 代理；可选参数
            """)
            ),
            tags=["TikTok"],
            response_model=UrlResponse,
        )
        async def handle_share_tiktok(
            extract: ShortUrl, token: str = Depends(token_dependency)
        ):
            if url := await self.handle_redirect_tiktok(extract.text, extract.proxy):
                return UrlResponse(
                    message=_("请求链接成功！"),
                    url=url,
                    params=extract.model_dump(),
                )
            return UrlResponse(
                message=_("请求链接失败！"),
                url=None,
                params=extract.model_dump(),
            )

        @self.server.post(
            "/tiktok/detail",
            summary=_("获取单个作品数据"),
            description=_(
                dedent("""
                **参数**:

                - **cookie**: TikTok Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **detail_id**: TikTok 作品 ID；必需参数
                """)
            ),
            tags=["TikTok"],
            response_model=DataResponse,
        )
        async def handle_detail_tiktok(
            extract: DetailTikTok, token: str = Depends(token_dependency)
        ):
            return await self.handle_detail(extract, True)

        @self.server.post(
            "/tiktok/account",
            summary=_("获取账号作品数据"),
            description=_(
                dedent("""
                **参数**:

                - **cookie**: TikTok Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **sec_user_id**: TikTok 账号 secUid；必需参数
                - **tab**: 账号页面类型；可选参数，默认值：`post`
                - **earliest**: 作品最早发布日期；可选参数
                - **latest**: 作品最晚发布日期；可选参数
                - **pages**: 最大请求次数，仅对请求账号喜欢页数据有效；可选参数
                - **cursor**: 可选参数
                - **count**: 可选参数
                """)
            ),
            tags=["TikTok"],
            response_model=DataResponse,
        )
        async def handle_account_tiktok(
            extract: AccountTiktok, token: str = Depends(token_dependency)
        ):
            return await self.handle_account(extract, True)

        @self.server.post(
            "/tiktok/mix",
            summary=_("获取合辑作品数据"),
            description=_(
                dedent("""
                **参数**:

                - **cookie**: TikTok Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **mix_id**: TikTok 合集 ID；必需参数
                - **cursor**: 可选参数
                - **count**: 可选参数
                """)
            ),
            tags=["TikTok"],
            response_model=DataResponse,
        )
        async def handle_mix_tiktok(
            extract: MixTikTok, token: str = Depends(token_dependency)
        ):
            if data := await self.deal_mix_detail(
                True,
                extract.mix_id,
                api=True,
                source=extract.source,
                cookie=extract.cookie,
                proxy=extract.proxy,
                cursor=extract.cursor,
                count=extract.count,
            ):
                return self.success_response(extract, data)
            return self.failed_response(extract)

        @self.server.post(
            "/tiktok/live",
            summary=_("获取直播数据"),
            description=_(
                dedent("""
                **参数**:

                - **cookie**: TikTok Cookie；可选参数
                - **proxy**: 代理；可选参数
                - **source**: 是否返回原始响应数据；可选参数，默认值：False
                - **room_id**: TikTok 直播 room_id；必需参数
                """)
            ),
            tags=["TikTok"],
            response_model=DataResponse,
        )
        async def handle_live_tiktok(
            extract: Live, token: str = Depends(token_dependency)
        ):
            if data := await self.handle_live(
                extract,
                True,
            ):
                return self.success_response(extract, data[0])
            return self.failed_response(extract)

    async def handle_search(self, extract):
        if isinstance(
            data := await self.deal_search_data(
                extract,
                extract.source,
            ),
            list,
        ):
            return self.success_response(
                extract,
                *(data, None) if any(data) else (None, _("搜索结果为空！")),
            )
        return self.failed_response(extract)

    async def handle_detail(
        self,
        extract: Detail | DetailTikTok,
        tiktok=False,
    ):
        root, params, logger = self.record.run(self.parameter)
        async with logger(root, console=self.console, **params) as record:
            if data := await self._handle_detail(
                [extract.detail_id],
                tiktok,
                record,
                True,
                extract.source,
                extract.cookie,
                extract.proxy,
            ):
                return self.success_response(extract, data[0])
            return self.failed_response(extract)

    async def handle_account(
        self,
        extract: AccountPayload | AccountTiktok,
        tiktok=False,
    ):
        if tiktok:
            if data := await self.deal_account_detail(
                0,
                extract.sec_user_id,
                tab=extract.tab,
                earliest=extract.earliest,
                latest=extract.latest,
                pages=extract.pages,
                api=True,
                source=extract.source,
                cookie=extract.cookie,
                proxy=extract.proxy,
                tiktok=tiktok,
                cursor=extract.cursor,
                count=extract.count,
            ):
                return self.success_response(extract, data)
            return self.failed_response(extract)
        data, meta, cookie_id = await self._fetch_douyin_account_data(extract)
        if cookie_id and (data or (meta or {}).get("empty_data")):
            await self.database.touch_douyin_cookie(cookie_id)
        if data:
            return self.success_response(extract, data)
        return self.failed_response(extract)

    async def handle_account_live(
        self,
        extract: AccountLive,
    ):
        live_info = await self._build_live_info(extract)
        if not live_info:
            return self.failed_response(extract)
        return self.success_response(extract, live_info)

    @staticmethod
    def success_response(
        extract,
        data: dict | list[dict],
        message: str = None,
    ):
        return DataResponse(
            message=message or _("获取数据成功！"),
            data=data,
            params=extract.model_dump(),
        )

    @staticmethod
    def failed_response(
        extract,
        message: str = None,
    ):
        return DataResponse(
            message=message or _("获取数据失败！"),
            data=None,
            params=extract.model_dump(),
        )

    @staticmethod
    def generate_mix_params(mix_id: str = None, detail_id: str = None):
        if mix_id:
            return True, mix_id
        return (False, detail_id) if detail_id else (None, None)

    @staticmethod
    def check_live_params(
        web_rid: str = None,
        room_id: str = None,
        sec_user_id: str = None,
    ) -> bool:
        return bool(web_rid or room_id and sec_user_id)

    async def handle_live(self, extract: Live | LiveTikTok, tiktok=False):
        if tiktok:
            data = await self.get_live_data_tiktok(
                extract.room_id,
                extract.cookie,
                extract.proxy,
            )
        else:
            data = await self.get_live_data(
                extract.web_rid,
                # extract.room_id,
                # extract.sec_user_id,
                cookie=extract.cookie,
                proxy=extract.proxy,
            )
        if extract.source:
            return [data]
        return await self.extractor.run(
            [data],
            None,
            "live",
            tiktok=tiktok,
        )
