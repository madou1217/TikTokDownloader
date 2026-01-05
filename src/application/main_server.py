import asyncio
import re
from datetime import datetime, time, timedelta
from hashlib import sha256
import json
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pyperclip import paste
from uvicorn import Config, Server

from ..custom import (
    __VERSION__,
    REPOSITORY,
    SERVER_HOST,
    SERVER_PORT,
    VERSION_BETA,
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
from ..module import Cookie
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
    USER_ID_PATTERN = re.compile(r"^MS4wL[0-9A-Za-z_-]+$")
    USER_ID_SCAN_PATTERN = re.compile(r"MS4wL[0-9A-Za-z_-]+")

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
    def _extract_work_cover(cls, item: dict) -> str:
        video = item.get("video") if isinstance(item, dict) else None
        if not isinstance(video, dict):
            return ""
        for key in ("cover", "origin_cover", "dynamic_cover"):
            url = cls._extract_first_url(video.get(key))
            if url:
                return url
        return ""

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
        return {
            "sec_user_id": author.get("sec_uid", "") or fallback_sec_user_id,
            "aweme_id": aweme_id,
            "desc": desc,
            "create_ts": create_ts,
            "create_time": cls._format_timestamp(create_ts),
            "create_date": create_date,
            "nickname": author.get("nickname", ""),
            "cover": cls._extract_work_cover(item),
            "play_count": cls._extract_play_count(item),
        }

    def _build_work_from_row(self, row: dict) -> DouyinWork:
        create_ts = int(row.get("create_ts") or 0)
        return DouyinWork(
            sec_user_id=row.get("sec_user_id", ""),
            aweme_id=row.get("aweme_id", ""),
            desc=row.get("desc", ""),
            create_ts=create_ts,
            create_time=self._format_timestamp(create_ts),
            create_date=row.get("create_date", ""),
            nickname=row.get("nickname", ""),
            cover=row.get("cover", ""),
            play_count=int(row.get("play_count") or 0),
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

    async def _refresh_user_latest(self, sec_user_id: str) -> dict:
        data, next_cursor, has_more, cookie_id, cookie_invalid, empty_data = await self._fetch_douyin_account_page_with_pool(
            sec_user_id,
            cursor=0,
            count=18,
        )
        if cookie_id and (data or empty_data):
            await self.database.touch_douyin_cookie(cookie_id)
        video_items = [item for item in data if self._is_video_item(item)]
        profile_source = video_items[0] if video_items else (data[0] if data else None)
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
        works = [self._extract_work_brief(item, sec_user_id) for item in video_items]
        today = self._today_str()
        today_works = [item for item in works if item.get("create_date") == today]
        inserted = await self.database.insert_douyin_works(today_works)
        if today_works:
            await self.database.update_douyin_user_new(sec_user_id, True)
        return {
            "items": today_works,
            "inserted": inserted,
            "total": len(today_works),
        }

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
        extract = AccountLive(
            sec_user_id=sec_user_id,
            dump_html=False,
        )
        live_info = await self._build_live_info(extract)
        is_live = bool(live_info and live_info.get("live_status"))
        await self.database.update_douyin_user_live(sec_user_id, is_live)
        if live_info:
            self._cache_live_info(sec_user_id, live_info)
        return live_info or {}

    async def _schedule_tick(self) -> None:
        setting = self._resolve_schedule_setting(
            await self.database.get_douyin_schedule()
        )
        if not setting.get("enabled"):
            return
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
            await self._refresh_user_latest(user.get("sec_user_id", ""))
            await self._refresh_user_live(user.get("sec_user_id", ""))

    async def _run_schedule_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            try:
                await self._schedule_tick()
            except Exception:
                self.logger.error(_("计划任务执行异常"))

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

        @self.server.on_event("startup")
        async def startup_schedule():
            if not self._schedule_task:
                self._schedule_task = asyncio.create_task(self._run_schedule_loop())
            if not self._refresh_workers:
                self._refresh_workers = [
                    asyncio.create_task(self._refresh_latest_worker(index))
                    for index in range(self.REFRESH_CONCURRENCY)
                ]

        @self.server.on_event("shutdown")
        async def shutdown_schedule():
            if self._schedule_task:
                self._schedule_task.cancel()
                self._schedule_task = None
            if self._refresh_workers:
                for task in self._refresh_workers:
                    task.cancel()
                self._refresh_workers = []

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
            headers = {
                "User-Agent": self.parameter.headers.get("User-Agent", ""),
                "Referer": "https://www.douyin.com/",
            }
            try:
                resp = await self.parameter.client.get(url, headers=headers)
                resp.raise_for_status()
            except Exception:
                raise HTTPException(status_code=502, detail=_("图片获取失败"))
            content_type = resp.headers.get("Content-Type") or "image/jpeg"
            return Response(content=resp.content, media_type=content_type)

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
                    self._trigger_refresh_latest(sec_user_id)
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
                self._trigger_refresh_latest(sec_user_id)
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
            return DouyinUser(**self._normalize_user_row(record))

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
            video_items = [item for item in data if self._is_video_item(item)]
            items = [
                DouyinWork(**self._extract_work_brief(i, sec_user_id))
                for i in video_items
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
            await self.database.delete_douyin_user(sec_user_id)

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
