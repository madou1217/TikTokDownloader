import re
from typing import TYPE_CHECKING, Union
from urllib.parse import unquote

from httpx import get

from src.interface.template import API
from src.tools import DownloaderError, Retry, capture_error_request, timestamp
from src.custom import PROJECT_ROOT, wait
from src.translation import _

if TYPE_CHECKING:
    from src.config import Parameter
    from src.testers import Params


class AccountLive(API):
    """
    账号直播状态检测
    仅解析：
    - sec_user_id
    - web_rid
    - room_id
    - live_status
    """

    user_page = f"{API.domain}user/"
    profile_api = f"{API.domain}aweme/v1/web/user/profile/other/"
    live_reflow_api = "https://webcast.amemv.com/webcast/room/reflow/info/"

    def __init__(
        self,
        params: Union["Parameter", "Params"],
        sec_user_id: str,
        cookie: str = "",
        proxy: str = None,
        dump_html: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(params, cookie, proxy, *args, **kwargs)
        self.sec_user_id = sec_user_id
        self.room_id: str | None = None
        self.web_rid: str | None = None
        self.live_status: bool = False
        self.black_headers = params.headers_download
        self.response_text = ""
        self.dump_html = dump_html
        self.cache = getattr(params, "cache", PROJECT_ROOT.joinpath("Cache"))
        self.cache.mkdir(exist_ok=True)
        self.text = _("账号直播状态")

    @Retry.retry
    @capture_error_request
    async def request(
        self,
        url: str,
        method: str = "GET",
        headers: dict = None,
    ):
        headers = headers or self.headers
        match (method, bool(self.proxy)):
            case ("GET", False):
                response = await self.client.get(
                    url,
                    headers=headers,
                )
            case ("GET", True):
                response = get(
                    url,
                    headers=headers,
                    proxy=self.proxy,
                    follow_redirects=True,
                    verify=False,
                    timeout=self.timeout,
                )
            case _:
                raise DownloaderError
        response.raise_for_status()
        self.response_text = response.text
        await wait()
        return self.response_text

    async def run(self):
        """
        请求用户主页 HTML，并解析直播状态
        """
        url = f"{self.user_page}{self.sec_user_id}?from_tab_name=live"
        self.set_referer(url)

        self.response_text = ""
        if await self.request(
            url=url,
            method="GET",
        ) is None:
            return None

        self.parse_live_info(self.response_text)
        if not self.room_id and not self.web_rid:
            if profile_data := await self._fetch_profile_data():
                self._parse_profile_live_info(profile_data)
                self._scan_for_web_rid(profile_data)
        self._normalize_room_ids()
        if self.room_id and not self.web_rid:
            if reflow_data := await self._fetch_reflow_data():
                self._scan_for_web_rid(reflow_data)
                self._normalize_room_ids()
        if self.room_id and not self.web_rid:
            await self._resolve_web_rid_from_live_page()
            self._normalize_room_ids()

        result = {
            "sec_user_id": self.sec_user_id,
            "web_rid": self.web_rid,
            "room_id": self.room_id,
            "room_url": self._build_room_url(),
            "live_status": self.live_status,
        }
        if dump_path := self._dump_response():
            result["dump_path"] = dump_path
        return result

    def parse_live_info(self, text: str):
        """
        从 HTML / JS 文本中解析直播状态与 room_id
        """
        self.room_id = self._extract_room_id(text)
        self.web_rid = self._extract_web_rid(text)
        if not self.room_id and not self.web_rid:
            decoded = unquote(text)
            if decoded != text:
                self.room_id = self._extract_room_id(decoded)
                self.web_rid = self._extract_web_rid(decoded)

        self.live_status = bool(
            self.room_id
            or self.web_rid
            or re.search(r'"roomData"\s*:\s*\{', text)
            or "直播中" in text
        )

    async def _fetch_profile_data(self) -> dict | None:
        self.set_referer(f"{self.domain}user/{self.sec_user_id}")
        params = self.params | {
            "sec_user_id": self.sec_user_id,
            "publish_video_strategy_type": "2",
            "personal_center_strategy": "1",
            "profile_other_record_enable": "1",
            "land_to": "1",
            "version_code": "170400",
            "version_name": "17.4.0",
        }
        return await self.request_data(
            self.profile_api,
            params=params,
        )

    async def _fetch_reflow_data(self) -> dict | None:
        if not self.room_id:
            return None
        params = {
            "type_id": "0",
            "live_id": "1",
            "room_id": self.room_id,
            "sec_user_id": self.sec_user_id,
            "app_id": "1128",
        }
        return await self.request_data(
            self.live_reflow_api,
            params=params,
            headers=self.black_headers,
        )

    def _parse_profile_live_info(self, data_dict: dict):
        if not isinstance(data_dict, dict):
            return
        user = data_dict.get("user")
        if not isinstance(user, dict):
            data = data_dict.get("data")
            if isinstance(data, dict):
                user = data.get("user") or data
        if not isinstance(user, dict):
            return
        self._extract_room_info(user)
        for key in ("live_room", "room", "live"):
            if not self.room_id and isinstance(user.get(key), dict):
                self._extract_room_info(user.get(key))
        live_status = self._get_dict_value(
            user,
            ("live_status", "liveStatus", "is_live", "isLive"),
        )
        if live_status is not None:
            self.live_status = bool(live_status)
        if self.room_id:
            self.live_status = True

    def _normalize_room_ids(self):
        if self.web_rid or not self.room_id:
            return
        self._accept_web_rid(self.room_id)

    def _scan_for_web_rid(self, data):
        stack = [data]
        while stack and not self.web_rid:
            current = stack.pop()
            if isinstance(current, dict):
                self._extract_room_info(current)
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)
            elif isinstance(current, str):
                self._accept_web_rid(self._extract_web_rid(current))

    def _accept_web_rid(self, value: str | None):
        if not value:
            return
        if value.isdigit() and len(value) < 16:
            self.web_rid = value

    async def _resolve_web_rid_from_live_page(self):
        url = f"https://live.douyin.com/{self.room_id}"
        response = await self._request_follow_redirects(url)
        if not response:
            return
        self._accept_web_rid(self._extract_web_rid(str(response.url)))
        if not self.web_rid:
            self._accept_web_rid(self._extract_web_rid(response.text))

    async def _request_follow_redirects(
        self,
        url: str,
        headers: dict = None,
    ):
        headers = headers or self.headers
        match bool(self.proxy):
            case False:
                response = await self.client.get(
                    url,
                    headers=headers,
                    follow_redirects=True,
                )
            case True:
                response = get(
                    url,
                    headers=headers,
                    proxy=self.proxy,
                    follow_redirects=True,
                    verify=False,
                    timeout=self.timeout,
                )
            case _:
                raise DownloaderError
        response.raise_for_status()
        await wait()
        return response

    def _extract_room_info(self, data: dict):
        if not isinstance(data, dict):
            return
        if not self.room_id:
            room_id = self._get_dict_value(
                data,
                ("room_id", "room_id_str", "roomId", "roomIdStr"),
            )
            if room_id and str(room_id) != "0":
                self.room_id = str(room_id)
        if not self.web_rid:
            web_rid = self._get_dict_value(
                data,
                ("web_rid", "webRid"),
            )
            if web_rid and str(web_rid) != "0":
                self._accept_web_rid(str(web_rid))

    @staticmethod
    def _get_dict_value(data: dict, keys: tuple[str, ...]):
        for key in keys:
            if key in data:
                return data.get(key)
        return None

    @staticmethod
    def _extract_room_id(text: str) -> str | None:
        patterns = (
            r'\"room_id\"\s*:\s*\"(\d+)\"',
            r'\"room_id\"\s*:\s*(\d+)',
            r'\"roomId\"\s*:\s*\"(\d+)\"',
            r'\"roomId\"\s*:\s*(\d+)',
            r"%22room_id%22%3A%22(\d+)",
            r"%22roomId%22%3A%22(\d+)",
        )
        for pattern in patterns:
            if match := re.search(pattern, text):
                return match.group(1)
        return None

    @staticmethod
    def _extract_web_rid(text: str) -> str | None:
        patterns = (
            r"webRid=(\d+)",
            r"web_rid=(\d+)",
            r'\"webRid\"\s*:\s*\"(\d+)\"',
            r'\"webRid\"\s*:\s*(\d+)',
            r'\"web_rid\"\s*:\s*\"(\d+)\"',
            r'\"web_rid\"\s*:\s*(\d+)',
            r"\\\"webRid\\\":\\\"(\d+?)\\\"",
            r"\\\"web_rid\\\":\\\"(\d+?)\\\"",
            r"%22webRid%22%3A%22(\d+)",
            r"%22web_rid%22%3A%22(\d+)",
            r"live\.douyin\.com\\?/(\d+)",
        )
        for pattern in patterns:
            if match := re.search(pattern, text):
                return match.group(1)
        return None

    def _build_room_url(self) -> str | None:
        if self.web_rid:
            return f"https://live.douyin.com/{self.web_rid}"
        return None

    def _dump_response(self) -> str | None:
        if not self.dump_html or not self.response_text:
            return None
        try:
            name = f"account_live_{self.sec_user_id}_{timestamp()}.html"
            path = self.cache.joinpath(name)
            path.write_text(self.response_text, encoding="utf-8")
            return str(path)
        except OSError as exc:
            self.log.error(_("保存 HTML 失败: {error}").format(error=exc))
            return None

        if self.live_status:
            self.log.info(
                _("账号 {sec_user_id} 正在直播，room_id={room_id}").format(
                    sec_user_id=self.sec_user_id,
                    room_id=self.room_id,
                )
            )
        else:
            self.log.info(
                _("账号 {sec_user_id} 未在直播").format(
                    sec_user_id=self.sec_user_id
                )
            )


# -----------------------------
# 测试
# -----------------------------
async def test():
    from src.testers import Params

    async with Params() as params:
        live = AccountLive(
            params,
            sec_user_id="MS4wLjABAAAAaxttuvZ6F4RKq2DJ7P81bsvd7UTjCMEouIRMK22k6yIhyEHBau1QjcGgf-Cu1eiS",
        )
        print(await live.run())


if __name__ == "__main__":
    from asyncio import run

    run(test())
