from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import AsyncGenerator
from urllib.parse import quote
from xml.etree import ElementTree

from aiofiles import open
from httpx import AsyncClient, BasicAuth

__all__ = ["WebDAVUploader", "WebDAVResult"]


@dataclass(slots=True)
class WebDAVResult:
    success: bool
    destination: str
    already_exists: bool = False
    reason: str = ""


class WebDAVUploader:
    TEMP_SUFFIX = ".uploading"

    def __init__(
        self,
        config: dict,
        chunk_size: int,
        logger,
    ):
        self.enabled = bool(config.get("enabled", False))
        self.base_url = str(config.get("base_url", "")).rstrip("/")
        origin_base_url = str(config.get("origin_base_url", "")).rstrip("/")
        self.origin_base_url = origin_base_url or self.base_url
        self.username = str(config.get("username", ""))
        self.password = str(config.get("password", ""))
        self.remote_root = self._normalize_remote_root(config.get("remote_root", ""))
        self.timeout = int(config.get("timeout", 30))
        self.verify_ssl = bool(config.get("verify_ssl", True))
        self.chunk_size = max(int(chunk_size or 0), 1024 * 256)
        self.log = logger

    @staticmethod
    def _normalize_remote_root(value: str) -> str:
        text = str(value or "").strip()
        if not text:
            return "/"
        path = PurePosixPath("/", text)
        return f"/{path.as_posix().strip('/')}" if path.as_posix() != "/" else "/"

    @property
    def configured(self) -> bool:
        return self.enabled and bool(self.base_url)

    def build_remote_path(self, relative_path: Path) -> str:
        path = PurePosixPath(relative_path.as_posix())
        if self.remote_root == "/":
            return f"/{path.as_posix().strip('/')}"
        root = PurePosixPath(self.remote_root)
        return f"/{(root / path).as_posix().strip('/')}"

    def destination_url(self, remote_path: str) -> str:
        return f"{self.base_url}{self._encode_remote_path(remote_path)}"

    def destination_origin_url(self, remote_path: str) -> str:
        return f"{self.origin_base_url}{self._encode_remote_path(remote_path)}"

    @staticmethod
    def _encode_remote_path(remote_path: str) -> str:
        segments = [
            quote(segment, safe="")
            for segment in str(remote_path).strip("/").split("/")
            if segment
        ]
        return "/" + "/".join(segments)

    async def ensure_uploaded(
        self,
        local_file: Path,
        relative_path: Path,
    ) -> WebDAVResult:
        remote_path = self.build_remote_path(relative_path)
        destination = self.destination_url(remote_path)
        if not self.configured:
            return WebDAVResult(
                success=False,
                destination=destination,
                reason="WebDAV 未启用或配置不完整",
            )
        try:
            async with self._create_client() as client:
                size = local_file.stat().st_size
                final_size = await self._get_remote_size(client, remote_path)
                if final_size == size:
                    return WebDAVResult(
                        success=True,
                        destination=destination,
                        already_exists=True,
                    )

                if not await self._ensure_remote_directory(client, remote_path):
                    return WebDAVResult(
                        success=False,
                        destination=destination,
                        reason="创建 WebDAV 目录失败",
                    )

                temp_path = f"{remote_path}{self.TEMP_SUFFIX}"
                resume_from = await self._get_remote_size(client, temp_path)
                if resume_from is None:
                    resume_from = 0
                if resume_from > size:
                    await self._delete(client, temp_path)
                    resume_from = 0

                if resume_from < size:
                    ok = await self._put_file(
                        client,
                        temp_path,
                        local_file,
                        start=resume_from,
                        total=size,
                    )
                    if not ok:
                        return WebDAVResult(
                            success=False,
                            destination=destination,
                            reason="上传失败",
                        )

                    current = await self._get_remote_size(client, temp_path)
                    if current != size:
                        if resume_from:
                            self.log.warning(
                                "WebDAV 服务端不支持断点续传，已回退为整文件重传"
                            )
                            await self._delete(client, temp_path)
                            ok = await self._put_file(
                                client,
                                temp_path,
                                local_file,
                                start=0,
                                total=size,
                            )
                            if not ok:
                                return WebDAVResult(
                                    success=False,
                                    destination=destination,
                                    reason="回退重传失败",
                                )
                            current = await self._get_remote_size(client, temp_path)
                        if current != size:
                            return WebDAVResult(
                                success=False,
                                destination=destination,
                                reason="上传后文件大小校验失败",
                            )

                moved = await self._move(client, temp_path, remote_path)
                if not moved:
                    return WebDAVResult(
                        success=False,
                        destination=destination,
                        reason="上传完成但重命名失败",
                    )

                return WebDAVResult(success=True, destination=destination)
        except Exception as exc:
            return WebDAVResult(
                success=False,
                destination=destination,
                reason=repr(exc),
            )

    def _create_client(self) -> AsyncClient:
        auth = None
        if self.username:
            auth = BasicAuth(self.username, self.password)
        return AsyncClient(
            timeout=self.timeout,
            verify=self.verify_ssl,
            follow_redirects=True,
            auth=auth,
            headers={
                "User-Agent": "DouK-Downloader-WebDAV",
            },
        )

    async def _ensure_remote_directory(self, client: AsyncClient, remote_path: str) -> bool:
        parent = PurePosixPath(remote_path).parent
        if str(parent) in ("", ".", "/"):
            return True

        current = PurePosixPath("/")
        for segment in parent.parts:
            if segment == "/":
                continue
            current = current / segment
            url = self.destination_url(str(current))
            response = await client.request("MKCOL", url)
            if response.status_code in (200, 201, 204, 301, 302, 405):
                continue
            if response.status_code == 400:
                # 一些 WebDAV 服务（如部分 NAS）在目录已存在时返回 400
                if await self._resource_exists(client, str(current)):
                    continue
                return False
            if response.status_code == 409:
                return False
            if response.status_code in (401, 403):
                self.log.warning(
                    f"创建 WebDAV 目录失败，权限不足: {response.status_code} {url}"
                )
                return False
            if response.status_code >= 400:
                self.log.warning(
                    f"创建 WebDAV 目录失败: {response.status_code} {url}"
                )
                return False
        return True

    async def _resource_exists(self, client: AsyncClient, remote_path: str) -> bool:
        url = self.destination_url(remote_path)
        head = await client.head(url)
        if head.status_code in (200, 204):
            return True
        if head.status_code == 404:
            return False
        prop = await client.request(
            "PROPFIND",
            url,
            headers={"Depth": "0"},
        )
        return prop.status_code in (200, 207)

    async def _get_remote_size(
        self,
        client: AsyncClient,
        remote_path: str,
    ) -> int | None:
        url = self.destination_url(remote_path)
        response = await client.head(url)
        if response.status_code == 404:
            return None
        if response.status_code in (200, 204):
            return self._parse_content_length(response.headers.get("Content-Length"))
        if response.status_code in (405, 501):
            return await self._propfind_size(client, remote_path)
        if response.status_code >= 400:
            return await self._propfind_size(client, remote_path)
        return self._parse_content_length(response.headers.get("Content-Length"))

    async def _propfind_size(self, client: AsyncClient, remote_path: str) -> int | None:
        url = self.destination_url(remote_path)
        response = await client.request(
            "PROPFIND",
            url,
            headers={"Depth": "0"},
        )
        if response.status_code == 404:
            return None
        if response.status_code not in (200, 207):
            return None
        try:
            root = ElementTree.fromstring(response.text)
        except ElementTree.ParseError:
            return None
        for node in root.iter():
            if not str(node.tag).endswith("getcontentlength"):
                continue
            text = str(node.text or "").strip()
            if text.isdigit():
                return int(text)
        return None

    @staticmethod
    def _parse_content_length(value: str | None) -> int | None:
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    async def _put_file(
        self,
        client: AsyncClient,
        remote_path: str,
        local_file: Path,
        start: int,
        total: int,
    ) -> bool:
        url = self.destination_url(remote_path)
        headers = {
            "Content-Length": str(max(total - start, 0)),
        }
        if start:
            headers["Content-Range"] = f"bytes {start}-{total - 1}/{total}"
        response = await client.put(
            url,
            headers=headers,
            content=self._iter_file(local_file, start),
        )
        if response.status_code in (200, 201, 204):
            return True
        self.log.warning(
            f"WebDAV 上传失败: {response.status_code}, URL: {url}, start={start}"
        )
        return False

    async def _move(
        self,
        client: AsyncClient,
        from_path: str,
        to_path: str,
    ) -> bool:
        source_url = self.destination_url(from_path)
        destination_url = self.destination_url(to_path)
        response = await client.request(
            "MOVE",
            source_url,
            headers={
                "Destination": destination_url,
                "Overwrite": "T",
            },
        )
        if response.status_code in (200, 201, 204):
            return True
        self.log.warning(
            f"WebDAV MOVE 失败: {response.status_code}, {source_url} -> {destination_url}"
        )
        return False

    async def _delete(self, client: AsyncClient, remote_path: str) -> bool:
        url = self.destination_url(remote_path)
        response = await client.delete(url)
        return response.status_code in (200, 202, 204, 404)

    async def _iter_file(
        self,
        local_file: Path,
        start: int,
    ) -> AsyncGenerator[bytes, None]:
        async with open(local_file, "rb") as file:
            if start:
                await file.seek(start)
            while chunk := await file.read(self.chunk_size):
                yield chunk
