from asyncio import to_thread
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from re import search, sub
from typing import TYPE_CHECKING, Callable
from unicodedata import normalize

from ..translation import _
from .webdav import WebDAVUploader

if TYPE_CHECKING:
    from ..config import Parameter

__all__ = ["UploadService", "UploadOutcome"]


@dataclass(slots=True)
class UploadOutcome:
    attempted: bool
    success: bool
    destination: str = ""
    origin_destination: str = ""
    skipped: bool = False
    reason: str = ""


class UploadService:
    DEFAULT_CONFIG = {
        "enabled": False,
        "delete_local_after_upload": False,
        "video_suffixes": ["mp4", "mov"],
        "webdav": {
            "enabled": False,
            "base_url": "",
            "origin_base_url": "",
            "username": "",
            "password": "",
            "remote_root": "/DouK-Downloader",
            "timeout": 30,
            "verify_ssl": True,
        },
    }

    def __init__(
        self,
        params: "Parameter",
        metadata_resolver: Callable[[str], dict | None] | None = None,
    ):
        self.log = params.logger
        self.root = params.root
        self.recorder = params.upload_recorder
        self.metadata_resolver = metadata_resolver
        self.config = self._normalize_config(params.upload)
        self.enabled = self.config["enabled"]
        self.delete_local_after_upload = bool(
            self.config.get("delete_local_after_upload", False)
        )
        self.video_suffixes = {
            self._normalize_suffix(i)
            for i in self.config["video_suffixes"]
            if self._normalize_suffix(i)
        }
        self.webdav = WebDAVUploader(
            self.config["webdav"],
            params.chunk,
            self.log,
        )

    @staticmethod
    def _parse_config_bool(value, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off", ""}:
                return False
        return default

    def _normalize_config(self, config: dict | None) -> dict:
        merged = deepcopy(self.DEFAULT_CONFIG)
        if isinstance(config, dict):
            merged = self._merge_dict(merged, config)
        merged["enabled"] = self._parse_config_bool(
            merged.get("enabled", False),
            default=False,
        )
        merged["delete_local_after_upload"] = self._parse_config_bool(
            merged.get("delete_local_after_upload", False),
            default=False,
        )

        suffixes = merged.get("video_suffixes")
        if not isinstance(suffixes, list):
            suffixes = self.DEFAULT_CONFIG["video_suffixes"]
        merged["video_suffixes"] = [self._normalize_suffix(i) for i in suffixes if i]
        if not merged["video_suffixes"]:
            merged["video_suffixes"] = self.DEFAULT_CONFIG["video_suffixes"]

        webdav = merged.get("webdav")
        if not isinstance(webdav, dict):
            webdav = {}
        merged["webdav"] = self._merge_dict(self.DEFAULT_CONFIG["webdav"], webdav)
        merged["webdav"]["enabled"] = self._parse_config_bool(
            merged["webdav"].get("enabled", False),
            default=False,
        )

        timeout = merged["webdav"].get("timeout")
        merged["webdav"]["timeout"] = timeout if isinstance(timeout, int) and timeout > 0 else 30
        merged["webdav"]["verify_ssl"] = self._parse_config_bool(
            merged["webdav"].get("verify_ssl", True),
            default=True,
        )
        merged["webdav"]["base_url"] = str(merged["webdav"].get("base_url", "")).strip()
        merged["webdav"]["origin_base_url"] = str(
            merged["webdav"].get("origin_base_url", "")
        ).strip()
        merged["webdav"]["username"] = str(merged["webdav"].get("username", ""))
        merged["webdav"]["password"] = str(merged["webdav"].get("password", ""))
        merged["webdav"]["remote_root"] = str(
            merged["webdav"].get("remote_root", "/DouK-Downloader")
        )
        if not merged["webdav"]["origin_base_url"]:
            merged["webdav"]["origin_base_url"] = merged["webdav"]["base_url"]

        if merged["webdav"]["enabled"] and not merged["webdav"]["base_url"]:
            self.log.warning("upload.webdav.base_url 未配置，已自动禁用 WebDAV 上传")
            merged["webdav"]["enabled"] = False

        if merged["enabled"] and not merged["webdav"]["enabled"]:
            self.log.warning("upload.enabled 已开启，但未启用任何上传通道")

        return merged

    def should_delete_local(self, outcome: UploadOutcome) -> bool:
        if not self.enabled:
            return False
        if not self.delete_local_after_upload:
            return False
        if not isinstance(outcome, UploadOutcome):
            return False
        return bool(outcome.attempted and outcome.success)

    def delete_local_file(self, path: Path) -> None:
        if not isinstance(path, Path):
            return
        try:
            if path.is_file():
                path.unlink()
        except OSError as exc:
            self.log.warning(f"删除本地文件失败: {path}, {repr(exc)}")

    @staticmethod
    def _merge_dict(base: dict, patch: dict) -> dict:
        result = deepcopy(base)
        for key, value in patch.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = UploadService._merge_dict(result[key], value)
            else:
                result[key] = value
        return result

    @staticmethod
    def _normalize_suffix(value: str) -> str:
        return str(value or "").strip().lower().lstrip(".")

    def _should_upload(self, suffix: str) -> bool:
        if not self.enabled:
            return False
        if not self.webdav.configured:
            return False
        return self._normalize_suffix(suffix) in self.video_suffixes

    def _relative_path(self, path: Path) -> Path:
        try:
            return path.relative_to(self.root)
        except ValueError:
            return Path(path.name)

    def _resolve_metadata_by_work_id(self, work_id: str) -> dict:
        if not work_id or not self.metadata_resolver:
            return {}
        try:
            metadata = self.metadata_resolver(work_id)
        except Exception as exc:
            self.log.warning(f"根据作品 ID 获取上传元数据失败: {work_id}, {repr(exc)}")
            return {}
        return metadata if isinstance(metadata, dict) else {}

    @staticmethod
    def _sanitize_text(value: str, default: str) -> str:
        text = normalize("NFKC", str(value or ""))
        text = sub(r"\s+", "", text)
        text = sub(r"[^\w\u4e00-\u9fff-]+", "", text).strip("._-")
        return text[:80] or default

    @staticmethod
    def _extract_publish_date(value: str | int | float) -> tuple[str, str]:
        if isinstance(value, (int, float)) and value > 0:
            dt = datetime.fromtimestamp(value)
            return f"{dt:%Y}", f"{dt:%Y-%m-%d}"
        text = str(value or "").strip()
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y.%m.%d %H:%M:%S",
            "%Y.%m.%d",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                return f"{dt:%Y}", f"{dt:%Y-%m-%d}"
            except ValueError:
                continue
        if m := search(r"(20\d{2})\D?([01]?\d)\D?([0-3]?\d)", text):
            year, month, day = m.groups()
            return year, f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
        return "UnknownYear", "UnknownDate"

    def _target_relative_path(
        self,
        file_path: Path,
        suffix: str,
        work_id: str,
    ) -> Path:
        metadata = self._resolve_metadata_by_work_id(work_id)
        if not metadata:
            return self._relative_path(file_path)
        author = self._sanitize_text(
            metadata.get("author", ""),
            "UnknownAuthor",
        )
        title = self._sanitize_text(
            metadata.get("title", ""),
            self._sanitize_text(file_path.stem, "UnknownTitle"),
        )
        year, publish_date = self._extract_publish_date(
            metadata.get("publish_date", ""),
        )
        filename = self._sanitize_text(
            f"{title}{publish_date}",
            title,
        )
        extension = self._normalize_suffix(suffix) or file_path.suffix.lstrip(".")
        return Path(author).joinpath(year, f"{filename}.{extension}")

    async def upload_file(
        self,
        file_path: Path,
        suffix: str,
        work_id: str = "",
    ) -> UploadOutcome:
        return await self.upload_file_with_target(
            file_path=file_path,
            suffix=suffix,
            work_id=work_id,
            remote_relative_path=None,
        )

    async def upload_file_with_target(
        self,
        file_path: Path,
        suffix: str,
        work_id: str = "",
        remote_relative_path: Path | None = None,
    ) -> UploadOutcome:
        if not self._should_upload(suffix):
            return UploadOutcome(
                attempted=False,
                success=False,
                reason="上传未启用或后缀不匹配",
            )
        if not file_path.is_file():
            return UploadOutcome(
                attempted=True,
                success=False,
                reason="文件不存在",
            )

        relative_path = (
            remote_relative_path
            if isinstance(remote_relative_path, Path)
            else self._target_relative_path(file_path, suffix, work_id)
        )
        remote_path = self.webdav.build_remote_path(relative_path)
        destination = self.webdav.destination_url(remote_path)
        origin_destination = self.webdav.destination_origin_url(remote_path)
        file_hash = await self._sha256(file_path)

        if await self.recorder.has_upload(
            file_hash,
            "webdav",
            destination,
        ):
            await self.recorder.update_upload(
                file_hash=file_hash,
                provider="webdav",
                destination=destination,
                origin_destination=origin_destination,
                local_path=str(file_path.resolve()),
                local_size=file_path.stat().st_size,
                work_id=work_id,
            )
            self.log.info(
                _("已存在上传记录，跳过重复上传: {path}").format(path=destination),
            )
            return UploadOutcome(
                attempted=True,
                success=True,
                destination=destination,
                origin_destination=origin_destination,
                skipped=True,
            )

        result = await self.webdav.ensure_uploaded(file_path, relative_path)
        if not result.success:
            self.log.warning(
                _("上传失败: {path}, 原因: {reason}").format(
                    path=result.destination,
                    reason=result.reason,
                ),
            )
            return UploadOutcome(
                attempted=True,
                success=False,
                destination=result.destination,
                origin_destination=origin_destination,
                reason=result.reason,
            )

        await self.recorder.update_upload(
            file_hash=file_hash,
            provider="webdav",
            destination=result.destination,
            origin_destination=origin_destination,
            local_path=str(file_path.resolve()),
            local_size=file_path.stat().st_size,
            work_id=work_id,
        )
        if result.already_exists:
            self.log.info(_("远端文件已存在，已补充上传记录: {path}").format(path=destination))
        else:
            self.log.info(_("上传成功: {path}").format(path=destination))
        return UploadOutcome(
            attempted=True,
            success=True,
            destination=result.destination,
            origin_destination=origin_destination,
            skipped=bool(result.already_exists),
        )

    async def _sha256(self, path: Path) -> str:
        return await to_thread(self._sha256_sync, path)

    @staticmethod
    def _sha256_sync(path: Path) -> str:
        digest = sha256()
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()
