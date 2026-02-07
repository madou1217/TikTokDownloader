from asyncio import (
    FIRST_COMPLETED,
    Event,
    create_subprocess_exec,
    create_task,
    sleep,
    to_thread,
    wait,
)
from asyncio.subprocess import DEVNULL, PIPE
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from re import sub
from shutil import which
from typing import TYPE_CHECKING
from unicodedata import normalize

from ..uploader import UploadOutcome, UploadService

if TYPE_CHECKING:
    from ..config import Parameter
    from ..manager import Database

__all__ = ["DouyinLiveRecorder"]


@dataclass(slots=True)
class _LiveSession:
    record_id: int
    sec_user_id: str
    room_id: str
    web_rid: str
    nickname: str
    title: str
    stream_url: str
    cover_url: str
    started_at: datetime
    local_root: Path
    segment_dir: Path
    output_file: Path
    cover_file: Path
    width: int = 0
    height: int = 0
    stop_event: Event = field(default_factory=Event)
    task: object | None = None
    process: object | None = None
    retry_count: int = 0


class DouyinLiveRecorder:
    MONITOR_INTERVAL = 30
    SEGMENT_SECONDS = 30
    OFFLINE_THRESHOLD = 3
    SAVE_FOLDER = "LiveRecord"

    def __init__(
        self,
        params: "Parameter",
        database: "Database",
    ):
        self.params = params
        self.database = database
        self.log = params.logger
        self.root = params.root
        self.ffmpeg_path = params.ffmpeg.path
        self.proxy = params.proxy
        self.user_agent = params.headers_download.get("User-Agent", "")
        self.uploader = UploadService(params)

        self.sessions: dict[str, _LiveSession] = {}
        self.offline_hits: dict[str, int] = {}

        self.enabled = bool(self.ffmpeg_path)
        self.monitor_interval = self.MONITOR_INTERVAL
        self.segment_seconds = self.SEGMENT_SECONDS
        self.offline_threshold = self.OFFLINE_THRESHOLD
        self.save_folder = self.SAVE_FOLDER
        if not self.ffmpeg_path:
            self.log.warning("未检测到 ffmpeg，直播录制功能自动禁用")

    @staticmethod
    def _safe_text(value: str, default: str) -> str:
        text = normalize("NFKC", str(value or ""))
        text = sub(r"\s+", "", text)
        text = sub(r"[^\w\u4e00-\u9fff-]+", "", text).strip("._-")
        return text[:80] or default

    @staticmethod
    def _pick_stream_url(room: dict) -> str:
        if not isinstance(room, dict):
            return ""
        hls_map = room.get("hls_pull_url_map") or {}
        if isinstance(hls_map, dict):
            for value in hls_map.values():
                if value:
                    return str(value)
        flv_map = room.get("flv_pull_url") or {}
        if isinstance(flv_map, dict):
            for value in flv_map.values():
                if value:
                    return str(value)
        return ""

    def _build_session(self, sec_user_id: str, live_info: dict) -> _LiveSession | None:
        room = live_info.get("room") if isinstance(live_info, dict) else {}
        if not isinstance(room, dict):
            return None
        stream_url = self._pick_stream_url(room)
        if not stream_url:
            return None
        cover_url = str(room.get("cover") or "")
        width = int(room.get("width") or 0)
        height = int(room.get("height") or 0)

        now = datetime.now()
        nickname = self._safe_text(room.get("nickname", ""), sec_user_id)
        title = self._safe_text(room.get("title", ""), "直播")
        date_mark = now.strftime("%Y-%m-%d_%H-%M-%S")
        file_title = self._safe_text(f"直播-{title}-{date_mark}", f"直播-{date_mark}")

        local_root = self.root.joinpath(self.save_folder, nickname, f"{now:%Y}")
        local_root.mkdir(parents=True, exist_ok=True)
        segment_dir = local_root.joinpath(f".segments_{sec_user_id}_{now:%Y%m%d_%H%M%S}")
        segment_dir.mkdir(parents=True, exist_ok=True)
        output_file = local_root.joinpath(f"{file_title}.mp4")
        cover_file = local_root.joinpath(f"{file_title}.jpeg")

        return _LiveSession(
            record_id=0,
            sec_user_id=sec_user_id,
            room_id=str(live_info.get("room_id") or ""),
            web_rid=str(live_info.get("web_rid") or ""),
            nickname=nickname,
            title=title,
            stream_url=stream_url,
            cover_url=cover_url,
            started_at=now,
            local_root=local_root,
            segment_dir=segment_dir,
            output_file=output_file,
            cover_file=cover_file,
            width=width,
            height=height,
        )

    async def ensure_recording(self, sec_user_id: str, live_info: dict) -> None:
        if not self.enabled or not sec_user_id:
            return
        self.offline_hits[sec_user_id] = 0

        session = self.sessions.get(sec_user_id)
        if session and session.task and not session.task.done():
            return

        session = self._build_session(sec_user_id, live_info)
        if not session:
            return

        session.record_id = await self.database.create_douyin_live_record(
            sec_user_id=session.sec_user_id,
            room_id=session.room_id,
            web_rid=session.web_rid,
            nickname=session.nickname,
            title=session.title,
            stream_url=session.stream_url,
            local_root=str(session.local_root.resolve()),
            segment_dir=str(session.segment_dir.resolve()),
            output_file=str(session.output_file.resolve()),
        )

        session.task = create_task(self._record_loop(session))
        self.sessions[sec_user_id] = session
        self.log.info(
            f"已启动直播录制任务: sec_user_id={sec_user_id}, file={session.output_file.name}"
        )

    async def mark_offline(self, sec_user_id: str, force: bool = False) -> None:
        if not sec_user_id:
            return
        if sec_user_id not in self.sessions:
            return
        threshold = 1 if force else self.offline_threshold
        count = self.offline_hits.get(sec_user_id, 0) + 1
        self.offline_hits[sec_user_id] = count
        if count < threshold:
            return
        session = self.sessions.get(sec_user_id)
        if session:
            session.stop_event.set()

    async def prune_sessions(self, active_sec_user_ids: set[str]) -> None:
        for sec_user_id in list(self.sessions):
            if sec_user_id in active_sec_user_ids:
                continue
            await self.mark_offline(sec_user_id, force=True)

    async def shutdown(self) -> None:
        for session in list(self.sessions.values()):
            session.stop_event.set()
        for session in list(self.sessions.values()):
            if session.task:
                with suppress(Exception):
                    await session.task

    def _next_segment_number(self, segment_dir: Path) -> int:
        numbers = []
        for file in segment_dir.glob("*.ts"):
            try:
                numbers.append(int(file.stem))
            except ValueError:
                continue
        return max(numbers, default=-1) + 1

    def _build_record_command(self, session: _LiveSession) -> list[str]:
        output_pattern = str(session.segment_dir.joinpath("%08d.ts"))
        command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-rw_timeout",
            str(30 * 1000 * 1000),
            "-reconnect",
            "1",
            "-reconnect_streamed",
            "1",
            "-reconnect_at_eof",
            "1",
            "-reconnect_delay_max",
            "15",
            "-user_agent",
            self.user_agent,
            "-protocol_whitelist",
            "rtmp,crypto,file,http,https,tcp,tls,udp,rtp,httpproxy",
        ]
        if self.proxy:
            command.extend(["-http_proxy", self.proxy])
        command.extend(
            [
                "-i",
                session.stream_url,
                "-map",
                "0",
                "-c",
                "copy",
                "-f",
                "segment",
                "-segment_time",
                str(self.segment_seconds),
                "-segment_format",
                "mpegts",
                "-reset_timestamps",
                "1",
                "-segment_start_number",
                str(self._next_segment_number(session.segment_dir)),
                output_pattern,
            ]
        )
        return command

    async def _run_record_once(self, session: _LiveSession) -> int:
        command = self._build_record_command(session)
        process = await create_subprocess_exec(
            *command,
            stdout=DEVNULL,
            stderr=DEVNULL,
        )
        session.process = process

        wait_task = create_task(process.wait())
        stop_task = create_task(session.stop_event.wait())
        done, pending = await wait(
            {wait_task, stop_task},
            return_when=FIRST_COMPLETED,
        )

        if stop_task in done and process.returncode is None:
            process.terminate()
            with suppress(Exception):
                await process.wait()

        for task in pending:
            task.cancel()
            with suppress(Exception):
                await task

        if wait_task in done:
            return_code = wait_task.result()
        else:
            return_code = process.returncode if process.returncode is not None else 0

        session.process = None
        return int(return_code or 0)

    async def _record_loop(self, session: _LiveSession) -> None:
        error = ""
        output_path = ""
        upload_outcome = UploadOutcome(
            attempted=False,
            success=False,
            reason="未执行上传",
        )
        work_aweme_id = self._build_live_work_id(session)
        try:
            while not session.stop_event.is_set():
                return_code = await self._run_record_once(session)
                if session.stop_event.is_set():
                    break
                session.retry_count += 1
                error = f"ffmpeg exited with code {return_code}"
                await self.database.update_douyin_live_record_retry(
                    session.record_id,
                    session.retry_count,
                    error,
                )
                await sleep(min(3 * session.retry_count, 15))

            merged = await self._merge_segments(session)
            self._cleanup_segment_dir(session)
            if merged:
                output_path = str(session.output_file.resolve())
                await self._download_cover(session)
                width, height = await self._probe_output_size(session.output_file)
                if not width or not height:
                    width, height = session.width, session.height
                upload_outcome = await self._upload_record_file(session)
                if upload_outcome.success:
                    status = "uploaded"
                    error = ""
                    await self.database.mark_douyin_live_record_uploaded(session.record_id)
                elif upload_outcome.attempted:
                    status = "upload_failed"
                    error = upload_outcome.reason or "直播上传失败"
                else:
                    status = "finished"
                    error = ""

                await self.database.insert_douyin_live_work(
                    sec_user_id=session.sec_user_id,
                    aweme_id=work_aweme_id,
                    desc=f"直播-{session.title}",
                    create_ts=int(session.started_at.timestamp()),
                    create_date=f"{session.started_at:%Y-%m-%d}",
                    cover=session.cover_url,
                    width=width,
                    height=height,
                    upload_status=(
                        "uploaded"
                        if upload_outcome.success
                        else "failed" if upload_outcome.attempted else "downloaded"
                    ),
                    upload_provider="webdav" if upload_outcome.attempted else "",
                    upload_destination=upload_outcome.destination,
                    upload_origin_destination=upload_outcome.origin_destination,
                    local_path=output_path,
                    uploaded_at=(
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        if upload_outcome.success
                        else ""
                    ),
                )
                if self.uploader.should_delete_local(upload_outcome):
                    self.log.info(
                        f"已按配置删除直播本地文件: {session.output_file}"
                    )
                    self._cleanup_output_file(session.output_file)
                    self._cleanup_output_file(session.cover_file)
            else:
                status = "failed"
                error = "直播分段合并失败或无可用分段"
            await self.database.finish_douyin_live_record(
                session.record_id,
                status=status,
                output_file=output_path,
                upload_destination=upload_outcome.destination,
                upload_origin_destination=upload_outcome.origin_destination,
                work_aweme_id=work_aweme_id,
                error=error,
            )
        except Exception as exc:
            await self.database.finish_douyin_live_record(
                session.record_id,
                status="failed",
                output_file=output_path,
                upload_destination=upload_outcome.destination,
                upload_origin_destination=upload_outcome.origin_destination,
                work_aweme_id=work_aweme_id,
                error=repr(exc),
            )
            self.log.error(
                f"直播录制任务异常: sec_user_id={session.sec_user_id}, error={repr(exc)}"
            )
        finally:
            self.sessions.pop(session.sec_user_id, None)
            self.offline_hits.pop(session.sec_user_id, None)

    async def _merge_segments(self, session: _LiveSession) -> bool:
        segments = sorted(session.segment_dir.glob("*.ts"))
        if not segments:
            return False

        if session.output_file.exists():
            session.output_file.unlink()

        filelist = session.segment_dir.joinpath("filelist.txt")
        lines = [f"file '{str(i.resolve())}'" for i in segments]
        filelist.write_text("\n".join(lines), encoding="utf-8")

        command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(filelist),
            "-c",
            "copy",
            str(session.output_file),
        ]
        process = await create_subprocess_exec(
            *command,
            stdout=DEVNULL,
            stderr=DEVNULL,
        )
        return_code = await process.wait()

        if return_code == 0 and session.output_file.is_file():
            return True

        if len(segments) == 1:
            single = [
                self.ffmpeg_path,
                "-hide_banner",
                "-loglevel",
                "warning",
                "-i",
                str(segments[0]),
                "-c",
                "copy",
                str(session.output_file),
            ]
            process = await create_subprocess_exec(
                *single,
                stdout=DEVNULL,
                stderr=DEVNULL,
            )
            return_code = await process.wait()
            return return_code == 0 and session.output_file.is_file()
        return False

    @staticmethod
    def _build_live_work_id(session: _LiveSession) -> str:
        return f"live_{session.sec_user_id}_{session.started_at:%Y%m%d%H%M%S}"

    def _resolve_ffprobe_path(self) -> str:
        ffmpeg_path = Path(str(self.ffmpeg_path or "")).expanduser()
        if ffmpeg_path.name:
            candidate = ffmpeg_path.with_name("ffprobe")
            if candidate.is_file():
                return str(candidate)
        return which("ffprobe") or ""

    async def _probe_output_size(self, output_file: Path) -> tuple[int, int]:
        if not isinstance(output_file, Path) or not output_file.is_file():
            return 0, 0
        ffprobe_path = self._resolve_ffprobe_path()
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
            str(output_file),
        ]
        try:
            process = await create_subprocess_exec(
                *command,
                stdout=PIPE,
                stderr=PIPE,
            )
        except OSError:
            return 0, 0
        try:
            stdout, _ = await process.communicate()
        except Exception:
            return 0, 0
        text = stdout.decode("utf-8", errors="ignore").strip()
        left, sep, right = text.partition("x")
        if sep != "x":
            return 0, 0
        try:
            width = int(left or 0)
            height = int(right or 0)
        except ValueError:
            return 0, 0
        if width <= 0 or height <= 0:
            return 0, 0
        return width, height

    @staticmethod
    def _cleanup_output_file(path: Path) -> None:
        with suppress(OSError):
            if path.exists():
                path.unlink()

    @staticmethod
    def _cleanup_segment_dir(session: _LiveSession) -> None:
        if not session.segment_dir.exists():
            return
        for child in session.segment_dir.iterdir():
            with suppress(OSError):
                if child.is_file():
                    child.unlink()
        with suppress(OSError):
            session.segment_dir.rmdir()

    async def _download_cover(self, session: _LiveSession) -> None:
        if not session.cover_url:
            return
        if session.cover_file.exists():
            return
        try:
            response = await self.params.client.get(
                session.cover_url,
                follow_redirects=True,
            )
            response.raise_for_status()
            content = bytes(response.content or b"")
            if not content:
                return
            await to_thread(
                session.cover_file.write_bytes,
                content,
            )
        except Exception as exc:
            self.log.warning(
                f"下载直播封面失败: sec_user_id={session.sec_user_id}, {repr(exc)}"
            )

    async def _upload_record_file(self, session: _LiveSession) -> UploadOutcome:
        if not session.output_file.is_file():
            return UploadOutcome(
                attempted=True,
                success=False,
                reason="直播文件不存在",
            )
        author = self._safe_text(session.nickname, session.sec_user_id)
        year = f"{session.started_at:%Y}"
        file_name = session.output_file.name
        if not file_name.startswith("直播"):
            file_name = f"直播-{file_name}"
        remote_relative = Path(author).joinpath(year, file_name)
        return await self.uploader.upload_file_with_target(
            file_path=session.output_file,
            suffix="mp4",
            remote_relative_path=remote_relative,
        )
