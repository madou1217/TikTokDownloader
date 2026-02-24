from asyncio import CancelledError
from contextlib import suppress
from datetime import datetime
from shutil import move

from aiosqlite import Row, connect

from ..custom import PROJECT_ROOT

__all__ = ["Database"]


class Database:
    __FILE = "DouK-Downloader.db"

    def __init__(
        self,
    ):
        self.file = PROJECT_ROOT.joinpath(self.__FILE)
        self.database = None
        self.cursor = None

    async def __connect_database(self):
        self.database = await connect(self.file)
        self.database.row_factory = Row
        self.cursor = await self.database.cursor()
        await self.__create_table()
        await self.__ensure_columns()
        await self.__write_default_config()
        await self.__write_default_option()
        await self.database.commit()

    async def __create_table(self):
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS config_data (
            NAME TEXT PRIMARY KEY,
            VALUE INTEGER NOT NULL CHECK(VALUE IN (0, 1))
            );"""
        )
        await self.database.execute(
            "CREATE TABLE IF NOT EXISTS download_data (ID TEXT PRIMARY KEY);"
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS upload_data (
            FILE_HASH TEXT NOT NULL,
            PROVIDER TEXT NOT NULL,
            DESTINATION TEXT NOT NULL,
            ORIGIN_DESTINATION TEXT NOT NULL DEFAULT '',
            WORK_ID TEXT NOT NULL DEFAULT '',
            LOCAL_PATH TEXT NOT NULL DEFAULT '',
            LOCAL_SIZE INTEGER NOT NULL DEFAULT 0,
            UPLOADED_AT TEXT NOT NULL,
            PRIMARY KEY (FILE_HASH, PROVIDER, DESTINATION)
            );"""
        )
        await self.database.execute("""CREATE TABLE IF NOT EXISTS mapping_data (
        ID TEXT PRIMARY KEY,
        NAME TEXT NOT NULL,
        MARK TEXT NOT NULL
        );""")
        await self.database.execute("""CREATE TABLE IF NOT EXISTS option_data (
        NAME TEXT PRIMARY KEY,
        VALUE TEXT NOT NULL
        );""")
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sec_user_id TEXT NOT NULL UNIQUE,
            uid TEXT NOT NULL DEFAULT '',
            nickname TEXT NOT NULL DEFAULT '',
            avatar TEXT NOT NULL DEFAULT '',
            cover TEXT NOT NULL DEFAULT '',
            has_works INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'unknown',
            is_live INTEGER NOT NULL DEFAULT 0,
            live_width INTEGER NOT NULL DEFAULT 0,
            live_height INTEGER NOT NULL DEFAULT 0,
            has_new_today INTEGER NOT NULL DEFAULT 0,
            auto_update INTEGER NOT NULL DEFAULT 0,
            update_window_start TEXT NOT NULL DEFAULT '',
            update_window_end TEXT NOT NULL DEFAULT '',
            last_live_at TEXT NOT NULL DEFAULT '',
            last_new_at TEXT NOT NULL DEFAULT '',
            last_fetch_at TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_cookie (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account TEXT NOT NULL DEFAULT '',
            cookie TEXT NOT NULL,
            cookie_hash TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'active',
            fail_count INTEGER NOT NULL DEFAULT 0,
            last_used_at TEXT NOT NULL DEFAULT '',
            last_failed_at TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_work (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sec_user_id TEXT NOT NULL,
            aweme_id TEXT NOT NULL UNIQUE,
            desc TEXT NOT NULL DEFAULT '',
            create_ts INTEGER NOT NULL DEFAULT 0,
            create_date TEXT NOT NULL DEFAULT '',
            cover TEXT NOT NULL DEFAULT '',
            play_count INTEGER NOT NULL DEFAULT 0,
            width INTEGER NOT NULL DEFAULT 0,
            height INTEGER NOT NULL DEFAULT 0,
            work_type TEXT NOT NULL DEFAULT 'video',
            upload_status TEXT NOT NULL DEFAULT 'pending',
            upload_provider TEXT NOT NULL DEFAULT '',
            upload_destination TEXT NOT NULL DEFAULT '',
            upload_origin_destination TEXT NOT NULL DEFAULT '',
            upload_message TEXT NOT NULL DEFAULT '',
            download_progress INTEGER NOT NULL DEFAULT 0,
            local_path TEXT NOT NULL DEFAULT '',
            downloaded_at TEXT NOT NULL DEFAULT '',
            uploaded_at TEXT NOT NULL DEFAULT '',
            status_updated_at TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_schedule (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            enabled INTEGER NOT NULL DEFAULT 1,
            times_text TEXT NOT NULL DEFAULT '',
            interval_minutes INTEGER NOT NULL DEFAULT 30,
            window_start TEXT NOT NULL DEFAULT '',
            window_end TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_playlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_playlist_item (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER NOT NULL,
            aweme_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(playlist_id, aweme_id)
            );"""
        )
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS douyin_live_record (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sec_user_id TEXT NOT NULL,
            room_id TEXT NOT NULL DEFAULT '',
            web_rid TEXT NOT NULL DEFAULT '',
            nickname TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            stream_url TEXT NOT NULL DEFAULT '',
            local_root TEXT NOT NULL DEFAULT '',
            segment_dir TEXT NOT NULL DEFAULT '',
            output_file TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'running',
            retry_count INTEGER NOT NULL DEFAULT 0,
            error TEXT NOT NULL DEFAULT '',
            upload_destination TEXT NOT NULL DEFAULT '',
            upload_origin_destination TEXT NOT NULL DEFAULT '',
            work_aweme_id TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            ended_at TEXT NOT NULL DEFAULT '',
            uploaded_at TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
            );"""
        )

    async def __ensure_columns(self) -> None:
        columns = {
            "is_live": "INTEGER NOT NULL DEFAULT 0",
            "live_width": "INTEGER NOT NULL DEFAULT 0",
            "live_height": "INTEGER NOT NULL DEFAULT 0",
            "has_new_today": "INTEGER NOT NULL DEFAULT 0",
            "auto_update": "INTEGER NOT NULL DEFAULT 0",
            "update_window_start": "TEXT NOT NULL DEFAULT ''",
            "update_window_end": "TEXT NOT NULL DEFAULT ''",
            "last_live_at": "TEXT NOT NULL DEFAULT ''",
            "last_new_at": "TEXT NOT NULL DEFAULT ''",
            "avatar": "TEXT NOT NULL DEFAULT ''",
            "cover": "TEXT NOT NULL DEFAULT ''",
        }
        await self.cursor.execute("PRAGMA table_info(douyin_user);")
        existing = {row["name"] for row in await self.cursor.fetchall()}
        for name, ddl in columns.items():
            if name not in existing:
                await self.database.execute(
                    f"ALTER TABLE douyin_user ADD COLUMN {name} {ddl};"
                )
        await self.cursor.execute("PRAGMA table_info(douyin_work);")
        work_existing = {row["name"] for row in await self.cursor.fetchall()}
        work_columns = {
            "cover": "TEXT NOT NULL DEFAULT ''",
            "play_count": "INTEGER NOT NULL DEFAULT 0",
            "width": "INTEGER NOT NULL DEFAULT 0",
            "height": "INTEGER NOT NULL DEFAULT 0",
            "work_type": "TEXT NOT NULL DEFAULT 'video'",
            "upload_status": "TEXT NOT NULL DEFAULT 'pending'",
            "upload_provider": "TEXT NOT NULL DEFAULT ''",
            "upload_destination": "TEXT NOT NULL DEFAULT ''",
            "upload_origin_destination": "TEXT NOT NULL DEFAULT ''",
            "upload_message": "TEXT NOT NULL DEFAULT ''",
            "download_progress": "INTEGER NOT NULL DEFAULT 0",
            "local_path": "TEXT NOT NULL DEFAULT ''",
            "downloaded_at": "TEXT NOT NULL DEFAULT ''",
            "uploaded_at": "TEXT NOT NULL DEFAULT ''",
            "status_updated_at": "TEXT NOT NULL DEFAULT ''",
        }
        for name, ddl in work_columns.items():
            if name not in work_existing:
                await self.database.execute(
                    f"ALTER TABLE douyin_work ADD COLUMN {name} {ddl};"
                )
        await self.database.execute(
            """UPDATE douyin_work
            SET status_updated_at=created_at
            WHERE status_updated_at='';"""
        )
        await self.database.execute(
            """UPDATE douyin_work
            SET download_progress=100
            WHERE upload_status IN ('downloaded', 'uploading', 'uploaded')
              AND download_progress=0;"""
        )
        await self.cursor.execute("PRAGMA table_info(upload_data);")
        upload_existing = {row["name"] for row in await self.cursor.fetchall()}
        upload_columns = {
            "ORIGIN_DESTINATION": "TEXT NOT NULL DEFAULT ''",
            "WORK_ID": "TEXT NOT NULL DEFAULT ''",
        }
        for name, ddl in upload_columns.items():
            if name not in upload_existing:
                await self.database.execute(
                    f"ALTER TABLE upload_data ADD COLUMN {name} {ddl};"
                )
        await self.cursor.execute("PRAGMA table_info(douyin_live_record);")
        live_existing = {row["name"] for row in await self.cursor.fetchall()}
        live_columns = {
            "upload_destination": "TEXT NOT NULL DEFAULT ''",
            "upload_origin_destination": "TEXT NOT NULL DEFAULT ''",
            "work_aweme_id": "TEXT NOT NULL DEFAULT ''",
        }
        for name, ddl in live_columns.items():
            if name not in live_existing:
                await self.database.execute(
                    f"ALTER TABLE douyin_live_record ADD COLUMN {name} {ddl};"
                )
        await self.cursor.execute("PRAGMA table_info(douyin_schedule);")
        schedule_existing = {row["name"] for row in await self.cursor.fetchall()}
        if "times_text" not in schedule_existing:
            await self.database.execute(
                "ALTER TABLE douyin_schedule ADD COLUMN times_text TEXT NOT NULL DEFAULT '';"
            )

    async def __write_default_config(self):
        await self.database.execute("""INSERT OR IGNORE INTO config_data (NAME, VALUE)
                            VALUES ('Record', 1),
                            ('Logger', 0),
                            ('Disclaimer', 0);""")

    async def __write_default_option(self):
        await self.database.execute("""INSERT OR IGNORE INTO option_data (NAME, VALUE)
                            VALUES ('Language', 'zh_CN');""")

    async def _query_one(self, sql: str, params: tuple = ()):
        async with self.database.execute(sql, params) as cursor:
            return await cursor.fetchone()

    async def _query_all(self, sql: str, params: tuple = ()):
        async with self.database.execute(sql, params) as cursor:
            return await cursor.fetchall()

    async def read_config_data(self):
        return await self._query_all("SELECT * FROM config_data")

    async def read_option_data(self):
        return await self._query_all("SELECT * FROM option_data")

    async def update_config_data(
        self,
        name: str,
        value: int,
    ):
        await self.database.execute(
            "REPLACE INTO config_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_option_data(
        self,
        name: str,
        value: str,
    ):
        await self.database.execute(
            "REPLACE INTO option_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_mapping_data(self, id_: str, name: str, mark: str):
        await self.database.execute(
            "REPLACE INTO mapping_data (ID, NAME, MARK) VALUES (?,?,?)",
            (id_, name, mark),
        )
        await self.database.commit()

    async def read_mapping_data(self, id_: str):
        return await self._query_one(
            "SELECT NAME, MARK FROM mapping_data WHERE ID=?", (id_,)
        )

    async def has_download_data(self, id_: str) -> bool:
        row = await self._query_one("SELECT ID FROM download_data WHERE ID=?", (id_,))
        return bool(row)

    async def write_download_data(self, id_: str):
        await self.database.execute(
            "INSERT OR IGNORE INTO download_data (ID) VALUES (?);", (id_,)
        )
        await self.database.commit()

    async def has_upload_data(
        self,
        file_hash: str,
        provider: str,
        destination: str,
    ) -> bool:
        row = await self._query_one(
            """SELECT 1
            FROM upload_data
            WHERE FILE_HASH=? AND PROVIDER=? AND DESTINATION=?
            LIMIT 1;""",
            (file_hash, provider, destination),
        )
        return bool(row)

    async def write_upload_data(
        self,
        file_hash: str,
        provider: str,
        destination: str,
        origin_destination: str,
        local_path: str,
        local_size: int,
        work_id: str = "",
    ) -> None:
        await self.database.execute(
            """INSERT INTO upload_data (
                FILE_HASH,
                PROVIDER,
                DESTINATION,
                ORIGIN_DESTINATION,
                WORK_ID,
                LOCAL_PATH,
                LOCAL_SIZE,
                UPLOADED_AT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(FILE_HASH, PROVIDER, DESTINATION) DO UPDATE SET
                ORIGIN_DESTINATION=CASE
                    WHEN excluded.ORIGIN_DESTINATION!=''
                    THEN excluded.ORIGIN_DESTINATION
                    ELSE upload_data.ORIGIN_DESTINATION
                END,
                WORK_ID=CASE
                    WHEN excluded.WORK_ID!=''
                    THEN excluded.WORK_ID
                    ELSE upload_data.WORK_ID
                END,
                LOCAL_PATH=excluded.LOCAL_PATH,
                LOCAL_SIZE=excluded.LOCAL_SIZE,
                UPLOADED_AT=excluded.UPLOADED_AT;""",
            (
                file_hash,
                provider,
                destination,
                origin_destination or "",
                work_id or "",
                local_path,
                int(local_size),
                self._now_str(),
            ),
        )
        await self.database.commit()

    async def get_latest_upload_by_work_id(self, work_id: str) -> dict:
        row = await self._query_one(
            """SELECT FILE_HASH, PROVIDER, DESTINATION, ORIGIN_DESTINATION,
            WORK_ID, LOCAL_PATH, LOCAL_SIZE, UPLOADED_AT
            FROM upload_data
            WHERE WORK_ID=?
            ORDER BY UPLOADED_AT DESC
            LIMIT 1;""",
            (work_id,),
        )
        return dict(row) if row else {}

    async def delete_download_data(self, ids: list | tuple | str):
        if not ids:
            return
        if isinstance(ids, str):
            ids = [ids]
        [await self.__delete_download_data(i) for i in ids]
        await self.database.commit()

    async def __delete_download_data(self, id_: str):
        await self.database.execute("DELETE FROM download_data WHERE ID=?", (id_,))

    async def delete_all_download_data(self):
        await self.database.execute("DELETE FROM download_data")
        await self.database.commit()

    async def __aenter__(self):
        self.compatible()
        await self.__connect_database()
        return self

    async def close(self):
        with suppress(CancelledError):
            await self.cursor.close()
        await self.database.close()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def compatible(self):
        if (
            old := PROJECT_ROOT.parent.joinpath(self.__FILE)
        ).exists() and not self.file.exists():
            move(old, self.file)

    @staticmethod
    def _now_str() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    async def list_douyin_users(self) -> list[dict]:
        rows = await self._query_all(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            ORDER BY updated_at DESC;"""
        )
        return [dict(i) for i in rows]

    async def count_douyin_users_with_works(self) -> int:
        row = await self._query_one(
            """SELECT COUNT(1) AS total
            FROM douyin_user u
            WHERE EXISTS (
                SELECT 1 FROM douyin_work w
                WHERE w.sec_user_id = u.sec_user_id
                LIMIT 1
            );"""
        )
        return int(row["total"]) if row else 0

    async def list_douyin_users_with_works(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user u
            WHERE EXISTS (
                SELECT 1 FROM douyin_work w
                WHERE w.sec_user_id = u.sec_user_id
                LIMIT 1
            )
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in rows]

    async def count_douyin_users(self) -> int:
        row = await self._query_one("SELECT COUNT(1) AS total FROM douyin_user;")
        return int(row["total"]) if row else 0

    async def list_douyin_users_paged(
        self, page: int, page_size: int
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in rows]

    async def get_douyin_user(self, sec_user_id: str) -> dict:
        row = await self._query_one(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, live_width, live_height, has_new_today, auto_update,
            update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE sec_user_id=?;""",
            (sec_user_id,),
        )
        return dict(row) if row else {}

    async def upsert_douyin_user(
        self,
        sec_user_id: str,
        uid: str,
        nickname: str,
        avatar: str,
        cover: str,
        has_works: bool,
        status: str,
    ) -> dict:
        now = self._now_str()
        await self.database.execute(
            """INSERT INTO douyin_user (
                sec_user_id, uid, nickname, avatar, cover, has_works, status,
                last_fetch_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sec_user_id) DO UPDATE SET
                uid=excluded.uid,
                nickname=excluded.nickname,
                avatar=excluded.avatar,
                cover=excluded.cover,
                has_works=excluded.has_works,
                status=excluded.status,
                last_fetch_at=excluded.last_fetch_at,
                updated_at=excluded.updated_at;""",
            (
                sec_user_id,
                uid,
                nickname,
                avatar,
                cover,
                1 if has_works else 0,
                status,
                now,
                now,
                now,
            ),
        )
        await self.database.commit()
        row = await self._query_one(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE sec_user_id=?;""",
            (sec_user_id,),
        )
        return dict(row) if row else {}

    async def delete_douyin_user(self, sec_user_id: str) -> None:
        await self.database.execute(
            "DELETE FROM douyin_user WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        await self.database.commit()

    async def delete_douyin_user_with_works(self, sec_user_id: str) -> int:
        row = await self._query_one(
            "SELECT COUNT(1) AS total FROM douyin_work WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        total = int(row["total"]) if row else 0
        await self.database.execute(
            "DELETE FROM douyin_work WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        await self.database.execute(
            "DELETE FROM douyin_user WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        await self.database.commit()
        return total

    async def delete_orphan_douyin_works(self) -> int:
        row = await self._query_one(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            WHERE NOT EXISTS (
                SELECT 1 FROM douyin_user u WHERE u.sec_user_id = w.sec_user_id
            );"""
        )
        total = int(row["total"]) if row else 0
        if total <= 0:
            return 0
        await self.database.execute(
            """DELETE FROM douyin_work
            WHERE NOT EXISTS (
                SELECT 1 FROM douyin_user u WHERE u.sec_user_id = douyin_work.sec_user_id
            );"""
        )
        await self.database.commit()
        return total

    async def update_douyin_user_live(
        self,
        sec_user_id: str,
        is_live: bool,
    ) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET is_live=?,
                last_live_at=CASE WHEN ?=1 THEN ? ELSE last_live_at END,
                updated_at=?
            WHERE sec_user_id=?;""",
            (1 if is_live else 0, 1 if is_live else 0, now, now, sec_user_id),
        )
        await self.database.commit()

    async def update_douyin_user_live_size(
        self,
        sec_user_id: str,
        width: int,
        height: int,
    ) -> None:
        if not width or not height:
            return
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET live_width=?, live_height=?, updated_at=?
            WHERE sec_user_id=?;""",
            (int(width), int(height), now, sec_user_id),
        )
        await self.database.commit()

    async def mark_running_live_records_interrupted(self) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_live_record
            SET status='interrupted',
                ended_at=CASE WHEN ended_at='' THEN ? ELSE ended_at END,
                updated_at=?
            WHERE status='running';""",
            (now, now),
        )
        await self.database.commit()

    async def create_douyin_live_record(
        self,
        sec_user_id: str,
        room_id: str,
        web_rid: str,
        nickname: str,
        title: str,
        stream_url: str,
        local_root: str,
        segment_dir: str,
        output_file: str,
    ) -> int:
        now = self._now_str()
        cursor = await self.database.execute(
            """INSERT INTO douyin_live_record (
            sec_user_id,
            room_id,
            web_rid,
            nickname,
            title,
            stream_url,
            local_root,
            segment_dir,
            output_file,
            status,
            retry_count,
            error,
            upload_destination,
            upload_origin_destination,
            work_aweme_id,
            started_at,
            ended_at,
            uploaded_at,
            created_at,
            updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', 0, '', '', '', '', ?, '', '', ?, ?);""",
            (
                sec_user_id,
                room_id or "",
                web_rid or "",
                nickname or "",
                title or "",
                stream_url or "",
                local_root or "",
                segment_dir or "",
                output_file or "",
                now,
                now,
                now,
            ),
        )
        await self.database.commit()
        return int(cursor.lastrowid or 0)

    async def update_douyin_live_record_retry(
        self,
        record_id: int,
        retry_count: int,
        error: str = "",
    ) -> None:
        if not record_id:
            return
        await self.database.execute(
            """UPDATE douyin_live_record
            SET retry_count=?,
                error=?,
                updated_at=?
            WHERE id=?;""",
            (
                max(int(retry_count), 0),
                error or "",
                self._now_str(),
                int(record_id),
            ),
        )
        await self.database.commit()

    async def finish_douyin_live_record(
        self,
        record_id: int,
        status: str,
        output_file: str,
        upload_destination: str = "",
        upload_origin_destination: str = "",
        work_aweme_id: str = "",
        error: str = "",
    ) -> None:
        if not record_id:
            return
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_live_record
            SET status=?,
                output_file=CASE WHEN ?!='' THEN ? ELSE output_file END,
                upload_destination=CASE WHEN ?!='' THEN ? ELSE upload_destination END,
                upload_origin_destination=CASE
                    WHEN ?!='' THEN ? ELSE upload_origin_destination
                END,
                work_aweme_id=CASE WHEN ?!='' THEN ? ELSE work_aweme_id END,
                error=?,
                ended_at=?,
                updated_at=?
            WHERE id=?;""",
            (
                status or "finished",
                output_file or "",
                output_file or "",
                upload_destination or "",
                upload_destination or "",
                upload_origin_destination or "",
                upload_origin_destination or "",
                work_aweme_id or "",
                work_aweme_id or "",
                error or "",
                now,
                now,
                int(record_id),
            ),
        )
        await self.database.commit()

    async def update_douyin_work_upload(
        self,
        aweme_id: str,
        status: str,
        provider: str = "",
        destination: str = "",
        origin_destination: str = "",
        local_path: str = "",
        message: str = "",
        download_progress: int | None = None,
        mark_downloaded: bool = False,
        mark_uploaded: bool = False,
    ) -> None:
        if not aweme_id:
            return
        normalized = -1
        if download_progress is not None:
            normalized = max(0, min(100, int(download_progress)))
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_work
            SET upload_status=?,
                upload_provider=CASE WHEN ?!='' THEN ? ELSE upload_provider END,
                upload_destination=CASE WHEN ?!='' THEN ? ELSE upload_destination END,
                upload_origin_destination=CASE
                    WHEN ?!='' THEN ? ELSE upload_origin_destination
                END,
                local_path=CASE WHEN ?!='' THEN ? ELSE local_path END,
                upload_message=?,
                download_progress=CASE
                    WHEN ? >= 0 THEN ?
                    WHEN ? IN ('downloaded', 'uploading', 'uploaded') THEN 100
                    WHEN ?='pending' THEN 0
                    ELSE download_progress
                END,
                status_updated_at=?,
                downloaded_at=CASE
                    WHEN ?=1 THEN ?
                    WHEN downloaded_at='' AND ?='uploaded' THEN ?
                    ELSE downloaded_at
                END,
                uploaded_at=CASE
                    WHEN ?=1 THEN ?
                    WHEN ?='uploaded' THEN ?
                    ELSE uploaded_at
                END
            WHERE aweme_id=?;""",
            (
                status or "pending",
                provider or "",
                provider or "",
                destination or "",
                destination or "",
                origin_destination or "",
                origin_destination or "",
                local_path or "",
                local_path or "",
                message or "",
                normalized,
                normalized,
                status or "",
                status or "",
                now,
                1 if mark_downloaded else 0,
                now,
                status or "",
                now,
                1 if mark_uploaded else 0,
                now,
                status or "",
                now,
                aweme_id,
            ),
        )
        await self.database.commit()

    async def update_douyin_work_download_progress(
        self,
        aweme_id: str,
        progress: int,
        message: str = "",
    ) -> None:
        if not aweme_id:
            return
        value = max(0, min(100, int(progress or 0)))
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_work
            SET upload_status=CASE
                    WHEN upload_status='' OR upload_status='pending' THEN 'downloading'
                    ELSE upload_status
                END,
                download_progress=?,
                upload_message=CASE WHEN ?!='' THEN ? ELSE upload_message END,
                status_updated_at=?
            WHERE aweme_id=?;""",
            (
                value,
                message or "",
                message or "",
                now,
                aweme_id,
            ),
        )
        await self.database.commit()

    async def insert_douyin_live_work(
        self,
        sec_user_id: str,
        aweme_id: str,
        desc: str,
        create_ts: int,
        create_date: str,
        cover: str,
        width: int,
        height: int,
        upload_status: str,
        upload_provider: str = "",
        upload_destination: str = "",
        upload_origin_destination: str = "",
        local_path: str = "",
        uploaded_at: str = "",
    ) -> None:
        if not sec_user_id or not aweme_id:
            return
        now = self._now_str()
        await self.database.execute(
            """INSERT INTO douyin_work (
                sec_user_id, aweme_id, desc, create_ts, create_date,
                cover, play_count, width, height, work_type,
                upload_status, upload_provider, upload_destination,
                upload_origin_destination, upload_message, local_path,
                downloaded_at, uploaded_at, status_updated_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, 'live', ?, ?, ?, ?, '', ?, ?, ?, ?, ?)
            ON CONFLICT(aweme_id) DO UPDATE SET
                sec_user_id=excluded.sec_user_id,
                desc=excluded.desc,
                create_ts=excluded.create_ts,
                create_date=excluded.create_date,
                cover=excluded.cover,
                width=excluded.width,
                height=excluded.height,
                work_type='live',
                upload_status=excluded.upload_status,
                upload_provider=excluded.upload_provider,
                upload_destination=excluded.upload_destination,
                upload_origin_destination=excluded.upload_origin_destination,
                local_path=excluded.local_path,
                downloaded_at=excluded.downloaded_at,
                uploaded_at=excluded.uploaded_at,
                status_updated_at=excluded.status_updated_at;""",
            (
                sec_user_id,
                aweme_id,
                desc or "",
                int(create_ts or 0),
                create_date or "",
                cover or "",
                int(width or 0),
                int(height or 0),
                upload_status or "pending",
                upload_provider or "",
                upload_destination or "",
                upload_origin_destination or "",
                local_path or "",
                now,
                uploaded_at or "",
                now,
                now,
            ),
        )
        await self.database.commit()

    async def mark_douyin_live_record_uploaded(self, record_id: int) -> None:
        if not record_id:
            return
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_live_record
            SET uploaded_at=?,
                updated_at=?
            WHERE id=?;""",
            (now, now, int(record_id)),
        )
        await self.database.commit()

    async def update_douyin_work_size(
        self,
        aweme_id: str,
        width: int,
        height: int,
    ) -> None:
        if not aweme_id or not width or not height:
            return
        await self.database.execute(
            "UPDATE douyin_work SET width=?, height=? WHERE aweme_id=?;",
            (int(width), int(height), aweme_id),
        )
        await self.database.commit()

    async def clear_douyin_work_local_path(self, aweme_id: str) -> None:
        if not aweme_id:
            return
        await self.database.execute(
            "UPDATE douyin_work SET local_path='' WHERE aweme_id=?;",
            (aweme_id,),
        )
        await self.database.commit()

    async def set_douyin_work_local_path(self, aweme_id: str, local_path: str) -> None:
        if not aweme_id or not local_path:
            return
        await self.database.execute(
            "UPDATE douyin_work SET local_path=? WHERE aweme_id=?;",
            (str(local_path), aweme_id),
        )
        await self.database.commit()

    async def get_latest_douyin_live_record_output(self, work_aweme_id: str) -> str:
        if not work_aweme_id:
            return ""
        row = await self._query_one(
            """SELECT output_file
            FROM douyin_live_record
            WHERE work_aweme_id=?
              AND output_file!=''
            ORDER BY id DESC
            LIMIT 1;""",
            (work_aweme_id,),
        )
        if not row:
            return ""
        return str(row[0] or "").strip()

    async def get_douyin_work(self, aweme_id: str) -> dict:
        row = await self._query_one(
            """SELECT sec_user_id, aweme_id, desc, create_ts, create_date,
            cover, play_count, width, height, work_type,
            upload_status, upload_provider, upload_destination,
            upload_origin_destination, upload_message, local_path,
            downloaded_at, uploaded_at, status_updated_at
            FROM douyin_work
            WHERE aweme_id=?
            LIMIT 1;""",
            (aweme_id,),
        )
        return dict(row) if row else {}

    async def reset_stale_douyin_work_status(
        self,
        stale_before: str,
        limit: int = 500,
    ) -> int:
        if not stale_before:
            return 0
        limit = min(max(int(limit or 1), 1), 2000)
        now = self._now_str()
        cursor = await self.database.execute(
            """UPDATE douyin_work
            SET upload_status='pending',
                upload_message='自动补偿: 检测到超时僵尸任务，已重置',
                status_updated_at=?
            WHERE aweme_id IN (
                SELECT aweme_id
                FROM douyin_work
                WHERE upload_status IN ('downloading', 'uploading')
                  AND COALESCE(NULLIF(status_updated_at, ''), created_at) <= ?
                ORDER BY COALESCE(NULLIF(status_updated_at, ''), created_at) ASC
                LIMIT ?
            );""",
            (now, stale_before, limit),
        )
        await self.database.commit()
        return int(cursor.rowcount or 0)

    async def update_douyin_user_new(
        self,
        sec_user_id: str,
        has_new_today: bool,
    ) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET has_new_today=?,
                last_new_at=CASE WHEN ?=1 THEN ? ELSE last_new_at END,
                updated_at=?
            WHERE sec_user_id=?;""",
            (
                1 if has_new_today else 0,
                1 if has_new_today else 0,
                now,
                now,
                sec_user_id,
            ),
        )
        await self.database.commit()

    async def update_douyin_user_fetch_time(self, sec_user_id: str) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET last_fetch_at=?,
                updated_at=?
            WHERE sec_user_id=?;""",
            (now, now, sec_user_id),
        )
        await self.database.commit()

    async def clear_douyin_user_new(self, sec_user_id: str) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET has_new_today=0, updated_at=?
            WHERE sec_user_id=?;""",
            (now, sec_user_id),
        )
        await self.database.commit()

    async def update_douyin_user_settings(
        self,
        sec_user_id: str,
        auto_update: bool,
        window_start: str,
        window_end: str,
    ) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET auto_update=?, update_window_start=?, update_window_end=?, updated_at=?
            WHERE sec_user_id=?;""",
            (
                1 if auto_update else 0,
                window_start or "",
                window_end or "",
                now,
                sec_user_id,
            ),
        )
        await self.database.commit()

    async def update_douyin_user_profile(
        self,
        sec_user_id: str,
        uid: str,
        nickname: str,
        avatar: str,
        cover: str,
    ) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_user
            SET uid=CASE WHEN ?!='' THEN ? ELSE uid END,
                nickname=CASE WHEN ?!='' THEN ? ELSE nickname END,
                avatar=CASE WHEN ?!='' THEN ? ELSE avatar END,
                cover=CASE WHEN ?!='' THEN ? ELSE cover END,
                updated_at=?
            WHERE sec_user_id=?;""",
            (
                uid,
                uid,
                nickname,
                nickname,
                avatar,
                avatar,
                cover,
                cover,
                now,
                sec_user_id,
            ),
        )
        await self.database.commit()

    async def list_douyin_users_auto_update(self) -> list[dict]:
        rows = await self._query_all(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE auto_update=1
            ORDER BY updated_at DESC;"""
        )
        return [dict(i) for i in rows]

    async def insert_douyin_works(self, works: list[dict]) -> int:
        if not works:
            return 0
        now = self._now_str()
        inserted = 0
        for item in works:
            cursor = await self.database.execute(
                """INSERT INTO douyin_work (
                sec_user_id, aweme_id, desc, create_ts, create_date,
                cover, play_count, width, height, work_type, status_updated_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aweme_id) DO UPDATE SET
                    sec_user_id=excluded.sec_user_id,
                    desc=excluded.desc,
                    create_ts=excluded.create_ts,
                    create_date=excluded.create_date,
                    cover=excluded.cover,
                    play_count=excluded.play_count,
                    width=excluded.width,
                    height=excluded.height,
                    work_type=excluded.work_type;""",
                (
                    item.get("sec_user_id", ""),
                    item.get("aweme_id", ""),
                    item.get("desc", ""),
                    int(item.get("create_ts") or 0),
                    item.get("create_date", ""),
                    item.get("cover", ""),
                    int(item.get("play_count") or 0),
                    int(item.get("width") or 0),
                    int(item.get("height") or 0),
                    item.get("work_type") or item.get("type") or "video",
                    now,
                    now,
                ),
            )
            if cursor.rowcount and cursor.rowcount > 0:
                inserted += 1
        await self.database.commit()
        return inserted

    async def count_douyin_works_today(
        self,
        date_str: str,
        work_types: tuple[str, ...] | None = None,
    ) -> int:
        params: list = [date_str]
        sql = """SELECT COUNT(1) AS total
            FROM douyin_work w
            WHERE w.create_date=?
            AND EXISTS (
                SELECT 1 FROM douyin_user u WHERE u.sec_user_id = w.sec_user_id
            )"""
        if work_types:
            placeholders = ",".join(["?"] * len(work_types))
            sql += f"\n            AND w.work_type IN ({placeholders})"
            params.extend(work_types)
        sql += ";"
        row = await self._query_one(sql, tuple(params))
        return int(row["total"]) if row else 0

    async def count_douyin_user_works_today(
        self,
        sec_user_id: str,
        date_str: str,
    ) -> int:
        row = await self._query_one(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.create_date=? AND w.sec_user_id=?;""",
            (date_str, sec_user_id),
        )
        return int(row["total"]) if row else 0

    async def list_douyin_works_today(
        self,
        date_str: str,
        page: int,
        page_size: int,
        work_types: tuple[str, ...] | None = None,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        params: list = [date_str]
        sql = """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            w.upload_status, w.upload_provider, w.upload_destination,
            w.upload_origin_destination, w.upload_message, w.download_progress, w.local_path,
            w.downloaded_at, w.uploaded_at,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.create_date=?"""
        if work_types:
            placeholders = ",".join(["?"] * len(work_types))
            sql += f"\n            AND w.work_type IN ({placeholders})"
            params.extend(work_types)
        sql += "\n            ORDER BY w.create_ts DESC\n            LIMIT ? OFFSET ?;"
        params.extend((page_size, offset))
        rows = await self._query_all(sql, tuple(params))
        return [dict(i) for i in rows]

    async def list_douyin_user_works_today(
        self,
        sec_user_id: str,
        date_str: str,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            w.upload_status, w.upload_provider, w.upload_destination,
            w.upload_origin_destination, w.upload_message, w.download_progress, w.local_path,
            w.downloaded_at, w.uploaded_at,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.create_date=? AND w.sec_user_id=?
            ORDER BY w.create_ts DESC
            LIMIT ? OFFSET ?;""",
            (date_str, sec_user_id, page_size, offset),
        )
        return [dict(i) for i in rows]

    async def count_douyin_user_works(
        self,
        sec_user_id: str,
        work_types: tuple[str, ...] | None = None,
    ) -> int:
        params: list = [sec_user_id]
        sql = """SELECT COUNT(1) AS total
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.sec_user_id=?"""
        if work_types:
            placeholders = ",".join(["?"] * len(work_types))
            sql += f"\n            AND w.work_type IN ({placeholders})"
            params.extend(work_types)
        sql += ";"
        row = await self._query_one(sql, tuple(params))
        return int(row["total"]) if row else 0

    async def list_douyin_user_works(
        self,
        sec_user_id: str,
        page: int,
        page_size: int,
        work_types: tuple[str, ...] | None = None,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        params: list = [sec_user_id]
        sql = """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            w.upload_status, w.upload_provider, w.upload_destination,
            w.upload_origin_destination, w.upload_message, w.download_progress, w.local_path,
            w.downloaded_at, w.uploaded_at,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.sec_user_id=?"""
        if work_types:
            placeholders = ",".join(["?"] * len(work_types))
            sql += f"\n            AND w.work_type IN ({placeholders})"
            params.extend(work_types)
        sql += "\n            ORDER BY w.create_ts DESC\n            LIMIT ? OFFSET ?;"
        params.extend((page_size, offset))
        rows = await self._query_all(sql, tuple(params))
        return [dict(i) for i in rows]

    async def list_douyin_user_pending_works(
        self,
        sec_user_id: str,
        limit: int = 200,
    ) -> list[dict]:
        sec_user_id = (sec_user_id or "").strip()
        if not sec_user_id:
            return []
        limit = min(max(int(limit or 1), 1), 500)
        rows = await self._query_all(
            """SELECT aweme_id, work_type, upload_status, status_updated_at
            FROM douyin_work
            WHERE sec_user_id=?
              AND (upload_status='' OR upload_status='pending' OR upload_status='failed')
            ORDER BY create_ts DESC
            LIMIT ?;""",
            (sec_user_id, limit),
        )
        return [dict(i) for i in rows]

    async def summarize_douyin_user_work_status(self, sec_user_id: str) -> dict:
        sec_user_id = (sec_user_id or "").strip()
        if not sec_user_id:
            return {
                "total": 0,
                "pending": 0,
                "downloading": 0,
                "downloaded": 0,
                "uploading": 0,
                "uploaded": 0,
                "failed": 0,
            }
        row = await self._query_one(
            """SELECT
                COUNT(1) AS total,
                SUM(CASE
                    WHEN status='downloading' THEN 1
                    ELSE 0
                END) AS downloading,
                SUM(CASE
                    WHEN status='downloading' THEN progress
                    ELSE 0
                END) AS downloading_progress_total,
                SUM(CASE
                    WHEN status='downloaded' THEN 1
                    ELSE 0
                END) AS downloaded,
                SUM(CASE
                    WHEN status='uploading' THEN 1
                    ELSE 0
                END) AS uploading,
                SUM(CASE
                    WHEN status='uploaded' THEN 1
                    ELSE 0
                END) AS uploaded,
                SUM(CASE
                    WHEN status='failed' THEN 1
                    ELSE 0
                END) AS failed,
                SUM(CASE
                    WHEN status='' OR status='pending'
                         OR status NOT IN (
                            'downloading', 'downloaded', 'uploading', 'uploaded', 'failed'
                         ) THEN 1
                    ELSE 0
                END) AS pending
            FROM (
                SELECT
                    LOWER(TRIM(COALESCE(w.upload_status, ''))) AS status,
                    CAST(COALESCE(w.download_progress, 0) AS INTEGER) AS progress
                FROM douyin_work w
                JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
                WHERE w.sec_user_id=?
            ) AS works;""",
            (sec_user_id,),
        )
        if not row:
            return {
                "total": 0,
                "pending": 0,
                "downloading": 0,
                "downloaded": 0,
                "uploading": 0,
                "uploaded": 0,
                "failed": 0,
            }
        row_data = dict(row)
        return {
            "total": int(row_data.get("total") or 0),
            "pending": int(row_data.get("pending") or 0),
            "downloading": int(row_data.get("downloading") or 0),
            "downloading_progress_total": int(
                row_data.get("downloading_progress_total") or 0
            ),
            "downloaded": int(row_data.get("downloaded") or 0),
            "uploading": int(row_data.get("uploading") or 0),
            "uploaded": int(row_data.get("uploaded") or 0),
            "failed": int(row_data.get("failed") or 0),
        }

    async def count_douyin_works_all(self) -> int:
        row = await self._query_one("SELECT COUNT(1) AS total FROM douyin_work;")
        return int(row["total"]) if row else 0

    async def list_douyin_works_all(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            w.upload_status, w.upload_provider, w.upload_destination,
            w.upload_origin_destination, w.upload_message, w.download_progress, w.local_path,
            w.downloaded_at, w.uploaded_at,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            LEFT JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            ORDER BY w.create_ts DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in rows]

    async def count_douyin_live_today(self, date_str: str) -> int:
        row = await self._query_one(
            """SELECT COUNT(1) AS total FROM douyin_user
            WHERE is_live=1 AND substr(last_live_at, 1, 10)=?;""",
            (date_str,),
        )
        return int(row["total"]) if row else 0

    async def list_douyin_live_today(
        self,
        date_str: str,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT id, sec_user_id,
            COALESCE(uid, '') AS uid,
            COALESCE(nickname, '') AS nickname,
            COALESCE(avatar, '') AS avatar,
            cover, has_works, status, is_live, live_width, live_height,
            has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE is_live=1 AND substr(last_live_at, 1, 10)=?
            ORDER BY last_live_at DESC
            LIMIT ? OFFSET ?;""",
            (date_str, page_size, offset),
        )
        return [dict(i) for i in rows]

    async def count_douyin_playlists(self) -> int:
        row = await self._query_one("SELECT COUNT(1) AS total FROM douyin_playlist;")
        return int(row["total"]) if row else 0

    async def list_douyin_playlists(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT p.id, p.name, p.created_at, p.updated_at,
            COUNT(pi.id) AS item_count
            FROM douyin_playlist p
            LEFT JOIN douyin_playlist_item pi ON pi.playlist_id = p.id
            GROUP BY p.id
            ORDER BY p.updated_at DESC, p.id DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in rows]

    async def get_douyin_playlist(self, playlist_id: int) -> dict:
        row = await self._query_one(
            """SELECT p.id, p.name, p.created_at, p.updated_at,
            COUNT(pi.id) AS item_count
            FROM douyin_playlist p
            LEFT JOIN douyin_playlist_item pi ON pi.playlist_id = p.id
            WHERE p.id=?
            GROUP BY p.id;""",
            (playlist_id,),
        )
        return dict(row) if row else {}

    async def create_douyin_playlist(self, name: str) -> dict:
        now = self._now_str()
        cursor = await self.database.execute(
            """INSERT INTO douyin_playlist (name, created_at, updated_at)
            VALUES (?, ?, ?);""",
            (name, now, now),
        )
        await self.database.commit()
        return await self.get_douyin_playlist(cursor.lastrowid)

    async def delete_douyin_playlist(self, playlist_id: int) -> None:
        await self.database.execute(
            "DELETE FROM douyin_playlist_item WHERE playlist_id=?;",
            (playlist_id,),
        )
        await self.database.execute(
            "DELETE FROM douyin_playlist WHERE id=?;",
            (playlist_id,),
        )
        await self.database.commit()

    async def clear_douyin_playlist(self, playlist_id: int) -> int:
        now = self._now_str()
        cursor = await self.database.execute(
            "DELETE FROM douyin_playlist_item WHERE playlist_id=?;",
            (playlist_id,),
        )
        await self.database.execute(
            "UPDATE douyin_playlist SET updated_at=? WHERE id=?;",
            (now, playlist_id),
        )
        await self.database.commit()
        return int(cursor.rowcount or 0)

    async def insert_douyin_playlist_items(
        self,
        playlist_id: int,
        aweme_ids: list[str],
    ) -> int:
        if not aweme_ids:
            return 0
        now = self._now_str()
        inserted = 0
        for aweme_id in aweme_ids:
            if not aweme_id:
                continue
            cursor = await self.database.execute(
                """INSERT INTO douyin_playlist_item
                (playlist_id, aweme_id, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(playlist_id, aweme_id) DO NOTHING;""",
                (playlist_id, aweme_id, now),
            )
            if cursor.rowcount and cursor.rowcount > 0:
                inserted += 1
        if inserted:
            await self.database.execute(
                "UPDATE douyin_playlist SET updated_at=? WHERE id=?;",
                (now, playlist_id),
            )
        await self.database.commit()
        return inserted

    async def count_douyin_playlist_items(self, playlist_id: int) -> int:
        row = await self._query_one(
            """SELECT COUNT(1) AS total
            FROM douyin_playlist_item
            WHERE playlist_id=?;""",
            (playlist_id,),
        )
        return int(row["total"]) if row else 0

    async def list_douyin_playlist_items(
        self,
        playlist_id: int,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        rows = await self._query_all(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            w.upload_status, w.upload_provider, w.upload_destination,
            w.upload_origin_destination, w.upload_message, w.download_progress, w.local_path,
            w.downloaded_at, w.uploaded_at,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_playlist_item pi
            JOIN douyin_work w ON w.aweme_id = pi.aweme_id
            LEFT JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE pi.playlist_id=?
            ORDER BY pi.created_at DESC
            LIMIT ? OFFSET ?;""",
            (playlist_id, page_size, offset),
        )
        return [dict(i) for i in rows]

    async def list_douyin_playlist_item_ids(
        self,
        playlist_id: int,
        aweme_ids: list[str],
    ) -> list[str]:
        if not aweme_ids:
            return []
        placeholders = ",".join(["?"] * len(aweme_ids))
        rows = await self._query_all(
            f"""SELECT aweme_id
            FROM douyin_playlist_item
            WHERE playlist_id=? AND aweme_id IN ({placeholders});""",
            (playlist_id, *aweme_ids),
        )
        return [row["aweme_id"] for row in rows]

    async def delete_douyin_playlist_items(
        self,
        playlist_id: int,
        aweme_ids: list[str],
    ) -> int:
        if not aweme_ids:
            return 0
        placeholders = ",".join(["?"] * len(aweme_ids))
        cursor = await self.database.execute(
            f"""DELETE FROM douyin_playlist_item
            WHERE playlist_id=? AND aweme_id IN ({placeholders});""",
            (playlist_id, *aweme_ids),
        )
        removed = int(cursor.rowcount or 0)
        if removed:
            now = self._now_str()
            await self.database.execute(
                "UPDATE douyin_playlist SET updated_at=? WHERE id=?;",
                (now, playlist_id),
            )
        await self.database.commit()
        return removed

    async def get_douyin_schedule(self) -> dict:
        row = await self._query_one(
            """SELECT id, enabled, times_text, updated_at
            FROM douyin_schedule WHERE id=1;"""
        )
        return dict(row) if row else {}

    async def upsert_douyin_schedule(
        self,
        enabled: bool,
        times_text: str,
    ) -> dict:
        now = self._now_str()
        await self.database.execute(
            """INSERT INTO douyin_schedule (
            id, enabled, times_text, updated_at
            ) VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                enabled=excluded.enabled,
                times_text=excluded.times_text,
                updated_at=excluded.updated_at;""",
            (
                1 if enabled else 0,
                times_text or "",
                now,
            ),
        )
        await self.database.commit()
        return await self.get_douyin_schedule()

    async def list_douyin_cookies(
        self,
        status: str | None = None,
    ) -> list[dict]:
        if status:
            rows = await self._query_all(
                """SELECT id, account, cookie, cookie_hash, status, fail_count,
                last_used_at, last_failed_at, created_at, updated_at
                FROM douyin_cookie
                WHERE status=?
                ORDER BY updated_at DESC;""",
                (status,),
            )
        else:
            rows = await self._query_all(
                """SELECT id, account, cookie, cookie_hash, status, fail_count,
                last_used_at, last_failed_at, created_at, updated_at
                FROM douyin_cookie
                ORDER BY updated_at DESC;"""
            )
        return [dict(i) for i in rows]

    async def upsert_douyin_cookie(
        self,
        account: str,
        cookie: str,
        cookie_hash: str,
    ) -> dict:
        now = self._now_str()
        await self.database.execute(
            """INSERT INTO douyin_cookie (
                account, cookie, cookie_hash, status, fail_count,
                last_used_at, last_failed_at, created_at, updated_at
            ) VALUES (?, ?, ?, 'active', 0, '', '', ?, ?)
            ON CONFLICT(cookie_hash) DO UPDATE SET
                account=excluded.account,
                cookie=excluded.cookie,
                status='active',
                fail_count=0,
                updated_at=excluded.updated_at;""",
            (account, cookie, cookie_hash, now, now),
        )
        await self.database.commit()
        row = await self._query_one(
            """SELECT id, account, cookie, cookie_hash, status, fail_count,
            last_used_at, last_failed_at, created_at, updated_at
            FROM douyin_cookie
            WHERE cookie_hash=?;""",
            (cookie_hash,),
        )
        return dict(row) if row else {}

    async def update_douyin_cookie(
        self,
        cookie_id: int,
        account: str,
        cookie: str,
        cookie_hash: str,
    ) -> dict:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_cookie
            SET account=?,
                cookie=?,
                cookie_hash=?,
                status='active',
                fail_count=0,
                last_failed_at='',
                updated_at=?
            WHERE id=?;""",
            (
                account,
                cookie,
                cookie_hash,
                now,
                cookie_id,
            ),
        )
        await self.database.commit()
        row = await self._query_one(
            """SELECT id, account, cookie, cookie_hash, status, fail_count,
            last_used_at, last_failed_at, created_at, updated_at
            FROM douyin_cookie
            WHERE id=?;""",
            (cookie_id,),
        )
        return dict(row) if row else {}

    async def mark_douyin_cookie_expired(self, cookie_id: int) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_cookie
            SET status='expired',
                fail_count=fail_count + 1,
                last_failed_at=?,
                updated_at=?
            WHERE id=?;""",
            (now, now, cookie_id),
        )
        await self.database.commit()

    async def touch_douyin_cookie(self, cookie_id: int) -> None:
        now = self._now_str()
        await self.database.execute(
            """UPDATE douyin_cookie
            SET last_used_at=?,
                updated_at=?
            WHERE id=?;""",
            (now, now, cookie_id),
        )
        await self.database.commit()

    async def delete_douyin_cookie(self, cookie_id: int) -> None:
        await self.database.execute(
            "DELETE FROM douyin_cookie WHERE id=?;",
            (cookie_id,),
        )
        await self.database.commit()
