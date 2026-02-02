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
        }
        for name, ddl in work_columns.items():
            if name not in work_existing:
                await self.database.execute(
                    f"ALTER TABLE douyin_work ADD COLUMN {name} {ddl};"
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

    async def read_config_data(self):
        await self.cursor.execute("SELECT * FROM config_data")
        return await self.cursor.fetchall()

    async def read_option_data(self):
        await self.cursor.execute("SELECT * FROM option_data")
        return await self.cursor.fetchall()

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
        await self.cursor.execute(
            "SELECT NAME, MARK FROM mapping_data WHERE ID=?", (id_,)
        )
        return await self.cursor.fetchone()

    async def has_download_data(self, id_: str) -> bool:
        await self.cursor.execute("SELECT ID FROM download_data WHERE ID=?", (id_,))
        return bool(await self.cursor.fetchone())

    async def write_download_data(self, id_: str):
        await self.database.execute(
            "INSERT OR IGNORE INTO download_data (ID) VALUES (?);", (id_,)
        )
        await self.database.commit()

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
        await self.cursor.execute(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            ORDER BY updated_at DESC;"""
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_users_with_works(self) -> int:
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_user u
            WHERE EXISTS (
                SELECT 1 FROM douyin_work w
                WHERE w.sec_user_id = u.sec_user_id
                LIMIT 1
            );"""
        )
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_users_with_works(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        await self.cursor.execute(
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
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_users(self) -> int:
        await self.cursor.execute("SELECT COUNT(1) AS total FROM douyin_user;")
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_users_paged(
        self, page: int, page_size: int
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        await self.cursor.execute(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def get_douyin_user(self, sec_user_id: str) -> dict:
        await self.cursor.execute(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE sec_user_id=?;""",
            (sec_user_id,),
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            WHERE sec_user_id=?;""",
            (sec_user_id,),
        )
        row = await self.cursor.fetchone()
        return dict(row) if row else {}

    async def delete_douyin_user(self, sec_user_id: str) -> None:
        await self.database.execute(
            "DELETE FROM douyin_user WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        await self.database.commit()

    async def delete_douyin_user_with_works(self, sec_user_id: str) -> int:
        await self.cursor.execute(
            "SELECT COUNT(1) AS total FROM douyin_work WHERE sec_user_id=?;",
            (sec_user_id,),
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            WHERE NOT EXISTS (
                SELECT 1 FROM douyin_user u WHERE u.sec_user_id = w.sec_user_id
            );"""
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
            """SELECT id, sec_user_id, uid, nickname, avatar, cover, has_works, status,
            is_live, has_new_today, auto_update, update_window_start, update_window_end,
            last_live_at, last_new_at, last_fetch_at, created_at, updated_at
            FROM douyin_user
            ORDER BY updated_at DESC;"""
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def insert_douyin_works(self, works: list[dict]) -> int:
        if not works:
            return 0
        now = self._now_str()
        inserted = 0
        for item in works:
            cursor = await self.database.execute(
                """INSERT INTO douyin_work (
                sec_user_id, aweme_id, desc, create_ts, create_date,
                cover, play_count, width, height, work_type, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            if cursor.rowcount and cursor.rowcount > 0:
                inserted += 1
        await self.database.commit()
        return inserted

    async def count_douyin_works_today(self, date_str: str) -> int:
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            WHERE w.create_date=?
            AND EXISTS (
                SELECT 1 FROM douyin_user u WHERE u.sec_user_id = w.sec_user_id
            );""",
            (date_str,),
        )
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def count_douyin_user_works_today(
        self,
        sec_user_id: str,
        date_str: str,
    ) -> int:
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.create_date=? AND w.sec_user_id=?;""",
            (date_str, sec_user_id),
        )
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_works_today(
        self,
        date_str: str,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = max(page_size, 1)
        offset = (page - 1) * page_size
        await self.cursor.execute(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.create_date=?
            ORDER BY w.create_ts DESC
            LIMIT ? OFFSET ?;""",
            (date_str, page_size, offset),
        )
        return [dict(i) for i in await self.cursor.fetchall()]

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
        await self.cursor.execute(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
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
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_user_works(self, sec_user_id: str) -> int:
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.sec_user_id=?;""",
            (sec_user_id,),
        )
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_user_works(
        self,
        sec_user_id: str,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        await self.cursor.execute(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            WHERE w.sec_user_id=?
            ORDER BY w.create_ts DESC
            LIMIT ? OFFSET ?;""",
            (sec_user_id, page_size, offset),
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_works_all(self) -> int:
        await self.cursor.execute("SELECT COUNT(1) AS total FROM douyin_work;")
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_works_all(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        await self.cursor.execute(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
            COALESCE(u.nickname, '') AS nickname,
            COALESCE(u.avatar, '') AS avatar,
            COALESCE(u.uid, '') AS uid
            FROM douyin_work w
            LEFT JOIN douyin_user u ON w.sec_user_id = u.sec_user_id
            ORDER BY w.create_ts DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_live_today(self, date_str: str) -> int:
        await self.cursor.execute(
            """SELECT COUNT(1) AS total FROM douyin_user
            WHERE is_live=1 AND substr(last_live_at, 1, 10)=?;""",
            (date_str,),
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
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
        return [dict(i) for i in await self.cursor.fetchall()]

    async def count_douyin_playlists(self) -> int:
        await self.cursor.execute("SELECT COUNT(1) AS total FROM douyin_playlist;")
        row = await self.cursor.fetchone()
        return int(row["total"]) if row else 0

    async def list_douyin_playlists(
        self,
        page: int,
        page_size: int,
    ) -> list[dict]:
        page = max(page, 1)
        page_size = min(max(page_size, 1), 100)
        offset = (page - 1) * page_size
        await self.cursor.execute(
            """SELECT p.id, p.name, p.created_at, p.updated_at,
            COUNT(pi.id) AS item_count
            FROM douyin_playlist p
            LEFT JOIN douyin_playlist_item pi ON pi.playlist_id = p.id
            GROUP BY p.id
            ORDER BY p.updated_at DESC, p.id DESC
            LIMIT ? OFFSET ?;""",
            (page_size, offset),
        )
        return [dict(i) for i in await self.cursor.fetchall()]

    async def get_douyin_playlist(self, playlist_id: int) -> dict:
        await self.cursor.execute(
            """SELECT p.id, p.name, p.created_at, p.updated_at,
            COUNT(pi.id) AS item_count
            FROM douyin_playlist p
            LEFT JOIN douyin_playlist_item pi ON pi.playlist_id = p.id
            WHERE p.id=?
            GROUP BY p.id;""",
            (playlist_id,),
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
            """SELECT COUNT(1) AS total
            FROM douyin_playlist_item
            WHERE playlist_id=?;""",
            (playlist_id,),
        )
        row = await self.cursor.fetchone()
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
        await self.cursor.execute(
            """SELECT w.sec_user_id, w.aweme_id, w.desc, w.create_ts, w.create_date,
            w.cover, w.play_count, w.width, w.height, w.work_type,
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
        return [dict(i) for i in await self.cursor.fetchall()]

    async def list_douyin_playlist_item_ids(
        self,
        playlist_id: int,
        aweme_ids: list[str],
    ) -> list[str]:
        if not aweme_ids:
            return []
        placeholders = ",".join(["?"] * len(aweme_ids))
        await self.cursor.execute(
            f"""SELECT aweme_id
            FROM douyin_playlist_item
            WHERE playlist_id=? AND aweme_id IN ({placeholders});""",
            (playlist_id, *aweme_ids),
        )
        rows = await self.cursor.fetchall()
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
        await self.cursor.execute(
            """SELECT id, enabled, times_text, updated_at
            FROM douyin_schedule WHERE id=1;"""
        )
        row = await self.cursor.fetchone()
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
            await self.cursor.execute(
                """SELECT id, account, cookie, cookie_hash, status, fail_count,
                last_used_at, last_failed_at, created_at, updated_at
                FROM douyin_cookie
                WHERE status=?
                ORDER BY updated_at DESC;""",
                (status,),
            )
        else:
            await self.cursor.execute(
                """SELECT id, account, cookie, cookie_hash, status, fail_count,
                last_used_at, last_failed_at, created_at, updated_at
                FROM douyin_cookie
                ORDER BY updated_at DESC;"""
            )
        return [dict(i) for i in await self.cursor.fetchall()]

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
        await self.cursor.execute(
            """SELECT id, account, cookie, cookie_hash, status, fail_count,
            last_used_at, last_failed_at, created_at, updated_at
            FROM douyin_cookie
            WHERE cookie_hash=?;""",
            (cookie_hash,),
        )
        row = await self.cursor.fetchone()
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
