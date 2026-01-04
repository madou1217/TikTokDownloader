from pydantic import BaseModel, Field


class DouyinUserCreate(BaseModel):
    sec_user_id: str = Field(..., min_length=1)


class DouyinUser(BaseModel):
    id: int
    sec_user_id: str
    uid: str = ""
    nickname: str = ""
    avatar: str = ""
    cover: str = ""
    has_works: bool = False
    status: str = "unknown"
    is_live: bool = False
    has_new_today: bool = False
    auto_update: bool = False
    update_window_start: str = ""
    update_window_end: str = ""
    last_live_at: str = ""
    last_new_at: str = ""
    last_fetch_at: str = ""
    created_at: str = ""
    updated_at: str = ""


class DouyinUserPage(BaseModel):
    total: int
    items: list[DouyinUser]


class DouyinUserSettingsUpdate(BaseModel):
    auto_update: bool = False
    update_window_start: str = ""
    update_window_end: str = ""


class DouyinWork(BaseModel):
    sec_user_id: str = ""
    aweme_id: str = ""
    desc: str = ""
    create_ts: int = 0
    create_time: str = ""
    create_date: str = ""
    nickname: str = ""
    cover: str = ""
    play_count: int = 0


class DouyinWorkPage(BaseModel):
    items: list[DouyinWork]
    next_cursor: int = 0
    has_more: bool = False


class DouyinDailyWorkPage(BaseModel):
    total: int
    items: list[DouyinWork]


class DouyinWorkListPage(BaseModel):
    total: int
    items: list[DouyinWork]


class DouyinScheduleSetting(BaseModel):
    enabled: bool = True
    times: str = ""
    expression: str = ""


class DouyinCookieCreate(BaseModel):
    account: str = ""
    cookie: str = Field(..., min_length=1)


class DouyinCookieClipboardCreate(BaseModel):
    account: str = ""


class DouyinCookieBrowserCreate(BaseModel):
    account: str = ""
    browser: str = Field(..., min_length=1)


class DouyinCookie(BaseModel):
    id: int
    account: str = ""
    cookie_masked: str = ""
    status: str = "active"
    fail_count: int = 0
    last_used_at: str = ""
    last_failed_at: str = ""
    created_at: str = ""
    updated_at: str = ""
