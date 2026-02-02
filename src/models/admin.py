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
    next_auto_update_at: str = ""


class DouyinUserPage(BaseModel):
    total: int
    items: list[DouyinUser]


class DouyinUserSettingsUpdate(BaseModel):
    auto_update: bool = False
    update_window_start: str = ""
    update_window_end: str = ""


class DouyinWork(BaseModel):
    type: str = "video"
    sec_user_id: str = ""
    aweme_id: str = ""
    desc: str = ""
    create_ts: int = 0
    create_time: str = ""
    create_date: str = ""
    nickname: str = ""
    cover: str = ""
    play_count: int = 0
    width: int = 0
    height: int = 0


class DouyinWorkPage(BaseModel):
    items: list[DouyinWork]
    next_cursor: int = 0
    has_more: bool = False


class DouyinDailyWorkPage(BaseModel):
    total: int
    items: list[DouyinWork]


class DouyinClientFeedItem(BaseModel):
    type: str
    sec_user_id: str = ""
    uid: str = ""
    nickname: str = ""
    avatar: str = ""
    title: str = ""
    cover: str = ""
    sort_time: str = ""
    aweme_id: str = ""
    play_count: int = 0
    video_url: str = ""
    width: int = 0
    height: int = 0
    room_id: str = ""
    web_rid: str = ""
    live_url: str = ""
    last_live_at: str = ""
    flv_pull_url: dict = Field(default_factory=dict)
    hls_pull_url_map: dict = Field(default_factory=dict)


class DouyinClientFeedPage(BaseModel):
    total: int
    video_total: int
    live_total: int
    items: list[DouyinClientFeedItem]


class DouyinWorkListPage(BaseModel):
    total: int
    items: list[DouyinWork]


class DouyinPlaylistCreate(BaseModel):
    name: str = Field(..., min_length=1)


class DouyinPlaylist(BaseModel):
    id: int
    name: str
    item_count: int = 0
    created_at: str = ""
    updated_at: str = ""


class DouyinPlaylistPage(BaseModel):
    total: int
    items: list[DouyinPlaylist]


class DouyinPlaylistImport(BaseModel):
    aweme_ids: list[str] = Field(default_factory=list)


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
