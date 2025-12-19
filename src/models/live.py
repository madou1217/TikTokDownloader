from .base import APIModel


class AccountLive(APIModel):
    sec_user_id: str
    dump_html: bool = True


class Live(APIModel):
    web_rid: str | None = None
    # room_id: str | None = None
    # sec_user_id: str | None = None


class LiveTikTok(APIModel):
    room_id: str | None = None
