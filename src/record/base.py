from pathlib import Path
from time import localtime, strftime
import traceback
from typing import TYPE_CHECKING

from ..custom import (
    DEBUG,
    ERROR,
    GENERAL,
    INFO,
    VERSION_BETA,
    WARNING,
)
from ..tools import Cleaner

if TYPE_CHECKING:
    from ..tools import ColorfulConsole


class BaseLogger:
    """不记录日志，空白日志记录器"""

    DEBUG = VERSION_BETA

    def __init__(
        self,
        main_path: Path,
        console: "ColorfulConsole",
        root="",
        folder="",
        name="",
    ):
        self.log = None  # 记录器主体
        self.console = console
        self._root, self._folder, self._name = self.init_check(
            main_path=main_path,
            root=root,
            folder=folder,
            name=name,
        )

    def init_check(
        self,
        main_path: Path,
        root=None,
        folder=None,
        name=None,
    ) -> tuple:
        root = self.check_root(root, main_path)
        folder = self.check_folder(folder)
        name = self.check_name(name)
        return root, folder, name

    def check_root(self, root: str, default: Path) -> Path:
        if not root:
            return default
        if (r := Path(root)).is_dir():
            return r
        self.console.print(
            f"日志储存路径 {root} 无效，程序将使用项目根路径作为储存路径"
        )
        return default

    def check_name(self, name: str) -> str:
        if not name:
            return "%Y-%m-%d %H.%M.%S"
        try:
            _ = strftime(name, localtime())
            return name
        except ValueError:
            self.console.print(
                f"日志名称格式 {name} 无效，程序将使用默认时间格式：年-月-日 时.分.秒"
            )
            return "%Y-%m-%d %H.%M.%S"

    @staticmethod
    def check_folder(folder: str) -> str:
        return Cleaner().filter_name(folder, "Log")

    def run(self, *args, **kwargs):
        pass

    @staticmethod
    def _normalize_console_kwargs(kwargs: dict) -> tuple[dict, bool]:
        options = dict(kwargs or {})
        exc_info = bool(options.pop("exc_info", False))
        # 兼容 logging 风格参数，rich Console.print 不支持这些参数。
        options.pop("stack_info", None)
        options.pop("extra", None)
        return options, exc_info

    def info(self, text: str, output=True, **kwargs):
        if output:
            options, _ = self._normalize_console_kwargs(kwargs)
            self.console.print(text, style=INFO, **options)

    def warning(self, text: str, output=True, **kwargs):
        if output:
            options, _ = self._normalize_console_kwargs(kwargs)
            self.console.print(text, style=WARNING, **options)

    def error(self, text: str, output=True, **kwargs):
        if output:
            options, exc_info = self._normalize_console_kwargs(kwargs)
            if exc_info:
                trace = traceback.format_exc()
                if trace and trace.strip() and trace.strip() != "NoneType: None":
                    text = f"{text}\n{trace}"
            self.console.print(text, style=ERROR, **options)

    def debug(self, text: str, **kwargs):
        if self.DEBUG:
            options, _ = self._normalize_console_kwargs(kwargs)
            self.console.print(text, style=DEBUG, **options)

    def print(self, text: str, style=GENERAL, **kwargs) -> None:
        options, _ = self._normalize_console_kwargs(kwargs)
        self.console.print(text, style=style, **options)
