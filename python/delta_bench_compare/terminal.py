from __future__ import annotations

import os
import re
import sys

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def _supports_color() -> bool:
    """Return True when stdout is a TTY that likely supports ANSI escapes."""
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


_COLOR = _supports_color()

_RESET = "\033[0m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_DIM = "\033[2m"
_BOLD = "\033[1m"


def set_color_mode(enabled: bool) -> None:
    global _COLOR
    _COLOR = enabled


def red(text: str) -> str:
    return f"{_RED}{text}{_RESET}" if _COLOR else text


def green(text: str) -> str:
    return f"{_GREEN}{text}{_RESET}" if _COLOR else text


def yellow(text: str) -> str:
    return f"{_YELLOW}{text}{_RESET}" if _COLOR else text


def dim(text: str) -> str:
    return f"{_DIM}{text}{_RESET}" if _COLOR else text


def bold(text: str) -> str:
    return f"{_BOLD}{text}{_RESET}" if _COLOR else text


def visible_len(text: str) -> int:
    """Return the visible length of a string, stripping ANSI escape sequences."""
    return len(_ANSI_RE.sub("", text))
