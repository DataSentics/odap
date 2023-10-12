from typing import Optional


def widget_prefix(prefix: Optional[str]) -> str:
    prefix = "" if prefix is None else prefix

    return f"[{prefix}] "
