"""Bound text placed into live TUI widgets; the detail cache keeps full values."""


def preview(value: str, limit: int = 4096) -> str:
    if len(value) <= limit:
        return value
    return f"{value[:limit]}… ({len(value)} chars; use scoped CLI show for full value)"
