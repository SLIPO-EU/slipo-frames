import math

from datetime import datetime


def timestamp_to_datetime(value: str or None) -> datetime or None:
    if value is None:
        return None
    if math.isnan(value):
        return None

    return datetime.fromtimestamp(value / 1000)


def format_file_size(num: int, suffix: str = 'B') -> str:
    for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E']:
        if abs(num) < 1024.0:
            return '%3.1f %s%s' % (num, unit, suffix)
        num /= 1024.0

    return '%.1f%s%s' % (num, 'Z', suffix)
