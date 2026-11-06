"""Pixeltable UDFs for youth soccer match video ingest (module path e.g. `udfs.match_label`)."""

from __future__ import annotations

import pixeltable as pxt


@pxt.udf
def match_label(home: str, away: str, age_group: str) -> str:
    """Compact label like 'U12 · Hawks vs Otters'."""
    h = (home or '?').strip()
    a = (away or '?').strip()
    g = (age_group or '?').strip()
    return f'{g} · {h} vs {a}'


@pxt.udf
def duration_band(seconds: float) -> str:
    """Bucket match recording length for ops dashboards."""
    if seconds is None:
        return 'unknown'
    s = float(seconds)
    if s < 60:
        return 'clip'
    if s < 180:
        return 'half'
    return 'full'


@pxt.udf
def kickoff_hour_local(hour: int) -> str:
    """Morning / afternoon / evening slot from the local (default TZ) hour."""
    if hour is None:
        return 'unknown'
    h = int(hour)
    if h < 12:
        return 'morning'
    if h < 17:
        return 'afternoon'
    return 'evening'
