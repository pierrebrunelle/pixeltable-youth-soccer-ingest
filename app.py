"""Youth soccer match video ingestion pipeline built with Pixeltable.

A club records U8–U14 matches across fields that sit in different time zones.
Kickoff times are ingested as aware timestamps; Pixeltable stores them in UTC
and retrieves them in the configured default time zone (time_zone in
pixeltable.toml). Computed columns pull video metadata and split each
recording into fixed-length highlight clips. pipeline.py runs as an
incremental scheduled job with a crash/resume watermark. No HTTP router.

    pxt schema update app.py soccer
    python make_videos.py
    python pipeline.py
"""

from __future__ import annotations

import pixeltable as pxt
import pixeltable.functions as pxtf

from udfs import duration_band, kickoff_hour_local, match_label

TableModel = pxt.model_base()


class Matches(TableModel, name='matches', has_default_idxs=False):
    """One youth soccer match recording and its kickoff clock."""

    match_id = pxt.Column(type=pxt.String, primary_key=True)
    age_group: pxt.String
    home: pxt.String
    away: pxt.String
    field: pxt.String
    # Aware or naive; naive uses the configured default time zone on insert.
    kickoff: pxt.Timestamp
    video: pxt.Video

    label = match_label(home, away, age_group)
    meta = pxtf.video.get_metadata(video)
    duration = pxtf.video.get_duration(video)
    band = duration_band(duration)
    # Default-TZ calendar fields (retrieval is always in the configured TZ).
    kickoff_day = pxtf.timestamp.strftime(kickoff, '%a %Y-%m-%d')
    kickoff_hour = pxtf.timestamp.hour(kickoff)
    kickoff_slot = kickoff_hour_local(kickoff_hour)
    # Explicit conversions for clubs that publish in Eastern / UTC.
    kickoff_et = kickoff.astimezone('America/New_York')
    kickoff_utc = kickoff.astimezone('UTC')
    kickoff_et_day = pxtf.timestamp.strftime(kickoff_et, '%a %Y-%m-%d %H:%M %Z')

    __indexes__ = [
        pxt.BtreeIndex(age_group),
        pxt.BtreeIndex(kickoff),
        pxt.BtreeIndex(field),
    ]


class Clips(
    TableModel,
    name='clips',
    base=Matches,
    iterator=pxtf.video.video_splitter(Matches.video, duration=3.0, min_segment_duration=0.5),
):
    """Fixed-length highlight clips cut from each match recording."""

    clip_seconds = pxtf.video.get_duration(video_segment)  # type: ignore[name-defined]  # noqa: F821


@pxt.query
def matches_on_day(day: str):
    """Matches whose local kickoff day equals day (format 'Sat 2026-11-01')."""
    return (
        Matches.where(Matches.kickoff_day == day)
        .order_by(Matches.kickoff)
        .select(
            Matches.match_id,
            Matches.label,
            Matches.field,
            Matches.kickoff,
            Matches.kickoff_et_day,
            Matches.band,
            Matches.duration,
        )
    )


@pxt.query
def field_schedule(field: str):
    """All matches on one field, earliest kickoff first."""
    return (
        Matches.where(Matches.field == field)
        .order_by(Matches.kickoff)
        .select(
            Matches.match_id,
            Matches.label,
            Matches.kickoff_day,
            Matches.kickoff_slot,
            Matches.kickoff_et_day,
            Matches.duration,
        )
    )


@pxt.query
def afternoon_u12():
    """U12 matches kicking off in the afternoon slot on the club clock."""
    return (
        Matches.where((Matches.age_group == 'U12') & (Matches.kickoff_slot == 'afternoon'))
        .order_by(Matches.kickoff)
        .select(Matches.match_id, Matches.label, Matches.field, Matches.kickoff_day, Matches.kickoff_et_day)
    )
