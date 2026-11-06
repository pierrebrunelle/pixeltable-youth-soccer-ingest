"""Render tiny youth-soccer field recordings into data/ (no downloads).

Each match is a short solid-color MP4 with a scoreboard overlay so video
metadata and video_splitter have real bytes to work with.

    python make_videos.py
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path

import av
import numpy as np
from PIL import Image, ImageDraw

HERE = Path(__file__).parent
DATA = HERE / 'data'
FPS = 10
W, H = 320, 180

# match_id, age, home, away, field, kickoff_iso (with offset), seconds, color
MATCHES = [
    (
        'm-u10-hawks-otters',
        'U10',
        'Hawks',
        'Otters',
        'Shoreline Field A',
        '2026-11-01T09:00:00-07:00',  # Pacific PDT
        9.0,
        (34, 139, 34),
    ),
    (
        'm-u12-comets-wolves',
        'U12',
        'Comets',
        'Wolves',
        'Shoreline Field B',
        '2026-11-01T14:00:00-07:00',  # Pacific afternoon
        12.0,
        (30, 144, 255),
    ),
    (
        'm-u12-jets-lions',
        'U12',
        'Jets',
        'Lions',
        'Eastside Turf',
        '2026-11-01T16:00:00-04:00',  # Eastern — same UTC afternoon as 13:00 PT
        10.0,
        (220, 20, 60),
    ),
    (
        'm-u14-storm-fc',
        'U14',
        'Storm',
        'FC North',
        'Shoreline Field A',
        '2026-11-01T18:30:00-07:00',  # Pacific evening
        11.0,
        (72, 61, 139),
    ),
    (
        'm-u8-pumas-bees',
        'U8',
        'Pumas',
        'Bees',
        'Eastside Turf',
        '2026-11-02T10:15:00',  # naive — interpreted in default TZ (America/Los_Angeles)
        8.0,
        (255, 140, 0),
    ),
]


def render(match_id: str, home: str, away: str, seconds: float, color: tuple[int, int, int]) -> Path:
    DATA.mkdir(parents=True, exist_ok=True)
    out = DATA / f'{match_id}.mp4'
    n = max(1, int(round(seconds * FPS)))
    container = av.open(str(out), mode='w')
    stream = container.add_stream('libx264', rate=FPS)
    stream.width, stream.height = W, H
    stream.pix_fmt = 'yuv420p'
    stream.time_base = Fraction(1, FPS)
    for i in range(n):
        img = Image.new('RGB', (W, H), color)
        d = ImageDraw.Draw(img)
        d.text((12, 12), match_id, fill=(255, 255, 255))
        d.text((12, 40), f'{home} vs {away}', fill=(255, 255, 255))
        d.text((12, 70), f'frame {i + 1}/{n}', fill=(230, 230, 230))
        frame = av.VideoFrame.from_ndarray(np.asarray(img), format='rgb24')
        frame = frame.reformat(format='yuv420p')
        frame.pts = i
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()
    return out


def main() -> None:
    catalog = []
    for mid, age, home, away, field, kickoff, secs, color in MATCHES:
        path = render(mid, home, away, secs, color)
        catalog.append(
            {
                'match_id': mid,
                'age_group': age,
                'home': home,
                'away': away,
                'field': field,
                'kickoff': kickoff,
                'video': path.name,
                'seconds': secs,
            }
        )
        print('wrote', path.name, secs, 's')
    (DATA / 'manifest.json').write_text(json.dumps(catalog, indent=2))
    print('manifest', len(catalog), 'matches')


if __name__ == '__main__':
    main()
