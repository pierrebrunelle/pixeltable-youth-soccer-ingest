#!/usr/bin/env python3
"""Part of the youth soccer match recordings API example built with Pixeltable.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pixeltable as pxt

import udfs  # noqa: F401 — register UDF module path

ROOT = Path(__file__).resolve().parent
LOG = ROOT / 'logs' / 'run.jsonl'
STATE = ROOT / 'state.json'
MANIFEST = ROOT / 'data' / 'manifest.json'
VENV_PXT = Path(__file__).resolve().parents[2] / '.venv' / 'bin' / 'pxt'
LOCAL_CATALOG = 'soccer'
BATCH_SIZE = 2


def log(event: str, **payload) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open('a') as f:
        f.write(
            json.dumps(
                {'event': event, 'ts': datetime.now().isoformat(timespec='seconds'), **payload},
                default=str,
            )
            + '\n'
        )
    print(event, json.dumps(payload, default=str)[:500], flush=True)


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {'next_idx': 0, 'runs': 0, 'crashed': False}


def save_state(st: dict) -> None:
    STATE.write_text(json.dumps(st, indent=2, default=str))


def run_schema(target: str) -> None:
    cmd = [str(VENV_PXT), 'schema', 'update', 'app.py', target, '-f']
    log('schema_update_start', target=target)
    t0 = time.time()
    r = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    log(
        'schema_update_done',
        exit=r.returncode,
        seconds=round(time.time() - t0, 2),
        stdout=(r.stdout or '')[-800:],
        stderr=(r.stderr or '')[-800:],
    )
    if r.returncode != 0:
        raise SystemExit(f'schema update failed: {r.returncode}')


def ensure_videos() -> list[dict]:
    if not MANIFEST.exists():
        log('make_videos_start')
        r = subprocess.run([sys.executable, 'make_videos.py'], cwd=str(ROOT), capture_output=True, text=True)
        log(
            'make_videos_done',
            exit=r.returncode,
            stdout=(r.stdout or '')[-400:],
            stderr=(r.stderr or '')[-400:],
        )
        if r.returncode != 0:
            raise SystemExit('make_videos failed')
    return json.loads(MANIFEST.read_text())


def parse_kickoff(raw: str) -> datetime:
    if raw.endswith('Z'):
        return datetime.fromisoformat(raw.replace('Z', '+00:00'))
    return datetime.fromisoformat(raw)


def existing_ids(matches) -> set[str]:
    rows = matches.select(matches.match_id).collect()
    return {r['match_id'] for r in rows}


def ingest_slice(rows: list[dict], table_root: str) -> int:
    matches = pxt.get_table(f'{table_root}/matches')
    have = existing_ids(matches)
    to_insert = []
    for r in rows:
        if r['match_id'] in have:
            continue
        to_insert.append(
            {
                'match_id': r['match_id'],
                'age_group': r['age_group'],
                'home': r['home'],
                'away': r['away'],
                'field': r['field'],
                'kickoff': parse_kickoff(r['kickoff']),
                'video': str(ROOT / 'data' / r['video']),
            }
        )
    if not to_insert:
        log('insert_skip', reason='all match_ids already present', n=len(rows))
        return 0
    t0 = time.time()
    status = matches.insert(to_insert)
    log('insert_done', rows=len(to_insert), seconds=round(time.time() - t0, 2), status=str(status))
    return len(to_insert)


def exercise_tz(table_root: str) -> None:
    matches = pxt.get_table(f'{table_root}/matches')
    sample = list(
        matches.select(
            matches.match_id,
            matches.kickoff,
            matches.kickoff_day,
            matches.kickoff_hour,
            matches.kickoff_slot,
            matches.kickoff_et_day,
            matches.duration,
            matches.band,
            matches.label,
        )
        .order_by(matches.kickoff)
        .collect()
    )
    day = list(
        matches.where(matches.kickoff_day == 'Sat 2026-11-01')
        .select(matches.match_id, matches.kickoff_day, matches.kickoff_et_day, matches.kickoff_slot)
        .order_by(matches.kickoff)
        .collect()
    )
    afternoon = list(
        matches.where((matches.age_group == 'U12') & (matches.kickoff_slot == 'afternoon'))
        .select(matches.match_id, matches.label, matches.kickoff_day, matches.kickoff_et_day)
        .collect()
    )
    clips = pxt.get_table(f'{table_root}/clips')
    clip_n = clips.count()
    from pixeltable.env import Env

    tz = Env.get().default_time_zone
    log(
        'tz_queries_ok',
        default_tz=str(tz),
        sample=sample,
        day_n=len(day),
        day=day,
        afternoon_u12=afternoon,
        clip_n=clip_n,
        total=matches.count(),
    )


def cloud_keepalive_ops(uri: str) -> None:
    """Sequential cloud reads to exercise pooled proxy connections (#1875)."""
    log('cloud_keepalive_start', uri=uri)
    for i in range(5):
        t0 = time.time()
        matches = pxt.get_table(f'{uri}/matches')
        n = matches.count()
        _ = list(matches.select(matches.match_id, matches.kickoff_day).limit(3).collect())
        log('cloud_keepalive_tick', i=i, count=n, seconds=round(time.time() - t0, 3))
        time.sleep(0.5)
    log('cloud_keepalive_done')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--reset', action='store_true')
    ap.add_argument('--all-batches', action='store_true')
    ap.add_argument('--crash-after', type=int, default=0, help='abort after this many new inserts')
    ap.add_argument('--cloud', default='', help='cloud URI e.g. pxt://<your-org>:<your-db>')
    ap.add_argument('--skip-schema', action='store_true')
    args = ap.parse_args()

    if args.reset:
        save_state({'next_idx': 0, 'runs': 0, 'crashed': False})
        log('watermark_reset')
        return 0

    table_root = args.cloud if args.cloud else LOCAL_CATALOG
    schema_target = args.cloud if args.cloud else LOCAL_CATALOG

    if not args.skip_schema:
        run_schema(schema_target)

    rows = ensure_videos()
    st = load_state()
    inserted_total = 0
    remaining_crash = args.crash_after if args.crash_after > 0 else None

    while st['next_idx'] < len(rows):
        end = min(st['next_idx'] + BATCH_SIZE, len(rows))
        batch = rows[st['next_idx']:end]
        log('batch_start', start=st['next_idx'], end=end, ids=[b['match_id'] for b in batch])
        n = ingest_slice(batch, table_root)
        inserted_total += n
        st['next_idx'] = end
        st['runs'] = st.get('runs', 0) + 1
        save_state(st)

        if remaining_crash is not None:
            remaining_crash -= n
            if remaining_crash <= 0:
                st['crashed'] = True
                save_state(st)
                log('simulated_crash', next_idx=st['next_idx'], inserted_total=inserted_total)
                return 99

        if not args.all_batches and args.crash_after <= 0:
            break

    exercise_tz(table_root)
    if args.cloud:
        cloud_keepalive_ops(args.cloud)

    log(
        'pipeline_complete',
        next_idx=st['next_idx'],
        inserted_total=inserted_total,
        crashed=st.get('crashed'),
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
