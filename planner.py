"""plan-query: turn a spatial/temporal query into a pinned, verifiable plan.

The plan is bound to one ``build_id``; later catalog builds cannot change
it.  For every hit it records:

* the Item and the *real rectangle part indices* that matched which query
  piece (a plain bbox is only ever a candidate);
* the closed time-interval overlap;
* every asset identity ``(collection, item, asset_key, content_sha256)``
  with its size, 4096-byte block digests and ordered replica options.

Fetch may swap replicas but can never swap content: the content hash is
carried in the plan and re-checked against every byte that is read.
"""

import json
from dataclasses import dataclass, field

from catalog import CatalogError, item_matches_pieces, parse_time, split_bbox


@dataclass
class PlannedAsset:
    collection_id: str
    item_id: str
    asset_key: str
    content_sha256: str
    size: int
    blocks: list
    replicas: list
    # Optional byte range the caller wants; None means the whole asset.
    range_start: int | None = None
    range_end: int | None = None

    @property
    def identity(self):
        return (self.collection_id, self.item_id,
                self.asset_key, self.content_sha256)

    def to_dict(self):
        return {
            'identity': list(self.identity),
            'size': self.size,
            'range': [self.range_start, self.range_end],
            'blocks': self.blocks,
            'replicas': self.replicas,
        }


@dataclass
class Plan:
    plan_id: str
    build_id: str
    query: dict
    pieces: list
    candidate_item_ids: list
    hits: list = field(default_factory=list)
    rejected: list = field(default_factory=list)

    def to_dict(self):
        return {
            'plan_id': self.plan_id,
            'build_id': self.build_id,
            'query': self.query,
            'dateline_pieces': [list(p) for p in self.pieces],
            'candidate_item_ids': [list(c) for c in self.candidate_item_ids],
            'hits': self.hits,
            'rejected_after_exact_filter': self.rejected,
        }

    def __getitem__(self, key):
        return self.to_dict()[key]

    def get(self, key, default=None):
        return self.to_dict().get(key, default)


def make_plan(store, bbox, time_interval, build_id=None, *,
              asset_ranges=None, plan_id='plan'):
    """Build a plan.

    ``bbox``          -- [west, south, east, north]; west>east splits.
    ``time_interval`` -- [start_iso, end_iso], closed on both ends.
    ``asset_ranges``  -- optional {(item_id, asset_key): (start, end)} to
                         request only part of an asset.
    """
    build_id = store.resolve_build(build_id)
    pieces = split_bbox(bbox)
    q_start = parse_time(time_interval[0])
    q_end = parse_time(time_interval[1])
    if q_start > q_end:
        raise CatalogError('query time interval start is after end')

    candidate_keys = store.candidate_items(build_id, pieces)
    hits, rejected = [], []
    for collection_id, item_id in candidate_keys:
        item = store.load_item(build_id, collection_id, item_id)
        ok, hit_parts = item_matches_pieces(
            item['parts'], pieces,
            item['time_start_dt'], item['time_end_dt'], q_start, q_end)
        if not ok:
            reason = ('time' if not (item['time_start_dt'] <= q_end and
                                     item['time_end_dt'] >= q_start)
                      else 'exact-geometry')
            rejected.append({'collection': collection_id, 'item': item_id,
                             'reason': reason})
            continue
        matched_pieces = []
        for pidx in hit_parts:
            part = item['parts'][pidx]
            for qidx, piece in enumerate(pieces):
                from catalog import rects_intersect
                if rects_intersect(part, piece):
                    matched_pieces.append({'part_index': pidx,
                                           'query_piece': qidx,
                                           'rect': list(part)})
        planned_assets = []
        for key in store.item_asset_keys(build_id, collection_id, item_id):
            asset = store.load_asset(build_id, collection_id, item_id, key)
            rng = None
            if asset_ranges and (item_id, key) in asset_ranges:
                rng = asset_ranges[(item_id, key)]
                if not (0 <= rng[0] < rng[1] <= asset['size']):
                    raise CatalogError(
                        f'range {rng} outside asset {item_id}/{key} '
                        f"of size {asset['size']}")
            planned_assets.append(PlannedAsset(
                collection_id, item_id, key, asset['content_sha256'],
                asset['size'], asset['blocks'], asset['replicas'],
                rng[0] if rng else None, rng[1] if rng else None))
        hits.append({
            'collection': collection_id,
            'item': item_id,
            'time_interval': [item['time_start'], item['time_end']],
            'hit_parts': matched_pieces,
            'asset_identities': [list(a.identity) for a in planned_assets],
            'assets': [a.to_dict() for a in planned_assets],
        })

    plan = Plan(plan_id, build_id,
                {'bbox': list(bbox), 'time': list(time_interval)},
                pieces, candidate_keys, hits, rejected)
    return plan


def save_plan(plan, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(plan.to_dict(), f, indent=2, sort_keys=True)


def load_plan(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)
