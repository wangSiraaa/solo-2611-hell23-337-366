"""STAC 1.0 discovery, validation and exact spatial/temporal predicates.

The baseline functions :func:`discover` and :func:`search` are preserved so
``baseline_demo.py`` still runs.  The new code adds:

* strict parsing of the limited legacy subset (Catalog/Collection/Item,
  axis-aligned rectangular Polygon/MultiPolygon, closed time intervals);
* extraction of the *real* rectangle parts of every geometry (a dateline
  MultiPolygon becomes two parts instead of one bogus world-wide bbox);
* exact intersection tests and dateline-aware query splitting.

A plain item bbox may only be used as a *candidate*; membership is always
confirmed against the real rectangle parts.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

# Legacy contract limits.
MAX_ITEMS = 10_000
MAX_ASSETS = 30_000
MAX_REPLICAS_PER_ASSET = 3


class CatalogError(ValueError):
    """Validation failure for an input STAC document or manifest."""


# --------------------------------------------------------------------------
# Baseline-compatible helpers (kept for baseline_demo.py).
# --------------------------------------------------------------------------
def discover(root):
    out = []
    for p in Path(root).rglob('*.json'):
        d = json.load(open(p))
        if d.get('type') == 'Feature':
            out.append(d)
    return out


def search(items, bbox):
    """Baseline candidate-only bbox search (kept for the legacy demo)."""
    w, s, e, n = bbox
    return [i for i in items
            if i['bbox'][0] <= e and i['bbox'][2] >= w
            and i['bbox'][1] <= n and i['bbox'][3] >= s]


# --------------------------------------------------------------------------
# Time handling: closed intervals in UTC.
# --------------------------------------------------------------------------
def parse_time(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise CatalogError(f'timestamp must be a string, got {value!r}')
    txt = value.strip()
    if txt.endswith('Z'):
        txt = txt[:-1] + '+00:00'
    try:
        dt = datetime.fromisoformat(txt)
    except ValueError as exc:
        raise CatalogError(f'invalid timestamp {value!r}: {exc}')
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def item_time_interval(item):
    """Return the closed [start, end] UTC interval of an Item."""
    props = item.get('properties', {})
    if props.get('datetime') is not None:
        t = parse_time(props['datetime'])
        return t, t
    start = parse_time(props.get('start_datetime'))
    end = parse_time(props.get('end_datetime'))
    if start is None or end is None:
        raise CatalogError(
            f"item {item.get('id')!r} needs datetime or start/end_datetime")
    if start > end:
        raise CatalogError(f"item {item.get('id')!r}: start_datetime > end")
    return start, end


def time_intersects(item_start, item_end, q_start, q_end):
    """Closed-interval overlap; touching at an endpoint counts as a hit."""
    return item_start <= q_end and item_end >= q_start


# --------------------------------------------------------------------------
# Geometry: only axis-aligned rectangular Polygon / MultiPolygon.
# --------------------------------------------------------------------------
def _ring_to_rect(ring):
    """Validate one axis-aligned rectangle ring and return (w, s, e, n)."""
    if len(ring) != 5:
        raise CatalogError('rectangle ring must have exactly 5 positions')
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    if any(not isinstance(v, (int, float)) for v in xs + ys):
        raise CatalogError('coordinates must be numbers')
    w, e = min(xs), max(xs)
    s, n = min(ys), max(ys)
    # A valid axis-aligned rectangle ring visits exactly two distinct x and
    # two distinct y values and closes back on the first point.
    if (len(set(xs)) != 2 or len(set(ys)) != 2 or ring[0] != ring[-1]):
        raise CatalogError(f'ring is not a closed axis-aligned rectangle: {ring}')
    if not (-180.0 <= w <= e <= 180.0 and -90.0 <= s <= n <= 90.0):
        raise CatalogError(f'rectangle outside lon/lat bounds: {(w, s, e, n)}')
    return (float(w), float(s), float(e), float(n))


def geometry_parts(geometry):
    """Return the real rectangle parts of a limited-subset geometry.

    Each part is a tuple ``(west, south, east, north)`` in unwrapped
    longitude space (parts stay inside [-180, 180]; a dateline geometry is
    represented by two separate parts, never by one wide rectangle).
    """
    if not isinstance(geometry, dict):
        raise CatalogError('item geometry missing or not an object')
    gtype = geometry.get('type')
    coords = geometry.get('coordinates')
    parts = []
    if gtype == 'Polygon':
        if len(coords) != 1:
            raise CatalogError('only rectangle Polygons without holes are allowed')
        parts.append(_ring_to_rect(coords[0]))
    elif gtype == 'MultiPolygon':
        for poly in coords:
            if len(poly) != 1:
                raise CatalogError('only rectangle MultiPolygons without holes')
            parts.append(_ring_to_rect(poly[0]))
    else:
        raise CatalogError(f'unsupported geometry type {gtype!r}')
    if not parts:
        raise CatalogError('geometry has no rectangle parts')
    return parts


def rects_intersect(a, b):
    """Exact axis-aligned rectangle intersection; boundary touch counts."""
    aw, as_, ae, an = a
    bw, bs, be, bn = b
    return aw <= be and ae >= bw and as_ <= bn and an >= bs


def split_bbox(bbox):
    """Split a query bbox into ordinary longitude pieces.

    ``west > east`` means the query crosses the antimeridian and is split
    into two closed pieces.  A normal bbox yields one piece.  Each piece is
    an ordinary rectangle with ``west <= east``.
    """
    w, s, e, n = (float(x) for x in bbox)
    if not (-90.0 <= s <= n <= 90.0):
        raise CatalogError(f'invalid latitude in bbox {bbox}')
    if w > e:
        if not (-180.0 <= e and w <= 180.0):
            raise CatalogError(f'invalid dateline bbox {bbox}')
        return [(w, s, 180.0, n), (-180.0, s, e, n)]
    if not (-180.0 <= w <= e <= 180.0):
        raise CatalogError(f'invalid bbox longitude {bbox}')
    return [(w, s, e, n)]


def item_matches_pieces(parts, pieces, item_start, item_end, q_start, q_end):
    """Exact predicate: time overlap AND some real part hits some query piece."""
    if not time_intersects(item_start, item_end, q_start, q_end):
        return False, []
    hit_parts = []
    for idx, part in enumerate(parts):
        if any(rects_intersect(part, piece) for piece in pieces):
            hit_parts.append(idx)
    return bool(hit_parts), hit_parts


# --------------------------------------------------------------------------
# STAC graph discovery and validation.
# --------------------------------------------------------------------------
def _load(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def walk_stac(root):
    """Walk Catalog/Collection links and return (catalogs, collections, items).

    Item documents discovered by directory scan are also accepted, but each
    concrete file path is returned once even if a Collection links it twice
    (the weather fixture links date-line.json through a ``..`` duplicate).
    """
    root = Path(root).resolve()
    catalogs, collections, seen = {}, {}, set()
    items_by_path: dict[str, dict] = {}

    # BFS over catalog/collection link graphs from every Catalog document.
    queue = []
    for p in root.rglob('*.json'):
        doc = _load(p)
        doc['_path'] = str(p.resolve())
        if doc.get('type') == 'Catalog':
            catalogs[str(p.resolve())] = doc
            queue.append(p.resolve())
        elif doc.get('type') == 'Collection':
            collections[str(p.resolve())] = doc

    while queue:
        doc_path = queue.pop()
        doc = _load(doc_path)
        for link in doc.get('links', []):
            if link.get('rel') not in ('child', 'item'):
                continue
            target = (doc_path.parent / link['href']).resolve()
            if not str(target).startswith(str(root)) or not target.exists():
                # Missing link targets are tolerated at discovery; build()
                # reports them as warnings rather than fabricating items.
                continue
            if target in seen:
                continue
            seen.add(target)
            child = _load(target)
            child['_path'] = str(target)
            if child.get('type') == 'Catalog':
                if str(target) not in catalogs:
                    catalogs[str(target)] = child
                    queue.append(target)
            elif child.get('type') == 'Collection':
                collections.setdefault(str(target), child)
                queue.append(target)

    # Items linked from collections, plus any Feature file on disk.  Count
    # link occurrences so a Collection linking the same path twice is visible.
    link_counts: dict[str, int] = {}
    for cpath, coll in collections.items():
        for link in coll.get('links', []):
            if link.get('rel') != 'item':
                continue
            target = (Path(cpath).parent / link['href']).resolve()
            if not target.exists():
                continue
            link_counts[str(target)] = link_counts.get(str(target), 0) + 1
    duplicates = sorted(p for p, n in link_counts.items() if n > 1)
    for p in root.rglob('*.json'):
        doc = _load(p)
        if doc.get('type') == 'Feature':
            doc['_path'] = str(p.resolve())
            items_by_path[str(p.resolve())] = doc

    items = list(items_by_path.values())
    return (list(catalogs.values()), list(collections.values()),
            items, duplicates)


def validate_item(item, collection_ids):
    """Strict validation of one Item from the limited subset."""
    if item.get('type') != 'Feature' or not item.get('id'):
        raise CatalogError('STAC Item must be a Feature with an id')
    if item.get('collection') not in collection_ids:
        raise CatalogError(
            f"item {item['id']!r} references unknown collection "
            f"{item.get('collection')!r}")
    parts = geometry_parts(item['geometry'])
    bbox = item.get('bbox')
    if not (isinstance(bbox, list) and len(bbox) == 4):
        raise CatalogError(f"item {item['id']!r}: bbox must have 4 numbers")
    # The declared bbox must bound every real part (it is only a candidate).
    bw, bs, be, bn = (float(x) for x in bbox)
    for w, s, e, n in parts:
        if w < bw or s < bs or e > be or n > bn:
            raise CatalogError(
                f"item {item['id']!r}: bbox does not cover geometry part")
    item_time_interval(item)  # raises on bad temporal data
    if not isinstance(item.get('assets', {}), dict):
        raise CatalogError(f"item {item['id']!r}: assets must be an object")
    return parts


def enforce_limits(n_items, n_assets, replicas_per_asset):
    if n_items > MAX_ITEMS:
        raise CatalogError(f'item limit exceeded: {n_items} > {MAX_ITEMS}')
    if n_assets > MAX_ASSETS:
        raise CatalogError(f'asset limit exceeded: {n_assets} > {MAX_ASSETS}')
    for key, n in replicas_per_asset.items():
        if n > MAX_REPLICAS_PER_ASSET:
            raise CatalogError(
                f'asset {key}: replica limit exceeded: {n} > '
                f'{MAX_REPLICAS_PER_ASSET}')
