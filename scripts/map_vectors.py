"""Discover vectors for a StatsCan table, filtered by geography.

Pulls cube metadata and identifies vectors for Nova Scotia, Halifax,
and Canada. Outputs a mapping suitable for pasting into config.py.

Run: python -m scripts.map_vectors 18100004
"""

from __future__ import annotations

import json
import sys

from pipeline.statcan_client import StatCanClient


# Geographies we care about
TARGET_GEOS = [
    "Canada",
    "Nova Scotia",
    "Halifax",
    # CMA-level (Halifax Census Metropolitan Area)
    "Halifax, Nova Scotia",
]


def discover_vectors(product_id: int):
    print(f"Discovering vectors for table {product_id}...\n")

    with StatCanClient() as client:
        meta = client.get_cube_metadata(product_id)

    title = meta.get("cubeTitleEn", "Unknown")
    print(f"Table: {product_id} — {title}\n")

    dims = meta.get("dimension", [])
    geo_dim = None
    other_dims = []

    # Identify geography dimension vs. other dimensions
    for d in dims:
        name = d.get("dimensionNameEn", "").lower()
        if "geography" in name or "geo" in name:
            geo_dim = d
        else:
            other_dims.append(d)

    if not geo_dim:
        print("WARNING: No geography dimension found. Listing all dimensions:")
        for d in dims:
            members = d.get("member", [])
            print(f"  [{d.get('dimensionPositionId')}] {d.get('dimensionNameEn')}: {len(members)} members")
            for m in members[:10]:
                print(f"    {m.get('memberId')}: {m.get('memberNameEn')}")
        return

    # Find our target geographies
    print("Geography dimension members:")
    target_members = []
    for m in geo_dim.get("member", []):
        name = m.get("memberNameEn", "")
        matched = any(t.lower() in name.lower() for t in TARGET_GEOS)
        marker = " <<<" if matched else ""
        print(f"  {m.get('memberId')}: {name}{marker}")
        if matched:
            target_members.append(m)

    print(f"\n--- Target geographies found: {len(target_members)} ---\n")

    # Show other dimensions (so user understands what the coordinates mean)
    print("Other dimensions:")
    for d in other_dims:
        members = d.get("member", [])
        print(f"  [{d.get('dimensionPositionId')}] {d.get('dimensionNameEn')}: {len(members)} members")
        for m in members[:15]:
            print(f"    {m.get('memberId')}: {m.get('memberNameEn')}")
        if len(members) > 15:
            print(f"    ... and {len(members) - 15} more")

    # Output structured mapping
    print(f"\n{'='*60}")
    print(f"  Vector mapping template for config.py")
    print(f"{'='*60}")
    print(f'  # Table {product_id}: {title}')
    print(f'  "vectors": {{')
    for m in target_members:
        geo = m.get("memberNameEn", "")
        mid = m.get("memberId", "")
        print(f'      # {geo} (member {mid})')
        print(f'      # TODO: Use getSeriesInfoFromVector or full CSV to get vector IDs')
    print(f'  }}')

    # Save full metadata for reference
    out_path = f"tests/fixtures/metadata_{product_id}.json"
    with open(out_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"\nFull metadata saved to {out_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.map_vectors <product_id>")
        print("Example: python -m scripts.map_vectors 18100004")
        sys.exit(1)

    product_id = int(sys.argv[1].replace("-", ""))
    discover_vectors(product_id)


if __name__ == "__main__":
    main()
