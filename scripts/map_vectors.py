"""Discover vectors for a StatsCan table, filtered by geography.

Downloads the full table CSV to extract vector IDs for Nova Scotia,
Halifax, and Canada. Outputs a mapping suitable for config.py.

Run: python -m scripts.map_vectors 18100004
"""

from __future__ import annotations

import csv
import io
import json
import sys
import zipfile

import httpx

from pipeline.statcan_client import StatCanClient

# Geographies we care about
TARGET_GEOS = ["Canada", "Nova Scotia", "Halifax, Nova Scotia"]


def discover_vectors(product_id: int):
    print(f"Discovering vectors for table {product_id}...\n")

    with StatCanClient() as client:
        # Get metadata for context
        meta = client.get_cube_metadata(product_id)
        title = meta.get("cubeTitleEn", "Unknown")
        print(f"Table: {product_id} — {title}")

        # Get the CSV download URL
        print("Fetching CSV download URL...")
        resp = client._request("GET", f"getFullTableDownloadCSV/{product_id}/en")
        if isinstance(resp, dict) and "object" in resp:
            csv_url = resp["object"]
        elif isinstance(resp, str):
            csv_url = resp
        else:
            print(f"Unexpected response: {resp}")
            return

    print(f"Downloading from {csv_url}...")
    dl = httpx.get(csv_url, follow_redirects=True, timeout=120)
    dl.raise_for_status()
    print(f"Downloaded {len(dl.content):,} bytes")

    z = zipfile.ZipFile(io.BytesIO(dl.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv") and "MetaData" not in n][0]

    # Parse CSV and extract vectors for target geographies
    vectors_by_geo: dict[str, list[dict]] = {geo: [] for geo in TARGET_GEOS}

    with z.open(csv_name) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        fieldnames = reader.fieldnames
        print(f"\nCSV columns: {fieldnames}\n")

        # Identify the non-standard dimension columns
        # Standard columns: REF_DATE, GEO, DGUID, UOM, UOM_ID, SCALAR_FACTOR,
        #                   SCALAR_ID, VECTOR, COORDINATE, VALUE, STATUS, SYMBOL,
        #                   TERMINATED, DECIMALS
        standard = {
            "REF_DATE", "GEO", "DGUID", "UOM", "UOM_ID", "SCALAR_FACTOR",
            "SCALAR_ID", "VECTOR", "COORDINATE", "VALUE", "STATUS", "SYMBOL",
            "TERMINATED", "DECIMALS",
        }
        dim_columns = [c for c in fieldnames if c not in standard]
        print(f"Dimension columns: {dim_columns}")

        seen_vectors = set()
        for row in reader:
            geo = row.get("GEO", "")
            vector = row.get("VECTOR", "")
            if geo in TARGET_GEOS and vector and vector not in seen_vectors:
                seen_vectors.add(vector)
                dim_values = {c: row.get(c, "") for c in dim_columns}
                vectors_by_geo[geo].append({
                    "vector": vector,
                    "coordinate": row.get("COORDINATE", ""),
                    **dim_values,
                })

    # Display results
    for geo in TARGET_GEOS:
        vecs = vectors_by_geo[geo]
        print(f"\n{'='*60}")
        print(f"  {geo}: {len(vecs)} vectors")
        print(f"{'='*60}")
        for v in vecs[:30]:
            dim_str = ", ".join(f"{k}={v[k]}" for k in dim_columns if v.get(k))
            vid = v["vector"].lstrip("v")
            print(f"  {v['vector']} (coord {v['coordinate']}): {dim_str}")
        if len(vecs) > 30:
            print(f"  ... and {len(vecs) - 30} more")

    # Output config.py format
    print(f"\n{'='*60}")
    print(f"  config.py vectors dict")
    print(f"{'='*60}")
    print(f'"vectors": {{')
    for geo in TARGET_GEOS:
        short_geo = geo.split(",")[0]  # "Halifax, Nova Scotia" -> "Halifax"
        print(f"    # {short_geo}")
        for v in vectors_by_geo[geo]:
            dim_str = ";".join(v.get(c, "") for c in dim_columns)
            vid = v["vector"].lstrip("v")
            label = f"{short_geo};{dim_str}"
            print(f'    "{label}": {vid},')
    print("}")

    # Save fixture
    out_path = f"tests/fixtures/vectors_{product_id}.json"
    with open(out_path, "w") as f:
        json.dump(vectors_by_geo, f, indent=2, default=str)
    print(f"\nVector data saved to {out_path}")

    # Save metadata fixture
    meta_path = f"tests/fixtures/metadata_{product_id}.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    print(f"Metadata saved to {meta_path}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.map_vectors <product_id>")
        print("Example: python -m scripts.map_vectors 18100004")
        sys.exit(1)

    product_id = int(sys.argv[1].replace("-", ""))
    discover_vectors(product_id)


if __name__ == "__main__":
    main()
