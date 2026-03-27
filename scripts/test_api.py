"""StatsCan WDS API smoke test.

Verifies that live API calls work and that we can pull CPI data for Nova Scotia.
Run: python -m scripts.test_api
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta

from pipeline.statcan_client import StatCanClient, StatCanError

CPI_PID = 18100004  # Consumer Price Index, monthly, not seasonally adjusted


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def test_get_cube_metadata(client: StatCanClient) -> dict:
    section("1. getCubeMetadata — CPI table structure")
    meta = client.get_cube_metadata(CPI_PID)
    print(f"Table: {meta.get('productId')} — {meta.get('cubeTitleEn')}")
    dims = meta.get("dimension", [])
    print(f"Dimensions: {len(dims)}")
    for d in dims:
        members = d.get("member", [])
        print(f"  [{d.get('dimensionPositionId')}] {d.get('dimensionNameEn')}: {len(members)} members")
        # Show first 5 members
        for m in members[:5]:
            print(f"       {m.get('memberId')}: {m.get('memberNameEn')}")
        if len(members) > 5:
            print(f"       ... and {len(members) - 5} more")
    return meta


def find_ns_vectors(meta: dict) -> list[int]:
    """Extract vector IDs for Nova Scotia from CPI metadata."""
    section("2. Finding Nova Scotia vectors in CPI table")
    vectors = []
    dims = meta.get("dimension", [])

    # Look through dimensions for geography
    for d in dims:
        for m in d.get("member", []):
            name = m.get("memberNameEn", "")
            if "Nova Scotia" in name or "nova scotia" in name.lower():
                print(f"  Found NS member: {name} (ID: {m.get('memberId')})")

    # Also search for vectors in the series metadata if available
    # The cube metadata may not have vectors directly — we may need
    # getSeriesInfoFromVector or the full CSV to discover them.
    print("\n  Note: Vector IDs are not always in cube metadata.")
    print("  Use map_vectors.py for full vector discovery.")
    return vectors


def test_changed_cubes(client: StatCanClient):
    section("3. getChangedCubeList — what changed recently?")
    # Check last 7 days to ensure we get some results
    start = date.today() - timedelta(days=7)
    changes = client.get_changed_cube_list(start)
    print(f"Tables changed since {start}: {len(changes)}")

    # Check if CPI is in the list
    cpi_found = False
    for c in changes:
        pid = c.get("productId")
        if pid == CPI_PID:
            cpi_found = True
            print(f"\n  *** CPI table found in changes ***")
            print(f"  Release time: {c.get('releaseTime')}")
    if not cpi_found:
        print(f"  CPI table ({CPI_PID}) not in recent changes (normal if no release this week)")

    # Show a few changed tables
    print(f"\n  Sample of changed tables:")
    for c in changes[:10]:
        print(f"    {c.get('productId')}: released {c.get('releaseTime')}")


def test_latest_data(client: StatCanClient):
    section("4. getDataFromVectorsAndLatestNPeriods — pull sample data")
    # v41690973 is CPI All-items, Canada (a well-known vector)
    # If this vector doesn't work, we'll discover the right ones with map_vectors.py
    test_vector = 41690973
    print(f"Pulling latest 6 periods for vector {test_vector} (CPI All-items, Canada)...")

    try:
        result = client.get_data_from_vectors_latest_n([test_vector], n=6)
        if result:
            obj = result[0].get("object", result[0])
            points = obj.get("vectorDataPoint", [])
            vector_info = {
                "vectorId": obj.get("vectorId"),
                "coordinate": obj.get("coordinate"),
            }
            print(f"  Vector info: {json.dumps(vector_info, indent=2)}")
            print(f"  Data points ({len(points)}):")
            for pt in points:
                ref = pt.get("refPer") or pt.get("refPerRaw")
                val = pt.get("value")
                print(f"    {ref}: {val}")
        else:
            print("  No data returned. Vector ID may need updating.")
            print("  Run map_vectors.py to discover current NS vectors.")
    except StatCanError as e:
        print(f"  Error: {e}")
        print("  This is expected if the vector ID is outdated. Run map_vectors.py.")


def test_code_sets(client: StatCanClient):
    section("5. getCodeSets — reference codes")
    codes = client.get_code_sets()
    if isinstance(codes, list):
        for codeset in codes:
            obj = codeset.get("object", codeset) if isinstance(codeset, dict) else codeset
            if isinstance(obj, dict):
                desc = obj.get("codeSetTitleEn", obj.get("descEn", "unknown"))
                print(f"  Code set: {desc}")
    elif isinstance(codes, dict):
        for key, val in codes.items():
            print(f"  {key}: {type(val).__name__}")


def main():
    print("Stats Bluenoser — StatsCan WDS API Smoke Test")
    print(f"Date: {date.today()}")

    with StatCanClient() as client:
        try:
            # 1. Metadata
            meta = test_get_cube_metadata(client)

            # 2. Find NS vectors
            find_ns_vectors(meta)

            # 3. Changed cubes
            test_changed_cubes(client)

            # 4. Pull actual data
            test_latest_data(client)

            # 5. Code sets
            test_code_sets(client)

        except StatCanError as e:
            print(f"\n*** StatsCan API Error: {e} ***", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"\n*** Unexpected error: {e} ***", file=sys.stderr)
            raise

    section("DONE")
    print("API is reachable and returning data.")
    print("Next step: run map_vectors.py to discover NS/Halifax/Canada vectors for CPI.")


if __name__ == "__main__":
    main()
