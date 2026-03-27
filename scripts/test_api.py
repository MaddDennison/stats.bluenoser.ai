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
        for m in members[:5]:
            print(f"       {m.get('memberId')}: {m.get('memberNameEn')}")
        if len(members) > 5:
            print(f"       ... and {len(members) - 5} more")
    return meta


def find_ns_vectors(meta: dict):
    """Show geography dimension members matching NS/Halifax/Canada."""
    section("2. Geography dimension — NS/Halifax/Canada members")
    dims = meta.get("dimension", [])
    for d in dims:
        name = d.get("dimensionNameEn", "").lower()
        if "geography" in name or "geo" in name:
            print(f"Geography dimension [{d.get('dimensionPositionId')}]:")
            for m in d.get("member", []):
                mname = m.get("memberNameEn", "")
                if any(t in mname.lower() for t in ["canada", "nova scotia", "halifax"]):
                    print(f"  >>> {m.get('memberId')}: {mname}")
            return
    print("  No geography dimension found in this table.")


def test_changed_cubes(client: StatCanClient):
    section("3. getChangedCubeList — what changed recently?")
    start = date.today() - timedelta(days=7)
    changes = client.get_changed_cube_list(start)

    if isinstance(changes, list):
        print(f"Tables changed since {start}: {len(changes)}")
        cpi_found = False
        for c in changes:
            pid = c.get("productId")
            if pid == CPI_PID:
                cpi_found = True
                print(f"\n  *** CPI table found in changes ***")
                print(f"  Release time: {c.get('releaseTime')}")
        if not cpi_found:
            print(f"  CPI table ({CPI_PID}) not in recent changes (normal if no release this week)")

        print(f"\n  Sample of changed tables:")
        for c in changes[:10]:
            print(f"    {c.get('productId')}: released {c.get('releaseTime')}")
    else:
        print(f"  Unexpected response type: {type(changes)}")


def test_latest_data(client: StatCanClient):
    section("4. getDataFromVectorsAndLatestNPeriods — pull sample CPI data")
    # v41690973 = CPI All-items, Canada (confirmed working)
    test_vector = 41690973
    print(f"Pulling latest 6 periods for vector {test_vector} (CPI All-items, Canada)...")

    result = client.get_data_from_vectors_latest_n([test_vector], n=6)
    if result and isinstance(result, list):
        obj = result[0]
        points = obj.get("vectorDataPoint", [])
        print(f"  Vector ID: {obj.get('vectorId')}")
        print(f"  Product ID: {obj.get('productId')}")
        print(f"  Coordinate: {obj.get('coordinate')}")
        print(f"\n  Data points ({len(points)}):")
        for pt in points:
            ref = pt.get("refPer")
            val = pt.get("value")
            decimals = pt.get("decimals", 1)
            release = pt.get("releaseTime", "")[:10]
            print(f"    {ref}: {val} (released {release})")
    else:
        print("  No data returned.")


def test_series_info(client: StatCanClient):
    section("5. getSeriesInfoFromVector — vector metadata")
    test_vector = 41690973
    result = client.get_series_info_from_vector([test_vector])
    if result and isinstance(result, list):
        info = result[0]
        print(f"  Vector ID: {info.get('vectorId')}")
        print(f"  Table PID: {info.get('productId')}")
        print(f"  Coordinate: {info.get('coordinate')}")
        print(f"  Title: {info.get('SeriesTitleEn', info.get('seriesTitleEn', 'N/A'))}")


def test_code_sets(client: StatCanClient):
    section("6. getCodeSets — reference codes")
    codes = client.get_code_sets()
    if isinstance(codes, list):
        for cs in codes[:5]:
            if isinstance(cs, dict):
                desc = cs.get("codeSetTitleEn", cs.get("descEn", "unknown"))
                print(f"  Code set: {desc}")
    elif isinstance(codes, dict):
        for key in list(codes.keys())[:5]:
            print(f"  {key}: {type(codes[key]).__name__}")


def main():
    print("Stats Bluenoser — StatsCan WDS API Smoke Test")
    print(f"Date: {date.today()}")

    with StatCanClient() as client:
        try:
            meta = test_get_cube_metadata(client)
            find_ns_vectors(meta)
            test_changed_cubes(client)
            test_latest_data(client)
            test_series_info(client)
            test_code_sets(client)
        except StatCanError as e:
            print(f"\n*** StatsCan API Error: {e} ***", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"\n*** Unexpected error: {e} ***", file=sys.stderr)
            raise

    section("DONE")
    print("API is reachable and returning data.")
    print("Next step: python -m scripts.map_vectors 18100004")


if __name__ == "__main__":
    main()
