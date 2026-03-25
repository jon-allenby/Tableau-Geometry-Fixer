"""
fix_hyper_geometry.py — Fix invalid polygon geometry in Tableau .hyper files.

Repairs all TABGEOGRAPHY columns in every table across every .hyper file found
inside a .twbx (or a standalone .hyper) by:
  1. Repairing self-intersecting rings with Shapely's buffer(0)
  2. Correcting winding order to match Tableau's interior-left rule:
       outer boundary  -> counter-clockwise
       holes / cutouts -> clockwise
       first vertex == last vertex (closed ring, enforced by Shapely)

Usage:
    python fix_hyper_geometry.py <input.twbx|input.hyper> [--output <path>]

Requirements:
    pip install tableauhyperapi shapely
"""

import argparse
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import MultiPolygon
from shapely.geometry.polygon import orient
from shapely.validation import explain_validity
from tableauhyperapi import (
    Connection, CreateMode, HyperProcess, Inserter,
    SqlType, TableDefinition, TableName, Telemetry,
)

# ---------------------------------------------------------------------------
# Geometry repair
# ---------------------------------------------------------------------------

def _fix_geom(geom):
    """Recursively fix winding order and self-intersections."""
    t = geom.geom_type
    if t == "Polygon":
        if not geom.is_valid:
            geom = geom.buffer(0)
        # orient(sign=1.0) -> exterior CCW (positive area), holes CW
        return orient(geom, sign=1.0)
    if t == "MultiPolygon":
        fixed = [orient(p.buffer(0) if not p.is_valid else p, sign=1.0)
                 for p in geom.geoms]
        return MultiPolygon(fixed)
    # Points, LineStrings, GeometryCollections without polygons — no winding to fix
    return geom


def fix_wkt(wkt_str: str) -> tuple[str, bool, bool]:
    """
    Fix a WKT geometry string.

    Returns:
        (fixed_wkt, topology_was_repaired, winding_was_corrected)
    """
    geom = shapely_wkt.loads(wkt_str)
    topology_repaired = not geom.is_valid
    fixed = _fix_geom(geom)
    # Compare Shapely-serialised WKTs (same format, so a real diff means a real
    # change). This catches exterior winding, hole winding, and buffer repairs —
    # more reliable than checking is_ccw on the exterior alone.
    winding_corrected = (not topology_repaired) and (fixed.wkt != geom.wkt)
    return fixed.wkt, topology_repaired, winding_corrected


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------

@dataclass
class ColumnStats:
    name: str
    total_non_null: int = 0
    unique_shapes: int = 0
    topology_repaired: int = 0
    winding_corrected: int = 0
    still_invalid: list = field(default_factory=list)


@dataclass
class HyperFileResult:
    filename: str                  # bare filename (no directory) for display
    column_stats: list[ColumnStats]
    has_changes: bool = False      # False → file was untouched / skipped


# ---------------------------------------------------------------------------
# Core .hyper processing
# ---------------------------------------------------------------------------

def _bulk_wkt_to_geog_bytes(
    hyper: HyperProcess, src: Path, wkt_strings: list[str]
) -> dict[str, bytes]:
    """
    Convert a list of WKT strings to TABGEOGRAPHY bytes without SQL literals.

    Uses a temporary table so the WKT is sent as Inserter data (no length
    limit) rather than inlined as a SQL string literal.  Returns a dict of
    {wkt: bytes}.
    """
    if not wkt_strings:
        return {}

    tmp_table = TableDefinition(
        TableName("pg_temp", "wkt_staging"),
        [TableDefinition.Column("wkt", SqlType.text())],
    )

    result: dict[str, bytes] = {}
    with Connection(hyper.endpoint, src) as conn:
        conn.execute_command("CREATE TEMP TABLE wkt_staging (wkt TEXT)")
        with Inserter(conn, tmp_table) as ins:
            for wkt in wkt_strings:
                ins.add_row([wkt])
            ins.execute()

        with conn.execute_query(
            "SELECT wkt, CAST(wkt AS TABLEAU.TABGEOGRAPHY) FROM wkt_staging"
        ) as r:
            for wkt_val, geog_bytes in r:
                result[wkt_val] = geog_bytes

        conn.execute_command("DROP TABLE wkt_staging")

    return result


def fix_hyper_file(hyper: HyperProcess, src: Path, dst: Path) -> list[ColumnStats]:
    """
    Read src, fix all TABGEOGRAPHY columns, write to dst.
    Returns per-column statistics.
    """
    all_stats: list[ColumnStats] = []

    # ── 1. Discover schema ──────────────────────────────────────────────────
    with Connection(hyper.endpoint, src) as conn:
        table_names = [
            t
            for schema in conn.catalog.get_schema_names()
            for t in conn.catalog.get_table_names(schema)
        ]
        table_defs: dict = {}
        for tn in table_names:
            table_defs[tn] = conn.catalog.get_table_definition(tn)

    # ── 2. Read rows, collect unique WKT per geography column ───────────────
    # Structure: {table_name: {col_idx: {orig_wkt: (fixed_wkt, stats_obj)}}}
    table_fix_maps: dict = {}
    table_rows: dict = {}

    with Connection(hyper.endpoint, src) as conn:
        for tn, td in table_defs.items():
            geo_indices = [
                i for i, col in enumerate(td.columns)
                if col.type == SqlType.geography()
            ]

            if not geo_indices:
                # No geography columns — read raw and pass through unchanged
                with conn.execute_query(f"SELECT * FROM {tn}") as r:
                    table_rows[tn] = list(r)
                table_fix_maps[tn] = {}
                continue

            # Build SELECT: cast geography columns to TEXT for WKT access
            select_parts = []
            for i, col in enumerate(td.columns):
                escaped = str(col.name)
                select_parts.append(
                    f"CAST({escaped} AS TEXT)" if i in geo_indices else escaped
                )
            with conn.execute_query(
                f"SELECT {', '.join(select_parts)} FROM {tn}"
            ) as r:
                rows = list(r)
            table_rows[tn] = rows

            # Per-column fix maps and stats
            col_fix_maps: dict = {}
            for col_idx in geo_indices:
                col_name = td.columns[col_idx].name.unescaped
                stats = ColumnStats(name=f"{tn}.{col_name}")
                wkt_map: dict[str, tuple[str, bool, bool]] = {}  # orig -> (fixed, topo, wind)

                for row in rows:
                    wkt_str = row[col_idx]
                    if wkt_str is None:
                        continue
                    stats.total_non_null += 1
                    if wkt_str not in wkt_map:
                        fixed_wkt, topo, wind = fix_wkt(wkt_str)
                        fixed_geom = shapely_wkt.loads(fixed_wkt)
                        if not fixed_geom.is_valid:
                            stats.still_invalid.append(
                                (wkt_str[:80], explain_validity(fixed_geom))
                            )
                        wkt_map[wkt_str] = (fixed_wkt, topo, wind)
                        stats.unique_shapes += 1
                        if topo:
                            stats.topology_repaired += 1
                        if wind:
                            stats.winding_corrected += 1

                col_fix_maps[col_idx] = wkt_map
                all_stats.append(stats)

            table_fix_maps[tn] = col_fix_maps

    # ── 2b. Early exit if nothing actually changed ───────────────────────────
    any_changes = any(
        s.winding_corrected > 0 or s.topology_repaired > 0
        for s in all_stats
    )
    if not any_changes:
        return all_stats, False

    # ── 3. Convert all unique fixed WKTs -> TABGEOGRAPHY bytes ───────────────
    # Collect every distinct fixed WKT across all tables/columns, then convert
    # in one bulk call via a temp table (avoids SQL literal length limits).
    all_fixed_wkts: list[str] = list({
        fixed_wkt
        for col_fix_maps in table_fix_maps.values()
        for wkt_map in col_fix_maps.values()
        for _orig, (fixed_wkt, _t, _w) in wkt_map.items()
    })
    wkt_to_bytes = _bulk_wkt_to_geog_bytes(hyper, src, all_fixed_wkts)

    # ── 4. Write fixed data to dst ───────────────────────────────────────────
    with Connection(hyper.endpoint, dst, CreateMode.CREATE_AND_REPLACE) as conn:
        for tn, td in table_defs.items():
            schema = tn.schema_name
            if schema:
                conn.catalog.create_schema_if_not_exists(schema)
            # td.table_name carries the source database (file) name; rebuild
            # with the unqualified tn so it creates cleanly in the output file.
            output_td = TableDefinition(tn, td.columns)
            conn.catalog.create_table(output_td)

            col_fix_maps = table_fix_maps[tn]
            # Build per-column lookup: orig_wkt -> bytes
            col_bytes_lookup: dict[int, dict[str, bytes]] = {
                col_idx: {
                    orig: wkt_to_bytes[fixed]
                    for orig, (fixed, _t, _w) in wkt_map.items()
                }
                for col_idx, wkt_map in col_fix_maps.items()
            }

            with Inserter(conn, output_td) as inserter:
                for row in table_rows[tn]:
                    new_row = list(row)
                    for col_idx, lookup in col_bytes_lookup.items():
                        if row[col_idx] is not None:
                            new_row[col_idx] = lookup[row[col_idx]]
                    inserter.add_row(new_row)
                inserter.execute()

    return all_stats, True


# ---------------------------------------------------------------------------
# .twbx / .hyper entry points
# ---------------------------------------------------------------------------

def process_twbx(src: Path, dst: Path) -> list[HyperFileResult]:
    """Extract src.twbx, fix all .hyper files inside, repackage as dst.twbx."""
    results: list[HyperFileResult] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)

        print(f"Extracting {src.name} ...")
        with zipfile.ZipFile(src, "r") as zf:
            zf.extractall(tmp)

        hyper_files = list(tmp.rglob("*.hyper"))
        if not hyper_files:
            print("  No .hyper files found inside the .twbx — nothing to fix.")
            return results

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            for hyper_path in hyper_files:
                print(f"\nProcessing {hyper_path.relative_to(tmp)} ...")
                fixed_path = hyper_path.with_suffix(".hyper.fixed")
                stats, changed = fix_hyper_file(hyper, hyper_path, fixed_path)
                _print_stats(stats, changed)
                if changed:
                    _verify_counts(hyper, hyper_path, fixed_path)
                    fixed_path.replace(hyper_path)
                results.append(HyperFileResult(
                    filename=hyper_path.name, column_stats=stats, has_changes=changed
                ))

        if not any(r.has_changes for r in results):
            print("\nNo geometry issues found — output file not created.")
            return results

        print(f"\nRepackaging -> {dst.name} ...")
        with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmp.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp))

    print(f"\nDone. Output: {dst}")
    return results


def process_hyper(src: Path, dst: Path) -> list[HyperFileResult]:
    """Fix a standalone .hyper file."""
    print(f"Processing {src.name} ...")
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        stats, changed = fix_hyper_file(hyper, src, dst)
        _print_stats(stats, changed)
        if changed:
            _verify_counts(hyper, src, dst)
    if changed:
        print(f"\nDone. Output: {dst}")
    else:
        print("\nNo geometry issues found — output file not created.")
    return [HyperFileResult(filename=src.name, column_stats=stats, has_changes=changed)]


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------

def _print_stats(stats: list[ColumnStats], changed: bool = True) -> None:
    if not stats:
        print("  No TABGEOGRAPHY columns found.")
        return
    for s in stats:
        print(f"  Column : {s.name}")
        print(f"    Unique shapes    : {s.unique_shapes}")
        if changed:
            print(f"    Winding fixed    : {s.winding_corrected}")
            print(f"    Topology repaired: {s.topology_repaired}")
        else:
            print(f"    No changes needed.")
        if s.still_invalid:
            print(f"    *** {len(s.still_invalid)} shape(s) STILL INVALID after repair ***")
            for snippet, reason in s.still_invalid[:3]:
                print(f"      {reason}: {snippet}...")


def _verify_counts(hyper: HyperProcess, src: Path, dst: Path) -> None:
    with Connection(hyper.endpoint, src) as cs, Connection(hyper.endpoint, dst) as cd:
        for schema in cs.catalog.get_schema_names():
            for tn in cs.catalog.get_table_names(schema):
                orig = cs.execute_scalar_query(f"SELECT COUNT(*) FROM {tn}")
                fixed = cd.execute_scalar_query(f"SELECT COUNT(*) FROM {tn}")
                status = "OK" if orig == fixed else "MISMATCH"
                print(f"  Row count {tn}: {orig} -> {fixed}  [{status}]")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fix polygon winding order and self-intersections in Tableau .hyper/.twbx files."
    )
    parser.add_argument("input", type=Path, help=".twbx or .hyper file to fix")
    parser.add_argument(
        "--output", "-o", type=Path, default=None,
        help="Output path (default: <input>_fixed.<ext> alongside the input file)",
    )
    args = parser.parse_args()

    src: Path = args.input.resolve()
    if not src.exists():
        parser.error(f"File not found: {src}")

    if args.output:
        dst = args.output.resolve()
    else:
        dst = src.with_stem(src.stem + "_fixed")

    ext = src.suffix.lower()
    if ext == ".twbx":
        process_twbx(src, dst)
    elif ext == ".hyper":
        process_hyper(src, dst)
    else:
        parser.error(f"Unsupported file type '{ext}'. Expected .twbx or .hyper.")


if __name__ == "__main__":
    main()
