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

import argparse #used for writing CLIs and taking in arguments from the user.
import tempfile #used for generating temporary files.
import zipfile #used for creating and managing zip files.
from dataclasses import dataclass, field 
    # dataclass lets you build custom classes (basically object blueprints).
    # field usually lets you set a default value.
from pathlib import Path #used for handling file paths.

#shapely is the package which contains all our spatial functionality
from shapely import wkt as shapely_wkt #wkt = "well known text", a text markup language for geometry.
from shapely.geometry import MultiPolygon #a collection of polygons - enforces zero overlap.
from shapely.geometry.polygon import orient #converts a polygon into a new one with correct orientation (outer rings counter-clockwise and inner rings clockwise)
from shapely.validation import explain_validity #confirms whether an object is valid or not (orientation, overlap, etc)
from tableauhyperapi import (
    Connection, CreateMode, HyperProcess, Inserter,
    SqlType, TableDefinition, TableName, Telemetry,
) #all the basic Tableau API functions.

# ---------------------------------------------------------------------------
# Geometry repair
# ---------------------------------------------------------------------------

def _fix_geom(geom):
    """Recursively fix winding order and self-intersections."""
    t = geom.geom_type
    if t == "Polygon":
        if not geom.is_valid: #returns false in cases such as self-intersection.
            geom = geom.buffer(0) #buffer(0) is used to fix the above. The result covers the same area as the original polygon but buffer(0) rebuilds it with valid geometry.
        # orient(sign=1.0) -> exterior CCW (positive area), holes CW
        return orient(geom, sign=1.0) #orient fixes winding order, i.e. the order of the vertices. sign 1.0 means that the exterior rings will be counter clockwise and any interior will be clockwise but I'm not sure what any other sign value would actually do here.
    if t == "MultiPolygon":
        fixed = [orient(p.buffer(0) if not p.is_valid else p, sign=1.0)
                 for p in geom.geoms] #same as above, but done on every polygon within the multipolygon.
        return MultiPolygon(fixed) #the above creates a list of polygons, so this makes it a multipolygon again.
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
#a custom dataclass containing info about the state of the data source and any changes that were made to it

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
    #the main function, does the actual fixing.
    
    all_stats: list[ColumnStats] = []
    #prepares an empty list variable which will contain ColumnStats elements

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
    #discovers the schema, table names, and table definitions

    # ── 2. Read rows, collect unique WKT per geography column ───────────────
    # Structure: {table_name: {col_idx: {orig_wkt: (fixed_wkt, stats_obj)}}}
    table_fix_maps: dict = {}
    table_rows: dict = {}
    #set up some dict objects (key/value pairs)

    with Connection(hyper.endpoint, src) as conn:
        #the below is one big for loop across all tables
        for tn, td in table_defs.items():
            geo_indices = [
                i for i, col in enumerate(td.columns)
                if col.type == SqlType.geography()
            ]
            #returns all columns with a geography type.

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
                #creates a list of all geography column names.
                #this is where the columns names are cast to WKT format.
            with conn.execute_query(
                f"SELECT {', '.join(select_parts)} FROM {tn}"
            ) as r:
                rows = list(r)
            table_rows[tn] = rows
            #"{', '.join(select_parts)}" joins all column names in select_parts, separated by commas. Essentially creates a nice SELECT statement to get the relevant columns and spits them into a list.

            # Per-column fix maps and stats
            col_fix_maps: dict = {}
            for col_idx in geo_indices:
                col_name = td.columns[col_idx].name.unescaped #.unescaped gets the raw name, no quoting.
                stats = ColumnStats(name=f"{tn}.{col_name}")
                wkt_map: dict[str, tuple[str, bool, bool]] = {}  # orig -> (fixed, topo, wind)
                #the section above basically sets up the variables we'll be using later. 

                for row in rows:
                    wkt_str = row[col_idx]
                    if wkt_str is None:
                        continue #if null, move onto the next loop
                    stats.total_non_null += 1 #otherwise, add to the non-null count
                    if wkt_str not in wkt_map: #as the same object might be in multiple rows in the data this check saves processing the same shape twice.
                        fixed_wkt, topo, wind = fix_wkt(wkt_str) #fix the shape
                        fixed_geom = shapely_wkt.loads(fixed_wkt) #loads the shape from the string form
                        if not fixed_geom.is_valid:
                            stats.still_invalid.append(
                                (wkt_str[:80], explain_validity(fixed_geom))  #:80 just takes the first 80 characters of the string
                            ) #if the shape still isn't valid, append info about why to the results
                        wkt_map[wkt_str] = (fixed_wkt, topo, wind)
                        stats.unique_shapes += 1
                        if topo:
                            stats.topology_repaired += 1 #add to fixed topology count
                        if wind:
                            stats.winding_corrected += 1 #add to fixed winding count

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
        #creates a temp directory and grabs the path of it.
        #with TemporaryDirectory() the temp directory automatically gets deleted once the context of the function gets exited

        print(f"Extracting {src.name} ...")
        with zipfile.ZipFile(src, "r") as zf:
            zf.extractall(tmp)
            #extracts all files from the .twbx
            #ZipFile(src, "r") - the "r" puts the mode into "read an existing file"

        hyper_files = list(tmp.rglob("*.hyper"))
        if not hyper_files:
            print("  No .hyper files found inside the .twbx — nothing to fix.")
            return results
            #rglob is a recursive search for all files that match the string
        

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            for hyper_path in hyper_files:
                print(f"\nProcessing {hyper_path.relative_to(tmp)} ...")
                fixed_path = hyper_path.with_suffix(".hyper.fixed") #set up the path for the new fixed datasource.
                stats, changed = fix_hyper_file(hyper, hyper_path, fixed_path) #fun fix_hyper_file
                _print_stats(stats, changed) #print results
                if changed:
                    _verify_counts(hyper, hyper_path, fixed_path) #verify row counts
                    fixed_path.replace(hyper_path) #rename the .fixed path to the original path
                results.append(HyperFileResult(
                    filename=hyper_path.name, column_stats=stats, has_changes=changed
                ))
                #output the result and move to the next hyper file if one exists

        if not any(r.has_changes for r in results):
            print("\nNo geometry issues found — output file not created.")
            return results
            #if no hyper files were changed, print this.

        print(f"\nRepackaging -> {dst.name} ...")
        with zipfile.ZipFile(dst, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmp.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp))
            #repackages all hyper files back into a twbx.

    print(f"\nDone. Output: {dst}")
    return results


def process_hyper(src: Path, dst: Path) -> list[HyperFileResult]:
    """Fix a standalone .hyper file."""
    #takes in the provided hyper and the desired destination.
    #"-> list[HyperFileResult]" defines the return value.
    #HyperFileResult is one of the dataclasses we defined.
    print(f"Processing {src.name} ...")
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        stats, changed = fix_hyper_file(hyper, src, dst)
        _print_stats(stats, changed)
        if changed:
            _verify_counts(hyper, src, dst)
    #spawns a hyper process.
    #runs the fix_hyper_file process
    #note: python lets you unpack multiple variables in one go, hence the "stats, changed ="
    #calls the _print_stats() function to report the results in the GUI.
    #checks if anything changed in the file in terms of row counts using the _verify_counts() function
    if changed:
        print(f"\nDone. Output: {dst}")
    else:
        print("\nNo geometry issues found — output file not created.")
    #reports on if output was produced or not
    return [HyperFileResult(filename=src.name, column_stats=stats, has_changes=changed)]
    #the output here only gets used by the GUI to fill the results table


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
#used for printing the stats from ColumnStats in the GUI.
#changed: bool = True means that if no 'changed' value is supplied it defaults to 'true'.


def _verify_counts(hyper: HyperProcess, src: Path, dst: Path) -> None:
    with Connection(hyper.endpoint, src) as cs, Connection(hyper.endpoint, dst) as cd:
        for schema in cs.catalog.get_schema_names():
            for tn in cs.catalog.get_table_names(schema):
                orig = cs.execute_scalar_query(f"SELECT COUNT(*) FROM {tn}")
                fixed = cd.execute_scalar_query(f"SELECT COUNT(*) FROM {tn}")
                status = "OK" if orig == fixed else "MISMATCH"
                print(f"  Row count {tn}: {orig} -> {fixed}  [{status}]")
#verifies that the row counts match before and after fixing any geometry.
#gets the data source(s) schema(s) and their tables and runs a row count on the before and after.


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
    
    #main() is just the main entry point.
    #this created an argumentparse object, then defines two arguments: the input file and the output location.
    #it then parses the users input into the two args, i.e. args.input & args.output.
    #the -- in --output implies it's an optional parameter.

    src: Path = args.input.resolve()
    if not src.exists():
        parser.error(f"File not found: {src}") 
    #this converts the supplied filepath into an absolute one and checks if it exists.

    if args.output:
        dst = args.output.resolve()
    else:
        dst = src.with_stem(src.stem + "_fixed")
    #this does the same conversion as above, but if it doesn't exist it just appends _fixed to the input path.

    ext = src.suffix.lower()
    if ext == ".twbx":
        process_twbx(src, dst)
    elif ext == ".hyper":
        process_hyper(src, dst)
    else:
        parser.error(f"Unsupported file type '{ext}'. Expected .twbx or .hyper.")
    #checks the extension of the selected file and calls the appropriate function for it.


if __name__ == "__main__":
    main()
