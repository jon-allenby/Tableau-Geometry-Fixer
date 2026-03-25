# Tableau Geometry Fixer

Fixes invalid polygon geometry in Tableau `.twbx` packaged workbooks and standalone `.hyper` extract files.

Tableau recently removed internal leniency around polygon winding order, so geometry that worked in older versions may now render incorrectly (inside-out shapes) or disappear entirely. This tool repairs all `TABGEOGRAPHY` columns across every table in every `.hyper` file automatically.

## What it fixes

| Problem | Cause | Fix applied |
|---|---|---|
| Inside-out / inverted polygons | Exterior ring wound clockwise instead of counter-clockwise | `orient()` — reverses ring to CCW |
| Holes rendered as fills | Interior ring wound counter-clockwise instead of clockwise | `orient()` — reverses ring to CW |
| Shapes disappearing entirely | Self-intersecting ring edges | `buffer(0)` topology repair |

Tableau's interior-left rule requires:
- **Outer boundary** → vertices counter-clockwise
- **Holes / cutouts** → vertices clockwise
- First and last vertex must be the same point (closed ring)
- No edges crossing each other

## Usage

### GUI (recommended)

```
python fix_geometry_gui.py
```

1. Click **Browse** and select a `.twbx` or `.hyper` file
2. Confirm or change the output path (defaults to `<name>_fixed.<ext>`)
3. Click **Fix Geometry**

The log area shows live progress colour-coded by severity. The results table shows each geography column found, how many shapes had winding corrected, how many had topology repaired, and any shapes that could not be fully repaired.

If no geometry issues are found, no output file is created.

### Command line

```
python fix_hyper_geometry.py <input.twbx|input.hyper> [--output <path>]
```

**Examples:**

```bash
# Fix a packaged workbook (output: MyWorkbook_fixed.twbx)
python fix_hyper_geometry.py MyWorkbook.twbx

# Fix a standalone extract
python fix_hyper_geometry.py extract.hyper

# Specify output path explicitly
python fix_hyper_geometry.py MyWorkbook.twbx --output MyWorkbook_v2.twbx
```

## Installation

Requires Python 3.10+.

```bash
pip install -r requirements.txt
```

## Building a standalone .exe

To distribute to users without Python installed:

```bash
pip install pyinstaller
pyinstaller TableauGeometryFixer.spec
```

The executable will be created at `dist/TableauGeometryFixer.exe`. It is self-contained — no Python or pip installation required on the target machine.

> **Note:** Windows may show a SmartScreen warning on first launch ("unrecognised app") because the executable is unsigned. Click **More info → Run anyway** to proceed. This is expected for unsigned executables.

## Known limitations

- Shapes that are both self-intersecting and cannot be repaired by `buffer(0)` are flagged as "still invalid" in the output. These are written to the output file unchanged and will remain invisible in Tableau.
- When a `.hyper` file contains a mix of valid and invalid shapes, all shapes in that file go through a WKT round-trip (Tableau binary → text → binary). This preserves coordinates to 7 decimal places but may introduce negligible floating-point differences for otherwise unchanged shapes.
- The `.exe` is built for Windows x64 only. Run the Python scripts directly on macOS or Linux.
