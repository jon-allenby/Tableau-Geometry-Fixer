# TableauGeometryFixer.spec
#
# Build with:
#   pyinstaller TableauGeometryFixer.spec
#
# Output: dist/TableauGeometryFixer.exe

import os
import sys
from pathlib import Path

# ── Locate third-party package data ─────────────────────────────────────────

import tableauhyperapi
_hyper_bin = str(Path(tableauhyperapi.__file__).parent / "bin")

import shapely
_shapely_dir = str(Path(shapely.__file__).parent)

import cffi
_cffi_dir = str(Path(cffi.__file__).parent)

# ── Analysis ─────────────────────────────────────────────────────────────────

a = Analysis(
    ["fix_geometry_gui.py"],
    pathex=[str(Path("fix_geometry_gui.py").resolve().parent)],
    binaries=[],
    datas=[
        # tableauhyperapi needs its DLL + hyperd.exe at a specific relative path
        (_hyper_bin, "tableauhyperapi/bin"),
        # shapely .pyd extension modules and any GEOS libs it carries
        (_shapely_dir, "shapely"),
        # cffi backend
        (_cffi_dir, "cffi"),
        # app icon
        ("Logo.png", "."),
    ],
    hiddenimports=[
        "fix_hyper_geometry",
        "shapely",
        "shapely.geometry",
        "shapely.geometry.polygon",
        "shapely.geometry.multipolygon",
        "shapely.validation",
        "shapely.lib",
        "cffi",
        "_cffi_backend",
        "tableauhyperapi",
        "tableauhyperapi.impl",
        "tableauhyperapi.impl.dll",
        "tableauhyperapi.impl.util",
        "tableauhyperapi.impl.hapi",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="TableauGeometryFixer",
    debug=False,
    strip=False,
    upx=False,          # UPX can break DLL loading; leave off
    console=False,      # no console window (GUI app)
    bootloader_ignore_signals=False,
    runtime_tmpdir=None,
    icon="Logo.ico",
)
