"""
fix_geometry_gui.py — Graphical interface for fix_hyper_geometry.py

Run:
    python fix_geometry_gui.py

Requires:
    pip install tableauhyperapi shapely
"""

import contextlib
import os
import queue
import sys
import threading
import traceback
from pathlib import Path
import tkinter as tk
from tkinter import PhotoImage, filedialog, messagebox, scrolledtext, ttk

from fix_hyper_geometry import HyperFileResult, process_hyper, process_twbx


# ---------------------------------------------------------------------------
# Stdout redirector
# ---------------------------------------------------------------------------

class _QueueWriter:
    """Sends stdout writes to a thread-safe queue as ('log', text) messages."""
    def __init__(self, q: queue.Queue):
        self._q = q

    def write(self, text: str):
        if text:
            self._q.put(("log", text))

    def flush(self):
        pass


# ---------------------------------------------------------------------------
# Log line colour classifier
# ---------------------------------------------------------------------------

def _log_tag(text: str) -> str:
    t = text.strip()
    if not t:
        return ""
    if t.startswith(("Extracting ", "Processing ", "Repackaging", "Done.")):
        return "header"
    if "No TABGEOGRAPHY" in t or t.startswith("No ") or "No changes needed" in t or "No geometry issues" in t:
        return "muted"
    if "Row count" in t and "[OK]" in t:
        return "ok"
    if "Row count" in t and "MISMATCH" in t:
        return "error"
    if "Winding fixed" in t or "Topology repaired" in t:
        return "fixed"
    if "STILL INVALID" in t or t.startswith("Repairing ") or "WARNING" in t:
        return "warn"
    return ""


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

class App(tk.Tk):
    _PAD = 8

    def __init__(self):
        super().__init__()
        self.title("Tableau Geometry Fixer")
        _base = Path(getattr(sys, "_MEIPASS", Path(__file__).parent))
        self._icon = PhotoImage(file=_base / "Logo.png")
        self.iconphoto(True, self._icon)
        self.minsize(720, 540)
        self.resizable(True, True)
        self._queue: queue.Queue = queue.Queue()
        self._output_folder: Path | None = None
        self._build_ui()

    # ── UI construction ───────────────────────────────────────────────────

    def _build_ui(self):
        P = self._PAD
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)   # log expands

        # ── File selection ────────────────────────────────────────────────
        file_frame = ttk.LabelFrame(self, text="Workbook / Extract", padding=P)
        file_frame.grid(row=0, column=0, sticky="ew", padx=P, pady=(P, 0))
        file_frame.columnconfigure(1, weight=1)

        ttk.Label(file_frame, text="Input:").grid(row=0, column=0, sticky="w")
        self.input_var = tk.StringVar()
        self.input_var.trace_add("write", self._auto_output)
        ttk.Entry(file_frame, textvariable=self.input_var).grid(
            row=0, column=1, sticky="ew", padx=(P, 4)
        )
        ttk.Button(file_frame, text="Browse...", command=self._browse_input).grid(
            row=0, column=2
        )

        ttk.Label(file_frame, text="Output:").grid(
            row=1, column=0, sticky="w", pady=(4, 0)
        )
        self.output_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.output_var).grid(
            row=1, column=1, sticky="ew", padx=(P, 4), pady=(4, 0)
        )
        ttk.Button(file_frame, text="Browse...", command=self._browse_output).grid(
            row=1, column=2, pady=(4, 0)
        )

        # ── Run button ────────────────────────────────────────────────────
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=1, column=0, pady=P)
        self.run_btn = ttk.Button(
            btn_frame, text="Fix Geometry", command=self._run, width=22
        )
        self.run_btn.pack()

        # ── Log area ──────────────────────────────────────────────────────
        log_frame = ttk.LabelFrame(self, text="Log", padding=P)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=P, pady=(0, 0))
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.log = scrolledtext.ScrolledText(
            log_frame,
            state="disabled",
            height=12,
            font=("Courier New", 9),
            wrap="word",
            bg="#1e1e1e",
            fg="#d4d4d4",
            insertbackground="white",
            relief="flat",
        )
        self.log.grid(row=0, column=0, sticky="nsew")

        # VS Code–inspired colour tags
        self.log.tag_config("header", foreground="#569cd6",
                            font=("Courier New", 9, "bold"))
        self.log.tag_config("ok",     foreground="#4ec9b0")
        self.log.tag_config("fixed",  foreground="#b5cea8")
        self.log.tag_config("warn",   foreground="#ce9178")
        self.log.tag_config("error",  foreground="#f44747")
        self.log.tag_config("muted",  foreground="#6a9955")

        # ── Results table ─────────────────────────────────────────────────
        res_frame = ttk.LabelFrame(self, text="Results", padding=P)
        res_frame.grid(row=3, column=0, sticky="ew", padx=P, pady=(P, 0))
        res_frame.columnconfigure(0, weight=1)

        cols = ("file", "column", "shapes", "winding", "topology", "invalid")
        self.tree = ttk.Treeview(
            res_frame, columns=cols, show="headings", height=5
        )
        col_cfg = {
            "file":     ("Extract File",       170, "w"),
            "column":   ("Column",             155, "w"),
            "shapes":   ("Unique Shapes",       95, "center"),
            "winding":  ("Winding Fixed",       95, "center"),
            "topology": ("Topology Repaired",  120, "center"),
            "invalid":  ("Still Invalid",       85, "center"),
        }
        for col, (label, width, anchor) in col_cfg.items():
            self.tree.heading(col, text=label)
            self.tree.column(col, width=width, anchor=anchor, stretch=(col in ("file", "column")))

        vsb = ttk.Scrollbar(res_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky="ew")
        vsb.grid(row=0, column=1, sticky="ns")

        self.tree.tag_configure("warn_row", foreground="#ce9178")
        self.tree.tag_configure("no_geo",   foreground="#808080")
        self.tree.tag_configure("ok_row",   foreground="#4ec9b0")

        # ── Status bar ────────────────────────────────────────────────────
        status_frame = ttk.Frame(self, padding=(P, 4, P, P))
        status_frame.grid(row=4, column=0, sticky="ew")
        status_frame.columnconfigure(0, weight=1)

        self.status_var = tk.StringVar(value="Ready — choose a .twbx or .hyper file above.")
        ttk.Label(status_frame, textvariable=self.status_var, anchor="w").grid(
            row=0, column=0, sticky="ew"
        )
        self.open_btn = ttk.Button(
            status_frame,
            text="Open Output Folder",
            command=self._open_folder,
            state="disabled",
        )
        self.open_btn.grid(row=0, column=1, padx=(P, 0))

    # ── File browsing ─────────────────────────────────────────────────────

    def _browse_input(self):
        path = filedialog.askopenfilename(
            title="Select Tableau workbook or extract",
            filetypes=[
                ("Tableau files",       "*.twbx *.hyper"),
                ("Packaged workbook",   "*.twbx"),
                ("Hyper extract",       "*.hyper"),
                ("All files",           "*.*"),
            ],
        )
        if path:
            self.input_var.set(path)

    def _browse_output(self):
        src = self.input_var.get().strip()
        ext = Path(src).suffix if src else ".twbx"
        path = filedialog.asksaveasfilename(
            title="Save fixed file as",
            defaultextension=ext,
            filetypes=[
                ("Tableau files", f"*{ext}"),
                ("All files",     "*.*"),
            ],
        )
        if path:
            self.output_var.set(path)

    def _auto_output(self, *_):
        src = self.input_var.get().strip()
        if src:
            p = Path(src)
            self.output_var.set(str(p.with_stem(p.stem + "_fixed")))

    # ── Fix worker ────────────────────────────────────────────────────────

    def _run(self):
        src_str = self.input_var.get().strip()
        dst_str = self.output_var.get().strip()

        if not src_str:
            self.status_var.set("Please choose an input file.")
            return
        src = Path(src_str)
        if not src.exists():
            self.status_var.set(f"File not found: {src}")
            return
        if not dst_str:
            self.status_var.set("Please set an output path.")
            return
        dst = Path(dst_str)

        if dst.exists():
            if not tk.messagebox.askyesno(
                "Overwrite?",
                f'"{dst.name}" already exists.\n\nOverwrite it?',
                icon="warning",
            ):
                return

        # Reset UI
        self._log_clear()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.run_btn.config(state="disabled")
        self.open_btn.config(state="disabled")
        self.status_var.set("Running...")
        self._output_folder = None
        self._queue = queue.Queue()

        def worker():
            writer = _QueueWriter(self._queue)
            try:
                with contextlib.redirect_stdout(writer):
                    if src.suffix.lower() == ".twbx":
                        results = process_twbx(src, dst)
                    else:
                        results = process_hyper(src, dst)
                self._queue.put(("done", (dst, results)))
            except Exception:
                self._queue.put(("error", traceback.format_exc()))

        threading.Thread(target=worker, daemon=True).start()
        self.after(50, self._poll)

    def _poll(self):
        try:
            while True:
                kind, data = self._queue.get_nowait()
                if kind == "log":
                    self._log_append(data)
                elif kind == "done":
                    dst, results = data
                    self._populate_results(results)
                    any_changes = any(r.has_changes for r in results) if results else False
                    if any_changes:
                        self._output_folder = dst.parent
                        self.open_btn.config(state="normal")
                        self.status_var.set(f"Done  |  {dst}")
                    else:
                        self.status_var.set("No geometry issues found — no output file created.")
                    self.run_btn.config(state="normal")
                    return
                elif kind == "error":
                    self._log_append("\n" + data, "error")
                    self.status_var.set("Failed — see log for details.")
                    self.run_btn.config(state="normal")
                    return
        except queue.Empty:
            pass
        self.after(50, self._poll)

    # ── Results table population ──────────────────────────────────────────

    def _populate_results(self, results: list[HyperFileResult]):
        if not results:
            return
        for file_result in results:
            fname = file_result.filename
            stats = file_result.column_stats

            if not stats:
                self.tree.insert(
                    "", "end",
                    values=(fname, "no geography columns", "", "", "", ""),
                    tags=("no_geo",),
                )
                continue

            for i, s in enumerate(stats):
                # Strip schema/table prefix from column name for readability
                col_display = s.name.split(".")[-1]
                invalid_count = len(s.still_invalid)
                if not file_result.has_changes:
                    tag, winding, topology, invalid = "no_geo", "—", "—", ""
                else:
                    tag = "warn_row" if invalid_count else "ok_row"
                    winding  = s.winding_corrected
                    topology = s.topology_repaired
                    invalid  = invalid_count if invalid_count else ""
                self.tree.insert(
                    "", "end",
                    values=(
                        fname if i == 0 else "",
                        col_display,
                        s.unique_shapes,
                        winding,
                        topology,
                        invalid,
                    ),
                    tags=(tag,),
                )

    # ── Log helpers ───────────────────────────────────────────────────────

    def _log_clear(self):
        self.log.config(state="normal")
        self.log.delete("1.0", tk.END)
        self.log.config(state="disabled")

    def _log_append(self, text: str, force_tag: str | None = None):
        self.log.config(state="normal")
        tag = force_tag if force_tag is not None else _log_tag(text)
        self.log.insert(tk.END, text, tag)
        self.log.see(tk.END)
        self.log.config(state="disabled")

    # ── Open folder ───────────────────────────────────────────────────────

    def _open_folder(self):
        if self._output_folder and self._output_folder.exists():
            if sys.platform == "win32":
                os.startfile(self._output_folder)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", str(self._output_folder)])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", str(self._output_folder)])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app = App()
    app.mainloop()
