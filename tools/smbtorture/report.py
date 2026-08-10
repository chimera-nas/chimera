#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Render one smbtorture matrix run as a single self-contained HTML file.

   Navigation is backends -> suites -> failing tests -> full log, built from
   the `results.txt` that tools/smbtorture/regen.sh writes plus the per-cell
   `smbt-fail.<backend>.<subtest>.log` files beside it.

   Every failure is classified against the checked-in baseline in
   src/server/smb/tests/CMakeLists.txt: a FAIL inside a backend's gated set is
   a regression, a FAIL outside it is a known gap.  The baseline path is
   defaulted, and the report stamps which revision it was judged against --
   a results tarball and the working tree's allowlists can drift, and an
   unlabelled report would silently report phantom regressions.
"""

import argparse, collections, dataclasses, html, os, subprocess, sys

import regen


@dataclasses.dataclass
class Cell:
    backend: str
    subtest: str
    suite: str
    status: str
    classification: str


@dataclasses.dataclass
class Suite:
    name: str
    cells: list


@dataclasses.dataclass
class Backend:
    name: str
    measured: int
    passing: int
    failing: int
    gated: int
    regressions: int
    suites: list


@dataclasses.dataclass
class Report:
    backends: list
    measured: int
    passing: int
    failing: int
    regressions: int
    classified: bool


def load_baseline(cmake_path):
    """backend -> gated subtest set.  {} when the baseline is unusable."""
    try:
        with open(cmake_path, encoding="utf-8") as f:
            txt = f.read()
    except OSError:
        return {}
    lists = regen.parse_cmake_lists(txt)
    if "SUITES" not in lists:
        return {}
    disabled = set(regen.split_entries(lists.get("DISABLED_SUBTESTS", []))[0])
    out = {}
    for backend in regen.BACKENDS:
        raw = lists.get(f"ENABLED_{backend}")
        if raw is None:
            continue
        out[backend] = set(regen.split_entries(raw)[0]) - disabled
    return out


def suite_of(subtest):
    """regen.parent_of, tolerant of a malformed single-component name."""
    return regen.parent_of(subtest) if subtest.count(".") >= 1 else subtest


def build_model(results_path, baseline):
    res = regen.parse_results([results_path])
    if not res:
        raise ValueError(f"no valid result cells in {results_path}")

    unknown = sorted({b for (b, _s) in res} - set(regen.BACKENDS))
    if unknown:
        print(f"warning: ignoring unknown backend(s) in results: "
              f"{', '.join(unknown)}", file=sys.stderr)

    backends = []
    for name in regen.BACKENDS:
        cells = [(s, st[-1]) for (b, s), st in res.items() if b == name]
        if not cells:
            continue
        gated = baseline.get(name)
        by_suite = collections.defaultdict(list)
        failing = regressions = 0
        for subtest, status in cells:
            if status == "PASS":
                continue
            failing += 1
            if gated is None:
                classification = "unclassified"
            elif subtest in gated:
                classification = "regression"
                regressions += 1
            else:
                classification = "known-gap"
            suite = suite_of(subtest)
            by_suite[suite].append(
                Cell(name, subtest, suite, status, classification))

        suites = [Suite(k, sorted(
            v, key=lambda c: (c.classification != "regression", c.subtest)))
                  for k, v in by_suite.items()]
        suites.sort(key=lambda s: (
            -sum(1 for c in s.cells if c.classification == "regression"),
            -len(s.cells), s.name))

        backends.append(Backend(
            name=name,
            measured=len(cells),
            passing=len(cells) - failing,
            failing=failing,
            gated=len(gated) if gated is not None else 0,
            regressions=regressions,
            suites=suites))

    return Report(
        backends=backends,
        measured=sum(b.measured for b in backends),
        passing=sum(b.passing for b in backends),
        failing=sum(b.failing for b in backends),
        regressions=sum(b.regressions for b in backends),
        classified=bool(baseline))


@dataclasses.dataclass
class LogView:
    missing: bool
    headline: str
    torture: str
    server: str


def extract_headline(lines):
    """Best one-line summary of why a cell failed, or None.

       Preference order matches how much the line actually narrows the cause:
       the assertion under a `failure:` banner beats a bare panic line, which
       beats the exit-code line.  Purely additive -- the caller still renders
       every line of the log.
    """
    for i, line in enumerate(lines):
        if line.startswith(("failure:", "error:")):
            for nxt in lines[i + 1:]:
                if nxt.strip() and nxt.strip() != "]":
                    return nxt.strip()
            return line.strip()
    for line in lines:
        if line.startswith(("INTERNAL ERROR:", "PANIC (")):
            return line.strip()
    for line in lines:
        if line.startswith("smbtorture: FAILED"):
            return line.strip()
    return None


def load_log(results_path, backend, subtest):
    path = regen.log_path(results_path, backend, subtest)
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()
    except OSError:
        return LogView(missing=True, headline="", torture="", server="")
    server = [l for l in lines if l.startswith("time=")]
    torture = [l for l in lines if not l.startswith("time=")]
    return LogView(missing=False,
                   headline=extract_headline(torture) or "",
                   torture="\n".join(torture),
                   server="\n".join(server))


def load_all_logs(results_path, model):
    return {(c.backend, c.subtest): load_log(results_path, c.backend, c.subtest)
            for b in model.backends for s in b.suites for c in s.cells}


CSS = """
:root { color-scheme: light dark; }
body { font: 14px/1.5 system-ui, sans-serif; margin: 2rem auto; max-width: 70rem;
       padding: 0 1rem; background: #fff; color: #111; }
h1 { font-size: 1.4rem; margin-bottom: .25rem; }
.prov { color: #666; font-size: .85rem; margin-bottom: 1.5rem; }
.banner { background: #fff3cd; border: 1px solid #e0c86b; padding: .6rem .8rem;
          border-radius: 4px; margin-bottom: 1.5rem; }
details { border-top: 1px solid #ddd; }
summary { cursor: pointer; padding: .4rem .2rem; }
summary:hover { background: rgba(127,127,127,.08); }
.b > summary { font-weight: 600; font-size: 1.05rem; }
.s { margin-left: 1.4rem; }
.t { margin-left: 2.8rem; }
.n { color: #666; font-weight: 400; }
.headline { color: #666; font-family: ui-monospace, monospace; font-size: .82rem; }
.tag { font-size: .7rem; padding: .05rem .4rem; border-radius: 3px;
       border: 1px solid currentColor; margin-right: .4rem; }
.status { font-size: .7rem; padding: .05rem .4rem; border-radius: 3px;
          border: 1px solid currentColor; margin-right: .4rem; color: #666; }
.regression { color: #b00020; font-weight: 600; }
.known-gap { color: #777; }
.unclassified { color: #777; }
.missing { color: #b00020; font-style: italic; }
pre { background: rgba(127,127,127,.10); padding: .7rem; overflow-x: auto;
      font-size: .8rem; border-radius: 4px; }
@media (prefers-color-scheme: dark) {
  body { background: #16181c; color: #e6e6e6; }
  details { border-top-color: #333; }
  .prov, .n, .headline, .known-gap, .unclassified, .status { color: #9aa0a6; }
  .regression, .missing { color: #ff6b6b; }
  .banner { background: #3a3320; border-color: #6b5c2b; }
}
"""


def esc(s):
    return html.escape(s, quote=True)


def _render_cell(cell, log):
    out = []
    tag = f'<span class="tag {cell.classification}">{esc(cell.classification)}</span>'
    status = f'<span class="status">{esc(cell.status)}</span>'
    if log.missing:
        head = '<span class="missing">log missing</span>'
    else:
        head = f'<span class="headline">{esc(log.headline)}</span>'
    out.append('<details class="t">')
    out.append(f"<summary>{tag}{status}<code>{esc(cell.subtest)}</code> {head}</summary>")
    if log.missing:
        out.append("<p class=\"missing\">No log file was written for this cell.</p>")
    else:
        if log.headline:
            out.append(f"<pre>{esc(log.headline)}</pre>")
        out.append(f"<pre>{esc(log.torture)}</pre>")
        if log.server:
            out.append("<details><summary>chimera server log "
                       f'<span class="n">({len(log.server.splitlines())} lines)'
                       "</span></summary>")
            out.append(f"<pre>{esc(log.server)}</pre>")
            out.append("</details>")
    out.append("</details>")
    return out


def render(model, logs, provenance):
    out = ["<!doctype html>", "<html lang=\"en\"><head>",
           "<meta charset=\"utf-8\">",
           "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",
           "<title>smbtorture matrix report</title>",
           f"<style>{CSS}</style>", "</head><body>"]

    out.append("<h1>smbtorture matrix report</h1>")
    out.append(f'<p class="prov">{esc(provenance)}</p>')
    if not model.classified:
        out.append('<p class="banner">No baseline was available, so failures are '
                   "<strong>unclassified</strong> &mdash; this report cannot "
                   "distinguish a regression from a known gap.</p>")

    reg = (f'<span class="regression">{model.regressions} regressions</span>'
           if model.regressions else "0 regressions")
    out.append(f"<p>{model.passing}/{model.measured} passing &middot; "
               f"{model.failing} failing &middot; {reg}</p>")

    for backend in model.backends:
        breg = (f'<span class="regression">{backend.regressions} regressions</span>'
                if backend.regressions else f'<span class="n">0 regressions</span>')
        out.append('<details class="b">')
        out.append(f"<summary>{esc(backend.name)} "
                   f'<span class="n">&mdash; {backend.failing} failing, '
                   f"{backend.passing}/{backend.measured} passing, "
                   f"{backend.gated} gated</span> &middot; {breg}</summary>")
        for suite in backend.suites:
            out.append('<details class="s">')
            out.append(f"<summary><code>{esc(suite.name)}</code> "
                       f'<span class="n">{len(suite.cells)}</span></summary>')
            for cell in suite.cells:
                out.extend(_render_cell(cell, logs[(cell.backend, cell.subtest)]))
            out.append("</details>")
        out.append("</details>")

    out.append("</body></html>")
    return "\n".join(out)


def default_cmake_path():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.normpath(os.path.join(
        here, "..", "..", "src", "server", "smb", "tests", "CMakeLists.txt"))
    return path if os.path.exists(path) else None


def git_provenance(cmake_path):
    """`<short-sha>`, `<short-sha> (dirty)`, or `<short-sha> (dirty unknown)`
       for the baseline, best effort.

       Resolves `cmake_path` to an absolute path once and uses that same
       absolute form both for `-C` and as the pathspec: `git -C <dir>`
       resolves a *relative* pathspec against `<dir>`, not against the
       caller's cwd, so passing the original (possibly relative) string
       through unchanged can point `git status` at a nonexistent path.  Git
       then exits 0 with empty stdout and only a warning on stderr -- which
       silently reads as "clean" unless that stderr/exit status is checked.
    """
    abs_cmake = os.path.abspath(cmake_path)
    repo = os.path.dirname(abs_cmake)
    try:
        head = subprocess.run(["git", "-C", repo, "rev-parse", "--short", "HEAD"],
                              capture_output=True, text=True, timeout=5)
        if head.returncode != 0:
            return "unknown revision"
        rev = head.stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", repo, "status", "--porcelain", "--", abs_cmake],
            capture_output=True, text=True, timeout=5)
        if dirty.returncode != 0 or dirty.stderr.strip():
            return f"{rev} (dirty unknown)"
        return rev + (" (dirty)" if dirty.stdout.strip() else "")
    except (OSError, subprocess.SubprocessError):
        return "unknown revision"


def format_provenance(cmake_path, results_path):
    src = f"results: {os.path.abspath(results_path)}"
    if cmake_path is None:
        return f"{src} · baseline: none"
    abs_cmake = os.path.abspath(cmake_path)
    return f"{src} · baseline: {abs_cmake} @ {git_provenance(cmake_path)}"


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Render an smbtorture matrix run as one self-contained "
                    "HTML file.")
    ap.add_argument("--results", required=True,
                    help="results.txt as written by tools/smbtorture/regen.sh")
    ap.add_argument("--output", required=True, help="HTML file to write")
    ap.add_argument("--cmake",
                    help="baseline CMakeLists.txt (default: the checked-in "
                         "src/server/smb/tests/CMakeLists.txt beside this script)")
    ap.add_argument("--no-baseline", action="store_true",
                    help="skip classification even if a baseline is found")
    args = ap.parse_args(argv)

    cmake = None if args.no_baseline else (args.cmake or default_cmake_path())
    if cmake:
        try:
            baseline = load_baseline(cmake)
        except (OSError, ValueError) as e:
            print(f"warning: could not parse baseline {cmake} ({e}); "
                  "failures will be unclassified", file=sys.stderr)
            baseline, cmake = {}, None
    else:
        baseline = {}
    if cmake and not baseline:
        print(f"warning: no usable baseline in {cmake}; failures will be "
              "unclassified", file=sys.stderr)
        cmake = None

    try:
        model = build_model(args.results, baseline)
    except (OSError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    logs = load_all_logs(args.results, model)
    page = render(model, logs, format_provenance(cmake, args.results))
    try:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(page)
    except (OSError, ValueError) as e:
        print(f"error: could not write {args.output}: {e}", file=sys.stderr)
        if os.path.exists(args.output):
            try:
                os.unlink(args.output)
            except OSError:
                pass
        return 1

    print(f"{args.output}: {model.failing} failing across "
          f"{len(model.backends)} backends, {model.regressions} regressions")
    return 0


if __name__ == "__main__":
    sys.exit(main())
