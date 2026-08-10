# SPDX-FileCopyrightText: 2024-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
"""Unit tests for tools/smbtorture/report.py."""

import contextlib, io, os, re, shutil, subprocess, sys, tempfile, unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

import regen, report

FIX = os.path.join(HERE, "fixtures")
RESULTS = os.path.join(FIX, "results.txt")
CMAKE = os.path.join(FIX, "CMakeLists.txt")


class TestModel(unittest.TestCase):

    def test_load_baseline_subtracts_disabled(self):
        base = report.load_baseline(CMAKE)
        self.assertEqual(base["memfs"], {"smb2.acls.CREATOR", "smb2.acls.DENY1"})
        self.assertNotIn("smb2.setinfo", base["linux"])
        self.assertIn("smb2.charset.Testing wide-a", base["linux"])

    def test_load_baseline_missing_file_is_empty(self):
        self.assertEqual(report.load_baseline(os.path.join(FIX, "nope.txt")), {})

    def test_backends_use_canonical_order_and_skip_unmeasured(self):
        model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        self.assertEqual([b.name for b in model.backends], ["memfs", "linux"])

    def test_classification(self):
        model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        got = {(c.backend, c.subtest): c.classification
               for b in model.backends for s in b.suites for c in s.cells}
        self.assertEqual(got[("memfs", "smb2.acls.DENY1")], "regression")
        self.assertEqual(got[("linux", "smb2.acls.DENY1")], "known-gap")
        self.assertEqual(got[("linux", "smb2.setinfo")], "known-gap")
        self.assertEqual(model.regressions, 1)

    def test_unclassified_without_baseline(self):
        model = report.build_model(RESULTS, {})
        cls = {c.classification for b in model.backends for s in b.suites for c in s.cells}
        self.assertEqual(cls, {"unclassified"})
        self.assertFalse(model.classified)

    def test_counts(self):
        model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        memfs = model.backends[0]
        self.assertEqual((memfs.measured, memfs.passing, memfs.failing), (3, 1, 2))
        self.assertEqual((model.measured, model.passing, model.failing), (8, 3, 5))

    def test_regressions_sort_first_within_a_suite(self):
        """Suites already sort regression-heavy ones to the top of a
           backend; this pins the same rule one level down, inside a
           suite: a regression cell leads even when its name loses
           alphabetically, and non-regressions stay alphabetical among
           themselves.
        """
        with tempfile.TemporaryDirectory() as tmp:
            results = os.path.join(tmp, "results.txt")
            with open(results, "w") as f:
                f.write("FAIL|memfs|smb2.foo.alpha\n")
                f.write("FAIL|memfs|smb2.foo.beta\n")
                f.write("FAIL|memfs|smb2.foo.zeta\n")
            model = report.build_model(results, {"memfs": {"smb2.foo.zeta"}})
        suite = model.backends[0].suites[0]
        self.assertEqual([c.subtest for c in suite.cells],
                         ["smb2.foo.zeta", "smb2.foo.alpha", "smb2.foo.beta"])

    def test_suite_grouping_and_ordering(self):
        model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        linux = model.backends[1]
        self.assertEqual([s.name for s in linux.suites],
                         ["smb2.acls", "smb2.dir", "smb2.setinfo"])

    def test_empty_results_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            empty = os.path.join(tmp, "empty.txt")
            with open(empty, "w") as f:
                f.write("garbage\n")
            with self.assertRaises(ValueError):
                report.build_model(empty, {})

    def test_unknown_backend_is_dropped_with_a_warning(self):
        with tempfile.TemporaryDirectory() as tmp:
            results = os.path.join(tmp, "results.txt")
            with open(results, "w") as f:
                f.write("PASS|memfs|smb2.acls.CREATOR\n")
                f.write("FAIL|nonexistent_backend|smb2.acls.CREATOR\n")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                model = report.build_model(results, {})
        self.assertEqual([b.name for b in model.backends], ["memfs"])
        self.assertIn("nonexistent_backend", stderr.getvalue())


class TestLogs(unittest.TestCase):

    def test_headline_prefers_line_after_failure(self):
        lv = report.load_log(RESULTS, "memfs", "smb2.acls.DENY1")
        self.assertFalse(lv.missing)
        self.assertEqual(
            lv.headline,
            "source4/torture/smb2/acls.c:1234: status was "
            "NT_STATUS_ACCESS_DENIED, expected NT_STATUS_OK")

    def test_headline_falls_back_to_internal_error(self):
        lv = report.load_log(RESULTS, "linux", "smb2.dir.large-files")
        self.assertTrue(lv.headline.startswith("INTERNAL ERROR: Signal 11"))

    def assert_partition_is_exhaustive_and_exclusive(self, backend, subtest):
        """The real completeness proof: read the fixture's own raw bytes and
           show lv.server + lv.torture reconstitute it exactly -- as
           multisets, so a dropped, duplicated, or misrouted line fails this
           even though it wouldn't fail a handful of substring checks.
        """
        lv = report.load_log(RESULTS, backend, subtest)
        path = regen.log_path(RESULTS, backend, subtest)
        with open(path) as f:
            raw_lines = f.read().splitlines()
        server_lines = lv.server.splitlines()
        torture_lines = lv.torture.splitlines()
        # exhaustive: the two buckets, as a multiset, equal the raw file
        self.assertEqual(sorted(server_lines + torture_lines), sorted(raw_lines))
        # mutually exclusive: no line is misrouted between the two buckets
        self.assertTrue(all(l.startswith("time=") for l in server_lines))
        self.assertTrue(all(not l.startswith("time=") for l in torture_lines))
        return lv

    def test_server_and_torture_are_split_and_complete(self):
        lv = self.assert_partition_is_exhaustive_and_exclusive(
            "memfs", "smb2.acls.DENY1")
        self.assertEqual(len(lv.server.splitlines()), 2)
        self.assertNotIn("time=", lv.torture)
        self.assertIn("Backend: memfs", lv.torture)
        self.assertIn("Shutting down", lv.server)
        self.assertIn("]", lv.torture)
        # second fixture: torture content dominates, server is a single line
        self.assert_partition_is_exhaustive_and_exclusive(
            "linux", "smb2.dir.large-files")

    def test_backtrace_survives_into_torture_text(self):
        lv = report.load_log(RESULTS, "linux", "smb2.dir.large-files")
        self.assertIn("BACKTRACE: 3 stack frames", lv.torture)
        self.assertIn("log_stack_trace+0x32", lv.torture)

    def test_missing_log_is_flagged_not_fatal(self):
        lv = report.load_log(RESULTS, "linux", "smb2.setinfo")
        self.assertTrue(lv.missing)
        self.assertEqual(lv.torture, "")
        self.assertEqual(lv.headline, "")

    def test_load_all_logs_covers_every_failing_cell(self):
        model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        logs = report.load_all_logs(RESULTS, model)
        self.assertEqual(len(logs), 5)
        self.assertIn(("memfs", "smb2.charset.Testing wide-a"), logs)
        self.assertTrue(logs[("linux", "smb2.acls.DENY1")].missing)


def _slice_between(text, start_marker, end_marker):
    """Return text from the first occurrence of `start_marker` up to (but
       excluding) the first `end_marker` that follows it, or to the end of
       the string if `end_marker` never follows.  Used to isolate one
       backend's rendered block from the rest of the page.
    """
    start = text.index(start_marker)
    try:
        end = text.index(end_marker, start + len(start_marker))
    except ValueError:
        end = len(text)
    return text[start:end]


def _cell_block(block, subtest):
    """Return the rendered `<details class="t">...</details>` block (plus
       any nested server-log details) for one subtest within `block`.

       Locates the cell's own opening tag by walking backward from its
       `<code>` anchor, and bounds the block at the next cell's opening tag
       (or the end of `block`), so a check against this text can't
       accidentally match a sibling cell that happens to share a marker.
    """
    anchor = f"<code>{report.esc(subtest)}</code>"
    idx = block.index(anchor)
    start = block.rindex('<details class="t">', 0, idx)
    next_idx = block.find('<details class="t">', idx)
    end = next_idx if next_idx != -1 else len(block)
    return block[start:end]


class TestRender(unittest.TestCase):

    def setUp(self):
        self.model = report.build_model(RESULTS, report.load_baseline(CMAKE))
        self.logs = report.load_all_logs(RESULTS, self.model)
        self.html = report.render(self.model, self.logs, "baseline abc1234")
        # Isolate each backend's own rendered block so counts/markers can be
        # checked against the right backend rather than the whole page.
        self.memfs_block = _slice_between(
            self.html, "<summary>memfs ", "<summary>linux ")
        self.linux_block = _slice_between(
            self.html, "<summary>linux ", "</body>")

    def test_is_a_standalone_document_with_no_external_refs(self):
        self.assertTrue(self.html.startswith("<!doctype html>"))
        self.assertNotIn("<script", self.html)
        self.assertNotIn("src=\"http", self.html)
        self.assertNotIn("@import", self.html)

    def test_hostile_test_name_and_log_text_are_escaped(self):
        self.assertNotIn("<script>alert(1)</script>", self.html)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", self.html)

    def test_backend_rows_carry_exact_counts(self):
        # memfs: 2 failing, 1 of 3 passing (see test_counts in TestModel).
        self.assertIn(
            '<span class="n">&mdash; 2 failing, 1/3 passing,', self.memfs_block)
        # linux: 3 failing, 2 of 5 passing.
        self.assertIn(
            '<span class="n">&mdash; 3 failing, 2/5 passing,', self.linux_block)
        self.assertIn("baseline abc1234", self.html)

    def test_totals_line_reports_exact_numbers(self):
        m = re.search(
            r'<p>(\d+)/(\d+) passing &middot; (\d+) failing &middot; '
            r'<span class="regression">(\d+) regressions</span></p>',
            self.html)
        self.assertIsNotNone(m, "totals line not found in the expected shape")
        passing, measured, failing, regressions = (int(g) for g in m.groups())
        self.assertEqual((passing, measured, failing, regressions), (3, 8, 5, 1))

    def test_regression_is_attached_to_memfs_cell_and_not_to_linux_twin(self):
        # smb2.acls.DENY1 exists for both backends and differs only in
        # classification -- memfs is gated (a regression), linux is not (a
        # known gap).  Pin the tag to the right cell in each backend block.
        memfs_deny1 = _cell_block(self.memfs_block, "smb2.acls.DENY1")
        linux_deny1 = _cell_block(self.linux_block, "smb2.acls.DENY1")
        self.assertIn('<span class="tag regression">regression</span>', memfs_deny1)
        self.assertNotIn('class="tag regression"', linux_deny1)
        self.assertIn('<span class="tag known-gap">known-gap</span>', linux_deny1)
        self.assertNotIn('class="tag known-gap"', memfs_deny1)

    def test_suite_ordering_within_backend_is_positional(self):
        suite_names = [m.group(1) for m in re.finditer(
            r'<summary><code>([\w.]+)</code>', self.linux_block)]
        self.assertEqual(suite_names, ["smb2.acls", "smb2.dir", "smb2.setinfo"])

    def test_full_log_text_is_present_verbatim(self):
        self.assertIn("BACKTRACE: 3 stack frames", self.html)
        self.assertIn("log_stack_trace+0x32", self.html)
        # server lines reach the page too, inside the collapsed section
        self.assertIn("Shutting down", self.html)

    def test_missing_log_renders_a_flag_not_a_crash(self):
        self.assertIn("log missing", self.html)

    def test_supports_dark_theme(self):
        self.assertIn("prefers-color-scheme: dark", self.html)

    def test_status_label_distinguishes_time_and_skip_from_fail(self):
        # The fixture only has PASS/FAIL, but regen.sh also writes logs for
        # SKIP (exit 77) and TIME (exit 137) cells, which the model counts
        # as "failing" alongside FAIL.  Build a small synthetic model
        # directly (no fixture files needed) to prove the rendered cell
        # summary surfaces cell.status so those are distinguishable.
        cells = [
            report.Cell("memfs", "smb2.op.a", "smb2.op", "FAIL", "known-gap"),
            report.Cell("memfs", "smb2.op.b", "smb2.op", "TIME", "known-gap"),
            report.Cell("memfs", "smb2.op.c", "smb2.op", "SKIP", "known-gap"),
        ]
        suite = report.Suite("smb2.op", cells)
        backend = report.Backend(
            name="memfs", measured=3, passing=0, failing=3, gated=0,
            regressions=0, suites=[suite])
        model = report.Report(
            backends=[backend], measured=3, passing=0, failing=3,
            regressions=0, classified=True)
        logs = {(c.backend, c.subtest): report.LogView(
            missing=True, headline="", torture="", server="") for c in cells}
        html_out = report.render(model, logs, "synthetic")

        def status_block(subtest):
            anchor = f"<code>{report.esc(subtest)}</code>"
            idx = html_out.index(anchor)
            start = html_out.rindex('<details class="t">', 0, idx)
            summary_end = html_out.index("</summary>", idx)
            return html_out[start:summary_end]

        self.assertIn('<span class="status">FAIL</span>', status_block("smb2.op.a"))
        self.assertIn('<span class="status">TIME</span>', status_block("smb2.op.b"))
        self.assertIn('<span class="status">SKIP</span>', status_block("smb2.op.c"))
        self.assertNotIn("TIME", status_block("smb2.op.a"))
        self.assertNotIn("SKIP", status_block("smb2.op.a"))
        self.assertNotIn("FAIL", status_block("smb2.op.b"))


class TestCli(unittest.TestCase):

    def test_default_cmake_path_finds_the_real_baseline(self):
        path = report.default_cmake_path()
        self.assertIsNotNone(path)
        self.assertTrue(path.endswith(
            os.path.join("src", "server", "smb", "tests", "CMakeLists.txt")))
        self.assertTrue(os.path.exists(path))

    def test_git_provenance_never_raises(self):
        self.assertIsInstance(report.git_provenance("/nonexistent/CMakeLists.txt"), str)

    def test_end_to_end_writes_a_classified_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.html")
            rc = report.main(["--results", RESULTS, "--output", out, "--cmake", CMAKE])
            self.assertEqual(rc, 0)
            with open(out, encoding="utf-8") as f:
                page = f.read()
            self.assertIn("regression", page)
            self.assertNotIn("cannot distinguish", page)

    def test_no_baseline_flag_emits_the_banner(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out2.html")
            rc = report.main(["--results", RESULTS, "--output", out, "--no-baseline"])
            self.assertEqual(rc, 0)
            with open(out, encoding="utf-8") as f:
                page = f.read()
            self.assertIn("unclassified", page)
            self.assertIn("cannot", page)

    def test_unreadable_results_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out3.html")
            rc = report.main(["--results", os.path.join(FIX, "nope.txt"),
                              "--output", out])
            self.assertNotEqual(rc, 0)
            self.assertFalse(os.path.exists(out))

    def test_git_provenance_detects_dirty_baseline_via_relative_path(self):
        """Regression test for the false-clean bug: `git -C <dir>` resolves a
           *relative* pathspec against `<dir>`, not against the caller's
           cwd.  Passing the original relative string through unchanged
           points `git status` at a nonexistent path, which exits 0 with
           empty stdout -- silently reading as clean even though the file is
           genuinely dirty.  Reproduces with a real repo and a path relative
           to a cwd the function itself changes into, so it exercises the
           exact call shape a `--cmake src/server/smb/tests/CMakeLists.txt`
           invocation takes.
        """
        if shutil.which("git") is None:
            raise unittest.SkipTest("git not available")

        with tempfile.TemporaryDirectory() as tmp:
            def run(*args):
                subprocess.run(["git", *args], cwd=tmp, check=True,
                               capture_output=True, text=True)

            run("init", "-q")
            run("config", "user.email", "t@example.com")
            run("config", "user.name", "t")
            rel = os.path.join("src", "server", "smb", "tests", "CMakeLists.txt")
            abs_path = os.path.join(tmp, rel)
            os.makedirs(os.path.dirname(abs_path))
            with open(abs_path, "w") as f:
                f.write("set(ENABLED_memfs)\n")
            run("add", "-A")
            run("commit", "-q", "-m", "init")
            # Dirty it, uncommitted.
            with open(abs_path, "a") as f:
                f.write("# uncommitted edit\n")

            old_cwd = os.getcwd()
            os.chdir(tmp)
            try:
                result = report.git_provenance(rel)
            finally:
                os.chdir(old_cwd)

        self.assertIn("dirty", result)

    def test_git_provenance_reports_dirty_unknown_when_status_check_fails(self):
        """When the dirty check itself can't be trusted (non-zero exit or
           stderr from `git status`), the stamp must say so explicitly
           rather than falling through to a bare clean-looking sha -- the
           whole point of the stamp is to never imply "clean" on a check
           that didn't actually run.
        """
        def fake_run(cmd, **kwargs):
            if cmd[3] == "rev-parse":
                return subprocess.CompletedProcess(cmd, 0, stdout="abc1234\n", stderr="")
            return subprocess.CompletedProcess(cmd, 0, stdout="",
                                               stderr="warning: could not open directory\n")

        with mock.patch("report.subprocess.run", side_effect=fake_run):
            result = report.git_provenance("/some/path/CMakeLists.txt")
        self.assertNotEqual(result, "abc1234")
        self.assertIn("abc1234", result)
        self.assertIn("dirty", result)

    def test_main_returns_nonzero_for_unwritable_output_and_leaves_no_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "no-such-directory", "out.html")
            rc = report.main(["--results", RESULTS, "--output", out, "--cmake", CMAKE])
            self.assertNotEqual(rc, 0)
            self.assertFalse(os.path.exists(out))

    def test_output_file_is_valid_utf8_under_a_non_utf8_locale(self):
        """Regression test for the locale-dependent UnicodeEncodeError:
           `open(path, "w")` without an explicit encoding uses
           locale.getpreferredencoding(), which raises on the provenance
           line's `·` separator under a non-UTF-8 locale (e.g. LC_ALL=C
           in a musl container or a CI runner without PEP 538 coercion).
           UnicodeEncodeError subclasses ValueError, so it slipped past a
           bare `except OSError` guard, leaving a 0-byte file with no error
           message.  The locale's preferred encoding is latched at
           interpreter startup, so forcing it after the fact in this
           process would not reproduce the bug -- report.py must be run as
           a fresh subprocess with the hostile environment in place.
        """
        report_py = os.path.join(os.path.dirname(HERE), "report.py")
        env = dict(os.environ)
        env.update(LC_ALL="C", LANG="C", LANGUAGE="",
                   PYTHONCOERCECLOCALE="0", PYTHONUTF8="0")
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "out.html")
            result = subprocess.run(
                [sys.executable, report_py, "--results", RESULTS,
                 "--output", out, "--cmake", CMAKE],
                env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            with open(out, "rb") as f:
                data = f.read()
        # Decoding as UTF-8 fails outright on mojibake/truncated output;
        # the provenance separator is a known non-ASCII character that
        # must survive intact.
        text = data.decode("utf-8")
        self.assertIn("·", text)

    def test_malformed_cmake_falls_back_to_no_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad_cmake = os.path.join(tmp, "bad.txt")
            with open(bad_cmake, "wb") as f:
                f.write(b"set(ENABLED_memfs\n\xff\xfe bogus)\n")
            out = os.path.join(tmp, "out4.html")
            rc = report.main(["--results", RESULTS, "--output", out,
                              "--cmake", bad_cmake])
            self.assertEqual(rc, 0)
            with open(out, encoding="utf-8") as f:
                page = f.read()
            self.assertIn("unclassified", page)
            self.assertIn("baseline: none", page)


if __name__ == "__main__":
    unittest.main()
