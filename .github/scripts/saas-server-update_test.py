#!/usr/bin/env python3
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("saas-server-update.sh")


class ServerUpdateTriggerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.calls = self.root / "gh-calls"
        self.summary = self.root / "summary"
        self.event = self.root / "event.json"
        stub = self.root / "bin"
        stub.mkdir()
        (stub / "gh").write_text('''#!/usr/bin/env bash
set -euo pipefail
[[ "$1 $2 $3" == 'workflow run build-server-next-branch.yml' ]] || exit 99
printf '%s\\n' "$*" >>"$GH_CALLS"
[[ -z ${FAIL_SHA:-} || " $* " != *" server_commit=$FAIL_SHA "* ]]
''')
        (stub / "gh").chmod(0o755)
        self.env = dict(os.environ, PATH=f"{stub}:{os.environ['PATH']}",
                        GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null",
                        GIT_CONFIG_NOSYSTEM="1", GIT_AUTHOR_NAME="Fixture",
                        GIT_COMMITTER_NAME="Fixture", GIT_AUTHOR_EMAIL="fixture@example.com",
                        GIT_COMMITTER_EMAIL="fixture@example.com",
                        GIT_AUTHOR_DATE="2024-01-01T00:00:00Z", GIT_COMMITTER_DATE="2024-01-01T00:00:00Z",
                        GITHUB_EVENT_NAME="push", GITHUB_REPOSITORY="temporalio/temporal",
                        GITHUB_EVENT_PATH=str(self.event), GITHUB_STEP_SUMMARY=str(self.summary),
                        GH_CALLS=str(self.calls), GH_TOKEN="fixture", SAAS_SERVER_UPDATES_ENABLED="true")
        self.git("init", "-q", "-b", "main")
        self.base = self.commit("Base")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root, env=self.env, text=True).strip()

    def commit(self, message):
        self.git("commit", "-qm", message, "--allow-empty")
        return self.git("rev-parse", "HEAD")

    def payload(self, before, after, **updates):
        data = dict(repository={"full_name": "temporalio/temporal"}, ref="refs/heads/main",
                    before=before, after=after, deleted=False)
        data.update(updates)
        self.event.write_text(json.dumps(data))

    def run_script(self, mode, **env):
        return subprocess.run(["bash", str(SCRIPT), mode], cwd=self.root,
                              env=dict(self.env, **env), text=True, capture_output=True)

    def dispatches(self):
        return self.calls.read_text().splitlines() if self.calls.exists() else []

    def expected(self, *shas):
        return [f"workflow run build-server-next-branch.yml -R temporalio/saas-temporal -r main -f server_commit={sha}"
                for sha in shas]

    def test_squash_merge_enumerates_only_final_landed_commit(self):
        self.git("checkout", "-qb", "feature")
        (self.root / "change").write_text("first")
        self.git("add", "change")
        first = self.commit("PR first")
        (self.root / "change").write_text("second")
        self.git("add", "change")
        second = self.commit("PR second")
        self.git("checkout", "-q", "main")
        self.git("merge", "--squash", "feature")
        landed = self.commit("Final squash")
        self.payload(self.base, landed)
        result = self.run_script("enumerate")
        self.assertEqual((result.returncode, result.stdout.splitlines()), (0, [landed]), result.stderr)
        self.assertNotIn(first, result.stdout)
        self.assertNotIn(second, result.stdout)

    def test_multi_commit_push_dispatches_each_full_sha_and_reruns_repeat_identity(self):
        commits = [self.commit(name) for name in ("A", "B", "C")]
        self.payload(self.base, commits[-1])
        for attempt in ("1", "2"):
            result = self.run_script("dispatch", GITHUB_RUN_ATTEMPT=attempt)
            self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.dispatches(), self.expected(*commits, *commits))

    def test_enumeration_uses_the_event_range_after_the_checkout_moves(self):
        first = self.commit("A")
        last = self.commit("B")
        self.commit("Later main")
        self.payload(self.base, last)
        result = self.run_script("enumerate")
        self.assertEqual((result.returncode, result.stdout.splitlines()), (0, [first, last]), result.stderr)

    def test_invalid_ranges_fail_without_dispatch_or_tip_fallback(self):
        tip = self.commit("Tip")
        self.git("checkout", "-q", "--detach", self.base)
        other = self.commit("Other history")
        for before, after in (("0" * 40, tip), (self.base, "f" * 40),
                              (tip, self.base), (other, tip), ("main", tip), (None, tip)):
            with self.subTest(before=before, after=after):
                self.payload(before, after)
                result = self.run_script("dispatch")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Incomplete", result.stderr)
        self.assertEqual(self.dispatches(), [])

    def test_non_main_unmerged_and_fork_events_are_excluded(self):
        tip = self.commit("Tip")
        cases = [({"ref": "refs/heads/topic"}, {}), ({"deleted": True}, {}),
                 ({"repository": {"full_name": "contributor/temporal"}}, {}),
                 ({}, {"GITHUB_EVENT_NAME": "pull_request"}),
                 ({}, {"GITHUB_REPOSITORY": "contributor/temporal"})]
        for payload, env in cases:
            with self.subTest(payload=payload, env=env):
                self.payload(self.base, tip, **payload)
                self.assertNotEqual(self.run_script("dispatch", **env).returncode, 0)
        self.assertEqual(self.dispatches(), [])

    def test_partial_failure_reports_known_accepted_candidates_and_keeps_failure(self):
        first, failed, last = [self.commit(name) for name in ("A", "B", "C")]
        self.payload(self.base, last)
        result = self.run_script("dispatch", FAIL_SHA=failed)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.dispatches(), self.expected(first, failed, last))
        summary = self.summary.read_text()
        self.assertIn(f"Accepted dispatch: {first}", summary)
        self.assertIn(f"Accepted dispatch: {last}", summary)
        self.assertIn(f"Dispatch not confirmed: {failed}", summary)
        self.assertNotIn(f"Accepted dispatch: {failed}", summary)

    def test_disabled_by_default_and_false_configuration_sends_no_dispatch(self):
        self.payload(self.base, self.commit("Tip"))
        self.env.pop("SAAS_SERVER_UPDATES_ENABLED")
        for setting in ({}, {"SAAS_SERVER_UPDATES_ENABLED": "false"}):
            result = self.run_script("dispatch", **setting)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("disabled", result.stdout)
        self.assertEqual(self.dispatches(), [])

    def test_empty_valid_range_does_not_fall_back_to_tip(self):
        self.payload(self.base, self.base)
        result = self.run_script("enumerate")
        self.assertEqual((result.returncode, result.stdout), (0, ""))
        self.assertEqual(self.run_script("dispatch").returncode, 0)
        self.assertEqual(self.dispatches(), [])


if __name__ == "__main__":
    unittest.main()
