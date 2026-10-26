"""Regression checks for the release workflow's latest-selection contract."""

import json
from pathlib import Path
import subprocess
import sys
import unittest

SELECTOR = Path(__file__).with_name("select-release-latest.py")


def release(tag, **overrides):
    return {"tag_name": tag, "draft": False, "prerelease": False,
            "published_at": "2026-10-20T00:00:00Z", **overrides}


class LatestReleaseTest(unittest.TestCase):
    def select(self, version, pages, prerelease=False):
        return subprocess.run([sys.executable, str(SELECTOR), version, str(prerelease).lower()],
                              input=json.dumps(pages), text=True, capture_output=True)

    def test_latest_selection(self):
        cases = [
            ("0.64.2", [[release("v0.64.3")]], False),
            ("0.63.21", [[release("v0.64.3")]], False),
            ("0.58.37", [[release("v0.64.3")]], False),
            ("0.64.3", [[release("v0.64.3")]], False),
            ("0.64.10", [[release("v0.64.9")]], True),
            ("0.65.1", [[release("v0.64.3"), release("v1.3.1")]], True),
            ("0.64.1", [[release("v1.3.1")]], True),
            ("0.64.1", [[]], True),
            ("0.64.3", [[release("v0.65.1", draft=True),
                         release("v0.65.1", prerelease=True),
                         release("v0.65.0-beta"), release("v0.64.3.1")]], True),
            ("0.64.3", [[release("v1.3.1")], [release("v0.65.1")]], False),
            ("0.65.0", [[]], False),
            ("0.64.3.1", [[]], False),
        ]
        for version, pages, expected in cases:
            with self.subTest(version=version, pages=pages):
                result = self.select(version, pages)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), str(expected).lower())
        self.assertEqual(self.select("0.65.1", [[]], prerelease=True).stdout.strip(), "false")

    def test_incomplete_inventory_fails_closed(self):
        for pages in ([], {}, [None], [[{"tag_name": "v0.65.1"}]],
                      [[release("v0.65.1", published_at=None)]],
                      [[release("v0.65.1", draft="false")]]):
            with self.subTest(pages=pages):
                self.assertNotEqual(self.select("0.64.3", pages).returncode, 0)


if __name__ == "__main__":
    unittest.main()
