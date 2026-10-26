#!/usr/bin/env python3
"""Select GitHub Latest from the complete, paginated release inventory on stdin."""

import json
import re
import sys


def mirrored_version(version):
    match = re.fullmatch(r"0\.(\d+)\.(\d+)", version.removeprefix("v"))
    if match and int(match[2]) > 0:
        return tuple(map(int, match.groups()))
    return None


def make_latest(version, prerelease, pages):
    if not isinstance(pages, list) or not pages or any(not isinstance(page, list) for page in pages):
        raise ValueError("Expected at least one complete release API page")
    published = []
    for page in pages:
        for release in page:
            if (not isinstance(release, dict)
                    or not isinstance(release.get("tag_name"), str)
                    or type(release.get("draft")) is not bool
                    or type(release.get("prerelease")) is not bool
                    or (not release["draft"] and (not isinstance(release.get("published_at"), str)
                                                   or not release["published_at"]))):
                raise ValueError("Incomplete release metadata")
            number = mirrored_version(release["tag_name"])
            if number and not release["draft"] and not release["prerelease"]:
                published.append(number)
    candidate = mirrored_version(version)
    return not prerelease and candidate is not None and all(candidate > number for number in published)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[2] not in ("true", "false"):
        raise SystemExit("Usage: select-release-latest.py VERSION PRERELEASE < paginated-releases.json")
    print(str(make_latest(sys.argv[1], sys.argv[2] == "true", json.load(sys.stdin))).lower())
