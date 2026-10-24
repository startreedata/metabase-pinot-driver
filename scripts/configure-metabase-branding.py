#!/usr/bin/env python3
"""Add the StarTree connector logo to the pinned Metabase frontend before building."""

from pathlib import Path
import re
import shutil
import sys


root = Path(__file__).resolve().parents[1]
metabase = Path(sys.argv[1])
constants = metabase / "frontend/src/metabase/databases/constants.tsx"
logos = metabase / "resources/frontend_client/app/assets/img/drivers"
source = constants.read_text()
pattern = re.compile(
    r"(export const ENGINE_LOGO: Record<string, string> = \{\n)(.*?)(^\};)",
    re.MULTILINE | re.DOTALL,
)
match = pattern.search(source)
if match is None or not logos.is_dir():
    sys.exit("Metabase's database logo layout changed; review the branding integration.")

body = match.group(2)
existing = re.findall(r'^\s*(?:pinot|"pinot"):\s*"([^"]+)",\s*$', body, re.MULTILINE)
if existing and existing != ["startree.png"]:
    sys.exit("Metabase already defines a different Pinot logo; review before replacing it.")
if not existing:
    replacement = match.group(1) + '  pinot: "startree.png",\n' + body + match.group(3)
    constants.write_text(source[:match.start()] + replacement + source[match.end():])

shutil.copyfile(root / "assets/startree.png", logos / "startree.png")
print("Configured the StarTree(Pinot) logo for the Metabase frontend.")
