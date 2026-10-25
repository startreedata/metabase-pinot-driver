# Driver maintenance

This repository's maintainer chat coordinates daily upstream checks, focused
implementation tasks, compatibility validation, and driver publication.

## Release policy

- Check Metabase releases daily at 09:00 America/Los_Angeles using GitHub
  releases, the official [version feed](https://static.metabase.com/version-info.json),
  and supported-version documentation. The feed can list stable releases before
  GitHub has a release entry; verify their source tags and official downloads.
  Publish matching drivers for stable major and point (minor) releases after
  validation. Under Metabase's
  [License.Major.Point.Hotfix](https://www.metabase.com/docs/latest/developers-guide/versioning)
  scheme, require exactly three numeric components with a point greater than zero.
  Exclude hotfixes, drafts, previews, Cloud builds, floating `.x` versions, and
  point-zero beta releases from publication.
- Mirror the target Metabase version in the driver tag, artifact filename, and
  embedded plugin version: Metabase `v0.64.3` maps to driver `v0.64.3` with plugin
  version `0.64.3`. Mirror only eligible stable major and point releases.
  Record the exact versions tested; matching version numbers are not proof of
  compatibility. Preserve historical driver `v1.x` releases and explicitly mark
  the first mirrored stable release as latest.
- Prioritize the current stable Metabase line. Evaluate the previous supported
  line and LTS separately; promise support only after each has passed validation.
  Treat upstream previews as early compatibility warnings.
- Validate against the Pinot 1.5 release line using the exact patch pinned in
  `app_versions.json`. Record the actual Pinot patch tested; results against a
  different release line or release candidate do not establish 1.5 compatibility.
- Validate the same driver JAR against the build target and every additional
  runtime in `app_versions.json`'s `metabase_compatibility` list. For driver
  `v0.64.3`, require Metabase `v0.63.21` and `v0.64.3`, including native parameters.
- Compare upstream versions numerically, including hotfix components. A patch
  published on an older line is not a newer current line. Deduplicate paired
  open-source and paid-edition releases; do not claim paid-edition compatibility
  unless it has been tested.
- Reuse active tasks and PRs before launching more work. Keep changes focused,
  use isolated branches/checkouts, and avoid unnecessary tests or benchmarks.
  Dependency-only PR maintenance is already covered by a separate automation.

## Publication requirements

1. Pin the exact upstream release and source commit. Reuse existing focused
   tests and add coverage only for actual compatibility changes.
2. Check that the packaged plugin loads in the target Metabase and can connect
   to Pinot, sync metadata, execute native SQL and a query-builder aggregation,
   and substitute query parameters. A build or mocked test is not runtime proof.
3. Require successful CI for the exact driver source commit and resolve
   actionable review feedback. Follow normal branch protection without bypass.
4. Stamp the release version into plugin metadata; include tested Metabase
   versions, source SHA, installation instructions, and checksums in the release.
5. Verify the published tag points to the intended source commit and the JAR,
   embedded version, checksum, and release notes match the validated artifact.
   Distinguish prepared, tested, merged, and published states in reports.

Do not publish while compatibility evidence is incomplete. Record the concrete
blocker and continue independent work. Keep unchanged daily checks quiet; report
new upstream releases, completed work, publication, failures, or required input.

## Initial baseline — October 22, 2026

| Item | Observed state |
| --- | --- |
| Driver release | [v1.3.1](https://github.com/startreedata/metabase-pinot-driver/releases/tag/v1.3.1), published May 22, 2026 |
| Build target | Metabase v0.55.7; Pinot 1.3.0 (`app_versions.json`) |
| Plugin metadata | `1.3.0-SNAPSHOT` in source; release build must stamp its version |
| Current Metabase | [v0.64.1](https://github.com/metabase/metabase/releases/tag/v0.64.1) |
| Previous supported line | [v0.63.20](https://github.com/metabase/metabase/releases/tag/v0.63.20) |
| LTS to evaluate | [v0.58.35](https://github.com/metabase/metabase/releases/tag/v0.58.35) |
| Preview warning | [v0.65.0-beta](https://github.com/metabase/metabase/releases/tag/v0.65.0-beta) |
| Existing CI | [Successful on `468e88a`](https://github.com/startreedata/metabase-pinot-driver/actions/runs/38528719454), against the existing build pin |

This inventory does not establish compatibility with any newer Metabase release.
Refresh it from GitHub and the upstream supported-version documentation before
acting; the dates and tags above are a starting point.

## Active work

- Repair release metadata and CLI dispatch: branch
  `xiangfu0/codex2/release-maintenance` in the maintainer chat.
- Update Pinot driver for Metabase 0.64: implementation chat
  `01a16491-9ec8-7e72-939d-899ab5b8a3cf` on the local host. This task owns driver,
  build, and CI migration and must provide real packaged-plugin runtime evidence.
- After current-stable validation, assess the previous supported line and LTS;
  do not launch duplicate work while the current migration is active.

The daily automation keeps its mutable ledger beside its local automation
configuration: upstream tags seen, active chat/PR IDs, exact tested versions and
SHAs, publication results, artifact digests, and unresolved blockers. Repository
documentation records policy; the ledger records each run's live state.
