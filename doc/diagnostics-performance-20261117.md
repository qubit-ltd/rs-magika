# Managed runtime diagnostics and Provider latency experiment

Date: 2026-11-17

## Method

Executed the ignored real integration experiment with:

```text
cargo test --test tests test_slow_provider_mixed_latency -- --ignored --nocapture
```

The test used the bundled Magika model through `MagikaMimeDetector`, `MimeConfig::default()`, and the real managed provider path. Its `AsyncProviderGate` paused the first request's random access range read. A second small Python byte request was then admitted. In the shared case both requests used the same detector/runtime. In the independent case the slow provider request used the shared detector and the byte request used another detector/runtime. Both Magika sessions were warmed with the Python sample before collecting measurements; initialization and cold start are excluded.

For each 0, 10, and 100 ms controlled provider delay, each topology ran 30 rounds. The gate release thread slept for the requested delay after the second request's first poll/admission notification. Runtime byte budget and `max_bytes` were both derived from the default configuration; the slow request reserved 4,096 bytes, the second request used the Python source (38 bytes), and the service group capacity was 4. All 180 rounds were admitted; none were refused.

`admission_us` measures from second request submission to its first poll notification (the request has reserved capacity and submitted inference by then). `completion_us` measures from that notification to the second request's successful business outcome. The companion deterministic ordering test takes a runtime snapshot while the gate is held and asserts one worker-running request and one admitted-not-started request. The current public API does not expose a per-request worker-start timestamp, so the experiment cannot separate pure queue wait from inference time after release. `completion_us` is therefore end-to-end wait after admission, not a pure queue metric. No memory/CPU instrumentation was collected.

## Environment and input

- OS: Linux `mas-server`, kernel `6.8.0-147-generic`, x86_64
- CPU: Intel Xeon E5-2680 v4 @ 2.40 GHz, 56 logical CPUs
- Toolchain: rustc 1.94.0, cargo 1.94.0
- Feature: `bundled-onnxruntime` (the integration test is feature-gated)
- Input: `#!/usr/bin/env python3\nprint('hello')\n`
- Raw timing unit: microseconds; samples below are sorted ascending.

## Results

| Topology | Provider delay | n | Admitted / refused | completion p50 | completion p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Shared runtime | 0 ms | 30 | 30 / 0 | 10,154 us | 11,502 us |
| Independent runtimes | 0 ms | 30 | 30 / 0 | 5,637 us | 6,732 us |
| Shared runtime | 10 ms | 30 | 30 / 0 | 21,519 us | 23,561 us |
| Independent runtimes | 10 ms | 30 | 30 / 0 | 6,411 us | 6,795 us |
| Shared runtime | 100 ms | 30 | 30 / 0 | 113,624 us | 114,091 us |
| Independent runtimes | 100 ms | 30 | 30 / 0 | 6,851 us | 7,130 us |

### Raw sorted samples

```text
shared, delay=0ms, admission_us=[263,264,275,275,279,284,299,301,304,305,313,318,336,336,338,339,342,343,345,347,360,362,364,364,378,382,402,404,449,506]
shared, delay=0ms, completion_us=[9159,9175,9393,9413,9626,9631,9687,9747,9755,9778,9899,9999,10034,10053,10137,10154,10199,10280,10366,10398,10449,10543,10589,11341,11401,11455,11460,11487,11502,12728]
independent, delay=0ms, admission_us=[150,151,158,159,160,166,169,170,172,181,184,184,184,192,206,212,224,224,229,261,261,262,267,269,273,275,277,278,289,312]
independent, delay=0ms, completion_us=[3964,4033,4091,4145,4341,4488,4513,5163,5333,5395,5420,5479,5608,5618,5620,5637,5741,5793,5844,5860,5870,5996,6010,6151,6247,6290,6349,6621,6732,6906]
shared, delay=10ms, admission_us=[164,169,170,175,195,211,218,221,222,236,256,282,323,324,326,327,330,337,341,344,346,347,347,351,355,356,358,363,411,421]
shared, delay=10ms, completion_us=[16991,17139,17159,17366,18450,19103,19329,19351,19577,19656,20299,20839,20925,20926,20962,21519,21562,22004,22449,22641,22695,23217,23388,23400,23453,23496,23530,23533,23561,23718]
independent, delay=10ms, admission_us=[187,197,255,267,277,289,300,302,314,316,319,323,324,329,330,335,340,341,341,345,345,347,349,351,353,366,391,392,419,424]
independent, delay=10ms, completion_us=[5100,5213,5344,5464,5516,5860,6015,6036,6078,6098,6222,6247,6333,6352,6397,6411,6421,6461,6470,6525,6535,6576,6598,6622,6624,6684,6700,6793,6795,6963]
shared, delay=100ms, admission_us=[320,326,338,353,355,355,356,359,362,364,365,365,366,374,375,375,380,380,384,385,391,396,407,413,414,430,439,462,464,473]
shared, delay=100ms, completion_us=[111082,111231,111340,111355,111358,111422,111500,111686,111693,111991,112067,113099,113114,113491,113601,113624,113693,113732,113758,113776,113778,113819,113913,113935,114009,114020,114054,114087,114091,114327]
independent, delay=100ms, admission_us=[305,308,319,319,319,332,343,343,344,345,346,346,347,349,351,351,351,352,354,355,358,361,363,365,369,369,371,376,386,441]
independent, delay=100ms, completion_us=[4753,4984,5261,5886,6572,6593,6620,6622,6761,6806,6819,6826,6828,6846,6850,6851,6865,6869,6893,6894,6895,6905,6925,6943,6950,6991,7020,7066,7130,7199]
```

## Interpretation and decision

The shared-runtime completion latency rises with the gated provider delay, while the independent runtime remains near its warmed inference latency. This reproduces the head-of-line mechanism and shows that a separately owned runtime isolates the second request in this test. It also costs an additional Magika session/runtime; this experiment did not measure its memory or initialization cost.

There is no verified downstream SLO, workload mix, or memory budget in this task establishing that an additional session or scheduling redesign is warranted. Keep the existing single-worker architecture for this change. Deployments that require isolation can use separate runtimes and should budget for the extra session. Consider a future sampling/inference split only with workload-specific SLO and memory measurements.

## MAG-R11 workload evidence audit

Date: 2026-11-24

Searched the local source-layout root (`/data/working/qubit/rust-common`) across 60 `rs-*` repositories. Manifest search found no downstream `qubit-magika` consumer; the Magika crate itself is the sole manifest hit. Documentation search found Magika references in eight repositories: `rs-config`, `rs-fs`, `rs-fs-local`, `rs-io`, `rs-local-files`, `rs-magika`, `rs-mime`, and `rs-spi`. Version, CI, and planning references were excluded when assessing workload evidence. The relevant reviewed evidence was:

- `rs-mime/doc/user_guide.md` and `rs-mime/doc/user_guide.zh_CN.md` show how an application links the Magika provider into the MIME registry; both state that the default provider remains `repository`. They define no request mix, concurrency target, latency objective, or memory budget.
- `rs-io` has a historical downstream test reference, `rs-config` has a dependency snapshot, and `rs-mime` has a synthetic acceptance test. These establish compatibility or test coverage only, not deployment workload.
- The local FS and FS-local implementation planning documents mention Magika as a downstream dependency/compatibility validation target. They provide no deployment workload or performance acceptance criteria. These are internal planning files; their exact paths and search output are kept in the task's external evidence directory.
- The Magika repository has a manually gated two-request p50/p95 experiment documented above. Its deliberately gated provider read and small Python byte request are a mechanism check, not evidence of representative production traffic.

The 2026-11-17 experiment above remains useful as a mechanism check: it used one deliberately gated provider request and a small Python byte request, with 30 warm rounds per topology and injected delays of 0, 10, and 100 ms. Those values are test controls, not observed production delays or acceptance thresholds. It reports completion p50/p95 and admission/refusal counts, but not cold startup, peak RSS, CPU, or production request distributions. It therefore cannot establish whether either topology meets a real service objective.

No representative request mix, target concurrency, tail-latency SLO, or memory cap was found. The reviewed consumer and compatibility materials also provide no per-poll segment distribution or executor-fairness measurements; deterministic gate tests establish ordering and cancellation behavior, not fairness under representative load. Accordingly, the ignored experiment was not rerun as a workload comparison. The available evidence does not justify an architectural change, so retain the shared runtime architecture. A future decision needs an identified consumer, its mixed request distribution and concurrency, explicit p95/p99 and memory limits, then cold-start time, peak RSS, success/refusal rate, warm p50/p95 measurements, and small-segment per-poll work or fairness measurements for both topologies on the same machine and sample set. Do not use the historical p50/p95 table as a production SLO result.

Evidence search commands (read-only):

Run from the adjacent source-layout root (the directory containing `rs-*`):

```text
rg -l -i 'rs-magika|magika' rs-* --glob 'Cargo.toml' --glob '*.md' --glob '*.yml' --glob '*.yaml' --glob '*.toml'  # exit 0
rg -n -i 'qubit-magika|rs-magika|magika' rs-*/Cargo.toml  # exit 0
rg -n -i 'rs-magika|magika' rs-fs/doc rs-fs-local/doc --glob '*.md' rs-mime/doc/user_guide.md rs-mime/doc/user_guide.zh_CN.md  # exit 0
```

## 2026-11-24 downstream fixture and workload evidence

The recursive scan of tracked Cargo manifests in the source workspace found 60 `rs-*` Git repositories and 219 tracked `Cargo.toml` files (including nested fixtures); build output directories were excluded by scanning Git's tracked-file index. Four manifests matched `qubit-magika|rs-magika`: `rs-magika/Cargo.toml`, `rs-magika/tests/fixtures/dynamic_loader/Cargo.toml`, `rs-magika/tests/fixtures/readme_consumer/Cargo.toml`, and `rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml`. The matching source repositories were `rs-magika@41a444b109c22bc6757fc3571d821add46ddd9b6` and `rs-mime@61db4a3e6fd974cd7c9b52377de7e17d13ce0818`. No other tracked sibling manifest refers to Magika. The MIME fixture is explicit: without its `inventory` feature it imports and registers `MagikaMimeDetectorProvider`; with `inventory` it asserts that the built-in registry contains `magika` and selects it by name. Both branches assert that the registry default remains `repository`. The fixture checks Python content as `text/x-python`, PDF filename detection as `application/pdf`, and stops the explicitly owned runtime in the non-inventory branch.

The scan was run from the source workspace root with:

```bash
repo_count=0; manifest_count=0; hit_files=0
for repo in rs-*; do
  [ -d "$repo/.git" ] || continue
  repo_count=$((repo_count + 1))
  while IFS= read -r -d '' manifest; do
    manifest_count=$((manifest_count + 1))
    if rg -nH 'qubit-magika|rs-magika' "$repo/$manifest"; then
      hit_files=$((hit_files + 1))
    else
      code=$?
      [ "$code" -eq 1 ] || exit "$code"
    fi
  done < <(git -C "$repo" ls-files -z -- ':(glob)**/Cargo.toml' 'Cargo.toml')
done
printf 'repositories=%s tracked_cargo_manifests=%s matching_files=%s\n' \
  "$repo_count" "$manifest_count" "$hit_files"
```

Result: `repositories=60 tracked_cargo_manifests=219 matching_files=4`.

The fixture is present in `rs-mime@61db4a3e6fd974cd7c9b52377de7e17d13ce0818`. The fixed dependency pin `rs-mime@33424a6f63feb8dab3a1214fd68f9ce1151c7762` does not contain it; the requested manifest path there fails with `error: manifest path ... does not exist` (exit 101). No fixture files were copied or changed. The successful metadata checks used `rs-mime@61db4a3e6fd974cd7c9b52377de7e17d13ce0818`, `rs-magika@41a444b109c22bc6757fc3571d821add46ddd9b6`, and `rs-operation@175ddef5624abfb8d56f77fd5028da0b808cb0cc`.

The fixture manifest disables Magika default features and declares its own `inventory = ["qubit-magika/inventory"]`; its lockfile records `qubit-magika 0.18.0`. Its workflow, `.github/workflows/magika-consumer.yml`, is manually dispatchable and runs on relevant pull requests and `main` pushes. It pins the sibling graph, installs Rust 1.94.0, downloads and checksum-verifies ONNX Runtime 1.24.2, checks the unique `qubit-operation` identity in both feature graphs, then runs the consumer in explicit-registration and inventory-discovery modes. The workflow pins a historical Magika SHA (`d78dd23a1b849ff8a3e3cbfcdd97def889ce0a7b`); these offline metadata checks instead resolved the source checkout at `41a444b`.

Both locked offline metadata configurations succeeded (exit 0) against the matching registered layout:

```text
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml --features inventory
```

Each resolved the fixture root `magika-inventory-consumer 0.0.0`, `qubit-magika 0.18.0` from `rs-magika@41a444b`, `qubit-mime 0.22.0` from `rs-mime@61db4a3`, and `qubit-operation 0.3.2` from `rs-operation@175ddef`. Raw JSON and stderr were retained outside the source workspace as execution evidence. No build or runtime artifact was produced by metadata. The actual workflow's ONNX Runtime download and consumer runs were inspected but not run in this offline metadata check.

| Production evidence requested | Finding |
| --- | --- |
| Representative request proportions / input mix | Missing; fixture covers one Python content sample and one PDF filename assertion only. |
| Target concurrency | Missing; CI executes a single consumer process and sets no workload concurrency target. |
| p95 / p99 latency objectives | Missing; the workflow has no latency thresholds. The historical local mechanism experiment only reports p50/p95 for synthetic gated requests and defines no SLO. |
| Memory ceiling | Missing; neither fixture nor workflow sets or measures an RSS budget. |
| Per-poll segment sampling / fairness evidence | Missing; fixture behavior assertions do not record segment counts, work per poll, or fairness under load. |

There is no identified production load and no complete target set, so the planned same-input topology comparison was not run. Existing cold-start, peak RSS, p99, success/refusal under representative load, and segment fairness evidence remain absent. The downstream fixture establishes integration shape and locked dependency resolution, not production performance. No evidence supports changing the runtime topology in this pass; keep the architectural decision and evidence limits stated above.

## 2026-11-24 T8 revalidation against the assigned dependency layout

The assigned Magika worktree is `rs-magika@2ad8d078c20202eef3388cd2f38492ebc8f9f0ab`; its sibling dependency worktree is the requested `rs-mime@33424a6f63feb8dab3a1214fd68f9ce1151c7762`. The sibling worktree has no `tests/fixtures/magika_inventory_consumer` directory. Both requested locked offline metadata commands were therefore attempted from the assigned Magika worktree and failed before resolving a graph (exit 101, `manifest path ... does not exist`):

```text
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml --features inventory
```

For comparison, the read-only source-layout repository currently has `rs-mime@d47152393640a1c632e12c0204dea5f114a9ea27`, and contains the fixture and workflow. Running the same metadata configurations there succeeded offline (exit 0) for both explicit registration and `inventory`. Both graphs resolve the fixture root `magika-inventory-consumer 0.0.0`, `qubit-magika 0.18.0`, `qubit-mime 0.22.0`, and `qubit-operation 0.3.2`. These resolve the source-layout path dependencies at the time of the check; they are not the assigned fixed dependency graph and do not reproduce the workflow's historical pins.

The source-layout `.github/workflows/magika-consumer.yml` checks out Magika at `d78dd23a1b849ff8a3e3cbfcdd97def889ce0a7b` and operation at `81048bfdee881d45d510e14eec5c1c3016a545d1`, then verifies operation identity and runs both consumer configurations after downloading and checking ONNX Runtime. The assigned rs-mime SHA lacks that workflow and fixture, so this pass inspected the workflow and repeated metadata only; it did not run the historical checkout graph, download ONNX Runtime, or execute the consumer.

This revalidation does not change the workload evidence finding: the fixture is integration coverage, while representative request mix, target concurrency, p95/p99 objectives, memory ceiling, and per-poll segment/fairness data remain unavailable. The existing synthetic, warm two-request experiment remains a mechanism check and does not justify a topology change.

## 2026-11-24 T7 revalidation in the registered round-13 layouts

The assigned task worktree is `rs-magika@cf7959bb0e02c1f7758c068af1b4b64bd6b27626`. Its registered fixed dependency worktree is `rs-mime@33424a6f63feb8dab3a1214fd68f9ce1151c7762`, matching `.infra/ci/local-path-dependencies.tsv`; it has neither `tests/fixtures/magika_inventory_consumer/Cargo.toml` nor `.github/workflows/magika-consumer.yml`. Both planned commands were run from the assigned worktree and exited 101 because the manifest path does not exist:

```text
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml
cargo +1.94.0 metadata --locked --offline --format-version 1 --manifest-path ../rs-mime/tests/fixtures/magika_inventory_consumer/Cargo.toml --features inventory
```

For the requested latest-source comparison, the registered downstream copy is `rs-mime@5ebd73f40b4553503f56def4c98dd82b0bffeee4`, with `rs-magika@cf7959bb0e02c1f7758c068af1b4b64bd6b27626` and `rs-operation@893bcf6c7efb397cc8fac2f7fe18fb5e87017e63`. The same two metadata commands, run from that Magika copy, both exited 0. Each graph resolved the fixture package `magika-inventory-consumer 0.0.0`, local path packages `qubit-magika 0.18.0`, `qubit-mime 0.22.0`, and `qubit-operation 0.3.2`. These prove metadata compatibility for the registered latest-source layout and lockfile; they do not test the fixed graph or build/run the consumer. Raw JSON and fixed-layout stderr are retained in the external round-13 evidence directory.

The latest `rs-mime` workflow still describes a separate historical dependency graph: `.github/workflows/magika-consumer.yml` checks out Magika at `d78dd23a1b849ff8a3e3cbfcdd97def889ce0a7b` and operation at `893bcf6c7efb397cc8fac2f7fe18fb5e87017e63`, plus the fixed sibling pins listed in the workflow. Thus the metadata run above used the current registered source copies, not the workflow's historical Magika pin. The fixed `rs-mime` pin has no workflow or fixture to inspect. No historical workflow checkout, ONNX Runtime download, build, or consumer execution was performed.

The production-evidence review remains unchanged: the available fixture is a compatibility check, and the earlier synthetic two-request experiment is only a mechanism check. No representative request mix, production concurrency, p95/p99 objective, RSS limit, or per-poll segment/fairness measurements were found. Consequently no same-machine topology comparison has a defensible production workload or acceptance target, and this evidence does not justify changing runtime architecture.
