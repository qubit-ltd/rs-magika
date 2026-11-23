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

Date: 2026-11-23

Searched the local downstream documentation and manifests for Magika consumers and workload targets. The reviewed evidence was:

- `rs-mime/doc/user_guide.md` and `doc/user_guide.zh_CN.md` show how an application links the Magika provider into the MIME registry; both state that the default provider remains `repository`. They define no request mix, concurrency target, latency objective, or memory budget.
- `rs-fs/doc/implementation-plan-20261123T210328882Z-c92f371e.md` and `rs-fs-local/doc/implementation-plan-20261123T210136855Z-b0c6eadd.md` mention Magika as a downstream dependency/compatibility validation target. They provide no deployment workload or performance acceptance criteria.
- Searching local `rs-*/Cargo.toml` manifests found no downstream manifest declaring `qubit-magika`; the crate manifest itself identifies it as the Magika-backed MIME detector integration. The documentation examples are integration guidance, not evidence of a deployed workload.

The 2026-11-17 experiment above remains useful as a mechanism check: it used one deliberately gated provider request and a small Python byte request, with 30 warm rounds per topology and injected delays of 0, 10, and 100 ms. Those values are test controls, not observed production delays or acceptance thresholds. It reports completion p50/p95 and admission/refusal counts, but not cold startup, peak RSS, CPU, or production request distributions. It therefore cannot establish whether either topology meets a real service objective.

No representative workload, target concurrency, accepted tail-latency bound, or memory budget was found. Accordingly, the ignored experiment was not rerun as a workload comparison, since no representative input distribution or acceptance criteria exist. No isolation mechanism can be selected against an absent target; retain the shared runtime architecture for this change. A future decision needs an identified consumer, its mixed request distribution and concurrency, explicit p95/p99 and memory limits, then cold-start time, peak RSS, success/refusal rate, and warm p50/p95 measurements for both topologies on the same machine and sample set.

Evidence search commands (read-only, executed from the assigned worker worktree):

```text
rg -l -i 'rs-magika|magika' /data/working/qubit/rust-common/rs-* --glob 'Cargo.toml' --glob '*.md' --glob '*.yml' --glob '*.yaml' --glob '*.toml'  # exit 0
rg -n -i 'qubit-magika|rs-magika|magika' /data/working/qubit/rust-common/rs-*/Cargo.toml  # exit 0
rg -n -i 'rs-magika|magika' /data/working/qubit/rust-common/rs-fs/doc/implementation-plan-20261123T210328882Z-c92f371e.md /data/working/qubit/rust-common/rs-fs-local/doc/implementation-plan-20261123T210136855Z-b0c6eadd.md /data/working/qubit/rust-common/rs-mime/doc/user_guide.md /data/working/qubit/rust-common/rs-mime/doc/user_guide.zh_CN.md  # exit 0
```
