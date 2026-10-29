CSI stress tester 
=================

Continuously creates PVCs and pods that mount them, runs verified I/O on the volumes, then tears
everything down, measuring each step of the CSI lifecycle.

How to build
------------
```
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o csi-stress-test-x86 .
```

How to run
----------
```
./csi-stress-test-x86 --storage-class my-csi-sc --workers 50 --duration 30m --results-file results.jsonl
```
Run `--help` for all flags. Ctrl+C stops the test gracefully (in-flight cycles are torn down and
the test namespaces deleted); a second Ctrl+C exits immediately and leaves resources behind.

The tool only uses and deletes namespaces labelled `app.kubernetes.io/managed-by=csi-stress-test`.
It refuses to start if a namespace matching `--namespace-prefix` already exists without that label.

Measured metrics
----------------
All timestamps come from watch events, not polling.

| Metric          | Meaning                                                                 |
|-----------------|-------------------------------------------------------------------------|
| `pvc_bind`      | PVC create → Bound (with WaitForFirstConsumer this includes scheduling) |
| `pod_schedule`  | pod create → bound to a node (with WaitForFirstConsumer includes CreateVolume) |
| `attach_mount`  | pod scheduled → Running: ControllerPublish, NodeStage/Publish, container start |
| `startup_total` | first PVC create → pod Running                                          |
| `pod_delete`    | pod delete → pod gone (unmount)                                         |
| `pvc_delete`    | PVC delete → PVC gone                                                   |
| `pv_release`    | PVC delete → PV gone: detach + DeleteVolume (reclaimPolicy Delete only) |
| `io_write`      | fsync'd sequential write throughput per volume (MiB/s)                  |
| `io_read`       | O_DIRECT sequential read throughput per volume (MiB/s), with checksum check |

Progress is printed every `--report-interval`. At the end the tool prints p50/p95/p99 per metric,
failures by phase and per-node results. `--results-file` writes one JSON record per cycle.

The `report` subcommand rebuilds that summary from one or more results files, aggregating all
their cycles into a single report (no cluster access needed):

```sh
./csi-stress-test-x86 report results.jsonl
./csi-stress-test-x86 report runs/*.jsonl
```

Elapsed is then the span between the first and the last cycle start across all files.

A cycle fails if a step times out (`--startup-timeout`, `--teardown-timeout`), the pod stops running
during the hold (I/O error or checksum mismatch), or a volume is not released. The failure line
includes the pod conditions and the latest warning events (FailedAttachVolume, FailedMount, ...).

Pre-pull `--image` on the nodes (or run a short warm-up) so image pulls do not inflate the first
`attach_mount` samples on each node.

Pod placement (`--spread`)
--------------------------
* `balanced` (default): every pod is pinned, via required node affinity, to the eligible node with the
  fewest in-flight test pods, so attachments are spread evenly across all hosts. Nodes that are
  cordoned, NotReady, tainted NoSchedule/NoExecute, outside `--node-selector`, or (for CSI drivers)
  without the driver registered in CSINode are skipped. The node list refreshes every 30s, and
  in-flight test volumes are kept under each node's CSINode attach limit.
* `soft`: preferred pod anti-affinity across all test namespaces; the scheduler may still stack pods.
* `none`: the scheduler decides.

With `volumeBindingMode: Immediate` and a topology-constrained driver, the volume can be created in
a zone the pinned node cannot reach. Prefer `WaitForFirstConsumer` with `balanced`.
