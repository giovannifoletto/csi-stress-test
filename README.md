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

Progress is printed every `--report-interval`, with in-flight pods and volumes. At the end the tool prints p50/p95/p99 per metric, failures by phase and
per-node results. With `--spread=balanced` it also prints the peak number of volumes seen on a
single node and `attach_mount` grouped by how many volumes the node already had when the pod was
assigned. `--results-file` writes one JSON record per cycle (including `node_vols`, that occupancy).

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
  Volumes that other workloads attached to the node through the same driver (VolumeAttachments
  of non-test PVs) count towards that limit too. `--ignore-attach-limit` removes the cap; the
  scheduler still enforces the CSINode limit, so the extra pods stay Pending and fail at `startup`.
* `soft`: preferred pod anti-affinity across all test namespaces; the scheduler may still stack pods.
* `none`: the scheduler decides.

With `volumeBindingMode: Immediate` and a topology-constrained driver, the volume can be created in
a zone the pinned node cannot reach. Prefer `WaitForFirstConsumer` with `balanced`.

Maximum volumes on a single node
--------------------------------
Restrict the balancer to one node and keep the volumes attached instead of cycling them:

```sh
./csi-stress-test-x86 --storage-class my-csi-sc \
  --spread balanced --node-selector kubernetes.io/hostname=worker-3 \
  --workload sleep --min-pvc 1 --max-pvc 1 --workers 80 \
  --hold-until-end --ramp 20m --duration 30m --startup-timeout 10m --results-file maxvols.jsonl
```

* `--hold-until-end` keeps every pod and its volumes until the test stops (reaching the end counts
  as success), so attachments accumulate. Workers whose cycle fails retry.
* `--ramp` starts the workers one by one over that duration, so the `attach_mount` by occupancy
  table shows the latency curve as the node fills, instead of a single attach storm.
* Use more `--workers × --max-pvc` than the expected limit. Once the CSINode limit is reached the
  remaining workers wait (`WARN no node has capacity`). If the driver publishes no limit, the real
  backend limit shows up as `startup` failures with `FailedAttachVolume`.
* The node table printed at the end shows `PEAK VOLS` per node.
