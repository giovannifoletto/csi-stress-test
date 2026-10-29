CSI stress tester
=================

Continuously creates PVCs and pods that mount them, runs verified I/O on the volumes, then tears
everything down, measuring each step of the CSI lifecycle (provisioning, attach, mount, unmount,
detach, delete). The same tool covers short burst tests, per-node capacity tests and multi-day
soak tests, depending on the flags.

* [How to build](#how-to-build)
* [How to run](#how-to-run) (locally or in-cluster, required RBAC)
* [How a cycle works](#how-a-cycle-works)
* [Measured metrics](#measured-metrics)
* [Output](#output)
* [Test catalogue](#test-catalogue)
* [Flag reference](#flag-reference)
* [Troubleshooting](#troubleshooting)

How to build
------------
```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o csi-stress-test-x86 .
go test ./...
```

How to run
----------
```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workers 50 --duration 30m --results-file results.jsonl
```

Run `--help` for all flags (also listed in the [flag reference](#flag-reference)). Ctrl+C (or
SIGTERM) stops the test gracefully: in-flight cycles are torn down and the test namespaces deleted.
A second Ctrl+C exits immediately and leaves resources behind.

### Safety

The tool only uses and deletes namespaces labelled `app.kubernetes.io/managed-by=csi-stress-test`.
It refuses to start if a namespace matching `--namespace-prefix` (`<prefix>-1` ... `<prefix>-N`)
already exists without that label, or is still terminating from a previous run. The namespaces are
created with `pod-security.kubernetes.io/enforce=restricted`, and the test pod complies with it
(non-root, no privilege escalation, all capabilities dropped, RuntimeDefault seccomp).

### Before running

* Pre-pull `--image` on the nodes (or run a short warm-up) so image pulls do not inflate the first
  `attach_mount` samples on each node. The pod uses `imagePullPolicy: IfNotPresent`.
* Prefer a StorageClass with `volumeBindingMode: WaitForFirstConsumer` and `reclaimPolicy: Delete`.
  With `Retain`, PVs accumulate and `pv_release` is not measured (the tool warns at start).
* The pod writes as UID/GID `--run-as-user` (default 1000) and relies on `fsGroup` to make the
  volumes writable, so the CSIDriver `fsGroupPolicy` must not be `None`.

### Running in-cluster

For long tests run the tool inside the cluster, so a laptop going to sleep, a VPN drop or an
expiring kubeconfig token does not end the run. Without a kubeconfig file the tool uses the
in-cluster ServiceAccount.

```dockerfile
FROM scratch
COPY csi-stress-test-x86 /csi-stress-test
ENTRYPOINT ["/csi-stress-test"]
```

RBAC needed by the tool (also usable for a local run with a restricted user):

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: csi-stress-test
rules:
  - apiGroups: [""]
    resources: [namespaces]
    verbs: [get, create, delete]
  - apiGroups: [""]
    resources: [pods, persistentvolumeclaims]
    verbs: [get, list, watch, create, delete, deletecollection]
  - apiGroups: [""]
    resources: [pods/log]
    verbs: [get]
  - apiGroups: [""]
    resources: [events]
    verbs: [list]
  - apiGroups: [""]
    resources: [persistentvolumes]
    verbs: [get, list, watch]
  - apiGroups: [""]
    resources: [nodes]
    verbs: [list]
  - apiGroups: [storage.k8s.io]
    resources: [storageclasses]
    verbs: [get, list]
  - apiGroups: [storage.k8s.io]
    resources: [csinodes, volumeattachments]
    verbs: [list]
```

Example Job (bind the ClusterRole to the `csi-stress-test` ServiceAccount with a
ClusterRoleBinding). Keep the results on a volume of a **different** StorageClass than the one under
test, so a misbehaving driver cannot lose them:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: csi-soak
  namespace: storage-test
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: csi-stress-test
      restartPolicy: Never
      terminationGracePeriodSeconds: 900   # time for the graceful teardown
      containers:
        - name: tester
          image: registry.example.com/csi-stress-test:latest
          args: [--storage-class=my-csi-sc, --workers=20, --duration=72h,
                 --min-hold=5m, --max-hold=30m, --io-size=64, --report-interval=5m,
                 --results-file=/results/soak.jsonl]
          volumeMounts:
            - {name: results, mountPath: /results}
      volumes:
        - name: results
          persistentVolumeClaim: {claimName: csi-soak-results}
```

Follow it with `kubectl logs -f job/csi-soak -n storage-test`.

How a cycle works
-----------------
Each of the `--workers` runs cycles in a loop, each in a random test namespace:

1. Pick a node (with `--spread=balanced`) and reserve the cycle's volumes on it.
2. Create `--min-pvc`..`--max-pvc` PVCs of `--volume-size`.
3. Create a pod mounting them at `/data/vol0`, `/data/vol1`, ... and wait until it is Running.
4. Hold for a random time between `--min-hold` and `--max-hold` (or until the end of the test with
   `--hold-until-end`) while the pod runs `--workload`. The cycle fails as soon as the pod stops
   running (I/O error, checksum mismatch, failing `--command`).
5. Delete the pod and wait until it is gone (unmount), then delete the PVCs and wait for the PVCs
   and their PVs to disappear (detach + DeleteVolume).

A cycle fails if a step times out (`--startup-timeout`, `--teardown-timeout`), the pod stops running
during the hold, or a volume is not released. The failure line includes the pod conditions and the
latest warning events (FailedAttachVolume, FailedMount, ProvisioningFailed, ...).

### Workloads (`--workload`)

* `io` (default): writes a random `--io-size` MiB file to every volume with fsync, reads it back
  with O_DIRECT when the filesystem supports it, verifies the checksum, and repeats every second.
* `sleep`: the pod idles. Use it to test only the attach/mount/detach path without I/O load.
* `command`: runs `--command` with `sh -e`, then idles. A non-zero exit fails the cycle, so it can
  run any check or benchmark (fio, fsck, a database smoke test) shipped in `--image`.

### Pod placement (`--spread`)

* `balanced` (default): every pod is pinned, via required node affinity, to the eligible node with
  the fewest in-flight test pods, so attachments are spread evenly across all hosts. Nodes that are
  cordoned, NotReady, tainted NoSchedule/NoExecute, outside `--node-selector`, or (for CSI drivers)
  without the driver registered in CSINode are skipped. The node list refreshes every 30s, so
  cordoning or draining a node during a test removes it from the rotation.
  In-flight test volumes, plus the volumes other workloads attached to the node through the same
  driver (VolumeAttachments of non-test PVs), are kept under each node's CSINode attach limit.
  `--ignore-attach-limit` removes that cap; the scheduler still enforces the CSINode limit, so the
  extra pods stay Pending and fail at `startup`.
* `soft`: preferred pod anti-affinity across all test namespaces; the scheduler may still stack pods.
* `none`: the scheduler decides.

`--node-selector` and `--ignore-attach-limit` only apply to `balanced`, and are rejected otherwise.
With `volumeBindingMode: Immediate` and a topology-constrained driver, the volume can be created in
a zone the pinned node cannot reach. Prefer `WaitForFirstConsumer` with `balanced`.

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

Buffered reads (filesystems without O_DIRECT support) only measure the page cache and are not
reported in `io_read`.

Output
------
### Progress line

Every `--report-interval`:

```
[14:03:30] ok=412 failed=3 aborted=0 in-flight=50 pods/124 vols | last 30s: ok=21 failed=0 | pvc_bind p50=4.10s p95=7.92s | attach_mount p50=6.31s p95=11.20s | ...
```

The totals count since the start; the percentiles cover only the cycles completed in the last
interval, so a slowdown late in a long run is not averaged away.

### Failure line

```
[14:05:12] FAIL csi-stress-3/w17-9c1e22a0 node=worker-4 phase=startup: pod pod-w17-9c1e22a0 did not reach Running: ... [phase=Pending; event FailedAttachVolume: ...]
```

Failure phases: `pvc_create`, `pod_create`, `startup` (bind, schedule, attach, mount), `io`
(pod stopped during the hold), `teardown` (unmount, detach or delete stuck).

### Leak audit

Every `--audit-interval` (default 10m, 0 disables) the tool counts objects that should not
accumulate, and compares them with the baseline taken at start (growth is marked `(+N)` and
`WARN above baseline`):

```
[15:00:00] AUDIT orphan_va=0 va_errors=1(+1) pv_released=0 pv_failed=0 pv_stuck_deleting=0 test_pvs=118 WARN above baseline
```

| Counter             | Meaning                                                              |
|---------------------|----------------------------------------------------------------------|
| `orphan_va`         | VolumeAttachments of the driver whose PV or node no longer exists    |
| `va_errors`         | VolumeAttachments of the driver with an attach or detach error       |
| `pv_released`       | PVs of the StorageClass in phase Released                            |
| `pv_failed`         | PVs of the StorageClass in phase Failed (e.g. DeleteVolume failed)   |
| `pv_stuck_deleting` | PVs of the StorageClass deleted more than `--teardown-timeout` ago   |
| `test_pvs`          | PVs of the test namespaces (informational: varies with the load)     |

`va_errors` can blip during normal operation (a retried attach); a counter that keeps growing over
several audits is a leak. After the final cleanup the tool waits up to 2 minutes for the PVs of the
test namespaces to disappear and reports any left behind. The audit only sees Kubernetes objects:
also check the storage backend for volumes or exports that outlived their PV.

### Final summary

At the end the tool prints:

* successful, failed and aborted cycles, and failures by phase;
* min/p50/p95/p99/max for every metric;
* with `--spread=balanced`, the peak number of volumes seen on a single node and `attach_mount`
  grouped by how many volumes the node already had when the pod was assigned (to see attach
  latency grow as a node fills up);
* per-node OK/failed counts and `attach_mount` percentiles (to spot a bad node);
* with `--spread=balanced`, the node table: attach limit, other workloads' volumes, assigned pods,
  in-flight pods/volumes and `PEAK VOLS`.

### Results file and `report`

`--results-file` writes one JSON record per cycle: id, namespace, node, PVC count, `node_vols`
(node occupancy at assignment, balanced only), start time, status, failed phase, error, all
latencies and the per-volume I/O samples.

The `report` subcommand rebuilds the summary from one or more results files, without cluster
access. `--bucket` adds one row per time window (empty windows included) with OK/failed counts and
p50/p95 of the main metrics:

```sh
./csi-stress-test-x86 report results.jsonl
./csi-stress-test-x86 report runs/*.jsonl                 # aggregate several runs
./csi-stress-test-x86 report --bucket 1h soak.jsonl       # drift over time
```

Elapsed is then the span between the first and the last cycle start across all files.

Test catalogue
--------------
All examples use `--storage-class my-csi-sc`; add `--results-file` to keep the per-cycle data. Size
`--workers` to the cluster: in-flight volumes ≈ workers × average PVCs per pod.

### 1. Smoke test

Validate the setup (StorageClass, RBAC, image, fsGroup) before a long run.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workers 5 --duration 5m
```

Expect zero failures and `io_write`/`io_read` samples. `Successful cycles without a completed I/O
iteration` means the hold is too short for the storage: raise `--min-hold` or lower `--io-size`.

### 2. Lifecycle stress (default)

The full lifecycle with I/O, spread evenly over all nodes.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workers 50 --duration 1h \
  --results-file lifecycle.jsonl
```

### 3. Attach/detach churn

Maximise ControllerPublish/NodeStage/Unstage/Unpublish operations per second: short holds, no I/O.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workload sleep \
  --workers 100 --min-pvc 1 --max-pvc 4 --min-hold 1s --max-hold 5s \
  --duration 1h --results-file churn.jsonl
```

Watch `attach_mount`, `pod_delete` and `pv_release` p95, and `startup`/`teardown` failures. Raise
`--workers` step by step to find where latency degrades or the driver starts failing.

### 4. Provisioning burst

Many volumes created at once, to test CreateVolume/DeleteVolume throughput and API rate limits.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workload sleep \
  --workers 200 --min-pvc 1 --max-pvc 1 --min-hold 30s --max-hold 30s \
  --startup-timeout 10m --duration 20m --results-file burst.jsonl
```

With `Immediate` binding, `pvc_bind` is pure provisioning time; with `WaitForFirstConsumer` it is
included in `pod_schedule`.

### 5. Data integrity and throughput

Fewer, larger volumes with long holds and more data per iteration.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workload io \
  --volume-size 10Gi --io-size 512 --workers 10 --min-pvc 1 --max-pvc 2 \
  --min-hold 10m --max-hold 20m --duration 2h --results-file io.jsonl
```

Any checksum mismatch fails the cycle with `phase=io` and `IO_ERROR checksum mismatch` in the log
tail. `io_write`/`io_read` are per-volume sequential throughputs under concurrent load, not a
replacement for a dedicated benchmark (see test 11).

### 6. Per-node comparison

Same load on every node of a pool, to find a node with a slow or broken path (multipath, HBA,
iSCSI session, kernel module).

```sh
./csi-stress-test-x86 --storage-class my-csi-sc \
  --node-selector node-role.kubernetes.io/worker --workers 60 --duration 1h
```

Compare the per-node table: an outlier in `ATTACH_MOUNT P95` or `FAILED` points at the node. Run
again with `--spread none` to compare with the scheduler's own placement.

### 7. Maximum volumes on a single node

Fill one node with attachments, one at a time, and measure how attach latency evolves.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc \
  --spread balanced --node-selector kubernetes.io/hostname=worker-3 \
  --workload sleep --min-pvc 1 --max-pvc 1 --workers 80 \
  --hold-until-end --ramp 20m --duration 30m --startup-timeout 10m \
  --results-file maxvols.jsonl
```

* Choose a node with no other workloads using the driver (they count towards the limit, shown as
  `OTHER VOLS`).
* `--hold-until-end` keeps every pod and its volumes until the test stops (reaching the end counts
  as success), so attachments accumulate. Workers whose cycle fails retry.
* `--ramp` starts the workers one by one over that duration, so the "attach_mount by volumes already
  on the node" table shows the latency curve instead of a single attach storm. Without it you test
  a concurrent attach storm to one node, which is also worth a run.
* Use more `--workers × --max-pvc` than the expected limit, and a `--duration` longer than `--ramp`.
  Once the CSINode limit is reached the remaining workers wait (`WARN no node has capacity`). If the
  driver publishes no limit (`ATTACH LIMIT -`), the real backend limit shows up as `startup`
  failures with `FailedAttachVolume` or `FailedMount`.
* `PEAK VOLS` in the final node table and "Peak volumes on a single node" give the result.

To test the whole pool at once, drop `--node-selector` and use workers ≥ nodes × limit.

### 8. Attach limit enforcement

Check that the scheduler honours the CSINode limit the driver advertises.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc \
  --node-selector kubernetes.io/hostname=worker-3 --ignore-attach-limit \
  --workload sleep --min-pvc 1 --max-pvc 1 --workers 80 \
  --hold-until-end --ramp 10m --duration 20m --startup-timeout 2m
```

Expected: pods beyond the limit fail at `startup` with a scheduling message about the maximum volume
count, and `PEAK VOLS` exceeds the limit only by the pods that never got scheduled. Pods reaching
Running above the limit mean the limit is not enforced (or not advertised).

### 9. Soak (steady load)

Moderate, long-lived load to find slow degradation and leaks. Run it in-cluster.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workers 20 --duration 72h \
  --min-hold 5m --max-hold 30m --io-size 64 --report-interval 5m \
  --audit-interval 10m --results-file soak.jsonl
./csi-stress-test-x86 report --bucket 1h soak.jsonl
```

Look for: p95 drifting up across windows, throughput decaying, bursts of failures at regular times
(backups, certificate rotation, garbage collection on the array), AUDIT counters above the
baseline. Outside the tool, watch the CSI controller and node plugin pods for restarts and memory
growth, and the kubelet memory, during the run.

### 10. Soak (churn)

The long-running counterpart of test 3, for leaks in the attach/detach path (VolumeAttachments,
stale mounts, multipath devices left on the nodes).

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --workload sleep --workers 50 \
  --min-pvc 1 --max-pvc 3 --min-hold 10s --max-hold 60s \
  --duration 24h --report-interval 5m --results-file soak-churn.jsonl
```

After the run, also check the nodes for leftovers (`findmnt | grep kubernetes.io/csi`,
`multipath -ll`, `lsblk`).

### 11. Custom workload

Run any command against the mounted volumes. The image must contain it and run as a non-root user.
The hold must be longer than the command, and a non-zero exit fails the cycle.

```sh
./csi-stress-test-x86 --storage-class my-csi-sc --image <image-with-fio> \
  --workload command --min-pvc 1 --max-pvc 1 --volume-size 5Gi \
  --command 'fio --name=rand --directory=/data/vol0 --size=1G --rw=randrw --bs=4k --direct=1 --runtime=120 --time_based' \
  --min-hold 3m --max-hold 3m --workers 10 --duration 30m
```

Pod logs are not parsed for custom workloads; read the fio output with `kubectl logs` during the
hold, or have the command fail on a threshold.

### 12. Resilience (disruption during load)

Run test 3 or 9 and, during the run, disrupt the storage stack one action at a time, leaving a few
`--report-interval`s between actions:

* delete the CSI controller pod (leader election, sidecar restart);
* delete the CSI node plugin pod on one node;
* cordon and drain a node (the balancer stops using it within 30s);
* reboot a node, or fail over the storage backend / a network path.

Expected: failures limited to the cycles in flight on the affected component, latency back to the
baseline within a few windows (`report --bucket 5m`), and no AUDIT counter left above the baseline
at the end.

Flag reference
--------------
| Flag                    | Default                  | Description |
|-------------------------|--------------------------|-------------|
| `--storage-class`       | cluster default          | StorageClass used for the PVCs |
| `--volume-size`         | `1Gi`                    | Size of each PVC |
| `--workers`             | `50`                     | Concurrent cycles |
| `--duration`            | `0` (until Ctrl+C)       | Test duration |
| `--min-pvc` / `--max-pvc` | `2` / `3`              | PVCs per pod (random in range) |
| `--min-hold` / `--max-hold` | `10s` / `45s`        | Time the pod runs before teardown (random in range) |
| `--hold-until-end`      | `false`                  | Keep pods and volumes until the test stops (ignores the hold range) |
| `--ramp`                | `0`                      | Start workers evenly over this duration |
| `--workload`            | `io`                     | `io`, `sleep` or `command` |
| `--command`             |                          | Shell command for `--workload=command` |
| `--io-size`             | `10`                     | MiB written and read per volume per I/O iteration |
| `--image`               | `ubi9/ubi-minimal`       | Pod image (needs `sh`, `dd`, `md5sum`, `date` for `io`) |
| `--run-as-user`         | `1000`                   | Pod UID, GID and fsGroup |
| `--spread`              | `balanced`               | `balanced`, `soft` or `none` |
| `--node-selector`       |                          | Label selector for the nodes used by `balanced` |
| `--ignore-attach-limit` | `false`                  | Do not cap volumes per node at the CSINode limit (`balanced`) |
| `--startup-timeout`     | `3m`                     | Max time from PVC creation to pod Running |
| `--teardown-timeout`    | `5m`                     | Max time for pod, PVCs and PVs to be deleted; also the `pv_stuck_deleting` threshold |
| `--report-interval`     | `30s`                    | Progress line interval (and percentile window) |
| `--audit-interval`      | `10m`                    | Leak audit interval (0 disables) |
| `--results-file`        |                          | JSON Lines file with one record per cycle |
| `--namespace-prefix`    | `csi-stress`             | Test namespaces are `<prefix>-1` ... `<prefix>-N` |
| `--namespace-count`     | `5`                      | Number of test namespaces |
| `--cleanup`             | `true`                   | Delete leftovers and the test namespaces at the end |
| `--kubeconfig`          | `~/.kube/config`         | Kubeconfig; falls back to in-cluster config if missing |

`report` flags: `--bucket <duration>` (per-window table, default off).

Troubleshooting
---------------
| Symptom | Likely cause |
|---------|--------------|
| Refuses to start: namespace exists and is not labelled | Choose another `--namespace-prefix`; the tool never adopts namespaces it did not create |
| Refuses to start: namespace still terminating | A previous run is still cleaning up; wait or check stuck finalizers |
| `no eligible nodes` | `--node-selector` matches nothing, or every node is cordoned/tainted/NotReady, or the driver is not registered in CSINode |
| `WARN no node has capacity` | All eligible nodes are at their attach limit; expected in test 7, otherwise lower `--workers` |
| `phase=io` with `Permission denied` | fsGroup not applied: CSIDriver `fsGroupPolicy: None`, or the filesystem ignores ownership |
| `phase=startup` with `FailedScheduling` | Topology mismatch (Immediate binding with `balanced`), attach limit, or insufficient resources |
| `phase=teardown`, `PV not released` | Detach or DeleteVolume stuck; check the attacher and provisioner sidecar logs and the VolumeAttachment |
| `WARN cannot count other workloads' attachments` | No RBAC to list VolumeAttachments/PVs; the balancer keeps the last known counts |
| `WARN leak audit disabled` | No RBAC to list PVs, nodes or VolumeAttachments |
