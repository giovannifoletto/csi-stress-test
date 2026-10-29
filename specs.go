package main

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ioScript writes a random file to every volume, fsyncs it, reads it back (O_DIRECT when the
// filesystem supports it, so the page cache is bypassed) and verifies the checksum. Each
// iteration prints one "IO ..." line that the tester parses for throughput. Any I/O or
// checksum error exits non-zero, which turns the pod Failed and the cycle into a failure.
const ioScript = `
set -eu
MIB=__IO_MIB__
BYTES=$((MIB * 1048576))
dd if=/dev/urandom of=/tmp/src bs=1M count=$MIB status=none
set -- $(md5sum /tmp/src)
SUM=$1

DIRECT=0
for dir in /data/*; do
  if dd if=/tmp/src of="$dir/.probe" bs=1M count=1 conv=fsync status=none &&
     dd if="$dir/.probe" of=/dev/null bs=1M iflag=direct status=none 2>/dev/null; then
    DIRECT=1
  fi
  rm -f "$dir/.probe"
  break
done

while true; do
  for dir in /data/*; do
    [ -d "$dir" ] || continue
    f="$dir/testfile"
    t0=$(date +%s%N)
    dd if=/tmp/src of="$f" bs=1M conv=fsync status=none
    t1=$(date +%s%N)
    if [ "$DIRECT" = 1 ]; then
      set -- $(dd if="$f" bs=1M iflag=direct status=none | md5sum)
    else
      set -- $(dd if="$f" bs=1M status=none | md5sum)
    fi
    t2=$(date +%s%N)
    if [ "$1" != "$SUM" ]; then
      echo "IO_ERROR checksum mismatch on $f" >&2
      exit 1
    fi
    rm -f "$f"
    echo "IO vol=${dir##*/} bytes=$BYTES write_ns=$((t1 - t0)) read_ns=$((t2 - t1)) direct=$DIRECT"
  done
  sleep 1
done
`

// podScript returns the shell script the pod runs for the configured --workload. The pod must
// keep running for the whole hold (leaving Running fails the cycle), so a --command that
// completes successfully is followed by an idle sleep, while a non-zero exit fails the pod.
func podScript(cfg Config) string {
	switch cfg.Workload {
	case workloadSleep:
		return "exec sleep infinity\n"
	case workloadCommand:
		return "set -e\n" + cfg.Command + "\nexec sleep infinity\n"
	default:
		return strings.ReplaceAll(ioScript, "__IO_MIB__", strconv.Itoa(cfg.IOSizeMiB))
	}
}

func buildPVCSpec(cfg Config, name, cycleID string) *corev1.PersistentVolumeClaim {
	var scName *string
	if cfg.StorageClass != "" {
		scName = &cfg.StorageClass
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				trackingLabelKey: trackingLabelVal,
				cycleLabelKey:    cycleID,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: cfg.volumeQuantity,
				},
			},
		},
	}
}

// buildPodSpec builds the I/O pod. With a non-empty node the pod is pinned to it through
// required node affinity; the scheduler still runs, which WaitForFirstConsumer provisioning
// needs (setting spec.nodeName directly would leave such PVCs Pending forever).
func buildPodSpec(cfg Config, namespace, podName, cycleID string, pvcNames []string, node string) *corev1.Pod {
	volumes := make([]corev1.Volume, len(pvcNames))
	mounts := make([]corev1.VolumeMount, len(pvcNames))

	for i, pvc := range pvcNames {
		volName := fmt.Sprintf("vol%d", i)
		volumes[i] = corev1.Volume{
			Name: volName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc,
				},
			},
		}
		mounts[i] = corev1.VolumeMount{
			Name:      volName,
			MountPath: "/data/" + volName,
		}
	}

	var affinity *corev1.Affinity
	switch {
	case node != "":
		affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchFields: []corev1.NodeSelectorRequirement{{
							Key:      "metadata.name",
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{node},
						}},
					}},
				},
			},
		}
	case cfg.Spread == spreadSoft:
		// Soft pod anti-affinity to spread test pods across nodes
		affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{
					{
						Weight: 100,
						PodAffinityTerm: corev1.PodAffinityTerm{
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{trackingLabelKey: trackingLabelVal},
							},
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{managedByKey: managedByVal},
							},
							TopologyKey: "kubernetes.io/hostname",
						},
					},
				},
			},
		}
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				trackingLabelKey: trackingLabelVal,
				cycleLabelKey:    cycleID,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			// Compliant with the "restricted" Pod Security Standard. fsGroup makes the volumes
			// writable by the non-root user (requires a CSIDriver fsGroupPolicy other than None);
			// OnRootMismatch avoids a recursive chown on every mount inflating attach_mount.
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:        ptr(true),
				RunAsUser:           ptr(cfg.RunAsUser),
				RunAsGroup:          ptr(cfg.RunAsUser),
				FSGroup:             ptr(cfg.RunAsUser),
				FSGroupChangePolicy: ptr(corev1.FSGroupChangeOnRootMismatch),
				SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			Affinity: affinity,
			Containers: []corev1.Container{
				{
					Name:    "io-runner",
					Image:   cfg.Image,
					Command: []string{"/bin/sh", "-c", podScript(cfg)},
					// Avoid a registry round-trip per pod skewing attach_mount latency.
					ImagePullPolicy: corev1.PullIfNotPresent,
					VolumeMounts:    mounts,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("20m"),
							corev1.ResourceMemory: resource.MustParse("32Mi"),
						},
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: new(bool),
						Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					},
				},
			},
			Volumes: volumes,
		},
	}
}

// parseIOSamples aggregates the "IO vol=... bytes=... write_ns=... read_ns=... direct=..." lines per volume.
func parseIOSamples(logs string) []ioSample {
	type acc struct {
		iters           int
		bytes, wNs, rNs int64
		direct          bool
	}
	per := map[string]*acc{}
	var order []string

	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(line, "IO ") {
			continue
		}
		kv := map[string]string{}
		for _, f := range strings.Fields(line)[1:] {
			if k, v, ok := strings.Cut(f, "="); ok {
				kv[k] = v
			}
		}
		b, err1 := strconv.ParseInt(kv["bytes"], 10, 64)
		w, err2 := strconv.ParseInt(kv["write_ns"], 10, 64)
		r, err3 := strconv.ParseInt(kv["read_ns"], 10, 64)
		if err1 != nil || err2 != nil || err3 != nil || kv["vol"] == "" {
			continue
		}
		a, ok := per[kv["vol"]]
		if !ok {
			a = &acc{}
			per[kv["vol"]] = a
			order = append(order, kv["vol"])
		}
		a.iters++
		a.bytes += b
		a.wNs += w
		a.rNs += r
		a.direct = kv["direct"] == "1"
	}

	out := make([]ioSample, 0, len(order))
	for _, vol := range order {
		a := per[vol]
		mib := float64(a.bytes) / (1 << 20)
		s := ioSample{Volume: vol, Iterations: a.iters, DirectRead: a.direct}
		if a.wNs > 0 {
			s.WriteMiBps = mib / (float64(a.wNs) / 1e9)
		}
		if a.rNs > 0 {
			s.ReadMiBps = mib / (float64(a.rNs) / 1e9)
		}
		out = append(out, s)
	}
	return out
}

func ptr[T any](v T) *T { return &v }
