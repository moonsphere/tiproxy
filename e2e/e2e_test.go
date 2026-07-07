// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDiscoveryHub runs the P0 scenarios from docs/design/discovery-hub-e2e.md
// in order against one compose cluster: each scenario's final state is the
// next one's initial state.
//
//	S1 basic path → S2 scale-out → S3 graceful scale-in →
//	S5 hub failover → S6 total hub outage → S4 hard kill
func TestDiscoveryHub(t *testing.T) {
	composeUp(t)

	// S1: the sidecar routes to both TiDB instances through the hubs,
	// without any PD access. The generous timeout covers the TiKV/TiDB
	// bootstrap on a cold start.
	t.Log("S1: basic path")
	waitMetric(t, hub0API, "tidb_discovery_backends", 2, 300*time.Second)
	waitMetric(t, hub1API, "tidb_discovery_backends", 2, 60*time.Second)
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1")

	// S2: a new TiDB joins; the sidecar picks it up without a restart.
	t.Log("S2: scale-out")
	compose(t, "--profile", "scale", "up", "-d", "tidb-2")
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1", "tidb-2")

	// S3: graceful scale-in. TiDB cleans up its topology keys on shutdown,
	// so the removal propagates fast.
	t.Log("S3: graceful scale-in")
	compose(t, "--profile", "scale", "stop", "-t", "30", "tidb-2")
	waitFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")

	// S5: kill the hub the sidecar polls. The poller sticks to the first
	// configured address (hub-0) while it works, so killing it forces a
	// rotation to hub-1, and updates keep flowing.
	t.Log("S5: hub failover")
	compose(t, "kill", "hub-0")
	require.Eventually(t, func() bool {
		return strings.Contains(sidecarRawLogs(), "rotating to the next hub")
	}, 60*time.Second, time.Second, "the sidecar did not rotate to the surviving hub")
	compose(t, "--profile", "scale", "up", "-d", "tidb-2")
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1", "tidb-2")

	// S6: total hub outage. The sidecar keeps serving from its cached
	// topology and local health checks keep governing the cached entries:
	// a cached backend that dies is fenced, and comes back when it restarts.
	// Only a genuinely new backend (never pushed by a hub) stays invisible
	// until a hub returns.
	t.Log("S6: total hub outage")
	compose(t, "kill", "hub-1")
	assertContinuousSQL(t, 15*time.Second)

	// A cached backend dying during the outage is fenced by the local
	// health check even without any hub.
	compose(t, "--profile", "scale", "stop", "-t", "30", "tidb-2")
	waitFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")

	// The same cached backend restarting becomes routable again: the cache
	// still holds it and the health check recovers. This is by design —
	// known backends survive a full hub outage.
	compose(t, "--profile", "scale", "up", "-d", "tidb-2")
	waitFingerprint(t, 60*time.Second, "tidb-0", "tidb-1", "tidb-2")

	// A genuinely new backend stays invisible: no hub, no push.
	compose(t, "--profile", "scale", "up", "-d", "tidb-3")
	waitTiDBReady(t, "tidb-3", 90*time.Second)
	for i := 0; i < 3; i++ {
		hosts := fingerprint(t)
		_, ok := hosts["tidb-3"]
		require.False(t, ok, "tidb-3 must stay invisible while all hubs are down, got %v", hosts)
		time.Sleep(3 * time.Second)
	}

	// Hubs come back: a fresh full snapshot makes tidb-3 visible.
	compose(t, "start", "hub-0", "hub-1")
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1", "tidb-2", "tidb-3")

	// S4: hard-kill a TiDB. Two timelines: the local health check fences it
	// from new connections within seconds, while the topology converges
	// only after the etcd lease expires (~45s).
	t.Log("S4: hard kill")
	compose(t, "kill", "tidb-1")
	waitFingerprint(t, 30*time.Second, "tidb-0", "tidb-2", "tidb-3")
	waitMetric(t, hub0API, "tidb_discovery_backends", 3, 120*time.Second)
}

// waitTiDBReady waits until a TiDB container reports it is serving. The
// hub-outage scenario needs to know the backend is really up before asserting
// that it stays invisible to the sidecar.
func waitTiDBReady(t *testing.T, service string, timeout time.Duration) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		// The scale profile must be passed or compose does not resolve the
		// service; errors mean "not ready yet", never a test failure, since
		// this runs on the Eventually goroutine.
		out, err := composeOutput("--profile", "scale", "logs", "--no-log-prefix", service)
		return err == nil && strings.Contains(out, "server is running MySQL protocol")
	}, timeout, 2*time.Second, "%s did not become ready", service)
}

// TestHubRestartFullPush is S7: a topology change that happens while all hubs
// are down leaves a stale entry in the sidecar cache; the full snapshot on
// hub restart must replace the cache with no stale leftovers. The cache-level
// observation uses the per-backend metric series, which are purged only when
// a backend leaves the backend list (a stale-but-fenced backend still has
// them).
func TestHubRestartFullPush(t *testing.T) {
	composeUp(t)
	waitMetric(t, hub0API, "tidb_discovery_backends", 2, 300*time.Second)
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1")

	compose(t, "--profile", "scale", "up", "-d", "tidb-2")
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1", "tidb-2")

	// All hubs down, then the backend leaves gracefully: PD forgets it but
	// no hub can push the removal, so the sidecar cache goes stale and the
	// entry is only fenced by the health check.
	compose(t, "kill", "hub-0", "hub-1")
	compose(t, "--profile", "scale", "stop", "-t", "30", "tidb-2")
	waitFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")

	// The cache-level observable: as long as the stale entry is cached, the
	// observer keeps health-checking it and the router logs one "unhealthy
	// backend is not in router" tick per cycle (the backend left the router
	// long ago — it is unhealthy and connectionless). The eviction by the
	// full snapshot is observed as the CESSATION of these ticks. (Two
	// observables that do not work: the per-backend metric series are only
	// GCed after hours of retention, and the router's list-removal log only
	// fires for backends the router still tracks.)
	require.Eventually(t, func() bool {
		return staleTickCount() > 0
	}, 30*time.Second, time.Second, "the stale backend must keep being health-checked while all hubs are down")

	// Hubs return and bootstrap from PD, which no longer has tidb-2: the
	// full snapshot replaces the cache and the ticks stop.
	compose(t, "start", "hub-0", "hub-1")
	waitMetric(t, hub0API, "tidb_discovery_backends", 2, 120*time.Second)

	converged := false
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		before := staleTickCount()
		time.Sleep(8 * time.Second) // > 2 health-check cycles
		if staleTickCount() == before {
			converged = true
			break
		}
	}
	require.True(t, converged, "the stale backend must be evicted from the cache after the full snapshot")
	waitFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")
}

// staleTickCount counts the health-check ticks for the stale backend in the
// sidecar log. It never fails the test, so it is safe inside Eventually.
func staleTickCount() int {
	logs := sidecarRawLogs()
	count := 0
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "unhealthy backend is not in router") && strings.Contains(line, "tidb-2:4000") {
			count++
		}
	}
	return count
}

// TestPdHubMigration is S8: an instance running in the traditional pd mode is
// switched to the hub mode and back through the online config API. An
// existing client connection must not see a single error across both
// switches, and the routing result must stay the same.
func TestPdHubMigration(t *testing.T) {
	composeUp(t, "--profile", "canary")
	waitMetric(t, hub0API, "tidb_discovery_backends", 2, 300*time.Second)

	// The canary starts in the pd mode and routes to both backends.
	waitCanaryFingerprint(t, 120*time.Second, "tidb-0", "tidb-1")

	// The config API merges the payload over the current config, so the
	// backend-clusters array cannot be dropped by omission: the switch back
	// must explicitly override the entry with a pd-sourced one.
	pdMode := `
[proxy]
addr = "0.0.0.0:6000"
pd-addrs = "pd:2379"
[[proxy.backend-clusters]]
name = "default"
discovery-source = "pd"
pd-addrs = "pd:2379"
[api]
addr = "0.0.0.0:3080"
`
	hubMode := `
[proxy]
addr = "0.0.0.0:6000"
pd-addrs = ""
[[proxy.backend-clusters]]
name = "default"
discovery-source = "hub"
hub-addrs = "hub-0:3080,hub-1:3080"
[api]
addr = "0.0.0.0:3080"
`
	// The cluster manager logs one "updated backend cluster" per rebuild:
	// the counter increments prove that each PUT really switched the source.
	rebuilds := serviceLogCount("sidecar-pd", "updated backend cluster")

	// pd -> hub with a connection held open across the switch.
	stop := holdConnection(t, canaryDSN)
	putConfig(t, canaryAPI, hubMode)
	require.Eventually(t, func() bool {
		return serviceLogCount("sidecar-pd", "updated backend cluster") == rebuilds+1
	}, 60*time.Second, time.Second, "the canary did not rebuild the cluster after the switch")
	waitCanaryFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")
	require.Empty(t, stop(), "the held connection must survive the pd->hub switch")

	// hub -> pd, same contract.
	stop = holdConnection(t, canaryDSN)
	putConfig(t, canaryAPI, pdMode)
	require.Eventually(t, func() bool {
		return serviceLogCount("sidecar-pd", "updated backend cluster") == rebuilds+2
	}, 60*time.Second, time.Second, "the canary did not rebuild the cluster after switching back")
	waitCanaryFingerprint(t, 60*time.Second, "tidb-0", "tidb-1")
	require.Empty(t, stop(), "the held connection must survive the hub->pd switch")
}

// waitCanaryFingerprint is waitFingerprint against the canary instance.
func waitCanaryFingerprint(t *testing.T, timeout time.Duration, want ...string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[w] = struct{}{}
	}
	var last map[string]struct{}
	require.Eventuallyf(t, func() bool {
		last = fingerprintDSN(canaryDSN)
		if len(last) != len(wantSet) {
			return false
		}
		for h := range wantSet {
			if _, ok := last[h]; !ok {
				return false
			}
		}
		return true
	}, timeout, 2*time.Second, "want backends %v on the canary, last seen %v", want, last)
}
