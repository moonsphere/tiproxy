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
	waitMetric(t, hub0API, "tiproxy_discovery_backends", 2, 300*time.Second)
	waitMetric(t, hub1API, "tiproxy_discovery_backends", 2, 60*time.Second)
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

	// S5: kill the hub the sidecar is subscribed to. The sidecar fails over
	// to the other hub and keeps receiving topology updates.
	t.Log("S5: hub failover")
	_, subscribed := subscribedHub(t)
	survivorAPI, _ := otherHub(subscribed)
	compose(t, "kill", subscribed)
	require.Eventually(t, func() bool {
		v, err := metricValue(survivorAPI, "tiproxy_discovery_subscribers")
		return err == nil && v > 0
	}, 60*time.Second, time.Second, "the sidecar did not fail over to the surviving hub")
	require.Contains(t, sidecarLogs(t), "reconnecting")
	compose(t, "--profile", "scale", "up", "-d", "tidb-2")
	waitFingerprint(t, 120*time.Second, "tidb-0", "tidb-1", "tidb-2")

	// S6: total hub outage. The sidecar keeps serving from its cached
	// topology and local health checks keep governing the cached entries:
	// a cached backend that dies is fenced, and comes back when it restarts.
	// Only a genuinely new backend (never pushed by a hub) stays invisible
	// until a hub returns.
	t.Log("S6: total hub outage")
	_, subscribed = subscribedHub(t)
	compose(t, "kill", subscribed)
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
	liveHub, _ := subscribedHub(t)
	waitMetric(t, liveHub, "tiproxy_discovery_backends", 3, 120*time.Second)
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
