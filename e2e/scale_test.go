// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	fleetSize = 30
	// loadWindow is the sampling window for the PD load comparison. The
	// pd-mode fleet issues at least one linearizable range per instance per
	// 3s health-check interval, so 60s yields >=600 ranges on top of the
	// cluster background, far above any noise.
	loadWindow = 60 * time.Second
)

// pdRangeTotal reads the cumulative etcd range-request counter from the PD
// embedded etcd. It prefers the plain mvcc counter and falls back to summing
// the gRPC handled counter for the Range method.
func pdRangeTotal() (float64, error) {
	resp, err := http.Get(pdAPI + "/metrics")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	sum, found := 0.0, false
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "etcd_debugging_mvcc_range_total ") {
			fields := strings.Fields(line)
			return strconv.ParseFloat(fields[len(fields)-1], 64)
		}
		if strings.HasPrefix(line, "grpc_server_handled_total{") && strings.Contains(line, `grpc_method="Range"`) {
			fields := strings.Fields(line)
			v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
			if err == nil {
				sum, found = sum+v, true
			}
		}
	}
	if !found {
		return 0, fmt.Errorf("no etcd range counter found on %s", pdAPI)
	}
	return sum, nil
}

// hubRequestsTotal sums the topology request counters over both hubs.
func hubRequestsTotal() (float64, error) {
	v0, err := metricSum(hub0API, "tidb_discovery_requests_total")
	if err != nil {
		return 0, err
	}
	v1, err := metricSum(hub1API, "tidb_discovery_requests_total")
	if err != nil {
		return 0, err
	}
	return v0 + v1, nil
}

// TestScaleFleetAndPDLoad covers the P2 scale scenarios:
//
//	S9: a 30-instance hub-sourced fleet all serve the right topology,
//	    with the polling load spread over the hubs.
//	S10: the PD etcd range load of the same fleet in pd mode vs hub mode
//	    (the point of the whole feature), sampled over equal windows.
//
// It needs ~6 minutes and ~30 extra containers, so it is opted in with
// E2E_SCALE=1 and skipped in the regular `make e2e` run.
func TestScaleFleetAndPDLoad(t *testing.T) {
	if os.Getenv("E2E_SCALE") == "" {
		t.Skip("set E2E_SCALE=1 to run the scale scenarios (S9/S10)")
	}

	composeUp(t)
	waitMetric(t, hub0API, "tidb_discovery_backends", 2, 90*time.Second)
	waitMetric(t, hub1API, "tidb_discovery_backends", 2, 90*time.Second)

	// --- S9: hub-sourced fleet ---
	compose(t, "--profile", "fleet-hub", "up", "-d", "--scale", fmt.Sprintf("sidecar-fleet=%d", fleetSize))
	waitCondition(t, 120*time.Second, func() bool {
		return runningContainers("sidecar-fleet") == fleetSize
	}, "fleet not fully running")

	// Spot-check a few fleet members: both backends healthy in their local
	// topology (the b_status gauge is 1 per healthy backend).
	for _, idx := range []int{1, fleetSize / 2, fleetSize} {
		container := fmt.Sprintf("%s-sidecar-fleet-%d", composeProject, idx)
		waitCondition(t, 60*time.Second, func() bool {
			out, err := dockerExec(container, "wget", "-qO-", "http://127.0.0.1:3080/metrics")
			if err != nil {
				return false
			}
			healthy := 0
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "tiproxy_backend_b_status{") && strings.HasSuffix(strings.TrimSpace(line), " 1") {
					healthy++
				}
			}
			return healthy == 2
		}, container+" does not see 2 healthy backends")
	}

	// The polling load reaches the hubs: >= fleetSize polls per 3s interval
	// across both hubs (the lone `sidecar` service adds a bit on top).
	r0, err := hubRequestsTotal()
	if err != nil {
		t.Fatal(err)
	}
	const rateWindow = 15 * time.Second
	time.Sleep(rateWindow)
	r1, err := hubRequestsTotal()
	if err != nil {
		t.Fatal(err)
	}
	polls := r1 - r0
	minPolls := float64(fleetSize) * rateWindow.Seconds() / 3 * 0.6 // 40% slack for startup jitter
	t.Logf("S9: %v hub polls in %v from %d instances (want >= %v)", polls, rateWindow, fleetSize+1, minPolls)
	if polls < minPolls {
		t.Fatalf("hub poll rate too low: got %v, want >= %v", polls, minPolls)
	}

	// --- S10: PD load, hub mode window ---
	h0, err := pdRangeTotal()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(loadWindow)
	h1, err := pdRangeTotal()
	if err != nil {
		t.Fatal(err)
	}
	deltaHub := h1 - h0

	// Swap the fleet to pd mode: same size, each instance with its own
	// etcd client polling PD directly.
	compose(t, "--profile", "fleet-hub", "stop", "sidecar-fleet")
	compose(t, "--profile", "fleet-pd", "up", "-d", "--scale", fmt.Sprintf("sidecar-pd-fleet=%d", fleetSize))
	waitCondition(t, 120*time.Second, func() bool {
		return runningContainers("sidecar-pd-fleet") == fleetSize
	}, "pd-mode fleet not fully running")
	// Let every instance finish its PD bootstrap before the window opens.
	time.Sleep(15 * time.Second)

	p0, err := pdRangeTotal()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(loadWindow)
	p1, err := pdRangeTotal()
	if err != nil {
		t.Fatal(err)
	}
	deltaPD := p1 - p0

	ratio := deltaPD / deltaHub
	t.Logf("S10: PD etcd ranges per %v window: pd mode = %v, hub mode = %v (background included), ratio = %.1fx",
		loadWindow, deltaPD, deltaHub, ratio)
	// The pd-mode fleet must add >= 1 range per instance per 3s on top of
	// the shared background; assert half of that as slack.
	minExtra := float64(fleetSize) * loadWindow.Seconds() / 3 * 0.5
	if deltaPD-deltaHub < minExtra {
		t.Fatalf("pd-mode fleet load not visible on PD: pd=%v hub=%v, want extra >= %v", deltaPD, deltaHub, minExtra)
	}
	if ratio < 2 {
		t.Fatalf("expected the pd-mode window to dominate, got ratio %.2f", ratio)
	}
}
