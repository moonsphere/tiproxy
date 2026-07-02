// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

//go:build e2e

// Package e2e contains end-to-end tests for the discovery hub, driving a real
// PD/TiKV/TiDB cluster plus hubs and a sidecar through docker compose.
// See docs/design/discovery-hub-e2e.md.
package e2e

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

const (
	composeProject = "tiproxy-e2e"
	sidecarDSN     = "root@tcp(127.0.0.1:46000)/test"
	sidecarAPI     = "http://127.0.0.1:43080"
	hub0API        = "http://127.0.0.1:43081"
	hub1API        = "http://127.0.0.1:43082"

	// fingerprintSamples is how many connections one fingerprint round opens.
	// Routing is score-based, not round-robin, so use enough samples to hit
	// every healthy backend with overwhelming probability.
	fingerprintSamples = 30
)

// composeOutput runs a docker compose command and returns its combined
// output without failing the test, so it is safe inside require.Eventually
// conditions (require.FailNow must not run on a non-test goroutine).
func composeOutput(args ...string) (string, error) {
	all := append([]string{"compose", "-p", composeProject}, args...)
	cmd := exec.Command("docker", all...)
	cmd.Dir = "."
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// compose runs a docker compose command in the e2e directory.
func compose(t *testing.T, args ...string) string {
	t.Helper()
	out, err := composeOutput(args...)
	require.NoError(t, err, "docker compose %s failed:\n%s", strings.Join(args, " "), out)
	return out
}

func composeUp(t *testing.T) {
	t.Helper()
	compose(t, "up", "-d")
	t.Cleanup(func() {
		if os.Getenv("E2E_KEEP_CLUSTER") != "" {
			t.Log("E2E_KEEP_CLUSTER is set, leaving the compose cluster running")
			return
		}
		compose(t, "down", "-v", "--remove-orphans")
	})
}

// fingerprint opens connections through the sidecar and returns the set of
// backend hostnames that served them. The connections are held open while
// more are opened: routing is score-based (connection counts), so sequential
// open-query-close would keep landing on the same backend, while concurrent
// connections spread over all healthy backends. Every scenario's final
// assertion goes through here: it is the client's view of the routing table.
func fingerprint(t *testing.T) map[string]struct{} {
	t.Helper()
	hosts := make(map[string]struct{})
	dbs := make([]*sql.DB, 0, fingerprintSamples)
	defer func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}()
	for i := 0; i < fingerprintSamples; i++ {
		db, err := sql.Open("mysql", sidecarDSN+"?timeout=3s&readTimeout=3s")
		if err != nil {
			continue
		}
		dbs = append(dbs, db)
		db.SetMaxOpenConns(1)
		var host string
		// Connection-level errors surface as an empty/partial set and fail
		// the caller's assertion with retries.
		if err := db.QueryRow("SELECT @@hostname").Scan(&host); err == nil {
			hosts[host] = struct{}{}
		}
	}
	return hosts
}

func queryHostname() (string, error) {
	db, err := sql.Open("mysql", sidecarDSN+"?timeout=3s&readTimeout=3s")
	if err != nil {
		return "", err
	}
	defer func() {
		_ = db.Close()
	}()
	db.SetMaxOpenConns(1)
	var host string
	err = db.QueryRow("SELECT @@hostname").Scan(&host)
	return host, err
}

// waitFingerprint waits until the set of backends serving new connections is
// exactly `want`.
func waitFingerprint(t *testing.T, timeout time.Duration, want ...string) {
	t.Helper()
	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[w] = struct{}{}
	}
	var last map[string]struct{}
	require.Eventuallyf(t, func() bool {
		last = fingerprint(t)
		if len(last) != len(wantSet) {
			return false
		}
		for h := range wantSet {
			if _, ok := last[h]; !ok {
				return false
			}
		}
		return true
	}, timeout, 2*time.Second, "want backends %v, last seen %v", want, last)
}

// assertContinuousSQL keeps querying for the duration and fails on any error.
func assertContinuousSQL(t *testing.T, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		_, err := queryHostname()
		require.NoError(t, err, "SQL through the sidecar failed during the availability window")
		time.Sleep(500 * time.Millisecond)
	}
}

// metricValue scrapes one plain (untyped/gauge/counter) metric from an API address.
func metricValue(api, name string) (float64, error) {
	resp, err := http.Get(api + "/metrics")
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
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			return strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
		}
	}
	return 0, fmt.Errorf("metric %s not found on %s", name, api)
}

func waitMetric(t *testing.T, api, name string, want float64, timeout time.Duration) {
	t.Helper()
	var last float64
	require.Eventuallyf(t, func() bool {
		v, err := metricValue(api, name)
		if err != nil {
			return false
		}
		last = v
		return v == want
	}, timeout, time.Second, "want %s=%v on %s, last %v", name, want, api, last)
}

// subscribedHub returns the API address and service name of the hub that the
// sidecar is currently subscribed to.
func subscribedHub(t *testing.T) (api, service string) {
	t.Helper()
	require.Eventually(t, func() bool {
		v0, err0 := metricValue(hub0API, "tiproxy_discovery_subscribers")
		v1, err1 := metricValue(hub1API, "tiproxy_discovery_subscribers")
		switch {
		case err0 == nil && v0 > 0:
			api, service = hub0API, "hub-0"
			return true
		case err1 == nil && v1 > 0:
			api, service = hub1API, "hub-1"
			return true
		}
		return false
	}, 30*time.Second, time.Second, "no hub reports a subscriber")
	return api, service
}

func otherHub(service string) (api, other string) {
	if service == "hub-0" {
		return hub1API, "hub-1"
	}
	return hub0API, "hub-0"
}

func sidecarLogs(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("docker", "logs", composeProject+"-sidecar-1").CombinedOutput()
	require.NoError(t, err)
	return string(out)
}
