// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
)

const testTimeout = 3 * time.Second

// topoStub is a minimal stand-in for the PD-side tidb-discovery service:
// it serves a settable TopologyResponse with the same ETag/If-None-Match
// semantics (content hash).
type topoStub struct {
	t   *testing.T
	srv *httptest.Server
	mu  struct {
		sync.Mutex
		body []byte
		etag string
	}
}

func newTopoStub(t *testing.T) *topoStub {
	ts := &topoStub{t: t}
	ts.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts.mu.Lock()
		body, etag := ts.mu.body, ts.mu.etag
		ts.mu.Unlock()
		if r.URL.Path != "/api/topology" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if body == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get("If-None-Match") == etag {
			w.Header().Set("ETag", etag)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *topoStub) addr() string {
	return ts.srv.Listener.Addr().String()
}

func (ts *topoStub) set(resp TopologyResponse) {
	body, err := json.Marshal(&resp)
	require.NoError(ts.t, err)
	hash := fnv.New64a()
	_, _ = hash.Write(body)
	ts.mu.Lock()
	ts.mu.body = body
	ts.mu.etag = strconv.FormatUint(hash.Sum64(), 16)
	ts.mu.Unlock()
}

func backend(addr, ip string, port uint, keyspace string) TiDBInstance {
	return TiDBInstance{
		Addr:       addr,
		IP:         ip,
		StatusPort: port,
		Labels:     map[string]string{"zone": "z1"},
		Keyspace:   keyspace,
	}
}

func newTestHubClient(t *testing.T, addrs string) *HubClient {
	lg, _ := logger.CreateLoggerForTest(t)
	cli := NewHubClient(addrs, func() *tls.Config { return nil }, lg)
	cli.pollIntvl = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cli.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})
	return cli
}

func waitClientTopology(t *testing.T, cli *HubClient, check func(map[string]*infosync.TiDBTopologyInfo) bool) {
	require.Eventually(t, func() bool {
		topo, err := cli.GetTiDBTopology(context.Background())
		if err != nil {
			return false
		}
		return check(topo)
	}, testTimeout, 10*time.Millisecond)
}

func TestHubClientPollAndApply(t *testing.T) {
	stub := newTopoStub(t)
	stub.set(TopologyResponse{
		Revision: 7,
		Backends: []TiDBInstance{backend("1.1.1.1:4000", "1.1.1.1", 10080, "")},
	})
	cli := newTestHubClient(t, stub.addr())

	// The snapshot arrives with all the fields.
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["1.1.1.1:4000"]
		return len(topo) == 1 && info != nil && info.IP == "1.1.1.1" &&
			info.StatusPort == 10080 && info.Addr == "1.1.1.1:4000"
	})

	// Changes are picked up by the next polls.
	stub.set(TopologyResponse{
		Revision: 8,
		Backends: []TiDBInstance{
			backend("1.1.1.1:4000", "1.1.1.1", 10080, ""),
			backend("2.2.2.2:4000", "2.2.2.2", 10080, "ks1"),
		},
	})
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["2.2.2.2:4000"]
		return len(topo) == 2 && info != nil && info.Keyspace == "ks1"
	})
	stub.set(TopologyResponse{
		Revision: 9,
		Backends: []TiDBInstance{backend("1.1.1.1:4000", "1.1.1.1", 10080, "")},
	})
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})

	// The steady state negotiates with the ETag.
	require.Eventually(t, func() bool {
		cli.mu.RLock()
		defer cli.mu.RUnlock()
		return cli.mu.etag != ""
	}, testTimeout, 10*time.Millisecond)
}

func TestHubClientNotReady(t *testing.T) {
	// No hub is listening on this address.
	cli := newTestHubClient(t, "127.0.0.1:1")

	_, err := cli.GetTiDBTopology(context.Background())
	require.ErrorIs(t, err, ErrHubNotReady)
	_, err = cli.GetPromInfo(context.Background())
	require.ErrorIs(t, err, infosync.ErrNoProm)
}

func TestHubClientPromInfo(t *testing.T) {
	stub := newTopoStub(t)
	stub.set(TopologyResponse{
		Revision:   1,
		Backends:   []TiDBInstance{backend("1.1.1.1:4000", "1.1.1.1", 10080, "")},
		Prometheus: &PrometheusInfo{IP: "9.9.9.9", Port: 9090},
	})
	cli := newTestHubClient(t, stub.addr())

	require.Eventually(t, func() bool {
		prom, err := cli.GetPromInfo(context.Background())
		return err == nil && prom.IP == "9.9.9.9" && prom.Port == 9090
	}, testTimeout, 10*time.Millisecond)
}

func TestHubClientFailover(t *testing.T) {
	// Two stubs: the client sticks to the first and rotates on failure.
	stub1 := newTopoStub(t)
	stub2 := newTopoStub(t)
	topo1 := TopologyResponse{
		Revision: 1,
		Backends: []TiDBInstance{backend("1.1.1.1:4000", "1.1.1.1", 10080, "")},
	}
	stub1.set(topo1)
	stub2.set(topo1)

	cli := newTestHubClient(t, stub1.addr()+","+stub2.addr())
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})

	// Kill the first stub and change the topology on the second: the client
	// rotates and keeps picking up updates.
	stub1.srv.Close()
	stub2.set(TopologyResponse{
		Revision: 2,
		Backends: []TiDBInstance{
			backend("1.1.1.1:4000", "1.1.1.1", 10080, ""),
			backend("2.2.2.2:4000", "2.2.2.2", 10080, ""),
		},
	})
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
}

func TestHubClientCopyOnWrite(t *testing.T) {
	stub := newTopoStub(t)
	stub.set(TopologyResponse{
		Revision: 1,
		Backends: []TiDBInstance{backend("1.1.1.1:4000", "1.1.1.1", 10080, "")},
	})
	cli := newTestHubClient(t, stub.addr())
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})
	snapshot, err := cli.GetTiDBTopology(context.Background())
	require.NoError(t, err)

	// A refresh after the snapshot was handed out must not mutate it.
	stub.set(TopologyResponse{
		Revision: 2,
		Backends: []TiDBInstance{
			backend("1.1.1.1:4000", "1.1.1.1", 10080, ""),
			backend("2.2.2.2:4000", "2.2.2.2", 10080, ""),
		},
	})
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
	require.Len(t, snapshot, 1)
	require.Equal(t, "1.1.1.1", snapshot["1.1.1.1:4000"].IP)
}
