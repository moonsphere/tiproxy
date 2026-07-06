// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
)

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
	}, testRecvTimeout, 10*time.Millisecond)
}

func TestHubClientPollAndApply(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	cli := newTestHubClient(t, ts.addr)

	// The snapshot arrives with all the fields.
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["1.1.1.1:4000"]
		return len(topo) == 1 && info != nil && info.IP == "1.1.1.1" && info.StatusPort == 10080 && info.Addr == "1.1.1.1:4000"
	})

	// Changes are picked up by the next polls.
	ts.putTiDB("2.2.2.2:4000", "ks1", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["2.2.2.2:4000"]
		return len(topo) == 2 && info != nil && info.Keyspace == "ks1"
	})
	ts.deleteTTL("2.2.2.2:4000", "ks1")
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})

	// The steady state negotiates with the ETag.
	require.Eventually(t, func() bool {
		cli.mu.RLock()
		defer cli.mu.RUnlock()
		return cli.mu.etag != ""
	}, testRecvTimeout, 10*time.Millisecond)
}

func TestHubClientNotReady(t *testing.T) {
	// No hub is listening on this address.
	cli := newTestHubClient(t, "127.0.0.1:1")

	_, err := cli.GetTiDBTopology(context.Background())
	require.ErrorIs(t, err, ErrHubNotReady)
	_, err = cli.GetPromInfo(context.Background())
	require.ErrorIs(t, err, infosync.ErrNoProm)
}

func TestHubClientFailover(t *testing.T) {
	// Two hubs against the same PD: the client rotates on failure.
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	// The second hub shares the same etcd.
	lg, _ := logger.CreateLoggerForTest(t)
	hub2 := NewHub(lg.Named("hub2"), ts.testCli, nil)
	hub2.bootstrapRetryIntvl = 50 * time.Millisecond
	hub2.promRefreshIntvl = 100 * time.Millisecond
	ctx2, cancel2 := context.WithCancel(context.Background())
	hub2.Run(ctx2)
	t.Cleanup(func() {
		cancel2()
		require.NoError(t, hub2.Close())
	})
	srv2 := startHubHTTPServer(t, hub2)

	cli := newTestHubClient(t, ts.addr+","+srv2.Listener.Addr().String())
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})

	// Kill the first hub's endpoint: the client rotates to the second and
	// keeps picking up updates.
	ts.httpSrv.Close()
	ts.putTiDB("2.2.2.2:4000", "", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
}

func TestHubClientCopyOnWrite(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	cli := newTestHubClient(t, ts.addr)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})
	snapshot, err := cli.GetTiDBTopology(context.Background())
	require.NoError(t, err)

	// A refresh after the snapshot was handed out must not mutate it.
	ts.putTiDB("2.2.2.2:4000", "", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
	require.Len(t, snapshot, 1)
	require.Equal(t, "1.1.1.1", snapshot["1.1.1.1:4000"].IP)
}
