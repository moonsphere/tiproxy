// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func waitClientTopology(t *testing.T, cli *HubClient, check func(map[string]*infosync.TiDBTopologyInfo) bool) {
	require.Eventually(t, func() bool {
		topo, err := cli.GetTiDBTopology(context.Background())
		if err != nil {
			return false
		}
		return check(topo)
	}, testRecvTimeout, 10*time.Millisecond)
}

func TestHubClientFullAndDelta(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	lg, _ := logger.CreateLoggerForTest(t)
	cli := NewHubClient(ts.addr, "test-client", func() *tls.Config { return nil }, lg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})

	// The full snapshot arrives.
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["1.1.1.1:4000"]
		return len(topo) == 1 && info != nil && info.IP == "1.1.1.1" && info.StatusPort == 10080 && info.Addr == "1.1.1.1:4000"
	})

	// Deltas are applied.
	ts.putTiDB("2.2.2.2:4000", "ks1", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		info := topo["2.2.2.2:4000"]
		return len(topo) == 2 && info != nil && info.Keyspace == "ks1"
	})
	ts.deleteTTL("2.2.2.2:4000", "ks1")
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})
}

func TestHubClientNotReady(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	// No hub is listening on this address.
	cli := NewHubClient("127.0.0.1:1", "test-client", func() *tls.Config { return nil }, lg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})

	_, err := cli.GetTiDBTopology(context.Background())
	require.ErrorIs(t, err, ErrHubNotReady)
	_, err = cli.GetPromInfo(context.Background())
	require.ErrorIs(t, err, infosync.ErrNoProm)
}

func TestHubClientReconnect(t *testing.T) {
	// Two hubs watching the same PD: the client fails over between them.
	ts1 := newHubTestSuite(t)
	t.Cleanup(ts1.close)
	ts1.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	// The second hub shares the same etcd.
	ts2addr := startSecondHub(t, ts1)

	lg, _ := logger.CreateLoggerForTest(t)
	cli := NewHubClient(ts1.addr+","+ts2addr, "test-client", func() *tls.Config { return nil }, lg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})

	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})

	// Kill the first hub: the client reconnects to the second and keeps
	// receiving updates.
	ts1.grpcSrv.Stop()
	ts1.putTiDB("2.2.2.2:4000", "", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
}

func TestHubClientCopyOnWrite(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	lg, _ := logger.CreateLoggerForTest(t)
	cli := NewHubClient(ts.addr, "test-client", func() *tls.Config { return nil }, lg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli.Start(ctx)
	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})

	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 1
	})
	snapshot, err := cli.GetTiDBTopology(context.Background())
	require.NoError(t, err)

	// A delta after the snapshot was handed out must not mutate it.
	ts.putTiDB("2.2.2.2:4000", "", "2.2.2.2", 10080)
	waitClientTopology(t, cli, func(topo map[string]*infosync.TiDBTopologyInfo) bool {
		return len(topo) == 2
	})
	require.Len(t, snapshot, 1)
	require.Equal(t, "1.1.1.1", snapshot["1.1.1.1:4000"].IP)
}

// startSecondHub starts another hub + gRPC server against the same etcd.
func startSecondHub(t *testing.T, ts *hubTestSuite) string {
	lg, _ := logger.CreateLoggerForTest(t)
	hub := NewHub(lg.Named("hub2"), ts.testCli, nil)
	hub.bootstrapRetryIntvl = 50 * time.Millisecond
	hub.promRefreshIntvl = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	hub.Run(ctx)
	t.Cleanup(func() {
		cancel()
		require.NoError(t, hub.Close())
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	pb.RegisterTiDBDiscoveryServer(grpcSrv, hub)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	t.Cleanup(grpcSrv.Stop)
	return lis.Addr().String()
}
