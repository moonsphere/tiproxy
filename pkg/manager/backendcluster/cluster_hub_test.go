// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/discovery"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// startTestHub runs a discovery hub against the given etcd cluster and
// returns its gRPC address.
func startTestHub(t *testing.T, pd *managerTestEtcdCluster) string {
	lg := zapLoggerForTest(t)
	hub := discovery.NewHub(lg.Named("hub"), pd.client, nil)
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

func TestManagerWithHubSourcedCluster(t *testing.T) {
	// One PD-sourced cluster and one hub-sourced cluster in the same manager.
	pdA := newManagerTestEtcdCluster(t)
	pdB := newManagerTestEtcdCluster(t)
	t.Cleanup(func() { pdA.close(t) })
	t.Cleanup(func() { pdB.close(t) })
	pdA.putTopology(t, "10.0.0.1:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	pdB.putTopology(t, "10.0.0.2:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.2", StatusPort: 10080})

	hubAddr := startTestHub(t, pdB)

	cfg := newManagerTestConfig()
	cfg.Proxy.BackendClusters = []config.BackendCluster{
		{Name: "cluster-a", PDAddrs: pdA.addr},
		{Name: "cluster-b", DiscoverySource: config.DiscoverySourceHub, HubAddrs: hubAddr},
	}
	cfgGetter := newManagerTestConfigGetter(cfg)
	cfgCh := make(chan *config.Config, 1)

	mgr := NewManager(zapLoggerForTest(t), nilClusterTLS)
	require.NoError(t, mgr.Start(context.Background(), cfgGetter, cfgCh))
	t.Cleanup(func() {
		close(cfgCh)
		require.NoError(t, mgr.Close())
	})

	// The hub-sourced cluster has no PD client.
	require.Eventually(t, func() bool {
		clusters := mgr.Snapshot()
		clusterB := clusters["cluster-b"]
		return len(clusters) == 2 && clusterB != nil && clusterB.EtcdClient() == nil
	}, 5*time.Second, 100*time.Millisecond)

	// The topologies from both sources are merged.
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 2 {
			return false
		}
		return topology[backendID("cluster-a", "10.0.0.1:4000")] != nil &&
			topology[backendID("cluster-b", "10.0.0.2:4000")] != nil &&
			topology[backendID("cluster-b", "10.0.0.2:4000")].ClusterName == "cluster-b"
	}, 5*time.Second, 100*time.Millisecond)

	// The hub-sourced cluster sees topology changes pushed by the hub.
	pdB.putTopology(t, "10.0.0.3:4000", &infosync.TiDBTopologyInfo{IP: "10.0.0.3", StatusPort: 10080})
	require.Eventually(t, func() bool {
		topology, err := mgr.GetTiDBTopology(context.Background())
		return err == nil && len(topology) == 3 && topology[backendID("cluster-b", "10.0.0.3:4000")] != nil
	}, 5*time.Second, 100*time.Millisecond)
}
