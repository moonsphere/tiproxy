// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/discovery"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServer(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	etcdServer, err := etcd.CreateEtcdServer("0.0.0.0:0", dir, lg)
	require.NoError(t, err)
	configFile := dir + "/config.toml"
	endpoint := etcdServer.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)
	b, err := toml.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configFile, b, 0o644))

	server, err := NewServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)
	require.NoError(t, server.Close())
	etcdServer.Close()
}

func TestServerWithoutBackendCluster(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	dir := t.TempDir()
	configFile := dir + "/config.toml"
	require.NoError(t, os.WriteFile(configFile, []byte("[proxy]\npd-addrs = \"\"\n"), 0o644))

	server, err := NewServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)
	require.NoError(t, server.Close())
}

func resetPromRegistry() func() {
	registry := prometheus.NewRegistry()
	oldRegisterer := prometheus.DefaultRegisterer
	oldGatherer := prometheus.DefaultGatherer
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	return func() {
		prometheus.DefaultRegisterer = oldRegisterer
		prometheus.DefaultGatherer = oldGatherer
	}
}

func TestDiscoveryServer(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	etcdServer, err := etcd.CreateEtcdServer("0.0.0.0:0", dir, lg)
	require.NoError(t, err)
	t.Cleanup(etcdServer.Close)
	endpoint := etcdServer.Clients[0].Addr().String()

	// Pick a free port for the API server.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	apiAddr := lis.Addr().String()
	require.NoError(t, lis.Close())

	configFile := dir + "/config.toml"
	configData := fmt.Sprintf("[proxy]\npd-addrs = %q\n[api]\naddr = %q\n", endpoint, apiAddr)
	require.NoError(t, os.WriteFile(configFile, []byte(configData), 0o644))

	server, err := NewDiscoveryServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)

	// The server turns ready after the first topology bootstrap.
	conn, err := grpc.NewClient(apiAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})
	healthCli := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		resp, err := healthCli.Check(context.Background(), &healthpb.HealthCheckRequest{})
		return err == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
	}, 10*time.Second, 100*time.Millisecond)

	// Subscribing returns a full (empty) topology snapshot.
	stream, err := pb.NewTiDBDiscoveryClient(conn).Subscribe(context.Background(), &pb.SubscribeRequest{ClientId: "test"})
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, resp.Full)
	require.Empty(t, resp.Upserted)

	require.NoError(t, server.Close())
}

func TestServerWithHubSourcedCluster(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	// The PD side: an etcd with one TiDB registered, watched by a hub.
	dir := t.TempDir()
	lg, _ := logger.CreateLoggerForTest(t)
	etcdServer, err := etcd.CreateEtcdServer("0.0.0.0:0", dir, lg)
	require.NoError(t, err)
	t.Cleanup(etcdServer.Close)
	endpoint := etcdServer.Clients[0].Addr().String()
	etcdCli, err := etcd.InitEtcdClientWithAddrs(lg, endpoint, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, etcdCli.Close())
	})
	info, err := json.Marshal(&infosync.TiDBTopologyInfo{IP: "10.0.0.1", StatusPort: 10080})
	require.NoError(t, err)
	_, err = etcdCli.Put(context.Background(), infosync.TiDBTopologyPath+"10.0.0.1:4000/info", string(info))
	require.NoError(t, err)
	_, err = etcdCli.Put(context.Background(), infosync.TiDBTopologyPath+"10.0.0.1:4000/ttl", "1")
	require.NoError(t, err)

	hub := discovery.NewHub(lg.Named("hub"), etcdCli, nil)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	hub.Run(hubCtx)
	t.Cleanup(func() {
		hubCancel()
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

	// The sidecar side: a full proxy server whose only cluster is hub-sourced,
	// so it has no PD client at all.
	configFile := dir + "/config.toml"
	configData := fmt.Sprintf(`
[proxy]
pd-addrs = ""
[[proxy.backend-clusters]]
name = "c1"
discovery-source = "hub"
hub-addrs = %q
`, lis.Addr().String())
	require.NoError(t, os.WriteFile(configFile, []byte(configData), 0o644))

	server, err := NewServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.NoError(t, err)

	// The topology pushed by the hub reaches the cluster manager.
	require.Eventually(t, func() bool {
		topology, err := server.clusterManager.GetTiDBTopology(context.Background())
		if err != nil || len(topology) != 1 {
			return false
		}
		for _, info := range topology {
			return info.IP == "10.0.0.1" && info.ClusterName == "c1"
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)

	require.NoError(t, server.Close())
}
