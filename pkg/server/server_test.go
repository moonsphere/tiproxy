// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pelletier/go-toml/v2"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/discovery"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
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

	// The server turns ready after the first topology bootstrap, then the
	// topology endpoint serves a (here empty) snapshot.
	require.Eventually(t, func() bool {
		resp, err := nethttp.Get("http://" + apiAddr + "/api/topology")
		if err != nil {
			return false
		}
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != nethttp.StatusOK {
			return false
		}
		var topo discovery.TopologyResponse
		if err := json.NewDecoder(resp.Body).Decode(&topo); err != nil {
			return false
		}
		return len(topo.Backends) == 0 && resp.Header.Get("ETag") != ""
	}, 10*time.Second, 100*time.Millisecond)

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
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Group("api").GET("/topology", hub.HandleTopology)
	hubSrv := httptest.NewServer(engine.Handler())
	t.Cleanup(hubSrv.Close)

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
`, hubSrv.Listener.Addr().String())
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

func TestDiscoveryServerRejectsHubSource(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	dir := t.TempDir()
	configFile := dir + "/config.toml"
	configData := `
[proxy]
pd-addrs = ""
[[proxy.backend-clusters]]
name = "c1"
discovery-source = "hub"
hub-addrs = "127.0.0.1:3080"
`
	require.NoError(t, os.WriteFile(configFile, []byte(configData), 0o644))

	_, err := NewDiscoveryServer(context.Background(), &sctx.Context{
		ConfigFile: configFile,
	})
	require.ErrorContains(t, err, "discovery-source=hub")
}
