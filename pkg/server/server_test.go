// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	nethttp "net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/discovery"
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

func TestServerWithHubSourcedCluster(t *testing.T) {
	restore := resetPromRegistry()
	defer restore()

	// A stub of the PD-side tidb-discovery service serving one TiDB, with
	// the same ETag semantics (content hash).
	dir := t.TempDir()
	body, err := json.Marshal(&discovery.TopologyResponse{
		Revision: 1,
		Backends: []discovery.TiDBInstance{{Addr: "10.0.0.1:4000", IP: "10.0.0.1", StatusPort: 10080}},
	})
	require.NoError(t, err)
	hash := fnv.New64a()
	_, _ = hash.Write(body)
	etag := strconv.FormatUint(hash.Sum64(), 16)
	hubSrv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.URL.Path != "/api/topology" {
			w.WriteHeader(nethttp.StatusNotFound)
			return
		}
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(nethttp.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		_, _ = w.Write(body)
	}))
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

	// The topology served by the hub reaches the cluster manager.
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
