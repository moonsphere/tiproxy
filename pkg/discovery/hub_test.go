// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const testRecvTimeout = 3 * time.Second

type hubTestSuite struct {
	t       *testing.T
	lg      *zap.Logger
	server  *embed.Etcd
	testCli *clientv3.Client
	hubCli  *clientv3.Client
	hub     *Hub
	httpSrv *httptest.Server
	addr    string
	ready   chan struct{}
	cancel  context.CancelFunc
}

func newHubTestSuite(t *testing.T) *hubTestSuite {
	lg, _ := logger.CreateLoggerForTest(t)
	ts := &hubTestSuite{
		t:     t,
		lg:    lg,
		ready: make(chan struct{}),
	}

	etcdSrv, err := etcd.CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	ts.server = etcdSrv
	endpoint := etcdSrv.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)
	certMgr := cert.NewCertManager()
	require.NoError(t, certMgr.Init(cfg, lg, nil))
	ts.testCli, err = etcd.InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)
	ts.hubCli, err = etcd.InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)

	ts.hub = NewHub(lg.Named("hub"), ts.hubCli, func() {
		close(ts.ready)
	})
	ts.hub.bootstrapRetryIntvl = 50 * time.Millisecond
	ts.hub.promRefreshIntvl = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	ts.cancel = cancel
	ts.hub.Run(ctx)
	select {
	case <-ts.ready:
	case <-time.After(testRecvTimeout):
		require.Fail(t, "hub is not ready in time")
	}

	ts.httpSrv = startHubHTTPServer(t, ts.hub)
	ts.addr = ts.httpSrv.Listener.Addr().String()
	return ts
}

// startHubHTTPServer serves the hub's topology endpoint like the discovery
// API server does.
func startHubHTTPServer(t *testing.T, hub *Hub) *httptest.Server {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Group("api").GET("/topology", hub.HandleTopology)
	srv := httptest.NewServer(engine.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func (ts *hubTestSuite) close() {
	ts.cancel()
	require.NoError(ts.t, ts.hub.Close())
	require.NoError(ts.t, ts.testCli.Close())
	// hubCli may be closed by tests already.
	_ = ts.hubCli.Close()
	ts.server.Close()
}

func (ts *hubTestSuite) putTiDB(sqlAddr, keyspace, ip string, statusPort uint) {
	info := &infosync.TiDBTopologyInfo{
		IP:         ip,
		StatusPort: statusPort,
		Labels:     map[string]string{"zone": "z1"},
	}
	data, err := json.Marshal(info)
	require.NoError(ts.t, err)
	infoKey := path.Join(infosync.TiDBTopologyPath, sqlAddr, "info")
	ttlKey := path.Join(infosync.TiDBTopologyPath, sqlAddr, "ttl")
	if keyspace != "" {
		infoKey = path.Join(infosync.TiDBKeyspaceTopologyPath, keyspace, infoKey)
		ttlKey = path.Join(infosync.TiDBKeyspaceTopologyPath, keyspace, ttlKey)
	}
	_, err = ts.testCli.Put(context.Background(), infoKey, string(data))
	require.NoError(ts.t, err)
	_, err = ts.testCli.Put(context.Background(), ttlKey, fmt.Sprintf("%d", time.Now().UnixNano()))
	require.NoError(ts.t, err)
}

func (ts *hubTestSuite) deleteTTL(sqlAddr, keyspace string) {
	key := path.Join(infosync.TiDBTopologyPath, sqlAddr, "ttl")
	if keyspace != "" {
		key = path.Join(infosync.TiDBKeyspaceTopologyPath, keyspace, key)
	}
	_, err := ts.testCli.Delete(context.Background(), key)
	require.NoError(ts.t, err)
}

// getTopology fetches the endpoint once. It never fails the test, so it is
// safe inside require.Eventually conditions.
func (ts *hubTestSuite) getTopology(etag string) (status int, topo *TopologyResponse, newEtag string) {
	req, err := http.NewRequest(http.MethodGet, "http://"+ts.addr+"/api/topology", nil)
	if err != nil {
		return 0, nil, ""
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, ""
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, resp.Header.Get("ETag")
	}
	if resp.StatusCode == http.StatusOK {
		var t TopologyResponse
		if err := json.Unmarshal(body, &t); err == nil {
			topo = &t
		}
	}
	return resp.StatusCode, topo, resp.Header.Get("ETag")
}

// waitBackends waits until the endpoint serves exactly the given addresses.
func (ts *hubTestSuite) waitBackends(addrs ...string) *TopologyResponse {
	var last *TopologyResponse
	require.Eventuallyf(ts.t, func() bool {
		status, topo, _ := ts.getTopology("")
		if status != http.StatusOK || topo == nil {
			return false
		}
		last = topo
		if len(topo.Backends) != len(addrs) {
			return false
		}
		for i, addr := range addrs {
			if topo.Backends[i].Addr != addr {
				return false
			}
		}
		return true
	}, testRecvTimeout, 10*time.Millisecond, "want backends %v, last %+v", addrs, last)
	return last
}

func TestHubBootstrapAndServe(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.putTiDB("2.2.2.2:4000", "ks1", "2.2.2.2", 10080)

	// Sorted by address, with all the wire fields.
	topo := ts.waitBackends("1.1.1.1:4000", "2.2.2.2:4000")
	require.Equal(t, "1.1.1.1", topo.Backends[0].IP)
	require.Equal(t, uint(10080), topo.Backends[0].StatusPort)
	require.Equal(t, map[string]string{"zone": "z1"}, topo.Backends[0].Labels)
	require.Equal(t, "ks1", topo.Backends[1].Keyspace)
	require.Positive(t, topo.Revision)

	// A matching If-None-Match answers 304 with no body.
	status, _, etag := ts.getTopology("")
	require.Equal(t, http.StatusOK, status)
	require.NotEmpty(t, etag)
	status, topo, newEtag := ts.getTopology(etag)
	require.Equal(t, http.StatusNotModified, status)
	require.Nil(t, topo)
	require.Equal(t, etag, newEtag)
}

func TestHubContentChange(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.waitBackends("1.1.1.1:4000")
	_, _, etag := ts.getTopology("")

	// A new tidb changes the content: the old ETag no longer matches.
	ts.putTiDB("3.3.3.3:4000", "", "3.3.3.3", 10080)
	require.Eventually(t, func() bool {
		status, topo, _ := ts.getTopology(etag)
		return status == http.StatusOK && topo != nil && len(topo.Backends) == 2
	}, testRecvTimeout, 10*time.Millisecond)

	// Deleting the ttl key (lease expiry on tidb down) removes the backend.
	ts.deleteTTL("3.3.3.3:4000", "")
	ts.waitBackends("1.1.1.1:4000")
}

func TestHubTTLRefreshSuppressed(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.waitBackends("1.1.1.1:4000")
	_, _, etag := ts.getTopology("")
	parseCnt := ts.hub.ParseCount()

	// Simulate the periodic syncTopology of tidb: identical info, new ttl
	// timestamp. The content cannot change, so the ETag stays, the pollers
	// keep getting 304 and the snapshot is not re-derived.
	for range 5 {
		ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	}
	time.Sleep(300 * time.Millisecond)
	status, _, newEtag := ts.getTopology(etag)
	require.Equal(t, http.StatusNotModified, status)
	require.Equal(t, etag, newEtag)
	require.Equal(t, parseCnt, ts.hub.ParseCount())
}

func TestHubNotBootstrapped(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hub := NewHub(lg, nil, nil)
	srv := startHubHTTPServer(t, hub)

	resp, err := http.Get(srv.URL + "/api/topology")
	require.NoError(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestHubPromPiggyback(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.waitBackends("1.1.1.1:4000")
	_, _, etag := ts.getTopology("")

	prom := &infosync.PrometheusInfo{IP: "9.9.9.9", Port: 9090}
	data, err := json.Marshal(prom)
	require.NoError(t, err)
	_, err = ts.testCli.Put(context.Background(), infosync.PromTopologyPath, string(data))
	require.NoError(t, err)

	// The prometheus change bumps the version even though the etcd topology
	// revision is unchanged.
	require.Eventually(t, func() bool {
		status, topo, _ := ts.getTopology(etag)
		return status == http.StatusOK && topo != nil && topo.Prometheus != nil &&
			topo.Prometheus.IP == "9.9.9.9" && topo.Prometheus.Port == 9090
	}, testRecvTimeout, 10*time.Millisecond)
}

func TestHubRebootstrapOnClientClose(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	rebootstraps := testutil.ToFloat64(metrics.DiscoveryRebootstrapCounter)
	// Closing the hub's etcd client closes the watch channels. The hub must
	// detect the closed channel (not a hot loop on zero values) and fall
	// back to re-bootstrapping, which keeps failing with backoff until the
	// hub is closed.
	require.NoError(t, ts.hubCli.Close())
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DiscoveryRebootstrapCounter) > rebootstraps
	}, testRecvTimeout, 10*time.Millisecond)

	// No hot loop: the snapshot is not re-derived while bootstrap fails.
	parseCnt := ts.hub.ParseCount()
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, parseCnt, ts.hub.ParseCount())
}

// TestHubManySubscribers is a small-scale load test: many concurrent pollers
// all see the topology, and the steady state is served from the cached bytes.
func TestHubManyPollers(t *testing.T) {
	if testing.Short() {
		t.Skip("skip the load test in short mode")
	}
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.waitBackends("1.1.1.1:4000")

	const pollers = 200
	done := make(chan error, pollers)
	for i := 0; i < pollers; i++ {
		go func() {
			status, topo, etag := ts.getTopology("")
			if status != http.StatusOK || topo == nil || len(topo.Backends) != 1 {
				done <- fmt.Errorf("poller got status %d topo %+v", status, topo)
				return
			}
			status, _, _ = ts.getTopology(etag)
			if status != http.StatusNotModified {
				done <- fmt.Errorf("poller got status %d for a matching etag", status)
				return
			}
			done <- nil
		}()
	}
	for i := 0; i < pollers; i++ {
		require.NoError(t, <-done)
	}
}
