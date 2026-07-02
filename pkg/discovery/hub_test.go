// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const testRecvTimeout = 3 * time.Second

type hubTestSuite struct {
	t       *testing.T
	lg      *zap.Logger
	server  *embed.Etcd
	testCli *clientv3.Client
	hubCli  *clientv3.Client
	hub     *Hub
	grpcSrv *grpc.Server
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

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ts.addr = lis.Addr().String()
	ts.grpcSrv = grpc.NewServer()
	pb.RegisterTiDBDiscoveryServer(ts.grpcSrv, ts.hub)
	go func() {
		_ = ts.grpcSrv.Serve(lis)
	}()
	return ts
}

func (ts *hubTestSuite) close() {
	ts.cancel()
	require.NoError(ts.t, ts.hub.Close())
	ts.grpcSrv.Stop()
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

func (ts *hubTestSuite) subscribe(ctx context.Context) pb.TiDBDiscovery_SubscribeClient {
	conn, err := grpc.NewClient(ts.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(ts.t, err)
	ts.t.Cleanup(func() {
		_ = conn.Close()
	})
	stream, err := pb.NewTiDBDiscoveryClient(conn).Subscribe(ctx, &pb.SubscribeRequest{ClientId: "test"})
	require.NoError(ts.t, err)
	return stream
}

func recvWithTimeout(t *testing.T, stream pb.TiDBDiscovery_SubscribeClient) *pb.DiscoveryResponse {
	type result struct {
		resp *pb.DiscoveryResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := stream.Recv()
		ch <- result{resp, err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.resp
	case <-time.After(testRecvTimeout):
		require.Fail(t, "no message received in time")
		return nil
	}
}

func assertNoMessage(t *testing.T, stream pb.TiDBDiscovery_SubscribeClient, wait time.Duration) {
	ch := make(chan *pb.DiscoveryResponse, 1)
	go func() {
		resp, err := stream.Recv()
		if err == nil {
			ch <- resp
		}
	}()
	select {
	case resp := <-ch:
		require.Fail(t, "unexpected message", "%+v", resp)
	case <-time.After(wait):
	}
}

func TestHubBootstrapAndSubscribe(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	ts.putTiDB("2.2.2.2:4000", "ks1", "2.2.2.2", 10080)

	// The subscriber always receives a full snapshot first.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.Eventually(t, func() bool {
		ts.hub.mu.RLock()
		defer ts.hub.mu.RUnlock()
		return len(ts.hub.mu.snap) == 2
	}, testRecvTimeout, 10*time.Millisecond)

	stream := ts.subscribe(ctx)
	resp := recvWithTimeout(t, stream)
	require.True(t, resp.Full)
	require.Len(t, resp.Upserted, 2)
	// Sorted by address.
	require.Equal(t, "1.1.1.1:4000", resp.Upserted[0].Addr)
	require.Equal(t, "1.1.1.1", resp.Upserted[0].Ip)
	require.Equal(t, uint32(10080), resp.Upserted[0].StatusPort)
	require.Equal(t, map[string]string{"zone": "z1"}, resp.Upserted[0].Labels)
	require.Equal(t, "2.2.2.2:4000", resp.Upserted[1].Addr)
	require.Equal(t, "ks1", resp.Upserted[1].Keyspace)
}

func TestHubDeltaUpsertAndRemove(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := ts.subscribe(ctx)
	resp := recvWithTimeout(t, stream)
	require.True(t, resp.Full)

	// A new tidb triggers an upsert delta.
	ts.putTiDB("3.3.3.3:4000", "", "3.3.3.3", 10080)
	resp = recvWithTimeout(t, stream)
	require.False(t, resp.Full)
	require.Len(t, resp.Upserted, 1)
	require.Equal(t, "3.3.3.3:4000", resp.Upserted[0].Addr)
	require.Empty(t, resp.RemovedAddrs)

	// Deleting the ttl key (lease expiry on tidb down) triggers a removal.
	ts.deleteTTL("3.3.3.3:4000", "")
	resp = recvWithTimeout(t, stream)
	require.False(t, resp.Full)
	require.Empty(t, resp.Upserted)
	require.Equal(t, []string{"3.3.3.3:4000"}, resp.RemovedAddrs)
}

func TestHubTTLRefreshSuppressed(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := ts.subscribe(ctx)
	resp := recvWithTimeout(t, stream)
	require.True(t, resp.Full)

	// Wait until the hub has caught up with the initial writes.
	require.Eventually(t, func() bool {
		ts.hub.mu.RLock()
		defer ts.hub.mu.RUnlock()
		return len(ts.hub.mu.snap) == 1
	}, testRecvTimeout, 10*time.Millisecond)
	parseCnt := ts.hub.ParseCount()

	// Simulate the periodic syncTopology of tidb: identical info, new ttl
	// timestamp. The snapshot cannot change, so nothing is broadcast and
	// the snapshot is not re-derived.
	for range 5 {
		ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	}
	assertNoMessage(t, stream, 300*time.Millisecond)
	require.Equal(t, parseCnt, ts.hub.ParseCount())
}

func TestHubCacheConsistency(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := ts.subscribe(ctx)

	// A burst of writes concurrent with the subscription.
	for i := range 10 {
		ts.putTiDB(fmt.Sprintf("7.7.7.%d:4000", i), "", fmt.Sprintf("7.7.7.%d", i), 10080)
	}
	ts.deleteTTL("7.7.7.0:4000", "")

	// Apply the full snapshot and all deltas to a local cache. The result
	// must converge to the actual topology: full first, deltas strictly newer.
	cache := make(map[string]*pb.TiDBInstance)
	require.Eventually(t, func() bool {
		resp := recvWithTimeout(t, stream)
		if resp.Full {
			clear(cache)
		}
		for _, inst := range resp.Upserted {
			cache[inst.Addr] = inst
		}
		for _, addr := range resp.RemovedAddrs {
			delete(cache, addr)
		}
		return len(cache) == 10 && cache["7.7.7.0:4000"] == nil && cache["7.7.7.9:4000"] != nil
	}, testRecvTimeout, time.Millisecond)
}

func TestHubSlowSubscriberDropped(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	require.Eventually(t, func() bool {
		ts.hub.mu.RLock()
		defer ts.hub.mu.RUnlock()
		return len(ts.hub.mu.snap) == 1
	}, testRecvTimeout, 10*time.Millisecond)

	// A subscriber that never drains its channel.
	slowSub, fullResp := ts.hub.register()
	require.NotNil(t, slowSub)
	require.NotNil(t, fullResp)

	// More broadcasts than the channel capacity force a drop.
	for i := range subChanCap + 2 {
		ts.putTiDB(fmt.Sprintf("8.8.8.%d:4000", i), "", fmt.Sprintf("8.8.8.%d", i), 10080)
	}

	require.Eventually(t, func() bool {
		ts.hub.mu.RLock()
		defer ts.hub.mu.RUnlock()
		_, ok := ts.hub.mu.subs[slowSub.id]
		return !ok
	}, testRecvTimeout, 10*time.Millisecond)
	// The channel is closed after draining the buffered messages.
	drained := false
	for !drained {
		select {
		case _, ok := <-slowSub.ch:
			drained = !ok
		case <-time.After(testRecvTimeout):
			require.Fail(t, "channel is not closed")
		}
	}

	// A healthy subscriber is not affected.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := ts.subscribe(ctx)
	resp := recvWithTimeout(t, stream)
	require.True(t, resp.Full)
	require.Len(t, resp.Upserted, subChanCap+3)
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

func TestHubFullRespCached(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)
	require.Eventually(t, func() bool {
		ts.hub.mu.RLock()
		defer ts.hub.mu.RUnlock()
		return len(ts.hub.mu.snap) == 1
	}, testRecvTimeout, 10*time.Millisecond)

	// The full response is cached: all subscribers share one instance until
	// the snapshot changes.
	sub1, full1 := ts.hub.register()
	sub2, full2 := ts.hub.register()
	require.Same(t, full1, full2)
	ts.hub.deregister(sub1)
	ts.hub.deregister(sub2)

	ts.putTiDB("2.2.2.2:4000", "", "2.2.2.2", 10080)
	require.Eventually(t, func() bool {
		sub3, full3 := ts.hub.register()
		defer ts.hub.deregister(sub3)
		return full3 != full1 && len(full3.Upserted) == 2
	}, testRecvTimeout, 10*time.Millisecond)
}

func TestHubPromPiggyback(t *testing.T) {
	ts := newHubTestSuite(t)
	t.Cleanup(ts.close)
	ts.putTiDB("1.1.1.1:4000", "", "1.1.1.1", 10080)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := ts.subscribe(ctx)
	resp := recvWithTimeout(t, stream)
	require.True(t, resp.Full)

	prom := &infosync.PrometheusInfo{IP: "9.9.9.9", Port: 9090}
	data, err := json.Marshal(prom)
	require.NoError(t, err)
	_, err = ts.testCli.Put(context.Background(), infosync.PromTopologyPath, string(data))
	require.NoError(t, err)

	// The prometheus change is piggybacked as a delta.
	resp = recvWithTimeout(t, stream)
	require.False(t, resp.Full)
	require.Empty(t, resp.Upserted)
	require.Equal(t, "9.9.9.9", resp.Prometheus.Ip)
	require.Equal(t, int32(9090), resp.Prometheus.Port)

	// A new subscriber gets it in the full snapshot.
	stream2 := ts.subscribe(ctx)
	resp = recvWithTimeout(t, stream2)
	require.True(t, resp.Full)
	require.Equal(t, "9.9.9.9", resp.Prometheus.Ip)
}

func TestHubNotBootstrapped(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	hub := NewHub(lg, nil, nil)
	sub, fullResp := hub.register()
	require.Nil(t, sub)
	require.Nil(t, fullResp)

	// The gRPC handler rejects subscriptions before the first bootstrap.
	err := hub.Subscribe(&pb.SubscribeRequest{}, nil)
	require.Error(t, err)
	_, ok := status.FromError(err)
	require.True(t, ok)
}
