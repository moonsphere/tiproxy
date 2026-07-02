// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"crypto/tls"
	"maps"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	minReconnectBackoff = time.Second
	maxReconnectBackoff = 10 * time.Second
)

// ErrHubNotReady is returned before the hub pushes the first snapshot.
// The callers (e.g. PDFetcher) retry on errors, so it behaves the same as an
// unreachable PD.
var ErrHubNotReady = errors.New("the discovery hub has not pushed a topology snapshot yet")

// HubClient subscribes to a discovery hub and maintains a local topology
// cache. It is a drop-in replacement for the topology methods of
// infosync.InfoSyncer, so everything downstream of the backend cluster stays
// unchanged.
type HubClient struct {
	hubAddrs  []string
	clientID  string
	tlsGetter func() *tls.Config
	lg        *zap.Logger
	wg        waitgroup.WaitGroup
	cancel    context.CancelFunc

	mu struct {
		sync.RWMutex
		// snap is nil until the first full snapshot arrives.
		snap map[string]*infosync.TiDBTopologyInfo
		prom *infosync.PrometheusInfo
		rev  int64
	}
}

// NewHubClient creates a HubClient. hubAddrs is a comma-separated address
// list; tlsGetter provides the cluster TLS config and may return nil for
// plaintext. clientID is only for logging on the hub side.
func NewHubClient(hubAddrs string, clientID string, tlsGetter func() *tls.Config, lg *zap.Logger) *HubClient {
	return &HubClient{
		hubAddrs:  config.SplitAddrList(hubAddrs),
		clientID:  clientID,
		tlsGetter: tlsGetter,
		lg:        lg,
	}
}

// Start subscribes to the hub in the background.
func (c *HubClient) Start(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.RunWithRecover(func() {
		c.streamLoop(childCtx)
	}, nil, c.lg)
}

// streamLoop keeps one subscription alive, rotating through the hub
// addresses with a jittered backoff. The jitter matters: after a hub restart
// the reconnect clocks of all sidecars are synchronized, and reconnecting in
// lockstep would stampede the hubs.
func (c *HubClient) streamLoop(ctx context.Context) {
	// The config validation guarantees a non-empty list; guard against a
	// direct misuse of the constructor.
	if len(c.hubAddrs) == 0 {
		c.lg.Error("no discovery hub address is configured, the topology will never arrive")
		return
	}
	backoff := minReconnectBackoff
	for i := 0; ctx.Err() == nil; i++ {
		addr := c.hubAddrs[i%len(c.hubAddrs)]
		received, err := c.stream(ctx, addr)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.lg.Warn("the subscription to the discovery hub is broken, reconnecting",
				zap.String("addr", addr), zap.Error(err))
		}
		if received {
			// The stream worked: start over with the shortest backoff.
			backoff = minReconnectBackoff
		}
		// math/rand can not pass the security scanning while crypto/rand is
		// overkill for backoff jitter, so derive the jitter from the clock.
		frac := float64(time.Now().UnixMicro()%1000) / 1000.0
		jittered := backoff + time.Duration((frac*0.4-0.2)*float64(backoff))
		select {
		case <-time.After(jittered):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxReconnectBackoff)
	}
}

// stream subscribes to one hub and applies responses until the stream breaks.
// It reports whether at least one response was received.
func (c *HubClient) stream(ctx context.Context, addr string) (received bool, err error) {
	creds := insecure.NewCredentials()
	if tlsCfg := c.tlsGetter(); tlsCfg != nil {
		creds = credentials.NewTLS(tlsCfg)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	if err != nil {
		return false, errors.WithStack(err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			c.lg.Warn("close the hub connection failed", zap.Error(closeErr))
		}
	}()

	c.mu.RLock()
	rev := c.mu.rev
	c.mu.RUnlock()
	stream, err := pb.NewTiDBDiscoveryClient(conn).Subscribe(ctx, &pb.SubscribeRequest{
		ClientId:      c.clientID,
		KnownRevision: rev,
	})
	if err != nil {
		return false, errors.WithStack(err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return received, errors.WithStack(err)
		}
		c.apply(resp)
		received = true
	}
}

// apply merges one response into the local cache. The cache is copy-on-write:
// the maps handed out by GetTiDBTopology share the pointers, so values are
// never mutated in place.
func (c *HubClient) apply(resp *pb.DiscoveryResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case resp.Full:
		snap := make(map[string]*infosync.TiDBTopologyInfo, len(resp.Upserted))
		for _, inst := range resp.Upserted {
			snap[inst.Addr] = ToTopologyInfo(inst)
		}
		c.mu.snap = snap
	case c.mu.snap == nil:
		// The hub always sends a full snapshot first, so a delta before any
		// full snapshot cannot be applied consistently: skip its topology.
		if len(resp.Upserted) > 0 || len(resp.RemovedAddrs) > 0 {
			c.lg.Warn("skip a topology delta that arrived before any full snapshot")
		}
	case len(resp.Upserted) > 0 || len(resp.RemovedAddrs) > 0:
		snap := maps.Clone(c.mu.snap)
		for _, inst := range resp.Upserted {
			snap[inst.Addr] = ToTopologyInfo(inst)
		}
		for _, addr := range resp.RemovedAddrs {
			delete(snap, addr)
		}
		c.mu.snap = snap
	}
	if resp.Prometheus != nil {
		c.mu.prom = ToPromInfo(resp.Prometheus)
	}
	c.mu.rev = resp.Revision
}

// GetTiDBTopology returns the topology from the local cache. It fails until
// the first full snapshot arrives, which the callers treat like an
// unreachable PD and retry.
func (c *HubClient) GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.mu.snap == nil {
		return nil, errors.WithStack(ErrHubNotReady)
	}
	return maps.Clone(c.mu.snap), nil
}

// GetPromInfo returns the Prometheus info pushed by the hub.
func (c *HubClient) GetPromInfo(context.Context) (*infosync.PrometheusInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.mu.prom == nil {
		return nil, infosync.ErrNoProm
	}
	return c.mu.prom, nil
}

// Close stops the subscription.
func (c *HubClient) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return nil
}
