// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.uber.org/zap"
)

const (
	defPollInterval = 3 * time.Second
	pollTimeout     = 5 * time.Second
	// pollErrLogInterval rate-limits the polling error logs: one poll per
	// interval failing is one log line per errLogInterval failures.
	pollErrLogInterval = 10
)

// ErrHubNotReady is returned before the first topology snapshot arrives.
// The callers (e.g. PDFetcher) retry on errors, so it behaves the same as an
// unreachable PD.
var ErrHubNotReady = errors.New("the discovery hub has not served a topology snapshot yet")

// HubClient polls a discovery hub for the topology and maintains a local
// cache. It is a drop-in replacement for the topology methods of
// infosync.InfoSyncer, so everything downstream of the backend cluster stays
// unchanged. The transport is a plain HTTP GET with If-None-Match: the
// steady state is one empty 304 round trip per interval, and the same
// endpoint can be inspected with curl at any time.
type HubClient struct {
	hubAddrs  []string
	tlsGetter func() *tls.Config
	lg        *zap.Logger
	wg        waitgroup.WaitGroup
	cancel    context.CancelFunc
	// pollIntvl is configurable for tests.
	pollIntvl time.Duration
	httpCli   *http.Client

	mu struct {
		sync.RWMutex
		// snap is nil until the first snapshot arrives.
		snap map[string]*infosync.TiDBTopologyInfo
		prom *infosync.PrometheusInfo
		rev  int64
		etag string
	}
}

// NewHubClient creates a HubClient. hubAddrs is a comma-separated address
// list; tlsGetter provides the cluster TLS config and may return nil for
// plaintext.
func NewHubClient(hubAddrs string, tlsGetter func() *tls.Config, lg *zap.Logger) *HubClient {
	return &HubClient{
		hubAddrs:  config.SplitAddrList(hubAddrs),
		tlsGetter: tlsGetter,
		lg:        lg,
		pollIntvl: defPollInterval,
		httpCli: &http.Client{
			Timeout: pollTimeout,
			Transport: &http.Transport{
				TLSClientConfig: tlsGetter(),
				// The hubs may be redeployed and their DNS entries may lag;
				// polling opens a fresh connection per request to always
				// resolve the current address, like the cluster HTTP client.
				DisableKeepAlives: true,
			},
		},
	}
}

// Start polls the hubs in the background.
func (c *HubClient) Start(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.wg.RunWithRecover(func() {
		c.pollLoop(childCtx)
	}, nil, c.lg)
}

// pollLoop polls one hub per interval. It sticks to the current hub while it
// works and rotates to the next address on any failure. The interval doubles
// as a natural jitter source across the fleet: sidecars start at different
// times, so their poll clocks are not synchronized.
func (c *HubClient) pollLoop(ctx context.Context) {
	// The config validation guarantees a non-empty list; guard against a
	// direct misuse of the constructor.
	if len(c.hubAddrs) == 0 {
		c.lg.Error("no discovery hub address is configured, the topology will never arrive")
		return
	}
	idx := 0
	errCnt := 0
	for ctx.Err() == nil {
		if err := c.poll(ctx, c.hubAddrs[idx]); err != nil && ctx.Err() == nil {
			if errCnt%pollErrLogInterval == 0 {
				c.lg.Warn("polling the discovery hub failed, rotating to the next hub",
					zap.String("addr", c.hubAddrs[idx]), zap.Int("failures", errCnt+1), zap.Error(err))
			}
			errCnt++
			idx = (idx + 1) % len(c.hubAddrs)
		} else {
			errCnt = 0
		}
		select {
		case <-time.After(c.pollIntvl):
		case <-ctx.Done():
			return
		}
	}
}

// poll fetches the topology once and applies it on a content change.
func (c *HubClient) poll(ctx context.Context, addr string) error {
	scheme := "http"
	if c.tlsGetter() != nil {
		scheme = "https"
	}
	url := fmt.Sprintf("%s://%s/api/topology", scheme, addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	c.mu.RLock()
	etag := c.mu.etag
	c.mu.RUnlock()
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := c.httpCli.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil
	case http.StatusOK:
	default:
		return errors.Errorf("http status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.WithStack(err)
	}
	var topo TopologyResponse
	if err := json.Unmarshal(body, &topo); err != nil {
		return errors.WithStack(err)
	}
	c.apply(&topo, resp.Header.Get("ETag"))
	return nil
}

// apply replaces the local cache. The cache is copy-on-write: the maps handed
// out by GetTiDBTopology share the pointers, so values are never mutated.
func (c *HubClient) apply(topo *TopologyResponse, etag string) {
	snap := make(map[string]*infosync.TiDBTopologyInfo, len(topo.Backends))
	for _, inst := range topo.Backends {
		snap[inst.Addr] = ToTopologyInfo(inst)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.snap = snap
	if topo.Prometheus != nil {
		c.mu.prom = ToPromInfo(topo.Prometheus)
	}
	c.mu.rev = topo.Revision
	c.mu.etag = etag
}

// GetTiDBTopology returns the topology from the local cache. It fails until
// the first snapshot arrives, which the callers treat like an unreachable PD
// and retry.
func (c *HubClient) GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.mu.snap == nil {
		return nil, errors.WithStack(ErrHubNotReady)
	}
	return maps.Clone(c.mu.snap), nil
}

// GetPromInfo returns the Prometheus info served by the hub.
func (c *HubClient) GetPromInfo(context.Context) (*infosync.PrometheusInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.mu.prom == nil {
		return nil, infosync.ErrNoProm
	}
	return c.mu.prom, nil
}

// Close stops the polling.
func (c *HubClient) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	return nil
}
