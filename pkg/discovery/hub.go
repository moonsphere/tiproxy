// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/lib/util/retry"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/pingcap/tiproxy/pkg/util/waitgroup"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// ttlKeySuffix and infoKeySuffix are the suffixes of the full etcd keys,
	// e.g. /topology/tidb/{addr}/ttl.
	ttlKeySuffix  = "/ttl"
	infoKeySuffix = "/info"

	defBootstrapRetryIntvl = 3 * time.Second
	defPromRefreshIntvl    = 30 * time.Second
	getPromTimeout         = 2 * time.Second
	logInterval            = 10
)

// Hub watches the TiDB topology of one PD cluster and serves it over plain
// HTTP + JSON (GET /api/topology). The transport is deliberately the simplest
// possible one: a control plane is insensitive to latency but extremely
// sensitive to observability, and an HTTP endpoint can be inspected with curl
// and polled with no client dependencies. Freshness is negotiated with
// ETag/If-None-Match, so a steady-state poll is one empty 304 round trip.
type Hub struct {
	etcdCli *clientv3.Client
	lg      *zap.Logger
	wg      waitgroup.WaitGroup
	cancel  context.CancelFunc
	// onReady is called once after the first successful bootstrap.
	onReady   func()
	readyOnce sync.Once

	// bootstrapRetryIntvl and promRefreshIntvl are configurable for tests.
	bootstrapRetryIntvl time.Duration
	promRefreshIntvl    time.Duration
	// parseCnt counts snapshot re-derivations, exposed for tests to verify
	// that the ttl-refresh short circuit works.
	parseCnt atomic.Int64

	mu struct {
		sync.RWMutex
		// raw holds all watched KVs (both info and ttl keys). It is only
		// written by the watch loop.
		raw map[string][]byte
		// snap is the live topology derived from raw. It is copy-on-write:
		// updates always build a new map with new pointers.
		snap map[string]*infosync.TiDBTopologyInfo
		prom *infosync.PrometheusInfo
		// rev is the etcd revision the snapshot is derived from
		// (informational).
		rev int64
		// respJSON is the serialized TopologyResponse, rebuilt on every
		// content change and shared by all requests.
		respJSON []byte
		// etag is a hash of respJSON. A content hash (not a local counter)
		// so that it stays meaningful across hub replicas: a client failing
		// over to another hub must not get a spurious 304 from a colliding
		// counter, and identical content legitimately answers 304.
		etag string
	}
}

// NewHub creates a Hub. onReady is called once after the first successful
// bootstrap and may be nil.
func NewHub(lg *zap.Logger, etcdCli *clientv3.Client, onReady func()) *Hub {
	h := &Hub{
		lg:                  lg,
		etcdCli:             etcdCli,
		onReady:             onReady,
		bootstrapRetryIntvl: defBootstrapRetryIntvl,
		promRefreshIntvl:    defPromRefreshIntvl,
	}
	return h
}

// Run starts the watch loop and the prometheus refresh loop.
func (h *Hub) Run(ctx context.Context) {
	childCtx, cancel := context.WithCancel(ctx)
	h.cancel = cancel
	h.wg.RunWithRecover(func() {
		h.watchLoop(childCtx)
	}, nil, h.lg)
	h.wg.RunWithRecover(func() {
		h.promLoop(childCtx)
	}, nil, h.lg)
}

// Close stops the hub.
func (h *Hub) Close() error {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
	return nil
}

// ParseCount returns how many times the snapshot was re-derived. Only for tests.
func (h *Hub) ParseCount() int64 {
	return h.parseCnt.Load()
}

// HandleTopology serves GET /api/topology. A matching If-None-Match answers
// 304 with no body; otherwise the cached serialized response is written with
// the current ETag.
func (h *Hub) HandleTopology(c *gin.Context) {
	h.mu.RLock()
	respJSON, etag := h.mu.respJSON, h.mu.etag
	h.mu.RUnlock()
	if respJSON == nil {
		// Not bootstrapped. The readiness gate normally keeps requests away;
		// this is a defensive answer for direct probes.
		metrics.DiscoveryRequestCounter.WithLabelValues("503").Inc()
		c.JSON(http.StatusServiceUnavailable, "the topology is not bootstrapped yet")
		return
	}
	if c.GetHeader("If-None-Match") == etag {
		metrics.DiscoveryRequestCounter.WithLabelValues("304").Inc()
		c.Header("ETag", etag)
		c.Status(http.StatusNotModified)
		return
	}
	metrics.DiscoveryRequestCounter.WithLabelValues("200").Inc()
	c.Header("ETag", etag)
	c.Data(http.StatusOK, "application/json", respJSON)
}

// watchLoop is the only goroutine that talks to PD for the topology. It
// bootstraps a full snapshot and then applies watch events incrementally.
// On any watch failure it falls back to a full re-bootstrap.
func (h *Hub) watchLoop(ctx context.Context) {
	for ctx.Err() == nil {
		baseRev, err := h.bootstrap(ctx)
		if err != nil {
			// Only fails when ctx is cancelled: bootstrap retries infinitely.
			return
		}
		h.readyOnce.Do(func() {
			if h.onReady != nil {
				h.onReady()
			}
		})

		// Both watches are pinned to the same revision that the bootstrap
		// snapshot was read at, so no event is lost or duplicated.
		wch1 := h.etcdCli.Watch(ctx, infosync.TiDBTopologyPath, clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
		wch2 := h.etcdCli.Watch(ctx, infosync.TiDBKeyspaceTopologyPath, clientv3.WithPrefix(), clientv3.WithRev(baseRev+1))
	watch:
		for {
			// A closed watch channel yields zero values with Canceled=false,
			// so the ok flag must be checked to avoid a hot loop.
			select {
			case wr, ok := <-wch1:
				if !ok || wr.Canceled {
					break watch
				}
				h.applyEvents(wr.Events, wr.Header.Revision)
			case wr, ok := <-wch2:
				if !ok || wr.Canceled {
					break watch
				}
				h.applyEvents(wr.Events, wr.Header.Revision)
			case <-ctx.Done():
				return
			}
		}
		metrics.DiscoveryRebootstrapCounter.Inc()
		h.lg.Warn("topology watch is cancelled, re-bootstrapping")
	}
}

// bootstrap reads both topology prefixes at one consistent revision and
// replaces the snapshot. It retries infinitely until the context is cancelled.
func (h *Hub) bootstrap(ctx context.Context) (int64, error) {
	var txnResp *clientv3.TxnResponse
	err := retry.RetryNotify(func() error {
		var err error
		// A transaction reads both ranges at a single revision. Two separate
		// Gets may return at different revisions, which would make the watch
		// start point inconsistent with the snapshot.
		txnResp, err = h.etcdCli.Txn(ctx).Then(
			clientv3.OpGet(infosync.TiDBTopologyPath, clientv3.WithPrefix()),
			clientv3.OpGet(infosync.TiDBKeyspaceTopologyPath, clientv3.WithPrefix()),
		).Commit()
		return errors.WithStack(err)
	}, ctx, h.bootstrapRetryIntvl, retry.InfiniteCnt,
		func(err error, _ time.Duration) {
			h.lg.Error("bootstrap topology failed, retrying", zap.Error(err))
		}, logInterval)
	if err != nil {
		return 0, err
	}

	raw := make(map[string][]byte)
	for _, resp := range txnResp.Responses {
		for _, kv := range resp.GetResponseRange().Kvs {
			raw[string(kv.Key)] = kv.Value
		}
	}
	snap := infosync.ParseTiDBTopology(h.lg, raw)
	h.parseCnt.Inc()
	baseRev := txnResp.Header.Revision

	h.mu.Lock()
	h.mu.raw = raw
	h.mu.snap = snap
	h.mu.rev = baseRev
	h.rebuildRespLocked()
	h.mu.Unlock()

	h.lg.Info("topology bootstrapped", zap.Int64("revision", baseRev), zap.Int("backends", len(snap)))
	return baseRev, nil
}

// applyEvents applies one watch event batch.
func (h *Hub) applyEvents(events []*clientv3.Event, rev int64) {
	if len(events) == 0 {
		return
	}
	// Short circuit: the liveness check only cares about the existence of the
	// ttl keys, not their values, and each TiDB re-puts its ttl (a timestamp)
	// and info keys periodically. A batch that only overwrites existing keys
	// with the same liveness cannot change the snapshot, so skip the
	// re-derivation entirely. This collapses the steady-state cost to zero.
	changed := false
	for _, ev := range events {
		key := string(ev.Kv.Key)
		switch ev.Type {
		case mvccpb.DELETE:
			changed = true
		case mvccpb.PUT:
			old, ok := h.mu.raw[key]
			if !ok {
				changed = true
			} else if strings.HasSuffix(key, infoKeySuffix) && !bytes.Equal(old, ev.Kv.Value) {
				changed = true
			}
		}
		if changed {
			break
		}
	}
	if !changed {
		// raw is only written by this goroutine and never read by others,
		// so it can be updated in place.
		for _, ev := range events {
			h.mu.raw[string(ev.Kv.Key)] = ev.Kv.Value
		}
		return
	}

	raw := maps.Clone(h.mu.raw)
	for _, ev := range events {
		key := string(ev.Kv.Key)
		switch ev.Type {
		case mvccpb.PUT:
			raw[key] = ev.Kv.Value
		case mvccpb.DELETE:
			delete(raw, key)
		}
	}
	snap := infosync.ParseTiDBTopology(h.lg, raw)
	h.parseCnt.Inc()

	h.mu.Lock()
	upserted, removed := diffSnap(h.mu.snap, snap)
	h.mu.raw = raw
	h.mu.snap = snap
	h.mu.rev = max(h.mu.rev, rev)
	if len(upserted) > 0 || len(removed) > 0 {
		h.rebuildRespLocked()
		h.lg.Info("topology changed", zap.Strings("upserted", upserted),
			zap.Strings("removed", removed), zap.Int64("revision", h.mu.rev))
	}
	h.mu.Unlock()
}

// diffSnap compares two snapshots and returns the changed addresses, sorted,
// for logging and change detection.
func diffSnap(oldSnap, newSnap map[string]*infosync.TiDBTopologyInfo) (upserted, removed []string) {
	for _, addr := range slices.Sorted(maps.Keys(newSnap)) {
		oldInfo, ok := oldSnap[addr]
		if !ok || !instanceEqual(oldInfo, newSnap[addr]) {
			upserted = append(upserted, addr)
		}
	}
	for _, addr := range slices.Sorted(maps.Keys(oldSnap)) {
		if _, ok := newSnap[addr]; !ok {
			removed = append(removed, addr)
		}
	}
	return
}

// instanceEqual compares the fields that go on the wire.
func instanceEqual(a, b *infosync.TiDBTopologyInfo) bool {
	return a.IP == b.IP &&
		a.StatusPort == b.StatusPort &&
		a.Keyspace == b.Keyspace &&
		a.Version == b.Version &&
		maps.Equal(a.Labels, b.Labels)
}

// rebuildRespLocked reserializes the response and bumps the version (the
// ETag). It must be called with the lock held whenever the content changes.
// Serializing once per change keeps every request O(1): the bytes are shared
// by all requests, which is safe because they are never mutated.
func (h *Hub) rebuildRespLocked() {
	backends := make([]TiDBInstance, 0, len(h.mu.snap))
	for _, addr := range slices.Sorted(maps.Keys(h.mu.snap)) {
		backends = append(backends, ToTiDBInstance(h.mu.snap[addr]))
	}
	resp := TopologyResponse{
		Revision:   h.mu.rev,
		Backends:   backends,
		Prometheus: ToWirePromInfo(h.mu.prom),
	}
	respJSON, err := json.Marshal(&resp)
	if err != nil {
		// Marshalling plain structs cannot fail; keep the old response if it
		// somehow does.
		h.lg.Error("marshal topology response failed", zap.Error(err))
		return
	}
	hash := fnv.New64a()
	_, _ = hash.Write(respJSON)
	h.mu.respJSON = respJSON
	h.mu.etag = strconv.FormatUint(hash.Sum64(), 16)
	metrics.DiscoveryRevisionGauge.Set(float64(h.mu.rev))
	metrics.DiscoveryBackendsGauge.Set(float64(len(h.mu.snap)))
}

// promLoop periodically refreshes the Prometheus info; a change bumps the
// version so that pollers pick it up.
func (h *Hub) promLoop(ctx context.Context) {
	ticker := time.NewTicker(h.promRefreshIntvl)
	defer ticker.Stop()
	for {
		h.refreshProm(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *Hub) refreshProm(ctx context.Context) {
	kvs, err := etcd.GetKVs(ctx, h.etcdCli, infosync.PromTopologyPath,
		[]clientv3.OpOption{clientv3.WithPrefix()}, getPromTimeout, 0, 3)
	if err != nil {
		if ctx.Err() == nil {
			h.lg.Warn("fetch prometheus info failed", zap.Error(err))
		}
		return
	}
	if len(kvs) == 0 {
		return
	}
	var prom infosync.PrometheusInfo
	if err := json.Unmarshal(kvs[0].Value, &prom); err != nil {
		h.lg.Warn("unmarshal prometheus info failed", zap.Error(err))
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.prom != nil && *h.mu.prom == prom {
		return
	}
	h.mu.prom = &prom
	if h.mu.respJSON == nil {
		// Not bootstrapped yet: the first bootstrap will carry it.
		return
	}
	h.rebuildRespLocked()
}
