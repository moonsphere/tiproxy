// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/discovery"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	mgrcfg "github.com/pingcap/tiproxy/pkg/manager/config"
	"github.com/pingcap/tiproxy/pkg/metrics"
	"github.com/pingcap/tiproxy/pkg/sctx"
	"github.com/pingcap/tiproxy/pkg/server/api"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"go.uber.org/atomic"
)

// NewDiscoveryServer creates a server that runs as a discovery hub: it
// watches the TiDB topology of one PD cluster and pushes it to sidecar
// TiProxy instances. It reuses the Server struct with only the components
// that the hub needs; the other fields stay nil and Close handles them.
func NewDiscoveryServer(ctx context.Context, sctx *sctx.Context) (srv *Server, err error) {
	srv = &Server{
		configManager:  mgrcfg.NewConfigManager(),
		metricsManager: metrics.NewMetricsManager(),
		certManager:    cert.NewCertManager(),
	}
	ready := atomic.NewBool(false)

	cfg, lg, err := srv.initBase(ctx, sctx)
	if err != nil {
		return
	}

	// The hub serves the topology of exactly one PD cluster.
	clusters := cfg.GetBackendClusters()
	if len(clusters) != 1 {
		err = errors.Errorf("discovery mode requires exactly one PD cluster (proxy.pd-addrs or one proxy.backend-clusters entry), got %d", len(clusters))
		return
	}
	if clusters[0].DiscoverySource == config.DiscoverySourceHub {
		// A hub must watch PD itself: subscribing to another hub is not supported.
		err = errors.Errorf("discovery mode requires a PD-sourced cluster, but %q uses discovery-source=hub", clusters[0].Name)
		return
	}
	srv.hubEtcdCli, err = etcd.InitEtcdClientWithAddrs(lg.Named("etcd"), clusters[0].PDAddrs, srv.certManager.ClusterTLS())
	if err != nil {
		return
	}

	// The server is ready only after the first topology bootstrap, so that
	// the readiness probe (GET /api/topology or /debug/health) keeps the
	// pollers away from an empty hub.
	srv.hub = discovery.NewHub(lg.Named("hub"), srv.hubEtcdCli, func() {
		ready.Toggle()
	})
	srv.hub.Run(ctx)

	srv.apiServer, err = api.NewDiscoveryServer(cfg.API, lg.Named("api"), srv.configManager, srv.certManager, srv.hub, ready)
	return
}
