// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// NewDiscoveryServer creates the API server for the discovery hub mode.
// Compared with NewServer, it only serves the TiDBDiscovery gRPC service,
// the gRPC health service, diagnostics, metrics and debug endpoints: there
// is no proxy, so no namespace/config/backend/traffic routes.
func NewDiscoveryServer(cfg config.API, lg *zap.Logger, cfgMgr ConfigManager, certMgr *mgrcrt.CertManager,
	hub pb.TiDBDiscoveryServer, healthSrv *health.Server, ready *atomic.Bool) (*Server, error) {
	h, engine, err := newBaseServer(cfg, lg, Managers{CfgMgr: cfgMgr, CertMgr: certMgr}, ready)
	if err != nil {
		return nil, err
	}

	pb.RegisterTiDBDiscoveryServer(h.grpc, hub)
	healthpb.RegisterHealthServer(h.grpc, healthSrv)
	h.registerGrpc(cfgMgr)
	// The paths are consistent with the proxy API server.
	h.registerMetrics(engine.Group("metrics"))
	h.registerDebug(engine.Group("debug"))

	h.start(engine)
	return h, nil
}
