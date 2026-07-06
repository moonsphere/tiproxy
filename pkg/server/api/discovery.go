// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiproxy/lib/config"
	mgrcrt "github.com/pingcap/tiproxy/pkg/manager/cert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TopologyHandler serves the topology endpoint of the discovery hub.
type TopologyHandler interface {
	HandleTopology(c *gin.Context)
}

// NewDiscoveryServer creates the API server for the discovery hub mode.
// Compared with NewServer, it only serves the topology endpoint,
// diagnostics, metrics and debug endpoints: there is no proxy, so no
// namespace/config/backend/traffic routes.
func NewDiscoveryServer(cfg config.API, lg *zap.Logger, cfgMgr ConfigManager, certMgr *mgrcrt.CertManager,
	hub TopologyHandler, ready *atomic.Bool) (*Server, error) {
	h, engine, err := newBaseServer(cfg, lg, Managers{CfgMgr: cfgMgr, CertMgr: certMgr}, ready)
	if err != nil {
		return nil, err
	}

	h.registerGrpc(cfgMgr)
	// The paths are consistent with the proxy API server.
	engine.Group("api").GET("/topology", hub.HandleTopology)
	h.registerMetrics(engine.Group("metrics"))
	h.registerDebug(engine.Group("debug"))

	h.start(engine)
	return h, nil
}
