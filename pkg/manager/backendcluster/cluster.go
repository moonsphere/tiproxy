// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/pingcap/tiproxy/lib/config"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/pingcap/tiproxy/pkg/balance/metricsreader"
	"github.com/pingcap/tiproxy/pkg/discovery"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	httputil "github.com/pingcap/tiproxy/pkg/util/http"
	"github.com/pingcap/tiproxy/pkg/util/netutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// topoSource provides the TiDB topology of one cluster. It is either an
// InfoSyncer (discovery-source=pd, watching PD directly) or a discovery
// HubClient (discovery-source=hub, subscribing to a discovery hub).
type topoSource interface {
	GetTiDBTopology(context.Context) (map[string]*infosync.TiDBTopologyInfo, error)
	GetPromInfo(context.Context) (*infosync.PrometheusInfo, error)
	Close() error
}

// Cluster is the cluster-scoped container for one backend PD cluster.
type Cluster struct {
	cfg config.BackendCluster
	// etcdCli is nil when discovery-source is "hub": the cluster then has no
	// PD client at all.
	etcdCli *clientv3.Client
	topo    topoSource
	metrics *metricsreader.ClusterReader
	httpCli *httputil.Client
	dialer  *netutil.DNSDialer
}

func (c *Cluster) Config() config.BackendCluster {
	return c.cfg
}

func (c *Cluster) EtcdClient() *clientv3.Client {
	return c.etcdCli
}

func (c *Cluster) GetTiDBTopology(ctx context.Context) (map[string]*infosync.TiDBTopologyInfo, error) {
	return c.topo.GetTiDBTopology(ctx)
}

func (c *Cluster) GetPromInfo(ctx context.Context) (*infosync.PrometheusInfo, error) {
	return c.topo.GetPromInfo(ctx)
}

func (c *Cluster) HTTPClient() *httputil.Client {
	return c.httpCli
}

func (c *Cluster) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return c.dialer.DialContext(ctx, network, addr)
}

func (c *Cluster) PreClose() {
	if c.metrics != nil {
		c.metrics.PreClose()
	}
}

func (c *Cluster) Close() error {
	if c.metrics != nil {
		c.metrics.Close()
	}
	errs := []error{
		c.topo.Close(),
	}
	if c.etcdCli != nil {
		errs = append(errs, c.etcdCli.Close())
	}
	return errors.Collect(errors.New("close backend cluster"), errs...)
}

// NewCluster creates a new Cluster instance based on the given configuration.
func NewCluster(
	ctx context.Context,
	cfg *config.Config,
	clusterCfg config.BackendCluster,
	clusterTLS func() *tls.Config,
	logger *zap.Logger,
	cfgGetter config.ConfigGetter,
	metricsQuerier *MetricsQuerier,
) (*Cluster, error) {
	clusterCfg = normalizeCluster(clusterCfg)
	nameServers, err := config.ParseNSServers(clusterCfg.NSServers)
	if err != nil {
		return nil, err
	}
	dialer := netutil.NewDNSDialer(nameServers)
	httpCli := httputil.NewHTTPClientWithDialContext(clusterTLS, dialer.DialContext)
	clusterLogger := logger.With(zap.String("cluster", clusterCfg.Name))

	var topo topoSource
	var etcdCli *clientv3.Client
	switch clusterCfg.DiscoverySource {
	case config.DiscoverySourceHub:
		// The topology comes from a discovery hub and the cluster has no PD
		// client at all. This must branch on the source explicitly: PDAddrs
		// has a default value, so it may be non-empty even in hub mode.
		hubClient := discovery.NewHubClient(clusterCfg.HubAddrs, clusterTLS, clusterLogger.Named("hubcli"))
		hubClient.Start(ctx)
		topo = hubClient
	default: // config.DiscoverySourcePD
		var err error
		etcdCli, err = etcd.InitEtcdClientWithAddrsAndDialer(
			clusterLogger.Named("etcd"),
			clusterCfg.PDAddrs,
			clusterTLS(),
			dialer,
		)
		if err != nil {
			return nil, err
		}

		infoSyncer := infosync.NewInfoSyncer(clusterLogger.Named("infosync"), etcdCli)
		if err := infoSyncer.Init(ctx, cfg); err != nil {
			if closeErr := etcdCli.Close(); closeErr != nil {
				logger.Warn("close cluster etcd client failed after infosync init error",
					zap.String("cluster", clusterCfg.Name), zap.Error(closeErr))
			}
			return nil, err
		}
		topo = infoSyncer
	}

	cluster := &Cluster{
		cfg:     clusterCfg,
		etcdCli: etcdCli,
		topo:    topo,
		httpCli: httpCli,
		dialer:  dialer,
	}
	cluster.metrics = metricsreader.NewClusterReader(
		logger.With(zap.String("cluster", clusterCfg.Name)).Named("metrics"),
		clusterCfg.Name,
		cluster,
		cluster,
		httpCli,
		etcdCli,
		config.NewDefaultHealthCheckConfig(),
		cfgGetter,
	)
	for key, query := range metricsQuerier.snapshot() {
		cluster.metrics.AddQueryExpr(key, query.expr, query.rule)
	}
	if err := cluster.metrics.Start(ctx); err != nil {
		_ = topo.Close()
		if etcdCli != nil {
			if closeErr := etcdCli.Close(); closeErr != nil {
				logger.Warn("close cluster etcd client failed after metrics init error",
					zap.String("cluster", clusterCfg.Name), zap.Error(closeErr))
			}
		}
		return nil, err
	}

	return cluster, nil
}
