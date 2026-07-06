// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	LblStatusCode = "code"
)

var (
	DiscoveryRequestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "requests_total",
			Help:      "Number of topology requests served by the discovery hub, by status code.",
		}, []string{LblStatusCode})

	DiscoveryRevisionGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "revision",
			Help:      "The etcd revision that the topology snapshot is derived from.",
		})

	DiscoveryBackendsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "backends",
			Help:      "Number of alive TiDB instances in the topology snapshot.",
		})

	DiscoveryRebootstrapCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "rebootstrap_total",
			Help:      "Number of times the topology watch is re-established from a full read.",
		})
)
