// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	LblBroadcastType = "type"

	BroadcastTypeFull  = "full"
	BroadcastTypeDelta = "delta"
)

var (
	DiscoverySubscribersGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "subscribers",
			Help:      "Number of current subscribers of the discovery hub.",
		})

	DiscoverySubDroppedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "sub_dropped_total",
			Help:      "Number of subscribers dropped because they consume too slowly.",
		})

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

	DiscoveryBroadcastCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleProxy,
			Subsystem: LabelDiscovery,
			Name:      "broadcast_total",
			Help:      "Number of topology broadcasts to subscribers.",
		}, []string{LblBroadcastType})
)
