// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"maps"

	"github.com/pingcap/tiproxy/pkg/manager/infosync"
)

// The wire types of the topology endpoint (GET /api/topology). The JSON field
// names are the protocol contract and MUST stay stable: the transport is
// deliberately plain HTTP + JSON so that the topology can be inspected with
// curl and the endpoint can later move into another host (e.g. PD) without
// changing the clients.
//
// Versioning is carried by the ETag/If-None-Match headers: the server bumps
// an opaque version on every content change and answers 304 Not Modified to
// a matching If-None-Match, so the steady-state polling cost is one empty
// round trip. A future long-polling upgrade (?wait=30s) can reuse the same
// headers unchanged.

// TopologyResponse is the full topology of one PD cluster.
type TopologyResponse struct {
	// Revision is the etcd revision that the topology is derived from.
	// It is informational (for observability); freshness is negotiated
	// through the ETag, not this field.
	Revision int64 `json:"revision"`
	// Backends are the alive TiDB instances, sorted by address.
	Backends []TiDBInstance `json:"backends"`
	// Prometheus is the Prometheus of the cluster, if any.
	Prometheus *PrometheusInfo `json:"prometheus,omitempty"`
}

// TiDBInstance is one alive TiDB instance.
type TiDBInstance struct {
	// Addr is the "ip:port" SQL address and is the map key on the client.
	Addr       string            `json:"addr"`
	IP         string            `json:"ip"`
	StatusPort uint              `json:"status_port"`
	Labels     map[string]string `json:"labels,omitempty"`
	Keyspace   string            `json:"keyspace,omitempty"`
	Version    string            `json:"version,omitempty"`
}

// PrometheusInfo mirrors infosync.PrometheusInfo on the wire.
type PrometheusInfo struct {
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	BinaryPath string `json:"binary_path,omitempty"`
}

// ToTiDBInstance converts the topology info to the wire representation.
// GitHash, DeployPath and StartTimestamp are not consumed by any client and
// are intentionally dropped. ClusterName is a local configuration concept
// assigned by the backend cluster manager, so it never goes on the wire.
func ToTiDBInstance(info *infosync.TiDBTopologyInfo) TiDBInstance {
	return TiDBInstance{
		Addr:       info.Addr,
		IP:         info.IP,
		StatusPort: info.StatusPort,
		Labels:     maps.Clone(info.Labels),
		Keyspace:   info.Keyspace,
		Version:    info.Version,
	}
}

// ToTopologyInfo converts the wire representation back to the topology info.
func ToTopologyInfo(inst TiDBInstance) *infosync.TiDBTopologyInfo {
	return &infosync.TiDBTopologyInfo{
		Addr:       inst.Addr,
		IP:         inst.IP,
		StatusPort: inst.StatusPort,
		Labels:     maps.Clone(inst.Labels),
		Keyspace:   inst.Keyspace,
		Version:    inst.Version,
	}
}

// ToPromInfo converts the wire representation back to the Prometheus info.
func ToPromInfo(prom *PrometheusInfo) *infosync.PrometheusInfo {
	if prom == nil {
		return nil
	}
	return &infosync.PrometheusInfo{
		IP:         prom.IP,
		Port:       prom.Port,
		BinaryPath: prom.BinaryPath,
	}
}

// ToWirePromInfo converts the Prometheus info to the wire representation.
func ToWirePromInfo(prom *infosync.PrometheusInfo) *PrometheusInfo {
	if prom == nil {
		return nil
	}
	return &PrometheusInfo{
		IP:         prom.IP,
		Port:       prom.Port,
		BinaryPath: prom.BinaryPath,
	}
}
