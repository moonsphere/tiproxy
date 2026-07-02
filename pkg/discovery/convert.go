// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"maps"

	"github.com/pingcap/tiproxy/pkg/discovery/pb"
	"github.com/pingcap/tiproxy/pkg/manager/infosync"
)

// ToTiDBInstance converts the topology info to the wire representation.
// GitHash, DeployPath and StartTimestamp are not consumed by any subscriber
// and are intentionally dropped. ClusterName is a local configuration concept
// assigned by the backend cluster manager, so it never goes on the wire.
func ToTiDBInstance(info *infosync.TiDBTopologyInfo) *pb.TiDBInstance {
	return &pb.TiDBInstance{
		Addr:       info.Addr,
		Ip:         info.IP,
		StatusPort: uint32(info.StatusPort),
		Labels:     maps.Clone(info.Labels),
		Keyspace:   info.Keyspace,
		Version:    info.Version,
	}
}

// ToTopologyInfo converts the wire representation back to the topology info.
func ToTopologyInfo(inst *pb.TiDBInstance) *infosync.TiDBTopologyInfo {
	return &infosync.TiDBTopologyInfo{
		Addr:       inst.Addr,
		IP:         inst.Ip,
		StatusPort: uint(inst.StatusPort),
		Labels:     maps.Clone(inst.Labels),
		Keyspace:   inst.Keyspace,
		Version:    inst.Version,
	}
}

// ToPromInfo converts the wire representation back to the Prometheus info.
func ToPromInfo(prom *pb.PrometheusInfo) *infosync.PrometheusInfo {
	if prom == nil {
		return nil
	}
	return &infosync.PrometheusInfo{
		IP:         prom.Ip,
		Port:       int(prom.Port),
		BinaryPath: prom.BinaryPath,
	}
}

// ToPbPromInfo converts the Prometheus info to the wire representation.
func ToPbPromInfo(prom *infosync.PrometheusInfo) *pb.PrometheusInfo {
	if prom == nil {
		return nil
	}
	return &pb.PrometheusInfo{
		Ip:         prom.IP,
		Port:       int32(prom.Port),
		BinaryPath: prom.BinaryPath,
	}
}
