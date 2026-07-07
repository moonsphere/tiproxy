// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
)

// TestTopologyResponseGoldenJSON pins the wire format served by the PD-side
// tidb-discovery service (pingkai/pd pkg/mcs/tidbdiscovery). The same golden
// string is tested on the PD side; do not change the JSON without
// coordinating both repositories.
func TestTopologyResponseGoldenJSON(t *testing.T) {
	golden := `{"revision":254674,"backends":[{"addr":"10.0.0.1:4000","ip":"10.0.0.1","status_port":10080,"labels":{"zone":"z1"},"keyspace":"ks1","version":"v8.5.5"}],"prometheus":{"ip":"10.0.0.9","port":9090}}`

	var decoded TopologyResponse
	require.NoError(t, json.Unmarshal([]byte(golden), &decoded))
	require.Equal(t, int64(254674), decoded.Revision)
	require.Len(t, decoded.Backends, 1)
	inst := decoded.Backends[0]
	require.Equal(t, "10.0.0.1:4000", inst.Addr)
	require.Equal(t, "10.0.0.1", inst.IP)
	require.Equal(t, uint(10080), inst.StatusPort)
	require.Equal(t, map[string]string{"zone": "z1"}, inst.Labels)
	require.Equal(t, "ks1", inst.Keyspace)
	require.Equal(t, "v8.5.5", inst.Version)
	require.Equal(t, &PrometheusInfo{IP: "10.0.0.9", Port: 9090}, decoded.Prometheus)

	// The re-serialization is byte-identical: both sides marshal the same way.
	data, err := json.Marshal(&decoded)
	require.NoError(t, err)
	require.Equal(t, golden, string(data))

	// The conversion into the internal topology keeps every wire field.
	info := ToTopologyInfo(inst)
	require.Equal(t, &infosync.TiDBTopologyInfo{
		Addr:       "10.0.0.1:4000",
		IP:         "10.0.0.1",
		StatusPort: 10080,
		Labels:     map[string]string{"zone": "z1"},
		Keyspace:   "ks1",
		Version:    "v8.5.5",
	}, info)
}
