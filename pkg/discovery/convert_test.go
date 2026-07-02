// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"testing"

	"github.com/pingcap/tiproxy/pkg/manager/infosync"
	"github.com/stretchr/testify/require"
)

func TestConvertRoundtrip(t *testing.T) {
	info := &infosync.TiDBTopologyInfo{
		Addr:       "1.1.1.1:4000",
		IP:         "1.1.1.1",
		StatusPort: 10080,
		Labels:     map[string]string{"zone": "z1"},
		Keyspace:   "ks1",
		Version:    "v9.0.0",
	}
	require.Equal(t, info, ToTopologyInfo(ToTiDBInstance(info)))

	prom := &infosync.PrometheusInfo{
		IP:         "2.2.2.2",
		Port:       9090,
		BinaryPath: "/prom",
	}
	require.Equal(t, prom, ToPromInfo(ToPbPromInfo(prom)))
	require.Nil(t, ToPromInfo(nil))
	require.Nil(t, ToPbPromInfo(nil))
}
