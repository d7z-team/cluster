package cluster

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterOptionsFromURL(t *testing.T) {
	parsed := &url.URL{
		Scheme: "memory",
		RawQuery: url.Values{
			"node":                         {"worker-a"},
			"prefix":                       {"control"},
			"node_lease_ttl":               {"45s"},
			"node_renew_interval":          {"15s"},
			"master_lease_ttl":             {"30s"},
			"master_renew_interval":        {"10s"},
			"master_history_limit":         {"9"},
			"event_retention_count":        {"11"},
			"event_cleanup_interval":       {"25ms"},
			"admission_timeout":            {"5s"},
			"admission_retention_count":    {"7"},
			"admission_terminal_retention": {"2m"},
			"watch_buffer_size":            {"64"},
		}.Encode(),
	}

	options, err := clusterOptionsFromURL(parsed)
	require.NoError(t, err)
	require.Equal(t, "worker-a", options.NodeName)
	require.Equal(t, "control", options.Prefix)
	require.Equal(t, 45*time.Second, options.NodeLeaseTTL)
	require.Equal(t, 15*time.Second, options.NodeRenewInterval)
	require.Equal(t, 30*time.Second, options.MasterLeaseTTL)
	require.Equal(t, 10*time.Second, options.MasterRenewInterval)
	require.Equal(t, 9, options.MasterHistoryLimit)
	require.Equal(t, 11, options.EventRetentionCount)
	require.Equal(t, 25*time.Millisecond, options.EventCleanupInterval)
	require.Equal(t, 5*time.Second, options.AdmissionTimeout)
	require.Equal(t, 7, options.AdmissionRetentionCount)
	require.Equal(t, 2*time.Minute, options.AdmissionTerminalRetention)
	require.Equal(t, 64, options.WatchBufferSize)
}

func TestClusterOptionsFromURLRejectsInvalidValues(t *testing.T) {
	testCases := []string{
		"node_lease_ttl=0s",
		"node_renew_interval=1ms",
		"master_lease_ttl=0s",
		"master_renew_interval=1ms",
		"master_history_limit=0",
		"event_retention_count=0",
		"event_cleanup_interval=1ms",
		"admission_timeout=0s",
		"admission_retention_count=0",
		"admission_terminal_retention=0s",
		"watch_buffer_size=0",
	}

	for _, rawQuery := range testCases {
		t.Run(rawQuery, func(t *testing.T) {
			_, err := clusterOptionsFromURL(&url.URL{RawQuery: rawQuery})
			require.ErrorIs(t, err, ErrInvalidConfig)
		})
	}
}
