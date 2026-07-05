package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWatchScopeMatches(t *testing.T) {
	require.True(t, watchScopeMatches("", []string{"metadata.labels"}))
	require.True(t, watchScopeMatches(WatchScopeObject, []string{"status.phase"}))
	require.True(t, watchScopeMatches(WatchScopeMetadata, []string{"metadata.labels"}))
	require.False(t, watchScopeMatches(WatchScopeMetadata, []string{"status.phase"}))
	require.True(t, watchScopeMatches(WatchScopeStatus, []string{"status.phase"}))
	require.False(t, watchScopeMatches(WatchScopeStatus, []string{"metadata.labels"}))
	require.False(t, watchScopeMatches(WatchScope("broken"), []string{"metadata.labels"}))
}

func TestValidateWatchScope(t *testing.T) {
	require.NoError(t, validateWatchScope(""))
	require.NoError(t, validateWatchScope(WatchScopeMetadata))
	require.ErrorIs(t, validateWatchScope(WatchScope("broken")), ErrInvalidObject)
}
