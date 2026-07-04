package cluster

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEventCleanupDeletesLargeBacklogInBatches(t *testing.T) {
	ctx := testContext(t, 10*time.Second)
	rawURL := (&url.URL{
		Scheme: "badger",
		Path:   t.TempDir(),
		RawQuery: url.Values{
			"node":                   {"badger-batch-cleanup"},
			"event_retention_count":  {"2"},
			"event_cleanup_interval": {"1h"},
		}.Encode(),
	}).String()
	c, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	store, ok := c.store.(*badgerStore)
	require.True(t, ok)
	widgets := defineNamespacedWidgets(t, c, "badgerbatchwidgets")
	batch, err := widgets.Namespace("batch")
	require.NoError(t, err)

	for i := 0; i < defaultEventBatchSize+16; i++ {
		_, err := batch.Create(ctx, fmt.Sprintf("item-%03d", i), widgetSpec{Size: "small"}, CreateOptions{
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
	}
	list, err := batch.List(ctx, ListOptions{})
	require.NoError(t, err)
	before := parseStoredRV(list.ResourceVersion) - 2
	require.Greater(t, before, uint64(defaultEventBatchSize))

	c.cleanupEventsIfMaster(ctx)

	var compacted uint64
	eventPrefixes := []string{
		store.eventPrefix(resourceScope{}),
		store.eventPrefix(resourceScope{Resource: "badgerbatchwidgets", AllNamespaces: true}),
		store.eventPrefix(resourceScope{Resource: "badgerbatchwidgets", Namespace: "batch"}),
	}
	staleByPrefix := make(map[string]int, len(eventPrefixes))
	store.mu.RLock()
	err = store.db.View(func(txn *badger.Txn) error {
		var err error
		compacted, err = store.compactedRV(txn)
		if err != nil {
			return err
		}
		for _, prefix := range eventPrefixes {
			staleByPrefix[prefix] = 0
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(prefix)
			it := txn.NewIterator(opts)
			for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
				if parseRVKey(string(it.Item().Key())) <= before {
					staleByPrefix[prefix]++
				}
			}
			it.Close()
		}
		return nil
	})
	store.mu.RUnlock()
	require.NoError(t, err)
	require.GreaterOrEqual(t, compacted, before)
	for prefix, stale := range staleByPrefix {
		require.Zero(t, stale, prefix)
	}

	waitForWatchError(t, 3*time.Second, func(ctx context.Context) (<-chan WatchEvent[widgetSpec, widgetStatus], error) {
		return batch.Watch(ctx, WatchOptions{Since: "1"})
	}, ErrResourceVersionTooOld)
}

func TestBadgerURLPersistsTypedObjects(t *testing.T) {
	ctx := testContext(t, 5*time.Second)
	rawURL := (&url.URL{
		Scheme:   "badger",
		Path:     t.TempDir(),
		RawQuery: url.Values{"prefix": {"persist"}, "node": {"disk"}}.Encode(),
	}).String()

	c, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	widgets := defineWidgets(t, c, "persistwidgets")
	created, err := widgets.Create(ctx, "disk", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Annotations: Annotations{"tenant": "t1"},
	})
	require.NoError(t, err)
	require.NoError(t, c.Close())

	reopened, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	reopenedWidgets := defineWidgets(t, reopened, "persistwidgets")
	got, err := reopenedWidgets.Get(ctx, "disk")
	require.NoError(t, err)
	require.Equal(t, created.Metadata.UID, got.Metadata.UID)
	require.Equal(t, "team-a", got.Spec.Owner)
}
