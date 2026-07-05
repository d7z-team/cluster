package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type widgetSpec struct {
	Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
	Owner string `json:"owner,omitempty" cluster:"immutable,index=owner"`
}

type widgetStatus struct {
	Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index=phase"`
}

type clusterURLFactory struct {
	name string
	raw  func(t *testing.T, query url.Values) string
}

type cleanupErrorStore struct {
	resourceStore
	mu  sync.RWMutex
	err error
}

func (s *cleanupErrorStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *cleanupErrorStore) cleanupEvents(ctx context.Context) error {
	s.mu.RLock()
	err := s.err
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return s.resourceStore.cleanupEvents(ctx)
}

func memoryURLFactory() clusterURLFactory {
	counter := 0
	return clusterURLFactory{
		name: "memory",
		raw: func(t *testing.T, query url.Values) string {
			t.Helper()
			counter++
			if query.Get("node") == "" {
				query.Set("node", fmt.Sprintf("memory-%d", counter))
			}
			return (&url.URL{Scheme: "memory", RawQuery: query.Encode()}).String()
		},
	}
}

func badgerURLFactory(t *testing.T) clusterURLFactory {
	t.Helper()
	root := t.TempDir()
	counter := 0
	return clusterURLFactory{
		name: "badger",
		raw: func(t *testing.T, query url.Values) string {
			t.Helper()
			counter++
			if query.Get("node") == "" {
				query.Set("node", fmt.Sprintf("badger-%d", counter))
			}
			return (&url.URL{
				Scheme:   "badger",
				Path:     fmt.Sprintf("%s/%d", root, counter),
				RawQuery: query.Encode(),
			}).String()
		},
	}
}

func newURLCluster(t *testing.T, factory clusterURLFactory, query url.Values) *Cluster {
	t.Helper()
	copiedQuery := url.Values{}
	for key, values := range query {
		copiedQuery[key] = append([]string(nil), values...)
	}
	c, err := NewClusterFromURL(factory.raw(t, copiedQuery))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func testContext(t *testing.T, timeout time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

func defineWidgets(t *testing.T, c *Cluster, resource string) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	return defineWidgetResource(t, c, resource, false)
}

func defineNamespacedWidgets(t *testing.T, c *Cluster, resource string) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	return defineWidgetResource(t, c, resource, true)
}

func defineWidgetResource(t *testing.T, c *Cluster, resource string, namespaced bool) *Resource[widgetSpec, widgetStatus] {
	t.Helper()
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   resource,
		APIVersion: "example.test/v1",
		Kind:       "Widget",
		Namespaced: namespaced,
	})
	require.NoError(t, err)
	return widgets
}

func requireNoWatchEvent[S, T any](t *testing.T, events <-chan WatchEvent[S, T], wait time.Duration) {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		t.Fatalf("unexpected watch event: %#v", event)
	case <-time.After(wait):
	}
}

func nextWatchEvent[S, T any](t *testing.T, events <-chan WatchEvent[S, T]) WatchEvent[S, T] {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return WatchEvent[S, T]{}
	}
}

func waitForWatchError[S, T any](
	t *testing.T,
	timeout time.Duration,
	open func(context.Context) (<-chan WatchEvent[S, T], error),
	target error,
) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		watchCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		events, err := open(watchCtx)
		require.NoError(t, err)
		select {
		case event, ok := <-events:
			cancel()
			require.True(t, ok)
			if event.Type == WatchError && errors.Is(event.Error, target) {
				return
			}
		case <-watchCtx.Done():
			cancel()
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for watch error: %v", target)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func nextMasterWatchEvent(t *testing.T, events <-chan MasterWatchEvent) MasterWatchEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for master watch event")
		return MasterWatchEvent{}
	}
}

func nextUnstructuredWatchEvent(t *testing.T, events <-chan UnstructuredWatchEvent) UnstructuredWatchEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for unstructured watch event")
		return UnstructuredWatchEvent{}
	}
}

func requireNoUnstructuredWatchEvent(t *testing.T, events <-chan UnstructuredWatchEvent, wait time.Duration) {
	t.Helper()
	select {
	case event, ok := <-events:
		require.True(t, ok)
		t.Fatalf("unexpected unstructured watch event: %#v", event)
	case <-time.After(wait):
	}
}

func runClusterURLContract(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	t.Run("crud", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "widgets")
		ctx := testContext(t, 5*time.Second)
		created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		require.NotEmpty(t, created.Metadata.ResourceVersion)
		got, err := widgets.Get(ctx, "alpha")
		require.NoError(t, err)
		require.Equal(t, created.Metadata.UID, got.Metadata.UID)
	})

	t.Run("schema", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "validatedwidgets")
		ctx := testContext(t, 5*time.Second)
		created, err := widgets.Create(ctx, "defaulted", widgetSpec{Owner: "team-a"}, CreateOptions{})
		require.NoError(t, err)
		require.Equal(t, "medium", created.Spec.Size)
		_, err = widgets.Patch(ctx, "defaulted", []byte(`{"spec":{"size":"xlarge"}}`), PatchOptions{})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("list", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "listedwidgets")
		ctx := testContext(t, 5*time.Second)
		_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small"}, CreateOptions{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		})
		require.NoError(t, err)
		selected, err := widgets.List(ctx, ListOptions{
			Selector: Where(Label("app").Eq("demo"), Annotation("tenant").Eq("t1")),
		})
		require.NoError(t, err)
		require.Len(t, selected.Items, 1)
	})

	t.Run("watch", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		widgets := defineWidgets(t, c, "watchedwidgets")
		ctx := testContext(t, 5*time.Second)
		_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small"}, CreateOptions{})
		require.NoError(t, err)
		list, err := widgets.List(ctx, ListOptions{})
		require.NoError(t, err)
		watchCtx := testContext(t, 3*time.Second)
		events, err := widgets.Watch(watchCtx, WatchOptions{ResourceVersion: list.ResourceVersion})
		require.NoError(t, err)
		_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		require.NoError(t, err)
		event := nextWatchEvent(t, events)
		require.Equal(t, WatchModified, event.Type)
	})

	t.Run("master", func(t *testing.T) {
		c := newURLCluster(t, factory, nil)
		ctx := testContext(t, 5*time.Second)
		master, err := c.Master(ctx)
		require.NoError(t, err)
		require.True(t, master.Valid)
	})
}
