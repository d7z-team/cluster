package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWatchHubSubscribe(t *testing.T) {
	hub := newWatchHub()
	require.NotNil(t, hub)

	ch, cancel, err := hub.subscribe(resourceScope{Resource: "widgets"})
	require.NoError(t, err)
	require.NotNil(t, ch)
	require.NotNil(t, cancel)

	cancel()
	_, ok := <-ch
	require.False(t, ok)
}

func TestWatchHubSubscribeClosed(t *testing.T) {
	hub := newWatchHub()
	hub.close()

	ch, cancel, err := hub.subscribe(resourceScope{Resource: "widgets"})
	require.NoError(t, err)
	require.NotNil(t, ch)

	_, ok := <-ch
	require.False(t, ok)

	cancel()
}

func TestWatchHubNotify(t *testing.T) {
	hub := newWatchHub()

	ch1, cancel1, _ := hub.subscribe(resourceScope{Resource: "widgets"})
	defer cancel1()
	ch2, cancel2, _ := hub.subscribe(resourceScope{Resource: "widgets", Namespace: "ns"})
	defer cancel2()
	ch3, cancel3, _ := hub.subscribe(resourceScope{Resource: "other"})
	defer cancel3()

	hub.notify(objectRef{Resource: "widgets", Name: "test"})

	select {
	case <-ch1:
	default:
		t.Fatal("expected notification on all-namespace channel")
	}

	select {
	case <-ch3:
		t.Fatal("unexpected notification on other resource channel")
	default:
	}

	hub.notify(objectRef{Resource: "widgets", Namespace: "ns", Name: "test"})

	select {
	case <-ch2:
	default:
		t.Fatal("expected notification on namespace channel")
	}
}

func TestWatchHubNotifyNonBlocking(t *testing.T) {
	hub := newWatchHub()
	ch, cancel, _ := hub.subscribe(resourceScope{Resource: "widgets"})
	defer cancel()

	hub.notify(objectRef{Resource: "widgets", Name: "a"})
	hub.notify(objectRef{Resource: "widgets", Name: "b"})

	event := drainChannel(t, ch, 100*time.Millisecond)
	require.Equal(t, 1, event)
}

func TestWatchHubClose(t *testing.T) {
	hub := newWatchHub()
	ch1, _, _ := hub.subscribe(resourceScope{Resource: "widgets"})
	ch2, _, _ := hub.subscribe(resourceScope{Resource: "widgets"})

	hub.close()

	_, ok := <-ch1
	require.False(t, ok)
	_, ok = <-ch2
	require.False(t, ok)

	hub.close()
}

func TestWatchHubDoubleClose(t *testing.T) {
	hub := newWatchHub()
	hub.close()
	hub.close()
}

func TestWatchKey(t *testing.T) {
	require.Equal(t, "widgets", watchKey("widgets", ""))
	require.Equal(t, "widgets\x00ns", watchKey("widgets", "ns"))
}

func drainChannel(t *testing.T, ch <-chan struct{}, timeout time.Duration) int {
	t.Helper()
	count := 0
	for {
		select {
		case <-ch:
			count++
		case <-time.After(timeout):
			return count
		}
	}
}
