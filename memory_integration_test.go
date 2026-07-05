package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterFromURLValidation(t *testing.T) {
	_, err := NewClusterFromURL("memory://")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("unknown://?node=n1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=../x")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_lease_ttl=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_renew_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_renew_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&node_lease_ttl=1s&node_renew_interval=1s")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_lease_ttl=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_renew_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_renew_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_lease_ttl=1s&master_renew_interval=1s")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&master_history_limit=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_retention_count=-1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_retention_count=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&event_cleanup_interval=1ns")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&watch_buffer_size=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("badger://?node=n1")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&admission_timeout=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&admission_retention_count=0")
	require.ErrorIs(t, err, ErrInvalidConfig)
	_, err = NewClusterFromURL("memory://?node=n1&admission_terminal_retention=bad")
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestClusterNodeLeaseRequiresUniqueLocalNode(t *testing.T) {
	rawURL := "memory://?node=dup&prefix=lease-test&node_lease_ttl=2s&node_renew_interval=500ms"
	first, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)

	_, err = NewClusterFromURL(rawURL)
	require.ErrorIs(t, err, ErrNodeAlreadyExists)

	other, err := NewClusterFromURL("memory://?node=other&prefix=lease-test")
	require.NoError(t, err)
	require.NoError(t, other.Close())

	require.NoError(t, first.Close())
	second, err := NewClusterFromURL(rawURL)
	require.NoError(t, err)
	require.NoError(t, second.Close())
}

func TestClusterResourceDiscoveryClosed(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=closed-discovery")
	require.NoError(t, err)

	_, err = c.Resources()
	require.NoError(t, err)
	require.NoError(t, c.Close())

	_, err = c.Resources()
	require.ErrorIs(t, err, ErrClosed)
	_, err = c.Resource(ResourceNodes)
	require.ErrorIs(t, err, ErrClosed)
}

func TestClusterMasterHistoryLimit(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=history-limit&master_history_limit=1&master_lease_ttl=500ms&master_renew_interval=50ms&node_lease_ttl=2s&node_renew_interval=500ms")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	first, err := c.Master(ctx)
	require.NoError(t, err)
	require.True(t, first.Valid)
	require.NoError(t, c.StepDown(ctx))

	deadline := time.After(2 * time.Second)
	for {
		master, err := c.Master(ctx)
		require.NoError(t, err)
		if master.Valid && master.Term > first.Term+1 {
			history, err := c.MasterHistory(ctx, 10)
			require.NoError(t, err)
			require.Len(t, history, 1)
			require.Equal(t, masterTransitionAcquired, history[0].Reason)
			require.Equal(t, "history-limit", history[0].To)
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for master reacquire")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestClusterEventCleanupDefaults(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{
		"node": {"retention"},
	})
	defer c.Close()
	require.Equal(t, defaultEventRetentionCount, c.options.EventRetentionCount)
	require.Equal(t, c.options.MasterRenewInterval, c.options.EventCleanupInterval)
}

func TestClusterRecordsCleanupError(t *testing.T) {
	options, err := normalizeOptions(Options{
		Prefix:               "cleanup-error",
		NodeName:             "cleanup-error",
		EventCleanupInterval: time.Hour,
	})
	require.NoError(t, err)
	errCleanup := errors.New("cleanup failed")
	store := &cleanupErrorStore{
		resourceStore: newMemoryStore(options),
		err:           errCleanup,
	}
	c, err := newCluster(options, store)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, time.Second)

	c.cleanupEventsIfMaster(ctx)
	c.mu.RLock()
	gotErr := c.cleanupErr
	gotAt := c.cleanupErrAt
	c.mu.RUnlock()
	require.ErrorIs(t, gotErr, errCleanup)
	require.False(t, gotAt.IsZero())

	store.setError(nil)
	c.cleanupEventsIfMaster(ctx)
	c.mu.RLock()
	gotErr = c.cleanupErr
	gotAt = c.cleanupErrAt
	c.mu.RUnlock()
	require.NoError(t, gotErr)
	require.True(t, gotAt.IsZero())
}

func TestClusterDefineResourceSchemaValidation(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=schema-validate")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	_, err = DefineResource(c, ResourceDef{
		Resource:   "empties",
		APIVersion: "example.test/v1",
		Kind:       "Empty",
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = DefineResource(c, ResourceDef{
		Resource:   "invalidroot",
		APIVersion: "example.test/v1",
		Kind:       "InvalidRoot",
		Schema:     json.RawMessage(`{"type":"string"}`),
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = DefineResource(c, ResourceDef{
		Resource:   "missingspec",
		APIVersion: "example.test/v1",
		Kind:       "MissingSpec",
		Schema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"apiVersion":{"type":"string"},
				"kind":{"type":"string"},
				"metadata":{"type":"object"}
			}
		}`),
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = DefineResource(c, ResourceDef{
		Resource:   "statusimmutable",
		APIVersion: "example.test/v1",
		Kind:       "StatusImmutable",
		Schema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"apiVersion":{"type":"string"},
				"kind":{"type":"string"},
				"metadata":{"type":"object","properties":{"name":{"type":"string"}}},
				"spec":{"type":"object","properties":{},"additionalProperties":false},
				"status":{"type":"object","properties":{"phase":{"type":"string","x-cluster-immutable":true}},"additionalProperties":false}
			}
		}`),
	})
	require.ErrorIs(t, err, ErrInvalidResource)
}

func TestClusterSchemaPrunesUnknownFields(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=schema-prune")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	raw, err := DefineResource(c, ResourceDef{
		Resource:   "rawwidgets",
		APIVersion: "example.test/v1",
		Kind:       "RawWidget",
		Schema: json.RawMessage(`{
			"type":"object",
			"properties":{
				"apiVersion":{"type":"string"},
				"kind":{"type":"string"},
				"metadata":{
					"type":"object",
					"properties":{"name":{"type":"string"}},
					"required":["name"],
					"additionalProperties":false
				},
				"spec":{
					"type":"object",
					"properties":{
						"size":{"type":"string","x-cluster-index":true},
						"config":{"type":"object","x-cluster-preserve-unknown-fields":true}
					},
					"additionalProperties":false
				},
				"status":{
					"type":"object",
					"properties":{"phase":{"type":"string"}},
					"additionalProperties":false
				}
			}
		}`),
	})
	require.NoError(t, err)

	created, err := raw.Create(ctx, &Unstructured{
		APIVersion: "example.test/v1",
		Kind:       "RawWidget",
		Metadata:   Metadata{Name: "alpha"},
		Spec: json.RawMessage(`{
			"size":"small",
			"unknown":"drop",
			"config":{"keep":true,"nested":{"value":1}}
		}`),
	}, CreateOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"small","config":{"keep":true,"nested":{"value":1}}}`, string(created.Spec))

	statused, err := raw.PatchStatus(ctx, "alpha", []byte(`{"phase":"Ready","ghost":"drop"}`), PatchOptions{})
	require.NoError(t, err)
	require.JSONEq(t, `{"phase":"Ready"}`, string(statused.Status))
}

func TestClusterAdmissionCreateFlow(t *testing.T) {
	assertBackendAdmissionCreateFlow(t, memoryURLFactory())
}

func TestClusterAdmissionRejectFlow(t *testing.T) {
	assertBackendAdmissionRejectFlow(t, memoryURLFactory())
}

func TestClusterAdmissionExpires(t *testing.T) {
	assertBackendAdmissionExpires(t, memoryURLFactory())
}

func TestClusterMetadataPatchRejectsManagedKeys(t *testing.T) {
	assertBackendMetadataPatchRejectsManagedKeys(t, memoryURLFactory())
}

func TestClusterAdmissionMetadataSubresource(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	ctx := testContext(t, 5*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "metadataadmissionwidgets",
		APIVersion: "example.test/v1",
		Kind:       "MetadataAdmissionWidget",
		Admission: []AdmissionRule{
			{
				Name:         "metadata-check",
				Operations:   []AdmissionOperation{AdmissionUpdate},
				Subresources: []Subresource{SubresourceMetadata},
			},
		},
	})
	require.NoError(t, err)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	type patchResult struct {
		obj *Object[widgetSpec, widgetStatus]
		err error
	}
	resultCh := make(chan patchResult, 1)
	go func() {
		obj, err := widgets.PatchMetadata(ctx, created.Metadata.Name, []byte(`{"labels":{"app":"demo"}}`), PatchOptions{})
		resultCh <- patchResult{obj: obj, err: err}
	}()

	watchCtx := testContext(t, 5*time.Second)
	events, err := c.AdmissionRequests().Watch(watchCtx, WatchOptions{SendInitialEvents: true})
	require.NoError(t, err)

	var request WatchEvent[AdmissionRequestSpec, AdmissionRequestStatus]
	for {
		request = nextWatchEvent(t, events)
		if request.Type == WatchAdded && request.Object != nil && request.Object.Spec.Name == created.Metadata.Name {
			break
		}
	}
	require.Equal(t, SubresourceMetadata, request.Object.Spec.Subresource)

	approved, err := c.ApproveAdmission(ctx, request.Object.Metadata.Name, AdmissionDecisionOptions{
		Rule:    "metadata-check",
		Decider: "tester",
		Message: "ok",
	})
	require.NoError(t, err)
	require.Equal(t, AdmissionCommittedPhase, approved.Status.Phase)

	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, "demo", result.obj.Metadata.Labels["app"])
}

func TestClusterAdmissionFailureReleasesLock(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	ctx := testContext(t, 5*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "failedadmissionwidgets",
		APIVersion: "example.test/v1",
		Kind:       "FailedAdmissionWidget",
		Admission: []AdmissionRule{
			{Name: "update-check", Operations: []AdmissionOperation{AdmissionUpdate}},
		},
	})
	require.NoError(t, err)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, err := widgets.Patch(ctx, created.Metadata.Name, []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
		resultCh <- err
	}()

	watchCtx := testContext(t, 5*time.Second)
	events, err := c.AdmissionRequests().Watch(watchCtx, WatchOptions{SendInitialEvents: true})
	require.NoError(t, err)

	var request WatchEvent[AdmissionRequestSpec, AdmissionRequestStatus]
	for {
		request = nextWatchEvent(t, events)
		if request.Type == WatchAdded && request.Object != nil && request.Object.Spec.Name == created.Metadata.Name {
			break
		}
	}

	ref := objectRef{Resource: "failedadmissionwidgets", Name: created.Metadata.Name}
	current, err := c.store.get(ctx, ref)
	require.NoError(t, err)
	patched, err := applyObjectPatch(*current, []byte(`{"spec":{"size":"medium"}}`))
	require.NoError(t, err)
	updated, err := widgets.raw.prepareSpecUpdate(*current, patched)
	require.NoError(t, err)
	_, _, err = c.store.commit(ctx, commitRequest{
		Op:                commitUpdate,
		Ref:               ref,
		ExpectedRV:        parseStoredRV(current.Metadata.ResourceVersion),
		SkipAdmissionLock: true,
		Object:            &updated,
		EventType:         WatchModified,
		Changed:           changedPaths(current, &updated, SubresourceSpec),
	})
	require.NoError(t, err)

	approved, err := c.ApproveAdmission(ctx, request.Object.Metadata.Name, AdmissionDecisionOptions{
		Rule:    "update-check",
		Decider: "tester",
		Message: "apply",
	})
	require.NoError(t, err)
	require.Equal(t, AdmissionFailedPhase, approved.Status.Phase)
	require.Equal(t, ErrConflict.Error(), approved.Status.LastError)

	err = <-resultCh
	require.ErrorIs(t, err, ErrAdmissionFailed)

	pending, err := c.store.admissionPending(ctx, ref)
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestClusterAdmissionCanceledAndRequestReadonly(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=admission-cancel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "cancelwidgets",
		APIVersion: "example.test/v1",
		Kind:       "CancelWidget",
		Admission: []AdmissionRule{
			{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}},
		},
	})
	require.NoError(t, err)

	writeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := widgets.Create(writeCtx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
		resultCh <- err
	}()

	ctx := testContext(t, 5*time.Second)
	var list *ObjectList[AdmissionRequestSpec, AdmissionRequestStatus]
	require.Eventually(t, func() bool {
		var err error
		list, err = c.AdmissionRequests().List(ctx, ListOptions{})
		return err == nil && len(list.Items) == 1
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, AdmissionPendingPhase, list.Items[0].Status.Phase)

	err = <-resultCh
	require.ErrorIs(t, err, ErrAdmissionCanceled)

	req, err := c.AdmissionRequests().Get(ctx, list.Items[0].Metadata.Name)
	require.NoError(t, err)
	require.Equal(t, AdmissionCanceledPhase, req.Status.Phase)

	raw, err := c.Unstructured(ResourceAdmissionRequests)
	require.NoError(t, err)
	_, err = raw.Delete(ctx, req.Metadata.Name, DeleteOptions{})
	require.ErrorIs(t, err, ErrUnsupported)
}

func TestClusterRejectAdmissionIdempotent(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=reject-idempotent")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	ctx := testContext(t, 5*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "rejectidemwidgets",
		APIVersion: "example.test/v1",
		Kind:       "RejectIdemWidget",
		Admission: []AdmissionRule{
			{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}},
		},
	})
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
		resultCh <- err
	}()

	var list *ObjectList[AdmissionRequestSpec, AdmissionRequestStatus]
	require.Eventually(t, func() bool {
		var err error
		list, err = c.AdmissionRequests().List(ctx, ListOptions{})
		return err == nil && len(list.Items) == 1
	}, 2*time.Second, 20*time.Millisecond)

	reqName := list.Items[0].Metadata.Name
	_, err = c.RejectAdmission(ctx, reqName, AdmissionDecisionOptions{
		Rule:    "create-check",
		Decider: "tester",
		Message: "denied",
	})
	require.NoError(t, err)

	repeated, err := c.RejectAdmission(ctx, reqName, AdmissionDecisionOptions{
		Rule:    "create-check",
		Decider: "tester",
		Message: "double-reject",
	})
	require.NoError(t, err)
	require.Equal(t, AdmissionRejectedPhase, repeated.Status.Phase)

	err = <-resultCh
	require.ErrorIs(t, err, ErrAdmissionRejected)
}

func TestUnstructuredWatchMetadata(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=unstructured-watchmeta")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	ctx := testContext(t, 5*time.Second)
	widgets := defineWidgets(t, c, "unstructuredmetawidgets")

	_, err = widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Labels: Labels{"app": "demo"},
	})
	require.NoError(t, err)

	list, err := widgets.List(ctx, ListOptions{})
	require.NoError(t, err)

	raw, err := c.Unstructured("unstructuredmetawidgets")
	require.NoError(t, err)

	watchCtx := testContext(t, 3*time.Second)
	metaEvents, err := raw.WatchMetadata(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
	})
	require.NoError(t, err)

	statusEvents, err := raw.WatchStatus(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
	})
	require.NoError(t, err)

	_, err = raw.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{})
	require.NoError(t, err)

	patched, err := raw.PatchMetadata(ctx, "alpha", []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), PatchOptions{})
	require.NoError(t, err)

	event := nextUnstructuredWatchEvent(t, metaEvents)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, patched.Metadata.ResourceVersion, event.ResourceVersion)
	require.True(t, slices.Contains(event.Changed, "metadata.labels"), event.Changed)

	requireNoUnstructuredWatchEvent(t, statusEvents, 300*time.Millisecond)
}

func TestResourceSliceHelpers(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=helper-tests")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	_, err = c.Resource("unknown")
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = c.Unstructured("unknown")
	require.ErrorIs(t, err, ErrInvalidResource)
}

func TestNewClusterNilStore(t *testing.T) {
	_, err := newCluster(Options{NodeName: "test"}, nil)
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestOpenEtcdNilClient(t *testing.T) {
	_, err := OpenEtcd(nil, Options{NodeName: "test"})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestOptionsNegativeValidation(t *testing.T) {
	_, err := normalizeOptions(Options{
		NodeName:         "test",
		AdmissionTimeout: -1,
	})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = normalizeOptions(Options{
		NodeName:                "test",
		AdmissionRetentionCount: -1,
	})
	require.ErrorIs(t, err, ErrInvalidConfig)

	_, err = normalizeOptions(Options{
		NodeName:                   "test",
		AdmissionTerminalRetention: -1,
	})
	require.ErrorIs(t, err, ErrInvalidConfig)
}

func TestClusterCRUDAndStatus(t *testing.T) {
	assertBackendCRUDAndStatus(t, memoryURLFactory())
}

func TestClusterSchemaAndTags(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	widgets := defineWidgets(t, c, "validatedwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "defaulted", widgetSpec{Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, "medium", created.Spec.Size)
	_, err = widgets.Patch(ctx, "defaulted", []byte(`{"spec":{"owner":"team-b"}}`), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
	_, err = widgets.Patch(ctx, "defaulted", []byte(`{"spec":{"size":"xlarge"}}`), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
	_, err = widgets.UpdateStatus(ctx, "defaulted", widgetStatus{Phase: "Broken"}, UpdateOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
	_, err = widgets.Create(ctx, "../escape", widgetSpec{Size: "small"}, CreateOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
}

func TestClusterListPaginationAndSelectors(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	widgets := defineWidgets(t, c, "listedwidgets")
	ctx := testContext(t, 5*time.Second)

	for i, name := range []string{"alpha", "beta", "gamma"} {
		tenant := "t1"
		app := "demo"
		if name == "beta" {
			tenant = "t2"
			app = "other"
		}
		_, err := widgets.Create(ctx, name, widgetSpec{Size: "small", Owner: fmt.Sprintf("team-%d", i)}, CreateOptions{
			Labels:      Labels{"app": app},
			Annotations: Annotations{"tenant": tenant},
		})
		require.NoError(t, err)
	}
	_, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)
	_, err = widgets.UpdateStatus(ctx, "gamma", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)

	page, err := widgets.List(ctx, ListOptions{Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.NotEmpty(t, page.Continue)
	next, err := widgets.List(ctx, ListOptions{Limit: 10, Continue: page.Continue})
	require.NoError(t, err)
	require.Len(t, next.Items, 2)

	selected, err := widgets.List(ctx, ListOptions{
		Selector: Where(
			Label("app").In("demo"),
			Annotation("tenant").Eq("t1"),
			Annotation("tenant").Exists(),
			Field("status.phase").Eq("Ready"),
			Field("status.phase").NotIn("Failed"),
		),
	})
	require.NoError(t, err)
	require.Len(t, selected.Items, 2)

	notBeta, err := widgets.List(ctx, ListOptions{
		Selector: Where(Label("app").NotEq("other"), Field("spec.size").Exists()),
	})
	require.NoError(t, err)
	require.Len(t, notBeta.Items, 2)

	named, err := widgets.List(ctx, ListOptions{
		Selector: Where(Field("metadata.name").Eq("alpha")),
	})
	require.NoError(t, err)
	require.Len(t, named.Items, 1)
	require.Equal(t, "alpha", named.Items[0].Metadata.Name)

	_, err = widgets.List(ctx, ListOptions{
		Selector: Where(Field("kind").Eq("Widget")),
	})
	require.ErrorIs(t, err, ErrInvalidObject)
}

func TestClusterNamespacedResources(t *testing.T) {
	assertBackendNamespacedResources(t, memoryURLFactory())
}

func TestClusterWatchSelectorsAndChangedPaths(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	widgets := defineWidgets(t, c, "watchedwidgets")
	ctx := testContext(t, 5*time.Second)

	_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Labels:      Labels{"app": "demo"},
		Annotations: Annotations{"tenant": "t1"},
	})
	require.NoError(t, err)
	_, err = widgets.Create(ctx, "beta", widgetSpec{Size: "small", Owner: "team-b"}, CreateOptions{
		Labels:      Labels{"app": "other"},
		Annotations: Annotations{"tenant": "t2"},
	})
	require.NoError(t, err)
	_, err = widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)

	initialCtx := testContext(t, 3*time.Second)
	initialEvents, err := widgets.Watch(initialCtx, WatchOptions{
		Selector:          Where(Annotation("tenant").Eq("t1")),
		SendInitialEvents: true,
		AllowBookmarks:    true,
	})
	require.NoError(t, err)
	event := nextWatchEvent(t, initialEvents)
	require.Equal(t, WatchAdded, event.Type)
	require.Equal(t, "alpha", event.Object.Metadata.Name)
	event = nextWatchEvent(t, initialEvents)
	require.Equal(t, WatchBookmark, event.Type)
	requireNoWatchEvent(t, initialEvents, 300*time.Millisecond)

	list, err := widgets.List(ctx, ListOptions{
		Selector: Where(Annotation("tenant").Eq("t1"), Field("status.phase").Eq("Ready")),
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	watchCtx := testContext(t, 3*time.Second)
	events, err := widgets.Watch(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
		Selector:        Where(Annotation("tenant").Eq("t1"), Field("spec.size").Eq("large")),
	})
	require.NoError(t, err)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{
		EventAnnotations: Annotations{"reason": "resize"},
	})
	require.NoError(t, err)
	event = nextWatchEvent(t, events)
	require.Equal(t, WatchAdded, event.Type)
	require.Equal(t, "resize", event.Annotations["reason"])
	require.True(t, slices.Contains(event.Changed, "spec.size"), event.Changed)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"status":{"phase":"Failed"}}`), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)

	statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Failed"}, UpdateOptions{})
	require.NoError(t, err)
	event = nextWatchEvent(t, events)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, statused.Metadata.ResourceVersion, event.ResourceVersion)
	require.True(t, slices.Contains(event.Changed, "status.phase"), event.Changed)

	updated, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"small"}}`), PatchOptions{})
	require.NoError(t, err)
	event = nextWatchEvent(t, events)
	require.Equal(t, WatchDeleted, event.Type)
	require.Equal(t, updated.Metadata.ResourceVersion, event.ResourceVersion)
	require.Equal(t, "alpha", event.Object.Metadata.Name)
}

func TestClusterWatchMetadataAndStatusScopes(t *testing.T) {
	assertBackendWatchMetadataAndStatusScopes(t, memoryURLFactory())
}

func TestClusterMasterAPIHistoryAndWatch(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	ctx := testContext(t, 5*time.Second)

	master, err := c.Master(ctx)
	require.NoError(t, err)
	require.True(t, master.Valid)
	require.Equal(t, c.options.NodeName, master.Node)
	require.Equal(t, uint64(1), master.Term)

	isMaster, err := c.IsMaster(ctx)
	require.NoError(t, err)
	require.True(t, isMaster)

	history, err := c.MasterHistory(ctx, 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, masterTransitionAcquired, history[0].Reason)
	require.Equal(t, c.options.NodeName, history[0].To)

	watchCtx := testContext(t, 4*time.Second)
	events, err := c.WatchMaster(watchCtx, WatchOptions{ResourceVersion: master.ResourceVersion})
	require.NoError(t, err)

	require.NoError(t, c.StepDown(ctx))
	event := nextMasterWatchEvent(t, events)
	require.Equal(t, WatchModified, event.Type)
	require.NotNil(t, event.Master)
	require.False(t, event.Master.Valid)
	require.Equal(t, master.Term+1, event.Master.Term)
	require.NotNil(t, event.Transition)
	require.Equal(t, masterTransitionReleased, event.Transition.Reason)
	require.Equal(t, c.options.NodeName, event.Transition.From)

	isMaster, err = c.IsMaster(ctx)
	require.NoError(t, err)
	require.False(t, isMaster)
	require.ErrorIs(t, c.StepDown(ctx), ErrNotMaster)

	history, err = c.MasterHistory(ctx, 1)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, masterTransitionReleased, history[0].Reason)

	replayCtx := testContext(t, 4*time.Second)
	replayEvents, err := c.WatchMaster(replayCtx, WatchOptions{ResourceVersion: master.ResourceVersion})
	require.NoError(t, err)
	event = nextMasterWatchEvent(t, replayEvents)
	require.Equal(t, WatchModified, event.Type)
	require.NotNil(t, event.Transition)
	require.Equal(t, masterTransitionReleased, event.Transition.Reason)
}

func TestClusterNodeAPIAndResourceSchema(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	_ = defineWidgets(t, c, "schemawidgets")
	ctx := testContext(t, 5*time.Second)

	node, err := c.CurrentNode(ctx)
	require.NoError(t, err)
	require.Equal(t, c.options.NodeName, node.Metadata.Name)
	require.False(t, node.Status.LeaseUntil.IsZero())

	nodes := c.Nodes()
	list, err := nodes.List(ctx, ListOptions{Selector: Where(Field("metadata.name").Eq(c.options.NodeName))})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	watchCtx := testContext(t, 4*time.Second)
	metadataEvents, err := nodes.WatchMetadata(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            c.options.NodeName,
	})
	require.NoError(t, err)

	patchedMeta, err := c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"node":"current"},"annotations":{"role":"worker"}}`), PatchOptions{})
	require.NoError(t, err)
	require.Equal(t, "worker", patchedMeta.Metadata.Annotations["role"])
	require.Equal(t, node.Metadata.Generation, patchedMeta.Metadata.Generation)

	event := nextWatchEvent(t, metadataEvents)
	require.Equal(t, WatchModified, event.Type)
	require.True(t, slices.Contains(event.Changed, "metadata.labels"), event.Changed)

	patchedSpec, err := c.PatchCurrentNodeSpec(ctx, []byte(`{"metadata":{"zone":"test"}}`), PatchOptions{})
	require.NoError(t, err)
	require.Equal(t, "test", patchedSpec.Spec.Metadata["zone"])
	_, err = c.PatchCurrentNodeSpec(ctx, []byte(` `), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)

	statused, err := c.UpdateCurrentNodeStatus(ctx, NodeStatus{Metadata: Annotations{"ready": "true"}}, UpdateOptions{})
	require.NoError(t, err)
	require.Equal(t, "true", statused.Status.Metadata["ready"])
	require.False(t, statused.Status.LeaseUntil.IsZero())

	patchedStatus, err := c.PatchCurrentNodeStatus(ctx, []byte(`{"metadata":{"ready":"true","zone":"test"}}`), PatchOptions{})
	require.NoError(t, err)
	require.Equal(t, "test", patchedStatus.Status.Metadata["zone"])
	require.False(t, patchedStatus.Status.LeaseUntil.IsZero())
	_, err = c.PatchCurrentNodeStatus(ctx, []byte(` `), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)

	resources, err := c.Resources()
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
		return info.Resource == ResourceNodes && info.Builtin
	}))
	require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
		return info.Resource == ResourceMasters && info.Builtin
	}))
	require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
		return info.Resource == ResourceAdmissionRequests && info.Builtin
	}))
	require.True(t, slices.ContainsFunc(resources, func(info ResourceInfo) bool {
		return info.Resource == "schemawidgets" && !info.Builtin
	}))

	info, err := c.Resource("schemawidgets")
	require.NoError(t, err)
	require.Equal(t, "Widget", info.Kind)
	require.NotEmpty(t, info.Schema)
	require.True(t, slices.ContainsFunc(info.Indexes, func(index IndexInfo) bool {
		return index.Path == "spec.size"
	}))

	duplicate, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "schemawidgets",
		APIVersion: "example.test/v1",
		Kind:       "Widget",
	})
	require.NoError(t, err)
	require.NotNil(t, duplicate)

	_, err = Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "schemawidgets",
		APIVersion: "example.test/v1",
		Kind:       "OtherWidget",
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   ResourceNodes,
		APIVersion: "example.test/v1",
		Kind:       "Node",
	})
	require.ErrorIs(t, err, ErrInvalidResource)
	_, err = Define(c, TypedResourceDef[MasterSpec, MasterStatus]{
		Resource:   ResourceMasters,
		APIVersion: "example.test/v1",
		Kind:       "Master",
	})
	require.ErrorIs(t, err, ErrInvalidResource)
}

func TestClusterWatchReplayAndRetention(t *testing.T) {
	assertBackendWatchReplayAndRetention(t, memoryURLFactory())
}

func TestClusterUnstructuredHandle(t *testing.T) {
	assertBackendUnstructuredHandle(t, memoryURLFactory())
}

func TestClusterDefaultsDoNotBreakIdentityOrStatusIsolation(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), nil)
	ctx := testContext(t, 5*time.Second)

	type defaultSpec struct {
		Size string `json:"size,omitempty" cluster:"required,enum=small|medium|large,default=medium"`
	}

	statusDefaults, err := Define(c, TypedResourceDef[defaultSpec, widgetStatus]{
		Resource:   "statusdefaults",
		APIVersion: "example.test/v1",
		Kind:       "StatusDefault",
	})
	require.NoError(t, err)
	created, err := statusDefaults.Create(ctx, "alpha", defaultSpec{}, CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, "medium", created.Spec.Size)
	require.Empty(t, created.Status.Phase)
	statused, err := statusDefaults.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", statused.Status.Phase)
	patched, err := statusDefaults.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
	require.NoError(t, err)
	require.Equal(t, "Ready", patched.Status.Phase)
}

func TestNormalizeOptionsRatioAdjustment(t *testing.T) {
	t.Run("node_renew_too_close_to_lease", func(t *testing.T) {
		opts, err := normalizeOptions(Options{
			NodeName:          "test",
			NodeLeaseTTL:      35 * time.Millisecond,
			NodeRenewInterval: 30 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Equal(t, 35*time.Millisecond, opts.NodeLeaseTTL)
		require.Less(t, opts.NodeRenewInterval, opts.NodeLeaseTTL)
	})

	t.Run("node_renew_adjusted_hits_min", func(t *testing.T) {
		opts, err := normalizeOptions(Options{
			NodeName:     "test",
			NodeLeaseTTL: 20 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Equal(t, minBackgroundInterval, opts.NodeRenewInterval)
	})

	t.Run("master_renew_too_close_to_lease", func(t *testing.T) {
		optsWithMaster, err := normalizeOptions(Options{
			NodeName:            "test",
			MasterLeaseTTL:      35 * time.Millisecond,
			MasterRenewInterval: 30 * time.Millisecond,
			NodeRenewInterval:   1 * time.Second,
			NodeLeaseTTL:        3 * time.Second,
		})
		require.NoError(t, err)
		require.Less(t, optsWithMaster.MasterRenewInterval, optsWithMaster.MasterLeaseTTL)
	})

	t.Run("explicit_valid_values", func(t *testing.T) {
		opts, err := normalizeOptions(Options{
			NodeName:                   "test",
			NodeLeaseTTL:               10 * time.Second,
			NodeRenewInterval:          3 * time.Second,
			MasterLeaseTTL:             5 * time.Second,
			MasterRenewInterval:        1 * time.Second,
			MasterHistoryLimit:         500,
			EventRetentionCount:        1000,
			EventCleanupInterval:       500 * time.Millisecond,
			AdmissionTimeout:           10 * time.Second,
			AdmissionRetentionCount:    500,
			AdmissionTerminalRetention: 5 * time.Minute,
			WatchBufferSize:            128,
		})
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, opts.NodeLeaseTTL)
		require.Equal(t, 3*time.Second, opts.NodeRenewInterval)
		require.Equal(t, 5*time.Second, opts.MasterLeaseTTL)
		require.Equal(t, 1*time.Second, opts.MasterRenewInterval)
		require.Equal(t, 500, opts.MasterHistoryLimit)
		require.Equal(t, 1000, opts.EventRetentionCount)
		require.Equal(t, 500*time.Millisecond, opts.EventCleanupInterval)
		require.Equal(t, 10*time.Second, opts.AdmissionTimeout)
		require.Equal(t, 500, opts.AdmissionRetentionCount)
		require.Equal(t, 5*time.Minute, opts.AdmissionTerminalRetention)
		require.Equal(t, 128, opts.WatchBufferSize)
	})

	t.Run("master_lease_defaults_to_node_lease", func(t *testing.T) {
		opts, err := normalizeOptions(Options{
			NodeName:     "test",
			NodeLeaseTTL: 45 * time.Second,
		})
		require.NoError(t, err)
		require.Equal(t, 45*time.Second, opts.MasterLeaseTTL)
	})

	t.Run("event_cleanup_defaults_below_min", func(t *testing.T) {
		opts, err := normalizeOptions(Options{
			NodeName:            "test",
			MasterRenewInterval: 5 * time.Millisecond,
		})
		require.ErrorIs(t, err, ErrInvalidConfig)
		_ = opts
	})
}

func TestEnsureCurrentNodeStatusRefresh(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=node-refresh&node_lease_ttl=30s&node_renew_interval=5s")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	node, err := c.CurrentNode(ctx)
	require.NoError(t, err)
	require.Equal(t, "node-refresh", node.Metadata.Name)

	calledAgain, err := c.CurrentNode(ctx)
	require.NoError(t, err)
	require.Equal(t, node.Metadata.UID, calledAgain.Metadata.UID)
}

func TestEnsureCurrentNodeRace(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=node-race&node_lease_ttl=30s&node_renew_interval=5s")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	var node1, node2 *Object[NodeSpec, NodeStatus]
	var err1, err2 error
	done := make(chan struct{}, 2)
	go func() {
		node1, err1 = c.CurrentNode(ctx)
		done <- struct{}{}
	}()
	go func() {
		node2, err2 = c.CurrentNode(ctx)
		done <- struct{}{}
	}()
	<-done
	<-done
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, node1.Metadata.UID, node2.Metadata.UID)
}

func TestWaitAdmissionTimeout(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=adm-timeout&admission_timeout=80ms&event_cleanup_interval=20ms")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	ctx := testContext(t, 5*time.Second)

	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "timeoutwidgets",
		APIVersion: "example.test/v1",
		Kind:       "TimeoutWidget",
		Admission: []AdmissionRule{
			{Name: "timeout-check", Operations: []AdmissionOperation{AdmissionCreate}},
		},
	})
	require.NoError(t, err)

	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err = widgets.Create(writeCtx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
	require.ErrorIs(t, err, ErrAdmissionExpired)

	list, err := c.AdmissionRequests().List(ctx, ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Equal(t, AdmissionExpiredPhase, list.Items[0].Status.Phase)
}

func TestRegisterDefinitionDuplicateGVK(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=dup-gvk")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	_, err = Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "resource-a",
		APIVersion: "example.test/v1",
		Kind:       "SharedKind",
	})
	require.NoError(t, err)

	_, err = Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "resource-b",
		APIVersion: "example.test/v1",
		Kind:       "SharedKind",
	})
	require.ErrorIs(t, err, ErrInvalidResource)
}

func TestRegisterDefinitionEquivalent(t *testing.T) {
	c, err := NewClusterFromURL("memory://?node=equiv-reg")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	r1, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "equiv-res",
		APIVersion: "example.test/v1",
		Kind:       "Equiv",
	})
	require.NoError(t, err)
	require.NotNil(t, r1)

	r2, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "equiv-res",
		APIVersion: "example.test/v1",
		Kind:       "Equiv",
	})
	require.NoError(t, err)
	require.NotNil(t, r2)
}

func TestMemoryStoreEventsAfterCompacted(t *testing.T) {
	store := newMemoryStore(Options{NodeName: "comp-test", EventRetentionCount: 100})
	store.rv = 10
	store.compacted = 5

	_, _, err := store.eventsAfter(context.Background(), 3, resourceScope{}, 10)
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func TestMemoryStoreCommitEnforcesEventRetention(t *testing.T) {
	store := newMemoryStore(Options{NodeName: "retention-test", EventRetentionCount: 2})
	ctx := context.Background()

	for _, name := range []string{"one", "two", "three"} {
		obj := &Unstructured{
			APIVersion: "example.test/v1",
			Kind:       "Widget",
			Metadata:   Metadata{Name: name},
		}
		_, _, err := store.commit(ctx, commitRequest{
			Op:        commitCreate,
			Ref:       objectRef{Resource: "widgets", Name: name},
			Object:    obj,
			EventType: WatchAdded,
			Changed:   []string{"spec"},
		})
		require.NoError(t, err)
	}

	require.Len(t, store.events, 2)
	require.Equal(t, uint64(1), store.compacted)
	require.Equal(t, "2", store.events[0].ResourceVersion)
	require.Equal(t, "3", store.events[1].ResourceVersion)
}
