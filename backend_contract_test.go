package cluster

import (
	"context"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBackendContractMemory(t *testing.T) {
	runBackendContract(t, memoryURLFactory())
}

func TestBackendContractBadger(t *testing.T) {
	runBackendContract(t, badgerURLFactory(t))
}

func TestBackendContractEtcd(t *testing.T) {
	runBackendContract(t, newEtcdURLFactory(t))
}

func runBackendContract(t *testing.T, factory clusterURLFactory) {
	t.Helper()

	t.Run("url_contract", func(t *testing.T) {
		runClusterURLContract(t, factory)
	})
	t.Run("crud_and_status", func(t *testing.T) {
		assertBackendCRUDAndStatus(t, factory)
	})
	t.Run("namespaced_resources", func(t *testing.T) {
		assertBackendNamespacedResources(t, factory)
	})
	t.Run("watch_scopes", func(t *testing.T) {
		assertBackendWatchMetadataAndStatusScopes(t, factory)
	})
	t.Run("watch_replay_and_retention", func(t *testing.T) {
		assertBackendWatchReplayAndRetention(t, factory)
	})
	t.Run("unstructured_handle", func(t *testing.T) {
		assertBackendUnstructuredHandle(t, factory)
	})
	t.Run("metadata_patch_rejects_managed_keys", func(t *testing.T) {
		assertBackendMetadataPatchRejectsManagedKeys(t, factory)
	})
	t.Run("admission_create_flow", func(t *testing.T) {
		assertBackendAdmissionCreateFlow(t, factory)
	})
	t.Run("admission_reject_flow", func(t *testing.T) {
		assertBackendAdmissionRejectFlow(t, factory)
	})
	t.Run("admission_expires", func(t *testing.T) {
		assertBackendAdmissionExpires(t, factory)
	})
	t.Run("deletion_cleanup_with_finalizers", func(t *testing.T) {
		assertBackendDeletionCleanupWithFinalizers(t, factory)
	})
	t.Run("continue_token_expires_after_mutation", func(t *testing.T) {
		assertBackendContinueTokenExpiresAfterMutation(t, factory)
	})
	t.Run("owner_reference_garbage_collection", func(t *testing.T) {
		assertBackendOwnerReferenceGarbageCollection(t, factory)
	})
	t.Run("owner_reference_orphan_policy", func(t *testing.T) {
		assertBackendOwnerReferenceOrphanPolicy(t, factory)
	})
	t.Run("get_with_options_resource_version_exact", func(t *testing.T) {
		assertBackendGetWithOptionsResourceVersionExact(t, factory)
	})
	t.Run("list_resource_version_exact", func(t *testing.T) {
		assertBackendListResourceVersionExact(t, factory)
	})
	t.Run("watch_rejects_future_resource_version", func(t *testing.T) {
		assertBackendWatchRejectsFutureResourceVersion(t, factory)
	})
	t.Run("reject_cross_namespace_owner_reference", func(t *testing.T) {
		assertBackendRejectCrossNamespaceOwnerReference(t, factory)
	})
	t.Run("scale_subresource", func(t *testing.T) {
		assertBackendScaleSubresource(t, factory)
	})
}

func assertBackendCRUDAndStatus(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	widgets := defineWidgets(t, c, "widgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Labels:      Labels{"app": "demo"},
		Annotations: Annotations{"tenant": "t1"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, created.Metadata.ResourceVersion)
	require.EqualValues(t, 1, created.Metadata.Generation)
	require.NotEmpty(t, created.Metadata.UID)

	got, err := widgets.Get(ctx, "alpha")
	require.NoError(t, err)
	require.Equal(t, created.Metadata.UID, got.Metadata.UID)

	stale := *got
	patched, err := widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
	require.NoError(t, err)
	require.NotEqual(t, created.Metadata.ResourceVersion, patched.Metadata.ResourceVersion)
	require.EqualValues(t, 2, patched.Metadata.Generation)

	stale.Spec.Size = "medium"
	_, err = widgets.Update(ctx, &stale, UpdateOptions{})
	require.ErrorIs(t, err, ErrConflict)

	statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)
	require.NotEqual(t, patched.Metadata.ResourceVersion, statused.Metadata.ResourceVersion)
	require.EqualValues(t, 2, statused.Metadata.Generation)

	statused.Status.Phase = "Failed"
	_, err = widgets.Update(ctx, statused, UpdateOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
}

func assertBackendNamespacedResources(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	widgets := defineNamespacedWidgets(t, c, "namespacedwidgets")
	ctx := testContext(t, 6*time.Second)

	_, err := widgets.Create(ctx, "same", widgetSpec{Size: "small"}, CreateOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)

	teamA, err := widgets.Namespace("team-a")
	require.NoError(t, err)
	teamB, err := widgets.Namespace("team-b")
	require.NoError(t, err)
	all, err := widgets.AllNamespaces()
	require.NoError(t, err)

	createdA, err := teamA.Create(ctx, "same", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	createdB, err := teamB.Create(ctx, "same", widgetSpec{Size: "medium", Owner: "team-b"}, CreateOptions{})
	require.NoError(t, err)

	listAll, err := all.List(ctx, ListOptions{})
	require.NoError(t, err)
	require.Len(t, listAll.Items, 2)

	watchCtx := testContext(t, 5*time.Second)
	teamAEvents, err := teamA.Watch(watchCtx, WatchOptions{ResourceVersion: listAll.ResourceVersion})
	require.NoError(t, err)
	allEvents, err := all.Watch(watchCtx, WatchOptions{ResourceVersion: listAll.ResourceVersion})
	require.NoError(t, err)

	patchedB, err := teamB.Patch(ctx, "same", []byte(`{"spec":{"size":"large"}}`), PatchOptions{})
	require.NoError(t, err)
	event := nextWatchEvent(t, allEvents)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, patchedB.Metadata.ResourceVersion, event.ResourceVersion)
	require.Equal(t, "team-b", event.Object.Metadata.Namespace)
	requireNoWatchEvent(t, teamAEvents, 300*time.Millisecond)

	_, err = teamA.Delete(ctx, "same", DeleteOptions{})
	require.NoError(t, err)
	_, err = teamA.Get(ctx, "same")
	require.ErrorIs(t, err, ErrNotFound)
	gotB, err := teamB.Get(ctx, "same")
	require.NoError(t, err)
	require.Equal(t, createdB.Metadata.UID, gotB.Metadata.UID)
	require.Equal(t, "team-a", createdA.Metadata.Namespace)
}

func assertBackendWatchMetadataAndStatusScopes(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	widgets := defineWidgets(t, c, "scopedwidgets")
	ctx := testContext(t, 5*time.Second)

	_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Labels:      Labels{"app": "demo"},
		Annotations: Annotations{"tenant": "t1"},
	})
	require.NoError(t, err)

	list, err := widgets.List(ctx, ListOptions{Selector: Where(Field("metadata.name").Eq("alpha"))})
	require.NoError(t, err)

	watchCtx := testContext(t, 4*time.Second)
	metadataEvents, err := widgets.WatchMetadata(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
	})
	require.NoError(t, err)
	statusEvents, err := widgets.WatchStatus(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
	})
	require.NoError(t, err)

	patched, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), PatchOptions{})
	require.NoError(t, err)
	event := nextWatchEvent(t, metadataEvents)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, patched.Metadata.ResourceVersion, event.ResourceVersion)
	require.True(t, slices.Contains(event.Changed, "metadata.labels"), event.Changed)

	statused, err := widgets.UpdateStatus(ctx, "alpha", widgetStatus{Phase: "Ready"}, UpdateOptions{})
	require.NoError(t, err)
	event = nextWatchEvent(t, statusEvents)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, statused.Metadata.ResourceVersion, event.ResourceVersion)
	require.True(t, slices.Contains(event.Changed, "status.phase"), event.Changed)
}

func assertBackendWatchReplayAndRetention(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{
		"event_retention_count":  {"2"},
		"event_cleanup_interval": {"20ms"},
		"watch_buffer_size":      {"4"},
	})
	widgets := defineWidgets(t, c, "retentionwidgets")
	ctx := testContext(t, 5*time.Second)

	_, err := widgets.Create(ctx, "one", widgetSpec{Size: "small"}, CreateOptions{})
	require.NoError(t, err)
	list, err := widgets.List(ctx, ListOptions{})
	require.NoError(t, err)

	watchCtx := testContext(t, 3*time.Second)
	events, err := widgets.Watch(watchCtx, WatchOptions{ResourceVersion: list.ResourceVersion})
	require.NoError(t, err)

	created, err := widgets.Create(ctx, "two", widgetSpec{Size: "small"}, CreateOptions{})
	require.NoError(t, err)
	event := nextWatchEvent(t, events)
	require.Equal(t, WatchAdded, event.Type)
	require.Equal(t, created.Metadata.ResourceVersion, event.ResourceVersion)

	_, err = widgets.Patch(ctx, "two", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{})
	require.NoError(t, err)
	event = nextWatchEvent(t, events)
	require.Equal(t, WatchModified, event.Type)

	_, err = widgets.Delete(ctx, "two", DeleteOptions{})
	require.NoError(t, err)
	event = nextWatchEvent(t, events)
	require.Equal(t, WatchDeleted, event.Type)

	waitForWatchError(t, 3*time.Second, func(ctx context.Context) (<-chan WatchEvent[widgetSpec, widgetStatus], error) {
		return widgets.Watch(ctx, WatchOptions{ResourceVersion: "1"})
	}, ErrResourceVersionTooOld)
}

func assertBackendUnstructuredHandle(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	widgets := defineWidgets(t, c, "rawwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	raw, err := c.Unstructured("rawwidgets")
	require.NoError(t, err)
	got, err := raw.Get(ctx, "alpha")
	require.NoError(t, err)
	require.Equal(t, created.Metadata.UID, got.Metadata.UID)

	list, err := raw.List(ctx, ListOptions{Selector: Where(Field("metadata.name").Eq(created.Metadata.Name))})
	require.NoError(t, err)

	watchCtx := testContext(t, 3*time.Second)
	statusEvents, err := raw.WatchStatus(watchCtx, WatchOptions{
		ResourceVersion: list.ResourceVersion,
		Name:            "alpha",
	})
	require.NoError(t, err)
	statused, err := raw.PatchStatus(ctx, "alpha", []byte(`{"phase":"Ready"}`), PatchOptions{})
	require.NoError(t, err)
	event := nextUnstructuredWatchEvent(t, statusEvents)
	require.Equal(t, WatchModified, event.Type)
	require.Equal(t, statused.Metadata.ResourceVersion, event.ResourceVersion)
	require.True(t, slices.Contains(event.Changed, "status.phase"), event.Changed)
}

func assertBackendMetadataPatchRejectsManagedKeys(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	widgets := defineWidgets(t, c, "metapatchrejectwidgets")
	ctx := testContext(t, 5*time.Second)

	_, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.PatchMetadata(ctx, "alpha", []byte(`{"uid":"override"}`), PatchOptions{})
	require.ErrorIs(t, err, ErrInvalidObject)
}

func assertBackendAdmissionCreateFlow(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	ctx := testContext(t, 8*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "admissionwidgets",
		APIVersion: "example.test/v1",
		Kind:       "AdmissionWidget",
		Admission:  []AdmissionRule{{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}}},
	})
	require.NoError(t, err)

	type createResult struct {
		obj *Object[widgetSpec, widgetStatus]
		err error
	}
	resultCh := make(chan createResult, 1)
	go func() {
		obj, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
		resultCh <- createResult{obj: obj, err: err}
	}()

	watchCtx := testContext(t, 8*time.Second)
	events, err := c.AdmissionRequests().Watch(watchCtx, WatchOptions{SendInitialEvents: true})
	require.NoError(t, err)

	var request WatchEvent[AdmissionRequestSpec, AdmissionRequestStatus]
	for {
		request = nextWatchEvent(t, events)
		if request.Type == WatchAdded && request.Object != nil && request.Object.Spec.Name == "alpha" {
			break
		}
	}

	_, err = widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-b"}, CreateOptions{})
	require.ErrorIs(t, err, ErrAdmissionPending)

	approved, err := c.ApproveAdmission(ctx, request.Object.Metadata.Name, AdmissionDecisionOptions{
		Rule:    "create-check",
		Decider: "tester",
		Message: "ok",
	})
	require.NoError(t, err)
	require.Equal(t, AdmissionCommittedPhase, approved.Status.Phase)

	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, "alpha", result.obj.Metadata.Name)
}

func assertBackendAdmissionRejectFlow(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, nil)
	ctx := testContext(t, 8*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "rejectwidgets",
		APIVersion: "example.test/v1",
		Kind:       "RejectWidget",
		Admission:  []AdmissionRule{{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}}},
	})
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
		resultCh <- err
	}()

	var requestName string
	require.Eventually(t, func() bool {
		list, err := c.AdmissionRequests().List(ctx, ListOptions{})
		if err != nil || len(list.Items) != 1 {
			return false
		}
		requestName = list.Items[0].Metadata.Name
		return true
	}, 3*time.Second, 20*time.Millisecond)

	rejected, err := c.RejectAdmission(ctx, requestName, AdmissionDecisionOptions{
		Rule:    "create-check",
		Decider: "tester",
		Message: "denied",
	})
	require.NoError(t, err)
	require.Equal(t, AdmissionRejectedPhase, rejected.Status.Phase)
	require.ErrorIs(t, <-resultCh, ErrAdmissionRejected)
}

func assertBackendAdmissionExpires(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{
		"admission_timeout":      {"80ms"},
		"event_cleanup_interval": {"20ms"},
	})
	ctx := testContext(t, 8*time.Second)
	widgets, err := Define(c, TypedResourceDef[widgetSpec, widgetStatus]{
		Resource:   "expirewidgets",
		APIVersion: "example.test/v1",
		Kind:       "ExpireWidget",
		Admission:  []AdmissionRule{{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}}},
	})
	require.NoError(t, err)

	resultCh := make(chan error, 1)
	go func() {
		_, err := widgets.Create(ctx, "alpha", widgetSpec{Owner: "team-a"}, CreateOptions{})
		resultCh <- err
	}()

	var errResult error
	require.Eventually(t, func() bool {
		select {
		case errResult = <-resultCh:
			return true
		default:
			return false
		}
	}, 3*time.Second, 20*time.Millisecond)
	require.ErrorIs(t, errResult, ErrAdmissionExpired)
}

func assertBackendDeletionCleanupWithFinalizers(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{
		"event_cleanup_interval": {"20ms"},
		"master_lease_ttl":       {"300ms"},
		"master_renew_interval":  {"50ms"},
		"node_lease_ttl":         {"300ms"},
		"node_renew_interval":    {"50ms"},
	})
	widgets := defineWidgets(t, c, "cleanupwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{
		Finalizers: []string{"cleanup.example.test"},
	})
	require.NoError(t, err)

	deleting, err := widgets.Delete(ctx, "alpha", DeleteOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)
	require.NotNil(t, deleting.Metadata.DeletionTimestamp)

	updated, err := widgets.PatchMetadata(ctx, "alpha", []byte(`{"finalizers":[]}`), PatchOptions{ResourceVersion: deleting.Metadata.ResourceVersion})
	require.NoError(t, err)
	require.NotNil(t, updated.Metadata.DeletionTimestamp)

	require.Eventually(t, func() bool {
		_, err := widgets.Get(ctx, "alpha")
		return err != nil
	}, 2*time.Second, 20*time.Millisecond)
}

func assertBackendContinueTokenExpiresAfterMutation(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	widgets := defineWidgets(t, c, "continuewidgets")
	ctx := testContext(t, 5*time.Second)

	_, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	_, err = widgets.Create(ctx, "beta", widgetSpec{Size: "small", Owner: "team-b"}, CreateOptions{})
	require.NoError(t, err)

	page, err := widgets.List(ctx, ListOptions{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page.Continue)

	_, err = widgets.Create(ctx, "gamma", widgetSpec{Size: "small", Owner: "team-c"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.List(ctx, ListOptions{Limit: 1, Continue: page.Continue})
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func assertBackendOwnerReferenceGarbageCollection(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{
		"event_cleanup_interval": {"20ms"},
		"master_lease_ttl":       {"300ms"},
		"master_renew_interval":  {"50ms"},
		"node_lease_ttl":         {"300ms"},
		"node_renew_interval":    {"50ms"},
	})
	widgets := defineNamespacedWidgets(t, c, "gcwidgets")
	ns, err := widgets.Namespace("team-a")
	require.NoError(t, err)
	ctx := testContext(t, 5*time.Second)

	owner, err := ns.Create(ctx, "owner", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	dependent, err := ns.Create(ctx, "child", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = ns.PatchMetadata(ctx, dependent.Metadata.Name, []byte(`{"ownerReferences":[{"uid":"`+owner.Metadata.UID+`","resource":"gcwidgets","namespace":"team-a","name":"owner","controller":true}]}`), PatchOptions{ResourceVersion: dependent.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = ns.Delete(ctx, owner.Metadata.Name, DeleteOptions{ResourceVersion: owner.Metadata.ResourceVersion})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := ns.Get(ctx, "child")
		return err != nil
	}, 2*time.Second, 20*time.Millisecond)
}

func assertBackendOwnerReferenceOrphanPolicy(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{
		"event_cleanup_interval": {"20ms"},
		"master_lease_ttl":       {"300ms"},
		"master_renew_interval":  {"50ms"},
		"node_lease_ttl":         {"300ms"},
		"node_renew_interval":    {"50ms"},
	})
	widgets := defineNamespacedWidgets(t, c, "orphanwidgets")
	ns, err := widgets.Namespace("team-a")
	require.NoError(t, err)
	ctx := testContext(t, 5*time.Second)

	owner, err := ns.Create(ctx, "owner", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	child, err := ns.Create(ctx, "child", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)
	child, err = ns.PatchMetadata(ctx, child.Metadata.Name, []byte(`{"ownerReferences":[{"uid":"`+owner.Metadata.UID+`","resource":"orphanwidgets","namespace":"team-a","name":"owner","controller":true}]}`), PatchOptions{ResourceVersion: child.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = ns.Delete(ctx, owner.Metadata.Name, DeleteOptions{
		ResourceVersion:   owner.Metadata.ResourceVersion,
		PropagationPolicy: DeletePropagationOrphan,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, err := ns.Get(ctx, "child")
		return err == nil && len(got.Metadata.OwnerReferences) == 0
	}, 2*time.Second, 20*time.Millisecond)
}

func assertBackendGetWithOptionsResourceVersionExact(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	widgets := defineWidgets(t, c, "getwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.GetWithOptions(ctx, "alpha", GetOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.NoError(t, err)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = widgets.GetWithOptions(ctx, "alpha", GetOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func assertBackendListResourceVersionExact(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	widgets := defineWidgets(t, c, "listrvwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.List(ctx, ListOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.NoError(t, err)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = widgets.List(ctx, ListOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func assertBackendWatchRejectsFutureResourceVersion(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	widgets := defineWidgets(t, c, "watchrvwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.Watch(ctx, WatchOptions{ResourceVersion: formatRV(parseStoredRV(created.Metadata.ResourceVersion) + 10)})
	require.ErrorIs(t, err, ErrConflict)
}

func assertBackendRejectCrossNamespaceOwnerReference(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	widgets := defineNamespacedWidgets(t, c, "ownerrefwidgets")
	teamA, err := widgets.Namespace("team-a")
	require.NoError(t, err)
	ctx := testContext(t, 5*time.Second)

	created, err := teamA.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = teamA.PatchMetadata(ctx, created.Metadata.Name, []byte(`{"ownerReferences":[{"uid":"u1","resource":"ownerrefwidgets","namespace":"team-b","name":"other","controller":true}]}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.ErrorIs(t, err, ErrInvalidObject)
}

func assertBackendScaleSubresource(t *testing.T, factory clusterURLFactory) {
	t.Helper()
	c := newURLCluster(t, factory, url.Values{})
	res, err := Define(c, TypedResourceDef[scalableSpec, scalableStatus]{
		Resource:   "scales",
		APIVersion: "example.test/v1",
		Kind:       "ScaleObject",
		Scale: &ScaleDefinition{
			SpecReplicasPath:   "spec.replicas",
			StatusReplicasPath: "status.replicas",
			LabelSelectorPath:  "status.selector",
		},
	})
	require.NoError(t, err)
	ctx := testContext(t, 5*time.Second)

	created, err := res.Create(ctx, "alpha", scalableSpec{Replicas: 2}, CreateOptions{})
	require.NoError(t, err)
	_, err = res.UpdateStatus(ctx, "alpha", scalableStatus{Replicas: 1, Selector: "app=demo"}, UpdateOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)

	scale, err := res.GetScale(ctx, "alpha")
	require.NoError(t, err)
	require.EqualValues(t, 2, scale.Spec.Replicas)
	require.EqualValues(t, 1, scale.Status.Replicas)
	require.Equal(t, "app=demo", scale.Status.Selector)

	scale, err = res.PatchScale(ctx, "alpha", []byte(`{"spec":{"replicas":5}}`), PatchOptions{ResourceVersion: scale.Metadata.ResourceVersion})
	require.NoError(t, err)
	require.EqualValues(t, 5, scale.Spec.Replicas)
}
