package cluster

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterDeletionCleanupWithFinalizers(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{
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

func TestClusterContinueTokenExpiresAfterMutation(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
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

func TestClusterOwnerReferenceGarbageCollection(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{
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

func TestClusterOwnerReferenceOrphanPolicy(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{
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

func TestClusterGetWithOptionsResourceVersionExact(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
	widgets := defineWidgets(t, c, "getwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	got, err := widgets.GetWithOptions(ctx, "alpha", GetOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.NoError(t, err)
	require.Equal(t, created.Metadata.ResourceVersion, got.Metadata.ResourceVersion)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = widgets.GetWithOptions(ctx, "alpha", GetOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func TestClusterListResourceVersionExact(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
	widgets := defineWidgets(t, c, "listrvwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	list, err := widgets.List(ctx, ListOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = widgets.Patch(ctx, "alpha", []byte(`{"spec":{"size":"medium"}}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.NoError(t, err)

	_, err = widgets.List(ctx, ListOptions{
		ResourceVersion:      created.Metadata.ResourceVersion,
		ResourceVersionMatch: ResourceVersionExact,
	})
	require.ErrorIs(t, err, ErrResourceVersionTooOld)
}

func TestClusterWatchRejectsFutureResourceVersion(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
	widgets := defineWidgets(t, c, "watchrvwidgets")
	ctx := testContext(t, 5*time.Second)

	created, err := widgets.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = widgets.Watch(ctx, WatchOptions{ResourceVersion: formatRV(parseStoredRV(created.Metadata.ResourceVersion) + 10)})
	require.ErrorIs(t, err, ErrConflict)
}

func TestClusterRejectCrossNamespaceOwnerReference(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
	widgets := defineNamespacedWidgets(t, c, "ownerrefwidgets")
	teamA, err := widgets.Namespace("team-a")
	require.NoError(t, err)
	ctx := testContext(t, 5*time.Second)

	created, err := teamA.Create(ctx, "alpha", widgetSpec{Size: "small", Owner: "team-a"}, CreateOptions{})
	require.NoError(t, err)

	_, err = teamA.PatchMetadata(ctx, created.Metadata.Name, []byte(`{"ownerReferences":[{"uid":"u1","resource":"ownerrefwidgets","namespace":"team-b","name":"other","controller":true}]}`), PatchOptions{ResourceVersion: created.Metadata.ResourceVersion})
	require.ErrorIs(t, err, ErrInvalidObject)
}

type scalableSpec struct {
	Replicas int32 `json:"replicas,omitempty"`
}

type scalableStatus struct {
	Replicas int32  `json:"replicas,omitempty"`
	Selector string `json:"selector,omitempty"`
}

func TestClusterScaleSubresource(t *testing.T) {
	c := newURLCluster(t, memoryURLFactory(), url.Values{})
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

	obj, err := res.Get(ctx, "alpha")
	require.NoError(t, err)
	require.EqualValues(t, 5, obj.Spec.Replicas)
	require.EqualValues(t, 1, obj.Status.Replicas)
}
