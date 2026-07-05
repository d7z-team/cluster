package cluster

import (
	"testing"
)

func TestClusterDeletionCleanupWithFinalizers(t *testing.T) {
	assertBackendDeletionCleanupWithFinalizers(t, memoryURLFactory())
}

func TestClusterContinueTokenExpiresAfterMutation(t *testing.T) {
	assertBackendContinueTokenExpiresAfterMutation(t, memoryURLFactory())
}

func TestClusterOwnerReferenceGarbageCollection(t *testing.T) {
	assertBackendOwnerReferenceGarbageCollection(t, memoryURLFactory())
}

func TestClusterOwnerReferenceOrphanPolicy(t *testing.T) {
	assertBackendOwnerReferenceOrphanPolicy(t, memoryURLFactory())
}

func TestClusterGetWithOptionsResourceVersionExact(t *testing.T) {
	assertBackendGetWithOptionsResourceVersionExact(t, memoryURLFactory())
}

func TestClusterListResourceVersionExact(t *testing.T) {
	assertBackendListResourceVersionExact(t, memoryURLFactory())
}

func TestClusterWatchRejectsFutureResourceVersion(t *testing.T) {
	assertBackendWatchRejectsFutureResourceVersion(t, memoryURLFactory())
}

func TestClusterRejectCrossNamespaceOwnerReference(t *testing.T) {
	assertBackendRejectCrossNamespaceOwnerReference(t, memoryURLFactory())
}

type scalableSpec struct {
	Replicas int32 `json:"replicas,omitempty"`
}

type scalableStatus struct {
	Replicas int32  `json:"replicas,omitempty"`
	Selector string `json:"selector,omitempty"`
}

func TestClusterScaleSubresource(t *testing.T) {
	assertBackendScaleSubresource(t, memoryURLFactory())
}
