package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type UnstructuredResource struct {
	cluster       *Cluster
	def           *resourceDefinition
	namespace     string
	allNamespaces bool
}

func (r *UnstructuredResource) ensureWritable() error {
	if r.def != nil && r.def.Resource == ResourceAdmissionRequests {
		return ErrUnsupported
	}
	return nil
}

func (r *UnstructuredResource) Namespace(namespace string) (*UnstructuredResource, error) {
	if !r.def.Namespaced {
		return nil, ErrInvalidObject
	}
	if err := validateNamespace(namespace); err != nil {
		return nil, err
	}
	copied := *r
	copied.namespace = namespace
	copied.allNamespaces = false
	return &copied, nil
}

func (r *UnstructuredResource) AllNamespaces() (*UnstructuredResource, error) {
	if !r.def.Namespaced {
		return nil, ErrInvalidObject
	}
	copied := *r
	copied.namespace = ""
	copied.allNamespaces = true
	return &copied, nil
}

func (r *UnstructuredResource) Create(
	ctx context.Context,
	obj *Unstructured,
	opts CreateOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, ErrInvalidObject
	}
	created := cloneUnstructured(*obj)
	if len(opts.Labels) > 0 {
		created.Metadata.Labels = cloneLabels(opts.Labels)
	}
	if len(opts.Annotations) > 0 {
		created.Metadata.Annotations = cloneAnnotations(opts.Annotations)
	}
	if len(opts.Finalizers) > 0 {
		created.Metadata.Finalizers = append([]string(nil), opts.Finalizers...)
	}
	if created.APIVersion == "" {
		created.APIVersion = r.def.APIVersion
	}
	if created.Kind == "" {
		created.Kind = r.def.Kind
	}
	if created.APIVersion != r.def.APIVersion || created.Kind != r.def.Kind {
		return nil, fmt.Errorf("%w: object does not match resource definition", ErrInvalidObject)
	}
	if err := validateObjectName(created.Metadata.Name); err != nil {
		return nil, err
	}
	name := created.Metadata.Name
	namespace, err := r.writeNamespace(created.Metadata.Namespace)
	if err != nil {
		return nil, err
	}
	created.Metadata.Namespace = namespace
	if err := r.def.defaultObject(&created); err != nil {
		return nil, err
	}
	if err := r.def.pruneObject(&created); err != nil {
		return nil, err
	}
	if created.APIVersion != r.def.APIVersion || created.Kind != r.def.Kind || created.Metadata.Name != name || created.Metadata.Namespace != namespace {
		return nil, fmt.Errorf("%w: default changed object identity", ErrInvalidObject)
	}
	now := time.Now().UTC()
	uid, err := randomToken("uid")
	if err != nil {
		return nil, err
	}
	created.Metadata.UID = uid
	created.Metadata.ResourceVersion = ""
	created.Metadata.Generation = 1
	created.Metadata.CreationTimestamp = now
	created.Metadata.DeletionTimestamp = nil
	created.Metadata.DeletionGracePeriodSeconds = nil
	ensureMetadataMaps(&created.Metadata)
	if err := validateOwnerReferencesForDefinition(r.def, created.Metadata); err != nil {
		return nil, err
	}
	if err := validateRawObjectJSON(&created); err != nil {
		return nil, err
	}
	if err := r.def.validateObject(nil, &created, SubresourceSpec); err != nil {
		return nil, err
	}
	if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
		Operation:        AdmissionCreate,
		Subresource:      SubresourceSpec,
		Ref:              objectRef{Resource: r.def.Resource, Namespace: created.Metadata.Namespace, Name: created.Metadata.Name},
		NewObject:        &created,
		EventAnnotations: opts.EventAnnotations,
	}); handled {
		return out, err
	}
	return r.commit(ctx, commitRequest{
		Op:               commitCreate,
		Ref:              objectRef{Resource: r.def.Resource, Namespace: created.Metadata.Namespace, Name: created.Metadata.Name},
		Object:           &created,
		EventType:        WatchAdded,
		EventAnnotations: opts.EventAnnotations,
		Changed:          changedPaths(nil, &created, SubresourceSpec),
	})
}

func (r *UnstructuredResource) Get(ctx context.Context, name string) (*Unstructured, error) {
	return r.GetWithOptions(ctx, name, GetOptions{})
}

func (r *UnstructuredResource) GetWithOptions(ctx context.Context, name string, opts GetOptions) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	out, err := r.cluster.store.get(ctx, ref)
	if err != nil {
		return nil, err
	}
	requestedRV, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	currentRV := parseStoredRV(out.Metadata.ResourceVersion)
	if err := validateResourceVersionMatch(currentRV, requestedRV, opts.ResourceVersionMatch); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *UnstructuredResource) List(ctx context.Context, opts ListOptions) (*UnstructuredList, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := validateSelector(r.def, opts.Selector); err != nil {
		return nil, err
	}
	scope, err := r.readScope()
	if err != nil {
		return nil, err
	}
	objects, rv, err := r.cluster.store.list(ctx, scope)
	if err != nil {
		return nil, err
	}
	requestedRV, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	if err := validateResourceVersionMatch(rv, requestedRV, opts.ResourceVersionMatch); err != nil {
		return nil, err
	}
	sortUnstructured(objects)

	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	out := make([]Unstructured, 0, min(limit, len(objects)))
	started := opts.Continue == ""
	startAfter := ""
	if opts.Continue != "" {
		token, err := decodeContinueToken(opts.Continue)
		if err != nil {
			return nil, err
		}
		if token.Resource != scope.Resource || token.Namespace != scope.Namespace || token.AllNamespaces != scope.AllNamespaces || token.ResourceVersion != formatRV(rv) || token.SelectorHash != selectorHash(opts.Selector) {
			return nil, ErrResourceVersionTooOld
		}
		startAfter = token.LastCursor
	}
	for _, obj := range objects {
		key := objectCursor(obj)
		if !started {
			started = key > startAfter
			if !started {
				continue
			}
		}
		if !opts.IncludeDeleting && obj.Metadata.DeletionTimestamp != nil {
			continue
		}
		if !matchesSelector(obj, opts.Selector) {
			continue
		}
		out = append(out, cloneUnstructured(obj))
		if len(out) > limit {
			token, err := encodeContinueToken(continueToken{
				Resource:        scope.Resource,
				Namespace:       scope.Namespace,
				AllNamespaces:   scope.AllNamespaces,
				ResourceVersion: formatRV(rv),
				SelectorHash:    selectorHash(opts.Selector),
				LastCursor:      objectCursor(out[limit-1]),
			})
			if err != nil {
				return nil, err
			}
			return &UnstructuredList{
				Items:           out[:limit],
				ResourceVersion: formatRV(rv),
				Continue:        token,
			}, nil
		}
	}
	return &UnstructuredList{Items: out, ResourceVersion: formatRV(rv)}, nil
}

func (r *UnstructuredResource) Update(
	ctx context.Context,
	obj *Unstructured,
	opts UpdateOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, ErrInvalidObject
	}
	input := cloneUnstructured(*obj)
	if err := validateObjectName(input.Metadata.Name); err != nil {
		return nil, err
	}
	namespace, err := r.writeNamespace(input.Metadata.Namespace)
	if err != nil {
		return nil, err
	}
	input.Metadata.Namespace = namespace
	expectedRV, err := updateRV(input.Metadata.ResourceVersion, opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	ref := objectRef{Resource: r.def.Resource, Namespace: input.Metadata.Namespace, Name: input.Metadata.Name}
	oldObj, err := r.cluster.store.get(ctx, ref)
	if err != nil {
		return nil, err
	}
	if expectedRV != parseStoredRV(oldObj.Metadata.ResourceVersion) {
		return nil, ErrConflict
	}
	if !jsonEqual(input.Status, oldObj.Status) {
		return nil, fmt.Errorf("%w: status must be updated through status subresource", ErrInvalidObject)
	}
	updated, err := r.prepareSpecUpdate(*oldObj, input)
	if err != nil {
		return nil, err
	}
	if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
		Operation:        AdmissionUpdate,
		Subresource:      SubresourceSpec,
		Ref:              ref,
		OldObject:        oldObj,
		NewObject:        &updated,
		ExpectedRV:       expectedRV,
		EventAnnotations: opts.EventAnnotations,
	}); handled {
		return out, err
	}
	return r.commit(ctx, commitRequest{
		Op:               commitUpdate,
		Ref:              ref,
		ExpectedRV:       expectedRV,
		Object:           &updated,
		EventType:        WatchModified,
		EventAnnotations: opts.EventAnnotations,
		Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
	})
}

func (r *UnstructuredResource) Patch(
	ctx context.Context,
	name string,
	patch []byte,
	opts PatchOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	if err := validateSpecPatch(patch, r.def.metadataWritable); err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		if expected != 0 && parseStoredRV(oldObj.Metadata.ResourceVersion) != expected {
			return nil, ErrConflict
		}
		patched, err := applyObjectPatch(*oldObj, patch)
		if err != nil {
			return nil, err
		}
		updated, err := r.prepareSpecUpdate(*oldObj, patched)
		if err != nil {
			return nil, err
		}
		if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
			Operation:        AdmissionUpdate,
			Subresource:      SubresourceSpec,
			Ref:              ref,
			OldObject:        oldObj,
			NewObject:        &updated,
			ExpectedRV:       parseStoredRV(oldObj.Metadata.ResourceVersion),
			EventAnnotations: opts.EventAnnotations,
		}); handled {
			return out, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       parseStoredRV(oldObj.Metadata.ResourceVersion),
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) PatchMetadata(
	ctx context.Context,
	name string,
	patch []byte,
	opts PatchOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	if err := r.def.validateMetadataPatch(patch); err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		updated, err := r.prepareMetadataUpdate(*oldObj, patch)
		if err != nil {
			return nil, err
		}
		if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
			Operation:        AdmissionUpdate,
			Subresource:      SubresourceMetadata,
			Ref:              ref,
			OldObject:        oldObj,
			NewObject:        &updated,
			ExpectedRV:       oldRV,
			EventAnnotations: opts.EventAnnotations,
		}); handled {
			return out, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceMetadata),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) UpdateStatus(
	ctx context.Context,
	name string,
	status []byte,
	opts UpdateOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	return r.mutateStatus(ctx, ref, expected, opts.EventAnnotations, func(obj Unstructured) (Unstructured, error) {
		obj.Status = cloneRaw(status)
		return obj, nil
	})
}

func (r *UnstructuredResource) PatchStatus(
	ctx context.Context,
	name string,
	patch []byte,
	opts PatchOptions,
) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	return r.mutateStatus(ctx, ref, expected, opts.EventAnnotations, func(obj Unstructured) (Unstructured, error) {
		raw, err := applyRawMergePatch(obj.Status, patch)
		if err != nil {
			return Unstructured{}, err
		}
		obj.Status = raw
		return obj, nil
	})
}

func (r *UnstructuredResource) Delete(ctx context.Context, name string, opts DeleteOptions) (*Unstructured, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := r.ensureWritable(); err != nil {
		return nil, err
	}
	switch opts.PropagationPolicy {
	case "", DeletePropagationBackground, DeletePropagationOrphan:
	default:
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		now := time.Now().UTC()
		updated := cloneUnstructured(*oldObj)
		if opts.PropagationPolicy == DeletePropagationOrphan && oldObj.Metadata.DeletionTimestamp == nil {
			if err := r.cluster.orphanDependents(ctx, *oldObj); err != nil {
				return nil, err
			}
		}
		if oldObj.Metadata.DeletionTimestamp == nil {
			updated.Metadata.DeletionTimestamp = &now
			updated.Metadata.DeletionGracePeriodSeconds = cloneInt64Ptr(opts.GracePeriodSeconds)
		}
		op := commitUpdate
		eventType := WatchModified
		switch {
		case oldObj.Metadata.DeletionTimestamp != nil && len(oldObj.Metadata.Finalizers) == 0 && !gracePeriodActive(oldObj.Metadata, now):
			op = commitDelete
			eventType = WatchDeleted
		case oldObj.Metadata.DeletionTimestamp == nil && len(oldObj.Metadata.Finalizers) == 0 && !gracePeriodActive(updated.Metadata, now):
			op = commitDelete
			eventType = WatchDeleted
		case oldObj.Metadata.DeletionTimestamp != nil:
			return cloneUnstructuredPtr(oldObj), nil
		}
		if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
			Operation:        AdmissionDelete,
			Subresource:      SubresourceSpec,
			Ref:              ref,
			OldObject:        oldObj,
			NewObject:        &updated,
			ExpectedRV:       oldRV,
			EventAnnotations: opts.EventAnnotations,
		}); handled {
			return out, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               op,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        eventType,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceSpec),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) Watch(
	ctx context.Context,
	opts WatchOptions,
) (<-chan UnstructuredWatchEvent, error) {
	if err := r.cluster.ensureActive(ctx); err != nil {
		return nil, err
	}
	if err := validateWatchScope(opts.Scope); err != nil {
		return nil, err
	}
	if err := validateSelector(r.def, opts.Selector); err != nil {
		return nil, err
	}
	if opts.Name != "" {
		if err := validateObjectName(opts.Name); err != nil {
			return nil, err
		}
	}
	startRV, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	scope, err := r.readScope()
	if err != nil {
		return nil, err
	}
	notify, cancel, err := r.cluster.store.subscribe(ctx, scope)
	if err != nil {
		return nil, err
	}
	_, currentRV, err := r.cluster.store.list(ctx, scope)
	if err != nil {
		cancel()
		return nil, err
	}
	if startRV > currentRV {
		cancel()
		return nil, ErrConflict
	}
	if opts.ResourceVersion == "" && !opts.SendInitialEvents {
		startRV = currentRV
	}

	out := make(chan UnstructuredWatchEvent, r.cluster.options.WatchBufferSize)
	go r.watchLoop(ctx, opts, scope, startRV, notify, cancel, out)
	return out, nil
}

func (r *UnstructuredResource) WatchMetadata(
	ctx context.Context,
	opts WatchOptions,
) (<-chan UnstructuredWatchEvent, error) {
	opts.Scope = WatchScopeMetadata
	return r.Watch(ctx, opts)
}

func (r *UnstructuredResource) WatchStatus(
	ctx context.Context,
	opts WatchOptions,
) (<-chan UnstructuredWatchEvent, error) {
	opts.Scope = WatchScopeStatus
	return r.Watch(ctx, opts)
}

func (r *UnstructuredResource) GetScale(ctx context.Context, name string) (*Scale, error) {
	if r.def.Scale == nil {
		return nil, ErrUnsupported
	}
	obj, err := r.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	return r.scaleFromObject(*obj)
}

func (r *UnstructuredResource) UpdateScale(ctx context.Context, name string, scale Scale, opts UpdateOptions) (*Scale, error) {
	if r.def.Scale == nil {
		return nil, ErrUnsupported
	}
	raw, err := json.Marshal(map[string]any{"spec": map[string]any{"replicas": scale.Spec.Replicas}})
	if err != nil {
		return nil, err
	}
	obj, err := r.PatchScale(ctx, name, raw, PatchOptions{ResourceVersion: opts.ResourceVersion, EventAnnotations: opts.EventAnnotations})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *UnstructuredResource) PatchScale(ctx context.Context, name string, patch []byte, opts PatchOptions) (*Scale, error) {
	if r.def.Scale == nil {
		return nil, ErrUnsupported
	}
	if len(bytes.TrimSpace(patch)) == 0 {
		return nil, ErrInvalidObject
	}
	var body struct {
		Spec struct {
			Replicas *int32 `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(patch, &body); err != nil {
		return nil, err
	}
	if body.Spec.Replicas == nil {
		return nil, ErrInvalidObject
	}
	ref, err := r.ref(name)
	if err != nil {
		return nil, err
	}
	expected, err := parseOptionalRV(opts.ResourceVersion)
	if err != nil {
		return nil, err
	}
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		updated := cloneUnstructured(*oldObj)
		updated.Spec, err = setRawFieldPath(updated.Spec, strings.TrimPrefix(r.def.Scale.SpecReplicasPath, "spec."), *body.Spec.Replicas)
		if err != nil {
			return nil, err
		}
		updated, err = r.prepareSpecUpdate(*oldObj, updated)
		if err != nil {
			return nil, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: opts.EventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceScale),
		})
		if err == nil {
			return r.scaleFromObject(*out)
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) mutateStatus(
	ctx context.Context,
	ref objectRef,
	expected uint64,
	eventAnnotations Annotations,
	mutate func(Unstructured) (Unstructured, error),
) (*Unstructured, error) {
	attempts := 1
	if expected == 0 {
		attempts = maxMutationRetries
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		oldObj, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		oldRV := parseStoredRV(oldObj.Metadata.ResourceVersion)
		if expected != 0 && oldRV != expected {
			return nil, ErrConflict
		}
		updated, err := mutate(cloneUnstructured(*oldObj))
		if err != nil {
			return nil, err
		}
		updated.APIVersion = oldObj.APIVersion
		updated.Kind = oldObj.Kind
		updated.Metadata = cloneMetadata(oldObj.Metadata)
		if err := r.def.pruneObject(&updated); err != nil {
			return nil, err
		}
		if err := validateRawJSONField("status", updated.Status); err != nil {
			return nil, err
		}
		if err := r.def.validateObject(oldObj, &updated, SubresourceStatus); err != nil {
			return nil, err
		}
		if out, handled, err := r.maybeAdmit(ctx, admissionRequestInput{
			Operation:        AdmissionUpdate,
			Subresource:      SubresourceStatus,
			Ref:              ref,
			OldObject:        oldObj,
			NewObject:        &updated,
			ExpectedRV:       oldRV,
			EventAnnotations: eventAnnotations,
		}); handled {
			return out, err
		}
		out, err := r.commit(ctx, commitRequest{
			Op:               commitUpdate,
			Ref:              ref,
			ExpectedRV:       oldRV,
			Object:           &updated,
			EventType:        WatchModified,
			EventAnnotations: eventAnnotations,
			Changed:          changedPaths(oldObj, &updated, SubresourceStatus),
		})
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, ErrConflict) || expected != 0 {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (r *UnstructuredResource) prepareMetadataUpdate(oldObj Unstructured, patch []byte) (Unstructured, error) {
	raw, err := json.Marshal(oldObj.Metadata)
	if err != nil {
		return Unstructured{}, err
	}
	merged, err := applyMergePatch(raw, patch)
	if err != nil {
		return Unstructured{}, err
	}
	var metadata Metadata
	if err := json.Unmarshal(merged, &metadata); err != nil {
		return Unstructured{}, err
	}
	updated := cloneUnstructured(oldObj)
	updated.Metadata.Labels = cloneLabels(metadata.Labels)
	updated.Metadata.Annotations = cloneAnnotations(metadata.Annotations)
	updated.Metadata.Finalizers = append([]string(nil), metadata.Finalizers...)
	updated.Metadata.OwnerReferences = append([]OwnerReference(nil), metadata.OwnerReferences...)
	if oldObj.Metadata.DeletionTimestamp != nil {
		if hasNewFinalizers(oldObj.Metadata.Finalizers, updated.Metadata.Finalizers) {
			return Unstructured{}, fmt.Errorf("%w: finalizers can only be removed while deleting", ErrInvalidObject)
		}
		updated.Metadata.Finalizers = filterExistingFinalizers(oldObj.Metadata.Finalizers, updated.Metadata.Finalizers)
		if !reflect.DeepEqual(updated.Metadata.OwnerReferences, oldObj.Metadata.OwnerReferences) {
			return Unstructured{}, fmt.Errorf("%w: ownerReferences are immutable while deleting", ErrInvalidObject)
		}
	}
	updated.Metadata.DeletionTimestamp = cloneTimePtr(oldObj.Metadata.DeletionTimestamp)
	updated.Metadata.DeletionGracePeriodSeconds = cloneInt64Ptr(oldObj.Metadata.DeletionGracePeriodSeconds)
	ensureMetadataMaps(&updated.Metadata)
	if err := validateOwnerReferencesForDefinition(r.def, updated.Metadata); err != nil {
		return Unstructured{}, err
	}
	if err := r.def.validateObject(&oldObj, &updated, SubresourceMetadata); err != nil {
		return Unstructured{}, err
	}
	return updated, nil
}

func (r *UnstructuredResource) prepareSpecUpdate(oldObj, input Unstructured) (Unstructured, error) {
	if input.APIVersion != oldObj.APIVersion || input.Kind != oldObj.Kind || input.Metadata.Name != oldObj.Metadata.Name || input.Metadata.Namespace != oldObj.Metadata.Namespace {
		return Unstructured{}, fmt.Errorf("%w: apiVersion, kind, namespace, and name are immutable", ErrInvalidObject)
	}
	if input.Metadata.UID != "" && input.Metadata.UID != oldObj.Metadata.UID {
		return Unstructured{}, fmt.Errorf("%w: uid is immutable", ErrInvalidObject)
	}
	if input.Metadata.CreationTimestamp.IsZero() {
		input.Metadata.CreationTimestamp = oldObj.Metadata.CreationTimestamp
	}
	if !input.Metadata.CreationTimestamp.Equal(oldObj.Metadata.CreationTimestamp) {
		return Unstructured{}, fmt.Errorf("%w: creationTimestamp is immutable", ErrInvalidObject)
	}
	if input.Metadata.DeletionTimestamp != nil && oldObj.Metadata.DeletionTimestamp == nil {
		return Unstructured{}, fmt.Errorf("%w: deletionTimestamp is managed by Delete", ErrInvalidObject)
	}
	if input.Metadata.DeletionTimestamp == nil && oldObj.Metadata.DeletionTimestamp != nil {
		return Unstructured{}, fmt.Errorf("%w: deletionTimestamp is immutable", ErrInvalidObject)
	}
	if oldObj.Metadata.DeletionTimestamp != nil {
		return Unstructured{}, fmt.Errorf("%w: spec is immutable while deleting", ErrInvalidObject)
	}
	updated := cloneUnstructured(input)
	restoreManagedFields := func() {
		updated.APIVersion = oldObj.APIVersion
		updated.Kind = oldObj.Kind
		updated.Metadata.Namespace = oldObj.Metadata.Namespace
		updated.Metadata.Name = oldObj.Metadata.Name
		updated.Metadata.UID = oldObj.Metadata.UID
		updated.Metadata.ResourceVersion = oldObj.Metadata.ResourceVersion
		updated.Metadata.CreationTimestamp = oldObj.Metadata.CreationTimestamp
		updated.Metadata.DeletionTimestamp = cloneTimePtr(oldObj.Metadata.DeletionTimestamp)
		updated.Metadata.DeletionGracePeriodSeconds = cloneInt64Ptr(oldObj.Metadata.DeletionGracePeriodSeconds)
		updated.Status = cloneRaw(oldObj.Status)
		ensureMetadataMaps(&updated.Metadata)
	}
	restoreManagedFields()
	if err := validateOwnerReferencesForDefinition(r.def, updated.Metadata); err != nil {
		return Unstructured{}, err
	}
	if err := r.def.defaultObject(&updated); err != nil {
		return Unstructured{}, err
	}
	if err := r.def.pruneObject(&updated); err != nil {
		return Unstructured{}, err
	}
	restoreManagedFields()
	if err := validateOwnerReferencesForDefinition(r.def, updated.Metadata); err != nil {
		return Unstructured{}, err
	}
	if err := validateRawObjectJSON(&updated); err != nil {
		return Unstructured{}, err
	}
	if jsonEqual(updated.Spec, oldObj.Spec) {
		updated.Metadata.Generation = oldObj.Metadata.Generation
	} else {
		updated.Metadata.Generation = oldObj.Metadata.Generation + 1
	}
	if err := r.def.validateObject(&oldObj, &updated, SubresourceSpec); err != nil {
		return Unstructured{}, err
	}
	return updated, nil
}

func (r *UnstructuredResource) commit(ctx context.Context, req commitRequest) (*Unstructured, error) {
	obj, _, err := r.cluster.store.commit(ctx, req)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *UnstructuredResource) ref(name string) (objectRef, error) {
	if err := validateObjectName(name); err != nil {
		return objectRef{}, err
	}
	namespace, err := r.writeNamespace("")
	if err != nil {
		return objectRef{}, err
	}
	return objectRef{Resource: r.def.Resource, Namespace: namespace, Name: name}, nil
}

func (r *UnstructuredResource) scaleFromObject(obj Unstructured) (*Scale, error) {
	if r.def.Scale == nil {
		return nil, ErrUnsupported
	}
	specReplicas, _ := int32FieldValue(&obj, r.def.Scale.SpecReplicasPath)
	statusReplicas, _ := int32FieldValue(&obj, r.def.Scale.StatusReplicasPath)
	selector, _ := fieldStringValue(&obj, r.def.Scale.LabelSelectorPath)
	return &Scale{
		Metadata: cloneMetadata(obj.Metadata),
		Spec:     ScaleSpec{Replicas: specReplicas},
		Status: ScaleStatus{
			Replicas: statusReplicas,
			Selector: selector,
		},
	}, nil
}

func (r *UnstructuredResource) writeNamespace(namespace string) (string, error) {
	if r.allNamespaces {
		return "", ErrInvalidObject
	}
	if !r.def.Namespaced {
		if namespace != "" || r.namespace != "" {
			return "", ErrInvalidObject
		}
		return "", nil
	}
	if r.namespace == "" {
		return "", ErrInvalidObject
	}
	if namespace != "" && namespace != r.namespace {
		return "", ErrInvalidObject
	}
	return r.namespace, nil
}

func (r *UnstructuredResource) readScope() (resourceScope, error) {
	if !r.def.Namespaced {
		if r.namespace != "" || r.allNamespaces {
			return resourceScope{}, ErrInvalidObject
		}
		return resourceScope{Resource: r.def.Resource}, nil
	}
	if r.namespace != "" {
		return resourceScope{Resource: r.def.Resource, Namespace: r.namespace}, nil
	}
	return resourceScope{Resource: r.def.Resource, AllNamespaces: true}, nil
}

func sortUnstructured(objects []Unstructured) {
	sort.Slice(objects, func(i, j int) bool {
		return objectCursor(objects[i]) < objectCursor(objects[j])
	})
}
