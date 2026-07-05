package cluster

import (
	"context"
	"errors"
	"time"
)

type admissionRequestInput struct {
	Operation        AdmissionOperation
	Subresource      Subresource
	Ref              objectRef
	OldObject        *Unstructured
	NewObject        *Unstructured
	ExpectedRV       uint64
	EventAnnotations Annotations
}

type AdmissionRequestResource struct {
	raw *Resource[AdmissionRequestSpec, AdmissionRequestStatus]
}

func (r *AdmissionRequestResource) Get(ctx context.Context, name string) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error) {
	return r.raw.Get(ctx, name)
}

func (r *AdmissionRequestResource) List(ctx context.Context, opts ListOptions) (*ObjectList[AdmissionRequestSpec, AdmissionRequestStatus], error) {
	return r.raw.List(ctx, opts)
}

func (r *AdmissionRequestResource) Watch(ctx context.Context, opts WatchOptions) (<-chan WatchEvent[AdmissionRequestSpec, AdmissionRequestStatus], error) {
	return r.raw.Watch(ctx, opts)
}

func (c *Cluster) ApproveAdmission(ctx context.Context, name string, opts AdmissionDecisionOptions) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error) {
	if err := c.ensureActive(ctx); err != nil {
		return nil, err
	}
	_, req, err := c.store.approveAdmission(ctx, approveAdmissionRequest{
		Name:        name,
		Decision:    opts,
		RequireRule: opts.Rule,
	})
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[AdmissionRequestSpec, AdmissionRequestStatus](req)
}

func (c *Cluster) RejectAdmission(ctx context.Context, name string, opts AdmissionDecisionOptions) (*Object[AdmissionRequestSpec, AdmissionRequestStatus], error) {
	if err := c.ensureActive(ctx); err != nil {
		return nil, err
	}
	req, err := c.store.rejectAdmission(ctx, rejectAdmissionRequest{
		Name:     name,
		Decision: opts,
	})
	if err != nil {
		return nil, err
	}
	return unstructuredToTyped[AdmissionRequestSpec, AdmissionRequestStatus](req)
}

func (r *UnstructuredResource) maybeAdmit(ctx context.Context, input admissionRequestInput) (*Unstructured, bool, error) {
	rules := r.def.admissionRules(input.Operation, input.Subresource)
	if len(rules) == 0 {
		return nil, false, nil
	}
	requestName, err := randomToken("adm")
	if err != nil {
		return nil, true, err
	}
	timeout := r.cluster.options.AdmissionTimeout
	for _, rule := range rules {
		if rule.Timeout > 0 && rule.Timeout < timeout {
			timeout = rule.Timeout
		}
	}
	now := time.Now().UTC()
	precondition := AdmissionPrecondition{}
	if input.Operation == AdmissionCreate {
		precondition.MustNotExist = true
	} else {
		precondition.MustExist = true
		precondition.ResourceVersion = formatRV(input.ExpectedRV)
	}
	requestObj, err := encodeAdmissionRequest(Metadata{Name: requestName}, AdmissionRequestSpec{
		Rules:             admissionRuleNames(rules),
		Operation:         input.Operation,
		Resource:          input.Ref.Resource,
		APIVersion:        r.def.APIVersion,
		Kind:              r.def.Kind,
		Namespaced:        r.def.Namespaced,
		Namespace:         input.Ref.Namespace,
		Name:              input.Ref.Name,
		Subresource:       input.Subresource,
		Precondition:      precondition,
		SchemaFingerprint: r.def.SchemaFingerprint,
		OldObject:         cloneUnstructuredPtr(input.OldObject),
		Object:            cloneUnstructuredPtr(input.NewObject),
		EventAnnotations:  cloneAnnotations(input.EventAnnotations),
		CreatedByNode:     r.cluster.options.NodeName,
		ExpiresAt:         now.Add(timeout),
	}, AdmissionRequestStatus{
		Phase: AdmissionPendingPhase,
	})
	if err != nil {
		return nil, true, err
	}
	if _, err := r.cluster.store.beginAdmission(ctx, beginAdmissionRequest{Request: requestObj, Target: input.Ref}); err != nil {
		return nil, true, err
	}
	out, err := r.waitAdmission(ctx, requestName)
	return out, true, err
}

func (r *UnstructuredResource) waitAdmission(ctx context.Context, name string) (*Unstructured, error) {
	scope := resourceScope{Resource: ResourceAdmissionRequests}
	notify, cancel, err := r.cluster.store.subscribe(ctx, scope)
	if err != nil {
		return nil, err
	}
	defer cancel()
	ref := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	for {
		req, err := r.cluster.store.get(ctx, ref)
		if err != nil {
			return nil, err
		}
		spec, status, err := decodeAdmissionRequest(*req)
		if err != nil {
			return nil, err
		}
		switch status.Phase {
		case AdmissionCommittedPhase:
			return cloneUnstructuredPtr(status.TargetObject), nil
		case AdmissionRejectedPhase:
			return nil, ErrAdmissionRejected
		case AdmissionExpiredPhase:
			return nil, ErrAdmissionExpired
		case AdmissionCanceledPhase:
			return nil, ErrAdmissionCanceled
		case AdmissionFailedPhase:
			return nil, ErrAdmissionFailed
		}
		var timeoutCh <-chan time.Time
		if !spec.ExpiresAt.IsZero() {
			wait := time.Until(spec.ExpiresAt)
			if wait <= 0 {
				_, err = r.cluster.store.expireAdmission(
					context.Background(),
					name,
					AdmissionExpiredPhase,
					"admission timeout",
				)
				if err == nil {
					continue
				}
				if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
					continue
				}
				return nil, err
			}
			timeoutCh = time.After(wait)
		}
		select {
		case <-ctx.Done():
			if _, err := r.cluster.store.expireAdmission(
				context.Background(),
				name,
				AdmissionCanceledPhase,
				ctx.Err().Error(),
			); err == nil {
				return nil, ErrAdmissionCanceled
			}
			return nil, ctx.Err()
		case <-timeoutCh:
			_, err = r.cluster.store.expireAdmission(
				context.Background(),
				name,
				AdmissionExpiredPhase,
				"admission timeout",
			)
			if err == nil {
				continue
			}
			if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		case _, ok := <-notify:
			if !ok {
				return nil, ErrClosed
			}
		}
	}
}

func admissionRuleNames(rules []AdmissionRule) []string {
	out := make([]string, 0, len(rules))
	for _, rule := range rules {
		out = append(out, rule.Name)
	}
	return out
}
