package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *etcdStore) admissionPending(ctx context.Context, ref objectRef) (string, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return "", err
	}
	resp, err := s.client.Get(ctx, s.admissionLockKey(ref), clientv3.WithLimit(1))
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

func (s *etcdStore) beginAdmission(ctx context.Context, req beginAdmissionRequest) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	spec, _, err := decodeAdmissionRequest(*req.Request)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	token, err := randomToken("uid")
	if err != nil {
		return nil, err
	}
	req.Request.Metadata.UID = token
	req.Request.Metadata.ResourceVersion = ""
	req.Request.Metadata.Generation = 1
	req.Request.Metadata.CreationTimestamp = now
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name}
	lockKey := s.admissionLockKey(req.Target)
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(lockResp.Kvs) > 0 {
			return nil, ErrAdmissionPending
		}
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(requestResp.Kvs) > 0 {
			return nil, ErrAlreadyExists
		}
		targetResp, err := s.client.Get(ctx, s.objectKey(req.Target), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		targetExists := len(targetResp.Kvs) > 0
		var target Unstructured
		var targetRaw []byte
		if targetExists {
			targetRaw = append([]byte(nil), targetResp.Kvs[0].Value...)
			if err := json.Unmarshal(targetRaw, &target); err != nil {
				return nil, err
			}
		}
		switch {
		case spec.Precondition.MustNotExist:
			if targetExists {
				return nil, ErrAlreadyExists
			}
		case spec.Precondition.MustExist:
			if !targetExists {
				return nil, ErrNotFound
			}
			if spec.Precondition.ResourceVersion != "" && target.Metadata.ResourceVersion != spec.Precondition.ResourceVersion {
				return nil, ErrConflict
			}
		}

		nextRV := currentRV + 1
		out := cloneUnstructured(*req.Request)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitCreate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchAdded,
			Changed:           []string{"spec", "status"},
		}, nil, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}

		cmps := []clientv3.Cmp{
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Version(lockKey), "=", 0),
			clientv3.Compare(clientv3.Version(s.objectKey(requestRef)), "=", 0),
		}
		if spec.Precondition.MustNotExist {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(req.Target)), "=", 0))
		} else if targetExists {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(req.Target)), "=", string(targetRaw)))
		}
		ops := s.eventWriteOps(requestRef, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(requestRef), string(objectRaw)),
			clientv3.OpPut(lockKey, req.Request.Metadata.Name),
		)
		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) approveAdmission(ctx context.Context, req approveAdmissionRequest) (*Unstructured, *Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, nil, err
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		currentRV, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, nil, err
		}
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		if len(requestResp.Kvs) == 0 {
			return nil, nil, ErrNotFound
		}
		currentRaw := append([]byte(nil), requestResp.Kvs[0].Value...)
		var current Unstructured
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			return nil, nil, err
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return nil, nil, err
		}
		if status.Phase != AdmissionPendingPhase {
			return nil, cloneUnstructuredPtr(&current), nil
		}
		if req.RequireRule != "" && !slices.Contains(spec.Rules, req.RequireRule) {
			return nil, nil, ErrInvalidObject
		}
		if req.Decision.Rule == "" {
			req.Decision.Rule = req.RequireRule
		}
		if req.Decision.Rule == "" {
			return nil, nil, ErrInvalidObject
		}
		for _, decision := range status.Approved {
			if decision.Rule == req.Decision.Rule {
				return nil, cloneUnstructuredPtr(&current), nil
			}
		}

		lockKey := s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name})
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		if len(lockResp.Kvs) == 0 || string(lockResp.Kvs[0].Value) != req.Name {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}

		now := time.Now().UTC()
		status.Approved = append(status.Approved, AdmissionRuleDecision{
			Rule:    req.Decision.Rule,
			Message: req.Decision.Message,
			Decider: req.Decision.Decider,
			At:      now,
		})
		if len(status.Approved) < len(spec.Rules) {
			updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
			if err != nil {
				return nil, nil, err
			}
			requestOut, err := s.updateAdmissionRequestTxn(ctx, requestRef, currentRaw, metaRaw, metaVersion, updated, []string{"status.approved"})
			if err != nil {
				if errors.Is(err, ErrConflict) {
					select {
					case <-ctx.Done():
						return nil, nil, ctx.Err()
					case <-time.After(10 * time.Millisecond):
						continue
					}
				}
				return nil, nil, err
			}
			return nil, requestOut, nil
		}

		targetCommit, targetObj, err := admissionTargetCommit(spec)
		if err != nil {
			return nil, nil, err
		}
		targetCommit.SkipAdmissionLock = true
		targetResp, err := s.client.Get(ctx, s.objectKey(targetCommit.Ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, nil, err
		}
		targetExists := len(targetResp.Kvs) > 0
		var targetCurrent Unstructured
		var targetCurrentRaw []byte
		if targetExists {
			targetCurrentRaw = append([]byte(nil), targetResp.Kvs[0].Value...)
			if err := json.Unmarshal(targetCurrentRaw, &targetCurrent); err != nil {
				return nil, nil, err
			}
		}
		switch targetCommit.Op {
		case commitCreate:
			if targetExists {
				status.Phase = AdmissionFailedPhase
				status.Message = ErrAlreadyExists.Error()
				status.LastError = ErrAlreadyExists.Error()
				status.LastErrorAt = now
				status.DecidedBy = req.Decision.Decider
				status.DecidedAt = now
				updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
				if err != nil {
					return nil, nil, err
				}
				requestOut, err := s.finishFailedAdmissionTxn(ctx, currentRaw, metaRaw, metaVersion, lockKey, requestRef, updated)
				return nil, requestOut, err
			}
		case commitUpdate, commitDelete:
			if !targetExists {
				status.Phase = AdmissionFailedPhase
				status.Message = ErrNotFound.Error()
				status.LastError = ErrNotFound.Error()
				status.LastErrorAt = now
				status.DecidedBy = req.Decision.Decider
				status.DecidedAt = now
				updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
				if err != nil {
					return nil, nil, err
				}
				requestOut, err := s.finishFailedAdmissionTxn(ctx, currentRaw, metaRaw, metaVersion, lockKey, requestRef, updated)
				return nil, requestOut, err
			}
			if parseStoredRV(targetCurrent.Metadata.ResourceVersion) != targetCommit.ExpectedRV {
				status.Phase = AdmissionFailedPhase
				status.Message = ErrConflict.Error()
				status.LastError = ErrConflict.Error()
				status.LastErrorAt = now
				status.DecidedBy = req.Decision.Decider
				status.DecidedAt = now
				updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
				if err != nil {
					return nil, nil, err
				}
				requestOut, err := s.finishFailedAdmissionTxn(ctx, currentRaw, metaRaw, metaVersion, lockKey, requestRef, updated)
				return nil, requestOut, err
			}
		default:
			return nil, nil, ErrUnsupported
		}

		targetRV := currentRV + 1
		requestRV := currentRV + 2
		targetOut := cloneUnstructured(*targetCommit.Object)
		targetOut.Metadata.ResourceVersion = formatRV(targetRV)
		targetRaw, err := json.Marshal(targetOut)
		if err != nil {
			return nil, nil, err
		}
		var oldTarget *Unstructured
		if targetExists {
			oldTarget = cloneUnstructuredPtr(&targetCurrent)
		}
		targetEvent := newStoreEvent(targetCommit, oldTarget, &targetOut)
		targetEventRaw, err := json.Marshal(targetEvent)
		if err != nil {
			return nil, nil, err
		}

		status.Phase = AdmissionCommittedPhase
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
		status.LastError = ""
		status.LastErrorAt = time.Time{}
		status.TargetResourceVersion = targetOut.Metadata.ResourceVersion
		status.TargetObject = targetObj
		requestUpdated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return nil, nil, err
		}
		requestOut := cloneUnstructured(*requestUpdated)
		requestOut.Metadata.ResourceVersion = formatRV(requestRV)
		requestOutRaw, err := json.Marshal(requestOut)
		if err != nil {
			return nil, nil, err
		}
		requestEvent := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &requestOut,
			EventType:         WatchModified,
			Changed:           []string{"status"},
		}, &current, &requestOut)
		requestEventRaw, err := json.Marshal(requestEvent)
		if err != nil {
			return nil, nil, err
		}

		cmps := []clientv3.Cmp{
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(currentRaw)),
			clientv3.Compare(clientv3.Value(lockKey), "=", req.Name),
		}
		if targetCommit.Op == commitCreate {
			cmps = append(cmps, clientv3.Compare(clientv3.Version(s.objectKey(targetCommit.Ref)), "=", 0))
		} else {
			cmps = append(cmps, clientv3.Compare(clientv3.Value(s.objectKey(targetCommit.Ref)), "=", string(targetCurrentRaw)))
		}
		ops := s.eventStorageOps(targetCommit.Ref, targetRV, targetEventRaw)
		ops = append(ops, s.eventStorageOps(requestRef, requestRV, requestEventRaw)...)
		ops = append(ops, s.notifyOpsForRefs(requestRV, targetCommit.Ref, requestRef)...)
		ops = append(ops, clientv3.OpPut(s.metaKey("rv"), formatRV(requestRV)))
		if targetCommit.Op == commitDelete {
			ops = append(ops, clientv3.OpDelete(s.objectKey(targetCommit.Ref)))
		} else {
			ops = append(ops, clientv3.OpPut(s.objectKey(targetCommit.Ref), string(targetRaw)))
		}
		ops = append(ops,
			clientv3.OpPut(s.objectKey(requestRef), string(requestOutRaw)),
			clientv3.OpDelete(lockKey),
		)
		txnResp, err := s.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return nil, nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&targetOut), cloneUnstructuredPtr(&requestOut), nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, nil, ErrConflict
}

func (s *etcdStore) rejectAdmission(ctx context.Context, req rejectAdmissionRequest) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	return s.finishAdmission(ctx, req.Name, func(spec AdmissionRequestSpec, status *AdmissionRequestStatus) {
		now := time.Now().UTC()
		status.Phase = AdmissionRejectedPhase
		status.RejectedRule = req.Decision.Rule
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
	})
}

func (s *etcdStore) expireAdmission(ctx context.Context, name string, phase AdmissionPhase, message string) (*Unstructured, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	return s.finishAdmission(ctx, name, func(spec AdmissionRequestSpec, status *AdmissionRequestStatus) {
		status.Phase = phase
		status.Message = message
		status.DecidedAt = time.Now().UTC()
	})
}

func (s *etcdStore) updateAdmissionRequestTxn(ctx context.Context, ref objectRef, currentRaw []byte, metaRaw string, metaVersion int64, updated *Unstructured, changed []string) (*Unstructured, error) {
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		out := cloneUnstructured(*updated)
		current, err := s.client.Get(ctx, s.objectKey(ref), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(current.Kvs) == 0 {
			return nil, ErrNotFound
		}
		if string(current.Kvs[0].Value) != string(currentRaw) {
			return nil, ErrConflict
		}
		rv, raw, version, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		if raw != metaRaw || version != metaVersion {
			metaRaw, metaVersion = raw, version
		}
		nextRV := rv + 1
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		var oldObj Unstructured
		if err := json.Unmarshal(current.Kvs[0].Value, &oldObj); err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               ref,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchModified,
			Changed:           changed,
		}, &oldObj, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		ops := s.eventWriteOps(ref, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(ref), string(objectRaw)),
		)
		txnResp, err := s.client.Txn(ctx).If(
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(ref)), "=", string(currentRaw)),
		).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) finishFailedAdmissionTxn(
	ctx context.Context,
	currentRaw []byte,
	metaRaw string,
	metaVersion int64,
	lockKey string,
	requestRef objectRef,
	updated *Unstructured,
) (*Unstructured, error) {
	currentRV, raw, version, err := s.readMetaRV(ctx)
	if err != nil {
		return nil, err
	}
	if raw != metaRaw || version != metaVersion {
		metaRaw, metaVersion = raw, version
	}
	nextRV := currentRV + 1
	out := cloneUnstructured(*updated)
	out.Metadata.ResourceVersion = formatRV(nextRV)
	objectRaw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	var oldObj Unstructured
	if err := json.Unmarshal(currentRaw, &oldObj); err != nil {
		return nil, err
	}
	event := newStoreEvent(commitRequest{
		Op:                commitUpdate,
		Ref:               requestRef,
		SkipAdmissionLock: true,
		Object:            &out,
		EventType:         WatchModified,
		Changed:           []string{"status"},
	}, &oldObj, &out)
	eventRaw, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	ops := s.eventWriteOps(requestRef, nextRV, eventRaw)
	ops = append(ops,
		clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
		clientv3.OpPut(s.objectKey(requestRef), string(objectRaw)),
		clientv3.OpDelete(lockKey),
	)
	txnResp, err := s.client.Txn(ctx).If(
		s.metaRVCompare(metaRaw, metaVersion),
		clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(currentRaw)),
		clientv3.Compare(clientv3.Value(lockKey), "=", requestRef.Name),
	).Then(ops...).Commit()
	if err != nil {
		return nil, err
	}
	if !txnResp.Succeeded {
		return nil, ErrConflict
	}
	return cloneUnstructuredPtr(&out), nil
}

func (s *etcdStore) cleanupAdmissions(ctx context.Context) error {
	now := time.Now().UTC()
	resp, err := s.client.Get(ctx, s.objectPrefix(resourceScope{Resource: ResourceAdmissionRequests}), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return err
	}
	terminal := make([]Unstructured, 0)
	for _, kv := range resp.Kvs {
		var obj Unstructured
		if err := json.Unmarshal(kv.Value, &obj); err != nil {
			return err
		}
		spec, status, err := decodeAdmissionRequest(obj)
		if err != nil {
			return err
		}
		if status.Phase == AdmissionPendingPhase && !spec.ExpiresAt.IsZero() && !spec.ExpiresAt.After(now) {
			if _, err := s.expireAdmission(ctx, obj.Metadata.Name, AdmissionExpiredPhase, "admission timeout"); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				return err
			}
			status.Phase = AdmissionExpiredPhase
			status.Message = "admission timeout"
			status.DecidedAt = now
			updated, err := encodeAdmissionRequest(obj.Metadata, spec, status)
			if err != nil {
				return err
			}
			obj = *updated
		}
		if status.Phase != AdmissionPendingPhase {
			terminal = append(terminal, obj)
		}
	}
	if len(terminal) <= s.admissionRetention {
		return nil
	}
	sort.Slice(terminal, func(i, j int) bool {
		return terminalAdmissionTimestamp(terminal[i]).After(terminalAdmissionTimestamp(terminal[j]))
	})
	for _, obj := range terminal[s.admissionRetention:] {
		if now.Sub(terminalAdmissionTimestamp(obj)) < s.admissionTerminalRetention {
			continue
		}
		if err := s.deleteAdmissionRequest(ctx, obj.Metadata.Name); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return nil
}

func (s *etcdStore) finishAdmission(ctx context.Context, name string, mutate func(spec AdmissionRequestSpec, status *AdmissionRequestStatus)) (*Unstructured, error) {
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	for attempt := 0; attempt < etcdCommitRetries; attempt++ {
		_, metaRaw, metaVersion, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		requestResp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(requestResp.Kvs) == 0 {
			return nil, ErrNotFound
		}
		currentRaw := append([]byte(nil), requestResp.Kvs[0].Value...)
		var current Unstructured
		if err := json.Unmarshal(currentRaw, &current); err != nil {
			return nil, err
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return nil, err
		}
		if status.Phase != AdmissionPendingPhase {
			return cloneUnstructuredPtr(&current), nil
		}
		lockKey := s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name})
		lockResp, err := s.client.Get(ctx, lockKey, clientv3.WithLimit(1))
		if err != nil {
			return nil, err
		}
		if len(lockResp.Kvs) == 0 || string(lockResp.Kvs[0].Value) != name {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				continue
			}
		}
		mutate(spec, &status)
		updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return nil, err
		}
		currentRV, raw, version, err := s.readMetaRV(ctx)
		if err != nil {
			return nil, err
		}
		if raw != metaRaw || version != metaVersion {
			metaRaw, metaVersion = raw, version
		}
		nextRV := currentRV + 1
		out := cloneUnstructured(*updated)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		objectRaw, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		var oldObj Unstructured
		if err := json.Unmarshal(currentRaw, &oldObj); err != nil {
			return nil, err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitUpdate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchModified,
			Changed:           []string{"status"},
		}, &oldObj, &out)
		eventRaw, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		ops := s.eventWriteOps(requestRef, nextRV, eventRaw)
		ops = append(ops,
			clientv3.OpPut(s.metaKey("rv"), formatRV(nextRV)),
			clientv3.OpPut(s.objectKey(requestRef), string(objectRaw)),
			clientv3.OpDelete(lockKey),
		)
		txnResp, err := s.client.Txn(ctx).If(
			s.metaRVCompare(metaRaw, metaVersion),
			clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(currentRaw)),
			clientv3.Compare(clientv3.Value(lockKey), "=", name),
		).Then(ops...).Commit()
		if err != nil {
			return nil, err
		}
		if txnResp.Succeeded {
			return cloneUnstructuredPtr(&out), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil, ErrConflict
}

func (s *etcdStore) deleteAdmissionRequest(ctx context.Context, name string) error {
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	resp, err := s.client.Get(ctx, s.objectKey(requestRef), clientv3.WithLimit(1))
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return ErrNotFound
	}
	txnResp, err := s.client.Txn(ctx).If(
		clientv3.Compare(clientv3.Value(s.objectKey(requestRef)), "=", string(resp.Kvs[0].Value)),
	).Then(
		clientv3.OpDelete(s.objectKey(requestRef)),
	).Commit()
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return ErrConflict
	}
	return nil
}
