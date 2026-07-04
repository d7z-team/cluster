package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func (s *badgerStore) admissionPending(ctx context.Context, ref objectRef) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return "", ErrClosed
	}
	var out string
	err := s.db.View(func(txn *badger.Txn) error {
		value, exists, err := s.getAdmissionLockTxn(txn, ref)
		if err != nil || !exists {
			return err
		}
		out = value
		return nil
	})
	return out, err
}

func (s *badgerStore) beginAdmission(ctx context.Context, req beginAdmissionRequest) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	now := time.Now().UTC()
	token, err := randomToken("uid")
	if err != nil {
		return nil, err
	}
	req.Request.Metadata.UID = token
	req.Request.Metadata.ResourceVersion = ""
	req.Request.Metadata.Generation = 1
	req.Request.Metadata.CreatedAt = now
	req.Request.Metadata.UpdatedAt = now
	var out Unstructured
	err = s.db.Update(func(txn *badger.Txn) error {
		if _, exists, err := s.getAdmissionLockTxn(txn, req.Target); err != nil {
			return err
		} else if exists {
			return ErrAdmissionPending
		}
		spec, _, err := decodeAdmissionRequest(*req.Request)
		if err != nil {
			return err
		}
		currentTarget, targetExists, err := s.getObjectTxn(txn, req.Target)
		if err != nil {
			return err
		}
		switch {
		case spec.Precondition.MustNotExist:
			if targetExists {
				return ErrAlreadyExists
			}
		case spec.Precondition.MustExist:
			if !targetExists {
				return ErrNotFound
			}
			if spec.Precondition.ResourceVersion != "" && currentTarget.Metadata.ResourceVersion != spec.Precondition.ResourceVersion {
				return ErrConflict
			}
		}
		requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name}
		if _, exists, err := s.getObjectTxn(txn, requestRef); err != nil {
			return err
		} else if exists {
			return ErrAlreadyExists
		}
		currentRV, err := s.currentRV(txn)
		if err != nil {
			return err
		}
		nextRV := currentRV + 1
		out = cloneUnstructured(*req.Request)
		out.Metadata.ResourceVersion = formatRV(nextRV)
		raw, err := json.Marshal(out)
		if err != nil {
			return err
		}
		event := newStoreEvent(commitRequest{
			Op:                commitCreate,
			Ref:               requestRef,
			SkipAdmissionLock: true,
			Object:            &out,
			EventType:         WatchAdded,
			Changed:           []string{"spec", "status"},
		}, nil, &out)
		if err := txn.Set([]byte(s.objectKey(requestRef)), raw); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.admissionLockKey(req.Target)), []byte(req.Request.Metadata.Name)); err != nil {
			return err
		}
		if err := s.writeEventTxn(txn, event); err != nil {
			return err
		}
		return s.setRV(txn, nextRV)
	})
	if err != nil {
		return nil, err
	}
	s.hub.notify(objectRef{Resource: ResourceAdmissionRequests, Name: req.Request.Metadata.Name})
	return cloneUnstructuredPtr(&out), nil
}

func (s *badgerStore) approveAdmission(ctx context.Context, req approveAdmissionRequest) (*Unstructured, *Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, ErrClosed
	}
	var targetOut *Unstructured
	var requestOut *Unstructured
	var targetNotify *objectRef
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	err := s.db.Update(func(txn *badger.Txn) error {
		current, exists, err := s.getObjectTxn(txn, requestRef)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return err
		}
		if status.Phase != AdmissionPendingPhase {
			requestOut = cloneUnstructuredPtr(&current)
			return nil
		}
		if req.RequireRule != "" && !slices.Contains(spec.Rules, req.RequireRule) {
			return ErrInvalidObject
		}
		if req.Decision.Rule == "" {
			req.Decision.Rule = req.RequireRule
		}
		if req.Decision.Rule == "" {
			return ErrInvalidObject
		}
		for _, decision := range status.Approved {
			if decision.Rule == req.Decision.Rule {
				requestOut = cloneUnstructuredPtr(&current)
				return nil
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
				return err
			}
			requestOut, err = s.updateAdmissionRequestTxn(txn, requestRef, current.Metadata.ResourceVersion, updated, []string{"status.approved"})
			return err
		}
		targetCommit, targetObj, err := admissionTargetCommit(spec)
		if err != nil {
			return err
		}
		targetCommit.SkipAdmissionLock = true
		targetOut, _, err = s.commitAdmissionTargetTxn(txn, targetCommit)
		if err != nil {
			if !errors.Is(err, ErrAlreadyExists) && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				return err
			}
			status.Phase = AdmissionFailedPhase
			status.Message = err.Error()
			status.LastError = err.Error()
			status.LastErrorAt = now
			status.DecidedBy = req.Decision.Decider
			status.DecidedAt = now
			updated, encodeErr := encodeAdmissionRequest(current.Metadata, spec, status)
			if encodeErr != nil {
				return encodeErr
			}
			requestOut, err = s.updateAdmissionRequestTxn(txn, requestRef, current.Metadata.ResourceVersion, updated, []string{"status"})
			if err != nil {
				return err
			}
			if err := txn.Delete([]byte(s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return nil
		}
		status.Phase = AdmissionCommittedPhase
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
		status.LastError = ""
		status.LastErrorAt = time.Time{}
		status.TargetResourceVersion = targetOut.Metadata.ResourceVersion
		status.TargetObject = targetObj
		updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return err
		}
		requestOut, err = s.updateAdmissionRequestTxn(txn, requestRef, current.Metadata.ResourceVersion, updated, []string{"status"})
		if err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		targetNotify = &objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if targetNotify != nil {
		s.hub.notify(*targetNotify)
	}
	s.hub.notify(requestRef)
	return targetOut, requestOut, nil
}

func (s *badgerStore) rejectAdmission(ctx context.Context, req rejectAdmissionRequest) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: req.Name}
	var out *Unstructured
	err := s.db.Update(func(txn *badger.Txn) error {
		current, exists, err := s.getObjectTxn(txn, requestRef)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return err
		}
		if status.Phase != AdmissionPendingPhase {
			out = cloneUnstructuredPtr(&current)
			return nil
		}
		now := time.Now().UTC()
		status.Phase = AdmissionRejectedPhase
		status.RejectedRule = req.Decision.Rule
		status.Message = req.Decision.Message
		status.DecidedBy = req.Decision.Decider
		status.DecidedAt = now
		updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return err
		}
		out, err = s.updateAdmissionRequestTxn(txn, requestRef, current.Metadata.ResourceVersion, updated, []string{"status"})
		if err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.hub.notify(requestRef)
	return out, nil
}

func (s *badgerStore) expireAdmission(ctx context.Context, name string, phase AdmissionPhase, message string) (*Unstructured, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	requestRef := objectRef{Resource: ResourceAdmissionRequests, Name: name}
	var out *Unstructured
	err := s.db.Update(func(txn *badger.Txn) error {
		current, exists, err := s.getObjectTxn(txn, requestRef)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNotFound
		}
		spec, status, err := decodeAdmissionRequest(current)
		if err != nil {
			return err
		}
		if status.Phase != AdmissionPendingPhase {
			out = cloneUnstructuredPtr(&current)
			return nil
		}
		status.Phase = phase
		status.Message = message
		status.DecidedAt = time.Now().UTC()
		updated, err := encodeAdmissionRequest(current.Metadata, spec, status)
		if err != nil {
			return err
		}
		out, err = s.updateAdmissionRequestTxn(txn, requestRef, current.Metadata.ResourceVersion, updated, []string{"status"})
		if err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.hub.notify(requestRef)
	return out, nil
}

func (s *badgerStore) cleanupAdmissions(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	var notify []objectRef
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		notify, err = s.cleanupAdmissionsTxn(ctx, txn, time.Now().UTC())
		return err
	})
	if err != nil {
		return err
	}
	for _, ref := range notify {
		s.hub.notify(ref)
	}
	return nil
}

func (s *badgerStore) updateAdmissionRequestTxn(txn *badger.Txn, ref objectRef, currentRV string, updated *Unstructured, changed []string) (*Unstructured, error) {
	current, exists, err := s.getObjectTxn(txn, ref)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}
	if current.Metadata.ResourceVersion != currentRV {
		return nil, ErrConflict
	}
	rv, err := s.currentRV(txn)
	if err != nil {
		return nil, err
	}
	nextRV := rv + 1
	out := cloneUnstructured(*updated)
	out.Metadata.ResourceVersion = formatRV(nextRV)
	out.Metadata.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	if err := txn.Set([]byte(s.objectKey(ref)), raw); err != nil {
		return nil, err
	}
	event := newStoreEvent(commitRequest{
		Op:                commitUpdate,
		Ref:               ref,
		SkipAdmissionLock: true,
		Object:            &out,
		EventType:         WatchModified,
		Changed:           changed,
	}, &current, &out)
	if err := s.writeEventTxn(txn, event); err != nil {
		return nil, err
	}
	if err := s.setRV(txn, nextRV); err != nil {
		return nil, err
	}
	return cloneUnstructuredPtr(&out), nil
}

func (s *badgerStore) cleanupAdmissionsTxn(ctx context.Context, txn *badger.Txn, now time.Time) ([]objectRef, error) {
	prefix := s.objectPrefix(resourceScope{Resource: ResourceAdmissionRequests})
	opts := badger.DefaultIteratorOptions
	opts.Prefix = []byte(prefix)
	it := txn.NewIterator(opts)
	defer it.Close()

	notify := make([]objectRef, 0)
	terminal := make([]Unstructured, 0)
	for it.Rewind(); it.ValidForPrefix([]byte(prefix)); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var obj Unstructured
		if err := it.Item().Value(func(value []byte) error {
			return json.Unmarshal(value, &obj)
		}); err != nil {
			return nil, err
		}
		spec, status, err := decodeAdmissionRequest(obj)
		if err != nil {
			return nil, err
		}
		if status.Phase == AdmissionPendingPhase && !spec.ExpiresAt.IsZero() && !spec.ExpiresAt.After(now) {
			status.Phase = AdmissionExpiredPhase
			status.Message = "admission timeout"
			status.DecidedAt = now
			updated, err := encodeAdmissionRequest(obj.Metadata, spec, status)
			if err != nil {
				return nil, err
			}
			updated.Metadata.UpdatedAt = now
			objPtr, err := s.updateAdmissionRequestTxn(txn, objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name}, obj.Metadata.ResourceVersion, updated, []string{"status"})
			if err != nil {
				return nil, err
			}
			obj = *objPtr
			if err := txn.Delete([]byte(s.admissionLockKey(objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return nil, err
			}
			notify = append(notify, objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name})
		}
		if status.Phase != AdmissionPendingPhase {
			terminal = append(terminal, obj)
		}
	}
	if len(terminal) <= s.admissionRetention {
		return notify, nil
	}
	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].Metadata.UpdatedAt.After(terminal[j].Metadata.UpdatedAt)
	})
	for _, obj := range terminal[s.admissionRetention:] {
		if now.Sub(obj.Metadata.UpdatedAt) < s.admissionTerminalRetention {
			continue
		}
		if err := txn.Delete([]byte(s.objectKey(objectRef{Resource: ResourceAdmissionRequests, Name: obj.Metadata.Name}))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return nil, err
		}
	}
	return notify, nil
}

func (s *badgerStore) commitAdmissionTargetTxn(txn *badger.Txn, req commitRequest) (*Unstructured, resourceEvent, error) {
	current, exists, err := s.getObjectTxn(txn, req.Ref)
	if err != nil {
		return nil, resourceEvent{}, err
	}
	switch req.Op {
	case commitCreate:
		if exists {
			return nil, resourceEvent{}, ErrAlreadyExists
		}
	case commitUpdate, commitDelete:
		if !exists {
			return nil, resourceEvent{}, ErrNotFound
		}
		if parseStoredRV(current.Metadata.ResourceVersion) != req.ExpectedRV {
			return nil, resourceEvent{}, ErrConflict
		}
	default:
		return nil, resourceEvent{}, ErrUnsupported
	}
	currentRV, err := s.currentRV(txn)
	if err != nil {
		return nil, resourceEvent{}, err
	}
	nextRV := currentRV + 1
	out := cloneUnstructured(*req.Object)
	out.Metadata.ResourceVersion = formatRV(nextRV)
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, resourceEvent{}, err
	}
	if req.Op == commitDelete {
		if err := txn.Delete([]byte(s.objectKey(req.Ref))); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return nil, resourceEvent{}, err
		}
	} else if err := txn.Set([]byte(s.objectKey(req.Ref)), raw); err != nil {
		return nil, resourceEvent{}, err
	}
	var oldObj *Unstructured
	if exists {
		oldObj = cloneUnstructuredPtr(&current)
	}
	event := newStoreEvent(req, oldObj, &out)
	if err := s.writeEventTxn(txn, event); err != nil {
		return nil, resourceEvent{}, err
	}
	if err := s.setRV(txn, nextRV); err != nil {
		return nil, resourceEvent{}, err
	}
	return cloneUnstructuredPtr(&out), event, nil
}
