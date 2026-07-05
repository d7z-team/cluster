package cluster

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

type continueToken struct {
	Resource        string `json:"resource"`
	Namespace       string `json:"namespace,omitempty"`
	AllNamespaces   bool   `json:"allNamespaces,omitempty"`
	ResourceVersion string `json:"resourceVersion"`
	SelectorHash    string `json:"selectorHash"`
	LastCursor      string `json:"lastCursor"`
}

func selectorHash(selector Selector) string {
	if len(selector.requirements) == 0 {
		return ""
	}
	raw, err := json.Marshal(selector.requirements)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func encodeContinueToken(token continueToken) (string, error) {
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeContinueToken(value string) (continueToken, error) {
	if strings.TrimSpace(value) == "" {
		return continueToken{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return continueToken{}, fmt.Errorf("%w: invalid continue token", ErrInvalidObject)
	}
	var token continueToken
	if err := json.Unmarshal(raw, &token); err != nil {
		return continueToken{}, fmt.Errorf("%w: invalid continue token", ErrInvalidObject)
	}
	return token, nil
}

func lockKeyFromRef(ref objectRef) string {
	return ref.Resource + "\x00" + ref.Namespace + "\x00" + ref.Name
}

func decodeAdmissionRequest(obj Unstructured) (AdmissionRequestSpec, AdmissionRequestStatus, error) {
	typed, err := unstructuredToTyped[AdmissionRequestSpec, AdmissionRequestStatus](&obj)
	if err != nil {
		return AdmissionRequestSpec{}, AdmissionRequestStatus{}, err
	}
	return typed.Spec, typed.Status, nil
}

func encodeAdmissionRequest(meta Metadata, spec AdmissionRequestSpec, status AdmissionRequestStatus) (*Unstructured, error) {
	return typedToUnstructured(&Object[AdmissionRequestSpec, AdmissionRequestStatus]{
		APIVersion: "cluster.d7z.net/v1",
		Kind:       "AdmissionRequest",
		Metadata:   meta,
		Spec:       spec,
		Status:     status,
	})
}

func admissionTargetCommit(spec AdmissionRequestSpec) (commitRequest, *Unstructured, error) {
	if spec.Object == nil {
		return commitRequest{}, nil, ErrInvalidObject
	}
	target := cloneUnstructured(*spec.Object)
	ref := objectRef{Resource: spec.Resource, Namespace: spec.Namespace, Name: spec.Name}
	expectedRV, err := parseOptionalRV(spec.Precondition.ResourceVersion)
	if err != nil {
		return commitRequest{}, nil, err
	}
	req := commitRequest{
		Ref:              ref,
		Object:           &target,
		EventAnnotations: cloneAnnotations(spec.EventAnnotations),
	}
	switch spec.Operation {
	case AdmissionCreate:
		req.Op = commitCreate
		req.EventType = WatchAdded
		req.Changed = changedPaths(nil, &target, SubresourceSpec)
	case AdmissionUpdate:
		if spec.OldObject == nil {
			return commitRequest{}, nil, ErrInvalidObject
		}
		req.Op = commitUpdate
		req.ExpectedRV = expectedRV
		req.EventType = WatchModified
		req.Changed = changedPaths(spec.OldObject, &target, spec.Subresource)
	case AdmissionDelete:
		if spec.OldObject == nil {
			return commitRequest{}, nil, ErrInvalidObject
		}
		req.ExpectedRV = expectedRV
		if len(spec.OldObject.Metadata.Finalizers) > 0 && spec.OldObject.Metadata.DeletionTimestamp == nil {
			req.Op = commitUpdate
			req.EventType = WatchModified
			req.Changed = changedPaths(spec.OldObject, &target, SubresourceSpec)
		} else {
			req.Op = commitDelete
			req.EventType = WatchDeleted
			req.Changed = changedPaths(spec.OldObject, &target, SubresourceSpec)
		}
	default:
		return commitRequest{}, nil, ErrInvalidObject
	}
	return req, &target, nil
}
