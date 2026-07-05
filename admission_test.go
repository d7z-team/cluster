package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type waitAdmissionStore struct {
	expireErr error
}

func (s *waitAdmissionStore) get(context.Context, objectRef) (*Unstructured, error) {
	obj, err := encodeAdmissionRequest(
		Metadata{Name: "req-1"},
		AdmissionRequestSpec{
			Resource:  "widgets",
			Name:      "alpha",
			ExpiresAt: time.Now().UTC().Add(-time.Second),
		},
		AdmissionRequestStatus{Phase: AdmissionPendingPhase},
	)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (s *waitAdmissionStore) list(context.Context, resourceScope) ([]Unstructured, uint64, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) commit(context.Context, commitRequest) (*Unstructured, resourceEvent, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) admissionPending(context.Context, objectRef) (string, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) beginAdmission(context.Context, beginAdmissionRequest) (*Unstructured, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) approveAdmission(context.Context, approveAdmissionRequest) (*Unstructured, *Unstructured, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) rejectAdmission(context.Context, rejectAdmissionRequest) (*Unstructured, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) expireAdmission(context.Context, string, AdmissionPhase, string) (*Unstructured, error) {
	return nil, s.expireErr
}

func (s *waitAdmissionStore) eventsAfter(context.Context, uint64, resourceScope, int) ([]resourceEvent, uint64, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) cleanupEvents(context.Context) error {
	panic("unexpected call")
}

func (s *waitAdmissionStore) subscribe(context.Context, resourceScope) (<-chan struct{}, func(), error) {
	ch := make(chan struct{})
	return ch, func() { close(ch) }, nil
}

func (s *waitAdmissionStore) acquireNode(context.Context, string, time.Duration) (string, error) {
	panic("unexpected call")
}

func (s *waitAdmissionStore) renewNode(context.Context, string, string, time.Duration) error {
	panic("unexpected call")
}

func (s *waitAdmissionStore) releaseNode(context.Context, string, string) error {
	panic("unexpected call")
}

func (s *waitAdmissionStore) close() error {
	return nil
}

func TestWaitAdmissionReturnsExpireError(t *testing.T) {
	store := &waitAdmissionStore{expireErr: errors.New("expire failed")}
	resource := &UnstructuredResource{
		cluster: &Cluster{store: store},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := resource.waitAdmission(ctx, "req-1")
	require.EqualError(t, err, "expire failed")
}
