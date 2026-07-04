package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsEmptyJSONValue(t *testing.T) {
	require.True(t, isEmptyJSONValue(nil))
	require.True(t, isEmptyJSONValue(json.RawMessage("null")))
	require.False(t, isEmptyJSONValue(json.RawMessage(`""`)))
	require.False(t, isEmptyJSONValue(json.RawMessage(`"hello"`)))
	require.False(t, isEmptyJSONValue(json.RawMessage(`42`)))
	require.False(t, isEmptyJSONValue(json.RawMessage(`true`)))
}

func TestRawScalarString(t *testing.T) {
	val, ok := rawScalarString(json.RawMessage(`"hello"`))
	require.True(t, ok)
	require.Equal(t, "hello", val)

	val, ok = rawScalarString(json.RawMessage(`true`))
	require.True(t, ok)
	require.Equal(t, "true", val)

	val, ok = rawScalarString(json.RawMessage(`false`))
	require.True(t, ok)
	require.Equal(t, "false", val)

	val, ok = rawScalarString(json.RawMessage(`42`))
	require.True(t, ok)
	require.Equal(t, "42", val)

	_, ok = rawScalarString(json.RawMessage(`{"key":"value"}`))
	require.False(t, ok)

	_, ok = rawScalarString(json.RawMessage(`null`))
	require.False(t, ok)

	_, ok = rawScalarString(json.RawMessage(`["a","b"]`))
	require.False(t, ok)
}

func TestOperationTimeout(t *testing.T) {
	require.Equal(t, time.Second, operationTimeout(100*time.Millisecond, 30*time.Second))
	require.Equal(t, time.Second, operationTimeout(1*time.Millisecond, 30*time.Second))
	require.Equal(t, 2*time.Second, operationTimeout(5*time.Second, 2*time.Second))
	require.Equal(t, 3*time.Second, operationTimeout(3*time.Second, 30*time.Second))
}

func TestParseRVKey(t *testing.T) {
	rv := parseRVKey("prefix/resource/events/00000000000000000042")
	require.Equal(t, uint64(42), rv)
}

func TestFieldRawValueEdgeCases(t *testing.T) {
	obj := &Unstructured{
		Metadata: Metadata{
			Name:      "test",
			Namespace: "ns",
			Labels:    Labels{"key": "value"},
		},
		Spec:   json.RawMessage(`{"nested":{"deep":"found"}}`),
		Status: json.RawMessage(`{"phase":"Ready"}`),
	}

	val, ok := fieldRawValue(obj, "metadata.name")
	require.True(t, ok)
	require.Equal(t, `"test"`, string(val))

	val, ok = fieldRawValue(obj, "metadata.namespace")
	require.True(t, ok)
	require.Equal(t, `"ns"`, string(val))

	_, ok = fieldRawValue(obj, "metadata.labels")
	require.True(t, ok)

	val, ok = fieldRawValue(obj, "metadata.labels.key")
	require.True(t, ok)
	require.Equal(t, `"value"`, string(val))

	val, ok = fieldRawValue(obj, "spec.nested.deep")
	require.True(t, ok)
	require.Equal(t, `"found"`, string(val))

	_, ok = fieldRawValue(obj, "spec.nested.missing")
	require.False(t, ok)

	_, ok = fieldRawValue(obj, "spec")
	require.False(t, ok)

	val, ok = fieldRawValue(obj, "status.phase")
	require.True(t, ok)
	require.Equal(t, `"Ready"`, string(val))

	_, ok = fieldRawValue(obj, "invalid")
	require.False(t, ok)

	_, ok = fieldRawValue(obj, "invalid.path")
	require.False(t, ok)

	empty := &Unstructured{}
	_, ok = fieldRawValue(empty, "spec.missing")
	require.False(t, ok)
}

func TestValidateResourceName(t *testing.T) {
	require.Error(t, validateResourceName(""))
	require.Error(t, validateResourceName("."))
	require.Error(t, validateResourceName(".."))
	require.Error(t, validateResourceName("a/b"))
	require.Error(t, validateResourceName("a\\b"))
	require.NoError(t, validateResourceName("valid-resource"))
}

func TestValidateObjectName(t *testing.T) {
	require.Error(t, validateObjectName(""))
	require.Error(t, validateObjectName("."))
	require.Error(t, validateObjectName(".."))
	require.Error(t, validateObjectName("a/b"))
	require.NoError(t, validateObjectName("valid-name"))
}

func TestValidateNamespace(t *testing.T) {
	require.Error(t, validateNamespace(""))
	require.Error(t, validateNamespace("."))
	require.Error(t, validateNamespace(".."))
	require.Error(t, validateNamespace("a/b"))
	require.NoError(t, validateNamespace("valid-ns"))
}

func TestValidateMetadataKeys(t *testing.T) {
	require.NoError(t, validateMetadataKeys(Labels{}))
	require.NoError(t, validateMetadataKeys(Labels{"key": "value"}))
	require.Error(t, validateMetadataKeys(Labels{"": "value"}))
	require.Error(t, validateMetadataKeys(Labels{"k\x00ey": "value"}))
	require.NoError(t, validateMetadataKeys(Annotations{}))
	require.Error(t, validateMetadataKeys(Annotations{"": "value"}))
}

func TestValidateMetadataWithSchema(t *testing.T) {
	require.NoError(t, validateMetadataWithSchema(Metadata{Name: "test"}))
	require.NoError(t, validateMetadataWithSchema(Metadata{Name: "test", Namespace: "ns"}))
	require.Error(t, validateMetadataWithSchema(Metadata{Name: "test", Namespace: "../bad"}))
	require.Error(t, validateMetadataWithSchema(Metadata{Name: "test", Labels: Labels{"": "val"}}))
	require.Error(t, validateMetadataWithSchema(Metadata{Name: "test", Annotations: Annotations{"k\x00ey": "val"}}))
}

func TestValidateMetadataPatchWithSchema(t *testing.T) {
	writable := map[string]struct{}{
		"labels":      {},
		"annotations": {},
		"finalizers":  {},
	}
	require.NoError(t, validateMetadataPatchWithSchema([]byte(`{"labels":{"app":"demo"}}`), writable))
	require.Error(t, validateMetadataPatchWithSchema([]byte(`{"uid":"override"}`), writable))
	require.Error(t, validateMetadataPatchWithSchema([]byte(`{}`), writable))
}

func TestValidateSpecPatch(t *testing.T) {
	writable := map[string]struct{}{"labels": {}, "annotations": {}}
	require.NoError(t, validateSpecPatch([]byte(`{"metadata":{"labels":{"app":"demo"}}}`), writable))
	require.NoError(t, validateSpecPatch([]byte(`{"spec":{"size":"small"}}`), writable))
	require.Error(t, validateSpecPatch([]byte(`{"status":{"phase":"Ready"}}`), writable))
	require.Error(t, validateSpecPatch([]byte(`{"metadata":{"uid":"override"}}`), writable))
}

func TestValidateRawObjectJSON(t *testing.T) {
	require.NoError(t, validateRawObjectJSON(&Unstructured{
		Spec:   json.RawMessage(`{"key":"value"}`),
		Status: json.RawMessage(`{"phase":"Ready"}`),
	}))
	require.NoError(t, validateRawObjectJSON(&Unstructured{
		Spec:   json.RawMessage(nil),
		Status: json.RawMessage(nil),
	}))
	require.Error(t, validateRawObjectJSON(&Unstructured{
		Spec: json.RawMessage(`{invalid`),
	}))
	require.Error(t, validateRawObjectJSON(&Unstructured{
		Spec:   json.RawMessage(`{"key":"value"}`),
		Status: json.RawMessage(`{invalid`),
	}))
}

func TestValidateRawJSONField(t *testing.T) {
	require.NoError(t, validateRawJSONField("spec", json.RawMessage(`{"key":"value"}`)))
	require.NoError(t, validateRawJSONField("spec", json.RawMessage("  ")))
	require.NoError(t, validateRawJSONField("spec", nil))
	require.Error(t, validateRawJSONField("spec", json.RawMessage(`{invalid`)))
}

func TestParseOptionalRV(t *testing.T) {
	rv, err := parseOptionalRV("")
	require.NoError(t, err)
	require.Equal(t, uint64(0), rv)

	rv, err = parseOptionalRV("42")
	require.NoError(t, err)
	require.Equal(t, uint64(42), rv)

	_, err = parseOptionalRV("0")
	require.Error(t, err)

	_, err = parseOptionalRV("invalid")
	require.Error(t, err)
}

func TestParseRequiredRV(t *testing.T) {
	rv, err := parseRequiredRV("42")
	require.NoError(t, err)
	require.Equal(t, uint64(42), rv)

	_, err = parseRequiredRV("")
	require.ErrorIs(t, err, ErrConflict)
}

func TestUpdateRV(t *testing.T) {
	rv, err := updateRV("42", "99")
	require.NoError(t, err)
	require.Equal(t, uint64(42), rv)

	rv, err = updateRV("", "99")
	require.NoError(t, err)
	require.Equal(t, uint64(99), rv)

	_, err = updateRV("", "")
	require.ErrorIs(t, err, ErrConflict)
}

func TestFormatRV(t *testing.T) {
	require.Equal(t, "42", formatRV(42))
	require.Empty(t, formatRV(0))
}

func TestParseStoredRV(t *testing.T) {
	require.Equal(t, uint64(42), parseStoredRV("42"))
	require.Equal(t, uint64(0), parseStoredRV("invalid"))
}

func TestRVKey(t *testing.T) {
	require.Equal(t, "00000000000000000042", rvKey(42))
}

func TestObjectCursor(t *testing.T) {
	require.Equal(t, "ns\x00name\x00uid-1", objectCursor(Unstructured{
		APIVersion: "v1",
		Kind:       "Test",
		Metadata:   Metadata{Namespace: "ns", Name: "name", UID: "uid-1"},
	}))
}

func TestCloneLabels(t *testing.T) {
	require.Nil(t, cloneLabels(nil))
	orig := Labels{"a": "1", "b": "2"}
	copied := cloneLabels(orig)
	require.Equal(t, orig, copied)
	copied["a"] = "changed"
	require.Equal(t, "1", orig["a"])
}

func TestCloneAnnotations(t *testing.T) {
	require.Nil(t, cloneAnnotations(nil))
	orig := Annotations{"a": "1"}
	copied := cloneAnnotations(orig)
	require.Equal(t, orig, copied)
	copied["a"] = "changed"
	require.Equal(t, "1", orig["a"])
}

func TestCloneUnstructuredPtr(t *testing.T) {
	require.Nil(t, cloneUnstructuredPtr(nil))
	orig := &Unstructured{Metadata: Metadata{Name: "test"}}
	copied := cloneUnstructuredPtr(orig)
	require.Equal(t, orig.Metadata.Name, copied.Metadata.Name)
	copied.Metadata.Name = "changed"
	require.Equal(t, "test", orig.Metadata.Name)
}

func TestCloneMetadata(t *testing.T) {
	orig := Metadata{
		Name:        "test",
		Labels:      Labels{"a": "1"},
		Annotations: Annotations{"b": "2"},
		Finalizers:  []string{"f1"},
	}
	copied := cloneMetadata(orig)
	require.Equal(t, orig.Name, copied.Name)
	require.Equal(t, orig.Labels, copied.Labels)
	copied.Labels["a"] = "changed"
	require.Equal(t, "1", orig.Labels["a"])
}

func TestCloneTimePtr(t *testing.T) {
	require.Nil(t, cloneTimePtr(nil))
	now := time.Now().UTC()
	copied := cloneTimePtr(&now)
	require.Equal(t, now, *copied)
}

func TestEnsureMetadataMaps(t *testing.T) {
	meta := &Metadata{Name: "test"}
	ensureMetadataMaps(meta)
	require.NotNil(t, meta.Labels)
	require.NotNil(t, meta.Annotations)
}

func TestSortedUnique(t *testing.T) {
	require.Nil(t, sortedUnique(nil))
	require.Nil(t, sortedUnique([]string{}))
	result := sortedUnique([]string{"b", "a", "", "a", "c"})
	require.Equal(t, []string{"a", "b", "c"}, result)
}

func TestJSONEqual(t *testing.T) {
	require.True(t, jsonEqual(nil, nil))
	require.True(t, jsonEqual(json.RawMessage(`{"a":1}`), json.RawMessage(`{"a":1}`)))
	require.True(t, jsonEqual(json.RawMessage(``), json.RawMessage(`   `)))
	require.False(t, jsonEqual(json.RawMessage(`{"a":1}`), json.RawMessage(`{"a":2}`)))
	require.False(t, jsonEqual(json.RawMessage(`"a"`), json.RawMessage(`"b"`)))
}

func TestLockKeyFromRef(t *testing.T) {
	require.Equal(t, "res\x00ns\x00name", lockKeyFromRef(objectRef{Resource: "res", Namespace: "ns", Name: "name"}))
}

func TestNormalizeStorePrefix(t *testing.T) {
	require.Empty(t, normalizeStorePrefix(""))
	require.Equal(t, "app/", normalizeStorePrefix("app"))
	require.Equal(t, "app/", normalizeStorePrefix("/app"))
	require.Equal(t, "app/", normalizeStorePrefix("app/"))
	require.Equal(t, "app/", normalizeStorePrefix("/app/"))
}

func TestDecodeAdmissionRequest(t *testing.T) {
	obj := Unstructured{
		APIVersion: "cluster.d7z.net/v1",
		Kind:       "AdmissionRequest",
		Metadata:   Metadata{Name: "adm_test"},
		Spec:       json.RawMessage(`{"resource":"widgets","name":"alpha","operation":"CREATE"}`),
		Status:     json.RawMessage(`{"phase":"Pending"}`),
	}
	spec, status, err := decodeAdmissionRequest(obj)
	require.NoError(t, err)
	require.Equal(t, "widgets", spec.Resource)
	require.Equal(t, "alpha", spec.Name)
	require.Equal(t, AdmissionPendingPhase, status.Phase)

	objBad := Unstructured{Spec: json.RawMessage(`{invalid}`)}
	_, _, err = decodeAdmissionRequest(objBad)
	require.Error(t, err)
}

func TestEncodeAdmissionRequest(t *testing.T) {
	out, err := encodeAdmissionRequest(
		Metadata{Name: "adm_test"},
		AdmissionRequestSpec{Resource: "widgets", Name: "alpha"},
		AdmissionRequestStatus{Phase: AdmissionPendingPhase},
	)
	require.NoError(t, err)
	require.Equal(t, "cluster.d7z.net/v1", out.APIVersion)
	require.Equal(t, "AdmissionRequest", out.Kind)
	require.Equal(t, "adm_test", out.Metadata.Name)
}

func TestAdmissionTargetCommit(t *testing.T) {
	t.Run("create", func(t *testing.T) {
		obj := Unstructured{
			APIVersion: "example.test/v1",
			Kind:       "Widget",
			Metadata:   Metadata{Name: "alpha"},
			Spec:       json.RawMessage(`{"size":"small"}`),
		}
		req, target, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionCreate,
			Resource:  "widgets",
			Name:      "alpha",
			Object:    &obj,
			Precondition: AdmissionPrecondition{
				MustNotExist: true,
			},
		})
		require.NoError(t, err)
		require.Equal(t, commitCreate, req.Op)
		require.Equal(t, WatchAdded, req.EventType)
		require.NotNil(t, target)
	})

	t.Run("update", func(t *testing.T) {
		oldObj := Unstructured{
			Metadata: Metadata{Name: "alpha", ResourceVersion: "5"},
		}
		newObj := Unstructured{
			Metadata: Metadata{Name: "alpha"},
			Spec:     json.RawMessage(`{"size":"large"}`),
		}
		req, _, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionUpdate,
			Resource:  "widgets",
			Name:      "alpha",
			Object:    &newObj,
			OldObject: &oldObj,
			Precondition: AdmissionPrecondition{
				MustExist:       true,
				ResourceVersion: "5",
			},
		})
		require.NoError(t, err)
		require.Equal(t, commitUpdate, req.Op)
		require.Equal(t, uint64(5), req.ExpectedRV)
	})

	t.Run("update_missing_old", func(t *testing.T) {
		obj := Unstructured{Metadata: Metadata{Name: "alpha"}}
		_, _, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionUpdate,
			Object:    &obj,
		})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("delete_with_finalizers", func(t *testing.T) {
		now := time.Now().UTC()
		oldObj := Unstructured{
			Metadata: Metadata{Name: "alpha", Finalizers: []string{"cleanup"}},
		}
		newObj := Unstructured{
			Metadata: Metadata{Name: "alpha", DeletionTimestamp: &now, Finalizers: []string{"cleanup"}},
		}
		req, target, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionDelete,
			Resource:  "widgets",
			Name:      "alpha",
			Object:    &newObj,
			OldObject: &oldObj,
		})
		require.NoError(t, err)
		require.Equal(t, commitUpdate, req.Op)
		require.Equal(t, WatchModified, req.EventType)
		require.NotNil(t, target)
	})

	t.Run("delete_without_finalizers", func(t *testing.T) {
		oldObj := Unstructured{Metadata: Metadata{Name: "alpha"}}
		newObj := Unstructured{Metadata: Metadata{Name: "alpha"}}
		req, _, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionDelete,
			Resource:  "widgets",
			Name:      "alpha",
			Object:    &newObj,
			OldObject: &oldObj,
		})
		require.NoError(t, err)
		require.Equal(t, commitDelete, req.Op)
		require.Equal(t, WatchDeleted, req.EventType)
	})

	t.Run("nil_object", func(t *testing.T) {
		_, _, err := admissionTargetCommit(AdmissionRequestSpec{})
		require.ErrorIs(t, err, ErrInvalidObject)
	})

	t.Run("invalid_operation", func(t *testing.T) {
		obj := Unstructured{Metadata: Metadata{Name: "alpha"}}
		_, _, err := admissionTargetCommit(AdmissionRequestSpec{
			Operation: AdmissionOperation("BAD"),
			Object:    &obj,
		})
		require.ErrorIs(t, err, ErrInvalidObject)
	})
}

func TestApplyDefaultMap(t *testing.T) {
	t.Run("sets_missing_key", func(t *testing.T) {
		root := map[string]any{}
		changed := applyDefaultMap(root, []string{"name"}, json.RawMessage(`"default"`))
		require.True(t, changed)
		require.Equal(t, "default", root["name"])
	})

	t.Run("skips_existing_key", func(t *testing.T) {
		root := map[string]any{"name": "existing"}
		changed := applyDefaultMap(root, []string{"name"}, json.RawMessage(`"default"`))
		require.False(t, changed)
		require.Equal(t, "existing", root["name"])
	})

	t.Run("nested_path", func(t *testing.T) {
		root := map[string]any{}
		changed := applyDefaultMap(root, []string{"spec", "size"}, json.RawMessage(`"medium"`))
		require.True(t, changed)
		require.Equal(t, "medium", root["spec"].(map[string]any)["size"])
	})

	t.Run("empty_segments", func(t *testing.T) {
		root := map[string]any{}
		changed := applyDefaultMap(root, nil, json.RawMessage(`"x"`))
		require.False(t, changed)
	})
}

func TestApplyDefaultRule(t *testing.T) {
	obj := &Unstructured{
		Spec:   json.RawMessage(`{"size":"small"}`),
		Status: json.RawMessage(`{}`),
	}
	err := applyDefaultRule(obj, defaultRule{Path: "spec.size", Value: json.RawMessage(`"medium"`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"small"}`, string(obj.Spec))

	err = applyDefaultRule(obj, defaultRule{Path: "spec.owner", Value: json.RawMessage(`"default"`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"small","owner":"default"}`, string(obj.Spec))

	err = applyDefaultRule(obj, defaultRule{Path: "status.phase", Value: json.RawMessage(`"Pending"`)})
	require.NoError(t, err)
	require.JSONEq(t, `{"phase":"Pending"}`, string(obj.Status))
}

func TestApplyDefaultRules(t *testing.T) {
	obj := &Unstructured{Spec: json.RawMessage(`{}`)}
	err := applyDefaultRules(obj, []defaultRule{
		{Path: "spec.size", Value: json.RawMessage(`"medium"`)},
		{Path: "spec.owner", Value: json.RawMessage(`"team"`)},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"medium","owner":"team"}`, string(obj.Spec))
}

func TestCloneEvent(t *testing.T) {
	obj := &Unstructured{Metadata: Metadata{Name: "test"}}
	event := resourceEvent{
		Type:            WatchAdded,
		ResourceVersion: "42",
		Ref:             objectRef{Resource: "widgets", Name: "test"},
		Object:          obj,
		Annotations:     Annotations{"a": "1"},
		Changed:         []string{"spec.size"},
	}
	copied := cloneEvent(event)
	require.Equal(t, event.Type, copied.Type)
	require.Equal(t, event.ResourceVersion, copied.ResourceVersion)
	copied.Annotations["a"] = "changed"
	require.Equal(t, "1", event.Annotations["a"])
	copied.Object.Metadata.Name = "changed"
	require.Equal(t, "test", event.Object.Metadata.Name)
}

func TestNewStoreEvent(t *testing.T) {
	req := commitRequest{
		EventType:        WatchAdded,
		Ref:              objectRef{Resource: "widgets", Name: "test"},
		EventAnnotations: Annotations{"a": "1"},
		Changed:          []string{"spec.size"},
	}
	obj := &Unstructured{Metadata: Metadata{Name: "test", ResourceVersion: "42"}}
	event := newStoreEvent(req, nil, obj)
	require.Equal(t, WatchAdded, event.Type)
	require.Equal(t, "42", event.ResourceVersion)
	require.Equal(t, "widgets", event.Ref.Resource)
	require.Equal(t, "1", event.Annotations["a"])
}

func TestChangedPaths(t *testing.T) {
	t.Run("both_nil", func(t *testing.T) {
		paths := changedPaths(nil, nil, SubresourceSpec)
		require.Empty(t, paths)
	})

	t.Run("create", func(t *testing.T) {
		obj := &Unstructured{
			Spec:   json.RawMessage(`{"size":"small"}`),
			Status: json.RawMessage(`{"phase":"Ready"}`),
		}
		paths := changedPaths(nil, obj, SubresourceSpec)
		require.Contains(t, paths, "metadata")
		require.Contains(t, paths, "spec.size")
		require.Contains(t, paths, "status.phase")
	})

	t.Run("status_subresource", func(t *testing.T) {
		oldObj := &Unstructured{Status: json.RawMessage(`{"phase":"Pending"}`)}
		newObj := &Unstructured{Status: json.RawMessage(`{"phase":"Ready"}`)}
		paths := changedPaths(oldObj, newObj, SubresourceStatus)
		require.Equal(t, []string{"status.phase"}, paths)
	})

	t.Run("labels_changed", func(t *testing.T) {
		oldObj := &Unstructured{
			Metadata: Metadata{Labels: Labels{"a": "1"}},
			Spec:     json.RawMessage(`{"size":"small"}`),
		}
		newObj := &Unstructured{
			Metadata: Metadata{Labels: Labels{"a": "2"}},
			Spec:     json.RawMessage(`{"size":"small"}`),
		}
		paths := changedPaths(oldObj, newObj, SubresourceSpec)
		require.Contains(t, paths, "metadata.labels")
		require.NotContains(t, paths, "spec.size")
	})

	t.Run("deletionTimestamp_changed", func(t *testing.T) {
		now := time.Now().UTC()
		oldObj := &Unstructured{Metadata: Metadata{Name: "test"}, Spec: json.RawMessage(`{}`)}
		newObj := &Unstructured{Metadata: Metadata{Name: "test", DeletionTimestamp: &now}, Spec: json.RawMessage(`{}`)}
		paths := changedPaths(oldObj, newObj, SubresourceSpec)
		require.Contains(t, paths, "metadata.deletionTimestamp")
	})
}

func TestChangedJSONPaths(t *testing.T) {
	paths := changedJSONPaths("spec",
		json.RawMessage(`{"size":"small","owner":"a"}`),
		json.RawMessage(`{"size":"large","owner":"a"}`),
	)
	require.Equal(t, []string{"spec.size"}, paths)

	paths = changedJSONPaths("spec",
		json.RawMessage(`{"size":"small"}`),
		json.RawMessage(`{"size":"small"}`),
	)
	require.Nil(t, paths)

	paths = changedJSONPaths("spec",
		json.RawMessage(`[invalid`),
		json.RawMessage(`{"a":1}`),
	)
	require.Equal(t, []string{"spec"}, paths)
}

func TestRawObjectFields(t *testing.T) {
	result, ok := rawObjectFields(json.RawMessage(`{"a":"1","b":"2"}`))
	require.True(t, ok)
	require.Len(t, result, 2)

	result, ok = rawObjectFields(nil)
	require.True(t, ok)
	require.Empty(t, result)

	result, ok = rawObjectFields(json.RawMessage(``))
	require.True(t, ok)
	require.Empty(t, result)

	_, ok = rawObjectFields(json.RawMessage(`[1,2,3]`))
	require.False(t, ok)
}

func TestMustMarshalRaw(t *testing.T) {
	require.Equal(t, `"hello"`, string(mustMarshalRaw("hello")))

	ch := make(chan int)
	require.Nil(t, mustMarshalRaw(ch))
	close(ch)
}

func TestEventMatchesScope(t *testing.T) {
	event := resourceEvent{Ref: objectRef{Resource: "widgets", Namespace: "ns", Name: "test"}}
	require.True(t, eventMatchesScope(event, resourceScope{Resource: "widgets", Namespace: "ns"}))
	require.True(t, eventMatchesScope(event, resourceScope{Resource: "widgets", AllNamespaces: true}))
	require.False(t, eventMatchesScope(event, resourceScope{Resource: "widgets", Namespace: "other"}))
	require.False(t, eventMatchesScope(event, resourceScope{Resource: "other", Namespace: "ns"}))
}

func TestObjectMatchesScope(t *testing.T) {
	obj := Unstructured{Metadata: Metadata{Namespace: "ns", Name: "test"}}
	require.True(t, objectMatchesScope(obj, resourceScope{Namespace: "ns"}))
	require.True(t, objectMatchesScope(obj, resourceScope{AllNamespaces: true}))
	require.False(t, objectMatchesScope(obj, resourceScope{Namespace: "other"}))
	require.False(t, objectMatchesScope(obj, resourceScope{}))
}

func TestRandomToken(t *testing.T) {
	token, err := randomToken("test")
	require.NoError(t, err)
	require.Contains(t, token, "test_")
	require.Greater(t, len(token), 5)
}

func TestMarshalValue(t *testing.T) {
	raw, err := marshalValue("hello")
	require.NoError(t, err)
	require.Equal(t, `"hello"`, string(raw))

	raw, err = marshalValue(nil)
	require.NoError(t, err)
	require.Nil(t, raw)

	raw, err = marshalValue(map[string]any{"a": 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1}`, string(raw))
}

func TestPruneRawWithSchema(t *testing.T) {
	result, err := pruneRawWithSchema(json.RawMessage(`{"size":"small","ghost":"drop"}`), map[string]any{
		"type": "object",
		"properties": map[string]any{
			"size": map[string]any{"type": "string"},
		},
		"additionalProperties": false,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"small"}`, string(result))

	result, err = pruneRawWithSchema(nil, nil)
	require.NoError(t, err)
	require.Nil(t, result)

	result, err = pruneRawWithSchema(json.RawMessage(`null`), nil)
	require.NoError(t, err)
	require.JSONEq(t, "null", string(result))

	result, err = pruneRawWithSchema(json.RawMessage(``), nil)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestPruneValueWithSchemaArray(t *testing.T) {
	schema := map[string]any{
		"type": "array",
		"items": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{"type": "string"},
			},
			"additionalProperties": false,
		},
	}
	result, err := pruneValueWithSchema([]any{
		map[string]any{"name": "a", "ghost": 1},
		map[string]any{"name": "b"},
	}, schema)
	require.NoError(t, err)
	items := result.([]any)
	require.Len(t, items, 2)
	require.Equal(t, map[string]any{"name": "a"}, items[0])
	require.Equal(t, map[string]any{"name": "b"}, items[1])
}

func TestPruneValueWithSchemaPreserveUnknown(t *testing.T) {
	schema := map[string]any{
		"type":                              "object",
		"x-cluster-preserve-unknown-fields": true,
	}
	input := map[string]any{"any": "value", "nested": map[string]any{"deep": 42}}
	result, err := pruneValueWithSchema(input, schema)
	require.NoError(t, err)
	require.Equal(t, input, result)
}

func TestPruneValueWithSchemaAdditionalProperties(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"fixed": map[string]any{"type": "string"},
		},
		"additionalProperties": map[string]any{"type": "integer"},
	}
	result, err := pruneValueWithSchema(map[string]any{
		"fixed":  "hello",
		"extra1": float64(42),
		"extra2": float64(99),
	}, schema)
	require.NoError(t, err)
	out := result.(map[string]any)
	require.Equal(t, "hello", out["fixed"])
	require.InEpsilon(t, float64(42), out["extra1"], 0)
	require.InEpsilon(t, float64(99), out["extra2"], 0)
}
