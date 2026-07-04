package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyMergePatch(t *testing.T) {
	t.Run("overwrite_field", func(t *testing.T) {
		result, err := applyMergePatch(
			json.RawMessage(`{"a":"old","b":"keep"}`),
			json.RawMessage(`{"a":"new"}`),
		)
		require.NoError(t, err)
		require.JSONEq(t, `{"a":"new","b":"keep"}`, string(result))
	})

	t.Run("add_field", func(t *testing.T) {
		result, err := applyMergePatch(
			json.RawMessage(`{"a":"1"}`),
			json.RawMessage(`{"b":"2"}`),
		)
		require.NoError(t, err)
		require.JSONEq(t, `{"a":"1","b":"2"}`, string(result))
	})

	t.Run("delete_field", func(t *testing.T) {
		result, err := applyMergePatch(
			json.RawMessage(`{"a":"1","b":"2"}`),
			json.RawMessage(`{"a":null}`),
		)
		require.NoError(t, err)
		require.JSONEq(t, `{"b":"2"}`, string(result))
	})

	t.Run("nested_merge", func(t *testing.T) {
		result, err := applyMergePatch(
			json.RawMessage(`{"meta":{"a":"1","b":"2"}}`),
			json.RawMessage(`{"meta":{"a":"new"}}`),
		)
		require.NoError(t, err)
		require.JSONEq(t, `{"meta":{"a":"new","b":"2"}}`, string(result))
	})

	t.Run("invalid_patch", func(t *testing.T) {
		_, err := applyMergePatch(json.RawMessage(`{}`), json.RawMessage(`{invalid`))
		require.Error(t, err)
	})
}

func TestMergePatchValue(t *testing.T) {
	t.Run("non_object_patch", func(t *testing.T) {
		result := mergePatchValue("old", "new")
		require.Equal(t, "new", result)
	})

	t.Run("target_not_map", func(t *testing.T) {
		result := mergePatchValue("string", map[string]any{"a": 1})
		require.Equal(t, map[string]any{"a": 1}, result)
	})
}

func TestApplyRawMergePatch(t *testing.T) {
	result, err := applyRawMergePatch(nil, json.RawMessage(`{"a":"1"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"a":"1"}`, string(result))
}

func TestApplyObjectPatch(t *testing.T) {
	result, err := applyObjectPatch(
		Unstructured{
			Metadata: Metadata{Name: "test"},
			Spec:     json.RawMessage(`{"size":"small"}`),
		},
		json.RawMessage(`{"spec":{"size":"large"}}`),
	)
	require.NoError(t, err)
	require.JSONEq(t, `{"size":"large"}`, string(result.Spec))
}
