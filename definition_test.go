package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdmissionRulesEqual(t *testing.T) {
	require.True(t, admissionRulesEqual(nil, nil))
	require.False(t, admissionRulesEqual(nil, []AdmissionRule{{Name: "test"}}))
	require.False(t, admissionRulesEqual([]AdmissionRule{{Name: "test"}}, nil))
	require.True(t, admissionRulesEqual(
		[]AdmissionRule{{Name: "a", Operations: []AdmissionOperation{AdmissionCreate}, Subresources: []Subresource{SubresourceSpec}}},
		[]AdmissionRule{{Name: "a", Operations: []AdmissionOperation{AdmissionCreate}, Subresources: []Subresource{SubresourceSpec}}},
	))
	require.False(t, admissionRulesEqual(
		[]AdmissionRule{{Name: "a"}},
		[]AdmissionRule{{Name: "b"}},
	))
	require.False(t, admissionRulesEqual(
		[]AdmissionRule{{Name: "a", Timeout: time.Second}},
		[]AdmissionRule{{Name: "a", Timeout: time.Minute}},
	))
	require.False(t, admissionRulesEqual(
		[]AdmissionRule{{Name: "a", Operations: []AdmissionOperation{AdmissionCreate}}},
		[]AdmissionRule{{Name: "a", Operations: []AdmissionOperation{AdmissionUpdate}}},
	))
	require.False(t, admissionRulesEqual(
		[]AdmissionRule{{Name: "a", Subresources: []Subresource{SubresourceSpec}}},
		[]AdmissionRule{{Name: "a", Subresources: []Subresource{SubresourceStatus}}},
	))
}

func TestDefinitionEquivalent(t *testing.T) {
	require.True(t, definitionEquivalent(nil, nil))
	require.False(t, definitionEquivalent(nil, &resourceDefinition{}))
	require.False(t, definitionEquivalent(&resourceDefinition{}, nil))

	a := &resourceDefinition{
		Resource:          "test",
		APIVersion:        "v1",
		Kind:              "Test",
		Namespaced:        true,
		SchemaFingerprint: "abc123",
		Admission:         []AdmissionRule{{Name: "check"}},
	}
	b := &resourceDefinition{
		Resource:          "test",
		APIVersion:        "v1",
		Kind:              "Test",
		Namespaced:        true,
		SchemaFingerprint: "abc123",
		Admission:         []AdmissionRule{{Name: "check"}},
	}
	require.True(t, definitionEquivalent(a, b))

	b.Namespaced = false
	require.False(t, definitionEquivalent(a, b))
}

func TestDefinitionValidationErrors(t *testing.T) {
	err := validateDefinition(nil)
	require.ErrorIs(t, err, ErrInvalidResource)

	err = validateDefinition(&resourceDefinition{
		Resource:   "test",
		APIVersion: "/../etc/passwd",
		Kind:       "Test",
		Schema:     json.RawMessage(`{}`),
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	err = validateDefinition(&resourceDefinition{
		Resource:   "test",
		APIVersion: "v1",
		Kind:       "Bad/Kind",
		Schema:     json.RawMessage(`{}`),
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	err = validateDefinition(&resourceDefinition{
		Resource:   "test",
		APIVersion: "v1",
		Kind:       "Test",
		Schema:     json.RawMessage(`{}`),
		Admission: []AdmissionRule{
			{Name: ""},
		},
	})
	require.ErrorIs(t, err, ErrInvalidResource)

	err = validateDefinition(&resourceDefinition{
		Resource:   "test",
		APIVersion: "v1",
		Kind:       "Test",
		Schema:     json.RawMessage(`{}`),
	})
	require.NoError(t, err)
}

func TestCompileAdmissionRules(t *testing.T) {
	rules := []AdmissionRule{
		{Name: "check", Operations: []AdmissionOperation{AdmissionCreate}},
	}
	compiled := compileAdmissionRules(rules)
	require.Len(t, compiled, 1)
	key := admissionRuleKey(AdmissionCreate, SubresourceSpec)
	require.Len(t, compiled[key], 1)
	require.Equal(t, "check", compiled[key][0].Name)

	require.Nil(t, compileAdmissionRules(nil))
}

func TestAdmissionRuleKey(t *testing.T) {
	key := admissionRuleKey(AdmissionCreate, SubresourceSpec)
	require.Equal(t, "CREATE|spec", key)

	key = admissionRuleKey(AdmissionUpdate, SubresourceStatus)
	require.Equal(t, "UPDATE|status", key)
}

func TestAdmissionRulesLookup(t *testing.T) {
	def := &resourceDefinition{
		admissionMatches: map[string][]AdmissionRule{
			"CREATE|spec": {{Name: "create-check"}},
			"UPDATE|spec": {{Name: "update-check"}},
		},
	}
	rules := def.admissionRules(AdmissionCreate, SubresourceSpec)
	require.Len(t, rules, 1)
	require.Equal(t, "create-check", rules[0].Name)

	rules = def.admissionRules(AdmissionDelete, SubresourceSpec)
	require.Nil(t, rules)

	require.Nil(t, (*resourceDefinition)(nil).admissionRules(AdmissionCreate, SubresourceSpec))
}

func TestValidateNodeDefault(t *testing.T) {
	require.NoError(t, validateNodeDefault(map[string]any{"type": "string", "default": "hello"}))
	require.NoError(t, validateNodeDefault(map[string]any{"type": "boolean", "default": true}))
	require.NoError(t, validateNodeDefault(map[string]any{"type": "integer", "default": float64(42)}))
	require.NoError(t, validateNodeDefault(map[string]any{"type": "number", "default": float64(3.14)}))
	require.NoError(t, validateNodeDefault(map[string]any{"type": "string"}))
	require.Error(t, validateNodeDefault(map[string]any{"type": "string", "default": 42}))
	require.Error(t, validateNodeDefault(map[string]any{"type": "boolean", "default": "true"}))
	require.Error(t, validateNodeDefault(map[string]any{"type": "integer", "default": "not-a-number"}))
}

func TestBoolValue(t *testing.T) {
	require.True(t, boolValue(true))
	require.False(t, boolValue(false))
	require.False(t, boolValue(nil))
	require.False(t, boolValue("string"))
	require.False(t, boolValue(42))
}

func TestIsEmptyObjectSchema(t *testing.T) {
	require.True(t, isEmptyObjectSchema(map[string]any{}))
	require.False(t, isEmptyObjectSchema(map[string]any{"properties": map[string]any{"a": "b"}}))
}

func TestCloneAdmissionRules(t *testing.T) {
	require.Nil(t, cloneAdmissionRules(nil))
	rules := []AdmissionRule{
		{Name: "check", Operations: []AdmissionOperation{AdmissionCreate}},
	}
	copied := cloneAdmissionRules(rules)
	require.Len(t, copied, 1)
	copied[0].Name = "changed"
	require.Equal(t, "check", rules[0].Name)
}

func TestCollectSchemaIndexes(t *testing.T) {
	indexes := make([]IndexInfo, 0)
	node := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"size": map[string]any{"type": "string", "x-cluster-index": true},
			"nested": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"deep": map[string]any{"type": "string", "x-cluster-index": true},
				},
			},
		},
	}
	collectSchemaIndexes("spec", node, &indexes)
	require.Len(t, indexes, 2)
	paths := []string{indexes[0].Path, indexes[1].Path}
	require.ElementsMatch(t, []string{"spec.size", "spec.nested.deep"}, paths)
}

func TestTypeOf(t *testing.T) {
	typ := typeOf[string]()
	require.Equal(t, "string", typ.Kind().String())

	type myStruct struct{}
	typ = typeOf[myStruct]()
	require.Equal(t, "struct", typ.Kind().String())

	typ = typeOf[*myStruct]()
	require.Equal(t, "struct", typ.Kind().String())
}

func TestSchemaForValueType(t *testing.T) {
	schema, err := schemaForValueType(typeOf[*struct{ Name string }](), "spec.field")
	require.NoError(t, err)
	require.NotNil(t, schema)

	schema, err = schemaForValueType(typeOf[map[string]int](), "spec.map")
	require.NoError(t, err)
	require.Equal(t, "object", schema["type"])

	_, err = schemaForValueType(typeOf[map[int]string](), "spec.badmap")
	require.ErrorIs(t, err, ErrInvalidResource)

	schema, err = schemaForValueType(typeOf[[]string](), "spec.list")
	require.NoError(t, err)
	require.Equal(t, "array", schema["type"])

	schema, err = schemaForValueType(typeOf[[3]string](), "spec.arr")
	require.NoError(t, err)
	require.Equal(t, "array", schema["type"])
}

func TestCanonicalJSON(t *testing.T) {
	raw, err := canonicalJSON(map[string]any{"a": 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"a":1}`, string(raw))
}

func TestSchemaFingerprint(t *testing.T) {
	fp1 := schemaFingerprint(json.RawMessage(`{"type":"object"}`))
	fp2 := schemaFingerprint(json.RawMessage(`{"type":"object"}`))
	require.Equal(t, fp1, fp2)
	require.Len(t, fp1, 64)
}

func TestCollectMetadataWritable(t *testing.T) {
	out := make(map[string]struct{})
	collectMetadataWritable(map[string]any{
		"properties": map[string]any{
			"labels": map[string]any{
				"type":                        "object",
				"additionalProperties":        map[string]any{"type": "string"},
				"x-cluster-metadata-writable": true,
			},
		},
	}, out)
	require.Contains(t, out, "labels")
}

func TestParseSchemaClusterTag(t *testing.T) {
	tag, err := parseSchemaClusterTag("required,enum=small|medium|large,default=medium,index")
	require.NoError(t, err)
	require.True(t, tag.index)
	require.Equal(t, []string{"small", "medium", "large"}, tag.enum)
	require.Equal(t, "medium", tag.defaultValue)

	tag, err = parseSchemaClusterTag("immutable")
	require.NoError(t, err)
	require.True(t, tag.immutable)

	tag, err = parseSchemaClusterTag("preserveUnknown")
	require.NoError(t, err)
	require.True(t, tag.preserveUnknown)

	_, err = parseSchemaClusterTag("badtag")
	require.Error(t, err)

	_, err = parseSchemaClusterTag("enum=")
	require.Error(t, err)

	_, err = parseSchemaClusterTag("default")
	require.Error(t, err)
}

func TestJSONFieldName(t *testing.T) {
	type testStruct struct {
		Normal string `json:"normal"`
		Omit   string `json:"omit,omitempty"`
		Skip   string `json:"-"`
		NoTag  string
	}

	typ := typeOf[testStruct]()
	name, required := jsonFieldName(typ.Field(0))
	require.Equal(t, "normal", name)
	require.True(t, required)

	name, required = jsonFieldName(typ.Field(1))
	require.Equal(t, "omit", name)
	require.False(t, required)

	name, required = jsonFieldName(typ.Field(2))
	require.Equal(t, "-", name)
	require.False(t, required)

	name, _ = jsonFieldName(typ.Field(3))
	require.Equal(t, "NoTag", name)
}

func TestCompileSchema(t *testing.T) {
	_, err := compileSchema(nil)
	require.ErrorIs(t, err, ErrInvalidResource)

	_, err = compileSchema(json.RawMessage(`{"type":"string"}`))
	require.ErrorIs(t, err, ErrInvalidResource)

	compiled, err := compileSchema(json.RawMessage(`{
		"type":"object",
		"properties":{
			"apiVersion":{"type":"string"},
			"kind":{"type":"string"},
			"metadata":{"type":"object","properties":{"name":{"type":"string"}},"additionalProperties":false},
			"spec":{"type":"object","properties":{},"additionalProperties":false}
		}
	}`))
	require.NoError(t, err)
	require.NotEmpty(t, compiled.Raw)
	require.Empty(t, compiled.Indexes)
	require.Empty(t, compiled.Defaults)
}
