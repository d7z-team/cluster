package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhere(t *testing.T) {
	sel := Where(Label("app").Eq("demo"), Field("spec.size").Eq("small"))
	require.Len(t, sel.requirements, 2)
	require.Equal(t, Requirement{
		target: selectLabel,
		key:    "app",
		op:     selectorEquals,
		values: []string{"demo"},
	}, sel.requirements[0])
	require.Equal(t, Requirement{
		target: selectField,
		key:    "spec.size",
		op:     selectorEquals,
		values: []string{"small"},
	}, sel.requirements[1])
}

func TestLabel(t *testing.T) {
	term := Label("app")
	require.Equal(t, selectLabel, term.target)
	require.Equal(t, "app", term.key)
}

func TestAnnotation(t *testing.T) {
	term := Annotation("tenant")
	require.Equal(t, selectAnnotation, term.target)
}

func TestField(t *testing.T) {
	term := Field("spec.size")
	require.Equal(t, selectField, term.target)
}

func TestSelectorRequirements(t *testing.T) {
	req := Label("app").Eq("demo")
	require.Equal(t, selectorEquals, req.op)
	require.Equal(t, []string{"demo"}, req.values)

	req = Label("app").Exists()
	require.Equal(t, selectorExists, req.op)

	req = Label("app").NotEq("other")
	require.Equal(t, selectorNotEquals, req.op)
	require.Equal(t, []string{"other"}, req.values)

	req = Label("app").In("a", "b")
	require.Equal(t, selectorIn, req.op)
	require.Equal(t, []string{"a", "b"}, req.values)

	req = Field("spec.size").NotIn("xlarge")
	require.Equal(t, selectorNotIn, req.op)
}

func TestRequirementMatches(t *testing.T) {
	require.True(t, requirementMatches("demo", true, Requirement{op: selectorEquals, values: []string{"demo"}}))
	require.False(t, requirementMatches("demo", false, Requirement{op: selectorEquals, values: []string{"demo"}}))
	require.False(t, requirementMatches("other", true, Requirement{op: selectorEquals, values: []string{"demo"}}))

	require.True(t, requirementMatches("demo", true, Requirement{op: selectorExists}))
	require.False(t, requirementMatches("", false, Requirement{op: selectorExists}))

	require.True(t, requirementMatches("demo", true, Requirement{op: selectorNotEquals, values: []string{"other"}}))
	require.True(t, requirementMatches("", false, Requirement{op: selectorNotEquals, values: []string{"other"}}))
	require.False(t, requirementMatches("demo", true, Requirement{op: selectorNotEquals, values: []string{"demo"}}))

	require.True(t, requirementMatches("demo", true, Requirement{op: selectorIn, values: []string{"a", "demo", "b"}}))
	require.False(t, requirementMatches("demo", false, Requirement{op: selectorIn, values: []string{"demo"}}))
	require.False(t, requirementMatches("other", true, Requirement{op: selectorIn, values: []string{"a", "b"}}))

	require.True(t, requirementMatches("", false, Requirement{op: selectorNotIn, values: []string{"demo"}}))
	require.True(t, requirementMatches("other", true, Requirement{op: selectorNotIn, values: []string{"demo"}}))
	require.False(t, requirementMatches("demo", true, Requirement{op: selectorNotIn, values: []string{"demo"}}))
}

func TestSelectorValue(t *testing.T) {
	obj := Unstructured{
		Metadata: Metadata{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		},
		Spec: json.RawMessage(`{"size":"small"}`),
	}

	val, ok := selectorValue(obj, Requirement{target: selectLabel, key: "app"})
	require.True(t, ok)
	require.Equal(t, "demo", val)

	_, ok = selectorValue(obj, Requirement{target: selectLabel, key: "missing"})
	require.False(t, ok)

	val, ok = selectorValue(obj, Requirement{target: selectAnnotation, key: "tenant"})
	require.True(t, ok)
	require.Equal(t, "t1", val)
}

func TestMatchesSelector(t *testing.T) {
	obj := Unstructured{
		Metadata: Metadata{
			Labels:      Labels{"app": "demo"},
			Annotations: Annotations{"tenant": "t1"},
		},
		Spec: json.RawMessage(`{"size":"small"}`),
	}

	require.True(t, matchesSelector(obj, Where(Label("app").Eq("demo"))))
	require.False(t, matchesSelector(obj, Where(Label("app").Eq("other"))))
	require.True(t, matchesSelector(obj, Where(
		Label("app").Eq("demo"),
		Annotation("tenant").Eq("t1"),
	)))
	require.False(t, matchesSelector(obj, Where(
		Label("app").Eq("demo"),
		Annotation("tenant").Eq("t2"),
	)))
}

func TestValidateSelector(t *testing.T) {
	def := &resourceDefinition{
		Indexes:    []IndexInfo{{Path: "spec.size"}},
		Namespaced: true,
	}

	require.NoError(t, validateSelector(def, Where(Field("spec.size").Eq("small"))))
	require.NoError(t, validateSelector(def, Where(Field("metadata.name").Eq("alpha"))))
	require.NoError(t, validateSelector(def, Where(Field("metadata.namespace").Eq("ns"))))
	require.NoError(t, validateSelector(def, Where(Label("app").Eq("demo"))))
	require.Error(t, validateSelector(def, Where(Field("kind").Eq("Widget"))))
	require.Error(t, validateSelector(def, Where(Field("").Eq("x"))))
}

func TestAllowsFieldSelector(t *testing.T) {
	def := &resourceDefinition{
		Namespaced: true,
		Indexes:    []IndexInfo{{Path: "spec.size"}},
	}
	require.True(t, allowsFieldSelector(nil, "metadata.name"))
	require.False(t, allowsFieldSelector(nil, "spec.size"))
	require.True(t, allowsFieldSelector(def, "metadata.name"))
	require.True(t, allowsFieldSelector(def, "metadata.namespace"))
	require.True(t, allowsFieldSelector(def, "spec.size"))
	require.False(t, allowsFieldSelector(def, "spec.owner"))

	nonNs := &resourceDefinition{Namespaced: false}
	require.False(t, allowsFieldSelector(nonNs, "metadata.namespace"))
}

func TestFieldStringValue(t *testing.T) {
	obj := &Unstructured{
		Metadata: Metadata{Name: "test"},
		Spec:     json.RawMessage(`{"size":"small"}`),
	}

	val, ok := fieldStringValue(obj, "spec.size")
	require.True(t, ok)
	require.Equal(t, "small", val)

	val, ok = fieldStringValue(obj, "metadata.name")
	require.True(t, ok)
	require.Equal(t, "test", val)

	_, ok = fieldStringValue(obj, "spec.missing")
	require.False(t, ok)

	_, ok = fieldStringValue(obj, "")
	require.False(t, ok)
}
