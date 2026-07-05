package cluster

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

func objectCursor(obj Unstructured) string {
	return obj.Metadata.Namespace + "\x00" + obj.Metadata.Name + "\x00" + obj.Metadata.UID
}

func objectStorageKey(ref objectRef) string {
	if ref.Namespace == "" {
		return ref.Name
	}
	return ref.Namespace + "\x00" + ref.Name
}

func objectMatchesScope(obj Unstructured, scope resourceScope) bool {
	if scope.Namespace != "" {
		return obj.Metadata.Namespace == scope.Namespace
	}
	if scope.AllNamespaces {
		return obj.Metadata.Namespace != ""
	}
	return obj.Metadata.Namespace == ""
}

func eventMatchesScope(event resourceEvent, scope resourceScope) bool {
	if scope.Resource != "" && event.Ref.Resource != scope.Resource {
		return false
	}
	if scope.Namespace != "" {
		return event.Ref.Namespace == scope.Namespace
	}
	if scope.AllNamespaces {
		return event.Ref.Namespace != ""
	}
	return event.Ref.Namespace == ""
}

func randomToken(prefix string) (string, error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return prefix + "_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

func cloneUnstructuredPtr(obj *Unstructured) *Unstructured {
	if obj == nil {
		return nil
	}
	copied := cloneUnstructured(*obj)
	return &copied
}

func cloneUnstructured(obj Unstructured) Unstructured {
	return Unstructured{
		APIVersion: obj.APIVersion,
		Kind:       obj.Kind,
		Metadata:   cloneMetadata(obj.Metadata),
		Spec:       cloneRaw(obj.Spec),
		Status:     cloneRaw(obj.Status),
	}
}

func cloneMetadata(meta Metadata) Metadata {
	copied := meta
	copied.DeletionTimestamp = cloneTimePtr(meta.DeletionTimestamp)
	copied.DeletionGracePeriodSeconds = cloneInt64Ptr(meta.DeletionGracePeriodSeconds)
	copied.Labels = cloneLabels(meta.Labels)
	copied.Annotations = cloneAnnotations(meta.Annotations)
	copied.Finalizers = append([]string(nil), meta.Finalizers...)
	copied.OwnerReferences = append([]OwnerReference(nil), meta.OwnerReferences...)
	return copied
}

func cloneLabels(labels Labels) Labels {
	return cloneStringMap(labels)
}

func cloneAnnotations(annotations Annotations) Annotations {
	return cloneStringMap(annotations)
}

func cloneStringMap[M ~map[string]string](m M) M {
	if m == nil {
		return nil
	}
	copied := make(M, len(m))
	for key, value := range m {
		copied[key] = value
	}
	return copied
}

func fieldRawValue(obj *Unstructured, path string) (json.RawMessage, bool) {
	switch path {
	case "metadata.namespace":
		return mustMarshalRaw(obj.Metadata.Namespace), true
	case "metadata.name":
		return mustMarshalRaw(obj.Metadata.Name), true
	case "metadata.uid":
		return mustMarshalRaw(obj.Metadata.UID), true
	case "metadata.generation":
		return mustMarshalRaw(obj.Metadata.Generation), true
	}
	if strings.HasPrefix(path, "metadata.") {
		raw, err := json.Marshal(obj.Metadata)
		if err != nil {
			return nil, false
		}
		fields, ok := rawObjectFields(raw)
		if !ok {
			return nil, false
		}
		current, ok := fields[strings.TrimPrefix(path, "metadata.")]
		if ok {
			return current, true
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, false
		}
		currentValue := value
		for _, segment := range strings.Split(strings.TrimPrefix(path, "metadata."), ".") {
			object, ok := currentValue.(map[string]any)
			if !ok {
				return nil, false
			}
			currentValue, ok = object[segment]
			if !ok {
				return nil, false
			}
		}
		out, err := json.Marshal(currentValue)
		if err != nil {
			return nil, false
		}
		return out, true
	}

	prefix, field, ok := strings.Cut(path, ".")
	if !ok || field == "" {
		return nil, false
	}
	var raw json.RawMessage
	switch prefix {
	case "spec":
		raw = obj.Spec
	case "status":
		raw = obj.Status
	default:
		return nil, false
	}
	if len(raw) == 0 {
		return nil, false
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, false
	}
	current := value
	for _, segment := range strings.Split(field, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	out, err := json.Marshal(current)
	if err != nil {
		return nil, false
	}
	return out, true
}

func mustMarshalRaw(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	return raw
}

func isEmptyJSONValue(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}
	return value == nil
}

func rawScalarString(raw json.RawMessage) (string, bool) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", false
	}
	switch typed := value.(type) {
	case string:
		return typed, true
	case bool:
		if typed {
			return "true", true
		}
		return "false", true
	case float64:
		return fmt.Sprint(typed), true
	default:
		return "", false
	}
}

func applyDefaultRules(obj *Unstructured, rules []defaultRule) error {
	for _, rule := range rules {
		if err := applyDefaultRule(obj, rule); err != nil {
			return err
		}
	}
	return nil
}

func applyDefaultRule(obj *Unstructured, rule defaultRule) error {
	root, remainder, ok := strings.Cut(rule.Path, ".")
	if !ok || remainder == "" {
		return nil
	}
	var current json.RawMessage
	switch root {
	case "spec":
		current = obj.Spec
	case "status":
		current = obj.Status
	default:
		return nil
	}
	value := map[string]any{}
	if len(current) > 0 && string(current) != "null" {
		if err := json.Unmarshal(current, &value); err != nil {
			return err
		}
	}
	if applyDefaultMap(value, strings.Split(remainder, "."), rule.Value) {
		updated, err := json.Marshal(value)
		if err != nil {
			return err
		}
		switch root {
		case "spec":
			obj.Spec = updated
		case "status":
			obj.Status = updated
		}
	}
	return nil
}

func pruneRawWithSchema(raw json.RawMessage, schema map[string]any) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(raw) == "null" || schema == nil {
		return cloneRaw(raw), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	pruned, err := pruneValueWithSchema(value, schema)
	if err != nil {
		return nil, err
	}
	if pruned == nil {
		return nil, nil
	}
	out, err := json.Marshal(pruned)
	if err != nil {
		return nil, err
	}
	if string(out) == "null" {
		return nil, nil
	}
	return out, nil
}

func pruneValueWithSchema(value any, schema map[string]any) (any, error) {
	if boolValue(schema["x-cluster-preserve-unknown-fields"]) {
		return value, nil
	}
	switch schema["type"] {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return value, nil
		}
		result := make(map[string]any, len(object))
		properties, _ := schema["properties"].(map[string]any)
		for key, childRaw := range properties {
			childSchema, ok := childRaw.(map[string]any)
			if !ok {
				continue
			}
			childValue, exists := object[key]
			if !exists {
				continue
			}
			pruned, err := pruneValueWithSchema(childValue, childSchema)
			if err != nil {
				return nil, err
			}
			result[key] = pruned
		}
		if additional, ok := schema["additionalProperties"].(map[string]any); ok {
			for key, childValue := range object {
				if _, exists := properties[key]; exists {
					continue
				}
				pruned, err := pruneValueWithSchema(childValue, additional)
				if err != nil {
					return nil, err
				}
				result[key] = pruned
			}
		}
		return result, nil
	case "array":
		itemsSchema, _ := schema["items"].(map[string]any)
		items, ok := value.([]any)
		if !ok || itemsSchema == nil {
			return value, nil
		}
		result := make([]any, 0, len(items))
		for _, item := range items {
			pruned, err := pruneValueWithSchema(item, itemsSchema)
			if err != nil {
				return nil, err
			}
			result = append(result, pruned)
		}
		return result, nil
	default:
		return value, nil
	}
}

func applyDefaultMap(root map[string]any, segments []string, raw json.RawMessage) bool {
	if len(segments) == 0 {
		return false
	}
	if len(segments) == 1 {
		if _, exists := root[segments[0]]; exists {
			return false
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return false
		}
		root[segments[0]] = value
		return true
	}
	next, ok := root[segments[0]].(map[string]any)
	if !ok {
		next = map[string]any{}
		root[segments[0]] = next
	}
	return applyDefaultMap(next, segments[1:], raw)
}

func cloneRaw(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneInt64Ptr(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func gracePeriodActive(meta Metadata, now time.Time) bool {
	if meta.DeletionTimestamp == nil || meta.DeletionGracePeriodSeconds == nil || *meta.DeletionGracePeriodSeconds <= 0 {
		return false
	}
	deadline := meta.DeletionTimestamp.Add(time.Duration(*meta.DeletionGracePeriodSeconds) * time.Second)
	return deadline.After(now)
}

func hasNewFinalizers(oldFinalizers, newFinalizers []string) bool {
	oldSet := make(map[string]struct{}, len(oldFinalizers))
	for _, value := range oldFinalizers {
		oldSet[value] = struct{}{}
	}
	for _, value := range newFinalizers {
		if _, ok := oldSet[value]; !ok {
			return true
		}
	}
	return false
}

func filterExistingFinalizers(oldFinalizers, requested []string) []string {
	requestedSet := make(map[string]struct{}, len(requested))
	for _, value := range requested {
		requestedSet[value] = struct{}{}
	}
	out := make([]string, 0, len(requested))
	for _, value := range oldFinalizers {
		if _, ok := requestedSet[value]; ok {
			out = append(out, value)
		}
	}
	return out
}

func filterOwnerReferences(refs []OwnerReference, uid string) []OwnerReference {
	if len(refs) == 0 {
		return nil
	}
	out := refs[:0]
	for _, ref := range refs {
		if ref.UID != uid {
			out = append(out, ref)
		}
	}
	return append([]OwnerReference(nil), out...)
}

func terminalAdmissionTimestamp(obj Unstructured) time.Time {
	spec, status, err := decodeAdmissionRequest(obj)
	if err != nil {
		return obj.Metadata.CreationTimestamp
	}
	if status.Phase == AdmissionPendingPhase {
		return spec.ExpiresAt
	}
	if !status.DecidedAt.IsZero() {
		return status.DecidedAt
	}
	return obj.Metadata.CreationTimestamp
}

func ensureMetadataMaps(meta *Metadata) {
	if meta.Labels == nil {
		meta.Labels = Labels{}
	}
	if meta.Annotations == nil {
		meta.Annotations = Annotations{}
	}
}

func jsonEqual(a, b json.RawMessage) bool {
	if len(bytes.TrimSpace(a)) == 0 {
		a = nil
	}
	if len(bytes.TrimSpace(b)) == 0 {
		b = nil
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	var left any
	var right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return bytes.Equal(a, b)
	}
	return reflect.DeepEqual(left, right)
}

func changedPaths(oldObj, newObj *Unstructured, subresource Subresource) []string {
	changed := make([]string, 0)
	if oldObj == nil {
		if newObj == nil {
			return nil
		}
		changed = append(changed, "metadata")
		if len(newObj.Spec) > 0 {
			changed = append(changed, changedJSONPaths("spec", nil, newObj.Spec)...)
		}
		if len(newObj.Status) > 0 {
			changed = append(changed, changedJSONPaths("status", nil, newObj.Status)...)
		}
		return sortedUnique(changed)
	}
	if newObj == nil {
		return nil
	}
	if subresource == SubresourceStatus {
		return sortedUnique(changedJSONPaths("status", oldObj.Status, newObj.Status))
	}
	if subresource == SubresourceScale {
		return sortedUnique(changedJSONPaths("spec", oldObj.Spec, newObj.Spec))
	}
	if !reflect.DeepEqual(oldObj.Metadata.Labels, newObj.Metadata.Labels) {
		changed = append(changed, "metadata.labels")
	}
	if !reflect.DeepEqual(oldObj.Metadata.Annotations, newObj.Metadata.Annotations) {
		changed = append(changed, "metadata.annotations")
	}
	if !reflect.DeepEqual(oldObj.Metadata.Finalizers, newObj.Metadata.Finalizers) {
		changed = append(changed, "metadata.finalizers")
	}
	if oldObj.Metadata.Namespace != newObj.Metadata.Namespace {
		changed = append(changed, "metadata.namespace")
	}
	if !reflect.DeepEqual(oldObj.Metadata.OwnerReferences, newObj.Metadata.OwnerReferences) {
		changed = append(changed, "metadata.ownerReferences")
	}
	if (oldObj.Metadata.DeletionTimestamp == nil) != (newObj.Metadata.DeletionTimestamp == nil) ||
		oldObj.Metadata.DeletionTimestamp != nil && !oldObj.Metadata.DeletionTimestamp.Equal(*newObj.Metadata.DeletionTimestamp) {
		changed = append(changed, "metadata.deletionTimestamp")
	}
	if !reflect.DeepEqual(oldObj.Metadata.DeletionGracePeriodSeconds, newObj.Metadata.DeletionGracePeriodSeconds) {
		changed = append(changed, "metadata.deletionGracePeriodSeconds")
	}
	changed = append(changed, changedJSONPaths("spec", oldObj.Spec, newObj.Spec)...)
	return sortedUnique(changed)
}

func changedJSONPaths(prefix string, oldRaw, newRaw json.RawMessage) []string {
	if jsonEqual(oldRaw, newRaw) {
		return nil
	}
	oldValues, oldOK := rawObjectFields(oldRaw)
	newValues, newOK := rawObjectFields(newRaw)
	if !oldOK || !newOK {
		return []string{prefix}
	}
	keys := make(map[string]struct{}, len(oldValues)+len(newValues))
	for key := range oldValues {
		keys[key] = struct{}{}
	}
	for key := range newValues {
		keys[key] = struct{}{}
	}
	changed := make([]string, 0, len(keys))
	for key := range keys {
		if !jsonEqual(oldValues[key], newValues[key]) {
			changed = append(changed, prefix+"."+key)
		}
	}
	return changed
}

func rawObjectFields(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]json.RawMessage{}, true
	}
	values := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, false
	}
	for key, value := range values {
		values[key] = cloneRaw(value)
	}
	return values, true
}

func setRawFieldPath(raw json.RawMessage, path string, value any) (json.RawMessage, error) {
	root := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &root); err != nil {
			return nil, err
		}
	}
	current := root
	segments := strings.Split(path, ".")
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[segments[len(segments)-1]] = value
	out, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func int32FieldValue(obj *Unstructured, path string) int32 {
	raw, ok := fieldRawValue(obj, path)
	if !ok {
		return 0
	}
	var value int32
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0
	}
	return value
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	slices := append([]string(nil), values...)
	sort.Strings(slices)
	out := slices[:0]
	for _, value := range slices {
		if value == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out
}

func cloneEvent(event resourceEvent) resourceEvent {
	return resourceEvent{
		Type:            event.Type,
		ResourceVersion: event.ResourceVersion,
		Ref:             event.Ref,
		Object:          cloneUnstructuredPtr(event.Object),
		OldObject:       cloneUnstructuredPtr(event.OldObject),
		Annotations:     cloneAnnotations(event.Annotations),
		Changed:         append([]string(nil), event.Changed...),
	}
}

func newStoreEvent(req commitRequest, oldObj, newObj *Unstructured) resourceEvent {
	return resourceEvent{
		Type:            req.EventType,
		ResourceVersion: newObj.Metadata.ResourceVersion,
		Ref:             req.Ref,
		Object:          cloneUnstructuredPtr(newObj),
		OldObject:       cloneUnstructuredPtr(oldObj),
		Annotations:     cloneAnnotations(req.EventAnnotations),
		Changed:         append([]string(nil), req.Changed...),
	}
}

func normalizeStorePrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return ""
	}
	return prefix + "/"
}
