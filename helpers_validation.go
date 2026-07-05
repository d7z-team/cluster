package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func validateResourceName(name string) error {
	if invalidPathToken(name) {
		return fmt.Errorf("%w: invalid resource", ErrInvalidResource)
	}
	return nil
}

func validateObjectName(name string) error {
	if invalidPathToken(name) {
		return fmt.Errorf("%w: invalid name", ErrInvalidObject)
	}
	return nil
}

func validateNamespace(namespace string) error {
	if invalidPathToken(namespace) {
		return fmt.Errorf("%w: invalid namespace", ErrInvalidObject)
	}
	return nil
}

func invalidPathToken(value string) bool {
	value = strings.TrimSpace(value)
	return value == "" || value == "." || value == ".." || strings.ContainsAny(value, `/\`)
}

func validateMetadataWithSchema(meta Metadata) error {
	if meta.Namespace != "" {
		if err := validateNamespace(meta.Namespace); err != nil {
			return err
		}
	}
	if meta.Name != "" {
		if err := validateObjectName(meta.Name); err != nil {
			return err
		}
	}
	if err := validateMetadataKeys(meta.Labels); err != nil {
		return err
	}
	if err := validateMetadataKeys(meta.Annotations); err != nil {
		return err
	}
	controllerOwners := 0
	for _, owner := range meta.OwnerReferences {
		if err := validateObjectName(owner.Name); err != nil {
			return err
		}
		if owner.Namespace != "" {
			if err := validateNamespace(owner.Namespace); err != nil {
				return err
			}
		}
		if invalidPathToken(owner.Resource) || strings.TrimSpace(owner.UID) == "" {
			return fmt.Errorf("%w: invalid owner reference", ErrInvalidObject)
		}
		if owner.Controller {
			controllerOwners++
			if controllerOwners > 1 {
				return fmt.Errorf("%w: only one controller owner reference is allowed", ErrInvalidObject)
			}
		}
	}
	return nil
}

func validateOwnerReferencesForDefinition(def *resourceDefinition, meta Metadata) error {
	if err := validateMetadataWithSchema(meta); err != nil {
		return err
	}
	for _, owner := range meta.OwnerReferences {
		switch {
		case def == nil:
			continue
		case def.Namespaced:
			if owner.Namespace != meta.Namespace {
				return fmt.Errorf("%w: ownerReferences must stay in the same namespace", ErrInvalidObject)
			}
		default:
			if owner.Namespace != "" {
				return fmt.Errorf("%w: cluster-scoped resources cannot reference namespaced owners", ErrInvalidObject)
			}
		}
	}
	return nil
}

func validateMetadataKeys(values map[string]string) error {
	for key := range values {
		if strings.TrimSpace(key) == "" || strings.Contains(key, "\x00") {
			return fmt.Errorf("%w: invalid metadata key", ErrInvalidObject)
		}
	}
	return nil
}

func validateSpecPatch(patch []byte, writable map[string]struct{}) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(patch, &root); err != nil {
		return err
	}
	if _, ok := root["status"]; ok {
		return fmt.Errorf("%w: status must be patched through status subresource", ErrInvalidObject)
	}
	if raw, ok := root["metadata"]; ok {
		if err := validateMetadataPatchWithSchema(raw, writable); err != nil {
			return err
		}
	}
	return nil
}

func validateMetadataPatchWithSchema(patch []byte, writable map[string]struct{}) error {
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(patch, &meta); err != nil {
		return err
	}
	if len(meta) == 0 {
		return ErrInvalidObject
	}
	for key := range meta {
		if _, ok := writable[key]; !ok {
			return fmt.Errorf("%w: metadata.%s is managed", ErrInvalidObject, key)
		}
	}
	return nil
}

func validateRawObjectJSON(obj *Unstructured) error {
	if err := validateRawJSONField("spec", obj.Spec); err != nil {
		return err
	}
	return validateRawJSONField("status", obj.Status)
}

func validateRawJSONField(name string, raw json.RawMessage) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("%w: invalid %s JSON: %v", ErrInvalidObject, name, err)
	}
	return nil
}

func updateRV(primary, fallback string) (uint64, error) {
	if primary == "" {
		primary = fallback
	}
	return parseRequiredRV(primary)
}

func parseRequiredRV(value string) (uint64, error) {
	rv, err := parseOptionalRV(value)
	if err != nil {
		return 0, err
	}
	if rv == 0 {
		return 0, ErrConflict
	}
	return rv, nil
}

func parseOptionalRV(value string) (uint64, error) {
	if value == "" {
		return 0, nil
	}
	rv, err := strconv.ParseUint(value, 10, 64)
	if err != nil || rv == 0 {
		return 0, fmt.Errorf("%w: invalid resourceVersion", ErrInvalidObject)
	}
	return rv, nil
}

func parseStoredRV(value string) uint64 {
	rv, _ := strconv.ParseUint(value, 10, 64)
	return rv
}

func validateResourceVersionMatch(currentRV, requestedRV uint64, match ResourceVersionMatch) error {
	switch match {
	case ResourceVersionAny, ResourceVersionNotOlderThan:
		if requestedRV > currentRV {
			return ErrConflict
		}
		return nil
	case ResourceVersionExact:
		if requestedRV == 0 || requestedRV != currentRV {
			return ErrResourceVersionTooOld
		}
		return nil
	default:
		return ErrInvalidObject
	}
}

func formatRV(rv uint64) string {
	if rv == 0 {
		return ""
	}
	return strconv.FormatUint(rv, 10)
}

func rvKey(rv uint64) string {
	return fmt.Sprintf("%020d", rv)
}

func parseRVKey(key string) uint64 {
	key = key[strings.LastIndex(key, "/")+1:]
	rv, _ := strconv.ParseUint(key, 10, 64)
	return rv
}
