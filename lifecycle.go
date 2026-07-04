package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

func (c *Cluster) cleanupLifecycle(ctx context.Context) error {
	defs := c.snapshotDefinitions()
	objects := make([]Unstructured, 0)
	uidSet := make(map[string]struct{})
	for _, def := range defs {
		raw := &UnstructuredResource{cluster: c, def: def}
		if def.Namespaced {
			raw.allNamespaces = true
		}
		list, err := raw.List(ctx, ListOptions{IncludeDeleting: true, Limit: 100000})
		if err != nil {
			return err
		}
		for i := range list.Items {
			obj := cloneUnstructured(list.Items[i])
			objects = append(objects, obj)
			uidSet[obj.Metadata.UID] = struct{}{}
		}
	}
	now := time.Now().UTC()
	for _, obj := range objects {
		if obj.Metadata.DeletionTimestamp != nil {
			if len(obj.Metadata.Finalizers) == 0 && !gracePeriodActive(obj.Metadata, now) {
				if err := c.deleteForCleanup(ctx, obj); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
					return err
				}
			}
			continue
		}
		for _, owner := range obj.Metadata.OwnerReferences {
			if _, ok := uidSet[owner.UID]; ok {
				continue
			}
			if err := c.deleteForCleanup(ctx, obj); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				return err
			}
			break
		}
	}
	return nil
}

func (c *Cluster) orphanDependents(ctx context.Context, owner Unstructured) error {
	for _, def := range c.snapshotDefinitions() {
		raw := &UnstructuredResource{cluster: c, def: def}
		if def.Namespaced {
			raw.allNamespaces = true
		}
		list, err := raw.List(ctx, ListOptions{IncludeDeleting: true, Limit: 100000})
		if err != nil {
			return err
		}
		for i := range list.Items {
			obj := list.Items[i]
			updatedRefs := filterOwnerReferences(obj.Metadata.OwnerReferences, owner.Metadata.UID)
			if len(updatedRefs) == len(obj.Metadata.OwnerReferences) {
				continue
			}
			rawObj := &UnstructuredResource{cluster: c, def: def}
			if def.Namespaced {
				rawObj.namespace = obj.Metadata.Namespace
			}
			patch, err := json.Marshal(map[string]any{"ownerReferences": updatedRefs})
			if err != nil {
				return err
			}
			if _, err := rawObj.PatchMetadata(ctx, obj.Metadata.Name, patch, PatchOptions{ResourceVersion: obj.Metadata.ResourceVersion}); err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
				return err
			}
		}
	}
	return nil
}

func (c *Cluster) deleteForCleanup(ctx context.Context, obj Unstructured) error {
	def := c.definitionForGVK(obj.APIVersion, obj.Kind)
	if def == nil {
		return nil
	}
	raw := &UnstructuredResource{cluster: c, def: def}
	if def.Namespaced {
		raw.namespace = obj.Metadata.Namespace
	}
	_, err := raw.Delete(ctx, obj.Metadata.Name, DeleteOptions{
		ResourceVersion:   obj.Metadata.ResourceVersion,
		PropagationPolicy: DeletePropagationBackground,
	})
	return err
}

func (c *Cluster) snapshotDefinitions() []*resourceDefinition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	defs := make([]*resourceDefinition, 0, len(c.definitions))
	for _, def := range c.definitions {
		defs = append(defs, def)
	}
	return defs
}

func (c *Cluster) definitionForGVK(apiVersion, kind string) *resourceDefinition {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if def := c.definitionsByGVK[apiVersion+"\x00"+kind]; def != nil {
		return def
	}
	for _, candidate := range c.definitions {
		if candidate.APIVersion == apiVersion && candidate.Kind == kind {
			return candidate
		}
	}
	return nil
}
