// Package cluster provides a small Kubernetes-style typed resource control plane.
//
// Example:
//
//	c, _ := NewClusterFromURL("memory://?node=worker-a")
//	defer c.Close()
//
//	type WidgetSpec struct {
//		Size  string `json:"size,omitempty" cluster:"required,enum=small|medium|large,index,default=medium"`
//		Owner string `json:"owner,omitempty" cluster:"immutable,index"`
//	}
//	type WidgetStatus struct {
//		Phase string `json:"phase,omitempty" cluster:"enum=Pending|Ready|Failed,index"`
//	}
//
//	schema, _ := SchemaFrom[WidgetSpec, WidgetStatus]("example.test/v1", "Widget", false)
//	_, _ = DefineResource(c, ResourceDef{
//		Resource:   "rawwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "Widget",
//		Schema:     schema,
//	})
//
//	widgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "widgets",
//		APIVersion: "example.test/v1",
//		Kind:       "Widget",
//	})
//
//	created, _ := widgets.Create(ctx, "alpha", WidgetSpec{Owner: "team-a"}, CreateOptions{
//		Labels:      Labels{"app": "demo"},
//		Annotations: Annotations{"team": "platform"},
//	})
//	patched, _ := widgets.Patch(ctx, created.Metadata.Name, []byte(`{"spec":{"size":"large"}}`), PatchOptions{
//		ResourceVersion: created.Metadata.ResourceVersion,
//	})
//	metaPatched, _ := widgets.PatchMetadata(ctx, patched.Metadata.Name, []byte(`{"labels":{"app":"demo","tier":"frontend"}}`), PatchOptions{
//		ResourceVersion: patched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.UpdateStatus(ctx, metaPatched.Metadata.Name, WidgetStatus{Phase: "Ready"}, UpdateOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.PatchStatus(ctx, metaPatched.Metadata.Name, []byte(`{"phase":"Failed"}`), PatchOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//	_, _ = widgets.Delete(ctx, metaPatched.Metadata.Name, DeleteOptions{
//		ResourceVersion: metaPatched.Metadata.ResourceVersion,
//	})
//
//	list, _ := widgets.List(ctx, ListOptions{
//		Selector: Where(
//			Field("status.phase").Eq("Ready"),
//			Field("spec.owner").Eq("team-a"),
//			Label("app").Eq("demo"),
//			Annotation("team").Eq("platform"),
//		),
//	})
//	events, _ := widgets.Watch(ctx, WatchOptions{ResourceVersion: list.ResourceVersion})
//	metadataEvents, _ := widgets.WatchMetadata(ctx, WatchOptions{ResourceVersion: list.ResourceVersion})
//	statusEvents, _ := widgets.WatchStatus(ctx, WatchOptions{ResourceVersion: list.ResourceVersion})
//	_, _, _ = events, metadataEvents, statusEvents
//
//	guardedWidgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "guardedwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "GuardedWidget",
//		Admission: []AdmissionRule{
//			{Name: "create-check", Operations: []AdmissionOperation{AdmissionCreate}},
//			{Name: "metadata-check", Operations: []AdmissionOperation{AdmissionUpdate}, Subresources: []Subresource{SubresourceMetadata}},
//		},
//	})
//	_, _ = guardedWidgets.Create(ctx, "beta", WidgetSpec{Owner: "team-b"}, CreateOptions{})
//	requests, _ := c.AdmissionRequests().Watch(ctx, WatchOptions{SendInitialEvents: true})
//	_ = requests
//	_, _ = c.ApproveAdmission(ctx, "adm_x", AdmissionDecisionOptions{
//		Rule:    "create-check",
//		Decider: "controller-a",
//		Message: "approved",
//	})
//
//	namespacedWidgets, _ := Define(c, TypedResourceDef[WidgetSpec, WidgetStatus]{
//		Resource:   "teamwidgets",
//		APIVersion: "example.test/v1",
//		Kind:       "TeamWidget",
//		Namespaced: true,
//	})
//	teamWidgets, _ := namespacedWidgets.Namespace("team-a")
//	_, _ = teamWidgets.Create(ctx, "alpha", WidgetSpec{Owner: "team-a"}, CreateOptions{})
//	allWidgets, _ := namespacedWidgets.AllNamespaces()
//	_, _ = allWidgets.List(ctx, ListOptions{
//		Selector: Where(Field("metadata.namespace").Eq("team-a")),
//	})
//
//	info, _ := c.Resource("widgets")
//	resources, _ := c.Resources()
//	_, _ = info, resources
//
//	node, _ := c.CurrentNode(ctx)
//	nodeMeta, _ := c.PatchCurrentNodeMetadata(ctx, []byte(`{"labels":{"role":"worker"}}`), PatchOptions{
//		ResourceVersion: node.Metadata.ResourceVersion,
//	})
//	nodeSpec, _ := c.PatchCurrentNodeSpec(ctx, []byte(`{"metadata":{"zone":"cn-sh-1"}}`), PatchOptions{
//		ResourceVersion: nodeMeta.Metadata.ResourceVersion,
//	})
//	nodeStatus, _ := c.PatchCurrentNodeStatus(ctx, []byte(`{"metadata":{"ready":"true"}}`), PatchOptions{
//		ResourceVersion: nodeSpec.Metadata.ResourceVersion,
//	})
//	_, _ = c.UpdateCurrentNodeStatus(ctx, NodeStatus{
//		Metadata: Annotations{"ready": "true"},
//	}, UpdateOptions{
//		ResourceVersion: nodeStatus.Metadata.ResourceVersion,
//	})
//	_ = node
//
//	master, _ := c.Master(ctx)
//	isMaster, _ := c.IsMaster(ctx)
//	history, _ := c.MasterHistory(ctx, 20)
//	masterEvents, _ := c.WatchMaster(ctx, WatchOptions{ResourceVersion: master.ResourceVersion})
//	_, _, _, _ = isMaster, history, masterEvents, master
//
//	rawWidgets, _ := c.Unstructured("widgets")
//	rawObj, _ := rawWidgets.Get(ctx, "alpha")
//	_ = rawObj
package cluster
