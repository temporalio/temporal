package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

type (
	Namespace struct {
		stamp.Model[*Namespace]
		stamp.Scope[*Cluster]

		InternalID string
	}
	NamespaceCreated struct {
		Name string
	}
	NamespaceProvider interface {
		GetNamespace() *Namespace
	}
)

func (n *Namespace) Verify() {}

//func (n *Namespace) Identify(action *Action) stamp.ID {
//	switch t := action.Request.(type) {
//	case NewTaskQueue:
//		return t.NamespaceName
//
//	// GRPC
//	case proto.Message:
//		// First, try the namespace name.
//		if id := stamp.ID(findProtoValueByNameType[string](t, "namespace", protoreflect.StringKind)); id != "" {
//			return id
//		}
//
//		// Then, try to the namespace ID; either from the task token or from the request.
//		var namespaceID string
//		if reqInternalID := findProtoValueByNameType[string](t, "namespace_id", protoreflect.StringKind); reqInternalID != "" {
//			namespaceID = reqInternalID
//		} else if taskToken := findProtoValueByNameType[[]byte](t, "task_token", protoreflect.BytesKind); taskToken != nil {
//			namespaceID = mustDeserializeTaskToken(taskToken).NamespaceId
//		}
//		if namespaceID == n.InternalID {
//			return n.GetID()
//		}
//
//	// persistence
//	case *persistence.CreateNamespaceRequest:
//		return stamp.ID(t.Namespace.Info.Name)
//	case *persistence.InternalCreateNamespaceRequest:
//		// InternalCreateNamespaceRequest is a special case for the test setup
//		// which creates the namespace directly; instead of through the API.
//		return stamp.ID(t.Name)
//	case *persistence.InternalUpdateWorkflowExecutionRequest:
//		if mdlInternalID := n.InternalID; t.UpdateWorkflowMutation.ExecutionInfo.NamespaceId == mdlInternalID {
//			return n.GetID()
//		}
//	}
//
//	return ""
//}
