package api

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	expmaps "golang.org/x/exp/maps"
)

var publicMethodRgx = regexp.MustCompile("^[A-Z]")

func TestWorkflowServiceMetadata(t *testing.T) {
	tp := reflect.TypeOf((*workflowservice.WorkflowServiceServer)(nil)).Elem()
	checkService(t, tp, workflowServiceMetadata)
}

func TestOperatorServiceMetadata(t *testing.T) {
	tp := reflect.TypeOf((*operatorservice.OperatorServiceServer)(nil)).Elem()
	checkService(t, tp, operatorServiceMetadata)
}

func checkService(t *testing.T, tp reflect.Type, m map[string]MethodMetadata) {
	methods := getMethodNames(tp)
	require.ElementsMatch(t, methods, expmaps.Keys(m),
		"If you're adding a new method to Workflow/OperatorService, please add metadata for it in metadata.go")

	for _, method := range methods {
		refMethod, ok := tp.MethodByName(method)
		require.True(t, ok)

		checkNamespace := false
		hasNamespace := false
		namespaceIsString := false

		if refMethod.Type.NumIn() >= 2 {
			// not streaming
			checkNamespace = true
			requestType := refMethod.Type.In(1).Elem()
			var nsField reflect.StructField
			nsField, hasNamespace = requestType.FieldByName("Namespace")
			if hasNamespace {
				namespaceIsString = nsField.Type == reflect.TypeOf("string")
			}
		}

		md := m[method]
		switch md.Scope {
		case ScopeNamespace:
			if checkNamespace {
				assert.Truef(t, namespaceIsString, "%s with ScopeNamespace should have a Namespace field that is a string", method)
			}
		case ScopeCluster:
			if checkNamespace {
				assert.Falsef(t, hasNamespace, "%s with ScopeCluster should not have a Namespace field", method)
			}
		default:
			t.Error("unknown Scope for", method)
		}

		switch md.Access {
		case AccessReadOnly, AccessWrite, AccessAdmin:
		default:
			t.Error("unknown Access for", method)
		}
	}
}

func TestGetMethodMetadata(t *testing.T) {
	md := GetMethodMetadata("/temporal.api.workflowservice.v1.WorkflowService/RespondActivityTaskCompleted")
	assert.Equal(t, ScopeNamespace, md.Scope)
	assert.Equal(t, AccessWrite, md.Access)

	md = GetMethodMetadata("/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask")
	assert.Equal(t, ScopeNamespace, md.Scope)
	assert.Equal(t, AccessWrite, md.Access)

	// all AdminService is cluster/admin
	md = GetMethodMetadata("/temporal.server.api.adminservice.v1.AdminService/CloseShard")
	assert.Equal(t, ScopeCluster, md.Scope)
	assert.Equal(t, AccessAdmin, md.Access)

	md = GetMethodMetadata("/OtherService/Method1")
	assert.Equal(t, ScopeUnknown, md.Scope)
	assert.Equal(t, AccessUnknown, md.Access)
}

func getMethodNames(tp reflect.Type) []string {
	var out []string
	for i := 0; i < tp.NumMethod(); i++ {
		name := tp.Method(i).Name
		// Don't collect unimplemented methods. This weeds out the
		// `mustEmbedUnimplementedFooBarBaz` required by the GRPC v2 gateway
		if publicMethodRgx.MatchString(name) {
			out = append(out, name)
		}
	}
	return out
}

func TestServiceName(t *testing.T) {
	assert.Equal(t, WorkflowServicePrefix, ServiceName(WorkflowServicePrefix+"SomeAPI"))
	assert.Equal(t, AdminServicePrefix, ServiceName(AdminServicePrefix+"SomeAPI"))
	assert.Equal(t, "", ServiceName("SomeAPI"))
	assert.Equal(t, "", ServiceName(""))
}
