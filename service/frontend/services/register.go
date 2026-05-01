package services

import (
	"fmt"

	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
)

var entries = map[string]variant{}
var registryErr error

func register(name string, v variant) {
	if _, dup := entries[name]; dup {
		registryErr = fmt.Errorf("%w; duplicate registration for variant %s", registryErr, name)
		return
	}
	entries[name] = v
}

func get(name string) (variant, bool)   { v, ok := entries[name]; return v, ok }
func names() []string                   { out := make([]string, 0, len(entries)); for n := range entries { out = append(out, n) }; return out }
func registryError() error              { return registryErr }

type variant struct {
	registerWorkflow func(*grpc.Server, workflowservice.WorkflowServiceServer)
	registerAdmin    func(*grpc.Server, adminservice.AdminServiceServer)
	registerOperator func(*grpc.Server, operatorservice.OperatorServiceServer)
}

type Registration struct {
	Server   *grpc.Server
	Workflow workflowservice.WorkflowServiceServer
	Admin    adminservice.AdminServiceServer
	Operator operatorservice.OperatorServiceServer
	Variant  string
	Logger   log.Logger
}

// Register installs the stable frontend gRPC services, or an experimental
// variant compiled into this binary.
func Register(r Registration) {
	if err := registryError(); err != nil {
		r.Logger.Fatal("invalid experimental API registry", tag.Error(err))
	}

	selected := variant{}
	if r.Variant == "" {
		registerVariant(r, selected)
		return
	}

	var ok bool
	selected, ok = get(r.Variant)
	if !ok {
		r.Logger.Fatal(
			"frontend.apiVariant set but variant not wired into this binary",
			tag.NewStringTag("variant", r.Variant),
			tag.NewStringsTag("compiled_in", names()),
		)
	}

	registerVariant(r, selected)
	r.Logger.Info("Experimental API variant active",
		tag.NewStringTag("variant", r.Variant))
}

func registerVariant(r Registration, v variant) {
	registerWorkflow := registerStableWorkflow
	if v.registerWorkflow != nil {
		registerWorkflow = v.registerWorkflow
	}
	registerAdmin := registerStableAdmin
	if v.registerAdmin != nil {
		registerAdmin = v.registerAdmin
	}
	registerOperator := registerStableOperator
	if v.registerOperator != nil {
		registerOperator = v.registerOperator
	}

	registerWorkflow(r.Server, r.Workflow)
	registerAdmin(r.Server, r.Admin)
	registerOperator(r.Server, r.Operator)
}

func registerStableWorkflow(server *grpc.Server, workflow workflowservice.WorkflowServiceServer) {
	workflowservice.RegisterWorkflowServiceServer(server, workflow)
}

func registerStableAdmin(server *grpc.Server, admin adminservice.AdminServiceServer) {
	adminservice.RegisterAdminServiceServer(server, admin)
}

func registerStableOperator(server *grpc.Server, operator operatorservice.OperatorServiceServer) {
	operatorservice.RegisterOperatorServiceServer(server, operator)
}
