package testcore

import "google.golang.org/grpc"

type FrontendClients struct {
	workflow grpc.ClientConnInterface
	admin    grpc.ClientConnInterface
	operator grpc.ClientConnInterface
}

func (f FrontendClients) Workflow() grpc.ClientConnInterface {
	return f.workflow
}

func (f FrontendClients) Admin() grpc.ClientConnInterface {
	return f.admin
}

func (f FrontendClients) Operator() grpc.ClientConnInterface {
	return f.operator
}
