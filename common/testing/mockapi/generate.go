//go:generate mockgen -package workflowservicemock -destination workflowservicemock/v1/service_grpc.pb.mock.go go.temporal.io/api/workflowservice/v1 WorkflowServiceClient
//go:generate mockgen -package operatorservicemock -destination operatorservicemock/v1/service_grpc.pb.mock.go go.temporal.io/api/operatorservice/v1 OperatorServiceClient

package mockapi
