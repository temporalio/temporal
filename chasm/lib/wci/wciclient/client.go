// Package wciclient implements ASW's client.ChasmBackend (and the workflow-side MigrationClient)
// over the WCI CHASM service.
package wciclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/serviceerror"
	wciclient "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/wci"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"go.temporal.io/server/chasm/lib/wci/migration"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(wcipb.NewWorkerControllerInstanceServiceLayeredClient),
	fx.Provide(New),
	fx.Provide(func(c *Client) wciclient.ChasmBackend { return c }),
)

type Params struct {
	fx.In

	Service wcipb.WorkerControllerInstanceServiceClient
	// Only frontend and history provide one; List needs it.
	Visibility chasm.VisibilityManager `optional:"true"`
}

// Client must not depend on wciclient.Client: ClientProvider consumes it to build the router.
type Client struct {
	service    wcipb.WorkerControllerInstanceServiceClient
	visibility chasm.VisibilityManager
}

var _ wciclient.ChasmBackend = (*Client)(nil)

func New(p Params) *Client {
	return &Client{service: p.Service, visibility: p.Visibility}
}

// mapError translates CHASM errors, which cross gRPC as plain serviceerrors, onto ASW's sentinels.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var unavailable *serviceerror.Unavailable
	if errors.As(err, &unavailable) && strings.Contains(unavailable.Message, wci.ErrMigrationPending.Error()) {
		return fmt.Errorf("%w: %w", wciclient.ErrMigrationPending, err)
	}
	var precondition *serviceerror.FailedPrecondition
	if errors.As(err, &precondition) {
		switch {
		case strings.Contains(precondition.Message, wci.ErrClosed.Error()):
			return fmt.Errorf("%w: %w", wciclient.ErrInstanceClosed, err)
		case strings.Contains(precondition.Message, wci.ErrProviderNotSupportedOnCHASM.Error()):
			return fmt.Errorf("%w: %w", wciclient.ErrProviderNotSupportedOnCHASM, err)
		}
	}
	return err
}

func instanceID(version *deploymentpb.WorkerDeploymentVersion) string {
	return wciclient.GenerateWorkerControllerInstanceWorkflowID(version)
}

func upsertsToCHASM(upserts map[string]iface.ScalingGroupSpecUpdate) map[string]*wcipb.ScalingGroupSpecUpdate {
	out := make(map[string]*wcipb.ScalingGroupSpecUpdate, len(upserts))
	for id, u := range upserts {
		out[id] = &wcipb.ScalingGroupSpecUpdate{Spec: migration.SpecToCHASM(u.Spec), UpdateMask: u.UpdateMask}
	}
	return out
}

func specToWorkflow(specs map[string]*wcipb.ScalingGroupSpec) *iface.WorkerControllerInstanceSpec {
	out := &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: make(map[string]iface.ScalingGroupSpec, len(specs))}
	for id, s := range specs {
		out.ScalingGroupSpecs[id] = migration.SpecToWorkflow(s)
	}
	return out
}

// UpdateTaskQueueName only applies to the workflow backend's per-namespace worker.
func (c *Client) UpdateTaskQueueName(context.Context, string) error {
	return nil
}

func (c *Client) WorkerControllerInstanceExists(ctx context.Context, ns *namespace.Namespace, version *deploymentpb.WorkerDeploymentVersion) (bool, error) {
	_, _, err := c.DescribeWorkerControllerInstance(ctx, ns, version)
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) || errors.Is(err, wciclient.ErrInstanceClosed) {
		return false, nil
	}
	return err == nil, err
}

func (c *Client) DescribeWorkerControllerInstance(ctx context.Context, ns *namespace.Namespace, version *deploymentpb.WorkerDeploymentVersion) (*iface.QueryDescribeWorkerControllerInstanceResponse, []byte, error) {
	resp, err := c.service.DescribeWorkerControllerInstance(ctx, &wcipb.DescribeWorkerControllerInstanceRequest{
		NamespaceId: ns.ID().String(),
		InstanceId:  instanceID(version),
	})
	if err != nil {
		return nil, nil, mapError(err)
	}
	validation, err := migration.ValidationToWorkflow(resp.GetValidationStatus())
	if err != nil {
		return nil, nil, err
	}
	return &iface.QueryDescribeWorkerControllerInstanceResponse{
		DeploymentName:       resp.GetDeploymentName(),
		DeploymentBuildID:    resp.GetBuildId(),
		CreateTime:           resp.GetCreateTime(),
		Spec:                 specToWorkflow(resp.GetScalingGroupSpecs()),
		ConflictToken:        resp.GetConflictToken(),
		LastModifierIdentity: resp.GetLastModifierIdentity(),
		ValidationStatus:     validation,
	}, resp.GetConflictToken(), nil
}

// ListWorkerControllerInstances lists running CHASM WCIs; closed (migrated or deleted) ones are skipped.
func (c *Client) ListWorkerControllerInstances(ctx context.Context, ns *namespace.Namespace, pageSize int, nextPageToken []byte) ([]*wciclient.WorkerControllerInstanceSummary, []byte, error) {
	if c.visibility == nil {
		return nil, nil, serviceerror.NewUnimplemented("listing CHASM worker controller instances needs a visibility manager in this service")
	}
	resp, err := chasm.ListExecutions[*wci.WorkerControllerInstance, *wcipb.WorkerControllerInstanceMemo](
		chasm.NewVisibilityManagerContext(ctx, c.visibility),
		&chasm.ListExecutionsRequest{NamespaceName: ns.Name().String(), PageSize: pageSize, NextPageToken: nextPageToken},
	)
	if err != nil {
		return nil, nil, err
	}
	var out []*wciclient.WorkerControllerInstanceSummary
	for _, ex := range resp.Executions {
		// An unset close time decodes as the epoch.
		if ex.CloseTime.Unix() > 0 {
			continue
		}
		out = append(out, &wciclient.WorkerControllerInstanceSummary{
			DeploymentName: ex.ChasmMemo.GetDeploymentName(),
			BuildId:        ex.ChasmMemo.GetBuildId(),
			CreateTime:     ex.ChasmMemo.GetCreateTime(),
		})
	}
	return out, resp.NextPageToken, nil
}

func (c *Client) UpdateWorkerControllerInstance(
	ctx context.Context,
	ns *namespace.Namespace,
	version *deploymentpb.WorkerDeploymentVersion,
	conflictToken []byte,
	identity string,
	upsertScalingGroups map[string]iface.ScalingGroupSpecUpdate,
	removeScalingGroups []string,
) (*iface.UpdateWorkerControllerInstanceResponse, error) {
	resp, err := c.service.UpdateWorkerControllerInstance(ctx, &wcipb.UpdateWorkerControllerInstanceRequest{
		NamespaceId:         ns.ID().String(),
		InstanceId:          instanceID(version),
		DeploymentName:      version.GetDeploymentName(),
		BuildId:             version.GetBuildId(),
		ConflictToken:       conflictToken,
		Identity:            identity,
		UpsertScalingGroups: upsertsToCHASM(upsertScalingGroups),
		RemoveScalingGroups: removeScalingGroups,
	})
	if err != nil {
		return nil, mapError(err)
	}
	return &iface.UpdateWorkerControllerInstanceResponse{Spec: specToWorkflow(resp.GetScalingGroupSpecs())}, nil
}

func (c *Client) ValidateWorkerControllerInstanceSpec(
	ctx context.Context,
	ns *namespace.Namespace,
	version *deploymentpb.WorkerDeploymentVersion,
	identity string,
	upsertScalingGroups map[string]iface.ScalingGroupSpecUpdate,
	removeScalingGroups []string,
) error {
	_, err := c.service.ValidateWorkerControllerInstanceSpec(ctx, &wcipb.ValidateWorkerControllerInstanceSpecRequest{
		NamespaceId:         ns.ID().String(),
		InstanceId:          instanceID(version),
		Identity:            identity,
		UpsertScalingGroups: upsertsToCHASM(upsertScalingGroups),
		RemoveScalingGroups: removeScalingGroups,
	})
	return mapError(err)
}

func (c *Client) DeleteWorkerControllerInstance(ctx context.Context, ns *namespace.Namespace, version *deploymentpb.WorkerDeploymentVersion, identity string) error {
	_, err := c.service.DeleteWorkerControllerInstance(ctx, &wcipb.DeleteWorkerControllerInstanceRequest{
		NamespaceId: ns.ID().String(),
		InstanceId:  instanceID(version),
		Identity:    identity,
	})
	return mapError(err)
}

// SignalTaskAddEvent keeps the caller's context so the propagated caller type sets the lock
// priority in history (background → low). Alternative if lock waits grow: force
// headers.CallerTypeAPI here for high priority, at the cost of drawing on API rate-limit quotas.
func (c *Client) SignalTaskAddEvent(ctx context.Context, ns *namespace.Namespace, version *deploymentpb.WorkerDeploymentVersion, request *iface.SignalTaskAddRequest) error {
	_, err := c.service.RecordTaskAdd(ctx, &wcipb.RecordTaskAddRequest{
		NamespaceId: ns.ID().String(),
		InstanceId:  instanceID(version),
		Event:       migration.TaskAddToCHASM(request),
	})
	return mapError(err)
}

// CreateFromWorkflowState implements the workflow-side MigrationClient.
func (c *Client) CreateFromWorkflowState(ctx context.Context, args *iface.WorkerControllerInstanceWorkflowArgs) error {
	id := instanceID(&deploymentpb.WorkerDeploymentVersion{DeploymentName: args.DeploymentName, BuildId: args.BuildId})
	req, err := migration.WorkflowStateToCHASM(id, args)
	if err != nil {
		return err
	}
	_, err = c.service.CreateFromMigrationState(ctx, req)
	return mapError(err)
}
