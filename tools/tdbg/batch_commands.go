package tdbg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/urfave/cli/v2"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/primitives"
)

const (
	batchTypeTerminateWorkflows  = "terminate-workflows"
	batchTypeTerminateActivities = "terminate-activities"
)

var batchTypes = []string{
	batchTypeTerminateWorkflows,
	batchTypeTerminateActivities,
}

func newAdminBatchCommands(clientFactory ClientFactory, prompterFactory PrompterFactory) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "start",
			Usage: "Delegate termination in a user namespace to a batch workflow in temporal-system",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     FlagBatchType,
					Usage:    fmt.Sprintf("Batch operation to run, one of: %s", strings.Join(batchTypes, ", ")),
					Required: true,
				},
				&cli.StringFlag{
					Name:  FlagVisibilityQuery,
					Usage: "Visibility query selecting the executions to operate on",
				},
				&cli.StringFlag{
					Name:  FlagReason,
					Usage: "Reason for starting the batch job",
				},
				&cli.StringFlag{
					Name:  FlagJobID,
					Usage: "Optional job ID (auto-generated if not provided)",
				},
			},
			Action: func(c *cli.Context) error {
				return AdminBatchStart(c, clientFactory, prompterFactory(c))
			},
		},
	}
}

// AdminBatchStart starts a batch operation whose workflow runs in the system namespace and
// whose per-execution calls target the namespace given by --namespace. Unlike
// StartBatchOperation, this does not require the target namespace to have a per-namespace
// worker in this cluster.
func AdminBatchStart(c *cli.Context, clientFactory ClientFactory, prompter *Prompter) error {
	adminClient := clientFactory.AdminClient(c)
	workflowClient := clientFactory.WorkflowClient(c)

	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	query, err := getRequiredOption(c, FlagVisibilityQuery)
	if err != nil {
		return err
	}

	reason, err := getRequiredOption(c, FlagReason)
	if err != nil {
		return err
	}

	batchType, err := getRequiredOption(c, FlagBatchType)
	if err != nil {
		return err
	}

	delegatedType, err := delegatedBatchType(batchType)
	if err != nil {
		return err
	}

	jobID := c.String(FlagJobID)
	if jobID == "" {
		jobID = fmt.Sprintf("batch-%s-%d", batchType, time.Now().UnixNano())
	}
	// The workflow ID lives in the system namespace, so it has to distinguish target namespaces.
	jobIDWithNS := fmt.Sprintf("%s:%s", jobID, nsName)

	ctx, cancel := newContext(c)
	defer cancel()

	if err := checkTargetNamespaceActive(ctx, adminClient, workflowClient, nsName); err != nil {
		return err
	}

	matchCount, targetKind, err := countDelegatedBatchExecutions(ctx, workflowClient, nsName, query, delegatedType)
	if err != nil {
		return err
	}

	summary := fmt.Sprintf(
		"DANGER: destructive delegated batch operation\n\n"+
			"User namespace: %q\n"+
			"Batch workflow namespace: %q\n"+
			"Cluster eligibility: passed\n"+
			"Operation: %s\n"+
			"Visibility query: %q\n"+
			"Currently matching: %d %s\n\n"+
			"This delegates termination of matching %s in user namespace %q to a batch workflow running in %q.\n"+
			"For a global namespace, this operation must be submitted through its active cluster.\n"+
			"Supported operations are limited to %s and %s.\n\n"+
			"Review the user namespace, visibility query, and current match count carefully.\n"+
			"Visibility results can change while the batch is running.",
		nsName,
		primitives.SystemLocalNamespace,
		batchType,
		query,
		matchCount,
		targetKind,
		targetKind,
		nsName,
		primitives.SystemLocalNamespace,
		batchTypeTerminateWorkflows,
		batchTypeTerminateActivities,
	)
	if _, err := fmt.Fprintln(c.App.Writer, summary); err != nil {
		return fmt.Errorf("unable to write batch operation summary: %w", err)
	}
	prompter.Prompt(fmt.Sprintf("Proceed with terminating the currently matching %d %s?", matchCount, targetKind))

	_, err = adminClient.StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:       nsName,
		VisibilityQuery: query,
		JobId:           jobIDWithNS,
		Reason:          reason,
		Identity:        getCurrentUserFromEnv(),
		Operation: &adminservice.StartAdminBatchOperationRequest_DelegationOperation{
			DelegationOperation: &adminservice.BatchOperationDelegation{BatchType: delegatedType},
		},
	})
	if err != nil {
		return fmt.Errorf("unable to start batch operation: %w", err)
	}

	// nolint:errcheck // assuming that write will succeed.
	fmt.Fprintf(c.App.Writer,
		"Batch operation %q started successfully in namespace %s, targeting namespace %q, with Job ID: %s\n",
		batchType, primitives.SystemLocalNamespace, nsName, jobIDWithNS)
	return nil
}

func countDelegatedBatchExecutions(
	ctx context.Context,
	workflowClient workflowservice.WorkflowServiceClient,
	nsName string,
	query string,
	batchType enumspb.BatchOperationType,
) (int64, string, error) {
	switch batchType {
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW:
		resp, err := workflowClient.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: nsName,
			Query:     query,
		})
		if err != nil {
			return 0, "", fmt.Errorf("unable to count workflow executions: %w", err)
		}
		return resp.GetCount(), "workflows", nil
	case enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY:
		resp, err := workflowClient.CountActivityExecutions(ctx, &workflowservice.CountActivityExecutionsRequest{
			Namespace: nsName,
			Query:     query,
		})
		if err != nil {
			return 0, "", fmt.Errorf("unable to count activity executions: %w", err)
		}
		return resp.GetCount(), "activities", nil
	default:
		return 0, "", fmt.Errorf("unsupported delegated batch operation: %v", batchType)
	}
}

// checkTargetNamespaceActive fails before the job is started if this cluster is not active for
// the target namespace. StartAdminBatchOperation rejects it too; checking here names both
// clusters and avoids prompting for a job that cannot run.
func checkTargetNamespaceActive(
	ctx context.Context,
	adminClient adminservice.AdminServiceClient,
	workflowClient workflowservice.WorkflowServiceClient,
	nsName string,
) error {
	nsResp, err := workflowClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	if err != nil {
		return fmt.Errorf("unable to describe namespace %q: %w", nsName, err)
	}
	// A local namespace is active in every cluster it exists in.
	if !nsResp.GetIsGlobalNamespace() {
		return nil
	}

	clusterResp, err := adminClient.DescribeCluster(ctx, &adminservice.DescribeClusterRequest{})
	if err != nil {
		return fmt.Errorf("unable to describe cluster: %w", err)
	}

	activeCluster := nsResp.GetReplicationConfig().GetActiveClusterName()
	if activeCluster != clusterResp.GetClusterName() {
		return fmt.Errorf(
			"namespace %q is active in cluster %q, but this cluster is %q: a batch operation must be started in the active cluster",
			nsName, activeCluster, clusterResp.GetClusterName())
	}
	return nil
}

// delegatedBatchType maps the --batch-type value to the batch operation the admin API delegates.
// The operation itself needs no parameters here: identity and reason travel on the envelope.
func delegatedBatchType(batchType string) (enumspb.BatchOperationType, error) {
	switch batchType {
	case batchTypeTerminateWorkflows:
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, nil
	case batchTypeTerminateActivities:
		return enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, nil
	default:
		return enumspb.BATCH_OPERATION_TYPE_UNSPECIFIED,
			fmt.Errorf("unknown batch type %q, expected one of: %s", batchType, strings.Join(batchTypes, ", "))
	}
}
