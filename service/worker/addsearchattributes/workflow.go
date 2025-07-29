package addsearchattributes

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
)

const (
	// WorkflowName is workflowId of the system workflow performing addition of search attributes
	WorkflowName = "temporal-sys-add-search-attributes-workflow"
)

type (
	// WorkflowParams is the parameters for add search attributes workflow.
	WorkflowParams struct {
		// Elasticsearch index name. Can be empty string if Elasticsearch is not configured.
		IndexName string
		// Search attributes that need to be added to the index.
		CustomAttributesToAdd map[string]enumspb.IndexedValueType
		// If true skip Elasticsearch schema update and only update cluster metadata.
		SkipSchemaUpdate bool
	}

	activities struct {
		esClient       esclient.Client
		saManager      searchattribute.Manager
		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

var (
	addESMappingFieldActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Second,
	}

	waitForYellowStatusActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 5 * time.Second,
		},
		StartToCloseTimeout:    20 * time.Second,
		ScheduleToCloseTimeout: 60 * time.Second,
	}

	updateClusterMetadataActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    2 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Second,
	}

	ErrUnableToUpdateESMapping      = errors.New("unable to update Elasticsearch mapping")
	ErrUnableToExecuteActivity      = errors.New("unable to execute activity")
	ErrUnableToGetSearchAttributes  = errors.New("unable to get search attributes from cluster metadata")
	ErrUnableToSaveSearchAttributes = errors.New("unable to save search attributes to cluster metadata")
)

// AddSearchAttributesWorkflow is the workflow that adds search attributes to the cluster for specific index.
func AddSearchAttributesWorkflow(ctx workflow.Context, params WorkflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started.", tag.WorkflowType(WorkflowName))

	ctx = workflow.WithTaskQueue(ctx, primitives.AddSearchAttributesActivityTQ)

	var a *activities
	var err error

	if !params.SkipSchemaUpdate {
		ctx1 := workflow.WithActivityOptions(ctx, addESMappingFieldActivityOptions)
		err = workflow.ExecuteActivity(ctx1, a.AddESMappingFieldActivity, params).Get(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: AddESMappingFieldActivity: %v", ErrUnableToExecuteActivity, err)
		}

		ctx2 := workflow.WithActivityOptions(ctx, waitForYellowStatusActivityOptions)
		err = workflow.ExecuteActivity(ctx2, a.WaitForYellowStatusActivity, params.IndexName).Get(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: WaitForYellowStatusActivity: %v", ErrUnableToExecuteActivity, err)
		}
	}

	ctx3 := workflow.WithActivityOptions(ctx, updateClusterMetadataActivityOptions)
	err = workflow.ExecuteActivity(ctx3, a.UpdateClusterMetadataActivity, params).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: UpdateClusterMetadataActivity: %v", ErrUnableToExecuteActivity, err)
	}

	logger.Info("Workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return nil
}

func (a *activities) AddESMappingFieldActivity(ctx context.Context, params WorkflowParams) error {
	if a.esClient == nil {
		a.logger.Info("Elasticsearch client is not configured. Skipping mapping update.")
		return nil
	}

	a.logger.Info("Creating Elasticsearch mapping.", tag.ESIndex(params.IndexName), tag.ESMapping(params.CustomAttributesToAdd))
	_, err := a.esClient.PutMapping(ctx, params.IndexName, params.CustomAttributesToAdd)
	if err != nil {
		metrics.AddSearchAttributesFailuresCount.With(a.metricsHandler).Record(1)

		if a.isRetryableError(err) {
			a.logger.Error("Unable to update Elasticsearch mapping (retryable error).", tag.ESIndex(params.IndexName), tag.Error(err))
			return fmt.Errorf("%w: %v", ErrUnableToUpdateESMapping, err)
		}
		a.logger.Error("Unable to update Elasticsearch mapping (non-retryable error).", tag.ESIndex(params.IndexName), tag.Error(err))
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("%v: %v", ErrUnableToUpdateESMapping, err), "", nil)
	}
	a.logger.Info("Elasticsearch mapping created.", tag.ESIndex(params.IndexName), tag.ESMapping(params.CustomAttributesToAdd))

	return nil
}

func (a *activities) isRetryableError(err error) bool {
	// For V8 client, we check status codes from HTTP errors
	// This is a simplified approach since go-elasticsearch v8 doesn't have a specific Error type
	errStr := err.Error()

	// Check for common non-retryable status codes in error message
	if containsAnyOfSubstrings(errStr, []string{
		"400", "Bad Request",
		"401", "Unauthorized",
		"403", "Forbidden",
		"404", "Not Found",
		"409", "Conflict",
	}) {
		return false
	}

	return true
}

func containsAnyOfSubstrings(str string, substrings []string) bool {
	for _, substr := range substrings {
		if strings.Contains(str, substr) {
			return true
		}
	}
	return false
}

func (a *activities) WaitForYellowStatusActivity(ctx context.Context, indexName string) error {
	if a.esClient == nil {
		a.logger.Info("Elasticsearch client is not configured. Skipping Elasticsearch status check.")
		return nil
	}

	status, err := a.esClient.WaitForYellowStatus(ctx, indexName)
	if err != nil {
		a.logger.Error("Unable to get Elasticsearch cluster status.", tag.ESIndex(indexName), tag.Error(err))
		metrics.AddSearchAttributesFailuresCount.With(a.metricsHandler).Record(1)
		return err
	}
	a.logger.Info("Elasticsearch cluster status.", tag.ESIndex(indexName), tag.ESClusterStatus(status))
	return nil
}

func (a *activities) UpdateClusterMetadataActivity(ctx context.Context, params WorkflowParams) error {
	oldSearchAttributes, err := a.saManager.GetSearchAttributes(params.IndexName, true)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnableToGetSearchAttributes, err)
	}

	newCustomSearchAttributes := util.CloneMapNonNil(oldSearchAttributes.Custom())
	maps.Copy(newCustomSearchAttributes, params.CustomAttributesToAdd)
	err = a.saManager.SaveSearchAttributes(ctx, params.IndexName, newCustomSearchAttributes)
	if err != nil {
		a.logger.Info("Unable to save search attributes to cluster metadata.", tag.ESIndex(params.IndexName), tag.Error(err))
		metrics.AddSearchAttributesFailuresCount.With(a.metricsHandler).Record(1)
		return fmt.Errorf("%w: %v", ErrUnableToSaveSearchAttributes, err)
	}
	a.logger.Info("Search attributes saved to cluster metadata.", tag.ESIndex(params.IndexName))
	return nil
}
