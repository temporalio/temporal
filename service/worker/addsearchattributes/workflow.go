// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package addsearchattributes

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
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
		esClient      esclient.Client
		saManager     searchattribute.Manager
		metricsClient metrics.Client
		logger        log.Logger
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
		a.metricsClient.IncCounter(metrics.AddSearchAttributesWorkflowScope, metrics.AddSearchAttributesFailuresCount)
		if esclient.IsRetryableError(err) {
			a.logger.Error("Unable to update Elasticsearch mapping (retryable error).", tag.ESIndex(params.IndexName), tag.Error(err))
			return fmt.Errorf("%w: %v", ErrUnableToUpdateESMapping, err)
		}
		a.logger.Error("Unable to update Elasticsearch mapping (non-retryable error).", tag.ESIndex(params.IndexName), tag.Error(err))
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("%v: %v", ErrUnableToUpdateESMapping, err), "", nil)
	}
	a.logger.Info("Elasticsearch mapping created.", tag.ESIndex(params.IndexName), tag.ESMapping(params.CustomAttributesToAdd))

	return nil
}

func (a *activities) WaitForYellowStatusActivity(ctx context.Context, indexName string) error {
	if a.esClient == nil {
		a.logger.Info("Elasticsearch client is not configured. Skipping Elasticsearch status check.")
		return nil
	}

	status, err := a.esClient.WaitForYellowStatus(ctx, indexName)
	if err != nil {
		a.logger.Error("Unable to get Elasticsearch cluster status.", tag.ESIndex(indexName), tag.Error(err))
		a.metricsClient.IncCounter(metrics.AddSearchAttributesWorkflowScope, metrics.AddSearchAttributesFailuresCount)
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

	newCustomSearchAttributes := maps.Clone(oldSearchAttributes.Custom())
	maps.Copy(newCustomSearchAttributes, params.CustomAttributesToAdd)
	err = a.saManager.SaveSearchAttributes(ctx, params.IndexName, newCustomSearchAttributes)
	if err != nil {
		a.logger.Info("Unable to save search attributes to cluster metadata.", tag.ESIndex(params.IndexName), tag.Error(err))
		a.metricsClient.IncCounter(metrics.AddSearchAttributesWorkflowScope, metrics.AddSearchAttributesFailuresCount)
		return fmt.Errorf("%w: %v", ErrUnableToSaveSearchAttributes, err)
	}
	a.logger.Info("Search attributes saved to cluster metadata.", tag.ESIndex(params.IndexName))
	return nil
}
