package frontend

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/protohelpers/validate"
	"go.temporal.io/server/api/protohelpers/validation"
	"go.temporal.io/server/common/backoff"
	commonlinks "go.temporal.io/server/common/links"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
)

type startWorkflowRequest = workflowservice.StartWorkflowExecutionRequest

// newStartWorkflowRequestValidator builds an exhaustive, typed validator for
// StartWorkflowExecutionRequest. Every field is assigned a rule (nil would
// panic), so a new proto field forces a decision here rather than slipping
// through unvalidated. Rules delegate to the existing validators so error
// messages are unchanged; a few normalize the request in place (task queue,
// request id, links). The validator is built per request to capture ctx (for
// callback validation).
//
// Search attribute unaliasing is intentionally not a rule here: it is not
// idempotent and clones the whole request on change, which the in-place field
// model cannot express; the handler keeps that step.
func (wh *WorkflowHandler) newStartWorkflowRequestValidator(ctx context.Context) validate.StartWorkflowExecutionRequestFieldValidators {
	cfg := wh.config
	return validate.StartWorkflowExecutionRequestFieldValidators{
		WorkflowId: validation.Field[startWorkflowRequest](func(_ string, id string) error {
			return wh.validator.ValidateWorkflowID(id)
		}),
		WorkflowType: validation.Field[startWorkflowRequest](func(_ string, wt *commonpb.WorkflowType) error {
			if wt == nil || wt.GetName() == "" {
				return errWorkflowTypeNotSet
			}
			if len(wt.GetName()) > cfg.MaxIDLengthLimit() {
				return errWorkflowTypeTooLong
			}
			return nil
		}),
		TaskQueue: validation.Field[startWorkflowRequest](func(_ string, tq *taskqueuepb.TaskQueue) error {
			return tqid.NormalizeAndValidateUserDefined(tq, "", "", cfg.MaxIDLengthLimit())
		}),
		RetryPolicy: func(r *startWorkflowRequest, _ string, rp *commonpb.RetryPolicy) error {
			return wh.validator.ValidateRetryPolicy(r.GetNamespace(), rp)
		},
		// Cron and start delay are mutually exclusive, so validate them together.
		CronSchedule: func(r *startWorkflowRequest, _ string, cron string) error {
			if err := wh.validator.ValidateWorkflowStartDelay(cron, r.GetWorkflowStartDelay()); err != nil {
				return err
			}
			return backoff.ValidateSchedule(cron)
		},
		RequestId: func(r *startWorkflowRequest, _ string, _ string) error {
			return validateRequestId(&r.RequestId, cfg.MaxIDLengthLimit())
		},
		// Reuse and conflict policies are validated together after defaulting.
		WorkflowIdConflictPolicy: func(r *startWorkflowRequest, _ string, conflict enumspb.WorkflowIdConflictPolicy) error {
			return wh.validator.ValidateWorkflowIDReusePolicy(r.GetWorkflowIdReusePolicy(), conflict)
		},
		OnConflictOptions: validation.Field[startWorkflowRequest](func(_ string, oco *workflowpb.OnConflictOptions) error {
			return wh.validateOnConflictOptions(oco)
		}),
		Priority: validation.Field[startWorkflowRequest](func(_ string, p *commonpb.Priority) error {
			return priorities.Validate(p)
		}),
		CompletionCallbacks: func(r *startWorkflowRequest, _ string, cbs []*commonpb.Callback) error {
			if len(cbs) == 0 {
				return nil
			}
			return wh.callbackValidator.Validate(ctx, r.GetNamespace(), cbs)
		},
		// Dedups links against callback links (normalizing in place) then validates.
		Links: func(r *startWorkflowRequest, _ string, links []*commonpb.Link) error {
			r.Links = dedupLinksFromCallbacks(links, r.GetCompletionCallbacks())
			allLinks := make([]*commonpb.Link, 0, len(r.GetLinks())+len(r.GetCompletionCallbacks()))
			allLinks = append(allLinks, r.GetLinks()...)
			for _, cb := range r.GetCompletionCallbacks() {
				allLinks = append(allLinks, cb.GetLinks()...)
			}
			ns := r.GetNamespace()
			return commonlinks.Validate(allLinks, cfg.MaxLinksPerRequest(ns), cfg.LinkMaxSize(ns))
		},

		// Optional: validated elsewhere, defaulted, or covered by a rule above.
		Namespace:                    validation.Optional[startWorkflowRequest, string](),                        // resolved by the namespace registry
		WorkflowExecutionTimeout:     validation.Optional[startWorkflowRequest, *durationpb.Duration](),          // timeouts validated together in the handler (order-sensitive)
		WorkflowRunTimeout:           validation.Optional[startWorkflowRequest, *durationpb.Duration](),          // with WorkflowExecutionTimeout
		WorkflowTaskTimeout:          validation.Optional[startWorkflowRequest, *durationpb.Duration](),          // with WorkflowExecutionTimeout
		WorkflowStartDelay:           validation.Optional[startWorkflowRequest, *durationpb.Duration](),          // with CronSchedule
		WorkflowIdReusePolicy:        validation.Optional[startWorkflowRequest, enumspb.WorkflowIdReusePolicy](), // with WorkflowIdConflictPolicy
		Identity:                     validation.Optional[startWorkflowRequest, string](),
		Input:                        validation.Optional[startWorkflowRequest, *commonpb.Payloads](),
		Memo:                         validation.Optional[startWorkflowRequest, *commonpb.Memo](),
		SearchAttributes:             validation.Optional[startWorkflowRequest, *commonpb.SearchAttributes](), // unaliased in the handler (non-idempotent)
		Header:                       validation.Optional[startWorkflowRequest, *commonpb.Header](),
		RequestEagerExecution:        validation.Optional[startWorkflowRequest, bool](),
		ContinuedFailure:             validation.Optional[startWorkflowRequest, *failurepb.Failure](),
		LastCompletionResult:         validation.Optional[startWorkflowRequest, *commonpb.Payloads](),
		UserMetadata:                 validation.Optional[startWorkflowRequest, *sdkpb.UserMetadata](),
		VersioningOverride:           validation.Optional[startWorkflowRequest, *workflowpb.VersioningOverride](),
		EagerWorkerDeploymentOptions: validation.Optional[startWorkflowRequest, *deploymentpb.WorkerDeploymentOptions](),
		// TimeSkippingConfig is validated in the handler, after search-attribute
		// unaliasing (order-sensitive).
		TimeSkippingConfig: validation.Optional[startWorkflowRequest, *commonpb.TimeSkippingConfig](),
	}
}
