package workflow

import (
	"time"

	"go.temporal.io/sdk/client" // For metrics.Tags
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// PaymentDetails structure to hold information about the payment.
type PaymentDetails struct {
	PaymentID string
	Amount    float64
	Currency  string
}

// PaymentWorkflow defines the Temporal workflow for processing a payment.
func PaymentWorkflow(ctx workflow.Context, details PaymentDetails) (string, error) {
	handler := workflow.GetMetricsHandler(ctx)
	logger := workflow.GetLogger(ctx)

	handler.Counter("payment_workflows_started_total").Inc(1)
	logger.Info("PaymentWorkflow started", "PaymentDetails", details)

	// Configure activity options with a retry policy
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second, // Max duration of a single activity attempt
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,  // First retry delay
			BackoffCoefficient: 2.0,              // Double the delay each retry
			MaximumInterval:    15 * time.Second, // Maximum delay between retries
			MaximumAttempts:    3,                // Maximum number of attempts
			// NonRetryableErrorTypes: []string{}, // Define any errors that should not be retried
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Instantiate the activity struct.
	// Using &PaymentActivity{} is fine if it has no fields or if default values are okay.
	var pa = &PaymentActivity{} 
	
	var resultString string
	// Execute the payment processing activity
	err := workflow.ExecuteActivity(ctx, pa.ProcessPaymentActivity, details.PaymentID, details.Amount).Get(ctx, &resultString)

	if err != nil {
		logger.Error("Payment activity failed.", "Error", err, "PaymentID", details.PaymentID)
		handler.WithTags(client.Tags{"status": "failed"}).Counter("payment_workflows_completed_total").Inc(1)
		handler.Counter("payment_workflows_failed_permanently_total").Inc(1)
		return "", err
	}

	handler.WithTags(client.Tags{"status": "success"}).Counter("payment_workflows_completed_total").Inc(1)
	logger.Info("Payment workflow completed successfully.", "Result", resultString, "PaymentID", details.PaymentID)
	return resultString, nil
}
