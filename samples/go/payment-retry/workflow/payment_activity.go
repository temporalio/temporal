package workflow

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client" // For metrics.Tags
	"go.temporal.io/sdk/log"
)

// Seed the random number generator
func init() {
	rand.Seed(time.Now().UnixNano())
}

type PaymentActivity struct{}

func (a *PaymentActivity) ProcessPaymentActivity(ctx context.Context, paymentID string, amount float64) (string, error) {
	handler := activity.GetMetricsHandler(ctx)
	logger := activity.GetLogger(ctx)

	handler.Counter("payment_activity_invocations_total").Inc(1)
	logger.Info("ProcessPaymentActivity started", "PaymentID", paymentID, "Amount", amount)

	// Failure Injection
	failureRateStr := os.Getenv("INJECT_FAILURE_RATE")
	if failureRateStr != "" {
		failureRate, err := strconv.ParseFloat(failureRateStr, 64)
		if err == nil && failureRate >= 0 && failureRate <= 1 {
			if rand.Float64() < failureRate {
				logger.Warn("Injecting simulated payment gateway failure", "PaymentID", paymentID)
				handler.Counter("payment_activity_failure_simulated_total").Inc(1)
				return "", errors.New("simulated payment gateway failure")
			}
		} else if err != nil {
			logger.Error("Invalid INJECT_FAILURE_RATE value, proceeding with normal execution.", "error", err, "value", failureRateStr)
		} else if !(failureRate >= 0 && failureRate <= 1) {
			logger.Error("INJECT_FAILURE_RATE value out of range (0.0-1.0), proceeding with normal execution.", "value", failureRateStr)
		}
	}

	logger.Info("Processing payment...", "PaymentID", paymentID)
	// Simulate work
	time.Sleep(1 * time.Second)

	logger.Info("Payment processed successfully", "PaymentID", paymentID)
	handler.Counter("payment_activity_success_total").Inc(1)
	// Incrementing amount here. Note: This will overcount if the activity is retried,
	// as activities should be idempotent and this metric is recorded on each successful attempt.
	// For a production system, consider a more robust way to track total processed value
	// that accounts for retries, e.g., by the workflow after final success.
	handler.Counter("payment_amount_processed_total").Inc(int64(amount))
	return fmt.Sprintf("Payment for ID %s processed successfully for amount %.2f", paymentID, amount), nil
}
