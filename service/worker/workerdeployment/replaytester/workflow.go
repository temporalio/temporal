package replaytester

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

func HelloWorld(ctx workflow.Context) (string, error) {
	workflow.GetLogger(ctx).Info("HelloWorld workflow started")

	// Compute a subject
	var subject string
	if err := workflow.ExecuteActivity(ctx, GetSubject).Get(ctx, &subject); err != nil {
		return "", err
	}

	// Sleep for a while
	if err := workflow.Sleep(ctx, 30*time.Second); err != nil {
		return "", err
	}

	// Return the greeting
	return fmt.Sprintf("Hello %s", subject), nil
}

func GetSubject(ctx context.Context) (string, error) {
	return "World", nil
}
