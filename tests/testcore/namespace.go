package testcore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/primitives"
)

type NamespaceAvailabilityCheck func(*workflowservice.DescribeNamespaceResponse) error

func (tc *TestCluster) WaitForNamespaceAvailable(
	ctx context.Context,
	namespace string,
	waitTime time.Duration,
	checkInterval time.Duration,
	check NamespaceAvailabilityCheck,
) error {
	deadline := time.NewTimer(waitTime)
	defer deadline.Stop()
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		lastErr = tc.checkNamespaceAvailable(ctx, namespace, check)
		if lastErr == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("namespace %q did not become available before context deadline: %w", namespace, errors.Join(ctx.Err(), lastErr))
		case <-deadline.C:
			return fmt.Errorf("namespace %q did not become available before deadline: %w", namespace, lastErr)
		case <-ticker.C:
		}
	}
}

func (tc *TestCluster) checkNamespaceAvailable(
	ctx context.Context,
	namespace string,
	check NamespaceAvailabilityCheck,
) error {
	hosts := tc.host.hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All
	if len(hosts) == 0 {
		return errors.New("no frontend gRPC hosts configured")
	}

	var errs []error
	for _, host := range hosts {
		conn, err := tc.host.clients.newConnToAddress(primitives.FrontendService, host)
		if err != nil {
			errs = append(errs, fmt.Errorf("dial frontend %s: %w", host, err))
			continue
		}
		client := workflowservice.NewWorkflowServiceClient(conn)
		resp, err := client.DescribeNamespace(NewContext(ctx), &workflowservice.DescribeNamespaceRequest{
			Namespace: namespace,
		})
		if err == nil && check != nil {
			err = check(resp)
		}
		if closeErr := conn.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close frontend %s: %w", host, closeErr))
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("describe namespace on frontend %s: %w", host, err))
		}
	}
	return errors.Join(errs...)
}
