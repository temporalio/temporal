package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/tests/testcore"
)

// AdminRebuildRaceTestSuite tests race conditions in rebuild operations.
// Run with: go test -race ./tests -run TestAdminRebuildRaceTestSuite
type AdminRebuildRaceTestSuite struct {
	testcore.FunctionalTestBase
}

func TestAdminRebuildRaceTestSuite(t *testing.T) {
	// These tests are designed to detect race conditions with -race flag.
	// They will pass without -race but won't detect races.
	// Run with: go test -race ./tests -run TestAdminRebuildRaceTestSuite
	suite.Run(t, new(AdminRebuildRaceTestSuite))
}

// TestConcurrentRebuildOperations tests that concurrent rebuild operations
// on the same workflow don't cause race conditions or corruption.
// This specifically tests the TOCTOU race where multiple rebuilds might
// read stale state before acquiring locks.
func (s *AdminRebuildRaceTestSuite) TestConcurrentRebuildOperations() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a workflow with some state
	workflowFn := func(ctx workflow.Context) error {
		// Workflow with multiple activities to create non-trivial state
		_ = workflow.Sleep(ctx, 5*time.Second)
		return nil
	}
	s.Worker().RegisterWorkflow(workflowFn)

	workflowID := "race-test-concurrent-rebuild-" + testcore.RandomizeStr("wf")
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 30 * time.Second,
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// Wait for workflow to progress a bit
	time.Sleep(500 * time.Millisecond)

	// Launch multiple concurrent rebuild operations
	const numConcurrent = 10
	var wg sync.WaitGroup
	errors := make(chan error, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Each goroutine tries to rebuild the same workflow
			_, err := s.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
			})
			if err != nil {
				errors <- err
			}
		}(i)
	}

	// Wait for all rebuilds to complete
	wg.Wait()
	close(errors)

	// All rebuilds should succeed without errors
	// (or at least not cause crashes/panics)
	errorCount := 0
	for err := range errors {
		s.T().Logf("Rebuild error: %v", err)
		errorCount++
	}

	// Some errors might be acceptable (e.g., context deadlines)
	// but there should be no panics or data races
	s.T().Logf("Total errors out of %d concurrent rebuilds: %d", numConcurrent, errorCount)

	// Verify the workflow state is still consistent after all concurrent rebuilds
	descResp, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err, "Should be able to describe workflow after concurrent rebuilds")
	s.NotNil(descResp.DatabaseMutableState, "Mutable state should not be corrupted")
}

// TestRebuildWhileWorkflowProgressing tests the race condition where
// a rebuild happens while the workflow is actively progressing.
// This tests the scenario where the workflow state changes between
// reading the rebuildSpec and acquiring the lock.
func (s *AdminRebuildRaceTestSuite) TestRebuildWhileWorkflowProgressing() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create a workflow that continuously progresses
	workflowFn := func(ctx workflow.Context) error {
		for i := 0; i < 10; i++ {
			_ = workflow.Sleep(ctx, 100*time.Millisecond)
		}
		return nil
	}
	s.Worker().RegisterWorkflow(workflowFn)

	workflowID := "race-test-progressing-" + testcore.RandomizeStr("wf")
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 30 * time.Second,
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// Wait for workflow to start
	time.Sleep(200 * time.Millisecond)

	// Try to rebuild while the workflow is progressing
	// This tests the TOCTOU race where state might change between
	// getRebuildSpecFromMutableState() and lock acquisition
	var wg sync.WaitGroup
	const numRebuilds = 5

	for i := 0; i < numRebuilds; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
				Namespace: s.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      runID,
				},
			})
		}()
		// Stagger the rebuilds slightly
		time.Sleep(50 * time.Millisecond)
	}

	wg.Wait()

	// Workflow should complete successfully despite concurrent rebuilds
	var result string
	err = workflowRun.Get(ctx, &result)
	s.NoError(err, "Workflow should complete successfully")

	// Final state should be consistent
	descResp, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)
	s.NotNil(descResp.DatabaseMutableState)
}

// TestConcurrentCacheAccessDuringRebuild tests for races between
// cache operations (get/release/clear) during rebuild.
// This specifically targets the Clear-after-unlock race.
func (s *AdminRebuildRaceTestSuite) TestConcurrentCacheAccessDuringRebuild() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowFn := func(ctx workflow.Context) error {
		_ = workflow.Sleep(ctx, 5*time.Second)
		return nil
	}
	s.Worker().RegisterWorkflow(workflowFn)

	workflowID := "race-test-cache-" + testcore.RandomizeStr("wf")
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.TaskQueue(),
		WorkflowRunTimeout: 30 * time.Second,
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	time.Sleep(200 * time.Millisecond)

	// Perform multiple operations that access the cache concurrently
	var wg sync.WaitGroup
	const numOperations = 15

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			switch index % 3 {
			case 0:
				// Rebuild (modifies cache)
				_, _ = s.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: workflowID,
						RunId:      runID,
					},
				})
			case 1:
				// Describe (reads cache)
				_, _ = s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
					Namespace: s.Namespace().String(),
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: workflowID,
						RunId:      runID,
					},
				})
			case 2:
				// Query (reads cache through different path)
				_, _ = s.SdkClient().QueryWorkflow(ctx, workflowID, runID, "__stack_trace")
			}
		}(i)
	}

	wg.Wait()

	// Verify final state is consistent
	descResp, err := s.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)
	s.NotNil(descResp.DatabaseMutableState)
}
