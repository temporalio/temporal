package standalonenexusop

import (
	"fmt"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Rules is the package-level registry; rule definitions self-register at
// init via rules.Register(...).
var rules umpire.RuleSet

var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	r.CoveragePoint(
		"start-completed-after-dispatch",
		"start task completion had a prior start task dispatch",
		umpire.MinVerified(1),
	)
	r.CoveragePoint(
		"cancel-completed-after-dispatch",
		"cancel task completion had a prior cancel task dispatch",
	)

	return umpire.SafetyRule{
		Name: "nexus-task-causality",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			dispatched := make(map[string]map[TaskKind]bool)
			startPoint := umpire.CoveragePoint{Name: "start-completed-after-dispatch"}
			cancelPoint := umpire.CoveragePoint{Name: "cancel-completed-after-dispatch"}

			for _, rec := range history {
				event, ok := rec.Fact.(*TaskEvent)
				if !ok {
					continue
				}
				if event.Outcome == TaskOutcomeDispatched {
					if dispatched[event.OperationID] == nil {
						dispatched[event.OperationID] = make(map[TaskKind]bool)
					}
					dispatched[event.OperationID][event.Kind] = true
					continue
				}
				point := startPoint
				if event.Kind == TaskKindCancel {
					point = cancelPoint
				}
				ctx.Check(
					point,
					event.OperationID,
					dispatched[event.OperationID][event.Kind],
					fmt.Sprintf("%s %s without prior dispatch", event.Kind, event.Outcome),
					map[string]string{
						"operationID": event.OperationID,
						"kind":        string(event.Kind),
						"outcome":     string(event.Outcome),
					},
				)
			}
		},
		Examples: func(t *parallelsuite.T) {
			// Functional examples live next to a real cluster setup and
			// exercise the same code paths the property test does. They
			// document concrete scenarios the rule cares about and serve
			// as fast, deterministic regression tests.
			//
			// Each t.Run subtest auto-parallelizes — no t.Parallel needed:
			//
			//     t.Run("respond-without-poll-fails", func(t *parallelsuite.T) {
			//         env := newEnv(t.T)
			//         _, err := env.Client().RespondNexusTaskCompleted(...)
			//         require.Error(t, err)
			//     })
		},
	}
})

var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	completedOperationHadStartCompletion := r.CoveragePoint(
		"completed-operation-had-start-completion",
		"completed terminal state had a completed start task",
		umpire.MinVerified(1),
	)
	canceledOperationHadCancelCompletion := r.CoveragePoint(
		"canceled-operation-had-cancel-completion",
		"canceled terminal state had a completed cancel task",
	)

	return umpire.SafetyRule{
		Name: "nexus-terminal-consistency",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			startCompleted := make(map[string]bool)
			cancelCompleted := make(map[string]bool)
			terminalStatus := make(map[string]Status)

			for _, rec := range history {
				switch event := rec.Fact.(type) {
				case *TaskEvent:
					if event.Outcome != TaskOutcomeCompleted {
						continue
					}
					switch event.Kind {
					case TaskKindStart:
						startCompleted[event.OperationID] = true
					case TaskKindCancel:
						cancelCompleted[event.OperationID] = true
					default:
					}
				case *umpire.Transition[Status]:
					if IsTerminal(event.To) {
						terminalStatus[event.EntityID] = event.To
					}
				default:
				}
			}

			for operationID, status := range terminalStatus {
				switch status {
				case StatusCompleted:
					ctx.Check(
						completedOperationHadStartCompletion,
						operationID,
						startCompleted[operationID],
						"operation completed in model but no start completion observed",
						map[string]string{"operationID": operationID},
					)
				case StatusCanceled:
					ctx.Check(
						canceledOperationHadCancelCompletion,
						operationID,
						cancelCompleted[operationID],
						"operation canceled in model but no cancel completion observed",
						map[string]string{"operationID": operationID},
					)
				default:
				}
			}
		},
	}
})

var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	terminalOperationHadNoLateDispatch := r.CoveragePoint(
		"terminal-operation-had-no-late-dispatch",
		"terminal operation had no task dispatch after the terminal transition",
		umpire.MinVerified(1),
	)

	return umpire.SafetyRule{
		Name: "nexus-no-post-terminal-dispatch",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			point := terminalOperationHadNoLateDispatch
			lastTerminalSeq := umpire.LastTransitionSeqTo(history, IsTerminal)
			postTerminalDispatch := make(map[string]bool)

			for operationID := range lastTerminalSeq {
				ctx.Reached(point, operationID)
			}
			for _, rec := range history {
				event, ok := rec.Fact.(*TaskEvent)
				if !ok || event.Outcome != TaskOutcomeDispatched {
					continue
				}
				termSeq, hasTerminal := lastTerminalSeq[event.OperationID]
				if !hasTerminal || rec.Seq <= termSeq {
					continue
				}
				postTerminalDispatch[event.OperationID] = true
				ctx.ViolatePoint(
					point,
					"task dispatched after operation reached terminal state",
					map[string]string{
						"operationID": event.OperationID,
						"kind":        string(event.Kind),
						"eventSeq":    fmt.Sprintf("%d", rec.Seq),
						"terminalSeq": fmt.Sprintf("%d", termSeq),
					},
				)
			}
			for operationID := range lastTerminalSeq {
				if !postTerminalDispatch[operationID] {
					ctx.Verified(point, operationID)
				}
			}
		},
	}
})

var _ = rules.Register(func(r *umpire.RuleBuilder) umpire.SafetyRule {
	return umpire.SafetyRule{
		Name: "nexus-describe-runid-stability",
		Check: func(ctx *umpire.RuleContext, history []*umpire.Record) {
			const method = workflowservice.WorkflowService_DescribeNexusOperationExecution_FullMethodName
			firstSeen := make(map[string]string)
			for _, rec := range history {
				call, ok := rec.Fact.(*umpire.ObservedCall)
				if !ok || call.Method != method || call.Err != nil {
					continue
				}
				req, _ := call.Req.(*workflowservice.DescribeNexusOperationExecutionRequest)
				resp, _ := call.Resp.(*workflowservice.DescribeNexusOperationExecutionResponse)
				if req == nil || resp == nil {
					continue
				}
				opID := req.GetOperationId()
				runID := resp.GetRunId()
				if opID == "" || runID == "" {
					continue
				}
				if prev, seen := firstSeen[opID]; seen {
					if prev != runID {
						ctx.Violate(
							"DescribeNexusOperationExecution returned different runIDs for the same operationID",
							map[string]string{
								"operationID": opID,
								"first":       prev,
								"current":     runID,
							},
						)
					}
					continue
				}
				firstSeen[opID] = runID
			}
		},
	}
})

// The rules below are placeholders that mirror the structure of
// tests/nexus_standalone_test.go: each top-level Test* function maps to a
// rule, and each subtest there becomes a TODO line inside that rule's
// Examples. Filling a TODO with real assertions ports that subtest into
// the property-test framework. Until then the closures are no-ops, which
// is legal: the framework requires Check OR Examples to be non-nil and an
// empty Examples body satisfies that.

// nexus-start-properties — TestStartStandaloneNexusOperation (line 51).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-start-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO StartAndDescribe         — line 52
		// TODO IncludeInput             — line 139
		// TODO Validation               — line 153
		// TODO IDConflictPolicyFail     — line 163
		// TODO IDConflictPolicyUseExisting — line 198
	},
})

// nexus-describe-properties — TestDescribeStandaloneNexusOperation (line 220).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-describe-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO NotFound                              — line 221
		// TODO LongPollStateChange                   — line 233
		// TODO LongPollTimeoutReturnsEmptyResponse   — line 313
		// TODO LongPollTimeout/CallerDeadlineNotExceeded — line 344
		// TODO LongPollTimeout/NoCallerDeadline      — line 358
		// TODO IncludeOutcome_Success                — line 374
		// TODO IncludeOutcome_Failure                — line 483
		// TODO IncludeOutcome_Failure/ScheduleToStartTimeout — line 487
		// TODO IncludeOutcome_Failure/ScheduleToCloseTimeout_BeforeStart — line 535
		// TODO Validation                            — line 803
	},
})

// nexus-cancel-properties — TestStandaloneNexusOperationCancel (line 814).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-cancel-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO RequestCancel                              — line 815
		// TODO AlreadyCanceled                            — line 873
		// TODO RequestCancel_ForwardsOriginalNexusHeaders — line 912
		// TODO AlreadyTerminated                          — line 962
		// TODO NotFound                                   — line 993
		// TODO Validation                                 — line 1006
	},
})

// nexus-terminate-properties — TestTerminateStandaloneNexusOperation (line 1018).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-terminate-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO Terminate         — line 1019
		// TODO AlreadyTerminated — line 1056
		// TODO AlreadyCanceled   — line 1098
		// TODO NotFound          — line 1136
		// TODO Validation        — line 1150
	},
})

// nexus-list-properties — TestListStandaloneNexusOperation (line 1162).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-list-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO ListAndVerifyFields                — line 1163
		// TODO ListWithCustomSearchAttributes     — line 1197
		// TODO QueryByMultipleFields              — line 1231
		// TODO QueryBySupportedSearchAttributes   — line 1255
		// TODO PageSizeCapping                    — line 1396
		// TODO InvalidQuery                       — line 1462
		// TODO InvalidSearchAttribute             — line 1473
		// TODO NamespaceNotFound                  — line 1484
	},
})

// nexus-count-properties — TestCountStandaloneNexusOperation (line 1495).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-count-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO CountByOperationID            — line 1496
		// TODO CountByEndpoint               — line 1516
		// TODO CountByExecutionStatus        — line 1538
		// TODO GroupByExecutionStatus        — line 1558
		// TODO CountByCustomSearchAttribute  — line 1587
		// TODO GroupByUnsupportedField       — line 1614
	},
})

// nexus-delete-properties — TestDeleteStandaloneNexusOperation (line 1658).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-delete-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO subtests under TestDeleteStandaloneNexusOperation (line 1658) —
		//   open the source file and copy each subtest name into a TODO line
		//   here when porting begins.
	},
})

// nexus-poll-properties — TestStandaloneNexusOperationPoll (line 1749).
var _ = rules.Register(umpire.SafetyRule{
	Name: "nexus-poll-properties",
	Examples: func(t *parallelsuite.T) {
		// TODO subtests under TestStandaloneNexusOperationPoll (line 1749) —
		//   port one subtest at a time into a real example.
	},
})

// RunExamples runs every rule's example tests as parallel subtests. Use it
// from a top-level Go test to exercise the curated functional examples
// without bringing up the full property-test harness.
func RunExamples(t *testing.T) { rules.RunExamples(t) }
