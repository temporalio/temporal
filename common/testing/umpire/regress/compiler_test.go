package regress_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire/regress"
)

var (
	taskType  = regress.EntityType("Task")
	taskState = regress.ValueType("TaskState")

	taskStateOutcome = regress.OutcomeSchema(
		"task.state",
		regress.SymbolParameter("task", taskType),
		regress.LiteralParameter("state", taskState),
	)
	createTask = regress.ActionSchema(
		"task.create",
		regress.SymbolParameter("task", taskType),
	)
	startTask = regress.ActionSchema(
		"task.start",
		regress.SymbolParameter("task", taskType),
	)
	finishTask = regress.ActionSchema(
		"task.finish",
		regress.SymbolParameter("task", taskType),
	)
	finishTaskDirect = regress.ActionSchema(
		"task.finish_direct",
		regress.SymbolParameter("task", taskType),
	)
)

func TestCompileOnePathSelectsCanonicalShortestRouteAndResources(t *testing.T) {
	domain := newTaskDomain(t)
	plan := regress.OnePath(
		regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done")),
	)

	suite, err := regress.Compile(plan, domain, regress.Profile{
		Name:         "local",
		Capabilities: []string{"workers"},
	})
	require.NoError(t, err)
	require.Equal(t, "fake/v1", suite.ModelVersion)
	require.Equal(t, "local", suite.Profile.Name)
	require.Len(t, suite.Paths, 1)
	require.Equal(t, []regress.CompletedAction{
		{Name: "task.create", Arguments: []regress.Argument{regress.Symbol("job")}},
		{Name: "task.finish_direct", Arguments: []regress.Argument{regress.Symbol("job")}},
	}, suite.Paths[0].Actions)
	require.Equal(t, []regress.CompletedResource{{Name: "worker"}}, suite.Paths[0].Resources)
	require.Equal(t, 1, suite.PathCount)
}

func TestCompileStartsFromGroundedObservedState(t *testing.T) {
	domain := newTaskDomain(t)
	profile := regress.Profile{
		Name:         "local",
		Capabilities: []string{"workers"},
		ObservedFacts: []regress.CompletedAtom{{
			Predicate: "task.state",
			Arguments: []regress.Argument{regress.Symbol("job"), regress.Literal("done")},
		}},
		ObservedBindings: regress.Bindings{"job": "existing-job-id"},
	}

	suite, err := regress.Compile(
		regress.OnePath(regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done"))),
		domain,
		profile,
	)
	require.NoError(t, err)
	require.Empty(t, suite.Paths[0].Actions)
	require.Equal(t, regress.Bindings{"job": "existing-job-id"}, suite.Paths[0].Bindings)
}

func TestCompileAllPathsEnumeratesEverySemanticRoute(t *testing.T) {
	domain := newTaskDomain(t)
	plan := regress.AllPaths(
		regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done")),
	)

	suite, err := regress.Compile(plan, domain, regress.Profile{
		Name:         "local",
		Capabilities: []string{"workers"},
	})
	require.NoError(t, err)
	require.Equal(t, 2, suite.PathCount)
	require.Equal(t, [][]string{
		{"task.create", "task.finish_direct"},
		{"task.create", "task.start", "task.finish"},
	}, completedActionNames(suite.Paths))
}

func TestCompilePinnedActionNarrowsRoutes(t *testing.T) {
	domain := newTaskDomain(t)
	plan := regress.AllPaths(
		regress.Action(startTask, regress.Symbol("job")),
		regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done")),
	)

	suite, err := regress.Compile(plan, domain, regress.Profile{
		Name:         "local",
		Capabilities: []string{"workers"},
	})
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"task.create", "task.start", "task.finish"},
	}, completedActionNames(suite.Paths))
}

func TestCompileRejectsUnavailableEnvironmentCapability(t *testing.T) {
	domain := newTaskDomain(t)
	plan := regress.OnePath(
		regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done")),
	)

	_, err := regress.Compile(plan, domain, regress.Profile{Name: "remote"})
	require.ErrorIs(t, err, regress.ErrUnavailableEnvironmentCapability)
	require.Contains(t, err.Error(), "workers")
}

func TestCompileAllPathsKeepsMeaningfulAnyOrderRaces(t *testing.T) {
	domain := regress.NewDomain("race/v1")
	left := regress.ActionSchema("race.left")
	right := regress.ActionSchema("race.right")
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: left}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: right}))

	suite, err := regress.Compile(regress.AllPaths(regress.AnyOrder(
		regress.Action(left),
		regress.Action(right),
	)), domain, regress.Profile{Name: "fake"})
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"race.left", "race.right"},
		{"race.right", "race.left"},
	}, completedActionNames(suite.Paths))
}

func TestCompileAllPathsReducesDeclaredIndependentPermutations(t *testing.T) {
	domain := regress.NewDomain("race/v1")
	left := regress.ActionSchema("race.left")
	right := regress.ActionSchema("race.right")
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema:        left,
		IndependentOf: []string{"race.right"},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema:        right,
		IndependentOf: []string{"race.left"},
	}))

	suite, err := regress.Compile(regress.AllPaths(regress.AnyOrder(
		regress.Action(left),
		regress.Action(right),
	)), domain, regress.Profile{Name: "fake"})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"race.left", "race.right"}}, completedActionNames(suite.Paths))
}

func TestCompileAllPathsReturnsNoPartialSuiteAtExplicitLimit(t *testing.T) {
	domain := regress.NewDomain("race/v1")
	left := regress.ActionSchema("race.left")
	right := regress.ActionSchema("race.right")
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: left}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: right}))

	suite, err := regress.Compile(regress.AllPaths(regress.AnyOrder(
		regress.Action(left),
		regress.Action(right),
	)), domain, regress.Profile{
		Name:   "fake",
		Limits: regress.CompileLimits{MaxPaths: 1},
	})
	require.ErrorIs(t, err, regress.ErrIncompleteAllPaths)
	require.Empty(t, suite.Paths)
	require.Contains(t, err.Error(), "2 paths")
}

func TestCompileRejectsMissingActionBeforeSearch(t *testing.T) {
	missing := regress.ActionSchema("missing.action")

	_, err := regress.Compile(
		regress.OnePath(regress.Action(missing)),
		regress.NewDomain("empty/v1"),
		regress.Profile{Name: "fake"},
	)
	require.ErrorIs(t, err, regress.ErrMissingModelCapability)
	compileErr := new(regress.CompileError)
	require.ErrorAs(t, err, &compileErr)
	require.Equal(t, 1, compileErr.Source)
}

func TestCompileRejectsLiteralWithoutStableEncoding(t *testing.T) {
	domain := regress.NewDomain("literal/v1")
	setValue := regress.ActionSchema(
		"literal.set",
		regress.LiteralParameter("value", regress.ValueType("Value")),
	)
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema:    setValue,
		Variables: []regress.Variable{{Name: "value", Type: regress.ValueType("Value")}},
	}))

	_, err := regress.Compile(
		regress.OnePath(regress.Action(setValue, regress.Literal(func() {}))),
		domain,
		regress.Profile{Name: "fake"},
	)
	require.ErrorIs(t, err, regress.ErrInvalidInstruction)
	require.ErrorContains(t, err, "stable encoding")
}

func TestCompileScopesPoliciesAndTheirResources(t *testing.T) {
	domain := regress.NewDomain("policy/v1")
	drop := regress.PolicySchema(
		"rpc.drop",
		regress.LiteralParameter("rpc", regress.ValueType("RPC")),
	)
	cancel := regress.ActionSchema("operation.cancel")
	require.NoError(t, domain.AddResource(regress.ResourceCapability{
		Name:     "fault-injector",
		Requires: []string{"faults"},
	}))
	require.NoError(t, domain.AddPolicy(regress.PolicyCapability{
		Schema:    drop,
		Resources: []string{"fault-injector"},
		Requires:  []string{"faults"},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: cancel}))

	suite, err := regress.Compile(regress.OnePath(regress.During(
		regress.Policy(drop, regress.Literal("CancelNexusOperation")),
		regress.Action(cancel),
	)), domain, regress.Profile{Name: "local", Capabilities: []string{"faults"}})
	require.NoError(t, err)
	require.Equal(t, []regress.CompletedPolicy{{
		Name:      "rpc.drop",
		Arguments: []regress.Argument{regress.Literal("CancelNexusOperation")},
		Start:     0,
		End:       1,
	}}, suite.Paths[0].Policies)
	require.Equal(t, []regress.CompletedResource{{Name: "fault-injector"}}, suite.Paths[0].Resources)
}

func TestCompileRejectsMissingPolicyCapability(t *testing.T) {
	domain := regress.NewDomain("policy/v1")
	drop := regress.PolicySchema("rpc.drop")
	cancel := regress.ActionSchema("operation.cancel")
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: cancel}))

	_, err := regress.Compile(regress.OnePath(regress.During(
		regress.Policy(drop),
		regress.Action(cancel),
	)), domain, regress.Profile{Name: "local"})
	require.ErrorIs(t, err, regress.ErrMissingModelCapability)
}

func TestCompileFlowsProjectedValuesToDependentActions(t *testing.T) {
	tokenType := regress.ValueType("Token")
	workflowToken := regress.ProjectionSchema(
		"workflow.token",
		tokenType,
		regress.SymbolParameter("workflow", workflowRun),
	)
	tokenConsumed := regress.OutcomeSchema(
		"token.consumed",
		regress.SymbolParameter("token", tokenType),
	)
	issue := regress.ActionSchema(
		"token.issue",
		regress.SymbolParameter("workflow", workflowRun),
	)
	consume := regress.ActionSchema(
		"token.consume",
		regress.SymbolParameter("token", tokenType),
	)
	domain := regress.NewDomain("values/v1")
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: workflowToken}))
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: tokenConsumed}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: issue,
		Variables: []regress.Variable{
			{Name: "workflow", Type: workflowRun, Binding: regress.FreshBinding},
			{Name: "token", Type: tokenType, Binding: regress.ObservedBinding},
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("workflow.token", regress.TemplateVar("workflow"), regress.TemplateVar("token")),
		},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: consume,
		Variables: []regress.Variable{
			{Name: "workflow", Type: workflowRun},
			{Name: "token", Type: tokenType},
		},
		Preconditions: []regress.AtomTemplate{
			regress.Atom("workflow.token", regress.TemplateVar("workflow"), regress.TemplateVar("token")),
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("token.consumed", regress.TemplateVar("token")),
		},
	}))

	suite, err := regress.Compile(regress.OnePath(
		regress.Bind("callback-token", regress.Project(workflowToken, regress.Symbol("handler"))),
		regress.Action(consume, regress.Symbol("callback-token")),
	), domain, regress.Profile{Name: "fake"})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"token.issue", "token.consume"}}, completedActionNames(suite.Paths))
}

func TestCompileRejectsValueProducerOrderedAfterConsumer(t *testing.T) {
	tokenType := regress.ValueType("Token")
	workflowToken := regress.ProjectionSchema(
		"workflow.token",
		tokenType,
		regress.SymbolParameter("workflow", workflowRun),
	)
	consume := regress.ActionSchema(
		"token.consume",
		regress.SymbolParameter("token", tokenType),
	)

	_, err := regress.Normalize(regress.OnePath(
		regress.Action(consume, regress.Symbol("callback-token")),
		regress.Bind("callback-token", regress.Project(workflowToken, regress.Symbol("handler"))),
	))
	require.ErrorIs(t, err, regress.ErrContradictoryOrdering)
}

func TestCompileCyclicDomainEnumeratesSimplePathsAndExplicitRepeats(t *testing.T) {
	domain := regress.NewDomain("cycle/v1")
	stateSchema := regress.OutcomeSchema("cycle.state", regress.SymbolParameter("entity", taskType), regress.LiteralParameter("state", taskState))
	create := regress.ActionSchema("cycle.create", regress.SymbolParameter("entity", taskType))
	toB := regress.ActionSchema("cycle.to_b", regress.SymbolParameter("entity", taskType))
	toA := regress.ActionSchema("cycle.to_a", regress.SymbolParameter("entity", taskType))
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: stateSchema, ExclusiveBy: []int{0}}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: create, Variables: []regress.Variable{{Name: "entity", Type: taskType, Binding: regress.FreshBinding}}, Effects: []regress.AtomTemplate{regress.Atom("cycle.state", regress.TemplateVar("entity"), regress.TemplateLiteral("a"))}}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: toB, Variables: []regress.Variable{{Name: "entity", Type: taskType}}, Preconditions: []regress.AtomTemplate{regress.Atom("cycle.state", regress.TemplateVar("entity"), regress.TemplateLiteral("a"))}, Effects: []regress.AtomTemplate{regress.Atom("cycle.state", regress.TemplateVar("entity"), regress.TemplateLiteral("b"))}}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{Schema: toA, Variables: []regress.Variable{{Name: "entity", Type: taskType}}, Preconditions: []regress.AtomTemplate{regress.Atom("cycle.state", regress.TemplateVar("entity"), regress.TemplateLiteral("b"))}, Effects: []regress.AtomTemplate{regress.Atom("cycle.state", regress.TemplateVar("entity"), regress.TemplateLiteral("a"))}}))

	suite, err := regress.Compile(regress.AllPaths(regress.Outcome(stateSchema, regress.Symbol("item"), regress.Literal("b"))), domain, regress.Profile{Name: "fake"})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"cycle.create", "cycle.to_b"}}, completedActionNames(suite.Paths))

	repeated, err := regress.Compile(regress.OnePath(
		regress.Action(toB, regress.Symbol("item")),
		regress.Action(toA, regress.Symbol("item")),
		regress.Action(toB, regress.Symbol("item")),
	), domain, regress.Profile{Name: "fake"})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"cycle.create", "cycle.to_b", "cycle.to_a", "cycle.to_b"}}, completedActionNames(repeated.Paths))
}

func TestCompileUnreachableOutcomeReportsStructuredCausalGap(t *testing.T) {
	domain := regress.NewDomain("gap/v1")
	ready := regress.OutcomeSchema("gap.ready", regress.SymbolParameter("task", taskType))
	done := regress.OutcomeSchema("gap.done", regress.SymbolParameter("task", taskType))
	finish := regress.ActionSchema("gap.finish", regress.SymbolParameter("task", taskType))
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: ready}))
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: done}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema:        finish,
		Variables:     []regress.Variable{{Name: "task", Type: taskType}},
		Preconditions: []regress.AtomTemplate{regress.Atom("gap.ready", regress.TemplateVar("task"))},
		Effects:       []regress.AtomTemplate{regress.Atom("gap.done", regress.TemplateVar("task"))},
	}))

	_, err := regress.Compile(regress.OnePath(regress.Outcome(done, regress.Symbol("job"))), domain, regress.Profile{Name: "fake"})
	var compileErr *regress.CompileError
	require.ErrorAs(t, err, &compileErr)
	require.Equal(t, &regress.CompileError{
		Category:     regress.ErrorUnreachableOutcome,
		Source:       1,
		Detail:       "cannot satisfy gap.done($job); candidate capabilities: gap.finish",
		Predicate:    "gap.done($job)",
		Candidates:   []string{"gap.finish"},
		MissingChain: []string{"gap.done($job)", "gap.ready($job)"},
	}, compileErr)
}

func TestCompileRecordsPlanName(t *testing.T) {
	domain := newTaskDomain(t)
	suite, err := regress.Compile(regress.Named("task-completes", regress.OnePath(
		regress.Outcome(taskStateOutcome, regress.Symbol("job"), regress.Literal("done")),
	)), domain, regress.Profile{Name: "local", Capabilities: []string{"workers"}})
	require.NoError(t, err)
	require.Equal(t, "task-completes", suite.Name)
}

func TestDomainRejectsPredicateVariableTypeMismatch(t *testing.T) {
	domain := regress.NewDomain("types/v1")
	predicate := regress.OutcomeSchema("typed.state", regress.SymbolParameter("task", taskType))
	action := regress.ActionSchema("typed.create", regress.SymbolParameter("workflow", workflowRun))
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{Schema: predicate}))

	err := domain.AddAction(regress.ActionCapability{
		Schema:    action,
		Variables: []regress.Variable{{Name: "workflow", Type: workflowRun}},
		Effects:   []regress.AtomTemplate{regress.Atom("typed.state", regress.TemplateVar("workflow"))},
	})
	require.ErrorContains(t, err, "of type Entity<WorkflowRun>")
	require.ErrorContains(t, err, "of type Entity<Task>")
}

func TestCompileRejectsStaleTypedConstructorSchema(t *testing.T) {
	domain := newTaskDomain(t)
	stale := regress.OutcomeSchema(
		"task.state",
		regress.SymbolParameter("task", workflowRun),
		regress.LiteralParameter("state", taskState),
	)

	_, err := regress.Compile(
		regress.OnePath(regress.Outcome(stale, regress.Symbol("job"), regress.Literal("done"))),
		domain,
		regress.Profile{Name: "local", Capabilities: []string{"workers"}},
	)
	require.ErrorIs(t, err, regress.ErrMissingModelCapability)
	require.ErrorContains(t, err, "requires Entity<Task>")
}

func newTaskDomain(t *testing.T) *regress.Domain {
	t.Helper()

	domain := regress.NewDomain("fake/v1")
	require.NoError(t, domain.AddPredicate(regress.PredicateCapability{
		Schema:      taskStateOutcome,
		ExclusiveBy: []int{0},
	}))
	require.NoError(t, domain.AddResource(regress.ResourceCapability{
		Name:     "worker",
		Requires: []string{"workers"},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: createTask,
		Variables: []regress.Variable{
			{Name: "task", Type: taskType, Binding: regress.FreshBinding},
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("ready")),
		},
		Resources: []string{"worker"},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: startTask,
		Variables: []regress.Variable{
			{Name: "task", Type: taskType},
		},
		Preconditions: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("ready")),
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("running")),
		},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: finishTask,
		Variables: []regress.Variable{
			{Name: "task", Type: taskType},
		},
		Preconditions: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("running")),
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("done")),
		},
	}))
	require.NoError(t, domain.AddAction(regress.ActionCapability{
		Schema: finishTaskDirect,
		Variables: []regress.Variable{
			{Name: "task", Type: taskType},
		},
		Preconditions: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("ready")),
		},
		Effects: []regress.AtomTemplate{
			regress.Atom("task.state", regress.TemplateVar("task"), regress.TemplateLiteral("done")),
		},
	}))
	return domain
}

func completedActionNames(paths []regress.CompletedPath) [][]string {
	result := make([][]string, len(paths))
	for pathIndex, path := range paths {
		for _, action := range path.Actions {
			result[pathIndex] = append(result[pathIndex], action.Name)
		}
	}
	return result
}
