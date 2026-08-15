package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	umpirefw "go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
)

func (p *regressionPath) Reconcile(ctx context.Context, step coreregress.CompletedStep, bindings coreregress.Bindings) error {
	return p.awaitAtoms(ctx, step.Effects, bindings, true)
}

func (p *regressionPath) Observe(ctx context.Context, milestone coreregress.CompletedMilestone, bindings coreregress.Bindings) error {
	arguments := append([]coreregress.Argument(nil), milestone.Arguments...)
	if milestone.Kind == coreregress.BindingKind {
		arguments = append(arguments, coreregress.Symbol(milestone.Binding))
	}
	return p.awaitAtoms(ctx, []coreregress.CompletedAtom{{Predicate: milestone.Name, Arguments: arguments}}, bindings, true)
}

func (p *regressionPath) CheckSafety(ctx context.Context, _ coreregress.Checkpoint) error {
	if err := p.refreshLinkedExecutions(ctx); err != nil {
		return err
	}
	return violationsError(p.environment.GetMonitor().CheckNamespaceSafety(ctx, p.environment.NamespaceID().String()))
}

func (p *regressionPath) QualifiedVerdicts(_ context.Context, checkpoint coreregress.Checkpoint, violated bool) ([]umpirefw.QualifiedClaim, error) {
	profile := p.environmentProfile
	if profile.Name == "" {
		profile = umpirefw.InProcessProfile()
	}
	claim := umpirefw.QualifyEvidence(
		p.modelVersion,
		"live-regression",
		profile,
		umpirefw.EvidenceRequirement{Property: umpirefw.MonitorSafetyProperty(checkpoint.String()), Sources: []umpirefw.EvidenceSource{umpirefw.InProcessEvidence}},
		umpirefw.ObservedEvidence{Sources: []umpirefw.EvidenceSource{umpirefw.InProcessEvidence}},
		violated,
	)
	return []umpirefw.QualifiedClaim{claim}, nil
}

func (p *regressionPath) ArtifactFacts(_ context.Context) ([]json.RawMessage, error) {
	return p.environment.GetMonitor().ArtifactFacts(p.environment.NamespaceID().String())
}

func (p *regressionPath) refreshLinkedExecutions(ctx context.Context) error {
	p.mu.RLock()
	pairs := make(map[string]string, len(p.activityOps))
	runs := make(map[string]string, len(p.activityRuns))
	for activityID, operationID := range p.activityOps {
		pairs[activityID] = operationID
		runs[activityID] = p.activityRuns[activityID]
	}
	p.mu.RUnlock()
	for activityID, operationID := range pairs {
		if _, err := p.environment.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  p.environment.Namespace().String(),
			ActivityId: activityID,
			RunId:      runs[activityID],
		}); err != nil {
			return fmt.Errorf("refresh linked activity %s: %w", activityID, err)
		}
		if _, err := p.environment.FrontendClient().DescribeNexusOperationExecution(ctx, &workflowservice.DescribeNexusOperationExecutionRequest{
			Namespace:   p.environment.Namespace().String(),
			OperationId: operationID,
			RunId:       p.context.RunID,
		}); err != nil {
			return fmt.Errorf("refresh linked Nexus operation %s: %w", operationID, err)
		}
	}
	return nil
}

func (p *regressionPath) Quiesce(ctx context.Context) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	generation := p.environment.GetMonitor().Snapshot(p.environment.NamespaceID().String()).Generation
	stable := 0
	for stable < 3 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			current := p.environment.GetMonitor().Snapshot(p.environment.NamespaceID().String()).Generation
			if current == generation {
				stable++
			} else {
				generation = current
				stable = 0
			}
		}
	}
	return nil
}

func (p *regressionPath) ResolveLiveness(ctx context.Context) error {
	return violationsError(p.environment.GetMonitor().CheckNamespace(ctx, p.environment.NamespaceID().String()))
}

func (p *regressionPath) Close(ctx context.Context) error {
	p.context.Cleanup()
	p.environment.GetMonitor().PurgeNamespace(p.environment.NamespaceID().String())
	if p.cleanup != nil {
		return p.cleanup(ctx)
	}
	return nil
}

func violationsError(violations []umpirefw.Violation) error {
	var result []error
	for _, violation := range violations {
		result = append(result, fmt.Errorf("%s: %s (entity=%s state=%s)", violation.Rule, violation.Message, violation.Tags["entity"], violation.Tags["state"]))
	}
	return errors.Join(result...)
}

func (p *regressionPath) awaitAtoms(ctx context.Context, atoms []coreregress.CompletedAtom, bindings coreregress.Bindings, historical bool) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		allSatisfied := true
		var missing []string
		for _, atom := range atoms {
			if !p.atomSatisfied(ctx, atom, bindings, historical) {
				allSatisfied = false
				missing = append(missing, semanticAtomKey(atom))
			}
		}
		if err := p.CheckSafety(ctx, coreregress.ObservationCheckpoint); err != nil {
			return fmt.Errorf("monitor safety during observation: %w", err)
		}
		if allSatisfied {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w waiting for %s; callback observations: %s", ctx.Err(), strings.Join(missing, ", "), p.callbackObservationSummary())
		case <-ticker.C:
		}
	}
}

func (p *regressionPath) callbackObservationSummary() string {
	return p.environment.GetMonitor().ObservationSummary(p.environment.NamespaceID().String())
}
