package umpire2

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	coreregress "go.temporal.io/server/common/testing/umpire/regress"
	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/tests/umpire2/internal/assurance"
	"go.temporal.io/server/tests/umpire2/internal/model"
	"go.temporal.io/server/tests/umpire2/internal/protocol"
)

// Protocol is the immutable compiled Temporal protocol exposed through its supported operations.
type Protocol struct {
	compiled *protocol.Protocol
}

// CoverageCatalogOptions selects declarations for a semantic coverage denominator.
type CoverageCatalogOptions = protocol.CoverageCatalogOptions

const (
	WorkflowType       = model.WorkflowType
	WorkflowRunType    = model.WorkflowRunType
	NexusOperationType = model.NexusOperationType

	WorkflowCompleted         = model.WorkflowCompleted
	WorkflowRunCompleted      = model.WorkflowRunCompleted
	WorkflowRunContinuedAsNew = model.WorkflowRunContinuedAsNew
	NexusScheduled            = model.NexusScheduled
	NexusStarted              = model.NexusStarted
	NexusSucceeded            = model.NexusSucceeded
	NexusSucceed              = model.NexusSucceed
	NexusFail                 = model.NexusFail
	NexusTerminate            = model.NexusTerminate
	ProtocolAtomicTarget      = protocol.ProtocolAtomicTarget
)

// DefaultProtocol compiles the canonical Temporal protocol.
func DefaultProtocol() (*Protocol, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return nil, err
	}
	return &Protocol{compiled: compiled}, nil
}

// Lifecycle returns a fresh lifecycle for one monitored Temporal entity type.
func (p *Protocol) Lifecycle(entityType umpire.EntityType) (*umpire.Lifecycle, bool) {
	if p == nil || p.compiled == nil {
		return nil, false
	}
	return p.compiled.Lifecycle(entityType)
}

// PlanTo computes lifecycle routes to a structural target.
func (p *Protocol) PlanTo(
	entityType umpire.EntityType,
	target string,
	mode umpire.RouteMode,
	constraints umpire.Constraints,
	options ...umpire.Option,
) (*umpire.Plan, error) {
	if p == nil || p.compiled == nil {
		return nil, fmt.Errorf("protocol: protocol is nil")
	}
	return p.compiled.PlanTo(entityType, target, mode, constraints, options...)
}

// PlanEdge assembles the executable actions for one exact lifecycle edge.
func (p *Protocol) PlanEdge(
	entityType umpire.EntityType,
	from string,
	event string,
	hosting umpire.Hosting,
) ([]umpire.Action, error) {
	if p == nil || p.compiled == nil {
		return nil, fmt.Errorf("protocol: protocol is nil")
	}
	return p.compiled.PlanEdge(entityType, from, event, hosting)
}

// NewCoverage creates a semantic coverage collector derived from this protocol.
func (p *Protocol) NewCoverage(enabled bool, options CoverageCatalogOptions) (*umpire.Coverage, error) {
	if p == nil || p.compiled == nil {
		return nil, fmt.Errorf("protocol: protocol is nil")
	}
	return p.compiled.NewCoverage(enabled, options)
}

// CompileRegression compiles sparse intent against this protocol's typed regression vocabulary.
func (p *Protocol) CompileRegression(plan coreregress.Plan, profile coreregress.Profile) (coreregress.Suite, error) {
	if p == nil || p.compiled == nil {
		return coreregress.Suite{}, fmt.Errorf("protocol regression: protocol is nil")
	}
	return p.compiled.CompileRegression(plan, profile)
}

func (p *Protocol) planSettlingEdges(
	entityType umpire.EntityType,
	hosting umpire.Hosting,
) (protocol.SettlingActionPlans, error) {
	if p == nil || p.compiled == nil {
		return protocol.SettlingActionPlans{}, fmt.Errorf("protocol: protocol is nil")
	}
	return p.compiled.PlanSettlingEdges(entityType, hosting)
}

func (p *Protocol) sampleSettlingPlan(
	entityType umpire.EntityType,
	hosting umpire.Hosting,
	seed int64,
) (protocol.SettlingActionPlan, error) {
	if p == nil || p.compiled == nil {
		return protocol.SettlingActionPlan{}, fmt.Errorf("protocol: protocol is nil")
	}
	return p.compiled.SampleSettlingPlan(entityType, hosting, seed)
}

// DefaultVerificationFamily projects the canonical protocol and assurance catalog.
func DefaultVerificationFamily(defaultBound int) (verify.ModelFamily, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("compile default Umpire protocol: %w", err)
	}
	catalog, err := assurance.Default()
	if err != nil {
		return verify.ModelFamily{}, fmt.Errorf("compile default Umpire assurance catalog: %w", err)
	}
	return compiled.VerificationFamily(protocol.VerificationOptions{
		DefaultBound:  defaultBound,
		RuleInventory: catalog.VerificationInventory(),
	})
}

// NewCoverage creates a collector using the canonical protocol's semantic catalog.
func NewCoverage(enabled bool, options CoverageCatalogOptions) (*umpire.Coverage, error) {
	compiled, err := protocol.Default()
	if err != nil {
		return nil, err
	}
	return compiled.NewCoverage(enabled, options)
}
