// Package canary executes behavioral actions inside an explicit production safety envelope.
package canary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/common/testing/umpire"
)

const redacted = "[redacted]"

// SafetyEnvelope is the immutable authority and budget for one canary campaign.
type SafetyEnvelope struct {
	CampaignID              string        `json:"campaignID"`
	Namespace               string        `json:"namespace"`
	Tenant                  string        `json:"tenant"`
	NamespaceIsolated       bool          `json:"namespaceIsolated"`
	TenantIsolated          bool          `json:"tenantIsolated"`
	AllowedActions          []string      `json:"allowedActions"`
	AllowedFaults           []string      `json:"allowedFaults,omitempty"`
	AllowDestructiveActions bool          `json:"allowDestructiveActions,omitempty"`
	AllowDestructiveFaults  bool          `json:"allowDestructiveFaults,omitempty"`
	MaxActions              int           `json:"maxActions"`
	MaxFaults               int           `json:"maxFaults"`
	MaxConcurrent           int           `json:"maxConcurrent"`
	MaxDuration             time.Duration `json:"maxDuration"`
	MaxEvidenceBytes        int           `json:"maxEvidenceBytes"`
	CleanupTimeout          time.Duration `json:"cleanupTimeout"`
	Secrets                 []string      `json:"-"`
}

// Action is one allowlisted, isolated canary operation.
type Action struct {
	Name        string `json:"name"`
	Namespace   string `json:"namespace"`
	Tenant      string `json:"tenant"`
	Fault       bool   `json:"fault,omitempty"`
	Destructive bool   `json:"destructive,omitempty"`
}

// Resource is recovery-safe cleanup metadata created during preparation.
type Resource struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Observation is secret-safe semantic evidence from one action.
type Observation struct {
	Fields            map[string]string `json:"fields,omitempty"`
	InvariantViolated bool              `json:"invariantViolated,omitempty"`
	ObservationLost   bool              `json:"observationLost,omitempty"`
	Error             error             `json:"-"`
}

// PreparationRequest is the exact isolated scope a driver must prepare and later clean.
type PreparationRequest struct {
	CampaignID string `json:"campaignID"`
	Namespace  string `json:"namespace"`
	Tenant     string `json:"tenant"`
}

// Preparation attests the scope and resources actually prepared by a driver.
type Preparation struct {
	Scope        PreparationRequest `json:"scope"`
	Resources    []Resource         `json:"resources,omitempty"`
	CleanupToken string             `json:"-"`
}

// Driver prepares isolation, executes actions, and cleans every owned resource.
// Implementations must stop each method when its context is done; enforcing cancellation at a
// transport or process boundary belongs to the driver.
type Driver interface {
	Prepare(context.Context, PreparationRequest) (Preparation, error)
	Execute(context.Context, Action) Observation
	Cleanup(context.Context, Preparation) error
}

// Request contains one approved canary workload.
type Request struct {
	Environment umpire.EnvironmentProfile `json:"environment"`
	Envelope    SafetyEnvelope            `json:"envelope"`
	Actions     []Action                  `json:"actions"`
	Driver      Driver                    `json:"-"`
}

// AuditRecord records an action or stop decision without payloads or secrets.
type AuditRecord struct {
	Sequence   int    `json:"sequence"`
	Action     string `json:"action,omitempty"`
	Decision   string `json:"decision"`
	Reason     string `json:"reason,omitempty"`
	ErrorClass string `json:"errorClass,omitempty"`
}

// ActionResult is one bounded, redacted execution result.
type ActionResult struct {
	Sequence    int               `json:"sequence"`
	Action      string            `json:"action"`
	Observation map[string]string `json:"observation,omitempty"`
}

// CleanupRecord can be retained for recovery after process interruption.
type CleanupRecord struct {
	CampaignID string     `json:"campaignID"`
	Resources  []Resource `json:"resources,omitempty"`
	Complete   bool       `json:"complete"`
	Error      string     `json:"error,omitempty"`
}

// Result records the enforced safety envelope, audit trail, and cleanup outcome.
type Result struct {
	Environment    umpire.EnvironmentProfile `json:"environment"`
	Envelope       SafetyEnvelope            `json:"envelope"`
	ActionsStarted int                       `json:"actionsStarted"`
	FaultsStarted  int                       `json:"faultsStarted"`
	EvidenceBytes  int                       `json:"evidenceBytes"`
	Actions        []ActionResult            `json:"actions,omitempty"`
	Audit          []AuditRecord             `json:"audit"`
	StopReason     string                    `json:"stopReason,omitempty"`
	Cleanup        CleanupRecord             `json:"cleanup"`
	Complete       bool                      `json:"complete"`
}

// Summary returns a concise secret-free account of execution, stop, and cleanup status.
func (r Result) Summary() string {
	status := "complete"
	if !r.Complete {
		status = "stopped: " + r.StopReason
	}
	cleanup := "complete"
	if !r.Cleanup.Complete {
		cleanup = "incomplete"
	}
	return fmt.Sprintf("canary %s; actions=%d faults=%d evidenceBytes=%d cleanup=%s", status, r.ActionsStarted, r.FaultsStarted, r.EvidenceBytes, cleanup)
}

var ErrUnsafeRequest = errors.New("unsafe canary request")

// Run preflights all authority before allocation, reserves hard count and evidence budgets, and
// supplies execution and cleanup deadlines to a context-compliant driver.
func Run(ctx context.Context, request Request) (result Result, resultErr error) {
	request = cloneRequest(request)
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	secrets := slices.Clone(request.Envelope.Secrets)
	defer func() { redactResult(&result, secrets) }()
	result = Result{Environment: request.Environment, Envelope: request.Envelope}
	result.Envelope.Secrets = nil
	runContext, cancel := context.WithTimeout(ctx, request.Envelope.MaxDuration)
	defer cancel()
	scope := PreparationRequest{CampaignID: request.Envelope.CampaignID, Namespace: request.Envelope.Namespace, Tenant: request.Envelope.Tenant}
	preparation, prepareErr := request.Driver.Prepare(runContext, scope)
	preparation = clonePreparation(preparation)
	resources := slices.Clone(preparation.Resources)
	result.Cleanup = CleanupRecord{CampaignID: request.Envelope.CampaignID, Resources: redactResources(resources, request.Envelope.Secrets)}
	defer func() {
		cleanupContext := context.WithoutCancel(ctx)
		if request.Envelope.CleanupTimeout > 0 {
			var cancel context.CancelFunc
			cleanupContext, cancel = context.WithTimeout(cleanupContext, request.Envelope.CleanupTimeout)
			defer cancel()
		}
		cleanupErr := request.Driver.Cleanup(cleanupContext, clonePreparation(preparation))
		result.Cleanup.Complete = cleanupErr == nil
		if cleanupErr != nil {
			result.Cleanup.Error = umpire.ExecutionErrorClass(cleanupErr)
			result.StopReason = "cleanup failed"
			result.Audit = append(result.Audit, AuditRecord{Decision: "stopped", Reason: result.StopReason, ErrorClass: umpire.ExecutionErrorClass(cleanupErr)})
		}
		result.Complete = result.StopReason == "" && result.Cleanup.Complete
	}()
	if prepareErr != nil {
		result.StopReason = "isolation preparation failed"
		result.Audit = []AuditRecord{{Decision: "stopped", Reason: result.StopReason, ErrorClass: umpire.ExecutionErrorClass(prepareErr)}}
		return result, nil
	}
	if preparation.Scope != scope {
		result.StopReason = "isolation attestation mismatch"
		result.Audit = []AuditRecord{{Decision: "stopped", Reason: result.StopReason}}
		return result, nil
	}

	type sharedState struct {
		sync.Mutex
		next          int
		started       int
		faults        int
		evidenceBytes int
		stopReason    string
		audit         []AuditRecord
		actions       []ActionResult
	}
	state := &sharedState{}
	stop := func(reason string, err error) {
		state.Lock()
		defer state.Unlock()
		if state.stopReason != "" {
			return
		}
		state.stopReason = reason
		record := AuditRecord{Decision: "stopped", Reason: reason}
		if err != nil {
			record.ErrorClass = umpire.ExecutionErrorClass(err)
		}
		state.audit = append(state.audit, record)
		cancel()
	}
	claimNext := func() (int, Action, bool) {
		state.Lock()
		defer state.Unlock()
		if state.stopReason != "" || runContext.Err() != nil || state.next >= len(request.Actions) {
			return 0, Action{}, false
		}
		if state.started >= request.Envelope.MaxActions {
			return 0, Action{}, false
		}
		action := request.Actions[state.next]
		if action.Fault && state.faults >= request.Envelope.MaxFaults {
			return 0, Action{}, false
		}
		sequence := state.next
		state.next++
		state.started++
		if action.Fault {
			state.faults++
		}
		state.audit = append(state.audit, AuditRecord{Sequence: sequence, Action: action.Name, Decision: "started"})
		return sequence, action, true
	}
	var workers sync.WaitGroup
	for range min(request.Envelope.MaxConcurrent, request.Envelope.MaxActions) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				sequence, action, ok := claimNext()
				if !ok {
					return
				}
				observation := request.Driver.Execute(runContext, action)
				redactedFields := redactFields(observation.Fields, request.Envelope.Secrets)
				payload, marshalErr := json.Marshal(redactedFields)
				if marshalErr != nil {
					stop("evidence encoding failed", marshalErr)
					return
				}
				state.Lock()
				if state.evidenceBytes+len(payload) > request.Envelope.MaxEvidenceBytes {
					state.Unlock()
					stop("evidence budget exhausted", nil)
					return
				}
				state.evidenceBytes += len(payload)
				state.actions = append(state.actions, ActionResult{Sequence: sequence, Action: action.Name, Observation: redactedFields})
				state.audit = append(state.audit, AuditRecord{Sequence: sequence, Action: action.Name, Decision: "completed", ErrorClass: umpire.ExecutionErrorClass(observation.Error)})
				state.Unlock()
				switch {
				case observation.InvariantViolated:
					stop("invariant violation", observation.Error)
					return
				case observation.ObservationLost:
					stop("observation loss", observation.Error)
					return
				case observation.Error != nil:
					stop("action failed", observation.Error)
					return
				}
			}
		}()
	}
	workers.Wait()
	state.Lock()
	if state.stopReason == "" {
		if runContext.Err() != nil {
			state.stopReason = runContext.Err().Error()
			state.audit = append(state.audit, AuditRecord{Decision: "stopped", Reason: state.stopReason, ErrorClass: umpire.ExecutionErrorClass(runContext.Err())})
		} else if state.next < len(request.Actions) && state.started >= request.Envelope.MaxActions {
			state.stopReason = "action budget exhausted"
			state.audit = append(state.audit, AuditRecord{Decision: "stopped", Reason: state.stopReason})
		} else if state.next < len(request.Actions) && request.Actions[state.next].Fault && state.faults >= request.Envelope.MaxFaults {
			state.stopReason = "fault budget exhausted"
			state.audit = append(state.audit, AuditRecord{Decision: "stopped", Reason: state.stopReason})
		}
	}
	result.ActionsStarted = state.started
	result.FaultsStarted = state.faults
	result.EvidenceBytes = state.evidenceBytes
	result.StopReason = state.stopReason
	result.Actions = slices.Clone(state.actions)
	result.Audit = slices.Clone(state.audit)
	state.Unlock()
	slices.SortFunc(result.Actions, func(left, right ActionResult) int { return left.Sequence - right.Sequence })
	slices.SortStableFunc(result.Audit, func(left, right AuditRecord) int {
		if left.Sequence != right.Sequence {
			return left.Sequence - right.Sequence
		}
		return 0
	})
	return result, nil
}

func validateRequest(request Request) error {
	if request.Driver == nil {
		return fmt.Errorf("%w: driver is nil", ErrUnsafeRequest)
	}
	if err := umpire.ValidateEnvironmentProfile(request.Environment); err != nil {
		return fmt.Errorf("%w: %v", ErrUnsafeRequest, err)
	}
	if request.Environment.Kind != umpire.CanaryEnvironment {
		return fmt.Errorf("%w: environment kind must be canary", ErrUnsafeRequest)
	}
	envelope := request.Envelope
	if envelope.CampaignID == "" || envelope.Namespace == "" || envelope.Tenant == "" || !envelope.NamespaceIsolated || !envelope.TenantIsolated {
		return fmt.Errorf("%w: campaign, namespace, and tenant isolation are required", ErrUnsafeRequest)
	}
	if envelope.MaxActions < 1 || envelope.MaxConcurrent < 1 || envelope.MaxDuration <= 0 || envelope.MaxEvidenceBytes < 1 || envelope.CleanupTimeout <= 0 || envelope.MaxFaults < 0 {
		return fmt.Errorf("%w: traffic, time, concurrency, fault, evidence, and cleanup budgets are required", ErrUnsafeRequest)
	}
	if len(envelope.AllowedActions) == 0 || duplicates(envelope.AllowedActions) || duplicates(envelope.AllowedFaults) {
		return fmt.Errorf("%w: action allowlist is empty or contains duplicates", ErrUnsafeRequest)
	}
	if !request.Environment.Retention.RedactPayloads || !request.Environment.Retention.RedactSecrets {
		return fmt.Errorf("%w: evidence profile does not require redaction", ErrUnsafeRequest)
	}
	for _, action := range request.Actions {
		if action.Name == "" || action.Namespace != envelope.Namespace || action.Tenant != envelope.Tenant {
			return fmt.Errorf("%w: action %q escapes the isolated namespace or tenant", ErrUnsafeRequest, action.Name)
		}
		if !slices.Contains(envelope.AllowedActions, action.Name) {
			return fmt.Errorf("%w: action %q is not allowlisted", ErrUnsafeRequest, action.Name)
		}
		if action.Fault && !slices.Contains(envelope.AllowedFaults, action.Name) {
			return fmt.Errorf("%w: fault %q is not allowlisted", ErrUnsafeRequest, action.Name)
		}
		if action.Destructive && action.Fault && (!envelope.AllowDestructiveFaults || !slices.Contains(request.Environment.DriveCapabilities, "canary-destructive-faults")) {
			return fmt.Errorf("%w: destructive fault %q lacks explicit canary authority", ErrUnsafeRequest, action.Name)
		}
		if action.Destructive && !action.Fault && (!envelope.AllowDestructiveActions || !slices.Contains(request.Environment.DriveCapabilities, "canary-destructive-actions")) {
			return fmt.Errorf("%w: destructive action %q lacks explicit canary authority", ErrUnsafeRequest, action.Name)
		}
		if action.Fault && !slices.Contains(request.Environment.DriveCapabilities, "canary-faults") {
			return fmt.Errorf("%w: local fault capability does not authorize canary fault %q", ErrUnsafeRequest, action.Name)
		}
	}
	return nil
}

func redactFields(fields map[string]string, secrets []string) map[string]string {
	result := make(map[string]string, len(fields))
	for key, value := range fields {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "payload") || strings.Contains(lower, "authorization") || strings.Contains(lower, "credential") || strings.Contains(lower, "token") || strings.Contains(lower, "secret") {
			result[redactString(key, secrets)] = redacted
			continue
		}
		result[redactString(key, secrets)] = redactString(value, secrets)
	}
	return result
}

func redactResources(resources []Resource, secrets []string) []Resource {
	result := slices.Clone(resources)
	for index := range result {
		result[index].Kind = redactString(result[index].Kind, secrets)
		result[index].ID = redactString(result[index].ID, secrets)
	}
	return result
}

func cloneRequest(request Request) Request {
	request.Actions = slices.Clone(request.Actions)
	request.Envelope.AllowedActions = slices.Clone(request.Envelope.AllowedActions)
	request.Envelope.AllowedFaults = slices.Clone(request.Envelope.AllowedFaults)
	request.Envelope.Secrets = slices.Clone(request.Envelope.Secrets)
	request.Environment.DriveCapabilities = slices.Clone(request.Environment.DriveCapabilities)
	request.Environment.ObservationSources = slices.Clone(request.Environment.ObservationSources)
	request.Environment.OrderingGuarantees = slices.Clone(request.Environment.OrderingGuarantees)
	request.Environment.SupportedProperties = slices.Clone(request.Environment.SupportedProperties)
	request.Environment.ClockDomains = slices.Clone(request.Environment.ClockDomains)
	for index := range request.Environment.ClockDomains {
		request.Environment.ClockDomains[index].Sources = slices.Clone(request.Environment.ClockDomains[index].Sources)
	}
	return request
}

func clonePreparation(preparation Preparation) Preparation {
	preparation.Resources = slices.Clone(preparation.Resources)
	return preparation
}

func redactResult(result *Result, secrets []string) {
	if result == nil {
		return
	}
	result.Environment.Name = redactString(result.Environment.Name, secrets)
	for index := range result.Environment.DriveCapabilities {
		result.Environment.DriveCapabilities[index] = redactString(result.Environment.DriveCapabilities[index], secrets)
	}
	for index := range result.Environment.SupportedProperties {
		result.Environment.SupportedProperties[index] = redactString(result.Environment.SupportedProperties[index], secrets)
	}
	for index := range result.Environment.ClockDomains {
		result.Environment.ClockDomains[index].Name = redactString(result.Environment.ClockDomains[index].Name, secrets)
	}
	result.Envelope.CampaignID = redactString(result.Envelope.CampaignID, secrets)
	result.Envelope.Namespace = redactString(result.Envelope.Namespace, secrets)
	result.Envelope.Tenant = redactString(result.Envelope.Tenant, secrets)
	for index := range result.Envelope.AllowedActions {
		result.Envelope.AllowedActions[index] = redactString(result.Envelope.AllowedActions[index], secrets)
	}
	for index := range result.Envelope.AllowedFaults {
		result.Envelope.AllowedFaults[index] = redactString(result.Envelope.AllowedFaults[index], secrets)
	}
	for index := range result.Actions {
		result.Actions[index].Action = redactString(result.Actions[index].Action, secrets)
		result.Actions[index].Observation = redactFields(result.Actions[index].Observation, secrets)
	}
	for index := range result.Audit {
		result.Audit[index].Action = redactString(result.Audit[index].Action, secrets)
		result.Audit[index].Decision = redactString(result.Audit[index].Decision, secrets)
		result.Audit[index].Reason = redactString(result.Audit[index].Reason, secrets)
		result.Audit[index].ErrorClass = redactString(result.Audit[index].ErrorClass, secrets)
	}
	result.StopReason = redactString(result.StopReason, secrets)
	result.Cleanup.CampaignID = redactString(result.Cleanup.CampaignID, secrets)
	result.Cleanup.Resources = redactResources(result.Cleanup.Resources, secrets)
	result.Cleanup.Error = redactString(result.Cleanup.Error, secrets)
}

func redactString(value string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, redacted)
		}
	}
	return value
}

func duplicates(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
