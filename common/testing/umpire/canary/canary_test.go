package canary

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

func TestCanaryRejectsMissingIsolationBeforePreparingEnvironment(t *testing.T) {
	driver := &fakeDriver{}
	request := safeRequest(driver, 1)
	request.Envelope.NamespaceIsolated = false

	_, err := Run(context.Background(), request)
	require.ErrorIs(t, err, ErrUnsafeRequest)
	require.Zero(t, driver.prepareCalls)
	require.Zero(t, driver.executeCalls)
}

func TestCanaryEnforcesConcurrentTrafficAndFaultBudgets(t *testing.T) {
	driver := &fakeDriver{}
	request := safeRequest(driver, 20)
	request.Envelope.MaxActions = 5
	request.Envelope.MaxConcurrent = 4

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 5, result.ActionsStarted)
	require.Equal(t, 5, driver.executeCalls)
	require.Equal(t, "action budget exhausted", result.StopReason)
	require.True(t, result.Cleanup.Complete)
	require.Equal(t, 1, driver.cleanupCalls)

	faultDriver := &fakeDriver{}
	faultRequest := safeRequest(faultDriver, 1)
	faultRequest.Environment.DriveCapabilities = append(faultRequest.Environment.DriveCapabilities, "canary-faults")
	faultRequest.Envelope.AllowedActions = []string{"observe", "drop"}
	faultRequest.Envelope.AllowedFaults = []string{"drop"}
	faultRequest.Envelope.MaxActions = 2
	faultRequest.Envelope.MaxFaults = 1
	faultRequest.Actions = []Action{
		{Name: "drop", Namespace: "umpire-canary", Tenant: "umpire-tenant", Fault: true},
		{Name: "drop", Namespace: "umpire-canary", Tenant: "umpire-tenant", Fault: true},
	}

	faultResult, err := Run(context.Background(), faultRequest)
	require.NoError(t, err)
	require.Equal(t, 1, faultResult.FaultsStarted)
	require.Equal(t, "fault budget exhausted", faultResult.StopReason)
}

func TestCanaryStopsOnInvariantOrObservationLossAndAlwaysCleansUp(t *testing.T) {
	tests := []struct {
		name        string
		observation Observation
		stopReason  string
	}{
		{name: "invariant", observation: Observation{InvariantViolated: true}, stopReason: "invariant violation"},
		{name: "observation loss", observation: Observation{ObservationLost: true}, stopReason: "observation loss"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			driver := &fakeDriver{observations: []Observation{test.observation}}
			result, err := Run(context.Background(), safeRequest(driver, 4))
			require.NoError(t, err)
			require.Equal(t, test.stopReason, result.StopReason)
			require.True(t, result.Cleanup.Complete)
			require.Equal(t, 1, driver.cleanupCalls)
		})
	}
}

func TestCanaryCleanupIgnoresRunCancellation(t *testing.T) {
	driver := &fakeDriver{execute: func(ctx context.Context, _ Action) Observation {
		<-ctx.Done()
		return Observation{Error: ctx.Err()}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := Run(ctx, safeRequest(driver, 1))
	require.NoError(t, err)
	require.True(t, driver.cleanupContextLive)
	require.True(t, result.Cleanup.Complete)
}

func TestCanaryPropagatesExecutionAndCleanupDeadlines(t *testing.T) {
	prepareDriver := &fakeDriver{prepare: func(ctx context.Context, _ PreparationRequest) (Preparation, error) {
		<-ctx.Done()
		return Preparation{}, ctx.Err()
	}}
	prepareRequest := safeRequest(prepareDriver, 1)
	prepareRequest.Envelope.MaxDuration = 50 * time.Millisecond

	prepareResult, err := Run(context.Background(), prepareRequest)
	require.NoError(t, err)
	require.Equal(t, "isolation preparation failed", prepareResult.StopReason)
	require.True(t, prepareResult.Cleanup.Complete)

	executionDriver := &fakeDriver{execute: func(ctx context.Context, _ Action) Observation {
		<-ctx.Done()
		return Observation{Error: ctx.Err()}
	}}
	executionRequest := safeRequest(executionDriver, 1)
	executionRequest.Envelope.MaxDuration = 50 * time.Millisecond

	executionResult, err := Run(context.Background(), executionRequest)
	require.NoError(t, err)
	require.Equal(t, "action failed", executionResult.StopReason)
	require.True(t, executionResult.Cleanup.Complete)

	cleanupDriver := &fakeDriver{cleanup: func(ctx context.Context, _ Preparation) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	cleanupRequest := safeRequest(cleanupDriver, 1)
	cleanupRequest.Envelope.CleanupTimeout = 50 * time.Millisecond

	cleanupResult, err := Run(context.Background(), cleanupRequest)
	require.NoError(t, err)
	require.Equal(t, "cleanup failed", cleanupResult.StopReason)
	require.False(t, cleanupResult.Cleanup.Complete)
}

func TestCanaryCleansPartiallyPreparedResourcesAndRedactsRecoveryMetadata(t *testing.T) {
	driver := &fakeDriver{
		prepareResources: []Resource{{Kind: "worker-super-secret", ID: "resource-super-secret"}},
		prepareErr:       errors.New("partial preparation"),
	}
	request := safeRequest(driver, 1)
	request.Envelope.Secrets = []string{"super-secret"}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "isolation preparation failed", result.StopReason)
	require.Equal(t, 1, driver.cleanupCalls)
	require.True(t, result.Cleanup.Complete)
	require.Equal(t, "worker-[redacted]", result.Cleanup.Resources[0].Kind)
	require.Equal(t, "resource-[redacted]", result.Cleanup.Resources[0].ID)
}

func TestCanaryRejectsPreparationOutsideAttestedScope(t *testing.T) {
	var cleaned Preparation
	driver := &fakeDriver{prepare: func(_ context.Context, request PreparationRequest) (Preparation, error) {
		return Preparation{Scope: PreparationRequest{CampaignID: request.CampaignID, Namespace: request.Namespace + "-other", Tenant: request.Tenant}, Resources: []Resource{{Kind: "namespace", ID: "owned"}}}, nil
	}, cleanup: func(_ context.Context, preparation Preparation) error {
		cleaned = preparation
		return nil
	}}

	result, err := Run(context.Background(), safeRequest(driver, 1))
	require.NoError(t, err)
	require.Equal(t, "isolation attestation mismatch", result.StopReason)
	require.Zero(t, driver.executeCalls)
	require.True(t, result.Cleanup.Complete)
	require.Equal(t, "umpire-canary-other", cleaned.Scope.Namespace)
	require.Equal(t, []Resource{{Kind: "namespace", ID: "owned"}}, cleaned.Resources)
}

func TestCanarySnapshotsCallerOwnedRequestSlices(t *testing.T) {
	var request Request
	driver := &fakeDriver{prepare: func(_ context.Context, scope PreparationRequest) (Preparation, error) {
		request.Actions[0].Name = "mutated"
		request.Envelope.AllowedActions[0] = "mutated"
		request.Envelope.Secrets[0] = "mutated"
		return Preparation{Scope: scope}, nil
	}}
	request = safeRequest(driver, 1)
	request.Envelope.Secrets = []string{"original"}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.Equal(t, "observe", result.Actions[0].Action)
	require.Equal(t, []string{"observe"}, result.Envelope.AllowedActions)
}

func TestCanaryRedactsEvidenceAndReportsCleanupFailure(t *testing.T) {
	driver := &fakeDriver{
		observations: []Observation{{Fields: map[string]string{
			"payload":          "private payload",
			"note":             "prefix super-secret suffix",
			"token":            "credential",
			"key-super-secret": "value",
		}}},
		cleanupErr: errors.New("cleanup broke with super-secret"),
	}
	request := safeRequest(driver, 1)
	request.Envelope.Secrets = []string{"super-secret"}
	request.Envelope.CampaignID = "campaign-super-secret"
	request.Envelope.Namespace = "namespace-super-secret"
	request.Envelope.Tenant = "tenant-super-secret"
	request.Envelope.AllowedActions = []string{"observe-super-secret"}
	request.Actions[0] = Action{Name: "observe-super-secret", Namespace: request.Envelope.Namespace, Tenant: request.Envelope.Tenant}

	result, err := Run(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "cleanup failed", result.StopReason)
	require.False(t, result.Cleanup.Complete)
	require.Equal(t, "error", result.Cleanup.Error)
	require.Equal(t, redacted, result.Actions[0].Observation["payload"])
	require.Equal(t, redacted, result.Actions[0].Observation["token"])
	require.Equal(t, "prefix [redacted] suffix", result.Actions[0].Observation["note"])
	require.Equal(t, redacted, result.Actions[0].Observation["key-[redacted]"])
	encoded := fmt.Sprintf("%+v", result)
	require.NotContains(t, encoded, "super-secret")
	require.Empty(t, result.Envelope.Secrets)
	require.Contains(t, result.Summary(), "cleanup=incomplete")
	require.NotContains(t, result.Summary(), "super-secret")
}

func TestCanaryDoesNotInheritLocalDestructiveOrFaultAuthority(t *testing.T) {
	driver := &fakeDriver{}
	request := safeRequest(driver, 1)
	request.Environment.DriveCapabilities = append(request.Environment.DriveCapabilities, "faults")
	request.Envelope.AllowedActions = []string{"drop"}
	request.Envelope.AllowedFaults = []string{"drop"}
	request.Envelope.AllowDestructiveFaults = true
	request.Actions[0] = Action{Name: "drop", Namespace: "umpire-canary", Tenant: "umpire-tenant", Fault: true, Destructive: true}

	_, err := Run(context.Background(), request)
	require.ErrorContains(t, err, "lacks explicit canary authority")
	require.Zero(t, driver.prepareCalls)
}

func safeRequest(driver Driver, actions int) Request {
	profile, err := umpire.ForEnvironment(umpire.CanaryEnvironment, umpire.PublicAPIProfile())
	if err != nil {
		panic(err)
	}
	request := Request{
		Environment: profile,
		Envelope: SafetyEnvelope{
			CampaignID:        "campaign-1",
			Namespace:         "umpire-canary",
			Tenant:            "umpire-tenant",
			NamespaceIsolated: true,
			TenantIsolated:    true,
			AllowedActions:    []string{"observe"},
			MaxActions:        max(actions, 1),
			MaxFaults:         0,
			MaxConcurrent:     1,
			MaxDuration:       time.Second,
			MaxEvidenceBytes:  4096,
			CleanupTimeout:    time.Second,
		},
		Driver: driver,
	}
	for range actions {
		request.Actions = append(request.Actions, Action{Name: "observe", Namespace: "umpire-canary", Tenant: "umpire-tenant"})
	}
	return request
}

type fakeDriver struct {
	mu                 sync.Mutex
	prepareCalls       int
	executeCalls       int
	cleanupCalls       int
	cleanupContextLive bool
	observations       []Observation
	execute            func(context.Context, Action) Observation
	cleanupErr         error
	cleanup            func(context.Context, Preparation) error
	prepare            func(context.Context, PreparationRequest) (Preparation, error)
	prepareResources   []Resource
	prepareErr         error
}

func (d *fakeDriver) Prepare(ctx context.Context, request PreparationRequest) (Preparation, error) {
	d.mu.Lock()
	d.prepareCalls++
	custom := d.prepare
	resources := slices.Clone(d.prepareResources)
	err := d.prepareErr
	d.mu.Unlock()
	if custom != nil {
		return custom(ctx, request)
	}
	if resources != nil || err != nil {
		return Preparation{Scope: request, Resources: resources}, err
	}
	return Preparation{Scope: request, Resources: []Resource{{Kind: "namespace", ID: "umpire-canary"}}}, nil
}

func (d *fakeDriver) Execute(ctx context.Context, action Action) Observation {
	d.mu.Lock()
	index := d.executeCalls
	d.executeCalls++
	custom := d.execute
	var observation Observation
	if index < len(d.observations) {
		observation = d.observations[index]
	}
	d.mu.Unlock()
	if custom != nil {
		return custom(ctx, action)
	}
	return observation
}

func (d *fakeDriver) Cleanup(ctx context.Context, preparation Preparation) error {
	d.mu.Lock()
	d.cleanupCalls++
	d.cleanupContextLive = ctx.Err() == nil
	custom := d.cleanup
	err := d.cleanupErr
	d.mu.Unlock()
	if custom != nil {
		return custom(ctx, preparation)
	}
	return err
}
