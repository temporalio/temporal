package tdbg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace/nsreplication"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	namespaceReplicationRepairExecutionSucceeded     = "SUCCEEDED"
	namespaceReplicationRepairExecutionFailed        = "FAILED"
	namespaceReplicationRepairReconciliationPageSize = 25
)

type namespaceReplicationRepairExecutionRequest struct {
	VerifyRequest   namespaceReplicationVerifyRequest
	Observation     *namespaceReplicationObservation
	Plan            *namespaceReplicationRepairPlan
	Reason          string
	ReconcileBefore *time.Time
}

type namespaceReplicationRepairExecutionResult struct {
	ComponentReconciliationError string                                                     `json:"component_reconciliation_error,omitempty"`
	Plan                         *namespaceReplicationRepairPlan                            `json:"plan"`
	Status                       string                                                     `json:"status"`
	StatusDetail                 string                                                     `json:"status_detail,omitempty"`
	EffectiveCutoff              time.Time                                                  `json:"effective_cutoff"`
	Source                       namespaceReplicationRepairApplyResult                      `json:"source"`
	Targets                      []namespaceReplicationRepairApplyResult                    `json:"targets,omitempty"`
	FinalVerification            *namespaceReplicationVerificationResult                    `json:"final_verification,omitempty"`
	ComponentReconciliation      *adminservice.ReconcileNamespaceMutationComponentsResponse `json:"component_reconciliation,omitempty"`
	ComponentDiscoveryNote       string                                                     `json:"component_discovery_note,omitempty"`
	Warnings                     []string                                                   `json:"warnings,omitempty"`
}

type namespaceReplicationRepairApplyResult struct {
	Cluster  string `json:"cluster"`
	Action   string `json:"action"`
	Outcome  string `json:"outcome,omitempty"`
	Readback bool   `json:"readback_verified"`
	Error    string `json:"error,omitempty"`
}

type namespaceReplicationRepairer struct {
	verifier *namespaceReplicationVerifier
}

func (r *namespaceReplicationRepairer) Execute(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
) (_ *namespaceReplicationRepairExecutionResult, retErr error) {
	if request.Plan == nil {
		return nil, errors.New("repair execution requires an executable frozen plan")
	}
	result := &namespaceReplicationRepairExecutionResult{
		Plan:   request.Plan,
		Status: namespaceReplicationRepairExecutionFailed,
		Source: namespaceReplicationRepairApplyResult{
			Cluster: request.Plan.SourceCluster,
			Action:  request.Plan.SourceAction,
		},
	}
	if err := validateNamespaceReplicationRepairExecution(request); err != nil {
		return result, errors.New("repair execution requires an executable frozen plan")
	}

	sourceConnection, err := r.verifier.clients.OpenSource(request.VerifyRequest.SourceAddress)
	if err != nil {
		return result, fmt.Errorf("connect to source for repair: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, sourceConnection.closer.Close())
	}()

	effectiveCutoff, err := r.effectiveRepairCutoff(request.ReconcileBefore, request.Observation.sourceSnapshot.GetObservedAt())
	if err != nil {
		return result, err
	}
	result.EffectiveCutoff = effectiveCutoff

	frozenSource, err := r.freezeRepairSource(ctx, sourceConnection.client, request)
	if err != nil {
		return result, err
	}

	preparedSource, err := r.prepareRepairSource(ctx, sourceConnection.client, request, frozenSource, &result.Source)
	if err != nil {
		return result, err
	}

	result.Targets = r.applyRepairTargets(ctx, request, preparedSource)

	finalObservation, finalErr := r.verifier.observe(ctx, request.VerifyRequest)
	if finalObservation != nil {
		result.FinalVerification = finalObservation.result
	}
	reconciliation, reconcileErr := r.reconcileRepairComponents(
		ctx,
		sourceConnection.client,
		request,
		effectiveCutoff,
		finalObservation,
	)
	result.ComponentReconciliation = reconciliation
	result.Warnings = namespaceReplicationRepairWarnings(reconciliation)
	if reconcileErr != nil {
		result.ComponentReconciliationError = reconcileErr.Error()
	}
	result.ComponentDiscoveryNote = "Component reconciliation covers executions currently visible; rerun to catch delayed visibility records."

	failures := namespaceReplicationRepairFailures(result, finalObservation, finalErr, reconciliation, reconcileErr)
	if len(failures) != 0 {
		result.StatusDetail = errors.Join(failures...).Error()
		return result, errors.New(result.StatusDetail)
	}
	result.Status = namespaceReplicationRepairExecutionSucceeded
	result.StatusDetail = "Current replicas verified; success does not imply former replicas were synchronized."
	return result, nil
}

func namespaceReplicationRepairWarnings(response *adminservice.ReconcileNamespaceMutationComponentsResponse) []string {
	var removedPeers []string
	for _, component := range response.GetComponents() {
		for _, peer := range component.GetPeers() {
			if peer.GetBasis() == adminservice.NamespaceMutationPeerReconciliation_BASIS_REMOVED_MEMBERSHIP {
				removedPeers = append(removedPeers, peer.GetTargetCluster())
			}
		}
	}
	if len(removedPeers) == 0 {
		return nil
	}
	slices.Sort(removedPeers)
	return []string{fmt.Sprintf(
		"Deliveries to removed peers were abandoned: %s. These peers were not inspected or updated and may retain stale namespace membership.",
		strings.Join(slices.Compact(removedPeers), ", "),
	)}
}

func validateNamespaceReplicationRepairExecution(request namespaceReplicationRepairExecutionRequest) error {
	if request.Observation == nil || request.Observation.sourceSnapshot == nil ||
		!request.Observation.sourceStable || request.Plan == nil || request.Reason == "" ||
		request.Plan.RepairID == "" ||
		(request.Plan.Status != namespaceReplicationRepairStatusPlanned &&
			request.Plan.Status != namespaceReplicationRepairStatusNoChanges) {
		return errors.New("repair execution requires an executable frozen plan")
	}
	return nil
}

func (r *namespaceReplicationRepairer) effectiveRepairCutoff(requested *time.Time, observedAt *timestamppb.Timestamp) (time.Time, error) {
	if observedAt == nil || observedAt.CheckValid() != nil {
		return time.Time{}, errors.New("source must support a server-timed namespace observation for repair")
	}
	automatic := observedAt.AsTime()
	if requested == nil {
		return automatic, nil
	}
	if requested.After(automatic) {
		return time.Time{}, errors.New("--reconcile-before cannot be later than the automatic repair cutoff")
	}
	return requested.UTC(), nil
}

func (r *namespaceReplicationRepairer) freezeRepairSource(
	ctx context.Context,
	client adminservice.AdminServiceClient,
	request namespaceReplicationRepairExecutionRequest,
) (*adminservice.GetNamespaceResponse, error) {
	frozen, err := getNamespace(
		ctx,
		r.verifier.rpcTimeout,
		client,
		namespaceReplicationSelector{ID: request.Plan.NamespaceID},
	)
	if err != nil {
		return nil, fmt.Errorf("freeze source namespace: %w", err)
	}
	equal, err := namespaceReplicationSnapshotsEqual(
		request.Observation.sourceSnapshot,
		frozen,
		r.verifier.dataKeysToIgnore,
	)
	if err != nil {
		return nil, fmt.Errorf("compare frozen source namespace: %w", err)
	}
	if !equal {
		return nil, errors.New("source namespace changed after repair planning")
	}
	return frozen, nil
}

func (r *namespaceReplicationRepairer) prepareRepairSource(
	ctx context.Context,
	client adminservice.AdminServiceClient,
	request namespaceReplicationRepairExecutionRequest,
	frozen *adminservice.GetNamespaceResponse,
	result *namespaceReplicationRepairApplyResult,
) (*adminservice.GetNamespaceResponse, error) {
	if request.Plan.SourceAction != namespaceReplicationRepairActionPrepareSource {
		result.Outcome = namespaceReplicationRepairActionNone
		result.Readback = true
		return frozen, nil
	}
	task, err := buildNamespaceReplicationRepairTask(
		frozen,
		frozen,
		r.verifier.dataKeysToIgnore,
		namespaceRepairDataSourceExact,
	)
	if err != nil {
		return nil, err
	}
	task.ConfigVersion = request.Plan.ProposedSourceConfigVersion
	prepared, err := r.applyRepairAndReadback(ctx, client, request, task, result)
	if err != nil {
		result.Error = err.Error()
		return nil, fmt.Errorf("prepare source: %w", err)
	}
	return prepared, nil
}

func (r *namespaceReplicationRepairer) applyRepairTargets(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
	preparedSource *adminservice.GetNamespaceResponse,
) []namespaceReplicationRepairApplyResult {
	var results []namespaceReplicationRepairApplyResult
	for _, clusterPlan := range request.Plan.Clusters {
		if clusterPlan.Action != namespaceReplicationRepairActionNone {
			results = append(results, r.applyTargetRepair(ctx, request, preparedSource, clusterPlan))
		}
	}
	slices.SortFunc(results, func(left, right namespaceReplicationRepairApplyResult) int {
		return cmp.Compare(left.Cluster, right.Cluster)
	})
	return results
}

func (r *namespaceReplicationRepairer) reconcileRepairComponents(
	ctx context.Context,
	client adminservice.AdminServiceClient,
	request namespaceReplicationRepairExecutionRequest,
	cutoff time.Time,
	finalObservation *namespaceReplicationObservation,
) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
	result := &adminservice.ReconcileNamespaceMutationComponentsResponse{}
	if finalObservation == nil || !finalObservation.sourceStable || finalObservation.sourceSnapshot == nil {
		return result, errors.New("component reconciliation requires a stable final source observation")
	}
	verifiedPeers := verifiedRepairPeers(finalObservation)
	seenPageTokens := make(map[string]struct{})
	var nextPageToken []byte
	for {
		page, err := namespaceReplicationRPCCall(
			ctx,
			r.verifier.rpcTimeout,
			func(rpcCtx context.Context) (*adminservice.ReconcileNamespaceMutationComponentsResponse, error) {
				return client.ReconcileNamespaceMutationComponents(
					rpcCtx,
					&adminservice.ReconcileNamespaceMutationComponentsRequest{
						NamespaceId:               request.Plan.NamespaceID,
						RepairId:                  request.Plan.RepairID,
						Reason:                    request.Reason,
						CutoffTime:                timestamppb.New(cutoff),
						AuthoritativePeerClusters: authoritativeRepairPeers(request.Observation.result),
						VerifiedPeerClusters:      verifiedPeers,
						PageSize:                  namespaceReplicationRepairReconciliationPageSize,
						NextPageToken:             nextPageToken,
						VerifiedConfigVersion:     wrapperspb.Int64(finalObservation.sourceSnapshot.GetConfigVersion()),
						VerifiedFailoverVersion:   wrapperspb.Int64(finalObservation.sourceSnapshot.GetFailoverVersion()),
					},
				)
			},
		)
		if err != nil {
			return result, err
		}
		if page == nil {
			return result, errors.New("component reconciliation returned no response")
		}
		result.Components = append(result.Components, page.GetComponents()...)
		nextPageToken = page.GetNextPageToken()
		if len(nextPageToken) == 0 {
			return result, nil
		}
		tokenKey := string(nextPageToken)
		if _, duplicate := seenPageTokens[tokenKey]; duplicate {
			return result, errors.New("component reconciliation returned a repeated next-page token")
		}
		seenPageTokens[tokenKey] = struct{}{}
		nextPageToken = slices.Clone(nextPageToken)
	}
}

func namespaceReplicationRepairFailures(
	result *namespaceReplicationRepairExecutionResult,
	finalObservation *namespaceReplicationObservation,
	finalErr error,
	reconciliation *adminservice.ReconcileNamespaceMutationComponentsResponse,
	reconcileErr error,
) []error {
	var failures []error
	for _, target := range result.Targets {
		if target.Error != "" {
			failures = append(failures, fmt.Errorf("target %s: %s", target.Cluster, target.Error))
		}
	}
	if finalErr != nil {
		failures = append(failures, fmt.Errorf("final verification: %w", finalErr))
	} else if finalObservation == nil || finalObservation.result.Status != namespaceReplicationStatusHealthy {
		failures = append(failures, errors.New("final verification is not healthy"))
	}
	if reconcileErr != nil {
		failures = append(failures, fmt.Errorf("component reconciliation: %w", reconcileErr))
	}
	for _, component := range reconciliation.GetComponents() {
		if component.GetError() != "" {
			failures = append(failures, fmt.Errorf(
				"component %s/%s: %s",
				component.GetBusinessId(),
				component.GetRunId(),
				component.GetError(),
			))
		}
	}
	return failures
}

func (r *namespaceReplicationRepairer) applyTargetRepair(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
	preparedSource *adminservice.GetNamespaceResponse,
	clusterPlan namespaceReplicationClusterRepairPlan,
) (result namespaceReplicationRepairApplyResult) {
	result.Cluster = clusterPlan.Cluster
	result.Action = clusterPlan.Action
	address := request.Observation.clusterAddresses[clusterPlan.Cluster]
	connection, err := r.verifier.clients.OpenTarget(address)
	if err != nil {
		result.Error = fmt.Sprintf("connect: %v", err)
		return result
	}
	defer func() {
		if err := connection.closer.Close(); err != nil && result.Error == "" {
			result.Error = fmt.Sprintf("close connection: %v", err)
			result.Readback = false
		}
	}()

	failoverSource := request.Observation.targetSnapshots[clusterPlan.Cluster]
	freshTarget, err := getNamespace(ctx, r.verifier.rpcTimeout, connection.client, namespaceReplicationSelector{ID: request.Plan.NamespaceID})
	if clusterPlan.Action == namespaceReplicationRepairActionCreate {
		if !isNamespaceNotFound(err) {
			result.Error = "target appeared or could not be checked after planning; rerun verification"
			return result
		}
		_, nameErr := getNamespace(ctx, r.verifier.rpcTimeout, connection.client, namespaceReplicationSelector{Name: preparedSource.GetInfo().GetName()})
		if !isNamespaceNotFound(nameErr) {
			result.Error = "target namespace name appeared or could not be checked after planning; rerun verification"
			return result
		}
	} else {
		if err != nil {
			result.Error = fmt.Sprintf("refresh target: %v", err)
			return result
		}
		equal, compareErr := namespaceReplicationSnapshotsEqual(failoverSource, freshTarget, r.verifier.dataKeysToIgnore)
		if compareErr != nil || !equal {
			result.Error = "target configuration or versions changed after planning; rerun verification"
			return result
		}
		failoverSource = freshTarget
	}
	dataPolicy := namespaceRepairDataPreserveTargetIgnored
	if clusterPlan.Action == namespaceReplicationRepairActionCreate {
		failoverSource = preparedSource
		dataPolicy = namespaceRepairDataExcludeIgnored
	}
	task, err := buildNamespaceReplicationRepairTask(
		preparedSource,
		failoverSource,
		r.verifier.dataKeysToIgnore,
		dataPolicy,
	)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	_, err = r.applyRepairAndReadback(ctx, connection.client, request, task, &result)
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func (r *namespaceReplicationRepairer) applyRepairAndReadback(
	ctx context.Context,
	client adminservice.AdminServiceClient,
	request namespaceReplicationRepairExecutionRequest,
	task *replicationspb.NamespaceTaskAttributes,
	result *namespaceReplicationRepairApplyResult,
) (*adminservice.GetNamespaceResponse, error) {
	outcome, err := r.applyRepairTask(ctx, client, request, task)
	if err != nil {
		return nil, err
	}
	result.Outcome = outcome.String()
	// Receiver retries can report a no-op after a committed write. Accept that
	// response only when readback proves the requested state and exact versions.
	if !acceptableNamespaceRepairOutcome(result.Action, outcome) {
		return nil, fmt.Errorf("unexpected apply outcome %s", outcome)
	}
	actual, err := getNamespace(ctx, r.verifier.rpcTimeout, client, namespaceReplicationSelector{ID: task.GetId()})
	if err != nil {
		return nil, fmt.Errorf("read back namespace: %w", err)
	}
	if err := validateNamespaceRepairReadback(task, actual, r.verifier.dataKeysToIgnore); err != nil {
		return nil, err
	}
	result.Readback = true
	return actual, nil
}

func (r *namespaceReplicationRepairer) applyRepairTask(
	ctx context.Context,
	client adminservice.AdminServiceClient,
	request namespaceReplicationRepairExecutionRequest,
	task *replicationspb.NamespaceTaskAttributes,
) (adminservice.ApplyNamespaceMutationResponse_Outcome, error) {
	payload, err := nsreplication.MarshalNamespaceTask(task)
	if err != nil {
		return adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED, err
	}
	response, err := namespaceReplicationRPCCall(
		ctx,
		r.verifier.rpcTimeout,
		func(rpcCtx context.Context) (*adminservice.ApplyNamespaceMutationResponse, error) {
			return client.ApplyNamespaceMutation(rpcCtx, &adminservice.ApplyNamespaceMutationRequest{
				SourceCluster:        request.Plan.SourceCluster,
				NamespaceTaskPayload: payload,
				Attribution: &adminservice.NamespaceMutationAttribution{
					Origin:   adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR,
					RepairId: request.Plan.RepairID,
					Reason:   request.Reason,
				},
			})
		},
	)
	if err != nil {
		return adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED, err
	}
	return response.GetOutcome(), nil
}

func acceptableNamespaceRepairOutcome(
	action string,
	outcome adminservice.ApplyNamespaceMutationResponse_Outcome,
) bool {
	switch action {
	case namespaceReplicationRepairActionCreate:
		return outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
	case namespaceReplicationRepairActionPrepareSource, namespaceReplicationRepairActionApplySource:
		return outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
	default:
		return false
	}
}

func validateNamespaceRepairReadback(
	expected *replicationspb.NamespaceTaskAttributes,
	actual *adminservice.GetNamespaceResponse,
	dataKeysToIgnore map[string]struct{},
) error {
	expectedSnapshot := &adminservice.GetNamespaceResponse{
		Info: proto.CloneOf(expected.GetInfo()), Config: expected.GetConfig(), ReplicationConfig: expected.GetReplicationConfig(),
		FailoverHistory: expected.GetFailoverHistory(), IsGlobalNamespace: true,
	}
	expectedSnapshot.Info.Id = expected.GetId()
	expectedConfigProjection, expectedFailoverProjection, err := namespaceReplicationProjections(expectedSnapshot, dataKeysToIgnore)
	if err != nil {
		return err
	}
	if !actual.GetIsGlobalNamespace() {
		return errors.New("readback namespace is not global")
	}
	actualConfigProjection, actualFailoverProjection, err := namespaceReplicationProjections(actual, dataKeysToIgnore)
	if err != nil {
		return err
	}
	if !proto.Equal(expectedConfigProjection, actualConfigProjection) {
		return errors.New("config readback does not match prepared source")
	}
	if !proto.Equal(expectedFailoverProjection, actualFailoverProjection) {
		return errors.New("failover readback changed during config repair")
	}
	if actual.GetConfigVersion() != expected.GetConfigVersion() {
		return fmt.Errorf("config version readback is %d, expected %d", actual.GetConfigVersion(), expected.GetConfigVersion())
	}
	if actual.GetFailoverVersion() != expected.GetFailoverVersion() {
		return fmt.Errorf("failover version readback is %d, expected %d", actual.GetFailoverVersion(), expected.GetFailoverVersion())
	}
	return nil
}

func authoritativeRepairPeers(result *namespaceReplicationVerificationResult) []string {
	peers := make([]string, 0, len(result.ExpectedClusters)-1)
	for _, cluster := range result.ExpectedClusters {
		if cluster != result.SourceCluster {
			peers = append(peers, cluster)
		}
	}
	slices.Sort(peers)
	return peers
}

func verifiedRepairPeers(observation *namespaceReplicationObservation) []string {
	if observation == nil || observation.result == nil || observation.sourceSnapshot == nil || !observation.sourceStable {
		return nil
	}
	sourceVersion := observation.sourceSnapshot.GetConfigVersion()
	var peers []string
	for _, cluster := range observation.result.Clusters {
		if cluster.Role == "SOURCE" || cluster.Presence != namespaceReplicationPresencePresent ||
			cluster.ConfigVersion == nil || *cluster.ConfigVersion > sourceVersion ||
			cluster.ConfigMatch != namespaceReplicationMatchMatch ||
			namespaceReplicationFailoverBlocks(&cluster, observation.sourceSnapshot.GetFailoverVersion()) {
			continue
		}
		peers = append(peers, cluster.Cluster)
	}
	slices.Sort(peers)
	return peers
}
