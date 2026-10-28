package interceptor

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.uber.org/mock/gomock"
)

type (
	noopDCRedirectionPolicySuite struct {
		suite.Suite
		*require.Assertions

		currentClusterName string
		policy             *NoopRedirectionPolicy
	}

	selectedAPIsForwardingRedirectionPolicySuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockClusterMetadata *cluster.MockMetadata
		mockNamespaceCache  *namespace.MockRegistry

		namespace              namespace.Name
		namespaceID            namespace.ID
		currentClusterName     string
		alternativeClusterName string
		forwardingEnabled      dynamicconfig.BoolPropertyFnWithNamespaceFilter

		policy *SelectedAPIsForwardingRedirectionPolicy
	}
)

func TestNoopDCRedirectionPolicySuite(t *testing.T) {
	s := new(noopDCRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *noopDCRedirectionPolicySuite) SetupSuite() {
}

func (s *noopDCRedirectionPolicySuite) TearDownSuite() {

}

func (s *noopDCRedirectionPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.currentClusterName = cluster.TestCurrentClusterName
	s.policy = NewNoopRedirectionPolicy(s.currentClusterName)
}

func (s *noopDCRedirectionPolicySuite) TearDownTest() {

}

func (s *noopDCRedirectionPolicySuite) TestWithNamespaceRedirect() {
	namespaceName := namespace.Name("some random namespace name")
	namespaceID := namespace.ID("some random namespace ID")
	apiName := api.WorkflowServicePrefix + "AnyRandomAPIName"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), namespaceID, apiName, nil, callFn)
	s.NoError(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), namespaceName, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, callCount)
}

func TestSelectedAPIsForwardingRedirectionPolicySuite(t *testing.T) {
	s := new(selectedAPIsForwardingRedirectionPolicySuite)
	suite.Run(t, s)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) SetupSuite() {
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TearDownSuite() {

}

func (s *selectedAPIsForwardingRedirectionPolicySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)

	s.namespace = "some random namespace name"
	s.namespaceID = "deadd0d0-c001-face-d00d-000000000000"
	s.currentClusterName = cluster.TestCurrentClusterName
	s.alternativeClusterName = cluster.TestAlternativeClusterName
	s.forwardingEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.policy = NewSelectedAPIsForwardingPolicy(
		s.currentClusterName,
		func(ns string) bool { return s.forwardingEnabled(ns) },
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		s.mockNamespaceCache,
	)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestSelectedAPIs() {
	s.Equal(map[string]struct{}{
		// Workflow APIs
		wfMethod("StartWorkflowExecution"):           {},
		wfMethod("SignalWithStartWorkflowExecution"): {},
		wfMethod("SignalWorkflowExecution"):          {},
		wfMethod("UpdateWorkflowExecution"):          {},
		wfMethod("RequestCancelWorkflowExecution"):   {},
		wfMethod("TerminateWorkflowExecution"):       {},
		wfMethod("PauseWorkflowExecution"):           {},
		wfMethod("UnpauseWorkflowExecution"):         {},
		wfMethod("ResetWorkflowExecution"):           {},
		wfMethod("DeleteWorkflowExecution"):          {},
		wfMethod("QueryWorkflow"):                    {},
		wfMethod("ExecuteMultiOperation"):            {},
		// Kept because the bare key this list used to have covered AdminService's
		// same-named method too, not because a request reaches it.
		api.AdminServicePrefix + "DeleteWorkflowExecution": {},

		// Standalone Activity APIs
		wfMethod("StartActivityExecution"):         {},
		wfMethod("RequestCancelActivityExecution"): {},
		wfMethod("TerminateActivityExecution"):     {},
		wfMethod("DeleteActivityExecution"):        {},
		wfMethod("PauseActivityExecution"):         {},
		wfMethod("UnpauseActivityExecution"):       {},
		wfMethod("ResetActivityExecution"):         {},
		wfMethod("UpdateActivityExecutionOptions"): {},

		// Standalone Nexus Operation APIs
		wfMethod("StartNexusOperationExecution"):         {},
		wfMethod("RequestCancelNexusOperationExecution"): {},
		wfMethod("TerminateNexusOperationExecution"):     {},
		wfMethod("DeleteNexusOperationExecution"):        {},
	}, selectedAPIsForwardingRedirectionPolicyAllowedAPIs)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_LocalNamespace() {
	s.setupLocalNamespace()

	apiName := api.WorkflowServicePrefix + "AnyRandomAPIName"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
	s.NoError(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_OneReplicationCluster() {
	s.setupGlobalNamespaceWithOneReplicationCluster()

	apiName := api.WorkflowServicePrefix + "AnyRandomAPIName"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
	s.NoError(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_NoForwarding_NamespaceNotWhiltelisted() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(false, true)

	apiName := api.WorkflowServicePrefix + "AnyRandomAPIName"
	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
	s.NoError(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestWithNamespaceRedirect_GlobalNamespace_NoForwarding_APINotAllowed() {
	// Active cluster is alternative, but not allow-listed APIs must stay local.
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
		apiName += "_notallowed"
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
		s.NoError(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
		s.NoError(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_CurrentCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, true)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
		s.NoError(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
		s.NoError(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_AlternativeCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.alternativeClusterName, targetCluster)
		return nil
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
		s.NoError(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
		s.NoError(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_OneCluster() {
	s.setupGlobalNamespaceWithOneCluster(false)
	callCount := len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs) * 2

	testcases := []struct {
		name              string
		forwardingEnabled bool
		selectedAPIsOnly  bool
		apiAllowed        bool
		expectedCallCount map[string]int
	}{
		{
			name:              "Forwarding disabled",
			expectedCallCount: map[string]int{s.currentClusterName: callCount},
		},
		{
			name:              "Forwarding enabled, all APIs enabled",
			forwardingEnabled: true,
			selectedAPIsOnly:  false,
			expectedCallCount: map[string]int{s.alternativeClusterName: callCount},
		},
		{
			name:              "Forwarding enabled, all APIs disabled, API not allow-listed",
			forwardingEnabled: true,
			selectedAPIsOnly:  true,
			expectedCallCount: map[string]int{s.currentClusterName: callCount},
		},
		{
			name:              "Forwarding enabled, all APIs disabled, API allow-listed",
			forwardingEnabled: true,
			selectedAPIsOnly:  true,
			apiAllowed:        true,
			expectedCallCount: map[string]int{s.alternativeClusterName: callCount},
		},
	}

	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			s.forwardingEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(tc.forwardingEnabled)
			s.policy.selectedAPIsOnly = tc.selectedAPIsOnly

			callCountByCluster := make(map[string]int)
			callFn := func(targetCluster string) error {
				callCountByCluster[targetCluster]++
				return nil
			}

			for fullMethod := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
				if !tc.apiAllowed {
					fullMethod += "_notallowed"
				}
				err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, fullMethod, nil, callFn)
				s.NoError(err)

				err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, fullMethod, nil, callFn)
				s.NoError(err)
			}

			s.Equal(tc.expectedCallCount, callCountByCluster)
		})
	}
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_CurrentClusterToAlternativeCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, true)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return serviceerror.NewNamespaceNotActive("", s.currentClusterName, s.alternativeClusterName)
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return nil
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
		s.NoError(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
		s.NoError(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_AlternativeClusterToCurrentCluster() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return nil
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return serviceerror.NewNamespaceNotActive("", s.alternativeClusterName, s.currentClusterName)
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	for apiName := range selectedAPIsForwardingRedirectionPolicyAllowedAPIs {
		err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
		s.NoError(err)

		err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
		s.NoError(err)
	}

	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), currentClustercallCount)
	s.Equal(2*len(selectedAPIsForwardingRedirectionPolicyAllowedAPIs), alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_Forwarding_AlternativeClusterToCurrentCluster_AllAPIs() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)
	s.policy.selectedAPIsOnly = false

	currentClustercallCount := 0
	alternativeClustercallCount := 0
	callFn := func(targetCluster string) error {
		switch targetCluster {
		case s.currentClusterName:
			currentClustercallCount++
			return nil
		case s.alternativeClusterName:
			alternativeClustercallCount++
			return serviceerror.NewNamespaceNotActive("", s.alternativeClusterName, s.currentClusterName)
		default:
			panic(fmt.Sprintf("unknown cluster name %v", targetCluster))
		}
	}

	apiName := api.WorkflowServicePrefix + "NotExistRandomAPI"
	err := s.policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
	s.NoError(err)

	err = s.policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, currentClustercallCount)
	s.Equal(2, alternativeClustercallCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) TestGetTargetDataCenter_GlobalNamespace_SelectedForNSOverridesEnableForAllAPIs() {
	s.setupGlobalNamespaceWithTwoReplicationCluster(true, false)
	policy := NewAllAPIsForwardingPolicy(
		s.currentClusterName,
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		s.mockNamespaceCache,
	)

	callCount := 0
	callFn := func(targetCluster string) error {
		callCount++
		s.Equal(s.currentClusterName, targetCluster)
		return nil
	}

	apiName := "NotAllowedAPI"
	err := policy.WithNamespaceIDRedirect(context.Background(), s.namespaceID, apiName, nil, callFn)
	s.NoError(err)
	err = policy.WithNamespaceRedirect(context.Background(), s.namespace, apiName, nil, callFn)
	s.NoError(err)

	s.Equal(2, callCount)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupLocalNamespace() {
	namespaceEntry := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: s.namespaceID.String(), Name: s.namespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		cluster.TestCurrentClusterName,
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalNamespaceWithOneReplicationCluster() {
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: s.namespaceID.String(), Name: s.namespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234, // not used
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalNamespaceWithTwoReplicationCluster(forwardingEnabled bool, isRecordActive bool) {
	activeCluster := s.alternativeClusterName
	if isRecordActive {
		activeCluster = s.currentClusterName
	}
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: s.namespaceID.String(), Name: s.namespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234, // not used
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
	s.forwardingEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(forwardingEnabled)
}

func (s *selectedAPIsForwardingRedirectionPolicySuite) setupGlobalNamespaceWithOneCluster(isRecordActive bool) {
	activeCluster := s.alternativeClusterName
	if isRecordActive {
		activeCluster = s.currentClusterName
	}
	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: s.namespaceID.String(), Name: s.namespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeCluster,
			Clusters: []string{
				activeCluster,
			},
		},
		1234, // not used
	)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(namespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(s.namespace).Return(namespaceEntry, nil).AnyTimes()
}
