package tests

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
)

var (
	Version = int64(1234)

	NamespaceID                              = namespace.ID("deadbeef-0123-4567-890a-bcdef0123456")
	Namespace                                = namespace.Name("mock namespace name")
	ParentNamespaceID                        = namespace.ID("deadbeef-0123-4567-890a-bcdef0123457")
	ParentNamespace                          = namespace.Name("mock parent namespace name")
	TargetNamespaceID                        = namespace.ID("deadbeef-0123-4567-890a-bcdef0123458")
	TargetNamespace                          = namespace.Name("mock target namespace name")
	ChildNamespaceID                         = namespace.ID("deadbeef-0123-4567-890a-bcdef0123459")
	ChildNamespace                           = namespace.Name("mock child namespace name")
	StandbyNamespaceID                       = namespace.ID("deadbeef-0123-4567-890a-bcdef0123460")
	StandbyNamespace                         = namespace.Name("mock standby namespace name")
	StandbyWithVisibilityArchivalNamespaceID = namespace.ID("deadbeef-0123-4567-890a-bcdef0123461")
	StandbyWithVisibilityArchivalNamespace   = namespace.Name("mock standby with visibility archival namespace name")
	MissedNamespaceID                        = namespace.ID("missed-namespace-id")
	MissedNamespace                          = namespace.Name("missed-namespace-name")
	WorkflowID                               = "mock-workflow-id"
	RunID                                    = "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6"
	WorkflowKey                              = definition.NewWorkflowKey(NamespaceID.String(), WorkflowID, RunID)
	ArchetypeID                              = chasm.ArchetypeID(1234)

	LocalNamespaceEntry = namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: NamespaceID.String(), Name: Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(1),
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{
					"lololol": nil},
			},
		},
		cluster.TestCurrentClusterName,
	)

	GlobalNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: NamespaceID.String(), Name: Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(1),
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "test:///visibility/archival",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalParentNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: ParentNamespaceID.String(), Name: ParentNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalTargetNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: TargetNamespaceID.String(), Name: TargetNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalStandbyNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: StandbyNamespaceID.String(), Name: StandbyNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalStandbyWithVisibilityArchivalNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   StandbyWithVisibilityArchivalNamespaceID.String(),
			Name: StandbyWithVisibilityArchivalNamespace.String(),
		},
		&persistencespb.NamespaceConfig{
			Retention:               timestamp.DurationFromDays(1),
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "test:///visibility/archival",
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestAlternativeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	GlobalChildNamespaceEntry = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: ChildNamespaceID.String(), Name: ChildNamespace.String()},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		Version,
	)

	CreateWorkflowExecutionResponse = &persistence.CreateWorkflowExecutionResponse{
		NewMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}

	GetWorkflowExecutionResponse = &persistence.GetWorkflowExecutionResponse{
		MutableStateStats: persistence.MutableStateStatistics{},
	}

	UpdateWorkflowExecutionResponse = &persistence.UpdateWorkflowExecutionResponse{
		UpdateMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
		NewMutableStateStats: nil,
	}
)

func NewDynamicConfig() *configs.Config {
	dc := dynamicconfig.NewNoopCollection()
	config := configs.NewConfig(dc, 1)
	config.EnableActivityEagerExecution = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.NamespaceCacheRefreshInterval = dynamicconfig.GetDurationPropertyFn(time.Second)
	config.ReplicationEnableUpdateWithNewTaskMerge = dynamicconfig.GetBoolPropertyFn(true)
	config.EnableWorkflowIdReuseStartTimeValidation = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	config.EnableTransitionHistory = dynamicconfig.GetBoolPropertyFn(true)
	config.EnableChasm = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
	return config
}
