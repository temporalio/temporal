package wci

import (
	"testing"

	"github.com/stretchr/testify/require"
	wciconfig "go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

func regionCollection(region string) *dynamicconfig.Collection {
	return dynamicconfig.NewCollection(dynamicconfig.StaticClient{HostRegion.Key(): region}, log.NewNoopLogger())
}

func TestHostRegionFn(t *testing.T) {
	require.Equal(t, "aws-us-east-1", hostRegionFn(&wciconfig.StaticConfig{RegionID: "aws-us-east-1"}, regionCollection("aws-us-west-2"))(), "StaticConfig wins")
	require.Equal(t, "aws-us-west-2", hostRegionFn(nil, regionCollection("aws-us-west-2"))(), "dynamic config without StaticConfig")
	require.Empty(t, hostRegionFn(nil, nil)())
}

// Every handler that resolves or records the region takes it from dynamic config when there is no StaticConfig.
func TestHandlersTakeHostRegionFromDynamicConfig(t *testing.T) {
	dc := regionCollection("aws-us-west-2")
	for name, region := range map[string]func() string{
		"handler":           newHandler(HandlerOptions{DynamicConfig: dc}).hostRegion,
		"fetchSnapshot":     NewFetchSnapshotTaskHandler(SnapshotTaskHandlerOptions{DynamicConfig: dc}).hostRegion,
		"deferredDecision":  NewDeferredDecisionTaskHandler(SnapshotTaskHandlerOptions{DynamicConfig: dc}).hostRegion,
		"resize":            NewResizeTaskHandler(ComputeTaskHandlerOptions{DynamicConfig: dc}).hostRegion,
		"migrateToWorkflow": NewMigrateToWorkflowTaskHandler(MigrateToWorkflowTaskHandlerOptions{DynamicConfig: dc}).hostRegion,
	} {
		require.Equal(t, "aws-us-west-2", region(), name)
	}
}
