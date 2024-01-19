package matching

import (
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
)

func InsertAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule, maxSets, maxBuildIds int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{AssignmentRules: make([]*persistencespb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	data, err := updateAssignmentRuleImpl(clock, data, req)
	if err != nil {
		return nil, err
	}
	return data, checkAssignmentRuleLimits(data, maxSets, maxBuildIds)
}

func updateAssignmentRuleImpl(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

// returns false if there is no room for the new AssignmentRule
func checkAssignmentRuleLimits(g *persistencespb.VersioningData, maxSets, maxBuildIds int) error {
	return nil
}

func ReplaceAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

func DeleteAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

func InsertCompatibleRedirectRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule, maxSets, maxBuildIds int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{AssignmentRules: make([]*persistencespb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	return data, nil
}

func ReplaceCompatibleRedirectRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

func DeleteCompatibleRedirectRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}
