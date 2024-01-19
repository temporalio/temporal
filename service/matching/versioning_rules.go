package matching

import (
	"fmt"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"slices"
)

func InsertAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule, maxARs int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{AssignmentRules: make([]*persistencespb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	persistenceAR := persistencespb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: clock,
		DeleteTimestamp: nil,
	}
	slices.Insert(data.AssignmentRules, int(req.GetRuleIndex()), &persistenceAR)
	return data, checkAssignmentRuleLimits(data, maxARs)
}

// returns false if there is no room for the new AssignmentRule
func checkAssignmentRuleLimits(g *persistencespb.VersioningData, maxARs int) error {
	rules := g.GetAssignmentRules()
	if maxARs > 0 && len(rules) > maxARs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of compatible version sets permitted in namespace dynamic config (%v/%v)", len(rules), maxARs))
	}
	return nil
}

func ReplaceAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

func DeleteAssignmentRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	return nil, nil
}

func InsertCompatibleRedirectRule(clock *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule, maxCRRs int) (*persistencespb.VersioningData, error) {
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
