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

// returns error if there is no room for the new AssignmentRule, or if it causes the queue to fail the other requirements
func checkARConditions(g *persistencespb.VersioningData, maxARs int, force bool) error {
	if force == true {
		return nil
	}
	rules := g.GetAssignmentRules()
	if maxARs > 0 && len(rules) > maxARs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of assignment rules permitted in namespace dynamic config (%v/%v)", len(rules), maxARs))
	}

	for _, pRule := range rules {
		rule := pRule.GetRule()
		if rule.GetHintFilter() == "" && rule.GetPercentageRamp() == nil && rule.GetWorkerRatioRamp() == nil {
			// todo: check whether == nil is enough, or if it should be == 100
			return nil
		}
	}
	return serviceerror.NewFailedPrecondition("update breaks requirement that at least one assignment rule must have no ramp or hint")
}

func InsertAssignmentRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule, maxARs int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{AssignmentRules: make([]*persistencespb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	if req.GetRuleIndex() < 0 {
		return nil, serviceerror.NewInvalidArgument("rule index cannot be negative")
	}
	persistenceAR := persistencespb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	slices.Insert(data.GetAssignmentRules(), int(req.GetRuleIndex()), &persistenceAR)
	return data, checkARConditions(data, maxARs, false)
}

func ReplaceAssignmentRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a replace call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetAssignmentRules()
	idx := int(req.GetRuleIndex())
	if len(rules) > idx+1 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("rule index %d is out of bounds for assignment rule list of length %d", idx, len(rules)))
	}
	persistenceAR := persistencespb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	slices.Replace(data.AssignmentRules, idx, idx+1, &persistenceAR)
	return data, checkARConditions(data, 0, req.GetForce())
}

func DeleteAssignmentRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule) (*persistencespb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a delete call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetAssignmentRules()
	idx := int(req.GetRuleIndex())
	if len(rules) > idx+1 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("rule index %d is out of bounds for assignment rule list of length %d", idx, len(rules)))
	}
	slices.Delete(data.AssignmentRules, idx, idx+1) // todo: are we actually deleting this? or just doing something with the delete timestamp?
	return data, checkARConditions(data, 0, req.GetForce())
}

// returns error if there is no room for the new Redirect Rule, or if it causes the queue to fail the other requirements
func checkCRRConditions(g *persistencespb.VersioningData, maxRRs int) error {
	rules := g.GetRedirectRules()
	if maxRRs > 0 && len(rules) > maxRRs {
		if trimRedirectRules(rules) < 1 {
			return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of redirect rules permitted in namespace dynamic config (%v/%v)", len(rules), maxRRs))
		}
	}
	if isCyclic(rules) {
		return serviceerror.NewFailedPrecondition("update breaks requirement that at least one assignment rule must have no ramp or hint")
	}
	return nil
}

// trimRedirectRules attempts to trim the DAG of redirect rules. It returns the number of rules it was able to delete.
func trimRedirectRules(rules []*persistencespb.RedirectRule) int {
	// todo
	return 0
}

func isCyclic(rules []*persistencespb.RedirectRule) bool {
	// todo
	return true
}

func InsertCompatibleRedirectRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule, maxCRRs int) (*persistencespb.VersioningData, error) {
	if data == nil {
		data = &persistencespb.VersioningData{RedirectRules: make([]*persistencespb.RedirectRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	persistenceCRR := persistencespb.RedirectRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	src := req.GetRule().GetSourceBuildId()
	rules := data.GetRedirectRules()
	for _, rule := range rules {
		if rule.GetRule().GetSourceBuildId() == src {
			return nil, serviceerror.NewAlreadyExist(fmt.Sprintf(
				"there can only be one redirect rule per distinct source ID. source %s already redirects to target %s", src, rule.GetRule().GetTargetBuildId(),
			))
		}
	}
	slices.Insert(data.GetRedirectRules(), 0, &persistenceCRR)
	return data, checkCRRConditions(data, maxCRRs)
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule) (*persistencespb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a replace call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetRedirectRules()
	persistenceCRR := persistencespb.RedirectRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	src := req.GetRule().GetSourceBuildId()
	found := false
	for i, rule := range rules {
		if rule.GetRule().GetSourceBuildId() == src {
			found = true
			slices.Replace(rules, i, i+1, &persistenceCRR)
		}
	}
	if !found {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("no rule found with source Build ID %s", src))
	}
	return data, checkCRRConditions(data, 0)
}

func DeleteCompatibleRedirectRule(timestamp *hlc.Clock, data *persistencespb.VersioningData, req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule) (*persistencespb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a delete call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetRedirectRules()
	src := req.GetSourceBuildId()
	found := false
	for i, rule := range rules {
		if rule.GetRule().GetSourceBuildId() == src {
			found = true
			slices.Delete(rules, i, i+1)
			// todo: are we actually deleting this? or just doing something with the delete timestamp?
		}
	}
	if !found {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("no rule found with source Build ID %s", src))
	}
	return data, checkCRRConditions(data, 0)
}
