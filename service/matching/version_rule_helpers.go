package matching

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/util"
)

// checkAssignmentConditions returns an error if the new set of assignment rules don't meet the following requirements:
// - No more rules than dynamicconfig.VersionAssignmentRuleLimitPerQueue
// - Once assignment rules have been added, there must always be at least one "unfiltered" assignment rule, which can accept any build id
// - No assignment rule's TargetBuildId can be a member of an existing version set
func checkAssignmentConditions(g *persistencepb.VersioningData, maxARs int, force bool) error {
	if force == true {
		return nil
	}
	rules := g.GetAssignmentRules()
	if maxARs > 0 && len(rules) > maxARs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of assignment rules permitted in namespace dynamic config (%v/%v)", len(rules), maxARs))
	}
	foundUnfiltered := false
	for _, dbRule := range rules {
		rule := dbRule.GetRule()
		if dbRule.DeleteTimestamp == nil && isUnfiltered(rule) {
			foundUnfiltered = true
		}
		tbid := rule.GetTargetBuildId()
		if dbRule.DeleteTimestamp == nil && isInVersionSet(tbid, g.GetVersionSets()) {
			return serviceerror.NewFailedPrecondition(fmt.Sprintf("update breaks requirement, target build id %s is already a member of version set", tbid))
		}
	}
	if !foundUnfiltered {
		return serviceerror.NewFailedPrecondition("update breaks requirement that at least one assignment rule must have no ramp or hint")
	}
	return nil
}

// isUnfiltered returns true if the assignment rule has no hint, no worker ratio ramp, and no ramp percentage, or a ramp percentage of 100
func isUnfiltered(ar *taskqueue.BuildIdAssignmentRule) bool {
	percentageRamp := ar.GetPercentageRamp()
	return ar.GetHintFilter() == "" &&
		ar.GetWorkerRatioRamp() == nil &&
		(percentageRamp == nil || (percentageRamp != nil && percentageRamp.RampPercentage == 100))
}

// isInVersionSet returns true if the build id is in any of the listed version sets
func isInVersionSet(tbid string, sets []*persistencepb.CompatibleVersionSet) bool {
	for _, set := range sets {
		for _, bid := range set.BuildIds {
			if bid.GetId() == tbid {
				return true
			}
		}
	}
	return false
}

func InsertAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule,
	maxARs int) (*persistencepb.VersioningData, error) {
	if data == nil {
		data = &persistencepb.VersioningData{AssignmentRules: make([]*persistencepb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	if req.GetRuleIndex() < 0 {
		return nil, serviceerror.NewInvalidArgument("rule index cannot be negative")
	}
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	slices.Insert(data.GetAssignmentRules(), int(req.GetRuleIndex()), &persistenceAR)
	return data, checkAssignmentConditions(data, maxARs, false)
}

func ReplaceAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule) (*persistencepb.VersioningData, error) {
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
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	slices.Replace(data.AssignmentRules, idx, idx+1, &persistenceAR)
	return data, checkAssignmentConditions(data, 0, req.GetForce())
}

func DeleteAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule) (*persistencepb.VersioningData, error) {
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
	rule := rules[idx]
	rule.DeleteTimestamp = timestamp
	return data, checkAssignmentConditions(data, 0, req.GetForce())
}

// checkRedirectConditions returns an error if there is no room for the new Redirect Rule, or if it causes the
// queue to fail the other requirements
func checkRedirectConditions(g *persistencepb.VersioningData, maxRRs int) error {
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
func trimRedirectRules(rules []*persistencepb.RedirectRule) int {
	// todo: redirect rule PR
	return 0
}

func isCyclic(rules []*persistencepb.RedirectRule) bool {
	// todo: redirect rule PR
	return true
}

func InsertCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule,
	maxRRs int) (*persistencepb.VersioningData, error) {
	if data == nil {
		data = &persistencepb.VersioningData{RedirectRules: make([]*persistencepb.RedirectRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	persistenceCRR := persistencepb.RedirectRule{
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
	return data, checkRedirectConditions(data, maxRRs)
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a replace call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetRedirectRules()
	persistenceRR := persistencepb.RedirectRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	src := req.GetRule().GetSourceBuildId()
	found := false
	for i, rule := range rules {
		if rule.GetRule().GetSourceBuildId() == src {
			found = true
			slices.Replace(rules, i, i+1, &persistenceRR)
		}
	}
	if !found {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("no rule found with source Build ID %s", src))
	}
	return data, checkRedirectConditions(data, 0)
}

func DeleteCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	if data == nil {
		return nil, serviceerror.NewInvalidArgument("data cannot be nil for a delete call")
	} else {
		data = common.CloneProto(data)
	}
	rules := data.GetRedirectRules()
	src := req.GetSourceBuildId()
	found := false
	for _, rule := range rules {
		if rule.GetRule().GetSourceBuildId() == src {
			found = true
			rule.DeleteTimestamp = timestamp
		}
	}
	if !found {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("no rule found with source Build ID %s", src))
	}
	return data, checkRedirectConditions(data, 0)
}

// CleanupRuleTombstones clears all deleted rules from versioning data if the rule was deleted more than
// retentionTime ago. Clones data to avoid mutating in place.
func CleanupRuleTombstones(versioningData *persistencepb.VersioningData, retentionTime time.Duration) *persistencepb.VersioningData {
	modifiedData := shallowCloneVersioningData(versioningData)
	modifiedData.AssignmentRules = util.FilterSlice(modifiedData.GetAssignmentRules(), func(ar *persistencepb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil || (ar.DeleteTimestamp != nil && hlc.Since(ar.DeleteTimestamp) > retentionTime)
	})
	modifiedData.RedirectRules = util.FilterSlice(modifiedData.GetRedirectRules(), func(rr *persistencepb.RedirectRule) bool {
		return rr.DeleteTimestamp == nil || (rr.DeleteTimestamp != nil && hlc.Since(rr.DeleteTimestamp) > retentionTime)
	})
	return modifiedData
}
