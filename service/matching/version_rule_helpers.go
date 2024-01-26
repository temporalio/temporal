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
// - No assignment rule's TargetBuildId can be a member of an existing version set
// - If there existed an "unfiltered" assigment rule (which can accept any build id), at least one must still exist
// - To override the unfiltered assignment rule requirement, the user can specify force = true
func checkAssignmentConditions(g *persistencepb.VersioningData, maxARs int, force, hadUnfiltered bool) error {
	activeRules := getActiveRules(slices.Clone(g.GetAssignmentRules()))
	if cnt := len(activeRules); maxARs > 0 && cnt > maxARs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of assignment rules permitted in namespace (%v/%v)", cnt, maxARs))
	}
	if tbid, ok := isInVersionSet(activeRules, g.GetVersionSets()); ok {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update breaks requirement, target build id %s is already a member of version set", tbid))
	}
	if force == true {
		return nil
	}
	if hadUnfiltered && !containsUnfiltered(activeRules) {
		return serviceerror.NewFailedPrecondition("update breaks requirement that at least one assignment rule must have no ramp or hint")
	}
	return nil
}

func getActiveRules(rules []*persistencepb.AssignmentRule) []*persistencepb.AssignmentRule {
	return util.FilterSlice(rules, func(ar *persistencepb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil
	})
}

// containsUnfiltered returns true if there exists an assignment rule with no hint, no worker ratio ramp, and no ramp percentage, or a ramp percentage of 100
func containsUnfiltered(rules []*persistencepb.AssignmentRule) bool {
	isUnfiltered := func(ar *taskqueue.BuildIdAssignmentRule) bool {
		percentageRamp := ar.GetPercentageRamp()
		return ar.GetHintFilter() == "" &&
			ar.GetWorkerRatioRamp() == nil &&
			(percentageRamp == nil || (percentageRamp != nil && percentageRamp.RampPercentage == 100))
	}
	found := false
	for _, rule := range rules {
		ar := rule.GetRule()
		if isUnfiltered(ar) {
			found = true
		}
	}
	return found
}

// isInVersionSet returns true if the target build id of any assignment rule is in any of the listed version sets
func isInVersionSet(rules []*persistencepb.AssignmentRule, sets []*persistencepb.CompatibleVersionSet) (string, bool) {
	for _, rule := range rules {
		ar := rule.GetRule()
		tbid := ar.GetTargetBuildId()
		for _, set := range sets {
			for _, bid := range set.BuildIds {
				if bid.GetId() == tbid {
					return tbid, true
				}
			}
		}
	}
	return "", false
}

// given2ActualIdx takes in the user-given index, which only counts active assignment rules, and converts it to the
// actual index of that rule in the assignment rule list, which includes deleted rules.
// A negative return value means index out of bounds.
func given2ActualIdx(idx int32, rules []*persistencepb.AssignmentRule) int {
	for i, rule := range rules {
		if rule.DeleteTimestamp == nil {
			if idx == 0 {
				return i
			}
			idx--
		}
	}
	return -1
}

func InsertAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule,
	maxARs int,
	hadUnfiltered bool) (*persistencepb.VersioningData, error) {
	if req.GetRuleIndex() < 0 {
		return nil, serviceerror.NewInvalidArgument("rule index cannot be negative")
	}
	if data == nil {
		data = &persistencepb.VersioningData{AssignmentRules: make([]*persistencepb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	rules := data.GetAssignmentRules()

	if actualIdx := given2ActualIdx(req.GetRuleIndex(), rules); actualIdx < 0 {
		// given index was too large, insert at end
		rules = append(rules, &persistenceAR)
	} else {
		slices.Insert(rules, actualIdx, &persistenceAR)
	}

	return data, checkAssignmentConditions(data, maxARs, false, hadUnfiltered)
}

func ReplaceAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule,
	hadUnfiltered bool) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	rules := data.GetAssignmentRules()
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveRules(rules))))
	}
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            req.GetRule(),
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	slices.Replace(data.AssignmentRules, actualIdx, actualIdx+1, &persistenceAR)
	return data, checkAssignmentConditions(data, 0, req.GetForce(), hadUnfiltered)
}

func DeleteAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule,
	hadUnfiltered bool) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	rules := data.GetAssignmentRules()
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveRules(rules))))
	}
	rule := rules[actualIdx]
	rule.DeleteTimestamp = timestamp
	return data, checkAssignmentConditions(data, 0, req.GetForce(), hadUnfiltered)
}

// checkRedirectConditions returns an error if there is no room for the new Redirect Rule, or if it causes the
// queue to fail the other requirements
func checkRedirectConditions(g *persistencepb.VersioningData, maxRRs int) error {
	return nil
}

func countActiveRR(rules []*persistencepb.RedirectRule) int {
	return 0
}

// trimRedirectRules attempts to trim the DAG of redirect rules. It returns the number of rules it was able to delete.
func trimRedirectRules(rules []*persistencepb.RedirectRule) int {
	return 0
}

func isCyclic(rules []*persistencepb.RedirectRule) bool {
	return false
}

func InsertCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule,
	maxRRs int) (*persistencepb.VersioningData, error) {
	return nil, nil
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	return nil, nil
}

func DeleteCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	return nil, nil
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
