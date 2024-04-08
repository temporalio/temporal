// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package matching

import (
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/dgryski/go-farm"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/util"
)

func cloneOrMkData(data *persistencespb.VersioningData) *persistencespb.VersioningData {
	if data == nil {
		return &persistencespb.VersioningData{
			AssignmentRules: make([]*persistencespb.AssignmentRule, 0),
			RedirectRules:   make([]*persistencespb.RedirectRule, 0),
		}
	}
	return common.CloneProto(data)
}

func InsertAssignmentRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule,
	maxAssignmentRules int) (*persistencespb.VersioningData, error) {
	if req.GetRuleIndex() < 0 {
		return nil, serviceerror.NewInvalidArgument("rule index cannot be negative")
	}
	rule := req.GetRule()
	if ramp := rule.GetPercentageRamp(); !validRamp(ramp) {
		return nil, serviceerror.NewInvalidArgument("ramp percentage must be in range [0, 100)")
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, target build id is already a member of a version set")
	}
	if rule.GetRamp() != nil && isActiveRedirectRuleSource(target, data.GetRedirectRules()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, this target build id cannot have a ramp because it is the source of a redirect rule")
	}
	data = cloneOrMkData(data)
	rules := data.GetAssignmentRules()
	persistenceAR := persistencespb.AssignmentRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	if actualIdx := given2ActualIdx(req.GetRuleIndex(), rules); actualIdx < 0 {
		// given index was too large, insert at end
		data.AssignmentRules = append(rules, &persistenceAR)
	} else {
		data.AssignmentRules = slices.Insert(rules, actualIdx, &persistenceAR)
	}
	return data, checkAssignmentConditions(data, maxAssignmentRules, false)
}

func ReplaceAssignmentRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule,
) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rule := req.GetRule()
	if ramp := rule.GetPercentageRamp(); !validRamp(ramp) {
		return nil, serviceerror.NewInvalidArgument("ramp percentage must be in range [0, 100)")
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, target build id is already a member of a version set")
	}
	if rule.GetRamp() != nil && isActiveRedirectRuleSource(target, data.GetRedirectRules()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, this target build id cannot have a ramp because it is the source of a redirect rule")
	}
	rules := data.GetAssignmentRules()
	hadUnconditional := containsUnconditional(rules)
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveAssignmentRules(rules))))
	}
	rules[actualIdx].DeleteTimestamp = timestamp
	data.AssignmentRules = slices.Insert(rules, actualIdx, &persistencespb.AssignmentRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	})
	return data, checkAssignmentConditions(data, 0, hadUnconditional && !req.GetForce())
}

func DeleteAssignmentRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteBuildIdAssignmentRule,
) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rules := data.GetAssignmentRules()
	hadUnconditional := containsUnconditional(rules)
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 || actualIdx > len(rules)-1 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveAssignmentRules(rules))))
	}
	rules[actualIdx].DeleteTimestamp = timestamp
	return data, checkAssignmentConditions(data, 0, hadUnconditional && !req.GetForce())
}

func AddCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule,
	maxRedirectRules int) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rule := req.GetRule()
	source := rule.GetSourceBuildId()
	if isInVersionSets(source, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, resource build ID is already a member of a version set")
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, target build ID is already a member of a version set")
	}
	if isConditionalAssignmentRuleTarget(source, data.GetAssignmentRules()) {
		return nil, serviceerror.NewFailedPrecondition(
			"redirect rule source build ID cannot be the target of any assignment rule with non-nil ramp")
	}
	rules := data.GetRedirectRules()
	for _, r := range rules {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == source {
			return nil, serviceerror.NewAlreadyExist(fmt.Sprintf(
				"cannot insert: source %s already redirects to target %s",
				source, r.GetRule().GetTargetBuildId(),
			))
		}
	}
	data.RedirectRules = slices.Insert(rules, 0, &persistencespb.RedirectRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	})
	return data, checkRedirectConditions(data, maxRedirectRules)
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule,
) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rule := req.GetRule()
	source := rule.GetSourceBuildId()
	if isInVersionSets(source, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, resource build ID is already a member of a version set")
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			"update breaks requirement, target build ID is already a member of a version set")
	}
	rules := data.GetRedirectRules()
	for _, r := range rules {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == source {
			r.DeleteTimestamp = timestamp
			data.RedirectRules = slices.Insert(rules, 0, &persistencespb.RedirectRule{
				Rule:            rule,
				CreateTimestamp: timestamp,
				DeleteTimestamp: nil,
			})
			return data, checkRedirectConditions(data, 0)
		}
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("cannot replace: no redirect rule found with source ID %s", source))
}

func DeleteCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule,
) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	source := req.GetSourceBuildId()
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == source {
			r.DeleteTimestamp = timestamp
			return data, nil // no need to check cycle because removing a node cannot create a cycle
		}
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("cannot delete: no redirect rule found with source ID %s", source))
}

// CleanupRuleTombstones clears all deleted rules from versioning data if the rule was deleted more than
// retentionTime ago. Clones data to avoid mutating in place.
func CleanupRuleTombstones(versioningData *persistencespb.VersioningData,
	retentionTime time.Duration,
) *persistencespb.VersioningData {
	modifiedData := shallowCloneVersioningData(versioningData)
	modifiedData.AssignmentRules = util.FilterSlice(modifiedData.GetAssignmentRules(), func(ar *persistencespb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil || (ar.DeleteTimestamp != nil && hlc.Since(ar.DeleteTimestamp) < retentionTime)
	})
	modifiedData.RedirectRules = util.FilterSlice(modifiedData.GetRedirectRules(), func(rr *persistencespb.RedirectRule) bool {
		return rr.DeleteTimestamp == nil || (rr.DeleteTimestamp != nil && hlc.Since(rr.DeleteTimestamp) < retentionTime)
	})
	return modifiedData
}

// CommitBuildID makes the following changes. If no worker that can accept tasks for the
// target build id has been seen recently, the operation will fail.
// To override this check, set the force flag:
//  1. Adds an unconditional assignment rule for the target Build ID at the
//     end of the list. An unconditional assignment rule:
//     - Has no hint filter
//     - Has no ramp
//  2. Removes all previously added assignment rules to the given target
//     Build ID (if any).
//  3. Removes any *unconditional* assignment rule for other Build IDs.
func CommitBuildID(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId,
	hasRecentPoller bool,
	maxAssignmentRules int) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	target := req.GetTargetBuildId()
	if !hasRecentPoller && !req.GetForce() {
		return nil, serviceerror.NewFailedPrecondition(
			fmt.Sprintf("no versioned poller with build ID '%s' seen within the last %s, use force=true to commit anyways",
				target, versioningPollerSeenWindow.String()))
	}
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, serviceerror.NewFailedPrecondition(
			fmt.Sprintf("update breaks requirement, build id %s is already a member of version set", target))
	}

	for _, ar := range getActiveAssignmentRules(data.GetAssignmentRules()) {
		if ar.GetRule().GetTargetBuildId() == target {
			ar.DeleteTimestamp = timestamp
		}
		if isUnconditional(ar.GetRule()) {
			ar.DeleteTimestamp = timestamp
		}
	}

	data.AssignmentRules = append(data.GetAssignmentRules(), &persistencespb.AssignmentRule{
		Rule:            &taskqueue.BuildIdAssignmentRule{TargetBuildId: target},
		CreateTimestamp: timestamp,
	})
	if err := checkAssignmentConditions(data, maxAssignmentRules, false); err != nil {
		return nil, err
	}
	return data, nil
}

func GetWorkerVersioningRules(
	versioningData *persistencespb.VersioningData,
	clk *hlc.Clock,
) (*matchingservice.GetWorkerVersioningRulesResponse, error) {
	var cT []byte
	var err error
	if cT, err = clk.Marshal(); err != nil {
		return nil, serviceerror.NewInternal("error generating conflict token")
	}
	activeAssignmentRules := make([]*taskqueue.TimestampedBuildIdAssignmentRule, 0)
	for _, ar := range versioningData.GetAssignmentRules() {
		if ar.GetDeleteTimestamp() == nil {
			activeAssignmentRules = append(activeAssignmentRules, &taskqueue.TimestampedBuildIdAssignmentRule{
				Rule:       ar.GetRule(),
				CreateTime: hlc.ProtoTimestamp(ar.GetCreateTimestamp()),
			})
		}
	}
	activeRedirectRules := make([]*taskqueue.TimestampedCompatibleBuildIdRedirectRule, 0)
	for _, rr := range versioningData.GetRedirectRules() {
		if rr.GetDeleteTimestamp() == nil {
			activeRedirectRules = append(activeRedirectRules, &taskqueue.TimestampedCompatibleBuildIdRedirectRule{
				Rule:       rr.GetRule(),
				CreateTime: hlc.ProtoTimestamp(rr.GetCreateTimestamp()),
			})
		}
	}
	return &matchingservice.GetWorkerVersioningRulesResponse{
		Response: &workflowservice.GetWorkerVersioningRulesResponse{
			AssignmentRules:         activeAssignmentRules,
			CompatibleRedirectRules: activeRedirectRules,
			ConflictToken:           cT,
		},
	}, nil
}

// checkAssignmentConditions checks for validity conditions that must be assessed by looking at the entire set of rules.
// It returns an error if the new set of assignment rules don't meet the following requirements:
// - No more rules than dynamicconfig.VersionAssignmentRuleLimitPerQueue
// - If `requireUnconditional`, ensure at least one unconditional rule still exists
func checkAssignmentConditions(g *persistencespb.VersioningData, maxARs int, requireUnconditional bool) error {
	activeRules := getActiveAssignmentRules(g.GetAssignmentRules())
	if cnt := len(activeRules); maxARs > 0 && cnt > maxARs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of assignment rules permitted in namespace (%v/%v)", cnt, maxARs))
	}
	if requireUnconditional && !containsUnconditional(activeRules) {
		return serviceerror.NewFailedPrecondition("there must exist at least one fully-ramped 'unconditional' assignment rule, use force=true to bypass this requirement")
	}
	return nil
}

// checkRedirectConditions checks for validity conditions that must be assessed by looking at the entire set of rules.
// It returns an error if the new set of redirect rules don't meet the following requirements:
// - No more rules than dynamicconfig.VersionRedirectRuleLimitPerQueue
// - The DAG of redirect rules must not contain a cycle
func checkRedirectConditions(g *persistencespb.VersioningData, maxRRs int) error {
	activeRules := getActiveRedirectRules(g.GetRedirectRules())
	if maxRRs > 0 && len(activeRules) > maxRRs {
		return serviceerror.NewFailedPrecondition(
			fmt.Sprintf("update exceeds number of redirect rules permitted in namespace (%v/%v)", len(activeRules), maxRRs))
	}
	if isCyclic(activeRules) {
		return serviceerror.NewFailedPrecondition("update would break acyclic requirement")
	}
	return nil
}

func getActiveAssignmentRules(rules []*persistencespb.AssignmentRule) []*persistencespb.AssignmentRule {
	return util.FilterSlice(slices.Clone(rules), func(ar *persistencespb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil
	})
}

func getActiveRedirectRules(rules []*persistencespb.RedirectRule) []*persistencespb.RedirectRule {
	return util.FilterSlice(slices.Clone(rules), func(rr *persistencespb.RedirectRule) bool {
		return rr.DeleteTimestamp == nil
	})
}

func isActiveRedirectRuleSource(buildID string, redirectRules []*persistencespb.RedirectRule) bool {
	for _, r := range getActiveRedirectRules(redirectRules) {
		if buildID == r.GetRule().GetSourceBuildId() {
			return true
		}
	}
	return false
}

// isConditionalAssignmentRuleTarget checks whether the given buildID is the target of a conditional assignment rule
// (one with a ramp). We check this for any buildID that is the source of a proposed redirect rule, because having a
// ramped assignment rule target as the source for a redirect rule would lead to an unpredictable amount of traffic
// being redirected vs being passed through to the next assignment rule in the chain. This would not be a sensible use
// of redirect rules or assignment rule ramps, so it is prohibited.
//
// e.g. Scenario in which a conditional assignment rule target is the source for a redirect rule.
//
//	Assignment rules: [{target: 1, ramp: 50%}, {target: 2, ramp: nil}, {target: 3, ramp: nil}]
//	  Redirect rules: [{1->4}]
//	50% of tasks that start with buildID 1 would be sent on to buildID 2 per assignment rules, and the
//	remaining 50% that "stay" on buildID 1 would be redirected to buildID 4 per the redirect rules.
//	This doesn't make sense, so we prohibit it.
func isConditionalAssignmentRuleTarget(buildID string, assignmentRules []*persistencespb.AssignmentRule) bool {
	for _, r := range getActiveAssignmentRules(assignmentRules) {
		if !isUnconditional(r.GetRule()) && buildID == r.GetRule().GetTargetBuildId() {
			return true
		}
	}
	return false
}

func isUnconditional(ar *taskqueue.BuildIdAssignmentRule) bool {
	return ar.GetPercentageRamp() == nil
}

// containsUnconditional returns true if there exists an assignment rule with a nil ramp percentage
func containsUnconditional(rules []*persistencespb.AssignmentRule) bool {
	found := false
	for _, rule := range rules {
		ar := rule.GetRule()
		if isUnconditional(ar) {
			found = true
		}
	}
	return found
}

// isInVersionSets returns true if the given build id is in any of the listed version sets
func isInVersionSets(id string, sets []*persistencespb.CompatibleVersionSet) bool {
	for _, set := range sets {
		for _, bid := range set.BuildIds {
			if bid.GetId() == id {
				return true
			}
		}
	}
	return false
}

// given2ActualIdx takes in the user-given index, which only counts active assignment rules, and converts it to the
// actual index of that rule in the assignment rule list, which includes deleted rules.
// A negative return value means index out of bounds.
func given2ActualIdx(idx int32, rules []*persistencespb.AssignmentRule) int {
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

// validRamp returns true if the percentage ramp is within [0, 100), or if the ramp is nil
func validRamp(ramp *taskqueue.RampByPercentage) bool {
	if ramp == nil {
		return true
	}
	return ramp.RampPercentage >= 0 && ramp.RampPercentage < 100
}

// isCyclic returns true if there is a cycle in the DAG of redirect rules.
func isCyclic(rules []*persistencespb.RedirectRule) bool {
	makeEdgeMap := func(rules []*persistencespb.RedirectRule) map[string][]string {
		ret := make(map[string][]string)
		for _, rule := range rules {
			src := rule.GetRule().GetSourceBuildId()
			dst := rule.GetRule().GetTargetBuildId()
			list, ok := ret[src]
			if !ok {
				list = make([]string, 0)
			}
			list = append(list, dst)
			ret[src] = list
		}
		return ret
	}

	dag := makeEdgeMap(rules)
	visited := make(map[string]bool)
	for node := range dag {
		inStack := make(map[string]bool)
		if dfs(node, visited, inStack, dag) {
			return true
		}
	}
	return false
}

func dfs(curr string, visited, inStack map[string]bool, nodes map[string][]string) bool {
	if inStack[curr] {
		return true
	}
	if visited[curr] {
		return false
	}
	visited[curr] = true
	inStack[curr] = true
	for _, dst := range nodes[curr] {
		if dfs(dst, visited, inStack, nodes) {
			return true
		}
	}
	inStack[curr] = false
	return false
}

func FindAssignmentBuildId(rules []*persistencespb.AssignmentRule, runId string) string {
	rampThreshold := -1.
	for _, r := range rules {
		if r.GetDeleteTimestamp() != nil {
			continue
		}
		if ramp := r.GetRule().GetPercentageRamp(); ramp != nil {
			if rampThreshold == -1. {
				rampThreshold = calcRampThreshold(runId)
			}
			if float64(ramp.GetRampPercentage()) <= rampThreshold {
				continue
			}
		}
		return r.GetRule().GetTargetBuildId()
	}
	return ""
}

// calcRampThreshold returns a number in [0, 100) that is deterministically calculated based on the passed id
func calcRampThreshold(id string) float64 {
	h := farm.Fingerprint32([]byte(id))
	return 100 * (float64(h) / (float64(math.MaxUint32) + 1))
}
