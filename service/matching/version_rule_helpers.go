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
// - If there existed an "unfiltered" assigment rule (which can accept any task), at least one must still exist
// - To override the unfiltered assignment rule requirement, the user can specify force = true
func checkAssignmentConditions(g *persistencepb.VersioningData, maxARs int, force, hadUnfiltered bool) error {
	activeRules := getActiveAssignmentRules(slices.Clone(g.GetAssignmentRules()))
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

func getActiveAssignmentRules(rules []*persistencepb.AssignmentRule) []*persistencepb.AssignmentRule {
	return util.FilterSlice(rules, func(ar *persistencepb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil
	})
}

func isUnfiltered(ar *taskqueue.BuildIdAssignmentRule) bool {
	percentageRamp := ar.GetPercentageRamp()
	return ar.GetFilterExpression() == "" &&
		ar.GetWorkerRatioRamp() == nil &&
		(percentageRamp == nil || (percentageRamp != nil && percentageRamp.RampPercentage == 100))
}

// containsUnfiltered returns true if there exists an assignment rule with no filter expression,
// no worker ratio ramp, and no ramp percentage, or a ramp percentage of 100
func containsUnfiltered(rules []*persistencepb.AssignmentRule) bool {
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

// validRamp returns true if the percentage ramp is within [0, 100), or if the ramp is nil
func validRamp(ramp *taskqueue.RampByPercentage) bool {
	if ramp == nil {
		return true
	}
	return ramp.RampPercentage >= 0 && ramp.RampPercentage < 100
}

func InsertAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_InsertBuildIdAssignmentRule,
	maxARs int,
	hadUnfiltered bool) (*persistencepb.VersioningData, error) {
	if req.GetRuleIndex() < 0 {
		return nil, serviceerror.NewInvalidArgument("rule index cannot be negative")
	}
	rule := req.GetRule()
	if ramp := rule.GetPercentageRamp(); !validRamp(ramp) {
		return nil, serviceerror.NewInvalidArgument("ramp percentage must be in range [0, 100)")
	}
	if data == nil {
		data = &persistencepb.VersioningData{AssignmentRules: make([]*persistencepb.AssignmentRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	}
	rules := data.GetAssignmentRules()

	if actualIdx := given2ActualIdx(req.GetRuleIndex(), rules); actualIdx < 0 {
		// given index was too large, insert at end
		data.AssignmentRules = append(rules, &persistenceAR)
	} else {
		data.AssignmentRules = slices.Insert(rules, actualIdx, &persistenceAR)
	}
	return data, checkAssignmentConditions(data, maxARs, false, hadUnfiltered)
}

func ReplaceAssignmentRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceBuildIdAssignmentRule,
	hadUnfiltered bool) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	rule := req.GetRule()
	if ramp := rule.GetPercentageRamp(); !validRamp(ramp) {
		return nil, serviceerror.NewInvalidArgument("ramp percentage must be in range [0, 100)")
	}
	rules := data.GetAssignmentRules()
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveAssignmentRules(rules))))
	}
	persistenceAR := persistencepb.AssignmentRule{
		Rule:            rule,
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
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf(
			"rule index %d is out of bounds for assignment rule list of length %d", idx, len(getActiveAssignmentRules(rules))))
	}
	rule := rules[actualIdx]
	rule.DeleteTimestamp = timestamp
	return data, checkAssignmentConditions(data, 0, req.GetForce(), hadUnfiltered)
}

// checkRedirectConditions returns an error if the new set of redirect rules don't meet the following requirements:
// - No more rules than dynamicconfig.VersionRedirectRuleLimitPerQueue
// - The DAG of redirect rules must not contain a cycle
func checkRedirectConditions(g *persistencepb.VersioningData, maxRRs int) error {
	rules := g.GetRedirectRules()
	if maxRRs > 0 && countActiveRR(rules) > maxRRs {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update would exceed number of redirect rules permitted in namespace dynamic config (%v/%v)", len(rules), maxRRs))
	}
	if isCyclic(rules) {
		return serviceerror.NewFailedPrecondition("update would break acyclic requirement")
	}
	return nil
}

func countActiveRR(rules []*persistencepb.RedirectRule) int {
	cnt := 0
	for _, rule := range rules {
		if rule.DeleteTimestamp == nil {
			cnt++
		}
	}
	return cnt
}

// isCyclic returns true if there is a cycle in the DAG of redirect rules.
func isCyclic(rules []*persistencepb.RedirectRule) bool {
	dag := makeEdgeMap(rules)
	for node := range dag {
		visited := make(map[string]bool)
		inStack := make(map[string]bool)
		if dfs(node, visited, inStack, dag) {
			return true
		}
	}
	return false
}

func makeEdgeMap(rules []*persistencepb.RedirectRule) map[string][]string {
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

func InsertCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule,
	maxRRs int) (*persistencepb.VersioningData, error) {
	if data == nil {
		data = &persistencepb.VersioningData{RedirectRules: make([]*persistencepb.RedirectRule, 0)}
	} else {
		data = common.CloneProto(data)
	}
	rule := req.GetRule()
	src := rule.GetSourceBuildId()
	rules := data.GetRedirectRules()
	for _, r := range rules {
		if r.GetDeleteTimestamp() != nil && r.GetRule().GetSourceBuildId() == src {
			return nil, serviceerror.NewAlreadyExist(fmt.Sprintf(
				"cannot insert: source %s already redirects to target %s",
				src, r.GetRule().GetTargetBuildId(),
			))
		}
	}
	data.RedirectRules = slices.Insert(rules, 0, &persistencepb.RedirectRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	})
	return data, checkRedirectConditions(data, maxRRs)
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	rule := req.GetRule()
	src := rule.GetSourceBuildId()
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() != nil && r.GetRule().GetSourceBuildId() == src {
			r.Rule = rule
			r.CreateTimestamp = timestamp
			return data, checkRedirectConditions(data, 0)
		}
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("cannot replace: no redirect rule found with source ID %s", src))
}

func DeleteCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleBuildIdRedirectRule) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	src := req.GetSourceBuildId()
	for _, r := range data.GetRedirectRules() {
		if r.GetDeleteTimestamp() != nil && r.GetRule().GetSourceBuildId() == src {
			r.DeleteTimestamp = timestamp
			return data, checkRedirectConditions(data, 0)
		}
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("cannot delete: no redirect rule found with source ID %s", src))
}

// CleanupRuleTombstones clears all deleted rules from versioning data if the rule was deleted more than
// retentionTime ago. Clones data to avoid mutating in place.
func CleanupRuleTombstones(versioningData *persistencepb.VersioningData, retentionTime time.Duration) *persistencepb.VersioningData {
	modifiedData := shallowCloneVersioningData(versioningData)
	modifiedData.AssignmentRules = util.FilterSlice(modifiedData.GetAssignmentRules(), func(ar *persistencepb.AssignmentRule) bool {
		return ar.DeleteTimestamp == nil || (ar.DeleteTimestamp != nil && hlc.Since(ar.DeleteTimestamp) < retentionTime)
	})
	modifiedData.RedirectRules = util.FilterSlice(modifiedData.GetRedirectRules(), func(rr *persistencepb.RedirectRule) bool {
		return rr.DeleteTimestamp == nil || (rr.DeleteTimestamp != nil && hlc.Since(rr.DeleteTimestamp) < retentionTime)
	})
	return modifiedData
}

// CommitBuildID makes the following changes:
//  1. Adds an unconditional assignment rule for the target Build ID at the
//     end of the list. An unconditional assignment rule:
//     - Has no hint filter
//     - Has no ramp
//  2. Removes all previously added assignment rules to the given target
//     Build ID (if any).
//  3. Removes any *unconditional* assignment rule for other Build IDs.
func CommitBuildID(timestamp *hlc.Clock,
	data *persistencepb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId) (*persistencepb.VersioningData, error) {
	data = common.CloneProto(data)
	target := req.GetTargetBuildId()
	assignmentRules := data.GetAssignmentRules()
	for _, ar := range getActiveAssignmentRules(assignmentRules) {
		if ar.GetRule().GetTargetBuildId() == target {
			ar.DeleteTimestamp = timestamp
		}
		if isUnfiltered(ar.GetRule()) {
			ar.DeleteTimestamp = timestamp
		}
	}
	data.AssignmentRules = append(assignmentRules, &persistencepb.AssignmentRule{
		Rule:            &taskqueue.BuildIdAssignmentRule{TargetBuildId: target},
		CreateTimestamp: timestamp,
	})
	if err := checkAssignmentConditions(data, 0, false, true); err != nil {
		return nil, err
	}
	return data, nil
}
