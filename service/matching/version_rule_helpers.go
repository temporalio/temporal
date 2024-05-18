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
	"math/rand"
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

const (
	unversionedBuildId = ""
)

var (
	errInvalidNegativeIndex                     = serviceerror.NewInvalidArgument("rule index cannot be negative")
	errInvalidRampPercentage                    = serviceerror.NewInvalidArgument("ramp percentage must be in range [0, 100)")
	errTargetIsVersionSetMember                 = serviceerror.NewFailedPrecondition("update breaks requirement, target build ID is already a member of a version set")
	errSourceIsVersionSetMember                 = serviceerror.NewFailedPrecondition("update breaks requirement, source build ID is already a member of a version set")
	errRampedAssignmentRuleIsRedirectRuleSource = serviceerror.NewFailedPrecondition("update breaks requirement, this target build ID cannot have a ramp because it is the source of a redirect rule")
	errAssignmentRuleIndexOutOfBounds           = func(idx, length int) error {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("rule index %d is out of bounds for assignment rule list of length %d", idx, length))
	}
	errSourceIsConditionalAssignmentRuleTarget = serviceerror.NewFailedPrecondition("redirect rule source build ID cannot be the target of any assignment rule with non-nil ramp")
	errSourceAlreadyExists                     = func(source, target string) error {
		return serviceerror.NewAlreadyExist(fmt.Sprintf("source %s already redirects to target %s", source, target))
	}
	errSourceNotFound = func(source string) error {
		return serviceerror.NewNotFound(fmt.Sprintf("no redirect rule found with source ID %s", source))
	}
	errNoRecentPollerOnCommitVersion = func(target string) error {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("no versioned poller with build ID '%s' seen within the last %s, use force=true to commit anyways", target, versioningPollerSeenWindow.String()))
	}
	errExceedsMaxAssignmentRules = func(cnt, max int) error {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of assignment rules permitted in namespace (%v/%v)", cnt, max))
	}
	errRequireUnconditionalAssignmentRule = serviceerror.NewFailedPrecondition("there must exist at least one fully-ramped 'unconditional' assignment rule, use force=true to bypass this requirement")
	errExceedsMaxRedirectRules            = func(cnt, max int) error {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of redirect rules permitted in namespace (%v/%v)", cnt, max))
	}
	errIsCyclic                   = serviceerror.NewFailedPrecondition("update would break acyclic requirement")
	errExceedsMaxUpstreamBuildIDs = func(cnt, max int) error {
		return serviceerror.NewFailedPrecondition(fmt.Sprintf("update exceeds number of upstream build ids permitted in namespace (%v/%v)", cnt, max))
	}
	errUnversionedRedirectRuleTarget = serviceerror.NewInvalidArgument("the unversioned build ID cannot be the target of a redirect rule")
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
		return nil, errInvalidNegativeIndex
	}
	rule := req.GetRule()
	if ramp := rule.GetPercentageRamp(); !validRamp(ramp) {
		return nil, errInvalidRampPercentage
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, errTargetIsVersionSetMember
	}
	if rule.GetRamp() != nil && isActiveRedirectRuleSource(target, data.GetRedirectRules()) {
		return nil, errRampedAssignmentRuleIsRedirectRuleSource
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
		return nil, errInvalidRampPercentage
	}
	target := rule.GetTargetBuildId()
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, errTargetIsVersionSetMember
	}
	if rule.GetRamp() != nil && isActiveRedirectRuleSource(target, data.GetRedirectRules()) {
		return nil, errRampedAssignmentRuleIsRedirectRuleSource
	}
	rules := data.GetAssignmentRules()
	hadUnconditional := containsUnconditional(rules)
	idx := req.GetRuleIndex()
	actualIdx := given2ActualIdx(idx, rules)
	if actualIdx < 0 {
		return nil, errAssignmentRuleIndexOutOfBounds(int(idx), len(getActiveAssignmentRules(rules)))
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
		return nil, errAssignmentRuleIndexOutOfBounds(int(idx), len(getActiveAssignmentRules(rules)))
	}
	rules[actualIdx].DeleteTimestamp = timestamp
	return data, checkAssignmentConditions(data, 0, hadUnconditional && !req.GetForce())
}

func AddCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleBuildIdRedirectRule,
	maxRedirectRules,
	maxUpstreamBuildIds int) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rule := req.GetRule()
	source := rule.GetSourceBuildId()
	target := rule.GetTargetBuildId()
	if target == unversionedBuildId {
		return nil, errUnversionedRedirectRuleTarget
	}
	if isInVersionSets(source, data.GetVersionSets()) {
		return nil, errSourceIsVersionSetMember
	}
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, errTargetIsVersionSetMember
	}
	if isConditionalAssignmentRuleTarget(source, data.GetAssignmentRules()) {
		return nil, errSourceIsConditionalAssignmentRuleTarget
	}
	rules := data.GetRedirectRules()
	for _, r := range rules {
		if r.GetDeleteTimestamp() == nil && r.GetRule().GetSourceBuildId() == source {
			return nil, errSourceAlreadyExists(source, r.GetRule().GetTargetBuildId())
		}
	}
	data.RedirectRules = slices.Insert(rules, 0, &persistencespb.RedirectRule{
		Rule:            rule,
		CreateTimestamp: timestamp,
		DeleteTimestamp: nil,
	})
	return data, checkRedirectConditions(data, maxRedirectRules, maxUpstreamBuildIds)
}

func ReplaceCompatibleRedirectRule(timestamp *hlc.Clock,
	data *persistencespb.VersioningData,
	req *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleBuildIdRedirectRule,
	maxUpstreamBuildIDs int,
) (*persistencespb.VersioningData, error) {
	data = cloneOrMkData(data)
	rule := req.GetRule()
	source := rule.GetSourceBuildId()
	target := rule.GetTargetBuildId()
	if target == unversionedBuildId {
		return nil, errUnversionedRedirectRuleTarget
	}
	if isInVersionSets(source, data.GetVersionSets()) {
		return nil, errSourceIsVersionSetMember
	}
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, errTargetIsVersionSetMember
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
			return data, checkRedirectConditions(data, 0, maxUpstreamBuildIDs)
		}
	}
	return nil, errSourceNotFound(source)
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
			return data, nil // no need to check cycle or chain because removing a node cannot create a cycle or create a link
		}
	}
	return nil, errSourceNotFound(source)
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
// target build ID has been seen recently, the operation will fail.
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
		return nil, errNoRecentPollerOnCommitVersion(target)
	}
	if isInVersionSets(target, data.GetVersionSets()) {
		return nil, errTargetIsVersionSetMember
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

func GetTimestampedWorkerVersioningRules(
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
		return errExceedsMaxAssignmentRules(cnt, maxARs)
	}
	if requireUnconditional && !containsUnconditional(activeRules) {
		return errRequireUnconditionalAssignmentRule
	}
	return nil
}

// checkRedirectConditions checks for validity conditions that must be assessed by looking at the entire set of rules.
// It returns an error if the new set of redirect rules don't meet the following requirements:
//   - No more rules than dynamicconfig.VersionRedirectRuleLimitPerQueue
//   - The DAG of redirect rules must not contain a cycle
//   - The DAG of redirect rules must not contain a chain of connected rules longer than dynamicconfig.VersionRedirectRuleMaxUpstreamBuildIDsPerQueue
//     (Here, a "chain" counts the # of vertices, not the # of edges. So 3 ---> 4 ---> 5 has a chain length of 3)
func checkRedirectConditions(g *persistencespb.VersioningData, maxRRs, maxUpstreamBuildIds int) error {
	activeRules := getActiveRedirectRules(g.GetRedirectRules())
	if maxRRs > 0 && len(activeRules) > maxRRs {
		return errExceedsMaxRedirectRules(len(activeRules), maxRRs)
	}
	if isCyclic(activeRules) {
		return errIsCyclic
	}
	for _, r := range activeRules {
		upstream := getUpstreamBuildIds(r.GetRule().GetTargetBuildId(), activeRules)
		if len(upstream) > maxUpstreamBuildIds {
			return errExceedsMaxUpstreamBuildIDs(len(upstream), maxUpstreamBuildIds)
		}
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

// findTerminalBuildId follows redirect rules from the given build ID and returns the target of the last redirect rule.
// Returns empty if a cycle is found.
func findTerminalBuildId(buildID string, activeRedirectRules []*persistencespb.RedirectRule) string {
outer:
	for i := 0; i <= len(activeRedirectRules); i++ { // limiting the cycles to protect against loops in the graph.
		for _, r := range activeRedirectRules {
			if r.GetRule().GetSourceBuildId() == buildID {
				buildID = r.GetRule().GetTargetBuildId()
				continue outer
			}
		}
		return buildID
	}
	return ""
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

// isInVersionSets returns true if the given build ID is in any of the listed version sets
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

// getUpstreamBuildIds returns a list of build ids that point to the given buildId in the graph of redirect rules.
// It considers all rules in the input list, so the caller should provide a filtered list as needed.
// The result will contain no duplicates.
func getUpstreamBuildIds(buildId string, redirectRules []*persistencespb.RedirectRule) []string {
	return getUpstreamHelper(buildId, redirectRules, nil)
}

func getUpstreamHelper(
	buildId string,
	redirectRules []*persistencespb.RedirectRule,
	visited map[string]bool,
) []string {
	var upstream []string
	if visited == nil {
		visited = make(map[string]bool)
	}
	visited[buildId] = true
	directSources := getSourcesForTarget(buildId, redirectRules)

	for _, src := range directSources {
		if !visited[src] {
			upstream = append(upstream, src)
			upstream = append(upstream, getUpstreamHelper(src, redirectRules, visited)...)
		}
	}

	// dedupe
	upstreamUnique := make(map[string]bool)
	for _, bid := range upstream {
		upstreamUnique[bid] = true
	}
	upstream = make([]string, len(upstreamUnique))
	i := 0
	for k := range upstreamUnique {
		upstream[i] = k
		i++
	}
	return upstream
}

// getSourcesForTarget gets the first-degree sources for any redirect rule targeting buildId
func getSourcesForTarget(buildId string, redirectRules []*persistencespb.RedirectRule) []string {
	var sources []string
	for _, rr := range redirectRules {
		if rr.GetRule().GetTargetBuildId() == buildId {
			sources = append(sources, rr.GetRule().GetSourceBuildId())
		}
	}
	return sources
}

// FindAssignmentBuildId finds a build ID for the given runId based on the given rules.
// Non-empty runId is deterministically mapped to a ramp threshold, while empty runId is mapped randomly each time.
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
	if id == "" {
		return rand.Float64()
	}
	h := farm.Fingerprint32([]byte(id))
	return 100 * (float64(h) / (float64(math.MaxUint32) + 1))
}

// FindRedirectBuildId follows chain of redirect rules starting from the given sourceBuildId and returns the final
// target build ID that should be used for redirect. Returns sourceBuildId if no applicable redirect rules exist.
func FindRedirectBuildId(sourceBuildId string, rules []*persistencespb.RedirectRule) string {
outer:
	for {
		for _, r := range rules {
			if r.GetDeleteTimestamp() != nil {
				continue
			}
			if r.GetRule().GetSourceBuildId() == sourceBuildId {
				sourceBuildId = r.GetRule().GetTargetBuildId()
				continue outer
			}
		}
		return sourceBuildId
	}
}
