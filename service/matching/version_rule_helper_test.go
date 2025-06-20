package matching

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func TestFindAssignmentBuildId_NoRules(t *testing.T) {
	b := FindAssignmentBuildId(nil, "")
	assert.Equal(t, "", b)
}

func TestFindAssignmentBuildId_OneFullRule(t *testing.T) {
	buildId := "bld"
	b := FindAssignmentBuildId([]*persistencespb.AssignmentRule{createAssignmentRuleWithoutRamp(buildId)}, "")
	assert.Equal(t, buildId, b)
}

func TestFindAssignmentBuildId_TwoFullRules(t *testing.T) {
	buildId := "bld"
	buildId2 := "bld2"
	b := FindAssignmentBuildId(
		[]*persistencespb.AssignmentRule{
			createAssignmentRuleWithoutRamp(buildId),
			createAssignmentRuleWithoutRamp(buildId2),
		},
		"",
	)
	assert.Equal(t, buildId, b)
}

func TestFindAssignmentBuildId_WithRamp(t *testing.T) {
	buildId1 := "bld1"
	buildId2 := "bld2"
	buildId3 := "bld3"
	buildId4 := "bld4"
	buildId5 := "bld5"

	rules := []*persistencespb.AssignmentRule{
		createAssignmentRuleWithRamp(buildId1, 0),
		createAssignmentRuleWithRamp(buildId2, 20),
		createAssignmentRuleWithRamp(buildId3, 70),
		createAssignmentRuleWithoutRamp(buildId4),
		createAssignmentRuleWithRamp(buildId5, 90),
	}

	histogram := make(map[string]int)
	runs := 1000000
	for i := 0; i < runs; i++ {
		b := FindAssignmentBuildId(rules, "run-"+strconv.Itoa(i))
		histogram[b]++
	}

	assert.Equal(t, 0, histogram[buildId1])
	assert.InEpsilon(t, .2*float64(runs), histogram[buildId2], .02)
	// 20% has gone to build 2, so 70%-20%=50% should go to build 3
	assert.InEpsilon(t, .5*float64(runs), histogram[buildId3], .02)
	assert.InEpsilon(t, .3*float64(runs), histogram[buildId4], .02)
	assert.Equal(t, 0, histogram[buildId5])
}

func TestCalcRampThresholdUniform(t *testing.T) {
	buildPref := "bldXYZ-"
	histogram := [100]int{}
	for i := 0; i < 1000000; i++ {
		v := calcRampThreshold(buildPref + strconv.Itoa(i))
		histogram[int32(v)]++
	}

	for i := 0; i < 100; i++ {
		assert.InEpsilon(t, 10000, histogram[i], 0.1)
	}
}

func TestCalcRampThresholdDeterministic(t *testing.T) {
	// same input should always may to the same output
	assert.Equal(t, calcRampThreshold("A"), calcRampThreshold("A"))

	// unless it's an empty string which maps randomly each time
	assert.NotEqual(t, calcRampThreshold(""), calcRampThreshold(""))
}

func createAssignmentRuleWithoutRamp(buildId string) *persistencespb.AssignmentRule {
	return &persistencespb.AssignmentRule{Rule: &taskqueuepb.BuildIdAssignmentRule{TargetBuildId: buildId}}
}

func createAssignmentRuleWithRamp(buildId string, ramp float32) *persistencespb.AssignmentRule {
	return &persistencespb.AssignmentRule{Rule: &taskqueuepb.BuildIdAssignmentRule{
		TargetBuildId: buildId,
		Ramp:          mkNewAssignmentPercentageRamp(ramp),
	}}
}
