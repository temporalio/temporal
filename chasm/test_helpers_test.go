package chasm

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"testing"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common/definition"
)

// testHelper provides test value generation similar to testvars but without persistence dependency.
type testHelper struct {
	testName string
	testHash uint32
}

// newTestHelper creates a new test helper from a test name.
func newTestHelperFromName(testName string) *testHelper {
	return &testHelper{
		testName: testName,
		testHash: hashTestName(testName),
	}
}

// NewTestHelper creates a test helper from a testing.T.
func NewTestHelper(t testing.TB) *testHelper {
	return newTestHelperFromName(t.Name())
}

// hashTestName generates a hash from a test name using FNV-1a (same as testvars).
func hashTestName(name string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return h.Sum32()
}

// randInt generates a deterministic random integer based on test hash.
func (th *testHelper) randInt(hashLen, padLen, randomLen int) int {
	testID := int(th.testHash) % int(math.Pow10(hashLen))
	pad := int(math.Pow10(padLen + randomLen))
	random := rand.Int() % int(math.Pow10(randomLen))
	return testID*pad + random
}

// randString generates a random string of length n.
func (th *testHelper) randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// uniqueString returns a unique string based on test name and key.
func (th *testHelper) uniqueString(key string) string {
	return fmt.Sprintf("%s_%s", th.testName, key)
}

// uuidString returns a new UUID string.
func (th *testHelper) uuidString() string {
	return uuid.New()
}

// Any provides methods for generating arbitrary test values.
type testHelperAny struct {
	testName string
	testHash uint32
}

// Any returns an Any helper for generating arbitrary values.
func (th *testHelper) Any() *testHelperAny {
	return &testHelperAny{
		testName: th.testName,
		testHash: th.testHash,
	}
}

// String returns a random string unique to the test.
func (a *testHelperAny) String() string {
	return a.testName + "_any_random_string_" + randString(5)
}

// Int returns a deterministic integer unique to the test.
func (a *testHelperAny) Int() int {
	// This produces number in XXX000YYY format, where XXX is unique for every test and YYY is a random number.
	return randInt(a.testHash, 3, 3, 3)
}

// Int64 returns a deterministic int64 unique to the test.
func (a *testHelperAny) Int64() int64 {
	return int64(randInt(a.testHash, 3, 3, 3))
}

// RunID returns a new UUID string.
func (a *testHelperAny) RunID() string {
	return uuid.New()
}

// WorkflowKey returns a function that returns a WorkflowKey with unique values.
func (a *testHelperAny) WorkflowKey() func() definition.WorkflowKey {
	// Capture the values at call time to ensure uniqueness
	nsID := a.String()
	wfID := a.String()
	runID := a.RunID()
	return func() definition.WorkflowKey {
		return definition.NewWorkflowKey(nsID, wfID, runID)
	}
}

// NamespaceID returns a unique namespace ID string.
func (th *testHelper) NamespaceID() string {
	return th.uuidString()
}

// WorkflowID returns a unique workflow ID string.
func (th *testHelper) WorkflowID() string {
	return th.uniqueString("workflow_id")
}

// WorkflowIDWithNumber returns a workflow ID with a number suffix.
func (th *testHelper) WorkflowIDWithNumber(n int) string {
	return fmt.Sprintf("%s_%d", th.uniqueString("workflow_id"), n)
}

// RunID returns a unique run ID string (UUID).
func (th *testHelper) RunID() string {
	return th.uuidString()
}

// FailoverVersion returns a deterministic failover version.
func (th *testHelper) FailoverVersion() int64 {
	return int64(th.randInt(2, 1, 2))
}

// Helper functions for standalone use (not requiring testHelper instance)

func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func randInt(testHash uint32, hashLen, padLen, randomLen int) int {
	testID := int(testHash) % int(math.Pow10(hashLen))
	pad := int(math.Pow10(padLen + randomLen))
	random := rand.Int() % int(math.Pow10(randomLen))
	return testID*pad + random
}
