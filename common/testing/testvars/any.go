package testvars

import (
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Any struct {
	testName string
	testHash uint32
}

func newAny(testName string, testHash uint32) Any {
	return Any{
		testName: testName,
		testHash: testHash,
	}
}

func (a Any) String() string {
	return a.testName + "_any_random_string_" + randString(5)
}

func (a Any) Payload() *commonpb.Payload {
	return payload.EncodeString(a.String())
}

func (a Any) Payloads() *commonpb.Payloads {
	return payloads.EncodeString(a.String())
}

func (a Any) Int() int {
	// This produces number in XXX000YYY format, where XXX is unique for every test and YYY is a random number.
	return randInt(a.testHash, 3, 3, 3)
}

func (a Any) Int32() int32 {
	return int32(a.Int())
}

func (a Any) UInt32() uint32 {
	return uint32(a.Int())
}

func (a Any) Int64() int64 {
	return int64(a.Int())
}

func (a Any) UInt64() uint64 {
	return uint64(a.Int())
}

func (a Any) EventID() int64 {
	// This produces EventID in XX0YY format, where XX is unique for every test and YY is a random number.
	return int64(randInt(a.testHash, 2, 1, 2))
}

func (a Any) ApplicationFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: a.String(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         a.String(),
			NonRetryable: false,
		}},
	}
}

func (a Any) InfiniteTimeout() *durationpb.Duration {
	return durationpb.New(10 * time.Hour)
}

func (a Any) RunID() string {
	return uuid.NewString()
}

func (a Any) WorkflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(a.String(), a.String(), a.RunID())
}
