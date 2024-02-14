package testvars

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
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

func (a Any) EventID() int64 {
	// This produces EventID in XX00YY format, where XX is unique for every test and YY is a random number.
	return int64(randInt(a.testHash, 2, 1, 2))
}
