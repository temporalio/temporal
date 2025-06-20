package payloads

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Int    int
	String string
	Bytes  []byte
}

func TestToString(t *testing.T) {
	assert := assert.New(t)
	var result string

	p := EncodeString("str")
	result = ToString(p)
	assert.Equal(`["str"]`, result)

	p, err := Encode(10, "str")
	assert.NoError(err)
	result = ToString(p)
	assert.Equal(`[10, "str"]`, result)

	p, err = Encode([]byte{41, 42, 43}, 10, "str")
	assert.NoError(err)
	result = ToString(p)
	assert.Equal(`[KSor, 10, "str"]`, result)

	p, err = Encode(&testStruct{
		Int:    10,
		String: "str",
		Bytes:  []byte{51, 52, 53},
	}, 10, "str")
	assert.NoError(err)
	result = ToString(p)
	assert.Equal(`[{"Int":10,"String":"str","Bytes":"MzQ1"}, 10, "str"]`, result)

	result = ToString(nil)
	assert.Equal("[]", result)
}
