package dynamicconfig

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasSharedStructure(t *testing.T) {
	var v any = 3
	assert.Empty(t, hasSharedStructure(reflect.ValueOf(v), "root"))

	v = struct {
		i int
		s string
		c struct{ p *string }
	}{i: 5, s: "hi"}
	assert.Empty(t, hasSharedStructure(reflect.ValueOf(v), "root"))

	v = [5]*float64{nil, nil, nil, nil, nil}
	assert.Empty(t, hasSharedStructure(reflect.ValueOf(v), "root"))

	f := float64(35.124)
	v = [5]*float64{nil, nil, &f, nil, nil}
	assert.Equal(t, "root.[2]", hasSharedStructure(reflect.ValueOf(v), "root"))

	v = []byte(nil)
	assert.Empty(t, hasSharedStructure(reflect.ValueOf(v), "root"))

	v = []byte{}
	assert.Equal(t, "root", hasSharedStructure(reflect.ValueOf(v), "root"))

	str := "test"
	v = struct {
		i int
		s string
		c struct{ p *string }
	}{c: struct{ p *string }{p: &str}}
	assert.Equal(t, "root.c.p", hasSharedStructure(reflect.ValueOf(v), "root"))
}
