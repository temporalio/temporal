package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/searchattribute"
)

type testSearchAttributeInterceptor struct {
	seenFields []string
}

var _ SearchAttributeInterceptor = (*testSearchAttributeInterceptor)(nil)

func (t *testSearchAttributeInterceptor) Intercept(col *SAColumn) error {
	if col.FieldName == "Keyword01" {
		return errors.New("interceptor error")
	}
	t.seenFields = append(t.seenFields, col.FieldName)
	return nil
}

func TestSearchAttributeInterceptor(t *testing.T) {
	interceptor := &testSearchAttributeInterceptor{}
	c := NewNilQueryConverter(
		"",
		"",
		searchattribute.TestNameTypeMap,
		&searchattribute.TestMapper{},
	).WithSearchAttributeInterceptor(interceptor)

	_, err := c.Convert("ExecutionStatus='Running' order by StartTime")
	assert.NoError(t, err)
	assert.Equal(t, []string{"ExecutionStatus", "StartTime"}, interceptor.seenFields)

	_, err = c.Convert("AliasForKeyword01='Running' order by StartTime")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "interceptor error")
}
