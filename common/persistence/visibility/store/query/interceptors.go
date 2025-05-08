package query

type (
	FieldNameInterceptor interface {
		Name(name string, usage FieldNameUsage) (string, error)
	}
	FieldValuesInterceptor interface {
		Values(name string, fieldName string, values ...interface{}) ([]interface{}, error)
	}

	NopFieldNameInterceptor struct{}

	NopFieldValuesInterceptor struct{}

	FieldNameUsage int
)

const (
	FieldNameFilter FieldNameUsage = iota
	FieldNameSorter
	FieldNameGroupBy
)

func (n *NopFieldNameInterceptor) Name(name string, _ FieldNameUsage) (string, error) {
	return name, nil
}

func (n *NopFieldValuesInterceptor) Values(_ string, _ string, values ...interface{}) ([]interface{}, error) {
	return values, nil
}
