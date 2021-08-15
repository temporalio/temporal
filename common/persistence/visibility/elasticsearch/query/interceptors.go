package query

type (
	FieldNameInterceptor interface {
		Name(name string) (string, error)
	}
	FieldValuesInterceptor interface {
		Values(name string, values []interface{}) ([]interface{}, error)
	}

	nopFieldNameInterceptor struct{}

	nopFieldValuesInterceptor struct{}
)

func (n *nopFieldNameInterceptor) Name(name string) (string, error) {
	return name, nil
}

func (n *nopFieldValuesInterceptor) Values(_ string, values []interface{}) ([]interface{}, error) {
	return values, nil
}
