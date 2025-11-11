package query

// SearchAttributeInterceptor is used in the query converter to intercept the search attributes
// used in the filters of the query.
// Eg: if the query is `ExecutionStatus = 'Running' AND CustomKeywordField = 'foo'`, the interceptor
// will be called for the `ExecutionStatus` and `CustomKeywordField` search attributes.
// The interceptor can be useful if you need to capture the search attributes used in the query, or
// if you need to modify the field name that is used to query the data store.
type SearchAttributeInterceptor interface {
	Intercept(col *SAColumn) error
}

// NopSearchAttributeInterceptor is a dummy interceptor and the default interceptor used in the
// query converter.
type NopSearchAttributeInterceptor struct{}

var nopSearchAttributeInterceptor SearchAttributeInterceptor = &NopSearchAttributeInterceptor{}

func (i *NopSearchAttributeInterceptor) Intercept(col *SAColumn) error {
	return nil
}
