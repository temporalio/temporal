package query

type SearchAttributeInterceptor interface {
	Intercept(col *SAColName) error
}

type NopSearchAttributeInterceptor struct{}

var nopSearchAttributeInterceptor SearchAttributeInterceptor = &NopSearchAttributeInterceptor{}

func (i *NopSearchAttributeInterceptor) Intercept(col *SAColName) error {
	return nil
}
