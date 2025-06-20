package searchattribute

type (
	SystemProvider struct{}
)

func NewSystemProvider() *SystemProvider {
	return &SystemProvider{}
}

func (s *SystemProvider) GetSearchAttributes(_ string, _ bool) (NameTypeMap, error) {
	return NameTypeMap{}, nil
}
