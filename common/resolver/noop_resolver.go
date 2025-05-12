package resolver

type (
	NoopResolver struct {
	}
)

func NewNoopResolver() *NoopResolver {
	return &NoopResolver{}
}

func (c *NoopResolver) Resolve(service string) []string {
	return []string{service}
}
