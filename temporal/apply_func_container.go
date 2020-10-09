package temporal

type (
	applyFuncContainer struct {
		applyInternal func(*serverOptions)
	}
)

func (fso *applyFuncContainer) apply(s *serverOptions) {
	fso.applyInternal(s)
}

func newApplyFuncContainer(apply func(option *serverOptions)) *applyFuncContainer {
	return &applyFuncContainer{
		applyInternal: apply,
	}
}
