package calculator

type (
	Calculator interface {
		GetQuota() float64
	}

	NamespaceCalculator interface {
		GetQuota(namespace string) float64
	}
)
