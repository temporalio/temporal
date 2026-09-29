package health

type SignalReader interface {
	LatencyQuantile(quantile float64) (float64, bool)
	LatencyQuantileByGroup(groupName string, quantile float64) (float64, bool)
	ErrorRatio() (float64, bool)
	ErrorRatioByGroup(groupName string) (float64, bool)
}
