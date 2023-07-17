package quotas

type (
	// InstanceCounter returns the total number of instances there are for a given service.
	InstanceCounter interface {
		MemberCount() int
	}
	// Limits contains the per instance and per cluster limits. It exists to make it harder to mix up the two limits
	// when calling CalculateEffectiveResourceLimit.
	Limits struct {
		InstanceLimit int
		ClusterLimit  int
	}
)

// CalculateEffectiveResourceLimit returns the effective resource limit for a host given the per instance and per
// cluster limits. If the per cluster limit is not set, the per instance limit is returned. If the per cluster limit
// is set, the per instance limit is ignored. The effective limit is calculated by dividing the per cluster limit by
// the number of hosts in the cluster. If the number of hosts is not available, it is assumed to be 1. The "resource"
// here could be requests per second, total number of active requests, etc.
func CalculateEffectiveResourceLimit(instanceCounter InstanceCounter, limits Limits) float64 {
	// TODO: Determine if we can remove the nil check here.
	if clusterLimit := limits.ClusterLimit; clusterLimit > 0 && instanceCounter != nil {
		if clusterSize := instanceCounter.MemberCount(); clusterSize >= 1 {
			return float64(clusterLimit) / float64(clusterSize)
		}
	}

	return float64(limits.InstanceLimit)
}
