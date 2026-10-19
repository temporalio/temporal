package workers

import (
	"strings"

	otellog "go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/wideevents"
)

// WorkerMetricsConfig contains dynamic config flags for worker-related metrics.
type WorkerMetricsConfig struct {
	EnablePluginMetrics            dynamicconfig.BoolPropertyFn
	EnablePollerAutoscalingMetrics dynamicconfig.BoolPropertyFn
	BreakdownMetricsByTaskQueue    dynamicconfig.BoolPropertyFnWithTaskQueueFilter
	ExternalPayloadsEnabled        dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// workerMetricsEmitter encapsulates logic for emitting metrics derived from worker heartbeats.
type workerMetricsEmitter struct {
	handler                     metrics.Handler
	config                      WorkerMetricsConfig
	eventLogger                 otellog.Logger
	enableWorkerHeartbeatEvents dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

func (e *workerMetricsEmitter) emit(nsID namespace.ID, nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	// The SDK aggregates all workers on the same Client into one heartbeat RPC, so
	// len(heartbeats) approximates workers-per-process. It's per-Client-per-Namespace,
	// not strictly per-process, but multiple Clients per process is uncommon in practice.
	metrics.WorkerRegistryWorkersPerProcess.With(e.handler).Record(int64(len(heartbeats)))

	enablePluginMetrics := e.config.EnablePluginMetrics != nil && e.config.EnablePluginMetrics()
	enablePollerAutoscalingMetrics := e.config.EnablePollerAutoscalingMetrics != nil && e.config.EnablePollerAutoscalingMetrics()
	enableStorageDriverMetrics := e.config.ExternalPayloadsEnabled != nil && e.config.ExternalPayloadsEnabled(nsName.String())
	enableWorkerHeartbeatEvents := e.enableWorkerHeartbeatEvents != nil && e.enableWorkerHeartbeatEvents(nsName.String())

	recordedPlugins := make(map[string]bool)
	recordedDrivers := make(map[string]bool)

	for _, hb := range heartbeats {
		// Activity slots metric (always enabled)
		if hb.ActivityTaskSlotsInfo != nil {
			metrics.WorkerRegistryActivitySlotsUsed.With(e.handler).Record(int64(hb.ActivityTaskSlotsInfo.CurrentUsedSlots))
		}

		// Plugin metrics (if enabled)
		if enablePluginMetrics {
			for _, pluginInfo := range hb.Plugins {
				pluginName := pluginInfo.Name
				if !recordedPlugins[pluginName] {
					metrics.WorkerPluginNameMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerPluginNameTag(pluginName))
					recordedPlugins[pluginName] = true
				}
			}
		}

		// Poller autoscaling metrics (if enabled)
		if enablePollerAutoscalingMetrics {
			e.emitPollerAutoscaling(nsID, nsName, hb)
		}

		// Storage driver metrics (if external payloads enabled)
		if enableStorageDriverMetrics {
			for _, driver := range hb.GetDrivers() {
				driverType := driver.GetType()
				if !recordedDrivers[driverType] {
					metrics.WorkerStorageDriverTypeMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerStorageDriverTypeTag(driverType))
					recordedDrivers[driverType] = true
				}
			}
		}

		if enableWorkerHeartbeatEvents {
			e.emitWorkerMetricsEvent(nsName, hb)
		}
	}
}

func (e *workerMetricsEmitter) emitPollerAutoscaling(nsID namespace.ID, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	recordAutoscaling := func(taskType enumspb.TaskQueueType) {
		tq := family.TaskQueue(taskType)
		breakdownByTQ := e.config.BreakdownMetricsByTaskQueue != nil &&
			e.config.BreakdownMetricsByTaskQueue(nsName.String(), hb.GetTaskQueue(), taskType)
		handler := metrics.GetPerTaskQueueScope(e.handler, nsName.String(), tq, breakdownByTQ)
		metrics.PollerAutoscalingHeartbeatCount.With(handler).Record(1)
	}

	if hb.WorkflowPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
	if hb.ActivityPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	}
	if hb.NexusPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_NEXUS)
	}
}

func emitWorkerConfigEvent(logger otellog.Logger, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	var runtimes []wideevents.RuntimeInfo
	for _, rt := range hb.GetEnvironment().GetRuntimes() {
		runtimes = append(runtimes, wideevents.RuntimeInfo{
			Type:    runtimeTypeName(rt.GetType()),
			Version: rt.GetVersion(),
		})
	}
	var hostingEnvs []wideevents.HostingEnvironmentInfo
	for _, he := range hb.GetEnvironment().GetHostingEnvironments() {
		hostingEnvs = append(hostingEnvs, wideevents.HostingEnvironmentInfo{
			Type:    hostingEnvironmentTypeName(he.GetType()),
			Version: he.GetVersion(),
		})
	}
	var plugins []wideevents.PluginInfo
	for _, p := range hb.GetPlugins() {
		plugins = append(plugins, wideevents.PluginInfo{
			Name:    p.GetName(),
			Version: p.GetVersion(),
		})
	}
	var drivers []string
	for _, d := range hb.GetDrivers() {
		drivers = append(drivers, d.GetType())
	}
	var startTime string
	if st := hb.GetStartTime(); st != nil {
		startTime = st.AsTime().UTC().Format("2006-01-02T15:04:05Z")
	}
	osTag, osVersion, archTag, libc := platformInfo(hb.GetEnvironment().GetPlatform())
	wideevents.Emit(logger, wideevents.WorkerConfigPayload{
		Namespace:                 nsName.String(),
		TaskQueue:                 hb.GetTaskQueue(),
		WorkerInstanceKey:         hb.GetWorkerInstanceKey(),
		WorkerIdentity:            hb.GetWorkerIdentity(),
		StartTime:                 startTime,
		SdkName:                   hb.GetSdkName(),
		SdkVersion:                hb.GetSdkVersion(),
		DeploymentName:            hb.GetDeploymentVersion().GetDeploymentName(),
		BuildID:                   hb.GetDeploymentVersion().GetBuildId(),
		Runtimes:                  runtimes,
		HostingEnvironments:       hostingEnvs,
		OS:                        osTag,
		OSVersion:                 osVersion,
		Architecture:              archTag,
		Libc:                      libc,
		Plugins:                   plugins,
		Drivers:                   drivers,
		WorkflowPollerAutoscaling: hb.GetWorkflowPollerInfo().GetIsAutoscaling(),
		ActivityPollerAutoscaling: hb.GetActivityPollerInfo().GetIsAutoscaling(),
		NexusPollerAutoscaling:    hb.GetNexusPollerInfo().GetIsAutoscaling(),
	})
}

func (e *workerMetricsEmitter) emitWorkerMetricsEvent(nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	type taskEntry struct {
		taskType   string
		slots      *workerpb.WorkerSlotsInfo
		pollerInfo *workerpb.WorkerPollerInfo
		stickyInfo *workerpb.WorkerPollerInfo
	}
	entries := []taskEntry{
		{"workflow", hb.GetWorkflowTaskSlotsInfo(), hb.GetWorkflowPollerInfo(), hb.GetWorkflowStickyPollerInfo()},
		{"activity", hb.GetActivityTaskSlotsInfo(), hb.GetActivityPollerInfo(), nil},
		{"nexus", hb.GetNexusTaskSlotsInfo(), hb.GetNexusPollerInfo(), nil},
		{"local_activity", hb.GetLocalActivitySlotsInfo(), nil, nil}, // local activities run in-process, no pollers
	}

	stats := make(map[string]wideevents.WorkerTaskTypeStats)
	for _, te := range entries {
		// Only include task types that have at least one source message present.
		if te.slots == nil && te.pollerInfo == nil && te.stickyInfo == nil {
			continue
		}
		s := wideevents.WorkerTaskTypeStats{}
		if te.pollerInfo != nil {
			s.CurrentPollers = int32Ptr(te.pollerInfo.GetCurrentPollers())
		}
		if te.stickyInfo != nil {
			s.CurrentStickyPollers = int32Ptr(te.stickyInfo.GetCurrentPollers())
		}
		if te.slots != nil {
			totalSlots := te.slots.GetCurrentAvailableSlots() + te.slots.GetCurrentUsedSlots()
			if te.slots.GetCurrentAvailableSlots() < 0 {
				totalSlots = -1
			}
			s.TotalSlots = int32Ptr(totalSlots)
			s.UsedSlots = int32Ptr(te.slots.GetCurrentUsedSlots())
			s.TotalProcessedTasks = int32Ptr(te.slots.GetTotalProcessedTasks())
			s.TotalFailedTasks = int32Ptr(te.slots.GetTotalFailedTasks())
			s.LastIntervalProcessedTasks = int32Ptr(te.slots.GetLastIntervalProcessedTasks())
			s.LastIntervalFailureTasks = int32Ptr(te.slots.GetLastIntervalFailureTasks())
		}
		stats[te.taskType] = s
	}

	wideevents.Emit(e.eventLogger, wideevents.WorkerMetricsPayload{
		Namespace:         nsName.String(),
		TaskQueue:         hb.GetTaskQueue(),
		WorkerInstanceKey: hb.GetWorkerInstanceKey(),
		TaskTypeStats:     stats,
		Details: map[string]any{
			"sticky_cache_size": hb.GetCurrentStickyCacheSize(),
			"sticky_cache_hit":  hb.GetTotalStickyCacheHit(),
			"sticky_cache_miss": hb.GetTotalStickyCacheMiss(),
		},
	})
}

func int32Ptr(v int32) *int32 { return &v }

// runtimeTypeName returns a clean tag value for a RuntimeType enum, e.g. "go", "cpython", "jvm".
func runtimeTypeName(rt workerpb.EnvironmentInfo_Runtime_RuntimeType) string {
	if rt == workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_UNSPECIFIED {
		return "unknown"
	}
	v := rt.Descriptor().Values().ByNumber(rt.Number())
	if v == nil {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(string(v.Name()), "RUNTIME_TYPE_"))
}

func hostingEnvironmentTypeName(t workerpb.EnvironmentInfo_HostingEnvironment_HostingEnvironmentType) string {
	if t == workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_UNSPECIFIED {
		return "unknown"
	}
	v := t.Descriptor().Values().ByNumber(t.Number())
	if v == nil {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(string(v.Name()), "HOSTING_ENVIRONMENT_TYPE_"))
}

func libcName(l workerpb.EnvironmentInfo_LinuxPlatform_Libc) string {
	if l == workerpb.EnvironmentInfo_LinuxPlatform_LIBC_UNSPECIFIED {
		return ""
	}
	v := l.Descriptor().Values().ByNumber(l.Number())
	if v == nil {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(string(v.Name()), "LIBC_"))
}

func crtName(c workerpb.EnvironmentInfo_WindowsPlatform_Crt) string {
	if c == workerpb.EnvironmentInfo_WindowsPlatform_CRT_UNSPECIFIED {
		return ""
	}
	v := c.Descriptor().Values().ByNumber(c.Number())
	if v == nil {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(string(v.Name()), "CRT_"))
}

func architectureName(a workerpb.EnvironmentInfo_Architecture) string {
	if a == workerpb.EnvironmentInfo_ARCHITECTURE_UNSPECIFIED {
		return "unknown"
	}
	v := a.Descriptor().Values().ByNumber(a.Number())
	if v == nil {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(string(v.Name()), "ARCHITECTURE_"))
}

// platformInfo extracts OS, version, architecture, and libc/crt from the platform oneof.
// The libc field is used for Linux (glibc/musl) and Windows (ucrt/msvcrt/etc).
func platformInfo(p *workerpb.EnvironmentInfo_Platform) (os, version, arch, libc string) {
	if p == nil {
		return "unknown", "", "unknown", ""
	}
	switch v := p.GetVariant().(type) {
	case *workerpb.EnvironmentInfo_Platform_Linux:
		return "linux", v.Linux.GetVersion(), architectureName(v.Linux.GetArchitecture()), libcName(v.Linux.GetLibc())
	case *workerpb.EnvironmentInfo_Platform_Macos:
		return "macos", v.Macos.GetVersion(), architectureName(v.Macos.GetArchitecture()), ""
	case *workerpb.EnvironmentInfo_Platform_Windows:
		return "windows", v.Windows.GetVersion(), architectureName(v.Windows.GetArchitecture()), crtName(v.Windows.GetCrt())
	default:
		return "unknown", "", "unknown", ""
	}
}
