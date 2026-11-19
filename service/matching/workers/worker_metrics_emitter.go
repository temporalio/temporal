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
	handler     metrics.Handler
	config      WorkerMetricsConfig
	eventLogger otellog.Logger
}

func (e *workerMetricsEmitter) emit(nsID namespace.ID, nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	// The SDK aggregates all workers on the same Client into one heartbeat RPC, so
	// len(heartbeats) approximates workers-per-process. It's per-Client-per-Namespace,
	// not strictly per-process, but multiple Clients per process is uncommon in practice.
	metrics.WorkerRegistryWorkersPerProcess.With(e.handler).Record(int64(len(heartbeats)))

	enablePluginMetrics := e.config.EnablePluginMetrics != nil && e.config.EnablePluginMetrics()
	enablePollerAutoscalingMetrics := e.config.EnablePollerAutoscalingMetrics != nil && e.config.EnablePollerAutoscalingMetrics()
	enableStorageDriverMetrics := e.config.ExternalPayloadsEnabled != nil && e.config.ExternalPayloadsEnabled(nsName.String())

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

		// Per-task-queue heartbeat metrics. The total heartbeat count is always emitted; the poller autoscaling
		// heartbeat count is only emitted if enabled.
		e.emitPollerHeartbeat(nsID, nsName, hb, enablePollerAutoscalingMetrics)

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
	}
}

// emitPollerHeartbeat records the total heartbeat count for each task type the worker is polling. If
// enableAutoscalingMetrics is set, it additionally records the autoscaling heartbeat count for each poller that has
// autoscaling enabled, so the autoscaling count is a subset of the total.
func (e *workerMetricsEmitter) emitPollerHeartbeat(
	nsID namespace.ID,
	nsName namespace.Name,
	hb *workerpb.WorkerHeartbeat,
	enableAutoscalingMetrics bool,
) {
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	record := func(taskType enumspb.TaskQueueType, pollerInfo *workerpb.WorkerPollerInfo) {
		if pollerInfo == nil {
			return // worker is not polling this task type
		}
		tq := family.TaskQueue(taskType)
		breakdownByTQ := e.config.BreakdownMetricsByTaskQueue != nil &&
			e.config.BreakdownMetricsByTaskQueue(nsName.String(), hb.GetTaskQueue(), taskType)
		handler := metrics.GetPerTaskQueueScope(e.handler, nsName.String(), tq, breakdownByTQ)
		metrics.WorkerHeartbeatCount.With(handler).Record(1)
		if enableAutoscalingMetrics && pollerInfo.GetIsAutoscaling() {
			metrics.PollerAutoscalingHeartbeatCount.With(handler).Record(1)
		}
	}

	record(enumspb.TASK_QUEUE_TYPE_WORKFLOW, hb.WorkflowPollerInfo)
	record(enumspb.TASK_QUEUE_TYPE_ACTIVITY, hb.ActivityPollerInfo)
	record(enumspb.TASK_QUEUE_TYPE_NEXUS, hb.NexusPollerInfo)
}

func emitWorkerConfigEvent(logger otellog.Logger, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	runtimes := make(map[string]string)
	for _, rt := range hb.GetEnvironment().GetRuntimes() {
		runtimes[runtimeTypeName(rt.GetType())] = rt.GetVersion()
	}
	hostingEnvs := make(map[string]string)
	for _, he := range hb.GetEnvironment().GetHostingEnvironments() {
		hostingEnvs[hostingEnvironmentTypeName(he.GetType())] = he.GetVersion()
	}
	plugins := make(map[string]string)
	for _, p := range hb.GetPlugins() {
		plugins[p.GetName()] = p.GetVersion()
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
	// Stats are only set for the source messages present in the heartbeat.
	taskTypeStats := func(
		slots *workerpb.WorkerSlotsInfo,
		pollerInfo *workerpb.WorkerPollerInfo,
		stickyInfo *workerpb.WorkerPollerInfo,
	) wideevents.WorkerTaskTypeStats {
		s := wideevents.WorkerTaskTypeStats{}
		if pollerInfo != nil {
			s.CurrentPollers = new(pollerInfo.GetCurrentPollers())
			// A target of 0 means the SDK did not report one: either it predates
			// the field, or the pool never started. A real target is always at
			// least 1, so leave the stat unset rather than export a false zero.
			if target := pollerInfo.GetTargetPollers(); target > 0 {
				s.TargetPollers = new(target)
			}
		}
		if stickyInfo != nil {
			s.CurrentStickyPollers = new(stickyInfo.GetCurrentPollers())
			if target := stickyInfo.GetTargetPollers(); target > 0 {
				s.TargetStickyPollers = new(target)
			}
		}
		if slots != nil {
			totalSlots := slots.GetCurrentAvailableSlots() + slots.GetCurrentUsedSlots()
			if slots.GetCurrentAvailableSlots() < 0 { // -1 means unknown (resource-based slot suppliers)
				totalSlots = -1
			}
			s.TotalSlots = new(totalSlots)
			s.UsedSlots = new(slots.GetCurrentUsedSlots())
			s.TotalProcessedTasks = new(slots.GetTotalProcessedTasks())
			s.TotalFailedTasks = new(slots.GetTotalFailedTasks())
		}
		return s
	}

	wideevents.Emit(e.eventLogger, wideevents.WorkerMetricsPayload{
		Namespace:         nsName.String(),
		TaskQueue:         hb.GetTaskQueue(),
		WorkerInstanceKey: hb.GetWorkerInstanceKey(),
		Workflow:          taskTypeStats(hb.GetWorkflowTaskSlotsInfo(), hb.GetWorkflowPollerInfo(), hb.GetWorkflowStickyPollerInfo()),
		Activity:          taskTypeStats(hb.GetActivityTaskSlotsInfo(), hb.GetActivityPollerInfo(), nil),
		Nexus:             taskTypeStats(hb.GetNexusTaskSlotsInfo(), hb.GetNexusPollerInfo(), nil),
		LocalActivity:     taskTypeStats(hb.GetLocalActivitySlotsInfo(), nil, nil), // local activities run in-process, no pollers
		StickyCacheSize:   hb.GetCurrentStickyCacheSize(),
		StickyCacheHit:    hb.GetTotalStickyCacheHit(),
		StickyCacheMiss:   hb.GetTotalStickyCacheMiss(),
	})
}

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
