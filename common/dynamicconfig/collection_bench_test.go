package dynamicconfig_test

import (
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

const (
	benchNsMatch  = "match"
	benchNsMatch2 = "match2"
	benchNsOther  = "other"
	benchTqMatch  = "match"
	benchTqOther  = "other"
	benchTqType   = enumspb.TASK_QUEUE_TYPE_ACTIVITY
)

type (
	benchStructType struct {
		Int    int
		String string
		Map    map[string]float64
	}

	// aliases to make the code more readable
	cv   = dynamicconfig.ConstrainedValue
	cstr = dynamicconfig.Constraints
)

var (
	// "Set" means the key will be present in our client with one value (no constraints)
	// "Unset" means the key will not be present
	// "Multi" means the key will be present with multiple constrained values

	benchSetGlobalBool          = dynamicconfig.NewGlobalBoolSetting("bench.set.global.bool", false, "")
	benchUnsetGlobalBool        = dynamicconfig.NewGlobalBoolSetting("bench.unset.global.bool", false, "")
	benchSetNamespaceInt        = dynamicconfig.NewNamespaceIntSetting("bench.set.namespace.int", 55, "")
	benchMultiNamespaceInt      = dynamicconfig.NewNamespaceIntSetting("bench.multi.namespace.int", 55, "")
	benchUnsetNamespaceInt      = dynamicconfig.NewNamespaceIntSetting("bench.unset.namespace.int", 55, "")
	benchSetTaskQueueDuration   = dynamicconfig.NewTaskQueueDurationSetting("bench.set.taskqueue.duration", time.Minute, "")
	benchMultiTaskQueueDuration = dynamicconfig.NewTaskQueueDurationSetting("bench.multi.taskqueue.duration", time.Minute, "")
	benchUnsetTaskQueueDuration = dynamicconfig.NewTaskQueueDurationSetting("bench.unset.taskqueue.duration", time.Minute, "")
	benchSetGlobalStruct        = dynamicconfig.NewGlobalTypedSetting("bench.set.global.struct", benchStructType{Int: 5}, "")
	benchUnsetGlobalStruct      = dynamicconfig.NewGlobalTypedSetting("bench.unset.global.struct", benchStructType{Int: 5}, "")

	benchMultiTaskQueueInt = dynamicconfig.NewTaskQueueIntSetting("bench.multi.taskqueue.int", 10, "")
)

func BenchmarkCollection(b *testing.B) {
	staticClient := dynamicconfig.StaticClient{
		benchSetGlobalBool.Key():   []cv{{Value: true}},
		benchSetNamespaceInt.Key(): []cv{{Value: 88}},
		// benchUnsetGlobalBool not present
		benchMultiNamespaceInt.Key(): []cv{
			{Value: 77, Constraints: cstr{Namespace: benchNsMatch}},
			{Value: 77, Constraints: cstr{Namespace: benchNsMatch2}},
			{Value: 88},
		},
		// benchUnsetNamespaceInt not present
		benchSetTaskQueueDuration.Key(): time.Hour,
		benchMultiTaskQueueDuration.Key(): []cv{
			{Value: time.Second, Constraints: cstr{Namespace: benchNsMatch, TaskQueueName: benchTqMatch, TaskQueueType: benchTqType}},
			{Value: time.Millisecond, Constraints: cstr{Namespace: benchNsMatch}},
			{Value: time.Minute},
		},
		// benchUnsetTaskQueueDuration not present
		benchSetGlobalStruct.Key(): []cv{{Value: map[any]any{"Int": 8, "String": "a setting", "Map": map[any]any{"a key": -0.01}}}},
	}
	col := dynamicconfig.NewCollection(staticClient, log.NewNoopLogger())

	b.Run("set global bool", func(b *testing.B) {
		b.ReportAllocs()
		get := benchSetGlobalBool.Get(col)
		for b.Loop() {
			get()
		}
	})
	b.Run("unset global bool", func(b *testing.B) {
		b.ReportAllocs()
		get := benchUnsetGlobalBool.Get(col)
		for b.Loop() {
			get()
		}
	})

	b.Run("set ns int", func(b *testing.B) {
		b.ReportAllocs()
		get := benchSetNamespaceInt.Get(col)
		for b.Loop() {
			get(benchNsOther)
		}
	})
	b.Run("multi ns int match", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiNamespaceInt.Get(col)
		for b.Loop() {
			get(benchNsMatch)
		}
	})
	b.Run("multi ns int nomatch", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiNamespaceInt.Get(col)
		for b.Loop() {
			get(benchNsOther)
		}
	})
	b.Run("unset ns int", func(b *testing.B) {
		b.ReportAllocs()
		get := benchUnsetNamespaceInt.Get(col)
		for b.Loop() {
			get(benchNsOther)
		}
	})

	b.Run("set tq dur", func(b *testing.B) {
		b.ReportAllocs()
		get := benchSetTaskQueueDuration.Get(col)
		for b.Loop() {
			get(benchNsOther, benchTqOther, benchTqType)
		}
	})
	b.Run("multi tq dur match nstqt", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiTaskQueueDuration.Get(col)
		for b.Loop() {
			get(benchNsMatch, benchTqMatch, benchTqType)
		}
	})
	b.Run("multi tq dur match ns", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiTaskQueueDuration.Get(col)
		for b.Loop() {
			get(benchNsMatch, benchTqOther, benchTqType)
		}
	})
	b.Run("multi tq dur nomatch", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiTaskQueueDuration.Get(col)
		for b.Loop() {
			get(benchNsOther, benchTqOther, benchTqType)
		}
	})
	b.Run("unset tq dur", func(b *testing.B) {
		b.ReportAllocs()
		get := benchUnsetTaskQueueDuration.Get(col)
		for b.Loop() {
			get(benchNsOther, benchTqOther, benchTqType)
		}
	})

	b.Run("set global struct", func(b *testing.B) {
		b.ReportAllocs()
		get := benchSetGlobalStruct.Get(col)
		for b.Loop() {
			get()
		}
	})
	b.Run("unset global struct", func(b *testing.B) {
		b.ReportAllocs()
		get := benchUnsetGlobalStruct.Get(col)
		for b.Loop() {
			get()
		}
	})
}

func BenchmarkConstrainedDefaults(b *testing.B) {
	// This is just for comparing regular defaults to constrained defaults.
	staticClient := dynamicconfig.StaticClient{
		benchMultiTaskQueueInt.Key(): []cv{
			{Constraints: cstr{TaskQueueName: "other-tq"}, Value: 18},
			{Constraints: cstr{Namespace: "other-ns"}, Value: 15},
		},
	}
	col := dynamicconfig.NewCollection(staticClient, log.NewNoopLogger())
	b.Run("single default", func(b *testing.B) {
		b.ReportAllocs()
		get := benchMultiTaskQueueInt.Get(col)
		for b.Loop() { // note each iteration is four gets
			get("my-namespace", "my-task-queue", 1)
			get("my-namespace", "other-tq", 1)
			get("other-ns", "my-task-queue", 1)
			get("other-ns", "other-tq", 1)
		}
	})
	b.Run("constrained default", func(b *testing.B) {
		b.ReportAllocs()
		// note: using existing setting MatchingNumTaskqueueWritePartitions here
		get := dynamicconfig.MatchingNumTaskqueueWritePartitions.Get(col)
		for b.Loop() { // note each iteration is four gets
			get("my-namespace", "my-task-queue", 1)
			get("my-namespace", "other-tq", 1)
			get("other-ns", "my-task-queue", 1)
			get("other-ns", "other-tq", 1)
		}
	})
}
