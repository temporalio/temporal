package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
)

func TestRegisterFlagsUpdatesConfig(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	var cfg runConfig
	registerFlags(flags, &cfg)

	err := flags.Parse([]string{
		"-address=frontend:7233",
		"-namespace=load-test",
		"-task-queue=load-task-queue",
		"-workflows=7",
		"-concurrency=3",
		"-activities-each=2",
		"-signals-each=1",
		"-payload-bytes=512",
		"-timeout=30s",
		"-register-namespace=false",
		"-worker=false",
		"-cpu-profile=/tmp/cpu.pprof",
		"-heap-profile=/tmp/heap.pprof",
		"-server-pprof=http://frontend:7936",
		"-server-cpu-profile=/tmp/server-cpu.pprof",
		"-server-heap-profile=/tmp/server-heap.pprof",
		"-server-cpu-profile-duration=15s",
		"-metrics-snapshot-before=http://temporal:8000/metrics=/tmp/temporal.before.metrics",
		"-metrics-snapshot-after=http://temporal:8000/metrics=/tmp/temporal.after.metrics",
		"-metrics-snapshot=http://scylla:9180/metrics=/tmp/scylla.after.metrics",
		"-profile-summary=/tmp/cpu.pprof=/tmp/cpu.top.txt",
		"-profile-summary=/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt",
		"-result-file=/tmp/scyllaload.result.json",
		"-run-metadata-file=/tmp/scyllaload.metadata.json",
	})

	require.NoError(t, err)
	require.Equal(t, "frontend:7233", cfg.address)
	require.Equal(t, "load-test", cfg.namespace)
	require.Equal(t, "load-task-queue", cfg.taskQueue)
	require.Equal(t, 7, cfg.workflows)
	require.Equal(t, 3, cfg.concurrency)
	require.Equal(t, 2, cfg.activitiesEach)
	require.Equal(t, 1, cfg.signalsEach)
	require.Equal(t, 512, cfg.payloadBytes)
	require.Equal(t, 30*time.Second, cfg.timeout)
	require.False(t, cfg.registerNS)
	require.False(t, cfg.runWorker)
	require.Equal(t, "/tmp/cpu.pprof", cfg.cpuProfile)
	require.Equal(t, "/tmp/heap.pprof", cfg.heapProfile)
	require.Equal(t, "http://frontend:7936", cfg.serverPProf)
	require.Equal(t, "/tmp/server-cpu.pprof", cfg.serverCPU)
	require.Equal(t, "/tmp/server-heap.pprof", cfg.serverHeap)
	require.Equal(t, 15*time.Second, cfg.serverCPUTime)
	require.Equal(t, metricSnapshotFlags{
		{url: "http://temporal:8000/metrics", path: "/tmp/temporal.before.metrics"},
	}, cfg.metricSnapshotsBefore)
	require.Equal(t, metricSnapshotFlags{
		{url: "http://temporal:8000/metrics", path: "/tmp/temporal.after.metrics"},
		{url: "http://scylla:9180/metrics", path: "/tmp/scylla.after.metrics"},
	}, cfg.metricSnapshotsAfter)
	require.Equal(t, profileSummaryFlags{
		{profile: "/tmp/cpu.pprof", path: "/tmp/cpu.top.txt"},
		{profile: "/tmp/server-cpu.pprof", path: "/tmp/server-cpu.top.txt"},
	}, cfg.profileSummaries)
	require.Equal(t, "/tmp/scyllaload.result.json", cfg.resultFile)
	require.Equal(t, "/tmp/scyllaload.metadata.json", cfg.runMetadataFile)
}

func TestLoadWorkflowRunsActivities(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(loadWorkflow)
	env.RegisterActivity(loadActivity)

	env.ExecuteWorkflow(loadWorkflow, workflowInput{
		Activities: 3,
		Payload:    []byte("payload"),
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestLoadWorkflowConsumesSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(loadWorkflow)
	env.RegisterActivity(loadActivity)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalName, []byte("one"))
		env.SignalWorkflow(signalName, []byte("two"))
	}, 0)

	env.ExecuteWorkflow(loadWorkflow, workflowInput{
		Signals: 2,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
}

func TestRunLoadCountsUnlaunchedWorkflowsAsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	runner := func(context.Context, client.Client, runConfig, []byte, int64, int) bool {
		close(started)
		<-release
		return true
	}

	resultCh := make(chan runResult, 1)
	go func() {
		resultCh <- runLoadWithRunner(ctx, nil, runConfig{
			workflows:   3,
			concurrency: 1,
			cpuProfile:  "/tmp/cpu.pprof",
			heapProfile: "/tmp/heap.pprof",
			serverCPU:   "/tmp/server-cpu.pprof",
			serverHeap:  "/tmp/server-heap.pprof",
			metricSnapshotsBefore: metricSnapshotFlags{
				{url: "http://temporal:8000/metrics", path: "/tmp/temporal.before.metrics"},
			},
			metricSnapshotsAfter: metricSnapshotFlags{
				{url: "http://temporal:8000/metrics", path: "/tmp/temporal.after.metrics"},
				{url: "http://scylla:9180/metrics", path: "/tmp/scylla.after.metrics"},
			},
			profileSummaries: profileSummaryFlags{
				{profile: "/tmp/cpu.pprof", path: "/tmp/cpu.top.txt"},
				{profile: "/tmp/server-cpu.pprof", path: "/tmp/server-cpu.top.txt"},
			},
			resultFile:      "/tmp/scyllaload.result.json",
			runMetadataFile: "/tmp/scyllaload.metadata.json",
		}, runner)
	}()

	<-started
	cancel()
	close(release)

	result := <-resultCh
	require.Equal(t, int64(1), result.Completed)
	require.Equal(t, int64(2), result.Failed)
	require.Equal(t, 3, result.Workflows)
	require.Equal(t, "/tmp/cpu.pprof", result.CPUProfile)
	require.Equal(t, "/tmp/heap.pprof", result.HeapProfile)
	require.Equal(t, "/tmp/server-cpu.pprof", result.ServerCPUProfile)
	require.Equal(t, "/tmp/server-heap.pprof", result.ServerHeapProfile)
	require.Equal(t, []string{"/tmp/temporal.before.metrics"}, result.MetricSnapshotsBefore)
	require.Equal(t, []string{"/tmp/temporal.after.metrics", "/tmp/scylla.after.metrics"}, result.MetricSnapshotsAfter)
	require.Equal(t, []string{"/tmp/cpu.top.txt", "/tmp/server-cpu.top.txt"}, result.ProfileSummaries)
	require.Equal(t, "/tmp/scyllaload.result.json", result.ResultFile)
	require.Equal(t, "/tmp/scyllaload.metadata.json", result.RunMetadataFile)
}

func TestServerProfilesFetchPPROFEndpoints(t *testing.T) {
	var profileCalled bool
	var heapCalled bool
	var profileSeconds string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/pprof/profile":
			profileCalled = true
			profileSeconds = r.URL.Query().Get("seconds")
			_, _ = w.Write([]byte("cpu profile"))
		case "/debug/pprof/heap":
			heapCalled = true
			_, _ = w.Write([]byte("heap profile"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	dir := t.TempDir()
	cpuPath := dir + "/cpu.pprof"
	heapPath := dir + "/heap.pprof"
	cfg := runConfig{
		serverPProf:   server.URL,
		serverCPU:     cpuPath,
		serverHeap:    heapPath,
		serverCPUTime: 2 * time.Second,
	}

	waitCPU, err := startServerCPUProfile(t.Context(), cfg)
	require.NoError(t, err)
	require.NoError(t, waitCPU())
	require.NoError(t, writeServerHeapProfile(t.Context(), cfg))

	cpuBytes, err := os.ReadFile(cpuPath)
	require.NoError(t, err)
	require.Equal(t, "cpu profile", string(cpuBytes))
	heapBytes, err := os.ReadFile(heapPath)
	require.NoError(t, err)
	require.Equal(t, "heap profile", string(heapBytes))
	require.True(t, profileCalled)
	require.Equal(t, "2", profileSeconds)
	require.True(t, heapCalled)
}

func TestValidateConfigRequiresServerPPROFForServerProfiles(t *testing.T) {
	err := validateConfig(runConfig{
		workflows:     1,
		concurrency:   1,
		serverCPU:     "/tmp/server-cpu.pprof",
		serverCPUTime: time.Second,
	})
	require.ErrorContains(t, err, "-server-pprof")

	err = validateConfig(runConfig{
		workflows:     1,
		concurrency:   1,
		serverHeap:    "/tmp/server-heap.pprof",
		serverCPUTime: time.Second,
	})
	require.ErrorContains(t, err, "-server-pprof")
}

func TestMetricSnapshotFlags(t *testing.T) {
	var snapshots metricSnapshotFlags

	require.NoError(t, snapshots.Set("http://temporal:8000/metrics=/tmp/temporal.metrics"))
	require.NoError(t, snapshots.Set("http://scylla:9180/metrics=/tmp/scylla.metrics"))

	require.Equal(t, "http://temporal:8000/metrics=/tmp/temporal.metrics,http://scylla:9180/metrics=/tmp/scylla.metrics", snapshots.String())
	require.Equal(t, []string{"/tmp/temporal.metrics", "/tmp/scylla.metrics"}, snapshots.paths())
	require.Error(t, snapshots.Set("missing-output-path"))
}

func TestProfileSummaryFlags(t *testing.T) {
	var summaries profileSummaryFlags

	require.NoError(t, summaries.Set("/tmp/cpu.pprof=/tmp/cpu.top.txt"))
	require.NoError(t, summaries.Set("/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt"))

	require.Equal(t, "/tmp/cpu.pprof=/tmp/cpu.top.txt,/tmp/server-cpu.pprof=/tmp/server-cpu.top.txt", summaries.String())
	require.Equal(t, []string{"/tmp/cpu.top.txt", "/tmp/server-cpu.top.txt"}, summaries.paths())
	require.Error(t, summaries.Set("missing-output-path"))
}

func TestWriteMetricSnapshots(t *testing.T) {
	metricBody := "temporal_persistence_requests 1\n"
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		_, _ = w.Write([]byte(metricBody))
	}))
	defer server.Close()

	outputPath := t.TempDir() + "/metrics.prom"
	err := writeMetricSnapshots(t.Context(), []metricSnapshot{
		{url: server.URL + "/metrics", path: outputPath},
	})
	require.NoError(t, err)

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Equal(t, metricBody, string(data))
	require.Equal(t, "/metrics", requestedPath)
}

func TestWriteRunMetadata(t *testing.T) {
	var pprofPath string
	var metricsPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/debug/pprof/":
			pprofPath = r.URL.Path
		case "/metrics":
			metricsPath = r.URL.Path
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	t.Setenv("CASSANDRA_SEEDS", "node1,node2,node3")
	t.Setenv("CASSANDRA_MAX_CONNS", "12")
	t.Setenv("CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE", "2")

	outputPath := t.TempDir() + "/metadata.json"
	require.NoError(t, writeRunMetadata(t.Context(), runConfig{
		address:         "127.0.0.1:7233",
		namespace:       "scylla-load",
		taskQueue:       "scylla-load",
		workflows:       7,
		concurrency:     3,
		activitiesEach:  2,
		signalsEach:     1,
		payloadBytes:    512,
		serverPProf:     server.URL,
		runMetadataFile: outputPath,
		metricSnapshotsBefore: metricSnapshotFlags{
			{url: server.URL + "/metrics", path: "/tmp/temporal.before.metrics"},
		},
	}))

	data, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	var metadata runMetadata
	require.NoError(t, json.Unmarshal(data, &metadata))
	require.False(t, metadata.StartedAt.IsZero())
	metadata.StartedAt = time.Time{}
	require.Equal(t, "12", metadata.Environment["CASSANDRA_MAX_CONNS"])
	require.Equal(t, "2", metadata.Environment["CASSANDRA_MAX_EXCESS_SHARD_CONNECTIONS_RATE"])
	require.Equal(t, "node1,node2,node3", metadata.Environment["CASSANDRA_SEEDS"])
	metadata.Environment = nil
	require.Equal(t, runMetadata{
		Address:        "127.0.0.1:7233",
		Namespace:      "scylla-load",
		TaskQueue:      "scylla-load",
		Workflows:      7,
		Concurrency:    3,
		ActivitiesEach: 2,
		SignalsEach:    1,
		PayloadBytes:   512,
		GoVersion:      runtime.Version(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		NumCPU:         runtime.NumCPU(),
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
		PProfEndpoint: &endpointCheck{
			Name:   "pprof",
			URL:    server.URL + "/debug/pprof/",
			Status: "200 OK",
		},
		MetricSnapshotEndpoints: []endpointCheck{
			{
				Name:   "metrics",
				URL:    server.URL + "/metrics",
				Status: "200 OK",
			},
		},
	}, metadata)
	require.Equal(t, "/debug/pprof/", pprofPath)
	require.Equal(t, "/metrics", metricsPath)
}

func TestWriteProfileSummaries(t *testing.T) {
	dir := t.TempDir()
	fakeGo := dir + "/go"
	argsPath := dir + "/args.txt"
	err := os.WriteFile(fakeGo, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > "+argsPath+"\nprintf 'flat flat%% sum%% cum cum%% name\\n10ms 100%% 100%% 10ms 100%% test.hot\\n'\n"), 0o755)
	require.NoError(t, err)

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+oldPath)

	summaryPath := dir + "/cpu.top.txt"
	require.NoError(t, writeProfileSummaries(t.Context(), []profileSummary{
		{profile: "/tmp/cpu.pprof", path: summaryPath},
	}))

	args, err := os.ReadFile(argsPath)
	require.NoError(t, err)
	require.Equal(t, "tool\npprof\n-top\n/tmp/cpu.pprof\n", string(args))
	summary, err := os.ReadFile(summaryPath)
	require.NoError(t, err)
	require.Contains(t, string(summary), "test.hot")
}

func TestWriteResultFile(t *testing.T) {
	resultPath := t.TempDir() + "/result.json"
	result := runResult{
		Address:    "127.0.0.1:7233",
		Namespace:  "scylla-load",
		Workflows:  7,
		ResultFile: resultPath,
	}

	require.NoError(t, writeResultFile(resultPath, result))

	data, err := os.ReadFile(resultPath)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"address": "127.0.0.1:7233",
		"namespace": "scylla-load",
		"taskQueue": "",
		"workflows": 7,
		"concurrency": 0,
		"activitiesEach": 0,
		"signalsEach": 0,
		"payloadBytes": 0,
		"elapsed": 0,
		"completed": 0,
		"failed": 0,
		"workflowsPerSec": 0,
		"resultFile": "`+resultPath+`"
	}`, string(data))
}
