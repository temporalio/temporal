package testcore

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/testx/failpoint"
)

// FailpointRegistry returns the failpoint registry of the cluster, for example to look up a
// failpoint. On a shared cluster it holds the failpoints of other tests too.
func FailpointRegistry(cluster *TestCluster) *failpoint.Registry {
	return cluster.host.failpointSource
}

// StandardBuggifyKeys returns the standard failpoints.buggify.keys allowlist: gRPC and HTTP calls
// between services fail with Unavailable, the hand-placed keys take their branch, and dynamic
// config settings get other values. It lists no persistence, response or health key, so BUGGIFY
// never fails those: the health service has an entry with an activation probability of 0, before the
// broad gRPC entry that would match it. docs/development/failpoints.md has the same list as YAML to
// copy into a server config, and a test compares the two.
func StandardBuggifyKeys() []config.FailpointsBuggifyKey {
	probability := func(p float64) *float64 { return &p }
	return []config.FailpointsBuggifyKey{
		{Match: "grpc:/grpc.health.v1.Health/*", ActivationProbability: probability(0), FireProbability: probability(0)},
		{Match: "grpc:*", ActivationProbability: probability(0.1), FireProbability: probability(0.01), Error: "Unavailable"},
		{Match: "http:*", ActivationProbability: probability(0.1), FireProbability: probability(0.01), Error: "Unavailable"},
		{Match: matching.SkipSyncMatchKey.Name(), ActivationProbability: probability(0.25), FireProbability: probability(0.25)},
		{Match: serverbuggify.SettingPrefix + "*", ActivationProbability: probability(0.25), FireProbability: probability(0.25)},
	}
}

// WithBuggify gives the servers of the test's cluster the failpoints.buggify block of the static
// config, which seeds BUGGIFY, lists the keys that it applies to (see StandardBuggifyKeys) and
// switches on the trace. It is the only way that a test cluster configures BUGGIFY: the servers
// read no environment variable and no dynamic config setting. BUGGIFY applies to the whole
// cluster, so this implies a dedicated cluster.
func WithBuggify(buggify config.FailpointsBuggify) TestOption {
	return func(o *testOptions) {
		o.dedicatedCluster = true
		o.dedicatedReason = "BUGGIFY static config"
		o.clusterOptions = append(o.clusterOptions, WithClusterBuggify(buggify))
	}
}

// WithClusterBuggify gives the servers of the cluster the failpoints.buggify block of the static
// config, like WithBuggify, as an option of a suite's cluster, such as that of an XDC suite. It
// does not make the cluster dedicated: the suite already owns its clusters.
func WithClusterBuggify(buggify config.FailpointsBuggify) TestClusterOption {
	return func(params *testClusterParams) {
		params.Buggify = buggify
	}
}
