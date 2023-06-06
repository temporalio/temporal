package metrics

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"go.temporal.io/server/common/metrics/internal"
)

func ExampleWithHelpText() {
	r := &RegistryBuilder{}

	r.AddTimer("example_timer", WithHelpText("Help text for example_timer"))

	if err := printMetricsPage(r); err != nil {
		panic(err)
	}
	// Output:
	// # HELP example_timer Help text for example_timer
	// # TYPE example_timer summary
	// example_timer_sum 0
	// example_timer_count 0
}

func printMetricsPage(r *RegistryBuilder) error {
	pr := prometheus.NewRegistry()
	if err := r.Build(pr); err != nil {
		return err
	}

	return internal.PrintMetricsPage(pr, os.Stdout)
}
