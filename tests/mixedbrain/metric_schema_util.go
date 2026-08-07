package mixedbrain

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
)

type metricSchemas map[string]map[string]struct{}

// These label additions predate the compatibility check. Keeping the allowlist
// metric-specific ensures that another metric adding the same label still fails.
var allowedMetricLabelAdditions = map[string]string{
	"activity_info_count":                 "archetype",
	"activity_info_size":                  "archetype",
	"buffered_events_count":               "archetype",
	"buffered_events_size":                "archetype",
	"chasm_total_size":                    "archetype",
	"child_info_count":                    "archetype",
	"child_info_size":                     "archetype",
	"execution_info_size":                 "archetype",
	"execution_state_size":                "archetype",
	"mutable_state_size":                  "archetype",
	"persisted_mutable_state_size":        "archetype",
	"poll_latency":                        "poll_result",
	"pri_poll_latency":                    "poll_result",
	"request_cancel_info_count":           "archetype",
	"request_cancel_info_size":            "archetype",
	"signal_info_count":                   "archetype",
	"signal_info_size":                    "archetype",
	"signal_request_id_count":             "archetype",
	"signal_request_id_size":              "archetype",
	"task_count":                          "archetype",
	"timer_info_count":                    "archetype",
	"timer_info_size":                     "archetype",
	"total_activity_count":                "archetype",
	"total_child_execution_count":         "archetype",
	"total_request_cancel_external_count": "archetype",
	"total_signal_count":                  "archetype",
	"total_signal_external_count":         "archetype",
	"total_user_timer_count":              "archetype",
}

func fetchMetricSchemas(ctx context.Context, endpoint string) (metricSchemas, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+"/metrics", nil)
	if err != nil {
		return nil, fmt.Errorf("create metrics request: %w", err)
	}

	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape metrics endpoint %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("scrape metrics endpoint %s: status %s", endpoint, resp.Status)
	}

	return parseMetricSchemas(resp.Body)
}

func parseMetricSchemas(r io.Reader) (metricSchemas, error) {
	metricFamilies, err := (&expfmt.TextParser{}).TextToMetricFamilies(r)
	if err != nil {
		return nil, fmt.Errorf("parse Prometheus metrics: %w", err)
	}

	result := make(metricSchemas, len(metricFamilies))
	for name, family := range metricFamilies {
		for _, metric := range family.GetMetric() {
			labelNames := make([]string, 0, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labelNames = append(labelNames, label.GetName())
			}
			slices.Sort(labelNames)
			if result[name] == nil {
				result[name] = make(map[string]struct{})
			}
			result[name][strings.Join(labelNames, ",")] = struct{}{}
		}
	}
	return result, nil
}

func metricSchemaProblems(current, release metricSchemas) []string {
	metricNames := make(map[string]struct{}, len(current)+len(release))
	for name := range current {
		metricNames[name] = struct{}{}
	}
	for name := range release {
		metricNames[name] = struct{}{}
	}

	var problems []string
	for _, name := range slices.Sorted(maps.Keys(metricNames)) {
		currentSchemas, currentFound := current[name]
		releaseSchemas, releaseFound := release[name]

		if len(currentSchemas) > 1 {
			problems = append(problems, fmt.Sprintf(
				"current metric %q has inconsistent label sets: %s",
				name,
				formatSchemas(currentSchemas),
			))
		}
		if len(releaseSchemas) > 1 {
			problems = append(problems, fmt.Sprintf(
				"release metric %q has inconsistent label sets: %s",
				name,
				formatSchemas(releaseSchemas),
			))
		}
		if currentFound && releaseFound && len(currentSchemas) == 1 && len(releaseSchemas) == 1 &&
			firstSchema(currentSchemas) != firstSchema(releaseSchemas) &&
			!isAllowedMetricLabelAddition(name, firstSchema(currentSchemas), firstSchema(releaseSchemas)) {
			problems = append(problems, fmt.Sprintf(
				"metric %q has incompatible labels: current=%s release=%s",
				name,
				formatSchemas(currentSchemas),
				formatSchemas(releaseSchemas),
			))
		}
	}
	return problems
}

func isAllowedMetricLabelAddition(name, currentSchema, releaseSchema string) bool {
	allowedLabel, ok := allowedMetricLabelAdditions[name]
	if !ok {
		return false
	}

	currentLabels := schemaLabels(currentSchema)
	releaseLabels := schemaLabels(releaseSchema)
	if len(currentLabels) != len(releaseLabels)+1 {
		return false
	}
	if _, ok := currentLabels[allowedLabel]; !ok {
		return false
	}
	for label := range releaseLabels {
		if _, ok := currentLabels[label]; !ok {
			return false
		}
	}
	return true
}

func schemaLabels(schema string) map[string]struct{} {
	labels := make(map[string]struct{})
	if schema == "" {
		return labels
	}
	for label := range strings.SplitSeq(schema, ",") {
		labels[label] = struct{}{}
	}
	return labels
}

func firstSchema(schemas map[string]struct{}) string {
	for schema := range schemas {
		return schema
	}
	return ""
}

func formatSchemas(schemas map[string]struct{}) string {
	formatted := make([]string, 0, len(schemas))
	for schema := range schemas {
		if schema == "" {
			formatted = append(formatted, "{}")
		} else {
			formatted = append(formatted, "{"+strings.ReplaceAll(schema, ",", ", ")+"}")
		}
	}
	slices.Sort(formatted)
	return strings.Join(formatted, ", ")
}
