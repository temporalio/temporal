// Package golangci registers parallelsuitelint as a golangci-lint module plugin.
package golangci

import (
	"fmt"
	"strings"

	"github.com/golangci/plugin-module-register/register"
	"go.temporal.io/testx/parallelsuitelint"
	"golang.org/x/tools/go/analysis"
)

type plugin struct {
	AllowNonParallelsuiteTests []string `json:"allow-non-parallelsuite-tests"`
}

func init() {
	register.Plugin("parallelsuitelint", newPlugin)
}

func newPlugin(raw any) (register.LinterPlugin, error) {
	settings, err := register.DecodeSettings[plugin](raw)
	if err != nil {
		return nil, fmt.Errorf("loading parallelsuitelint settings: %w", err)
	}
	return &settings, nil
}

func (p *plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	analyzer := parallelsuitelint.New()
	if err := analyzer.Flags.Set("allow-non-parallelsuite-tests", strings.Join(p.AllowNonParallelsuiteTests, ",")); err != nil {
		return nil, fmt.Errorf("configuring adoption exclusions: %w", err)
	}
	return []*analysis.Analyzer{analyzer}, nil
}

func (*plugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
