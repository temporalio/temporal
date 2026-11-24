// Package golangci registers parallelsuitelint as a golangci-lint module plugin.
package golangci

import (
	"fmt"

	"github.com/golangci/plugin-module-register/register"
	"go.temporal.io/testx/parallelsuitelint"
	"golang.org/x/tools/go/analysis"
)

type plugin struct {
	AwaitPackages []string `json:"await-packages"`
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
	return []*analysis.Analyzer{parallelsuitelint.NewWithAwaitPackages(p.AwaitPackages...)}, nil
}

func (*plugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
