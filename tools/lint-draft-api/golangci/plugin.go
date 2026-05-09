package golangci

import (
	"github.com/golangci/plugin-module-register/register"
	"go.temporal.io/server/tools/lint-draft-api/rule"
	"golang.org/x/tools/go/analysis"
)

func init() {
	register.Plugin(rule.LinterName, newPlugin)
}

type plugin struct{}

func newPlugin(any) (register.LinterPlugin, error) {
	return plugin{}, nil
}

func (plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return []*analysis.Analyzer{rule.Analyzer}, nil
}

func (plugin) GetLoadMode() string {
	return register.LoadModeTypesInfo
}
