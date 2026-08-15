// Package toolchain generates backend models and executes pinned formal-verification tools.
package toolchain

import (
	"context"

	"go.temporal.io/server/common/testing/umpire/verify"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/fizz"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/ivy"
	pgenerator "go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/p"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/runner"
	"go.temporal.io/server/common/testing/umpire/verify/toolchain/internal/tla"
)

type Backend = runner.Backend
type EquivalenceEvidence = runner.EquivalenceEvidence
type Request = runner.Request
type Toolchain = runner.Toolchain
type PlanOptions = runner.PlanOptions
type IvyDiagnostic = ivy.Diagnostic
type FizzDiagnostic = fizz.Diagnostic

const (
	SANY          = runner.SANY
	TLC           = runner.TLC
	Apalache      = runner.Apalache
	ApalacheProof = runner.ApalacheProof
	P             = runner.P
	PEx           = runner.PEx
	Ivy           = runner.Ivy
	Fizz          = runner.Fizz
)

func Check(ctx context.Context, request Request) (verify.Result, error) {
	return runner.Check(ctx, request)
}

func BackendEquivalenceEvidence(backend Backend) EquivalenceEvidence {
	return runner.BackendEquivalenceEvidence(backend)
}

func ToolVersions() []verify.ToolVersion                  { return runner.ToolVersions() }
func ProfileBounds(profile string) (verify.Bounds, error) { return runner.ProfileBounds(profile) }
func FizzBounds(bounds verify.Bounds) verify.Bounds       { return runner.FizzBounds(bounds) }

func GenerateTLA(model verify.Model) (map[string][]byte, error) { return tla.Generate(model) }
func GenerateP(model verify.Model) (map[string][]byte, error)   { return pgenerator.Generate(model) }
func GenerateIvy(model verify.Model) (map[string][]byte, []IvyDiagnostic, error) {
	return ivy.Generate(model)
}
func GenerateFizz(model verify.Model) (map[string][]byte, []FizzDiagnostic, error) {
	return fizz.Generate(model)
}
func RenderFizzConfig(bounds verify.Bounds) ([]byte, error) { return fizz.RenderConfig(bounds) }

func TLATraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	return tla.TraceVocabulary(model)
}
func IvyTraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	return ivy.TraceVocabulary(model)
}
func FizzTraceVocabulary(model verify.Model) (verify.TraceVocabulary, error) {
	return fizz.TraceVocabulary(model)
}

func TLAActionIdentifier(name string) string   { return tla.ActionIdentifier(name) }
func TLAPropertyIdentifier(name string) string { return tla.PropertyIdentifier(name) }
func IvyActionIdentifier(name string) string   { return ivy.ActionIdentifier(name) }
func IvyPropertyIdentifier(name string) string { return ivy.PropertyIdentifier(name) }
func FizzActionIdentifier(name string) string  { return fizz.ActionIdentifier(name) }
func FizzPropertyIdentifier(name string) string {
	return fizz.PropertyIdentifier(name)
}
