package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/common/testing/umpire/verify"
)

func classify(request Request, execution execution) (result verify.Result) {
	output := execution.output
	unsupported := unsupportedForBackend(request.Backend, request.Unsupported)
	result = verify.Result{
		Backend:      string(request.Backend),
		Target:       request.Target,
		ModelVersion: request.Model.Version,
		Profile:      request.Profile,
		ToolVersion:  request.ToolVersion,
		Termination:  verify.Completed,
		Fairness:     slices.Clone(request.Fairness),
		Abstractions: slices.Clone(request.Abstractions),
		Unsupported:  unsupported,
	}
	defer qualifyResultEvidence(request, &result)
	if errors.Is(execution.err, context.DeadlineExceeded) || errors.Is(execution.err, context.Canceled) {
		result.Status = verify.Inconclusive
		if errors.Is(execution.err, context.DeadlineExceeded) {
			result.Termination = verify.Timeout
		} else {
			result.Termination = verify.Interrupted
		}
		return result
	}
	lower := strings.ToLower(output)
	limit := reportedLimit(request.Backend, output)
	fizzPassed := request.Backend == Fizz && strings.Contains(output, "PASSED: Model checker completed successfully")
	fizzFailed := request.Backend == Fizz && fizzFailurePattern.MatchString(output)
	fizzAnyFailure := request.Backend == Fizz && strings.Contains(output, "FAILED:")
	fizzDeadlock := request.Backend == Fizz && strings.Contains(output, "DEADLOCK")
	counterexampleFound := isCounterexample(lower) || (request.Backend == Ivy && ivyFailurePattern.MatchString(output)) || fizzFailed
	switch {
	case request.Backend == Fizz && request.ToolVersion != pinnedToolVersion("fizzbee"):
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = fmt.Sprintf("unsupported FizzBee tool version %q", request.ToolVersion)
	case request.Backend == Fizz && fizzPassed && (fizzAnyFailure || fizzDeadlock):
		result.Status = verify.Inconclusive
		result.Termination = verify.ParseFailure
		result.Diagnostic = "FizzBee output contains contradictory completion markers"
	case request.Backend == PEx && strings.Contains(lower, "cycle detected: infinite loop"):
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = "PEx reported an implicit cycle outside the generated Umpire properties"
	case counterexampleFound:
		result.Status = verify.Counterexample
		result.NativeTrace = output
		semanticBackend := request.Backend == P || request.Backend == PEx || request.Backend == TLC || request.Backend == Apalache || request.Backend == ApalacheProof || request.Backend == Ivy || request.Backend == Fizz
		if semanticBackend && request.Model.Version == "" {
			result.Status = verify.Inconclusive
			result.Termination = verify.EvidenceFailure
			result.Diagnostic = "native-trace-malformed: canonical model is unavailable"
			break
		}
		nativeProperty := nativeFailedProperty(request, output)
		properties := failedPropertyCandidates(request, nativeProperty)
		if len(properties) == 0 {
			result.Status = verify.Inconclusive
			result.Termination = verify.EvidenceFailure
			result.Diagnostic = "property-unmapped: counterexample has no recognized failed property"
			break
		}
		if semanticBackend {
			var evidence verify.TraceEvidence
			var evidenceErr error
			switch request.Backend {
			case P, PEx:
				if execution.nativeTraceErr != nil {
					evidenceErr = execution.nativeTraceErr
					break
				}
				tracePayload := output
				if execution.nativeTrace != "" {
					tracePayload = pTracePayload(execution.nativeTrace)
					result.NativeTrace = tracePayload
				}
				if len(tracePayload) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: P counterexample trace exceeds 4 MiB")
					break
				}
				parsed := normalizeActions(request, tracePayload)
				if len(parsed) == 0 {
					evidenceErr = errors.New("native-trace-missing: P counterexample has no Umpire action records")
					break
				}
				evidence.Steps = make([]verify.ObservedTraceStep, len(parsed))
				for index, step := range parsed {
					evidence.Steps[index] = verify.ObservedTraceStep{Action: step.Action, Bindings: step.Bindings}
				}
			case TLC:
				if len(output) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: TLC textual trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeTLCTrace(request, output)
			case Apalache, ApalacheProof:
				result.NativeTrace = execution.nativeTrace
				if execution.nativeTraceErr != nil {
					evidenceErr = execution.nativeTraceErr
					break
				}
				if execution.nativeTrace == "" {
					evidenceErr = errors.New("native-trace-missing: Apalache counterexample has no ITF trace")
					break
				}
				if len(execution.nativeTrace) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: Apalache ITF trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeITFTrace(request, execution.nativeTrace)
			case Ivy:
				if len(output) > maxNativeTraceBytes {
					evidenceErr = errors.New("native-trace-too-large: Ivy textual trace exceeds 4 MiB")
					break
				}
				evidence, evidenceErr = decodeIvyTrace(request, output)
			case Fizz:
				result.NativeTrace = execution.nativeTrace
				if execution.nativeTraceErr != nil {
					evidenceErr = execution.nativeTraceErr
					break
				}
				if execution.nativeTrace == "" {
					evidenceErr = errors.New("native-trace-missing: FizzBee counterexample has no error graph")
					break
				}
				evidence, evidenceErr = decodeFizzTrace(request, execution.nativeTrace)
			default:
			}
			if evidenceErr != nil {
				result.Status = verify.Inconclusive
				result.Termination = verify.EvidenceFailure
				result.Diagnostic = evidenceErr.Error()
				break
			}
			property, normalized, normalizationErr := normalizeEvidence(request.Model, properties, evidence)
			if normalizationErr != nil && request.Bounds.MaxDepth != 0 &&
				(request.Backend == P || request.Backend == PEx || request.Backend == Ivy) {
				nativeNormalizationErr := normalizationErr
				canonicalProperties := slices.Clone(properties)
				if request.Backend == Ivy {
					canonicalProperties = append(canonicalProperties, canonicalPropertyNames(request.Model)...)
				}
				property, normalized, normalizationErr = findCanonicalCounterexample(request.Model, canonicalProperties, request.Bounds.MaxDepth)
				if normalizationErr != nil {
					normalizationErr = nativeNormalizationErr
				}
			}
			if normalizationErr != nil {
				result.Status = verify.Inconclusive
				result.Termination = verify.EvidenceFailure
				result.Diagnostic = normalizationErr.Error()
				break
			}
			result.FailedProperty = property
			result.Trace = normalized
		} else {
			result.FailedProperty = properties[0]
			result.Trace = normalizeActions(request, output)
		}
	case limit != "":
		result.Status = verify.Inconclusive
		result.Termination = limit
		if limit == verify.ToolLimit && strings.Contains(lower, "toomanychoicesexception") {
			result.Diagnostic = "PEx reached its native per-statement choice limit"
		}
	case incompleteTransitionFailure(request.Backend, lower):
		result.Status = verify.Inconclusive
		result.Termination = verify.EvidenceFailure
		result.NativeTrace = output
		result.Diagnostic = "native-trace-malformed: backend transition leaves canonical state unconstrained"
	case execution.err != nil:
		result.Status = verify.Inconclusive
		result.Termination = verify.ToolError
		result.Diagnostic = execution.err.Error()
	case len(unsupported) > 0:
		result.Status = verify.UnsupportedStatus
		result.Diagnostic = fmt.Sprintf("%s: %s", unsupported[0].Construct, unsupported[0].Reason)
	case request.Backend == SANY && strings.Contains(output, "Semantic processing of module"):
		result.Status = verify.Generated
	case request.Backend == TLC && strings.Contains(output, "Model checking completed. No error has been found"):
		result.Status = verify.FiniteExhaustive
	case request.Backend == ApalacheProof && strings.Count(lower, "checker reports no error") == 3:
		result.Status = verify.InvariantProved
	case request.Backend == Apalache && (strings.Contains(output, "NoError") || strings.Contains(lower, "no counterexample") || strings.Contains(lower, "checker reports no error")):
		result.Status = verify.BoundedNoCounterexample
	case (request.Backend == P || request.Backend == PEx) && strings.Contains(lower, "found 0 bugs"):
		result.Status = verify.BoundedNoCounterexample
	case request.Backend == Ivy && (ivySuccessPattern.MatchString(output) || strings.Contains(lower, "finished with 0 errors")):
		result.Status = verify.InvariantProved
	case request.Backend == Fizz && fizzPassed && !fizzDeadlock:
		result.Status = verify.BoundedNoCounterexample
	default:
		result.Status = verify.Inconclusive
		result.Termination = verify.ParseFailure
		result.Diagnostic = "tool output did not contain a recognized completion marker"
	}
	if matches := stateCountsPattern.FindStringSubmatch(output); len(matches) == 3 {
		if err := setStateCounts(&result, matches[1], matches[2]); err != nil {
			return invalidStateCounts(result, err)
		}
	} else if matches := fizzStateCountsPattern.FindStringSubmatch(output); len(matches) == 3 {
		if err := setStateCounts(&result, matches[1], matches[2]); err != nil {
			return invalidStateCounts(result, err)
		}
	}
	return result
}

func findCanonicalCounterexample(model verify.Model, properties []string, maxDepth uint64) (string, []verify.TraceStep, error) {
	properties = uniqueStrings(properties)
	type match struct {
		property string
		trace    []verify.TraceStep
	}
	var matches []match
	var firstErr error
	for _, property := range properties {
		trace, err := verify.FindCounterexampleTrace(model, property, maxDepth)
		if err == nil {
			matches = append(matches, match{property: property, trace: trace})
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if len(matches) == 1 {
		return matches[0].property, matches[0].trace, nil
	}
	if len(matches) > 1 {
		return "", nil, fmt.Errorf("property-unmapped: native property maps to %d violated canonical properties", len(matches))
	}
	return "", nil, firstErr
}

func canonicalPropertyNames(model verify.Model) []string {
	properties := make([]string, 0, len(model.Properties)+len(model.Relations)*3)
	for _, property := range model.Properties {
		properties = append(properties, property.Name)
	}
	for _, relation := range model.Relations {
		properties = append(properties, "relation "+relation.Name+" endpoints")
		if relation.SourceCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" source cardinality")
		}
		if relation.TargetCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" target cardinality")
		}
	}
	return properties
}

func incompleteTransitionFailure(backend Backend, lowerOutput string) bool {
	switch backend {
	case TLC:
		return strings.Contains(lowerOutput, "successor state is not completely specified by the next-state action")
	case Apalache, ApalacheProof:
		return strings.Contains(lowerOutput, "assignment error: no assignments found for:")
	default:
		return false
	}
}

func qualifyResultEvidence(request Request, result *verify.Result) {
	if result == nil || request.Backend == "" {
		return
	}
	environmentName := "formal/" + string(request.Backend)
	if request.Profile != "" {
		environmentName += "/" + request.Profile
	}
	properties := make([]string, 0, len(request.Model.Properties))
	for _, property := range request.Model.Properties {
		properties = append(properties, property.Name)
	}
	if result.Status == verify.Counterexample && result.FailedProperty != "" {
		properties = append(properties, result.FailedProperty)
	}
	slices.Sort(properties)
	properties = slices.Compact(properties)
	result.Environment = umpire.EnvironmentProfile{
		Name:                environmentName,
		DriveCapabilities:   []string{"formal-model-checking"},
		ObservationSources:  []umpire.EvidenceSource{umpire.FormalModelEvidence},
		OrderingGuarantees:  []umpire.OrderingGuarantee{umpire.CausalOrdering, umpire.SourceSequenceOrdering},
		IdentityLineage:     true,
		SupportedProperties: properties,
		Retention: umpire.RetentionPolicy{
			RedactPayloads: true,
			RedactSecrets:  true,
		},
	}
	result.Observations = []umpire.EvidenceSource{umpire.FormalModelEvidence}
	result.Omissions = nil
	for _, unsupported := range result.Unsupported {
		result.Omissions = append(result.Omissions, unsupported.Construct+": "+unsupported.Reason)
	}
	if result.Status == verify.Inconclusive {
		result.Omissions = append(result.Omissions, "verification:"+string(result.Termination))
	}
	if result.Status == verify.Generated {
		result.Omissions = append(result.Omissions, "verification:not-executed")
	}
	for _, property := range properties {
		claim := umpire.QualifiedClaim{
			ModelVersion: request.Model.Version,
			Target:       request.Target,
			Property:     property,
			Environment:  environmentName,
			Observed:     []umpire.EvidenceSource{umpire.FormalModelEvidence},
		}
		switch result.Status {
		case verify.BoundedNoCounterexample, verify.FiniteExhaustive, verify.InvariantProved:
			claim.Status = umpire.ClaimEstablished
		case verify.Counterexample:
			if property == result.FailedProperty {
				claim.Status = umpire.ClaimViolated
			} else {
				claim.Status = umpire.ClaimInconclusive
				claim.Omissions = []string{"run:terminated-after-counterexample"}
				claim.Diagnostic = "the failing run did not establish this property"
			}
		case verify.UnsupportedStatus, verify.Generated:
			claim.Status = umpire.ClaimUnsupported
			claim.Omissions = slices.Clone(result.Omissions)
			claim.Diagnostic = result.Diagnostic
		case verify.Inconclusive:
			claim.Status = umpire.ClaimInconclusive
			claim.Omissions = slices.Clone(result.Omissions)
			claim.Diagnostic = result.Diagnostic
		default:
			claim.Status = umpire.ClaimInconclusive
			claim.Omissions = []string{"verification:unclassified"}
		}
		result.Claims = append(result.Claims, claim)
	}
}

func setStateCounts(result *verify.Result, generated, distinct string) error {
	generatedStates, err := strconv.ParseUint(generated, 10, 64)
	if err != nil {
		return fmt.Errorf("parse generated state count %q: %w", generated, err)
	}
	distinctStates, err := strconv.ParseUint(distinct, 10, 64)
	if err != nil {
		return fmt.Errorf("parse distinct state count %q: %w", distinct, err)
	}
	result.GeneratedStates = generatedStates
	result.DistinctStates = distinctStates
	return nil
}

func invalidStateCounts(result verify.Result, err error) verify.Result {
	result.Status = verify.Inconclusive
	result.Termination = verify.ParseFailure
	result.GeneratedStates = 0
	result.DistinctStates = 0
	result.Diagnostic = err.Error()
	return result
}

func reportedLimit(backend Backend, output string) verify.TerminationReason {
	lower := strings.ToLower(output)
	switch {
	case timeoutPattern.MatchString(output) || (backend == Apalache || backend == ApalacheProof) && apalacheTimeout.MatchString(output):
		return verify.Timeout
	case strings.Contains(lower, "memory limit") || strings.Contains(lower, "out of memory"):
		return verify.MemoryLimit
	case backend == PEx && depthLimitPattern.MatchString(output):
		return verify.DepthLimit
	case stateLimitPattern.MatchString(output):
		return verify.StateLimit
	case stepLimitPattern.MatchString(output):
		return verify.StepLimit
	case backend == PEx && scheduleLimit.MatchString(output):
		return verify.ScheduleLimit
	case backend == PEx && strings.Contains(lower, "toomanychoicesexception"):
		return verify.ToolLimit
	default:
		return ""
	}
}

func isCounterexample(lower string) bool {
	return strings.Contains(lower, "assertion failed") ||
		strings.Contains(lower, "invariant is violated") ||
		strings.Contains(lower, "invariant violation") ||
		strings.Contains(lower, "counterexample found") ||
		strings.Contains(lower, "checker found a bug") ||
		strings.Contains(lower, "found 1 bug") ||
		strings.Contains(lower, "property violated") ||
		strings.Contains(lower, "error: failed checks:") ||
		apalacheInvariantFailure.MatchString(lower) ||
		apalacheNumberedFailure.MatchString(lower) ||
		invariantViolation.MatchString(lower)
}

func nativeFailedProperty(request Request, output string) string {
	name := ""
	if request.Backend == Fizz {
		if matches := fizzFailurePattern.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
	if request.Backend == Apalache || request.Backend == ApalacheProof {
		if matches := apalacheInvariantFailure.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
	if request.Backend == P || request.Backend == PEx {
		if matches := pPropertyPattern.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
		if name == "" {
			if matches := pRelationPropertyPattern.FindStringSubmatch(output); len(matches) == 3 {
				suffix := strings.TrimPrefix(strings.ToLower(matches[2]), "exceeds ")
				if suffix == "has an absent endpoint" {
					suffix = "endpoints"
				}
				name = "relation " + matches[1] + " " + suffix
			}
		}
	}
	if request.Backend == Ivy {
		if matches := ivyFailurePattern.FindStringSubmatch(output); len(matches) == 2 {
			name = matches[1]
		}
	}
	if name == "" {
		matches := propertyPattern.FindStringSubmatch(output)
		if len(matches) == 2 {
			name = strings.Trim(matches[1], "]")
		}
	}
	return name
}

func failedPropertyCandidates(request Request, name string) []string {
	if name == "" {
		return nil
	}
	if properties := request.TraceVocabulary.Properties[name]; len(properties) != 0 {
		return slices.Clone(properties)
	}
	if source := request.PropertyNames[name]; source != "" {
		return []string{source}
	}
	for _, property := range request.Model.Properties {
		if property.Name == name {
			return []string{name}
		}
	}
	for _, relation := range request.Model.Relations {
		properties := []string{"relation " + relation.Name + " endpoints"}
		if relation.SourceCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" source cardinality")
		}
		if relation.TargetCardinality == verify.One {
			properties = append(properties, "relation "+relation.Name+" target cardinality")
		}
		for _, property := range properties {
			if property == name {
				return []string{name}
			}
		}
	}
	if request.Backend == Apalache || request.Backend == ApalacheProof {
		return slices.Clone(request.TraceVocabulary.Properties["Safety"])
	}
	return nil
}

func pTracePayload(output string) string {
	header := regexp.MustCompile(`(?m)^--- [^\r\n]+ ---\r?\n`)
	sections := header.FindAllStringIndex(output, -1)
	for index, section := range sections {
		end := len(output)
		if index+1 < len(sections) {
			end = sections[index+1][0]
		}
		payload := output[section[1]:end]
		if actionPattern.MatchString(payload) {
			return payload
		}
	}
	return output
}

func normalizeActions(request Request, output string) []verify.TraceStep {
	var result []verify.TraceStep
	for _, match := range actionPattern.FindAllStringSubmatch(output, -1) {
		bindings := verify.Bindings{}
		for _, binding := range bindingPattern.FindAllStringSubmatch(match[2], -1) {
			bindings[binding[1]] = binding[2]
		}
		result = append(result, verify.TraceStep{Action: match[1], Bindings: bindings})
	}
	if len(result) != 0 || request.Backend != TLC {
		if len(result) != 0 || request.Backend != Ivy {
			return result
		}
		return normalizeIvyActions(request, output)
	}
	for _, match := range tlaActionPattern.FindAllStringSubmatch(output, -1) {
		name := match[1]
		sourceName, known := request.ActionNames[name]
		if len(request.ActionNames) != 0 && !known {
			continue
		}
		if sourceName != "" {
			name = sourceName
		}
		bindings := verify.Bindings{}
		for _, binding := range tlaBindingPattern.FindAllStringSubmatch(match[2], -1) {
			bindings[binding[1]] = binding[2]
		}
		result = append(result, verify.TraceStep{Action: name, Bindings: bindings})
	}
	return result
}

func normalizeIvyActions(request Request, output string) []verify.TraceStep {
	var result []verify.TraceStep
	current := ""
	seen := map[string]struct{}{}
	for _, line := range strings.Split(output, "\n") {
		if match := ivyActionPattern.FindStringSubmatch(line); len(match) == 2 {
			current = match[1]
			continue
		}
		if current == "" || !ivyFailurePattern.MatchString(line) {
			continue
		}
		name := current
		if source := request.ActionNames[name]; source != "" {
			name = source
		}
		if _, duplicate := seen[name]; !duplicate {
			seen[name] = struct{}{}
			result = append(result, verify.TraceStep{Action: name})
		}
	}
	return result
}

func writeArtifacts(directory string, backend Backend, result verify.Result) ([]string, error) {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	prefix := string(backend)
	stdoutPath := filepath.Join(directory, prefix+".stdout.log")
	stderrPath := filepath.Join(directory, prefix+".stderr.log")
	resultPath := filepath.Join(directory, prefix+".result.json")
	paths := []string{stdoutPath, stderrPath, resultPath}
	result.Artifacts = append(result.Artifacts, paths...)
	if err := os.WriteFile(stdoutPath, []byte(result.StandardOutput), 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(stderrPath, []byte(result.StandardError), 0o600); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(resultPath, append(encoded, '\n'), 0o600); err != nil {
		return nil, err
	}
	return paths, nil
}

func nativeArtifacts(request Request) []string {
	var path string
	switch request.Backend {
	case TLC:
		path = filepath.Join(request.ArtifactDir, "tlc-native")
	case Apalache:
		path = filepath.Join(request.ArtifactDir, "apalache-native")
	case ApalacheProof:
		path = filepath.Join(request.ArtifactDir, "apalache-proof-native")
	case P:
		path = filepath.Join(request.ArtifactDir, "bugfinding-native")
	case PEx:
		path = filepath.Join(request.ArtifactDir, "pex-native")
	case Fizz:
		path = filepath.Join(request.ArtifactDir, "fizz-native")
	default:
		return nil
	}
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return []string{path}
}
