package umpire

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

// EvidenceSource identifies one environment-independent observation channel.
type EvidenceSource string

const (
	PublicAPIEvidence   EvidenceSource = "public-api"
	HistoryEvidence     EvidenceSource = "history"
	TelemetryEvidence   EvidenceSource = "telemetry"
	InProcessEvidence   EvidenceSource = "in-process"
	FormalModelEvidence EvidenceSource = "formal-model"
)

// OrderingGuarantee identifies an ordering relation that an environment can establish.
type OrderingGuarantee string

const (
	CausalOrdering         OrderingGuarantee = "causal-reference"
	SourceSequenceOrdering OrderingGuarantee = "source-sequence"
	SynchronizedOrdering   OrderingGuarantee = "synchronized-clock"
)

// EnvironmentKind identifies where a behavioral profile executes.
type EnvironmentKind string

const (
	LocalEnvironment      EnvironmentKind = "local"
	CIEnvironment         EnvironmentKind = "ci"
	DeploymentEnvironment EnvironmentKind = "deployment"
	CanaryEnvironment     EnvironmentKind = "canary"
)

// ClockDomain names observations whose source sequence is directly comparable.
type ClockDomain struct {
	Name         string           `json:"name"`
	Sources      []EvidenceSource `json:"sources,omitempty"`
	Synchronized bool             `json:"synchronized,omitempty"`
}

// RetentionPolicy records evidence restrictions without containing environment secrets.
type RetentionPolicy struct {
	MaxEvents      int  `json:"maxEvents,omitempty"`
	MaxBytes       int  `json:"maxBytes,omitempty"`
	RedactPayloads bool `json:"redactPayloads"`
	RedactSecrets  bool `json:"redactSecrets"`
}

// EnvironmentProfile is the portable capability and evidence contract for one environment.
type EnvironmentProfile struct {
	Name                string              `json:"name"`
	Kind                EnvironmentKind     `json:"kind,omitempty"`
	DriveCapabilities   []string            `json:"driveCapabilities,omitempty"`
	ObservationSources  []EvidenceSource    `json:"observationSources,omitempty"`
	ClockDomains        []ClockDomain       `json:"clockDomains,omitempty"`
	OrderingGuarantees  []OrderingGuarantee `json:"orderingGuarantees,omitempty"`
	IdentityLineage     bool                `json:"identityLineage,omitempty"`
	SupportedProperties []string            `json:"supportedProperties,omitempty"`
	Retention           RetentionPolicy     `json:"retention"`
}

// EvidenceRequirement declares what must be available to establish one property.
type EvidenceRequirement struct {
	Property               string           `json:"property"`
	Sources                []EvidenceSource `json:"sources,omitempty"`
	RequireCausalOrdering  bool             `json:"requireCausalOrdering,omitempty"`
	RequireIdentityLineage bool             `json:"requireIdentityLineage,omitempty"`
}

// ObservedEvidence describes the evidence actually retained for one evaluation.
type ObservedEvidence struct {
	Sources                    []EvidenceSource `json:"sources,omitempty"`
	LostSources                []EvidenceSource `json:"lostSources,omitempty"`
	CausalOrderingEstablished  bool             `json:"causalOrderingEstablished,omitempty"`
	IdentityLineageEstablished bool             `json:"identityLineageEstablished,omitempty"`
	AmbiguousIdentity          bool             `json:"ambiguousIdentity,omitempty"`
	ConflictingLineage         bool             `json:"conflictingLineage,omitempty"`
	IncomparableOrdering       bool             `json:"incomparableOrdering,omitempty"`
}

// ClaimStatus qualifies what the available evidence justifies.
type ClaimStatus string

const (
	ClaimEstablished  ClaimStatus = "established"
	ClaimViolated     ClaimStatus = "violated"
	ClaimUnsupported  ClaimStatus = "unsupported"
	ClaimInconclusive ClaimStatus = "inconclusive"
)

// QualifiedClaim records the evidence boundary of a live or generated verdict.
type QualifiedClaim struct {
	ModelVersion string           `json:"modelVersion,omitempty"`
	Target       string           `json:"target,omitempty"`
	Property     string           `json:"property"`
	Environment  string           `json:"environment"`
	Status       ClaimStatus      `json:"status"`
	Observed     []EvidenceSource `json:"observed,omitempty"`
	Omissions    []string         `json:"omissions,omitempty"`
	Diagnostic   string           `json:"diagnostic,omitempty"`
}

var ErrEnvironmentProfile = errors.New("invalid environment profile")
var ErrQualifiedClaim = errors.New("invalid qualified claim")

// ValidateEnvironmentProfile checks that a profile makes a coherent, secret-safe claim.
func ValidateEnvironmentProfile(profile EnvironmentProfile) error {
	if profile.Name == "" {
		return fmt.Errorf("%w: name is empty", ErrEnvironmentProfile)
	}
	switch profile.Kind {
	case "", LocalEnvironment, CIEnvironment, DeploymentEnvironment, CanaryEnvironment:
	default:
		return fmt.Errorf("%w: unknown environment kind %q", ErrEnvironmentProfile, profile.Kind)
	}
	if duplicates(profile.DriveCapabilities) || duplicates(profile.SupportedProperties) {
		return fmt.Errorf("%w: drive capabilities and supported properties must be unique", ErrEnvironmentProfile)
	}
	if duplicates(profile.ObservationSources) || duplicates(profile.OrderingGuarantees) {
		return fmt.Errorf("%w: evidence sources and ordering guarantees must be unique", ErrEnvironmentProfile)
	}
	for _, source := range profile.ObservationSources {
		if !validEvidenceSource(source) {
			return fmt.Errorf("%w: unknown evidence source %q", ErrEnvironmentProfile, source)
		}
	}
	for _, guarantee := range profile.OrderingGuarantees {
		if !validOrderingGuarantee(guarantee) {
			return fmt.Errorf("%w: unknown ordering guarantee %q", ErrEnvironmentProfile, guarantee)
		}
	}
	seenDomains := map[string]struct{}{}
	available := makeSet(profile.ObservationSources)
	for _, domain := range profile.ClockDomains {
		if domain.Name == "" || duplicates(domain.Sources) {
			return fmt.Errorf("%w: clock domains require a name and unique sources", ErrEnvironmentProfile)
		}
		if _, exists := seenDomains[domain.Name]; exists {
			return fmt.Errorf("%w: duplicate clock domain %q", ErrEnvironmentProfile, domain.Name)
		}
		seenDomains[domain.Name] = struct{}{}
		for _, source := range domain.Sources {
			if _, exists := available[source]; !exists {
				return fmt.Errorf("%w: clock domain %q references unavailable source %q", ErrEnvironmentProfile, domain.Name, source)
			}
		}
	}
	if profile.Retention.MaxEvents < 0 || profile.Retention.MaxBytes < 0 {
		return fmt.Errorf("%w: retention limits cannot be negative", ErrEnvironmentProfile)
	}
	return nil
}

// ValidateQualifiedClaim checks that a claim is coherent within its evidence profile.
func ValidateQualifiedClaim(profile EnvironmentProfile, claim QualifiedClaim) error {
	if err := ValidateEnvironmentProfile(profile); err != nil {
		return fmt.Errorf("%w: %v", ErrQualifiedClaim, err)
	}
	if claim.Property == "" {
		return fmt.Errorf("%w: property is empty", ErrQualifiedClaim)
	}
	if claim.Environment != profile.Name {
		return fmt.Errorf("%w: environment %q does not match profile %q", ErrQualifiedClaim, claim.Environment, profile.Name)
	}
	switch claim.Status {
	case ClaimEstablished, ClaimViolated, ClaimUnsupported, ClaimInconclusive:
	default:
		return fmt.Errorf("%w: unknown status %q", ErrQualifiedClaim, claim.Status)
	}
	if duplicates(claim.Observed) {
		return fmt.Errorf("%w: observed evidence sources must be unique", ErrQualifiedClaim)
	}
	available := makeSet(profile.ObservationSources)
	for _, source := range claim.Observed {
		if !validEvidenceSource(source) {
			return fmt.Errorf("%w: unknown observed evidence source %q", ErrQualifiedClaim, source)
		}
		if _, exists := available[source]; !exists {
			return fmt.Errorf("%w: observed evidence source %q is unavailable", ErrQualifiedClaim, source)
		}
	}
	if duplicates(claim.Omissions) {
		return fmt.Errorf("%w: omissions must be unique", ErrQualifiedClaim)
	}
	for _, omission := range claim.Omissions {
		if omission == "" {
			return fmt.Errorf("%w: omission is empty", ErrQualifiedClaim)
		}
	}
	if (claim.Status == ClaimEstablished || claim.Status == ClaimViolated) && len(claim.Omissions) > 0 {
		return fmt.Errorf("%w: status %q cannot have evidence omissions", ErrQualifiedClaim, claim.Status)
	}
	if (claim.Status == ClaimEstablished || claim.Status == ClaimViolated) && len(profile.SupportedProperties) > 0 && !slices.Contains(profile.SupportedProperties, claim.Property) {
		return fmt.Errorf("%w: property %q is unsupported by profile %q", ErrQualifiedClaim, claim.Property, profile.Name)
	}
	return nil
}

func validEvidenceSource(source EvidenceSource) bool {
	switch source {
	case PublicAPIEvidence, HistoryEvidence, TelemetryEvidence, InProcessEvidence, FormalModelEvidence:
		return true
	default:
		return false
	}
}

func validOrderingGuarantee(guarantee OrderingGuarantee) bool {
	switch guarantee {
	case CausalOrdering, SourceSequenceOrdering, SynchronizedOrdering:
		return true
	default:
		return false
	}
}

// ForEnvironment binds an evidence profile to a portable execution class.
func ForEnvironment(kind EnvironmentKind, evidence EnvironmentProfile) (EnvironmentProfile, error) {
	switch kind {
	case LocalEnvironment, CIEnvironment, DeploymentEnvironment, CanaryEnvironment:
	default:
		return EnvironmentProfile{}, fmt.Errorf("%w: unknown environment kind %q", ErrEnvironmentProfile, kind)
	}
	if err := ValidateEnvironmentProfile(evidence); err != nil {
		return EnvironmentProfile{}, err
	}
	evidence.Kind = kind
	evidence.Name = string(kind) + "/" + evidence.Name
	return evidence, nil
}

// QualifyEvidence produces the strongest claim justified by the declared and observed evidence.
func QualifyEvidence(
	modelVersion string,
	target string,
	profile EnvironmentProfile,
	requirement EvidenceRequirement,
	observed ObservedEvidence,
	violated bool,
) QualifiedClaim {
	claim := QualifiedClaim{
		ModelVersion: modelVersion,
		Target:       target,
		Property:     requirement.Property,
		Environment:  profile.Name,
		Observed:     sortedEvidenceSources(observed.Sources),
	}
	if err := ValidateEnvironmentProfile(profile); err != nil {
		claim.Status = ClaimUnsupported
		claim.Diagnostic = err.Error()
		return claim
	}
	if requirement.Property == "" {
		claim.Status = ClaimUnsupported
		claim.Diagnostic = "property is empty"
		return claim
	}
	if len(profile.SupportedProperties) > 0 && !slices.Contains(profile.SupportedProperties, requirement.Property) {
		claim.Status = ClaimUnsupported
		claim.Omissions = []string{"property:" + requirement.Property}
		claim.Diagnostic = "environment does not support the property"
		return claim
	}
	available := makeSet(profile.ObservationSources)
	for _, source := range requirement.Sources {
		if _, exists := available[source]; !exists {
			claim.Omissions = append(claim.Omissions, "source:"+string(source))
		}
	}
	if requirement.RequireCausalOrdering && !slices.Contains(profile.OrderingGuarantees, CausalOrdering) && !slices.Contains(profile.OrderingGuarantees, SourceSequenceOrdering) {
		claim.Omissions = append(claim.Omissions, "ordering:causal")
	}
	if requirement.RequireIdentityLineage && !profile.IdentityLineage {
		claim.Omissions = append(claim.Omissions, "identity:lineage")
	}
	if len(claim.Omissions) > 0 {
		slices.Sort(claim.Omissions)
		claim.Status = ClaimUnsupported
		claim.Diagnostic = "required evidence is unavailable"
		return claim
	}
	retained := makeSet(observed.Sources)
	for _, source := range requirement.Sources {
		if _, exists := retained[source]; !exists {
			claim.Omissions = append(claim.Omissions, "observation:"+string(source))
		}
	}
	for _, source := range observed.LostSources {
		claim.Omissions = append(claim.Omissions, "lost:"+string(source))
	}
	if observed.AmbiguousIdentity {
		claim.Omissions = append(claim.Omissions, "identity:ambiguous")
	}
	if observed.ConflictingLineage {
		claim.Omissions = append(claim.Omissions, "lineage:conflicting")
	}
	if requirement.RequireCausalOrdering && !observed.CausalOrderingEstablished {
		claim.Omissions = append(claim.Omissions, "ordering:unestablished")
	}
	if requirement.RequireIdentityLineage && !observed.IdentityLineageEstablished {
		claim.Omissions = append(claim.Omissions, "lineage:unestablished")
	}
	if requirement.RequireCausalOrdering && observed.IncomparableOrdering {
		claim.Omissions = append(claim.Omissions, "ordering:incomparable")
	}
	if len(claim.Omissions) > 0 {
		slices.Sort(claim.Omissions)
		claim.Omissions = slices.Compact(claim.Omissions)
		claim.Status = ClaimInconclusive
		claim.Diagnostic = "expected evidence is incomplete or ambiguous"
		return claim
	}
	if violated {
		claim.Status = ClaimViolated
	} else {
		claim.Status = ClaimEstablished
	}
	return claim
}

// PublicAPIProfile returns the minimum black-box profile.
func PublicAPIProfile() EnvironmentProfile {
	return EnvironmentProfile{
		Name:               "public-api",
		DriveCapabilities:  []string{"public-api"},
		ObservationSources: []EvidenceSource{PublicAPIEvidence},
		ClockDomains:       []ClockDomain{{Name: "client", Sources: []EvidenceSource{PublicAPIEvidence}}},
		OrderingGuarantees: []OrderingGuarantee{CausalOrdering, SourceSequenceOrdering},
		Retention:          secureRetention(),
	}
}

// HistoryProfile adds server history evidence to the public interface.
func HistoryProfile() EnvironmentProfile {
	profile := PublicAPIProfile()
	profile.Name = "history"
	profile.ObservationSources = append(profile.ObservationSources, HistoryEvidence)
	profile.ClockDomains = append(profile.ClockDomains, ClockDomain{Name: "history", Sources: []EvidenceSource{HistoryEvidence}})
	profile.IdentityLineage = true
	return profile
}

// TelemetryProfile adds telemetry without claiming a shared clock with API observations.
func TelemetryProfile() EnvironmentProfile {
	profile := PublicAPIProfile()
	profile.Name = "telemetry"
	profile.ObservationSources = append(profile.ObservationSources, TelemetryEvidence)
	profile.ClockDomains = append(profile.ClockDomains, ClockDomain{Name: "telemetry", Sources: []EvidenceSource{TelemetryEvidence}})
	return profile
}

// InProcessProfile returns the richest white-box observation profile.
func InProcessProfile() EnvironmentProfile {
	profile := HistoryProfile()
	profile.Name = "in-process"
	profile.DriveCapabilities = []string{"direct", "faults", "public-api"}
	profile.ObservationSources = append(profile.ObservationSources, TelemetryEvidence, InProcessEvidence)
	profile.ClockDomains = append(profile.ClockDomains,
		ClockDomain{Name: "telemetry", Sources: []EvidenceSource{TelemetryEvidence}},
		ClockDomain{Name: "process", Sources: []EvidenceSource{InProcessEvidence}},
	)
	return profile
}

func secureRetention() RetentionPolicy {
	return RetentionPolicy{MaxEvents: 10_000, MaxBytes: 8 << 20, RedactPayloads: true, RedactSecrets: true}
}

func sortedEvidenceSources(sources []EvidenceSource) []EvidenceSource {
	result := slices.Clone(sources)
	slices.SortFunc(result, func(left, right EvidenceSource) int { return cmp.Compare(string(left), string(right)) })
	return slices.Compact(result)
}

func makeSet[T comparable](values []T) map[T]struct{} {
	result := make(map[T]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func duplicates[T comparable](values []T) bool {
	return len(makeSet(values)) != len(values)
}
