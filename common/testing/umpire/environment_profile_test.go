package umpire

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvidenceProfilesQualifyClaimsByAvailableEvidence(t *testing.T) {
	requirement := EvidenceRequirement{Property: "workflow-lineage", Sources: []EvidenceSource{PublicAPIEvidence, HistoryEvidence}, RequireCausalOrdering: true, RequireIdentityLineage: true}

	unsupported := QualifyEvidence("model/v1", "workflow", PublicAPIProfile(), requirement, ObservedEvidence{Sources: []EvidenceSource{PublicAPIEvidence}}, false)
	require.Equal(t, ClaimUnsupported, unsupported.Status)
	require.Equal(t, []string{"identity:lineage", "source:history"}, unsupported.Omissions)

	inconclusive := QualifyEvidence("model/v1", "workflow", HistoryProfile(), requirement, ObservedEvidence{
		Sources:                    []EvidenceSource{PublicAPIEvidence, HistoryEvidence},
		CausalOrderingEstablished:  true,
		IdentityLineageEstablished: true,
		AmbiguousIdentity:          true,
		ConflictingLineage:         true,
		IncomparableOrdering:       true,
	}, false)
	require.Equal(t, ClaimInconclusive, inconclusive.Status)
	require.Equal(t, []string{"identity:ambiguous", "lineage:conflicting", "ordering:incomparable"}, inconclusive.Omissions)

	established := QualifyEvidence("model/v1", "workflow", HistoryProfile(), requirement, ObservedEvidence{
		Sources:                    []EvidenceSource{HistoryEvidence, PublicAPIEvidence},
		CausalOrderingEstablished:  true,
		IdentityLineageEstablished: true,
	}, false)
	require.Equal(t, ClaimEstablished, established.Status)
	require.Equal(t, []EvidenceSource{HistoryEvidence, PublicAPIEvidence}, established.Observed)
}

func TestEvidenceProfilesRequirePositiveOrderingAndLineageEvidence(t *testing.T) {
	requirement := EvidenceRequirement{
		Property:               "workflow-lineage",
		Sources:                []EvidenceSource{HistoryEvidence},
		RequireCausalOrdering:  true,
		RequireIdentityLineage: true,
	}
	claim := QualifyEvidence("model/v1", "workflow", HistoryProfile(), requirement, ObservedEvidence{Sources: []EvidenceSource{HistoryEvidence}}, false)

	require.Equal(t, ClaimInconclusive, claim.Status)
	require.Equal(t, []string{"lineage:unestablished", "ordering:unestablished"}, claim.Omissions)
}

func TestEvidenceProfilesTreatObservationLossAsInconclusive(t *testing.T) {
	claim := QualifyEvidence("model/v1", "workflow", TelemetryProfile(), EvidenceRequirement{
		Property: "workflow-completes",
		Sources:  []EvidenceSource{TelemetryEvidence},
	}, ObservedEvidence{LostSources: []EvidenceSource{TelemetryEvidence}}, false)

	require.Equal(t, ClaimInconclusive, claim.Status)
	require.Equal(t, []string{"lost:telemetry", "observation:telemetry"}, claim.Omissions)
}

func TestStandardEvidenceProfilesAreValidAndSecretSafe(t *testing.T) {
	for _, profile := range []EnvironmentProfile{PublicAPIProfile(), HistoryProfile(), TelemetryProfile(), InProcessProfile()} {
		require.NoError(t, ValidateEnvironmentProfile(profile))
		require.True(t, profile.Retention.RedactPayloads)
		require.True(t, profile.Retention.RedactSecrets)
	}
}

func TestEvidenceProfilesRejectUnknownEvidenceVocabulary(t *testing.T) {
	profile := PublicAPIProfile()
	profile.ObservationSources = append(profile.ObservationSources, EvidenceSource("unknown"))
	require.ErrorContains(t, ValidateEnvironmentProfile(profile), "unknown evidence source")

	profile = PublicAPIProfile()
	profile.OrderingGuarantees = append(profile.OrderingGuarantees, OrderingGuarantee("unknown"))
	require.ErrorContains(t, ValidateEnvironmentProfile(profile), "unknown ordering guarantee")
}

func TestEvidenceProfilesBindToPortableEnvironmentKinds(t *testing.T) {
	for _, kind := range []EnvironmentKind{LocalEnvironment, CIEnvironment, DeploymentEnvironment, CanaryEnvironment} {
		profile, err := ForEnvironment(kind, HistoryProfile())
		require.NoError(t, err)
		require.Equal(t, kind, profile.Kind)
		require.Equal(t, string(kind)+"/history", profile.Name)
	}
}

func TestValidateQualifiedClaimRejectsEvidenceContradictions(t *testing.T) {
	profile := PublicAPIProfile()
	valid := QualifiedClaim{
		ModelVersion: "model/v1", Property: "safety", Environment: profile.Name,
		Status: ClaimViolated, Observed: []EvidenceSource{PublicAPIEvidence},
	}
	require.NoError(t, ValidateQualifiedClaim(profile, valid))

	tests := []struct {
		name   string
		mutate func(*QualifiedClaim)
		err    string
	}{
		{name: "mismatched environment", mutate: func(claim *QualifiedClaim) { claim.Environment = "history" }, err: "does not match"},
		{name: "unavailable source", mutate: func(claim *QualifiedClaim) { claim.Observed = []EvidenceSource{HistoryEvidence} }, err: "unavailable"},
		{name: "duplicate source", mutate: func(claim *QualifiedClaim) { claim.Observed = append(claim.Observed, PublicAPIEvidence) }, err: "must be unique"},
		{name: "conclusive omissions", mutate: func(claim *QualifiedClaim) { claim.Omissions = []string{"lost:public-api"} }, err: "cannot have evidence omissions"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim := valid
			claim.Observed = slices.Clone(valid.Observed)
			test.mutate(&claim)
			require.ErrorContains(t, ValidateQualifiedClaim(profile, claim), test.err)
		})
	}
}
