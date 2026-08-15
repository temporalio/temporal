package verify

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

func TestValidateResultRejectsSuccessfulUnsupportedEvidenceClaim(t *testing.T) {
	err := ValidateResult(Result{
		Backend:      "go",
		ModelVersion: "model/v1",
		Target:       "workflow",
		Status:       BoundedNoCounterexample,
		Termination:  Completed,
		Environment:  umpire.PublicAPIProfile(),
		Observations: []umpire.EvidenceSource{umpire.PublicAPIEvidence},
		Claims: []umpire.QualifiedClaim{{
			ModelVersion: "model/v1",
			Target:       "workflow",
			Property:     "history-lineage",
			Environment:  "public-api",
			Status:       umpire.ClaimUnsupported,
			Observed:     []umpire.EvidenceSource{umpire.PublicAPIEvidence},
		}},
	})

	require.ErrorContains(t, err, "cannot claim success")
}

func TestValidateResultRejectsUnqualifiedFormalSuccess(t *testing.T) {
	err := ValidateResult(Result{
		Backend:      "fizz",
		ModelVersion: "model/v1",
		Status:       BoundedNoCounterexample,
		Termination:  Completed,
	})

	require.ErrorContains(t, err, "no evidence profile or observations")
}

func TestValidateResultRejectsUnversionedFormalSuccess(t *testing.T) {
	err := ValidateResult(Result{
		Backend:     "fizz",
		Status:      BoundedNoCounterexample,
		Termination: Completed,
	})

	require.ErrorContains(t, err, "no model version")
}
