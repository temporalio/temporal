package child

import (
	"testing"

	legacy "github.com/stretchr/testify/suite"
)

func TestOrdinary(t *testing.T) { t.Log("ordinary child") }

type legacySuite struct{ legacy.Suite }

func TestLegacy(t *testing.T) { legacy.Run(t, &legacySuite{}) }
