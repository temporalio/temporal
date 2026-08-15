package umpire2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProtocolFacadeHidesImplementationCatalogsAndStores(t *testing.T) {
	typeOfProtocol := reflect.TypeOf((*Protocol)(nil))
	for _, method := range []string{
		"ActionCatalog",
		"CausalFootprints",
		"NewRelationStore",
		"RelationSchemas",
		"RuntimeDeclaration",
		"VerificationFamily",
	} {
		_, exposed := typeOfProtocol.MethodByName(method)
		require.False(t, exposed, method)
	}
}
