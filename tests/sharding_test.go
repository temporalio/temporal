package tests

import (
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/testx/parallelsuite"
)

func init() {
	parallelsuite.ConfigureSharding(testcore.FunctionalTestShard)
}
