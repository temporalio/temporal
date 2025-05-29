package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/tools/cassandra"
)

func TestCQLClientTestSuite(t *testing.T) {
	suite.Run(t, new(cassandra.CQLClientTestSuite))
}

func TestSetupCQLSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(cassandra.SetupSchemaTestSuite))
}

func TestUpdateCQLSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(cassandra.UpdateSchemaTestSuite))
}

func TestVersionTestSuite(t *testing.T) {
	suite.Run(t, new(cassandra.VersionTestSuite))
}
