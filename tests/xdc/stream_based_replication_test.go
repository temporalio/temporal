package xdc

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestStreamBasedReplicationTestSuite(t *testing.T) {
	suite.Run(t, new(streamBasedReplicationTestSuite))
}
