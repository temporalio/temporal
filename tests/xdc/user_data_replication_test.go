package xdc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestUserDataReplicationTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(UserDataReplicationTestSuite))
}
