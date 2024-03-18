package xdc

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestAdvVisCrossDCTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(AdvVisCrossDCTestSuite))
}
