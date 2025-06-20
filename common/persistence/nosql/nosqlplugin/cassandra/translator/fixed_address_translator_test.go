package translator

import (
	"net"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.uber.org/mock/gomock"
)

type (
	fixedTranslatorPluginTestSuite struct {
		suite.Suite
		controller *gomock.Controller
	}
)

func TestSessionTestSuite(t *testing.T) {
	s := new(fixedTranslatorPluginTestSuite)
	suite.Run(t, s)
}

func (s *fixedTranslatorPluginTestSuite) SetupSuite() {

}

func (s *fixedTranslatorPluginTestSuite) TearDownSuite() {

}

func (s *fixedTranslatorPluginTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
}

func (s *fixedTranslatorPluginTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *fixedTranslatorPluginTestSuite) TestFixedAddressTranslator() {
	plugin := &FixedAddressTranslatorPlugin{}

	configuration := &config.Cassandra{
		AddressTranslator: &config.CassandraAddressTranslator{
			Translator: fixedTranslatorName,
			Options:    map[string]string{advertisedHostnameKey: "temporal.io"},
		},
	}

	lookupIP, err := net.LookupIP("temporal.io")
	if err != nil {
		s.Errorf(err, "fail to lookup IP for temporal.io")
	}

	ipToExpect := lookupIP[0].To4()

	translator, err := plugin.GetTranslator(configuration)
	translatedHost, translatedPort := translator.Translate(net.ParseIP("1.1.1.1"), 6001)

	s.Equal(ipToExpect, translatedHost)
	s.Equal(6001, translatedPort)

	s.Equal(nil, err)
}
