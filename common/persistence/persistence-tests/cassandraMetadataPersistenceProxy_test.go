// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistencetests

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	p "github.com/uber/cadence/common/persistence"
)

type (
	// metadataPersistenceProxySuite contains metadata proxy persistence tests
	metadataPersistenceProxySuite struct {
		TestBase
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
	}
)

func TestMetadataPersistenceProxySuite(t *testing.T) {
	s := new(metadataPersistenceProxySuite)
	suite.Run(t, s)
}

// SetupSuite implementation
func (s *metadataPersistenceProxySuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
}

// TearDownSuite implementation
func (s *metadataPersistenceProxySuite) TearDownSuite() {

}

// SetupTest implementation
func (s *metadataPersistenceProxySuite) SetupTest() {

}

// TearDownTest implementation
func (s *metadataPersistenceProxySuite) TearDownTest() {
	s.TestBase.TearDownWorkflowStore()
}

// TestDomainV1V2Migration test
func (s *metadataPersistenceProxySuite) TestDomainV1V2Migration() {
	id := uuid.New()
	name := "v1-v2-domain-migration-test-name"
	status := p.DomainStatusRegistered
	description := "get-domain-test-description"
	owner := "get-domain-test-owner"
	data := map[string]string{"k1": "v1"}
	retention := int32(10)
	emitMetric := true
	historyArchivalStatus := gen.ArchivalStatusEnabled
	historyArchivalURI := "test://history/uri"
	visibilityArchivalStatus := gen.ArchivalStatusEnabled
	visibilityArchivalURI := "test://visibility/uri"
	badBinaries := gen.BadBinaries{Binaries: map[string]*gen.BadBinaryInfo{
		uuid.New(): &gen.BadBinaryInfo{
			Reason:          common.StringPtr("some random reason,"),
			Operator:        common.StringPtr("some random operator,"),
			CreatedTimeNano: common.Int64Ptr(time.Now().UnixNano()),
		},
	}}

	clusterActive := "some random active cluster name"
	clusters := []*p.ClusterReplicationConfig{{ClusterName: clusterActive}}

	resp, err := s.MetadataProxy.GetDomain(&p.GetDomainRequest{
		Name: name,
	})
	s.Nil(resp)
	s.NotNil(err)
	s.IsType(&gen.EntityNotExistsError{}, err)
	resp, err = s.MetadataProxy.GetDomain(&p.GetDomainRequest{
		ID: id,
	})
	s.Nil(resp)
	s.NotNil(err)
	s.IsType(&gen.EntityNotExistsError{}, err)

	// create the domain using v1 metadata manager
	createResp, err1 := s.MetadataManager.CreateDomain(&p.CreateDomainRequest{
		Info: &p.DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
			Data:        data,
		},
		Config: &p.DomainConfig{
			Retention:                retention,
			EmitMetric:               emitMetric,
			HistoryArchivalStatus:    historyArchivalStatus,
			HistoryArchivalURI:       historyArchivalURI,
			VisibilityArchivalStatus: visibilityArchivalStatus,
			VisibilityArchivalURI:    visibilityArchivalURI,
			BadBinaries:              badBinaries,
		},
		ReplicationConfig: &p.DomainReplicationConfig{
			ActiveClusterName: clusterActive,
			Clusters:          clusters,
		},
		IsGlobalDomain:  false,
		ConfigVersion:   0,
		FailoverVersion: common.EmptyVersion,
	})
	s.Nil(err1)
	s.NotNil(createResp)
	s.Equal(id, createResp.ID)

	// use proxy to get the domain
	resp, err = s.MetadataProxy.GetDomain(&p.GetDomainRequest{
		Name: name,
	})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(id, resp.Info.ID)
	s.Equal(name, resp.Info.Name)
	s.Equal(status, resp.Info.Status)
	s.Equal(description, resp.Info.Description)
	s.Equal(owner, resp.Info.OwnerEmail)
	s.Equal(data, resp.Info.Data)
	s.Equal(retention, resp.Config.Retention)
	s.Equal(emitMetric, resp.Config.EmitMetric)
	s.Equal(historyArchivalStatus, resp.Config.HistoryArchivalStatus)
	s.Equal(historyArchivalURI, resp.Config.HistoryArchivalURI)
	s.Equal(visibilityArchivalStatus, resp.Config.VisibilityArchivalStatus)
	s.Equal(visibilityArchivalURI, resp.Config.VisibilityArchivalURI)
	s.Equal(clusterActive, resp.ReplicationConfig.ActiveClusterName)
	s.Equal(len(clusters), len(resp.ReplicationConfig.Clusters))
	for index := range clusters {
		s.Equal(clusters[index], resp.ReplicationConfig.Clusters[index])
	}
	s.Equal(false, resp.IsGlobalDomain)
	s.Equal(int64(0), resp.ConfigVersion)
	s.Equal(common.EmptyVersion, resp.FailoverVersion)
	s.Equal(int64(0), resp.NotificationVersion)

	// for testing below
	resp.TableVersion = p.DomainTableVersionV2

	// use proxy to get the domain
	respV2, err := s.MetadataManagerV2.GetDomain(&p.GetDomainRequest{
		Name: name,
	})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(resp, respV2)

	respV2, err = s.MetadataManagerV2.GetDomain(&p.GetDomainRequest{
		ID: id,
	})
	s.Nil(err)
	s.NotNil(resp)
	s.Equal(resp, respV2)
}
