// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexus_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
	"google.golang.org/grpc/codes"
)

// This file contains unit tests for the Nexus outgoing service registry. Most of the tests here are for errors and edge
// cases. There's also functional tests.

const (
	testNamespace   = "test-namespace"
	testServiceName = "test-service"
	testServiceURL  = "http://localhost/"
)

func TestGet_NoNamespace(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, nil)
	_, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Name:      testServiceName,
			Namespace: "",
		},
	)
	assert.ErrorContains(t, err, nexus.IssueNamespaceNotSet)
}

func TestGet_NoName(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, nil)
	_, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Name:      "",
			Namespace: testNamespace,
		},
	)
	assert.ErrorContains(t, err, nexus.IssueNameNotSet)
}

func TestGet_GetNamespaceErr(t *testing.T) {
	t.Parallel()
	getNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return nil, getNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	assert.ErrorIs(t, err, getNamespaceErr)
}

func TestGet_ServiceNotFound(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{}, nil
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.Error(t, err)
	code := serviceerror.ToStatus(err).Code()
	assert.Equal(t, codes.NotFound, code, err)
	assert.ErrorContains(t, err, testServiceName)
}

func TestGet_Ok(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    testServiceName,
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
				},
			},
		}, nil
	}

	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	res, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, &nexuspb.OutgoingService{
		Version: 1,
		Name:    testServiceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url: testServiceURL,
		},
	}, res.Service)
}

func TestUpdate_NoNamespace(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Name: testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.ErrorContains(t, err, nexus.IssueNamespaceNotSet)
}

func TestUpdate_NoServiceName(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.ErrorContains(t, err, nexus.IssueNameNotSet)
}

func TestUpdate_NotFound(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{},
		}, nil
	}

	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName + "x",
			Version:   1,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL + "x",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code(), err)
}

func TestUpdate_VersionMismatch(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    testServiceName,
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
				},
			},
		}, nil
	}

	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Version:   2,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL + "x",
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, serviceerror.ToStatus(err).Code(), err)
}

func TestUpdate_Ok(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	ns := &persistencespb.NamespaceDetail{
		OutgoingServices: []*persistencespb.NexusOutgoingService{
			{
				Version: 1,
				Name:    testServiceName,
				Spec: &nexuspb.OutgoingServiceSpec{
					Url: testServiceURL,
				},
			},
		},
	}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: common.CloneProto(ns),
		}, nil
	}
	service.OnUpdateNamespace = func(ctx context.Context, request *persistence.UpdateNamespaceRequest) error {
		ns = request.Namespace
		return nil
	}

	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Version:   1,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL + "x",
			},
		},
	)
	require.NoError(t, err)
	res, err := registry.Get(
		context.Background(),
		&operatorservice.GetNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, &nexuspb.OutgoingService{
		Version: 2,
		Name:    testServiceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url: testServiceURL + "x",
		},
	}, res.Service)
}

func TestCreate_NameTooLong(t *testing.T) {
	t.Parallel()
	config := newConfig()
	name := strings.Repeat("x", config.NameMaxLength()+1)
	registry := nexus.NewOutgoingServiceRegistry(nil, config)
	_, err := registry.Update(
		context.Background(),
		&operatorservice.UpdateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      name,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, strconv.Itoa(config.NameMaxLength()))
}

func TestCreate_NameInvalidFormat(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      "!@&#%^$",
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, "a-z")
}

func TestCreate_NoURL(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.ErrorContains(t, err, nexus.IssueURLNotSet)
}

func TestCreate_URLTooLong(t *testing.T) {
	t.Parallel()
	config := newConfig()
	registry := nexus.NewOutgoingServiceRegistry(nil, config)
	u := testServiceURL + "/"
	u += strings.Repeat("x", config.MaxURLLength()-len(u)+1)
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: u,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, strconv.Itoa(config.MaxURLLength()))
}

func TestCreate_URLMalformed(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	u := "://example.com"
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: u,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.Contains(t, strings.ToLower(err.Error()), "malformed")
	assert.ErrorContains(t, err, u)
}

func TestCreate_InvalidScheme(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	u := "oops://example.com"
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: u,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, "scheme")
	assert.ErrorContains(t, err, "oops")
	assert.ErrorContains(t, err, "http")
	assert.ErrorContains(t, err, "https")
}

func TestCreate_GetNamespaceErr(t *testing.T) {
	t.Parallel()
	getNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return nil, getNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.ErrorIs(t, err, getNamespaceErr)
}

func TestCreate_UpdateNamespaceErr(t *testing.T) {
	t.Parallel()
	updateNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace:           &persistencespb.NamespaceDetail{},
			IsGlobalNamespace:   true,
			NotificationVersion: 1,
		}, nil
	}
	service.OnUpdateNamespace = func(ctx context.Context, request *persistence.UpdateNamespaceRequest) error {
		assert.True(t, request.IsGlobalNamespace)
		assert.Equal(t, 2, int(request.NotificationVersion))
		protoassert.ProtoEqual(t, &persistencespb.NamespaceDetail{
			OutgoingServices: []*persistencespb.NexusOutgoingService{
				{
					Version: 1,
					Name:    testServiceName,
					Spec: &nexuspb.OutgoingServiceSpec{
						Url: testServiceURL,
					},
				},
			},
		}, request.Namespace)
		return updateNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.ErrorIs(t, err, updateNamespaceErr)
}

func TestCreate_NameAlreadyTaken(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    testServiceName,
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
				},
			},
			IsGlobalNamespace:   true,
			NotificationVersion: 1,
		}, nil
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.AlreadyExists, serviceerror.ToStatus(err).Code(), err)
}

func TestCreate_Ok(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	ns := &persistencespb.NamespaceDetail{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: common.CloneProto(ns),
		}, nil
	}
	service.OnUpdateNamespace = func(ctx context.Context, request *persistence.UpdateNamespaceRequest) error {
		ns = request.Namespace
		return nil
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	res, err := registry.Create(
		context.Background(),
		&operatorservice.CreateNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
			Spec: &nexuspb.OutgoingServiceSpec{
				Url: testServiceURL,
			},
		},
	)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, &nexuspb.OutgoingService{
		Version: 1,
		Name:    testServiceName,
		Spec: &nexuspb.OutgoingServiceSpec{
			Url: testServiceURL,
		},
	}, res.Service)
}

func TestDelete_NoNamespace(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Delete(
		context.Background(),
		&operatorservice.DeleteNexusOutgoingServiceRequest{
			Namespace: "",
			Name:      testServiceName,
		},
	)
	require.ErrorContains(t, err, nexus.IssueNamespaceNotSet)
}

func TestDelete_NoName(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.Delete(
		context.Background(),
		&operatorservice.DeleteNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      "",
		},
	)
	require.ErrorContains(t, err, nexus.IssueNameNotSet)
}

func TestDelete_GetNamespaceErr(t *testing.T) {
	t.Parallel()
	getNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return nil, getNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Delete(
		context.Background(),
		&operatorservice.DeleteNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.ErrorIs(t, err, getNamespaceErr)
}

func TestDelete_ServiceNotFound(t *testing.T) {
	t.Parallel()
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    "other-service",
					},
				},
			},
		}, nil
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.Delete(
		context.Background(),
		&operatorservice.DeleteNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code(), err)
}

func TestDelete_UpdateNamespaceErr(t *testing.T) {
	t.Parallel()
	updateNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    testServiceName,
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
					{
						Version: 1,
						Name:    "other-service",
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
				},
			},
			IsGlobalNamespace:   true,
			NotificationVersion: 1,
		}, nil
	}
	service.OnUpdateNamespace = func(ctx context.Context, request *persistence.UpdateNamespaceRequest) error {
		assert.Equal(t, &persistence.UpdateNamespaceRequest{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    "other-service",
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: testServiceURL,
						},
					},
				},
			},
			IsGlobalNamespace:   true,
			NotificationVersion: 2,
		}, request)
		return updateNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, nil)
	_, err := registry.Delete(
		context.Background(),
		&operatorservice.DeleteNexusOutgoingServiceRequest{
			Namespace: testNamespace,
			Name:      testServiceName,
		},
	)
	require.ErrorIs(t, err, updateNamespaceErr)
}

func TestList_NoNamespace(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: "",
		},
	)
	require.ErrorContains(t, err, nexus.IssueNamespaceNotSet)
}

func TestList_NegativePageSize(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
			PageSize:  -1,
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, "PageSize")
	assert.ErrorContains(t, err, "negative")
}

func TestList_PageSizeTooLarge(t *testing.T) {
	t.Parallel()
	config := nexus.NewOutgoingServiceRegistryConfig(dynamicconfig.NewNoopCollection())
	registry := nexus.NewOutgoingServiceRegistry(nil, config)
	_, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
			PageSize:  int32(config.MaxPageSize() + 1),
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, "PageSize")
	assert.ErrorContains(t, err, strconv.Itoa(config.MaxPageSize()))
}

func TestList_InvalidPageToken(t *testing.T) {
	t.Parallel()
	registry := nexus.NewOutgoingServiceRegistry(nil, newConfig())
	_, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
			PageToken: []byte("invalid-token"),
		},
	)
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code(), err)
	assert.ErrorContains(t, err, "NextPageToken")
}

func TestList_GetNamespaceErr(t *testing.T) {
	t.Parallel()
	getNamespaceErr := errors.New("test error")
	service := nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return nil, getNamespaceErr
	}
	registry := nexus.NewOutgoingServiceRegistry(&service, newConfig())
	_, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
		},
	)
	require.ErrorIs(t, err, getNamespaceErr)
}

func TestList_NoNextPageToken(t *testing.T) {
	t.Parallel()
	service := &nexustest.NamespaceService{}
	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{
				OutgoingServices: []*persistencespb.NexusOutgoingService{
					{
						Version: 1,
						Name:    "service1",
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: "url1",
						},
					},
					{
						Version: 1,
						Name:    "service2",
						Spec: &nexuspb.OutgoingServiceSpec{
							Url: "url2",
						},
					},
				},
			},
		}, nil
	}
	config := newConfig()
	registry := nexus.NewOutgoingServiceRegistry(service, config)
	res, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
			PageSize:  1,
		},
	)
	require.NoError(t, err)
	assert.Len(t, res.Services, 1)
}

func TestList_NoServices(t *testing.T) {
	t.Parallel()
	service := &nexustest.NamespaceService{}

	service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
		return &persistence.GetNamespaceResponse{
			Namespace: &persistencespb.NamespaceDetail{},
		}, nil
	}
	config := newConfig()
	registry := nexus.NewOutgoingServiceRegistry(service, config)
	res, err := registry.List(
		context.Background(),
		&operatorservice.ListNexusOutgoingServicesRequest{
			Namespace: testNamespace,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, res.Services)
}

func TestList_Paginate(t *testing.T) {
	t.Parallel()
	service := &nexustest.NamespaceService{}
	{
		services := make([]*persistencespb.NexusOutgoingService, 10)
		for i := range services {
			services[i] = &persistencespb.NexusOutgoingService{
				Version: 1,
				Name:    fmt.Sprintf("service/%3d", i+1),
				Spec: &nexuspb.OutgoingServiceSpec{
					Url: "url" + strconv.Itoa(i+1),
				},
			}
		}
		service.OnGetNamespace = func(ctx context.Context, request *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
			return &persistence.GetNamespaceResponse{
				Namespace: &persistencespb.NamespaceDetail{
					OutgoingServices: services,
				},
			}, nil
		}
	}
	config := newConfig()
	registry := nexus.NewOutgoingServiceRegistry(service, config)
	var pageToken []byte
	for i := 0; i < 4; i++ {
		res, err := registry.List(
			context.Background(),
			&operatorservice.ListNexusOutgoingServicesRequest{
				Namespace: testNamespace,
				PageSize:  3,
				PageToken: pageToken,
			},
		)
		require.NoError(t, err)
		if i < 3 {
			assert.Len(t, res.Services, 3)
			require.NotEmpty(t, res.NextPageToken)
			pageToken = res.NextPageToken
		} else {
			assert.Len(t, res.Services, 1)
			assert.Empty(t, res.NextPageToken)
		}
	}
}

func newConfig() *nexus.OutgoingServiceRegistryConfig {
	return nexus.NewOutgoingServiceRegistryConfig(dynamicconfig.NewNoopCollection())
}
