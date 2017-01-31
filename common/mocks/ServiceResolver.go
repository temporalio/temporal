package mocks

import mock "github.com/stretchr/testify/mock"
import membership "code.uber.internal/devexp/minions/common/membership"

type ServiceResolver struct {
	mock.Mock
}

func (_m *ServiceResolver) Lookup(key string) (*membership.HostInfo, error) {
	ret := _m.Called(key)

	var r0 *membership.HostInfo
	if rf, ok := ret.Get(0).(func(string) *membership.HostInfo); ok {
		r0 = rf(key)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*membership.HostInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

func (_m *ServiceResolver) AddListener(name string, notifyChannel chan<- *membership.ChangedEvent) error {
	ret := _m.Called(name, notifyChannel)

	var r0 error
	if rf, ok := ret.Get(0).(func(string, chan<- *membership.ChangedEvent) error); ok {
		r0 = rf(name, notifyChannel)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

func (_m *ServiceResolver) RemoveListener(name string) error {
	ret := _m.Called(name)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(name)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

var _ membership.ServiceResolver = (*ServiceResolver)(nil)
