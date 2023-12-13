// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package visibility

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination manager_selector_mock.go

import (
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

type (
	managerSelector interface {
		readManager(nsName namespace.Name) manager.VisibilityManager
		writeManagers() ([]manager.VisibilityManager, error)
	}

	defaultManagerSelector struct {
		visibilityManager                 manager.VisibilityManager
		secondaryVisibilityManager        manager.VisibilityManager
		enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter
		secondaryVisibilityWritingMode    dynamicconfig.StringPropertyFn
	}
)

var _ managerSelector = (*defaultManagerSelector)(nil)

func newDefaultManagerSelector(
	visibilityManager manager.VisibilityManager,
	secondaryVisibilityManager manager.VisibilityManager,
	enableSecondaryVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	secondaryVisibilityWritingMode dynamicconfig.StringPropertyFn,
) *defaultManagerSelector {
	return &defaultManagerSelector{
		visibilityManager:                 visibilityManager,
		secondaryVisibilityManager:        secondaryVisibilityManager,
		enableReadFromSecondaryVisibility: enableSecondaryVisibilityRead,
		secondaryVisibilityWritingMode:    secondaryVisibilityWritingMode,
	}
}

func (v *defaultManagerSelector) writeManagers() ([]manager.VisibilityManager, error) {
	switch v.secondaryVisibilityWritingMode() {
	case SecondaryVisibilityWritingModeOff:
		return []manager.VisibilityManager{v.visibilityManager}, nil
	case SecondaryVisibilityWritingModeOn:
		return []manager.VisibilityManager{v.secondaryVisibilityManager}, nil
	case SecondaryVisibilityWritingModeDual:
		return []manager.VisibilityManager{v.visibilityManager, v.secondaryVisibilityManager}, nil
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"Unknown secondary visibility writing mode: %s",
			v.secondaryVisibilityWritingMode(),
		))
	}
}

func (v *defaultManagerSelector) readManager(nsName namespace.Name) manager.VisibilityManager {
	if v.enableReadFromSecondaryVisibility(nsName.String()) {
		return v.secondaryVisibilityManager
	}
	return v.visibilityManager
}
