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

package authorization

type Role int16

// @@@SNIPSTART temporal-common-authorization-role-enum
// User authz within the context of an entity, such as system, namespace or workflow.
// User may have any combination of these authz within each context, except for RoleUndefined, as a bitmask.
const (
	RoleWorker = Role(1 << iota)
	RoleReader
	RoleWriter
	RoleAdmin
	RoleUndefined = Role(0)
)

// @@@SNIPEND

// Checks if the provided role bitmask represents a valid combination of authz
func (b Role) IsValid() bool {
	return b&^(RoleWorker|RoleReader|RoleWriter|RoleAdmin) == 0
}

// @@@SNIPSTART temporal-common-authorization-claims
// Claims contains the identity of the subject and subject's roles at the system level and for individual namespaces
type Claims struct {
	// Identity of the subject
	Subject string
	// Role within the context of the whole Temporal cluster or a multi-cluster setup
	System Role
	// Roles within specific namespaces
	Namespaces map[string]Role
	// Free form bucket for extra data
	Extensions interface{}
}

// @@@SNIPEND
