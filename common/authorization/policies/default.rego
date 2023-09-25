# The MIT License
#
# Copyright (c) 2023 Manetu Inc.  All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

package temporal.authz

default allow = false

###################################################################################################
# Primary rules
###################################################################################################

# APIs that are essentially read-only health checks with no sensitive information are
# always allowed
allow {
	is_health_check
}

# System Admin is allowed for everything
allow {
	is_system_admin
}

# System Writer is allowed for writing non admin or reading any read-only service APIs
allow {
	is_system_writer
	not is_admin_service
}
allow {
	is_system_writer
	is_read_only_api
}

# System Reader is allowed for all read only APIs, including admin read-only
allow {
	is_system_reader
	is_read_only_api
}

# Namespace Admin is allowed for everything within the namespace
allow {
	is_ns_admin
	not is_admin_service
}

# Namespace Writer is allowed for non admin service APIs within the namespace
allow {
	is_ns_writer
	not is_admin_service
}

# Namespace Reader is allowed for all non-admin read only APIs within the namespace
allow {
	is_ns_reader
	is_read_only_api
	not is_admin_service
}

###################################################################################################
# Helpers
###################################################################################################

#--------------------------------------------------------------------------------------------------
# Health Checks
#--------------------------------------------------------------------------------------------------
health_checks := {
	"/grpc.health.v1.Health/Check",
	"/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo",
}

is_health_check {
	health_checks[_] = input.target.APIName
}

#--------------------------------------------------------------------------------------------------
# Roles
#--------------------------------------------------------------------------------------------------
is_reader_role(r) {
	r >= 2
}

is_writer_role(r) {
	r >= 4
}

is_admin_role(r) {
	r >= 8
}

# Reader
is_system_reader {
	is_reader_role(input.claims.System)
}

is_ns_reader {
	is_reader_role(input.claims.Namespaces[input.target.Namespace])
}

# Writer
is_system_writer {
	is_writer_role(input.claims.System)
}

is_ns_writer {
	is_writer_role(input.claims.Namespaces[input.target.Namespace])
}

# Admin
is_system_admin {
	is_admin_role(input.claims.System)
}

is_ns_admin {
	is_admin_role(input.claims.Namespaces[input.target.Namespace])
}

#--------------------------------------------------------------------------------------------------
# Is Admin?
#--------------------------------------------------------------------------------------------------
admin_services := {
	"/temporal.server.api.adminservice.v1.AdminService/*",
}

is_admin_service {
	glob.match(admin_services[_], [], input.target.APIName)
}

#--------------------------------------------------------------------------------------------------
# Is Read-Only?
#--------------------------------------------------------------------------------------------------
read_only_namespace_apis := {
	"DescribeNamespace",
	"GetWorkflowExecutionHistory",
	"GetWorkflowExecutionHistoryReverse",
	"ListOpenWorkflowExecutions",
	"ListClosedWorkflowExecutions",
	"ListWorkflowExecutions",
	"ListArchivedWorkflowExecutions",
	"ScanWorkflowExecutions",
	"CountWorkflowExecutions",
	"QueryWorkflow",
	"DescribeWorkflowExecution",
	"DescribeTaskQueue",
	"ListTaskQueuePartitions",
	"DescribeSchedule",
	"ListSchedules",
	"ListScheduleMatchingTimes",
	"DescribeBatchOperation",
	"ListBatchOperations",
	"ListSearchAttributes"
}

read_only_global_apis := {
	"ListNamespaces",
	"GetSearchAttributes",
	"GetClusterInfo",
	"GetSystemInfo",
}

is_read_only_namespace_api {
	read_only_namespace_apis[_] = get_api_name(input.target.APIName)
}

is_read_only_global_api {
	read_only_global_apis[_] = get_api_name(input.target.APIName)
}

is_read_only_api {
	is_read_only_namespace_api
}

is_read_only_api {
	is_read_only_global_api
}

get_api_name(path) = last {
	segments := split(path, "/")
	last := segments[count(segments) - 1]
}
