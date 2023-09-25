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

#--------------------------------------------------------------------------------------------------
# Health Check
#--------------------------------------------------------------------------------------------------
test_any_can_healthcheck {
    allow with input as {
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/GetSystemInfo"}
    }
}

#--------------------------------------------------------------------------------------------------
# Unauthenticated
#--------------------------------------------------------------------------------------------------
test_unauthenticated_cannot_read {
    not allow with input as {
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"}
    }
}

#--------------------------------------------------------------------------------------------------
# System Reader Role
#--------------------------------------------------------------------------------------------------
test_system_reader_can_read {
    allow with input as {
         "claims": {"System": 2},
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"}
    }
}
test_system_reader_cannot_write {
    not allow with input as {
         "claims": {"System": 2},
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflow"}
    }
}
test_system_reader_cannot_admin {
    not allow with input as {
         "claims": {"System": 2},
         "target": {"APIName": "/temporal.server.api.adminservice.v1.AdminService/CreateNamespace"}
    }
}

#--------------------------------------------------------------------------------------------------
# Namespace Reader Role
#--------------------------------------------------------------------------------------------------
test_ns_reader_can_read_own {
    allow with input as {
         "claims": {"Namespaces": {"foo": 2}},
         "target": {"Namespace": "foo", "APIName": "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"}
    }
}
test_ns_reader_cannot_read_other {
    not allow with input as {
         "claims": {"Namespaces": {"foo": 2}},
         "target": {"Namespace": "bar", "APIName": "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"}
    }
}
test_ns_reader_cannot_admin_own {
    not allow with input as {
         "claims": {"Namespaces": {"foo": 2}},
         "target": {"Namespace": "foo", "APIName": "/temporal.server.api.adminservice.v1.AdminService/GetSearchAttributes"}
    }
}

#--------------------------------------------------------------------------------------------------
# System Writer Role
#--------------------------------------------------------------------------------------------------
test_system_writer_can_read {
    allow with input as {
         "claims": {"System": 4},
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow"}
    }
}
test_system_writer_can_write {
    allow with input as {
         "claims": {"System": 4},
         "target": {"APIName": "/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflow"}
    }
}
test_system_writer_cannot_admin {
    not allow with input as {
         "claims": {"System": 4},
         "target": {"APIName": "/temporal.server.api.adminservice.v1.AdminService/CreateNamespace"}
    }
}

#--------------------------------------------------------------------------------------------------
# Namespace Writer Role
#--------------------------------------------------------------------------------------------------
test_ns_writer_can_write_own {
    allow with input as {
         "claims": {"Namespaces": {"foo": 4}},
         "target": {"Namespace": "foo", "APIName": "/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflow"}
    }
}
test_ns_writer_cannot_write_other {
    not allow with input as {
         "claims": {"Namespaces": {"foo": 4}},
         "target": {"Namespace": "bar", "APIName": "/temporal.api.workflowservice.v1.WorkflowService/CreateWorkflow"}
    }
}
test_ns_writer_cannot_admin_own {
    not allow with input as {
         "claims": {"Namespaces": {"foo": 4}},
         "target": {"Namespace": "foo", "APIName": "/temporal.server.api.adminservice.v1.AdminService/GetSearchAttributes"}
    }
}

#--------------------------------------------------------------------------------------------------
# System Admin Role
#--------------------------------------------------------------------------------------------------
test_system_admin_can_admin {
    allow with input as {
         "claims": {"System": 8},
         "target": {"APIName": "/temporal.server.api.adminservice.v1.AdminService/CreateNamespace"}
    }
}

#--------------------------------------------------------------------------------------------------
# Namespace Admin Role
#--------------------------------------------------------------------------------------------------
test_ns_reader_can_write_own {
    allow with input as {
         "claims": {"Namespaces": {"foo": 8}},
         "target": {"Namespace": "foo", "APIName": "/temporal.server.api.adminservice.v1.AdminService/DeleteNamespace"}
    }
}
test_ns_reader_cannot_write_other {
    not allow with input as {
         "claims": {"Namespaces": {"foo": 8}},
         "target": {"Namespace": "bar", "APIName": "/temporal.server.api.adminservice.v1.AdminService/DeleteNamespace"}
    }
}
