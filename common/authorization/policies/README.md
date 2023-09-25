# About this directory

This directory contains [Open Policy Agent](https://www.openpolicyagent.org/) based policies that ship with the product for use as Authorizers in conjunction with with OpaAuthorizer.  The OpaAuthorizer is enabled with:

```yaml
global:
  authorizer: opa
```

Any *.rego files included in this directory become accessible via the 'internal:' prefix in configuration.  For example, 'default.rego' can be selected with the following config:

```yaml
global:
  authorizer: opa
  policies:
    - internal:default
```

## Policy development

### Adding new policies

Adding policies is as simple as adding a new *.rego file to this directory, and adding a matching unit-test as ./test/*.rego.

For example, consider the noop policy structure:

```
common/authorization/policies/
├── noop.rego
└── test
    └── noop_test.rego
```

### Testing

#### Prerequisites

- opa command line tool - https://www.openpolicyagent.org/docs/latest/#running-opa

#### Execute tests

You may run 'make' within this directory to execute all available unit-tests. 

```shell
$ make
opa test -v --explain full -m 0 default.rego test/default_test.rego;
test/default_test.rego:
data.temporal.authz.test_any_can_healthcheck: PASS (2.490566ms)
data.temporal.authz.test_unauthenticated_cannot_read: PASS (904.076µs)
data.temporal.authz.test_system_reader_can_read: PASS (1.447477ms)
data.temporal.authz.test_system_reader_cannot_write: PASS (578.541µs)
data.temporal.authz.test_system_reader_cannot_admin: PASS (468.835µs)
data.temporal.authz.test_ns_reader_can_read_own: PASS (1.28003ms)
data.temporal.authz.test_ns_reader_cannot_read_other: PASS (645.315µs)
data.temporal.authz.test_ns_reader_cannot_admin_own: PASS (641.73µs)
data.temporal.authz.test_system_writer_can_read: PASS (329.296µs)
data.temporal.authz.test_system_writer_can_write: PASS (281.118µs)
data.temporal.authz.test_system_writer_cannot_admin: PASS (708.282µs)
data.temporal.authz.test_ns_writer_can_write_own: PASS (519.168µs)
data.temporal.authz.test_ns_writer_cannot_write_other: PASS (358.848µs)
data.temporal.authz.test_ns_writer_cannot_admin_own: PASS (581.004µs)
data.temporal.authz.test_system_admin_can_admin: PASS (187.063µs)
data.temporal.authz.test_ns_reader_can_write_own: PASS (236.945µs)
data.temporal.authz.test_ns_reader_cannot_write_other: PASS (259.962µs)
--------------------------------------------------------------------------------
PASS: 17/17
opa test -v --explain full -m 0 noop.rego test/noop_test.rego;
test/noop_test.rego:
data.temporal.authz.test_allow_anything: PASS (337.159µs)
--------------------------------------------------------------------------------
PASS: 1/1
```