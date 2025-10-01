# Dynamic Configuration

Dynamic configuration allows you to modify Temporal server behavior at runtime without restarting the server. Settings defined in these files override the default values specified in the Go source code.

For complete self-hosted deployment information, see the [Self-Hosted Deployment Guide](https://docs.temporal.io/self-hosted-guide).

## Finding Available Dynamic Config Settings

**All available dynamic config keys and their documentation are defined in the Go source code:**

- **Core server settings**: `common/dynamicconfig/constants.go`
- **Component-specific settings**: `components/*/config.go`

Each setting definition includes:
- Key name (e.g., `history.persistenceMaxQPS`)
- Default value
- Description of what it controls
- Whether it's scoped globally, per-namespace, per-taskqueue, etc.

You can search for all settings with:
```bash
grep -r "New.*Setting" common/dynamicconfig/constants.go
grep -r "New.*Setting" components/
```

## Quick Start

### Configuration Files

- **`docker.yaml`** - Container/Docker deployments (minimal file, add settings as needed)
- **`development-sql.yaml`** - SQL database development setup with example settings
- **`development-cass.yaml`** - Cassandra development setup with example settings
- **`development-xdc.yaml`** - Cross-datacenter replication development setup

### How It Works

1. Dynamic config files are loaded by the main server config via `dynamicConfigClient` section
2. Changes to these files take effect automatically (default poll interval: 10-60 seconds)
3. No server restart required for changes to take effect
4. Settings defined here override defaults in the codebase

## Configuration Format

Each setting follows this structure:

```yaml
settingName:
  - value: <setting-value>
    constraints: {}
```

### Constraints

Each key can have zero or more values, and each value can have zero or more constraints to target specific namespaces, task queues, or task types:

- `namespace: "string"` - Apply only to a specific namespace
- `taskQueueName: "string"` - Apply only to a specific task queue
- `taskType: "Workflow"|"Activity"` - Apply only to specific task type

A value will be selected and returned if it has exactly the same constraints as the ones specified in query filters (including the number of constraints).

### Examples

Simple global setting:
```yaml
limit.maxIDLength:
  - value: 1000
    constraints: {}
```

Namespace-specific override:
```yaml
frontend.persistenceMaxQPS:
  - value: 3000                     # Default for all namespaces
    constraints: {}
  - value: 5000                     # Override for specific namespace
    constraints:
      namespace: "production"
```

Task queue specific setting:
```yaml
history.defaultActivityRetryPolicy:
  - value:
      InitialIntervalInSeconds: 1
      BackoffCoefficient: 2.0
      MaximumAttempts: 0
    constraints:
      namespace: "production"
      taskQueueName: "critical-operations"
```

Task type specific setting:
```yaml
someSettingKey:
  - value: 100
    constraints:
      namespace: "production"
      taskQueueName: "my-queue"
      taskType: "Workflow"
  - value: 200
    constraints:
      namespace: "production"
      taskQueueName: "my-queue"
      taskType: "Activity"
```

Complex value types:
```yaml
component.callbacks.allowedAddresses:
  - value:
      - Pattern: "*"
        AllowInsecure: true
      - Pattern: "*.example.com"
        AllowInsecure: false
```

## Examples from Source Code

To see examples of how settings are defined in the source:

**Example from `common/dynamicconfig/constants.go`:**
```go
HistoryPersistenceMaxQPS = NewGlobalIntSetting(
    "history.persistenceMaxQPS",
    9000,
    `HistoryPersistenceMaxQPS is the max qps history host can query DB`,
)
```

**Example from `components/nexusoperations/config.go`:**
```go
RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
    "component.nexusoperations.request.timeout",
    time.Second*10,
    `RequestTimeout is the timeout for making a single nexus start or cancel request.`,
)
```

These definitions show you the setting key, default value, and description to use when configuring them in your YAML files.

## Validation

Validate your dynamic config file before deploying:

```bash
temporal server validate-dynamic-config <file>
```

This checks for:
- Valid YAML syntax
- Known configuration keys
- Correct value types for each setting
