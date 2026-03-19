# CATCH Architecture

## Baseball Metaphor

| Term | Baseball | CATCH System |
|------|----------|--------------|
| **Fault** | Atomic action (bunt, steal, etc.) | Atomic fault injection (delay, fail, cancel, timeout) |
| **Pitcher** | Makes plays | Executes fault injection |
| **Scorebook** | Records what happened | Records events from OTLP spans (importer) |
| **Umpire** | Validates outcomes | Receives OTLP traces, validates properties |
| **Lineup** | Team members | Entities (Workflow, TaskQueue, WorkflowTask) |
| **Rulebook** | Rules of the game | Property models (invariants) |

## Core Concepts

### Fault (Atomic Injection Config)
**Location**: `tools/umpire/fault/fault.go`

A Fault is an atomic injection config that:
- Describes only the INTENTION (delay, fail, cancel, timeout)
- Has NO knowledge of specific services (Matching, History, etc.)
- Has NO expected outcomes (that's Umpire's job)

```go
type Fault struct {
    Type      FaultType        // delay, fail, cancel, timeout, drop
    Params    map[string]any   // Action-specific parameters
    Timestamp time.Time        // When fault was injected
    Target    string           // Set at runtime (for scorebook)
}

// Fault types
const (
    FaultDelay   FaultType = "delay"
    FaultFail    FaultType = "fail"
    FaultCancel  FaultType = "cancel"
    FaultTimeout FaultType = "timeout"
    FaultDrop    FaultType = "drop"
)
```

### Pitcher (Injects Faults)
**Location**: `tools/umpire/pitcher/pitcher.go`

The Pitcher:
- Injects faults based on matching criteria
- Returns the fault that was injected (for scorebook recording)
- Uses typed proto message references for targets
- Each fault matches ONCE and is deleted

```go
type Pitcher interface {
    // MakePlay executes a fault if matching criteria are met
    // Returns the fault that was injected and error to inject
    MakePlay(ctx context.Context, targetType any, request any) (*fault.Fault, error)

    // Configure sets fault configuration with matching criteria
    Configure(targetType any, config FaultConfig)

    Reset()
}

type FaultConfig struct {
    Fault fault.Fault      // The action to take
    Match *MatchCriteria   // When to inject this fault
}

type MatchCriteria struct {
    // RPC target
    Target string // e.g., "matchingservice.AddWorkflowTask"

    // Entity criteria
    WorkflowID  string
    NamespaceID string
    TaskQueue   string
    RunID       string
    Custom      map[string]any
}
```

### Lineup (Entities and Relationships)
**Location**: `tools/umpire/lineup/`

Lineup defines the entities in the system:
- **Workflow**: Workflow instances with FSM state
- **TaskQueue**: Task queue entities
- **WorkflowTask**: Workflow task entities
- **Identity**: Entity identification system

Entities track state and relationships. Lineup provides the entity registry.

### Rulebook (Property Models)
**Location**: `tools/umpire/rulebook/`

Rulebook contains property validation models (invariants):
- `StuckWorkflowModel`: Detects workflows that don't complete
- `LostTaskModel`: Detects tasks that aren't polled
- `WorkflowLifecycleInvariants`: Validates workflow state transitions
- `TaskDeliveryGuarantees`: Validates task delivery
- `RetryResilienceModel`: Validates retry behavior

Models query the lineup to validate properties.

### Scorebook (Records Events)
**Location**: `tools/umpire/scorebook/`

Scorebook:
- Imports OTLP spans and converts to moves (events that happened)
- Records what faults were injected
- Stores entity state transitions
- Provides moves to lineup for entity updates

**Moves** (`scorebook/moves/`): Domain events like StartWorkflow, AddWorkflowTask, PollWorkflowTask, etc.

### Umpire (Orchestrates Validation)
**Location**: `tools/umpire/umpire/`

Umpire:
- Receives OTLP traces (acts as OTLP collector endpoint)
- Routes events to lineup entities via scorebook
- Runs rulebook models to check for violations
- Reports violations
- Does NOT define faults or expected outcomes

## Module Structure

```
tools/umpire/
├── fault/             # Fault injection configs
│   └── fault.go      # Fault definitions (delay, fail, timeout, etc.)
├── pitcher/           # Injects faults
│   ├── pitcher.go    # Pitcher interface with MakePlay()
│   └── interceptor.go # gRPC interceptor
├── lineup/            # Entities and relationships
│   ├── lineup.go     # Entity registry
│   ├── types/        # Entity identity types
│   └── entities/     # Workflow, TaskQueue, WorkflowUpdate, etc.
├── rulebook/          # Property models
│   ├── registry.go   # Model registry
│   ├── stuck_workflow.go
│   ├── lost_task.go
│   └── ...          # Other models
├── scorebook/         # Records events
│   ├── importer.go   # OTLP span -> move importer
│   └── moves/        # Move definitions (events that happened)
└── umpire/            # Orchestration + OTLP receiver
    ├── umpire.go     # Main umpire
    ├── model.go      # Model dependencies
    └── violation.go  # Violation reporting
```

## Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                        Test                              │
│  ┌──────────┐                                            │
│  │ Pitcher  │ Configures faults with matching criteria  │
│  └────┬─────┘                                            │
│       │                                                   │
│       v                                                   │
│  ┌──────────┐         ┌──────────┐                      │
│  │  gRPC    │─injects─> │  Fault  │                      │
│  │Intercept │         └──────────┘                      │
│  └────┬─────┘                                            │
│       │                                                   │
│       v                                                   │
│  ┌──────────┐                                            │
│  │  OTLP    │ Telemetry spans                           │
│  │  Traces  │                                            │
│  └────┬─────┘                                            │
│       │                                                   │
│       v                                                   │
│  ┌──────────┐         ┌──────────┐      ┌──────────┐   │
│  │Scorebook │─event─> │  Lineup  │ <─── │ Rulebook │   │
│  │(Importer)│         │(Entities)│ ───> │ (Models) │   │
│  └──────────┘         └──────────┘      └────┬─────┘   │
│                                                │          │
│                                                v          │
│                                           ┌──────────┐   │
│                                           │ Umpire   │   │
│                                           │(Validate)│   │
│                                           └──────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Key Design Principles

1. **Separation of Concerns**
   - Fault = what to do (delay, fail)
   - Pitcher = when/where to do it (matching criteria)
   - Umpire = is it correct? (property validation)

2. **Service Agnostic**
   - Fault/Pitcher don't know about Matching, History, Frontend
   - MatchCriteria provides that knowledge

3. **Observable Actions**
   - Every fault injected can be recorded by scorebook
   - Scorebook records are inputs to umpire validation

4. **One-Time Execution**
   - Each fault matches once and is deleted
   - Multiple matches require multiple fault configurations

5. **Typed Proto References**
   - Use `&matchingservice.AddWorkflowTaskRequest{}` instead of strings
   - Type-safe target identification via reflection

## Usage Example

```go
// Setup pitcher in test
p := pitcher.New()

// Configure fault injection
p.Configure(
    &matchingservice.AddWorkflowTaskRequest{},
    pitcher.FaultConfig{
        Fault: fault.FailFault(pitcher.ErrorResourceExhausted),
        Match: &pitcher.MatchCriteria{
            WorkflowID: "test-workflow-123",
        },
    },
)

// Run test - interceptor automatically injects faults
// Scorebook records faults
// Umpire validates properties
```
