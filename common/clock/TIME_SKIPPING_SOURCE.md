# TimeSkippingTimeSource

## Overview

`TimeSkippingTimeSource` is a `TimeSource` implementation that wraps another base `TimeSource` and adds accumulated skipped time to it. This is useful for testing scenarios where time can be artificially advanced (e.g., simulating system sleep, clock adjustments, or time zone changes).

## Key Features

1. **Wraps any TimeSource**: Can use `RealTimeSource` (default) or `EventTimeSource` as the base
2. **Accumulates skipped time**: Stores a slice of skipped durations that are summed for the current time
3. **Smart timer handling**: Timers fire early if skipped time causes their deadline to pass
4. **Thread-safe**: All operations are protected by mutexes

## API

### Constructors

#### `NewTimeSkippingTimeSource`
```go
func NewTimeSkippingTimeSource(baseSource TimeSource) *TimeSkippingTimeSource
```
Creates a new `TimeSkippingTimeSource` with no initial skipped time.
- If `baseSource` is `nil`, defaults to `RealTimeSource`

#### `NewTimeSkippingTimeSourceWithSkippedTime`
```go
func NewTimeSkippingTimeSourceWithSkippedTime(baseSource TimeSource, skippedTime []time.Duration) *TimeSkippingTimeSource
```
Creates a new `TimeSkippingTimeSource` with an initial set of skipped time durations.
- If `baseSource` is `nil`, defaults to `RealTimeSource`
- The `skippedTime` slice is copied, so modifications to the original slice won't affect the TimeSource
- Useful for initializing with a known state or restoring from persisted data

**Example:**
```go
skippedTime := []time.Duration{5*time.Second, 3*time.Second, 2*time.Second}
ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(baseSource, skippedTime)
// Now() returns base time + 10 seconds
```

### TimeSource Interface Implementation

#### `Now() time.Time`
Returns the current time from the base source **plus** all accumulated skipped time.

```go
ts := clock.NewTimeSkippingTimeSource(nil)
ts.AddSkippedTime(10 * time.Second)
now := ts.Now() // Real time + 10 seconds
```

#### `Since(t time.Time) time.Duration`
Returns the duration since `t`, using the adjusted `Now()`.

#### `AfterFunc(d time.Duration, f func()) Timer`
Creates a timer that fires after duration `d` in **adjusted time**.

**Important**: If skipped time causes the deadline to pass before the real timer fires, the callback fires immediately.

```go
timer := ts.AfterFunc(10 * time.Second, callback)
// Timer set to fire at adjusted_now + 10s

ts.AddSkippedTime(15 * time.Second)
// Callback fires immediately because deadline passed
```

#### `NewTimer(d time.Duration) (<-chan time.Time, Timer)`
Creates a timer that sends the adjusted time on a channel when it fires.

### Additional Methods

#### `AddSkippedTime(d time.Duration)`
Adds a duration to the accumulated skipped time.

**Key behavior**: When time is added, any pending timers whose deadlines are now in the past will fire immediately.

```go
ts := clock.NewTimeSkippingTimeSource(baseSource)

// Create timers
ts.AfterFunc(5*time.Second, callback1)  // fires at t=5
ts.AfterFunc(10*time.Second, callback2) // fires at t=10
ts.AfterFunc(15*time.Second, callback3) // fires at t=15

// Skip 12 seconds
ts.AddSkippedTime(12*time.Second)
// callback1 and callback2 fire immediately
// callback3 is still pending (fires at t=15)
```

## How It Works

### Time Tracking
- Base time comes from the wrapped `TimeSource`
- Skipped time is stored as `[]int64` (nanoseconds)
- Adjusted time = base time + sum(skipped time)

### Timer Management
Each timer created via `AfterFunc` or `NewTimer` is tracked with:
- `deadline`: When it should fire in adjusted time
- `baseTimer`: The actual timer from the base source
- `fired`: Whether it has already fired

When `AddSkippedTime` is called:
1. Calculate new adjusted `Now()`
2. Check all pending timers
3. Fire any timers whose `deadline <= Now()`
4. Stop their base timers
5. Remove them from tracking

### Thread Safety
- All operations acquire locks before accessing shared state
- Timer callbacks fire **outside** the lock to prevent deadlocks
- Each timer has its own mutex for state management

## Use Cases

### 1. Initializing with Historical Skips
```go
// Restore time source from persisted state
historicalSkips := []time.Duration{
    1 * time.Hour,   // System sleep #1
    30 * time.Minute, // System sleep #2
    15 * time.Minute, // Clock adjustment
}

ts := clock.NewTimeSkippingTimeSourceWithSkippedTime(nil, historicalSkips)
// Time source immediately reflects 1h 45m of skipped time
```

### 2. Testing with Real Time + Skips
```go
ts := clock.NewTimeSkippingTimeSource(nil) // Uses RealTimeSource

// Simulate system going to sleep for 1 hour
ts.AddSkippedTime(1 * time.Hour)

// All future time queries reflect the skip
now := ts.Now() // Real time + 1 hour
```

### 2. Testing with Controlled Time + Skips
```go
baseSource := clock.NewEventTimeSource()
ts := clock.NewTimeSkippingTimeSource(baseSource)

// Control both real time AND skipped time
baseSource.Advance(5 * time.Second)   // Real time passes
ts.AddSkippedTime(10 * time.Second)   // Additional skip
// Total adjusted time: 15 seconds
```

### 3. Simulating Clock Adjustments
```go
ts := clock.NewTimeSkippingTimeSource(nil)

// User adjusts system clock forward 30 minutes
ts.AddSkippedTime(30 * time.Minute)

// All timers and time queries account for the adjustment
```

### 4. Testing Timer Behavior During Skips
```go
ts := clock.NewTimeSkippingTimeSource(baseSource)

// Set a 10-second timer
timer := ts.AfterFunc(10*time.Second, func() {
    fmt.Println("Timer fired!")
})

// Simulate 15-second skip (e.g., system sleep)
ts.AddSkippedTime(15*time.Second)
// Output: "Timer fired!" - fired immediately due to skip
```

## Comparison with Other TimeSource Implementations

| Feature | RealTimeSource | EventTimeSource | TimeSkippingTimeSource |
|---------|---------------|-----------------|------------------------|
| **Base time** | Real wall clock | Fake, controlled | Configurable (any TimeSource) |
| **Time progression** | Automatic | Manual (`Advance`) | Automatic + manual skips |
| **Timers** | Real OS timers | Synchronous fake | Real/fake + skip-aware |
| **Use case** | Production | Testing | Production + testing (skip scenarios) |
| **Skips affect timers** | No | N/A | Yes |

## Example: Complete Test Scenario

```go
func TestWorkflowWithSystemSleep(t *testing.T) {
    // Use EventTimeSource for full control
    baseSource := clock.NewEventTimeSource()
    ts := clock.NewTimeSkippingTimeSource(baseSource)

    workflow := NewWorkflow(ts)

    // Workflow sets a 30-second timeout
    workflow.Start()

    // 10 seconds pass normally
    baseSource.Advance(10 * time.Second)
    assert.False(t, workflow.IsTimedOut())

    // System goes to sleep for 25 seconds
    ts.AddSkippedTime(25 * time.Second)

    // Timeout should have triggered (10 + 25 = 35 > 30)
    assert.True(t, workflow.IsTimedOut())
}
```

## Implementation Details

### Negative Skips
Negative durations are supported - they reduce the total skipped time:
```go
ts.AddSkippedTime(10 * time.Second)  // +10s
ts.AddSkippedTime(-5 * time.Second)  // -5s
// Net skip: 5 seconds
```

### Concurrent Safety
All public methods are thread-safe. Multiple goroutines can:
- Call `AddSkippedTime` concurrently
- Create timers concurrently
- Query `Now()` concurrently

### Timer Ordering
When multiple timers fire due to a skip, they fire in **deadline order** (earliest first).

### Memory Management
- Fired timers are removed from the tracking list
- Stopped timers are removed from the tracking list
- No timer leaks
