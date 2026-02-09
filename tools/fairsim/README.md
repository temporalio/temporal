
# fairsim

This tool simulates the behavior of Temporal's fair task queues, for use in
evaluating different parameters.

## Build
run `make fairsim`

## Usage

There are two main modes:

- **Generation**: Generates tasks by random distribution and then dispatches
  them.
- **Script**: Process a script with queue pushes and pops. This can be used to
  test with actual data or a custom distribution, and with continuous task
  creation/dispatch.

### Generation

By default, fairsim generates tasks with fairness keys following a Zipf
distribution.

Examples:

```bash
# Generate a million tasks with 20 keys
fairsim -- -tasks=1000000 -keys=20

# Generate a million tasks with a much more lopsided distribution
fairsim -- -tasks=1000000 -keys=50 -zipf-s=3 -zipf-v=1.1

# Try alternate counter paramters
for w in 1 10 100 1000 10000; do
  fairsim -counter-params <(echo '{"CMS":{"W":'$w'}}') -- -tasks=1000000 -keys=1000 | grep p90s: | tail -1
done

# Disable fairness to compare to fifo
fairsim -fair=0 -- -tasks=500

# Use only one partition (default is 4)
fairsim -partitions=1 -- -tasks=500
```

### Script

Examples:

```bash
# Priority order
{
  echo "task -pri=4 -payload four"
  echo "task -pri=2 -payload two"
  echo "task -pri=3 -payload three"
  echo "task -pri=5 -payload five"
  echo "task -pri=1 -payload one"
} | fairsim -script=/dev/stdin -partitions=1
# should see one, two, three, four, five

# Fairness
{
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey b"
} | fairsim -script=/dev/stdin -partitions=1
# should see a, b, a, a, a

# Alternating
{
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey a"
  echo "task -fkey b"
  echo poll # gets a
  echo poll # gets b
  echo "task -fkey c"
  echo "task -fkey c"
} | fairsim -script=/dev/stdin -partitions=1
# should see a, b, c, a, c, a, a

# Weight
{
  for i in {1..20}; do echo "task -fkey a"; done
  for i in {1..20}; do echo "task -fkey b -fweight 5"; done
} | fairsim -script=/dev/stdin -partitions=1
# should see five b's for each a until b's are done
```

## Interpreting output

### Task section

First, fairsim will print one line for each task dispatched:

```
task idx:    33  dsp:    15  lat:   -18  pri: 3  fkey:    "key1"  fweight:  1  part: 2  payload:"external-key-32"

```

- `idx`: Creation index. Tasks are assigned an incrementing index as they are created.
- `dsp`: Dispatch index. Order this task was dispatched in.
- `lat`: "Latency": dispatch index minus creation index. For a FIFO queue this
  would always be zero. Negative means the task was moved earlier compared to
  FIFO, positive means it was penalized.
- `pri`: Task priority.
- `fkey`: Fairness key.
- `fweight`: Fairness weight.
- `part`: Partition task was assigned to.
- `payload`: User-defined payload (could be used for correlation).

### Statistics

Next are statistics on the "latency" values. In general, lower numbers are
better, since that means more tasks were moved ahead of where they would be in a
FIFO queue.

**Raw Latency Statistics**

Basic stats on the "latency" values. Mean must always be zero since it's a
permutation.

**Normalized Latency Statistics**

When looking at per-key latencies, we expect a "heavy" fairness key with more
tasks to have worse latency than a "light" key, since the light key's tasks get
pushed in front of the heavy one. This is desirable, though, and doesn't really
reflect how much the heavy one was "penalized". We can normalize the latency by
dividing by the number of tasks for that key.

Both the raw and normalized can be useful to look at.

**Per-task Statistics**

The raw and normalized latency stats are printed for each key, along with the
count.

**Percentile of percentiles**

Raw and normalized percentile stats are printed to give a summary of latency
across different keys:

Rows are percentiles of latency for each key, and columns are percentiles across
those percentiles (counting each key once). E.g. the "p90s" row describes the
90th percentile latency for each key. The "@50" column of that is the median of
those 90th percentile latencies.

