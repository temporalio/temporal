//nolint:errcheck // don't need to check fmt.Fprintf
package fairsim

import (
	"bufio"
	"cmp"
	"container/heap"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strings"

	"go.temporal.io/server/service/matching/counter"
)

// matches service/matching.strideFactor, but we don't want to export that
const defaultStrideFactor = 1000

type (
	task struct {
		pri     int
		fkey    string
		fweight float32
		pass    int64
		index   int64
		payload string
	}

	state struct {
		rnd            *rand.Rand
		counterFactory func() counter.Counter
		partitions     []partitionState
		strideFactor   float32
	}

	simulator struct {
		state           *state
		stats           *latencyStats
		w               io.Writer
		verbose         bool
		nextIndex       int64
		dispatchIndex   int64
		defaultPriority int
	}

	latencyStats struct {
		byKey             map[string][]int64   // latencies by fairness key
		byKeyNormalized   map[string][]float64 // normalized latencies by fairness key
		overall           []int64              // all latencies
		overallNormalized []float64            // all normalized latencies
	}

	partitionState struct {
		perPri map[int]perPriState
		heap   taskHeap
	}

	taskHeap []*task

	perPriState struct {
		c counter.Counter
	}

	unfairCounter struct{}
)

// parseFlags creates a FlagSet, calls setup to register flags, and parses args.
// Returns remaining (non-flag) arguments.
func parseFlags(name string, args []string, setup func(*flag.FlagSet)) ([]string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	var flagErrors strings.Builder
	fs.SetOutput(&flagErrors)
	setup(fs)
	if err := fs.Parse(args); err != nil {
		if flagErrors.Len() > 0 {
			return nil, fmt.Errorf("%s: %w\n%s", name, err, flagErrors.String())
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return fs.Args(), nil
}

func RunTool(args []string) error {
	var (
		seed         *int64
		fair         *bool
		partitions   *int
		strideFactor *int
		counterFile  *string
		scriptFile   *string
		verbose      *bool
	)
	remainingArgs, err := parseFlags("fairsim", args, func(fs *flag.FlagSet) {
		seed = fs.Int64("seed", rand.Int64(), "Random seed")
		fair = fs.Bool("fair", true, "Enable fairness (false for FIFO)")
		partitions = fs.Int("partitions", 4, "Number of partitions")
		strideFactor = fs.Int("strideFactor", defaultStrideFactor, "Stride factor")
		counterFile = fs.String("counter-params", "", "JSON file with CounterParams")
		scriptFile = fs.String("script", "", "Script file to execute instead of generating tasks")
		verbose = fs.Bool("verbose", false, "verbose output")
	})
	if err != nil {
		return err
	}

	// Load counter params
	var params counter.CounterParams
	if *counterFile == "" {
		params = counter.DefaultCounterParams
	} else {
		data, err := os.ReadFile(*counterFile)
		if err != nil {
			return fmt.Errorf("failed to load counter params: %w", err)
		} else if err = json.Unmarshal(data, &params); err != nil {
			return fmt.Errorf("failed to load counter params: %w", err)
		}
		fmt.Printf("Using counter params: %#v\n\n", params)
	}

	src := rand.NewPCG(uint64(*seed), uint64(*seed+1))
	rnd := rand.New(src)

	counterFactory := func() counter.Counter { return unfairCounter{} }
	if *fair {
		counterFactory = func() counter.Counter { return counter.NewHybridCounter(params, src) }
	}

	st := newState(rnd, counterFactory, *partitions, *strideFactor)
	stats := newLatencyStats()

	const defaultPriority = 3
	sim := newSimulator(st, stats, defaultPriority, os.Stdout, *verbose)

	// Check if script mode
	if *scriptFile != "" {
		return sim.runScript(*scriptFile)
	}

	// Default behavior: run gentasks command with remaining args from command line
	if err := sim.executeGenTasksCommand(remainingArgs); err != nil {
		return err
	}

	sim.finish()
	return nil
}

func newLatencyStats() *latencyStats {
	return &latencyStats{
		byKey:           make(map[string][]int64),
		byKeyNormalized: make(map[string][]float64),
	}
}

func newState(rnd *rand.Rand, counterFactory func() counter.Counter, partitions, strideFactor int) *state {
	return &state{
		rnd:            rnd,
		counterFactory: counterFactory,
		partitions:     make([]partitionState, partitions),
		strideFactor:   float32(strideFactor),
	}
}

func newSimulator(state *state, stats *latencyStats, defaultPriority int, w io.Writer, verbose bool) *simulator {
	return &simulator{
		state:           state,
		stats:           stats,
		w:               w,
		verbose:         verbose,
		defaultPriority: defaultPriority,
	}
}

// addTask adds a task to the simulator, assigning defaults and an index.
func (sim *simulator) addTask(t *task) {
	t.pri = cmp.Or(t.pri, sim.defaultPriority)
	t.fweight = cmp.Or(t.fweight, 1.0)
	t.index = sim.nextIndex
	sim.nextIndex++
	sim.state.addTask(t)
}

// processTask records stats for a dispatched task and returns the latency.
func (sim *simulator) processTask(t *task) int64 {
	latency := sim.dispatchIndex - t.index
	sim.stats.byKey[t.fkey] = append(sim.stats.byKey[t.fkey], latency)
	sim.stats.overall = append(sim.stats.overall, latency)
	sim.dispatchIndex++
	return latency
}

// printTask writes a single task's dispatch info to the writer.
func (sim *simulator) printTask(t *task, partition int, latency int64) {
	if !sim.verbose {
		return
	}
	fmt.Fprintf(sim.w, "task idx:%6d  dsp:%6d  lat:%6d  pri:%2d  fkey:%10q  fweight:%3g  part:%2d  payload:%q\n",
		t.index, sim.dispatchIndex-1, latency, t.pri, t.fkey, t.fweight, partition, t.payload)
}

// drainTasks pops and processes all remaining tasks, printing each one.
func (sim *simulator) drainTasks() {
	for t, partition := sim.state.popTask(); t != nil; t, partition = sim.state.popTask() {
		latency := sim.processTask(t)
		sim.printTask(t, partition, latency)
	}
}

// finish drains remaining tasks, calculates normalized stats, and prints stats.
func (sim *simulator) finish() {
	sim.drainTasks()
	sim.stats.calculateNormalized()
	sim.stats.fprint(sim.w, sim.verbose)
}

func (sim *simulator) runScript(scriptFile string) error {
	file, err := os.Open(scriptFile)
	if err != nil {
		return fmt.Errorf("failed to open script file: %w", err)
	}
	defer file.Close()

	var commands []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		commands = append(commands, line)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading script file: %w", err)
	}

	return sim.runCommands(commands)
}

func (sim *simulator) runCommands(commands []string) error {
	for _, cmd := range commands {
		if err := sim.executeCommand(cmd); err != nil {
			return fmt.Errorf("error executing command %q: %w", cmd, err)
		}
	}
	sim.finish()
	return nil
}

func (sim *simulator) executeCommand(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "task":
		return sim.executeTaskCommand(parts[1:])
	case "poll":
		return sim.executePollCommand()
	case "stats":
		return sim.executeStatsCommand()
	case "clearstats":
		return sim.executeClearStatsCommand()
	case "gentasks":
		return sim.executeGenTasksCommand(parts[1:])
	default:
		return fmt.Errorf("unknown command: %q", parts[0])
	}
}

func (sim *simulator) executeTaskCommand(args []string) error {
	var (
		fkey    *string
		fweight *float64
		pri     *int
		payload *string
	)
	if _, err := parseFlags("task", args, func(fs *flag.FlagSet) {
		fkey = fs.String("fkey", "", "fairness key")
		fweight = fs.Float64("fweight", 1.0, "fairness weight")
		pri = fs.Int("pri", sim.defaultPriority, "priority")
		payload = fs.String("payload", "", "payload")
	}); err != nil {
		return err
	}

	sim.addTask(&task{
		fkey:    *fkey,
		fweight: float32(*fweight),
		pri:     *pri,
		payload: *payload,
	})
	return nil
}

func (sim *simulator) executePollCommand() error {
	t, partition := sim.state.popTask()
	if t == nil {
		fmt.Fprintln(sim.w, "No tasks in queue")
		return nil
	}
	latency := sim.processTask(t)
	sim.printTask(t, partition, latency)
	return nil
}

func (sim *simulator) executeStatsCommand() error {
	sim.stats.calculateNormalized()
	sim.stats.fprint(sim.w, sim.verbose)
	return nil
}

func (sim *simulator) executeClearStatsCommand() error {
	sim.stats = newLatencyStats()
	return nil
}

func (sim *simulator) executeGenTasksCommand(args []string) error {
	var (
		tasks     *int
		keys      *int
		keyprefix *string
		zipfS     *float64
		zipfV     *float64
	)
	if _, err := parseFlags("gentasks", args, func(fs *flag.FlagSet) {
		tasks = fs.Int("tasks", 100, "number of tasks to generate")
		keys = fs.Int("keys", 10, "number of unique fairness keys")
		keyprefix = fs.String("keyprefix", "key", "prefix for generated fairness keys")
		zipfS = fs.Float64("zipf-s", 2.0, "zipf distribution s parameter")
		zipfV = fs.Float64("zipf-v", 2.0, "zipf distribution v parameter")
	}); err != nil {
		return err
	}

	if *tasks <= 0 {
		return fmt.Errorf("tasks must be positive, got %d", *tasks)
	}
	if *keys <= 0 {
		return fmt.Errorf("keys must be positive, got %d", *keys)
	}

	zipf := rand.NewZipf(sim.state.rnd, *zipfS, *zipfV, uint64(*keys-1))
	for range *tasks {
		sim.addTask(&task{
			fkey: fmt.Sprintf("%s%d", *keyprefix, zipf.Uint64()),
		})
	}
	return nil
}

// --- state methods ---

// addTask adds a task to a random partition, computing its pass via the counter.
func (s *state) addTask(t *task) {
	partition := &s.partitions[s.rnd.IntN(len(s.partitions))]

	if partition.perPri == nil {
		partition.perPri = make(map[int]perPriState)
	}

	priState, exists := partition.perPri[t.pri]
	if !exists {
		priState = perPriState{c: s.counterFactory()}
		partition.perPri[t.pri] = priState
	}

	t.pass = priState.c.GetPass(t.fkey, 0, max(1, int64(s.strideFactor/t.fweight)))
	heap.Push(&partition.heap, t)
}

// popTask returns the task with minimum (pri, pass, index) from a random partition.
func (s *state) popTask() (*task, int) {
	for _, idx := range s.rnd.Perm(len(s.partitions)) {
		partition := &s.partitions[idx]
		if partition.heap.Len() > 0 {
			t := heap.Pop(&partition.heap).(*task) //nolint:revive
			return t, idx
		}
	}
	return nil, -1
}

// --- unfairCounter (FIFO mode) ---

func (u unfairCounter) GetPass(key string, base int64, inc int64) int64 { return base }
func (u unfairCounter) EstimateDistinctKeys() int                       { return 0 }
func (u unfairCounter) TopK() []counter.TopKEntry                       { return nil }

// --- taskHeap (heap.Interface) ---

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	if h[i].pri != h[j].pri {
		return h[i].pri < h[j].pri
	}
	if h[i].pass != h[j].pass {
		return h[i].pass < h[j].pass
	}
	return h[i].index < h[j].index
}

func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x any) { *h = append(*h, x.(*task)) }

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// --- latencyStats ---

func (stats *latencyStats) calculateNormalized() {
	totalTasks := len(stats.overall)
	totalKeys := len(stats.byKey)
	if totalTasks == 0 || totalKeys == 0 {
		return
	}
	// Normalized latency: raw_displacement / count_for_key * total_keys / total_tasks.
	// Dividing by count_for_key adjusts for volume (high-volume keys naturally have larger
	// displacements). The total_keys/total_tasks factor further scales by the key's share
	// of total traffic, making values comparable across different workload sizes.
	// Values center on 0: negative = expedited, positive = delayed.
	fairShareFactor := float64(totalKeys) / float64(totalTasks)
	stats.overallNormalized = stats.overallNormalized[:0]
	for key, latencies := range stats.byKey {
		taskCount := float64(len(latencies))
		normalizedLatencies := make([]float64, len(latencies))
		for i, rawLatency := range latencies {
			normalizedLatencies[i] = float64(rawLatency) / taskCount * fairShareFactor
			stats.overallNormalized = append(stats.overallNormalized, normalizedLatencies[i])
		}
		stats.byKeyNormalized[key] = normalizedLatencies
	}
}

func (stats *latencyStats) fprint(w io.Writer, verbose bool) {
	if verbose {
		if len(stats.overall) > 0 {
			slices.Sort(stats.overall)
			mean := float64(sum(stats.overall)) / float64(len(stats.overall))
			median := stats.overall[len(stats.overall)/2]
			p95 := stats.overall[int(float64(len(stats.overall))*0.95)]

			fmt.Fprint(w, "\n=== Raw Latency Statistics ===\n")
			fmt.Fprintf(w, "Overall: mean=%.2f, median=%d, p95=%d, min=%d, max=%d\n",
				mean, median, p95, stats.overall[0], stats.overall[len(stats.overall)-1])
		}

		if len(stats.overallNormalized) > 0 {
			slices.Sort(stats.overallNormalized)
			mean := sum(stats.overallNormalized) / float64(len(stats.overallNormalized))
			median := stats.overallNormalized[len(stats.overallNormalized)/2]
			p95 := stats.overallNormalized[int(float64(len(stats.overallNormalized))*0.95)]

			fmt.Fprint(w, "\n=== Normalized Latency Statistics ===\n")
			fmt.Fprintf(w, "Overall: mean=%.4f, median=%.4f, p95=%.4f, min=%.4f, max=%.4f\n",
				mean, median, p95, stats.overallNormalized[0], stats.overallNormalized[len(stats.overallNormalized)-1])
		}

		type keyStats struct {
			key              string
			meanRaw          float64
			medianRaw        int64
			meanNormalized   float64
			medianNormalized float64
			count            int
		}

		var keyStatsList []keyStats
		for key, latencies := range stats.byKey {
			if len(latencies) == 0 {
				continue
			}

			slices.Sort(latencies)
			meanRaw := float64(sum(latencies)) / float64(len(latencies))
			medianRaw := latencies[len(latencies)/2]

			normalizedLatencies := stats.byKeyNormalized[key]
			slices.Sort(normalizedLatencies)
			meanNormalized := sum(normalizedLatencies) / float64(len(normalizedLatencies))
			medianNormalized := normalizedLatencies[len(normalizedLatencies)/2]

			keyStatsList = append(keyStatsList, keyStats{
				key:              key,
				meanRaw:          meanRaw,
				medianRaw:        medianRaw,
				meanNormalized:   meanNormalized,
				medianNormalized: medianNormalized,
				count:            len(latencies),
			})
		}

		slices.SortFunc(keyStatsList, func(a, b keyStats) int { return cmp.Compare(a.medianNormalized, b.medianNormalized) })

		fmt.Fprint(w, "\nPer-key stats (sorted by median normalized latency):\n")
		for _, ks := range keyStatsList {
			fmt.Fprintf(w, "  %s: raw(mean=%.2f, median=%d) norm(mean=%.4f, median=%.4f) count=%d\n",
				ks.key, ks.meanRaw, ks.medianRaw, ks.meanNormalized, ks.medianNormalized, ks.count)
		}
	}

	ps := []float64{20, 50, 80, 90, 95}

	fmt.Fprint(w, "\nRaw fairness metrics (percentile of per-key percentiles):\n")
	fmt.Fprintf(w, "            @%2.0f      @%2.0f      @%2.0f      @%2.0f      @%2.0f\n",
		ps[0], ps[1], ps[2], ps[3], ps[4])
	for _, p := range ps {
		pofps := percentileOfPercentiles(stats.byKey, p, ps)
		fmt.Fprintf(w, "  p%2.0fs: %7.0f  %7.0f  %7.0f  %7.0f  %7.0f\n",
			p, pofps[0], pofps[1], pofps[2], pofps[3], pofps[4])
	}

	fmt.Fprint(w, "\nNormalized fairness metrics (percentile of per-key percentiles):\n")
	fmt.Fprintf(w, "            @%2.0f      @%2.0f      @%2.0f      @%2.0f      @%2.0f\n",
		ps[0], ps[1], ps[2], ps[3], ps[4])
	for _, p := range ps {
		pofps := percentileOfPercentiles(stats.byKeyNormalized, p, ps)
		fmt.Fprintf(w, "  p%2.0fs: %7.3f  %7.3f  %7.3f  %7.3f  %7.3f\n",
			p, pofps[0], pofps[1], pofps[2], pofps[3], pofps[4])
	}
}

// --- latencyStats query methods (for testing) ---

func (stats *latencyStats) meanNormalized(key string) float64 {
	values := stats.byKeyNormalized[key]
	if len(values) == 0 {
		return 0
	}
	return sum(values) / float64(len(values))
}

func (stats *latencyStats) percentile(key string, p float64) float64 {
	values := stats.byKey[key]
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	idx := int(p / 100.0 * float64(len(sorted)-1))
	return float64(sorted[idx])
}

func (stats *latencyStats) overallPercentile(p float64) float64 {
	if len(stats.overall) == 0 {
		return 0
	}
	sorted := slices.Clone(stats.overall)
	slices.Sort(sorted)
	idx := int(p / 100.0 * float64(len(sorted)-1))
	return float64(sorted[idx])
}

func (stats *latencyStats) keyCount() int {
	return len(stats.byKey)
}

func (stats *latencyStats) taskCount(key string) int {
	return len(stats.byKey[key])
}

// --- generic helpers ---

func sum[T int64 | float64](slice []T) T {
	var total T
	for _, v := range slice {
		total += v
	}
	return total
}

func percentileOfPercentiles[T int64 | float64](dataByKey map[string][]T, keyPercentile float64, crossPercentile []float64) []float64 {
	var keyPercentiles []float64

	for _, values := range dataByKey {
		if len(values) == 0 {
			continue
		}
		sorted := make([]float64, len(values))
		for i, v := range values {
			sorted[i] = float64(v)
		}
		slices.Sort(sorted)
		idx := int(keyPercentile / 100.0 * float64(len(sorted)-1))
		keyPercentiles = append(keyPercentiles, sorted[idx])
	}

	if len(keyPercentiles) == 0 {
		return nil
	}

	slices.Sort(keyPercentiles)

	out := make([]float64, len(crossPercentile))
	for i, p := range crossPercentile {
		idx := int(p / 100.0 * float64(len(keyPercentiles)-1))
		out[i] = keyPercentiles[idx]
	}
	return out
}
