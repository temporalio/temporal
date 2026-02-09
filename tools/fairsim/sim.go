package fairsim

import (
	"bufio"
	"cmp"
	"container/heap"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strings"

	"go.temporal.io/server/service/matching/counter"
)

const stride = 10000

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
	}

	simulator struct {
		state           *state
		stats           *latencyStats
		nextIndex       int64
		dispatchIndex   int64
		defaultPriority int
		rnd             *rand.Rand
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

func RunTool(args []string) error {
	fs := flag.NewFlagSet("fairsim", flag.ContinueOnError)
	var flagErrors strings.Builder
	fs.SetOutput(&flagErrors)

	seed := fs.Int64("seed", rand.Int64(), "Random seed")
	fair := fs.Bool("fair", true, "Enable fairness (false for FIFO)")
	partitions := fs.Int("partitions", 4, "Number of partitions")
	counterFile := fs.String("counter-params", "", "JSON file with CounterParams")
	scriptFile := fs.String("script", "", "Script file to execute instead of generating tasks")

	if err := fs.Parse(args); err != nil {
		if flagErrors.Len() > 0 {
			return fmt.Errorf("flag parsing: %w\n%s", err, flagErrors.String())
		}
		return fmt.Errorf("flag parsing: %w", err)
	}

	// Get remaining unparsed args for gentasks
	remainingArgs := fs.Args()

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

	state := newState(rnd, counterFactory, *partitions)

	stats := newLatencyStats()

	const defaultPriority = 3

	sim := newSimulator(state, stats, defaultPriority, rnd)

	// Check if script mode
	if *scriptFile != "" {
		return sim.runScript(*scriptFile)
	}

	// Default behavior: run gentasks command with remaining args from command line
	if err := sim.executeGenTasksCommand(remainingArgs); err != nil {
		return err
	}

	// Finish simulation
	sim.finish()

	return nil
}

func (stats *latencyStats) calculateNormalized() {
	// Calculate normalized latencies: raw_latency / task_count_for_key
	for key, latencies := range stats.byKey {
		taskCount := float64(len(latencies))
		normalizedLatencies := make([]float64, len(latencies))

		for i, rawLatency := range latencies {
			normalizedLatencies[i] = float64(rawLatency) / taskCount
			stats.overallNormalized = append(stats.overallNormalized, normalizedLatencies[i])
		}

		stats.byKeyNormalized[key] = normalizedLatencies
	}
}

func (stats *latencyStats) print() {
	// Overall raw stats
	if len(stats.overall) > 0 {
		slices.Sort(stats.overall)
		mean := float64(sum(stats.overall)) / float64(len(stats.overall))
		median := stats.overall[len(stats.overall)/2]
		p95 := stats.overall[int(float64(len(stats.overall))*0.95)]

		fmt.Printf("\n=== Raw Latency Statistics ===\n")
		fmt.Printf("Overall: mean=%.2f, median=%d, p95=%d, min=%d, max=%d\n",
			mean, median, p95, stats.overall[0], stats.overall[len(stats.overall)-1])
	}

	// Overall normalized stats
	if len(stats.overallNormalized) > 0 {
		slices.Sort(stats.overallNormalized)
		mean := sum(stats.overallNormalized) / float64(len(stats.overallNormalized))
		median := stats.overallNormalized[len(stats.overallNormalized)/2]
		p95 := stats.overallNormalized[int(float64(len(stats.overallNormalized))*0.95)]

		fmt.Printf("\n=== Normalized Latency Statistics ===\n")
		fmt.Printf("Overall: mean=%.4f, median=%.4f, p95=%.4f, min=%.4f, max=%.4f\n",
			mean, median, p95, stats.overallNormalized[0], stats.overallNormalized[len(stats.overallNormalized)-1])
	}

	// Per-key stats (sorted by mean normalized latency)
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

		// Raw stats
		slices.Sort(latencies)
		meanRaw := float64(sum(latencies)) / float64(len(latencies))
		medianRaw := latencies[len(latencies)/2]

		// Normalized stats
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

	fmt.Printf("\nPer-key stats (sorted by median normalized latency):\n")
	for _, ks := range keyStatsList {
		fmt.Printf("  %s: raw(mean=%.2f, median=%d) norm(mean=%.4f, median=%.4f) count=%d\n",
			ks.key, ks.meanRaw, ks.medianRaw, ks.meanNormalized, ks.medianNormalized, ks.count)
	}

	// Raw fairness metrics (percentile of percentiles)
	ps := []float64{20, 50, 80, 90, 95}

	fmt.Printf("\nRaw fairness metrics (percentile of per-key percentiles):\n")
	fmt.Printf("            @%2.0f      @%2.0f      @%2.0f      @%2.0f      @%2.0f\n",
		ps[0], ps[1], ps[2], ps[3], ps[4])
	for _, p := range ps {
		pofps := percentileOfPercentiles(stats.byKey, p, ps)
		fmt.Printf("  p%2.0fs: %7.0f  %7.0f  %7.0f  %7.0f  %7.0f\n",
			p, pofps[0], pofps[1], pofps[2], pofps[3], pofps[4])
	}

	// Normalized fairness metrics (percentile of percentiles)
	fmt.Printf("\nNormalized fairness metrics (percentile of per-key percentiles):\n")
	fmt.Printf("            @%2.0f      @%2.0f      @%2.0f      @%2.0f      @%2.0f\n",
		ps[0], ps[1], ps[2], ps[3], ps[4])
	for _, p := range ps {
		pofps := percentileOfPercentiles(stats.byKeyNormalized, p, ps)
		fmt.Printf("  p%2.0fs: %7.0f  %7.0f  %7.0f  %7.0f  %7.0f\n",
			p, pofps[0], pofps[1], pofps[2], pofps[3], pofps[4])
	}
}

func sum[T int64 | float64](slice []T) T {
	var total T
	for _, v := range slice {
		total += v
	}
	return total
}

// percentileOfPercentiles calculates the cross-key percentile of per-key percentiles
// Works with both int64 and float64 maps by converting everything to float64
func percentileOfPercentiles[T int64 | float64](dataByKey map[string][]T, keyPercentile float64, crossPercentile []float64) []float64 {
	var keyPercentiles []float64

	for _, values := range dataByKey {
		if len(values) == 0 {
			continue
		}

		// Convert to float64 and sort
		sorted := make([]float64, len(values))
		for i, v := range values {
			sorted[i] = float64(v)
		}
		slices.Sort(sorted)

		// Calculate the keyPercentile for this key
		idx := int(keyPercentile / 100.0 * float64(len(sorted)-1))
		keyPercentiles = append(keyPercentiles, sorted[idx])
	}

	if len(keyPercentiles) == 0 {
		return nil
	}

	// Sort the per-key percentiles
	slices.Sort(keyPercentiles)

	out := make([]float64, len(crossPercentile))
	for i, p := range crossPercentile {
		idx := int(p / 100.0 * float64(len(keyPercentiles)-1))
		out[i] = keyPercentiles[idx]
	}
	return out
}

// Implement heap.Interface for taskHeap
func (h taskHeap) Len() int {
	return len(h)
}

func (h taskHeap) Less(i, j int) bool {
	// Order by (pri, pass, id)
	if h[i].pri != h[j].pri {
		return h[i].pri < h[j].pri
	}
	if h[i].pass != h[j].pass {
		return h[i].pass < h[j].pass
	}
	return h[i].index < h[j].index
}

func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *taskHeap) Push(x interface{}) {
	*h = append(*h, x.(*task))
}

func (h *taskHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// addTask adds a task to the state, picking a random partition and pass using the counter
func (s *state) addTask(t *task) {
	// Pick a random partition
	partition := &s.partitions[s.rnd.IntN(len(s.partitions))]

	if partition.perPri == nil {
		partition.perPri = make(map[int]perPriState)
	}

	priState, exists := partition.perPri[t.pri]
	if !exists {
		priState = perPriState{c: s.counterFactory()}
		partition.perPri[t.pri] = priState
	}

	// Pick pass using counter like fairTaskWriter does
	// Baseline is 0 (current ack level assumed to be zero)
	t.pass = priState.c.GetPass(t.fkey, 0, int64(float32(stride)/t.fweight))

	heap.Push(&partition.heap, t)
}

// popTask returns the task with minimum (pri, pass, id) from a random partition
func (s *state) popTask(rnd *rand.Rand) (*task, int) {
	for _, idx := range rnd.Perm(len(s.partitions)) {
		partition := &s.partitions[idx]
		if partition.heap.Len() > 0 {
			t := heap.Pop(&partition.heap).(*task)
			return t, idx
		}
	}
	return nil, -1
}

func (u unfairCounter) GetPass(key string, base int64, inc int64) int64 { return base }
func (u unfairCounter) EstimateDistinctKeys() int                       { return 0 }

func newLatencyStats() *latencyStats {
	return &latencyStats{
		byKey:           make(map[string][]int64),
		byKeyNormalized: make(map[string][]float64),
	}
}

func newState(rnd *rand.Rand, counterFactory func() counter.Counter, partitions int) *state {
	return &state{
		rnd:            rnd,
		counterFactory: counterFactory,
		partitions:     make([]partitionState, partitions),
	}
}

func newSimulator(state *state, stats *latencyStats, defaultPriority int, rnd *rand.Rand) *simulator {
	return &simulator{
		state:           state,
		stats:           stats,
		nextIndex:       0,
		dispatchIndex:   0,
		defaultPriority: defaultPriority,
		rnd:             rnd,
	}
}

func (sim *simulator) addTask(t *task) {
	t.pri = cmp.Or(t.pri, sim.defaultPriority)
	t.fweight = cmp.Or(t.fweight, 1.0)
	t.index = sim.nextIndex
	sim.nextIndex++
	sim.state.addTask(t)
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

	// After commands are done, finish simulation
	sim.finish()

	return nil
}

func (sim *simulator) executeCommand(line string) error {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil
	}

	cmd := parts[0]
	switch cmd {
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
		return fmt.Errorf("unknown command: %q", cmd)
	}
}

func (sim *simulator) executeTaskCommand(args []string) error {
	fs := flag.NewFlagSet("task", flag.ContinueOnError)
	var flagErrors strings.Builder
	fs.SetOutput(&flagErrors)

	fkey := fs.String("fkey", "", "fairness key")
	fweight := fs.Float64("fweight", 1.0, "fairness weight")
	pri := fs.Int("pri", sim.defaultPriority, "priority")
	payload := fs.String("payload", "", "payload")

	if err := fs.Parse(args); err != nil {
		if flagErrors.Len() > 0 {
			return fmt.Errorf("task command: %w\n%s", err, flagErrors.String())
		}
		return fmt.Errorf("task command: %w", err)
	}

	t := &task{
		fkey:    *fkey,
		fweight: float32(*fweight),
		pri:     *pri,
		payload: *payload,
	}

	sim.addTask(t)
	return nil
}

func (sim *simulator) executePollCommand() error {
	t, partition := sim.state.popTask(sim.rnd)
	if t == nil {
		fmt.Println("No tasks in queue")
		return nil
	}

	sim.processAndPrintTask(t, partition)
	return nil
}

func (sim *simulator) executeStatsCommand() error {
	sim.stats.calculateNormalized()
	sim.stats.print()
	return nil
}

func (sim *simulator) executeClearStatsCommand() error {
	sim.stats = newLatencyStats()
	return nil
}

func (sim *simulator) drainAndPrintTasks() {
	for t, partition := sim.state.popTask(sim.rnd); t != nil; t, partition = sim.state.popTask(sim.rnd) {
		sim.processAndPrintTask(t, partition)
	}
}

func (sim *simulator) processAndPrintTask(t *task, partition int) {
	latency := sim.dispatchIndex - t.index

	sim.stats.byKey[t.fkey] = append(sim.stats.byKey[t.fkey], latency)
	sim.stats.overall = append(sim.stats.overall, latency)

	fmt.Printf("task idx:%6d  dsp:%6d  lat:%6d  pri:%2d  fkey:%10q  fweight:%3g  part:%2d  payload:%q\n",
		t.index, sim.dispatchIndex, latency, t.pri, t.fkey, t.fweight, partition, t.payload)

	sim.dispatchIndex++
}

func (sim *simulator) finish() {
	// Pop and print all remaining tasks
	sim.drainAndPrintTasks()

	// Calculate normalized latencies and print stats
	sim.stats.calculateNormalized()
	sim.stats.print()
}

func (sim *simulator) executeGenTasksCommand(args []string) error {
	fs := flag.NewFlagSet("gentasks", flag.ContinueOnError)
	var flagErrors strings.Builder
	fs.SetOutput(&flagErrors)

	tasks := fs.Int("tasks", 100, "number of tasks to generate")
	keys := fs.Int("keys", 10, "number of unique fairness keys")
	keyprefix := fs.String("keyprefix", "key", "prefix for generated fairness keys")
	zipf_s := fs.Float64("zipf-s", 2.0, "zipf distribution s parameter")
	zipf_v := fs.Float64("zipf-v", 2.0, "zipf distribution v parameter")

	if err := fs.Parse(args); err != nil {
		if flagErrors.Len() > 0 {
			return fmt.Errorf("gentasks command: %w\n%s", err, flagErrors.String())
		}
		return fmt.Errorf("gentasks command: %w", err)
	}

	if *tasks <= 0 {
		return fmt.Errorf("tasks must be positive, got %d", *tasks)
	}
	if *keys <= 0 {
		return fmt.Errorf("keys must be positive, got %d", *keys)
	}

	// Generate tasks using zipf distribution
	zipf := rand.NewZipf(sim.rnd, *zipf_s, *zipf_v, uint64(*keys-1))

	for range *tasks {
		sim.addTask(&task{
			fkey: fmt.Sprintf("%s%d", *keyprefix, zipf.Uint64()),
		})
	}

	return nil
}
