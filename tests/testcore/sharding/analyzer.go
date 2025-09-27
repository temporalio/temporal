package sharding

import (
	"fmt"
	"math"
	"sort"

	"github.com/dgryski/go-farm"
)

// Analyzer provides tools for analyzing and optimizing test shard distribution
type Analyzer struct {
	testNames []string
	numShards int
}

// NewAnalyzer creates a new analyzer with the given test names
func NewAnalyzer(testNames []string, numShards int) *Analyzer {
	return &Analyzer{
		testNames: testNames,
		numShards: numShards,
	}
}

// DistributionStats holds statistics about shard distribution
type DistributionStats struct {
	ShardCounts     []int     // Number of tests per shard
	Mean            float64   // Average tests per shard
	StdDev          float64   // Standard deviation
	ChiSquared      float64   // Chi-squared test statistic
	ChiSquaredP     float64   // Chi-squared p-value (approximate)
	UniformityScore float64   // Score from 0-1, 1 being perfect uniformity
	MinShard        int       // Shard with fewest tests
	MaxShard        int       // Shard with most tests
	LoadImbalance   float64   // Ratio of max to min load
}

// AnalyzeDistribution analyzes the current distribution with the given salt
func (a *Analyzer) AnalyzeDistribution(salt string) *DistributionStats {
	shardCounts := make([]int, a.numShards)

	// Distribute tests across shards using the current algorithm
	for _, testName := range a.testNames {
		nameToHash := testName + salt
		testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % a.numShards
		shardCounts[testIndex]++
	}

	return a.calculateStats(shardCounts)
}

// CalculateStats computes distribution statistics (exported for external use)
func (a *Analyzer) CalculateStats(shardCounts []int) *DistributionStats {
	return a.calculateStats(shardCounts)
}

// calculateStats computes distribution statistics
func (a *Analyzer) calculateStats(shardCounts []int) *DistributionStats {
	totalTests := len(a.testNames)
	expectedPerShard := float64(totalTests) / float64(a.numShards)

	// Find min and max
	minCount, maxCount := shardCounts[0], shardCounts[0]
	minShard, maxShard := 0, 0

	for i, count := range shardCounts {
		if count < minCount {
			minCount = count
			minShard = i
		}
		if count > maxCount {
			maxCount = count
			maxShard = i
		}
	}

	// Calculate standard deviation
	sumSquaredDiff := 0.0
	for _, count := range shardCounts {
		diff := float64(count) - expectedPerShard
		sumSquaredDiff += diff * diff
	}
	stdDev := math.Sqrt(sumSquaredDiff / float64(a.numShards))

	// Calculate chi-squared statistic
	chiSquared := 0.0
	for _, count := range shardCounts {
		diff := float64(count) - expectedPerShard
		chiSquared += (diff * diff) / expectedPerShard
	}

	// Approximate p-value for chi-squared (degrees of freedom = numShards - 1)
	// This is a simplified approximation
	df := float64(a.numShards - 1)
	chiSquaredP := a.approximateChiSquaredPValue(chiSquared, df)

	// Uniformity score (1.0 = perfect, 0.0 = worst possible)
	maxPossibleStdDev := math.Sqrt(float64(totalTests*totalTests) / float64(a.numShards))
	uniformityScore := math.Max(0, 1.0-(stdDev/maxPossibleStdDev))

	// Load imbalance ratio
	loadImbalance := 1.0
	if minCount > 0 {
		loadImbalance = float64(maxCount) / float64(minCount)
	}

	return &DistributionStats{
		ShardCounts:     shardCounts,
		Mean:            expectedPerShard,
		StdDev:          stdDev,
		ChiSquared:      chiSquared,
		ChiSquaredP:     chiSquaredP,
		UniformityScore: uniformityScore,
		MinShard:        minShard,
		MaxShard:        maxShard,
		LoadImbalance:   loadImbalance,
	}
}

// approximateChiSquaredPValue provides a rough approximation of chi-squared p-value
func (a *Analyzer) approximateChiSquaredPValue(chiSquared, df float64) float64 {
	// Very rough approximation using normal approximation for large df
	if df >= 30 {
		mean := df
		variance := 2 * df
		stdDev := math.Sqrt(variance)
		z := (chiSquared - mean) / stdDev
		return 1.0 - a.normalCDF(z)
	}

	// For smaller df, return a rough estimate
	// This is not accurate but gives a sense of significance
	critical := df + 2*math.Sqrt(2*df) // Rough 95% critical value
	if chiSquared > critical {
		return 0.05
	}
	return 0.5 // Assume not significant
}

// normalCDF approximates the standard normal cumulative distribution function
func (a *Analyzer) normalCDF(x float64) float64 {
	return 0.5 * (1.0 + math.Erf(x/math.Sqrt(2)))
}

// FindOptimalSalt searches for the best salt value to minimize distribution imbalance
func (a *Analyzer) FindOptimalSalt(candidates []string) (string, *DistributionStats) {
	var bestSalt string
	var bestStats *DistributionStats
	bestScore := -1.0

	for _, salt := range candidates {
		stats := a.AnalyzeDistribution(salt)

		// Combined score: prioritize uniformity and low load imbalance
		score := stats.UniformityScore - (stats.LoadImbalance-1.0)*0.1

		if score > bestScore {
			bestScore = score
			bestSalt = salt
			bestStats = stats
		}
	}

	return bestSalt, bestStats
}

// GenerateSaltCandidates creates a list of salt candidates to test
func GenerateSaltCandidates(maxSaltNum int) []string {
	candidates := make([]string, maxSaltNum)
	for i := 0; i < maxSaltNum; i++ {
		candidates[i] = fmt.Sprintf("-salt-%d", i)
	}
	return candidates
}

// PrintReport prints a detailed analysis report
func (stats *DistributionStats) PrintReport(salt string) {
	fmt.Printf("Distribution Analysis for salt '%s':\n", salt)
	fmt.Printf("  Mean tests per shard: %.2f\n", stats.Mean)
	fmt.Printf("  Standard deviation: %.2f\n", stats.StdDev)
	fmt.Printf("  Load imbalance ratio: %.2f\n", stats.LoadImbalance)
	fmt.Printf("  Uniformity score: %.3f\n", stats.UniformityScore)
	fmt.Printf("  Chi-squared statistic: %.2f (p ≈ %.3f)\n", stats.ChiSquared, stats.ChiSquaredP)
	fmt.Printf("  Min shard %d: %d tests\n", stats.MinShard, stats.ShardCounts[stats.MinShard])
	fmt.Printf("  Max shard %d: %d tests\n", stats.MaxShard, stats.ShardCounts[stats.MaxShard])

	fmt.Printf("  Per-shard distribution: ")
	sortedCounts := make([]int, len(stats.ShardCounts))
	copy(sortedCounts, stats.ShardCounts)
	sort.Ints(sortedCounts)
	for i, count := range sortedCounts {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("%d", count)
	}
	fmt.Printf("\n\n")
}