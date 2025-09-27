package sharding

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"os"
	"time"

	"github.com/dgryski/go-farm"
)

// HashAlgorithm defines different hash algorithms available for test sharding
type HashAlgorithm string

const (
	HashFarmHash HashAlgorithm = "farm"
	HashCRC32    HashAlgorithm = "crc32"
	HashFNV      HashAlgorithm = "fnv"
	HashMD5      HashAlgorithm = "md5"
	HashSHA1     HashAlgorithm = "sha1"
	HashSHA256   HashAlgorithm = "sha256"
)

// HashFunction represents a hash function that can be used for test sharding
type HashFunction func([]byte) uint32

// GetHashFunction returns the hash function for the given algorithm
func GetHashFunction(algorithm HashAlgorithm) HashFunction {
	switch algorithm {
	case HashCRC32:
		return func(data []byte) uint32 {
			return crc32.ChecksumIEEE(data)
		}
	case HashFNV:
		return func(data []byte) uint32 {
			h := fnv.New32a()
			h.Write(data)
			return h.Sum32()
		}
	case HashMD5:
		return func(data []byte) uint32 {
			hash := md5.Sum(data)
			return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
		}
	case HashSHA1:
		return func(data []byte) uint32 {
			hash := sha1.Sum(data)
			return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
		}
	case HashSHA256:
		return func(data []byte) uint32 {
			hash := sha256.Sum256(data)
			return uint32(hash[0])<<24 | uint32(hash[1])<<16 | uint32(hash[2])<<8 | uint32(hash[3])
		}
	default: // HashFarmHash or fallback
		return func(data []byte) uint32 {
			return farm.Fingerprint32(data)
		}
	}
}

// HashAlgorithmConfig holds configuration for hash algorithm selection
type HashAlgorithmConfig struct {
	Algorithm        HashAlgorithm
	EnableComparison bool   // Compare multiple algorithms
	BenchmarkEnabled bool   // Run performance benchmarks
	ReportFile       string // File to write comparison reports
}

// GetHashAlgorithmConfig reads hash algorithm configuration from environment
func GetHashAlgorithmConfig() *HashAlgorithmConfig {
	config := &HashAlgorithmConfig{
		Algorithm:        HashFarmHash, // Default to FarmHash
		EnableComparison: false,
		BenchmarkEnabled: false,
		ReportFile:       "",
	}

	// Read algorithm preference
	if alg := os.Getenv("TEST_HASH_ALGORITHM"); alg != "" {
		config.Algorithm = HashAlgorithm(alg)
	}

	// Check if comparison mode is enabled
	config.EnableComparison = os.Getenv("TEST_HASH_COMPARE") == "true"

	// Check if benchmarking is enabled
	config.BenchmarkEnabled = os.Getenv("TEST_HASH_BENCHMARK") == "true"

	// Report file location
	config.ReportFile = os.Getenv("TEST_HASH_REPORT_FILE")

	return config
}

// HashAlgorithmComparison compares different hash algorithms for test distribution
type HashAlgorithmComparison struct {
	testNames []string
	numShards int
	results   map[HashAlgorithm]*HashComparisonResult
}

// HashComparisonResult holds the results of comparing a hash algorithm
type HashComparisonResult struct {
	Algorithm         HashAlgorithm
	DistributionStats *DistributionStats
	BenchmarkNS       int64 // Nanoseconds per operation
	OptimalSalt       string
}

// NewHashAlgorithmComparison creates a new comparison analyzer
func NewHashAlgorithmComparison(testNames []string, numShards int) *HashAlgorithmComparison {
	return &HashAlgorithmComparison{
		testNames: testNames,
		numShards: numShards,
		results:   make(map[HashAlgorithm]*HashComparisonResult),
	}
}

// CompareAlgorithms compares all available hash algorithms
func (h *HashAlgorithmComparison) CompareAlgorithms() {
	algorithms := []HashAlgorithm{
		HashFarmHash, HashCRC32, HashFNV, HashMD5, HashSHA1, HashSHA256,
	}

	for _, alg := range algorithms {
		h.analyzeAlgorithm(alg)
	}
}

// analyzeAlgorithm analyzes a specific hash algorithm
func (h *HashAlgorithmComparison) analyzeAlgorithm(algorithm HashAlgorithm) {
	// Create a custom analyzer for this hash algorithm
	analyzer := &customHashAnalyzer{
		testNames: h.testNames,
		numShards: h.numShards,
		hashFunc:  GetHashFunction(algorithm),
	}

	// Find optimal salt for this algorithm
	candidates := GenerateSaltCandidates(50)
	bestSalt, bestStats := analyzer.FindOptimalSalt(candidates)

	result := &HashComparisonResult{
		Algorithm:         algorithm,
		DistributionStats: bestStats,
		OptimalSalt:       bestSalt,
	}

	// Run benchmark if enabled
	config := GetHashAlgorithmConfig()
	if config.BenchmarkEnabled {
		result.BenchmarkNS = h.benchmarkAlgorithm(algorithm)
	}

	h.results[algorithm] = result
}

// benchmarkAlgorithm measures the performance of a hash algorithm
func (h *HashAlgorithmComparison) benchmarkAlgorithm(algorithm HashAlgorithm) int64 {
	hashFunc := GetHashFunction(algorithm)
	iterations := 10000

	// Warm up
	for i := 0; i < 1000; i++ {
		hashFunc([]byte("warmup-test"))
	}

	// Benchmark
	start := time.Now()
	for i := 0; i < iterations; i++ {
		for _, testName := range h.testNames {
			hashFunc([]byte(testName + "-salt-0"))
		}
	}
	elapsed := time.Since(start)

	return elapsed.Nanoseconds() / int64(iterations*len(h.testNames))
}

// GetBestAlgorithm returns the algorithm with the best distribution quality
func (h *HashAlgorithmComparison) GetBestAlgorithm() HashAlgorithm {
	bestAlgorithm := HashFarmHash
	bestScore := -1.0

	for alg, result := range h.results {
		// Score based on uniformity and low load imbalance
		score := result.DistributionStats.UniformityScore - (result.DistributionStats.LoadImbalance-1.0)*0.1

		if score > bestScore {
			bestScore = score
			bestAlgorithm = alg
		}
	}

	return bestAlgorithm
}

// PrintComparison prints a detailed comparison of all algorithms
func (h *HashAlgorithmComparison) PrintComparison() {
	fmt.Printf("=== Hash Algorithm Comparison ===\n")
	fmt.Printf("Test Names: %d, Shards: %d\n\n", len(h.testNames), h.numShards)

	algorithms := []HashAlgorithm{HashFarmHash, HashCRC32, HashFNV, HashMD5, HashSHA1, HashSHA256}

	for _, alg := range algorithms {
		if result, exists := h.results[alg]; exists {
			fmt.Printf("%s:\n", alg)
			fmt.Printf("  Optimal Salt: %s\n", result.OptimalSalt)
			fmt.Printf("  Uniformity Score: %.3f\n", result.DistributionStats.UniformityScore)
			fmt.Printf("  Load Imbalance: %.2f\n", result.DistributionStats.LoadImbalance)
			fmt.Printf("  Standard Deviation: %.2f\n", result.DistributionStats.StdDev)

			if result.BenchmarkNS > 0 {
				fmt.Printf("  Performance: %d ns/op\n", result.BenchmarkNS)
			}

			fmt.Printf("  Distribution: ")
			for i, count := range result.DistributionStats.ShardCounts {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%d", count)
			}
			fmt.Printf("\n\n")
		}
	}

	best := h.GetBestAlgorithm()
	fmt.Printf("Recommended Algorithm: %s\n", best)
	fmt.Printf("================================\n")
}

// customHashAnalyzer is like ShardDistributionAnalyzer but uses a custom hash function
type customHashAnalyzer struct {
	testNames []string
	numShards int
	hashFunc  HashFunction
}

// FindOptimalSalt finds the best salt for the custom hash function
func (c *customHashAnalyzer) FindOptimalSalt(candidates []string) (string, *DistributionStats) {
	var bestSalt string
	var bestStats *DistributionStats
	bestScore := -1.0

	for _, salt := range candidates {
		stats := c.AnalyzeDistribution(salt)
		score := stats.UniformityScore - (stats.LoadImbalance-1.0)*0.1

		if score > bestScore {
			bestScore = score
			bestSalt = salt
			bestStats = stats
		}
	}

	return bestSalt, bestStats
}

// AnalyzeDistribution analyzes distribution with the custom hash function
func (c *customHashAnalyzer) AnalyzeDistribution(salt string) *DistributionStats {
	shardCounts := make([]int, c.numShards)

	for _, testName := range c.testNames {
		nameToHash := testName + salt
		testIndex := int(c.hashFunc([]byte(nameToHash))) % c.numShards
		shardCounts[testIndex]++
	}

	// Use the standard analyzer's calculation method
	analyzer := NewAnalyzer(c.testNames, c.numShards)
	return analyzer.CalculateStats(shardCounts)
}

// RunHashAlgorithmComparison is a utility function to run a complete comparison
func RunHashAlgorithmComparison(numShards int) {
	testNames := GenerateRepresentativeTestNames(2000)
	comparison := NewHashAlgorithmComparison(testNames, numShards)
	comparison.CompareAlgorithms()
	comparison.PrintComparison()
}
