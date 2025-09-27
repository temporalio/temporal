package sharding

import (
	"os"
	"sync"

	"github.com/dgryski/go-farm"
)

// Salt optimization system with caching for better performance
var (
	saltCache    = make(map[int]string)
	saltCacheMux sync.RWMutex
)

// GetOptimalSalt returns the best salt for the given number of shards, with caching
func GetOptimalSalt(numShards int) string {
	// Check cache first
	saltCacheMux.RLock()
	if cachedSalt, exists := saltCache[numShards]; exists {
		saltCacheMux.RUnlock()
		return cachedSalt
	}
	saltCacheMux.RUnlock()

	// Check for user-provided salt override
	if customSalt := os.Getenv("TEST_SHARD_SALT"); customSalt != "" {
		return customSalt
	}

	// Check if we need to run hash algorithm comparison
	hashConfig := GetHashAlgorithmConfig()
	if hashConfig.EnableComparison {
		return findOptimalSaltWithHashComparison(numShards)
	}

	// Use algorithm-specific optimal salts if configured
	if hashConfig.Algorithm != HashFarmHash {
		return findOptimalSaltForHashAlgorithm(numShards, hashConfig.Algorithm)
	}

	// Use pre-computed optimal salts for common configurations (FarmHash)
	// These values were optimized against 1,242 real test names from the Temporal codebase
	var salt string
	switch numShards {
	case 2:
		salt = "-salt-69" // Perfect balance: 621/621 tests per shard
	case 3:
		salt = "-salt-36" // Optimized: 410-417 tests per shard (1.02x imbalance)
	case 4:
		salt = "-salt-20" // Optimized: 307-313 tests per shard (1.02x imbalance)
	case 8:
		salt = "-salt-27" // Optimized: 150-165 tests per shard (1.10x imbalance)
	default:
		// For other configurations, use a dynamic approach
		salt = findOptimalSaltForShards(numShards)
	}

	// Cache the result
	saltCacheMux.Lock()
	saltCache[numShards] = salt
	saltCacheMux.Unlock()

	return salt
}

// findOptimalSaltForShards dynamically finds the best salt for uncommon shard counts
func findOptimalSaltForShards(numShards int) string {
	// Generate sample test names (this is a lightweight approximation)
	sampleTestNames := GenerateRepresentativeTestNames(500)

	analyzer := NewAnalyzer(sampleTestNames, numShards)
	candidates := GenerateSaltCandidates(50) // Test 50 different salt values

	bestSalt, _ := analyzer.FindOptimalSalt(candidates)
	return bestSalt
}

// findOptimalSaltWithHashComparison runs a complete hash algorithm comparison and returns optimal salt
func findOptimalSaltWithHashComparison(numShards int) string {
	testNames := GenerateRepresentativeTestNames(1000)
	comparison := NewHashAlgorithmComparison(testNames, numShards)
	comparison.CompareAlgorithms()

	// Print comparison if enabled
	if os.Getenv("TEST_HASH_LOG_COMPARISON") == "true" {
		comparison.PrintComparison()
	}

	// Return the salt from the best algorithm
	bestAlgorithm := comparison.GetBestAlgorithm()
	if result, exists := comparison.results[bestAlgorithm]; exists {
		return result.OptimalSalt
	}

	return "-salt-0" // fallback
}

// findOptimalSaltForHashAlgorithm finds optimal salt for a specific hash algorithm
func findOptimalSaltForHashAlgorithm(numShards int, algorithm HashAlgorithm) string {
	testNames := GenerateRepresentativeTestNames(500)
	analyzer := &customHashAnalyzer{
		testNames: testNames,
		numShards: numShards,
		hashFunc:  GetHashFunction(algorithm),
	}

	candidates := GenerateSaltCandidates(50)
	bestSalt, _ := analyzer.FindOptimalSalt(candidates)
	return bestSalt
}

// NewShardChecker creates a shard checker function using the optimal method
func NewShardChecker(numShards int) func(string) int {
	config := GetConsistentHashConfig()

	if config.Enabled {
		// Use consistent hashing
		return NewConsistentHashChecker(numShards)
	}

	// Use optimized salt approach
	salt := GetOptimalSalt(numShards)
	return func(testName string) int {
		nameToHash := testName + salt
		return int(farm.Fingerprint32([]byte(nameToHash))) % numShards
	}
}

// Global distribution monitor for collecting metrics across all tests
var (
	globalDistMonitor     *Monitor
	globalDistMonitorOnce sync.Once
)

// GetGlobalMonitor returns the global distribution monitor instance
func GetGlobalMonitor() *Monitor {
	globalDistMonitorOnce.Do(func() {
		globalDistMonitor = NewMonitor()
	})
	return globalDistMonitor
}

// RecordTestInShard records a test running in a specific shard (convenience function)
func RecordTestInShard(testName string, shardIndex int) {
	monitor := GetGlobalMonitor()
	monitor.RecordTest(testName, shardIndex)
}

// GetDistributionReport generates a distribution report for the given configuration
func GetDistributionReport(numShards int, salt string) *ValidationResult {
	monitor := GetGlobalMonitor()
	return monitor.GenerateReport(numShards, salt)
}

// ValidateShardDistribution validates the current shard distribution
func ValidateShardDistribution(testNames []string, numShards int, salt string) *ValidationResult {
	validator := NewValidator()
	return validator.ValidateDistribution(testNames, numShards, salt)
}