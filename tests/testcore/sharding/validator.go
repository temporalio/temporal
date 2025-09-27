package sharding

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Validator provides validation and monitoring for test shard distribution
type Validator struct {
	enableValidation bool
	logDistribution  bool
	validationLevel  ValidationLevel
}

// ValidationLevel defines how strict the distribution validation should be
type ValidationLevel int

const (
	ValidationNone ValidationLevel = iota
	ValidationBasic
	ValidationStrict
)

// NewValidator creates a new validator with settings from environment
func NewValidator() *Validator {
	validator := &Validator{
		enableValidation: os.Getenv("TEST_SHARD_VALIDATION") == "true",
		logDistribution:  os.Getenv("TEST_SHARD_LOG_DISTRIBUTION") == "true",
		validationLevel:  ValidationBasic,
	}

	if levelStr := os.Getenv("TEST_SHARD_VALIDATION_LEVEL"); levelStr != "" {
		switch levelStr {
		case "none":
			validator.validationLevel = ValidationNone
		case "basic":
			validator.validationLevel = ValidationBasic
		case "strict":
			validator.validationLevel = ValidationStrict
		}
	}

	return validator
}

// ValidationResult contains the results of shard distribution validation
type ValidationResult struct {
	IsValid           bool
	TotalTests        int
	NumShards         int
	Salt              string
	Stats             *DistributionStats
	Warnings          []string
	Errors            []string
	RecommendedAction string
}

// ValidateDistribution validates the shard distribution for a given test suite
func (v *Validator) ValidateDistribution(testNames []string, numShards int, salt string) *ValidationResult {
	if !v.enableValidation || v.validationLevel == ValidationNone {
		return &ValidationResult{IsValid: true}
	}

	analyzer := NewAnalyzer(testNames, numShards)
	stats := analyzer.AnalyzeDistribution(salt)

	result := &ValidationResult{
		IsValid:    true,
		TotalTests: len(testNames),
		NumShards:  numShards,
		Salt:       salt,
		Stats:      stats,
		Warnings:   []string{},
		Errors:     []string{},
	}

	// Validate distribution quality
	v.checkUniformity(result)
	v.checkLoadBalance(result)
	v.checkStatisticalSignificance(result)
	v.checkMinimumTests(result)

	// Log distribution if requested
	if v.logDistribution {
		v.logDistributionDetails(result)
	}

	return result
}

// checkUniformity validates that tests are reasonably uniformly distributed
func (v *Validator) checkUniformity(result *ValidationResult) {
	stats := result.Stats

	// Basic uniformity checks
	if stats.UniformityScore < 0.7 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Low uniformity score: %.3f (target: >0.7)", stats.UniformityScore))
	}

	// Strict uniformity checks
	if v.validationLevel == ValidationStrict && stats.UniformityScore < 0.8 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("Uniformity score too low for strict validation: %.3f (required: >0.8)", stats.UniformityScore))
		result.IsValid = false
	}
}

// checkLoadBalance validates that no shard is significantly overloaded
func (v *Validator) checkLoadBalance(result *ValidationResult) {
	stats := result.Stats

	// Basic load balance check
	if stats.LoadImbalance > 2.0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("High load imbalance: %.2f (target: <2.0)", stats.LoadImbalance))
	}

	// Strict load balance check
	if v.validationLevel == ValidationStrict && stats.LoadImbalance > 1.5 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("Load imbalance too high for strict validation: %.2f (required: <1.5)", stats.LoadImbalance))
		result.IsValid = false
	}

	// Check for empty shards
	minCount := stats.ShardCounts[stats.MinShard]
	if minCount == 0 {
		result.Errors = append(result.Errors, "Empty shard detected - all shards must have at least one test")
		result.IsValid = false
		result.RecommendedAction = "Consider using a different salt value or adjusting test distribution"
	}
}

// checkStatisticalSignificance validates using chi-squared test
func (v *Validator) checkStatisticalSignificance(result *ValidationResult) {
	stats := result.Stats

	// Chi-squared test for uniformity (p-value should be high for uniform distribution)
	if stats.ChiSquaredP < 0.05 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Chi-squared test suggests non-uniform distribution (p=%.3f)", stats.ChiSquaredP))
	}

	// For strict validation, require higher p-value
	if v.validationLevel == ValidationStrict && stats.ChiSquaredP < 0.1 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("Distribution fails chi-squared test for strict validation (p=%.3f, required: >0.1)", stats.ChiSquaredP))
		result.IsValid = false
	}
}

// checkMinimumTests ensures each shard has a reasonable minimum number of tests
func (v *Validator) checkMinimumTests(result *ValidationResult) {
	expectedPerShard := float64(result.TotalTests) / float64(result.NumShards)
	minimumPerShard := int(expectedPerShard * 0.5) // At least 50% of expected

	for i, count := range result.Stats.ShardCounts {
		if count < minimumPerShard {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Shard %d has only %d tests (minimum recommended: %d)", i, count, minimumPerShard))
		}
	}
}

// logDistributionDetails logs detailed information about the distribution
func (v *Validator) logDistributionDetails(result *ValidationResult) {
	fmt.Printf("=== Test Shard Distribution Analysis ===\n")
	fmt.Printf("Salt: %s, Shards: %d, Total Tests: %d\n", result.Salt, result.NumShards, result.TotalTests)
	fmt.Printf("Uniformity Score: %.3f\n", result.Stats.UniformityScore)
	fmt.Printf("Load Imbalance: %.2f\n", result.Stats.LoadImbalance)
	fmt.Printf("Standard Deviation: %.2f\n", result.Stats.StdDev)
	fmt.Printf("Chi-squared: %.2f (p=%.3f)\n", result.Stats.ChiSquared, result.Stats.ChiSquaredP)

	fmt.Printf("Per-shard distribution: ")
	for i, count := range result.Stats.ShardCounts {
		if i > 0 {
			fmt.Printf(", ")
		}
		fmt.Printf("[%d]=%d", i, count)
	}
	fmt.Printf("\n")

	if len(result.Warnings) > 0 {
		fmt.Printf("Warnings:\n")
		for _, warning := range result.Warnings {
			fmt.Printf("  - %s\n", warning)
		}
	}

	if len(result.Errors) > 0 {
		fmt.Printf("Errors:\n")
		for _, err := range result.Errors {
			fmt.Printf("  - %s\n", err)
		}
	}

	if result.RecommendedAction != "" {
		fmt.Printf("Recommendation: %s\n", result.RecommendedAction)
	}
	fmt.Printf("==========================================\n\n")
}

// Monitor provides runtime monitoring of shard distribution
type Monitor struct {
	testCounts   map[int]int // shard -> test count
	startTime    time.Time
	testNames    []string
	mu           sync.Mutex
}

// NewMonitor creates a new runtime monitor
func NewMonitor() *Monitor {
	return &Monitor{
		testCounts: make(map[int]int),
		startTime:  time.Now(),
		testNames:  []string{},
	}
}

// RecordTest records that a test is running on a specific shard
func (m *Monitor) RecordTest(testName string, shardIndex int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.testCounts[shardIndex]++
	m.testNames = append(m.testNames, testName)
}

// GenerateReport generates a runtime distribution report
func (m *Monitor) GenerateReport(numShards int, salt string) *ValidationResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Convert map to slice for analysis
	shardCounts := make([]int, numShards)
	for shard, count := range m.testCounts {
		if shard < numShards {
			shardCounts[shard] = count
		}
	}

	analyzer := NewAnalyzer(m.testNames, numShards)
	stats := analyzer.CalculateStats(shardCounts)

	return &ValidationResult{
		IsValid:    true,
		TotalTests: len(m.testNames),
		NumShards:  numShards,
		Salt:       salt,
		Stats:      stats,
	}
}

// GetCurrentHashFunction returns the current hash function being used
func GetCurrentHashFunction() string {
	return "farm.Fingerprint32"
}

// ValidateHashFunction checks if the hash function provides good distribution
func ValidateHashFunction(testNames []string, numShards int) map[string]*DistributionStats {
	results := make(map[string]*DistributionStats)

	// Test with FarmHash (current)
	analyzer := NewAnalyzer(testNames, numShards)
	results["farm.Fingerprint32"] = analyzer.AnalyzeDistribution("-salt-0")

	// Note: In a real implementation, you might want to test other hash functions
	// For now, we focus on optimizing the salt with the existing hash function

	return results
}