# Test Sharding Distribution Improvements

This document describes the enhanced test sharding system implemented to improve test distribution across CI shards and reduce execution time imbalances.

## Overview

The improved sharding system provides several enhancements over the original simple modulo-based distribution:

1. **Dynamic Salt Optimization**: Automatically selects optimal salt values for different shard counts
2. **Consistent Hashing**: Optional consistent hashing with virtual nodes for better scalability
3. **Distribution Validation**: Runtime validation of distribution quality with statistical tests
4. **Multiple Hash Algorithms**: Support for different hash functions with performance comparisons
5. **Runtime Monitoring**: Live monitoring and reporting of shard distribution metrics

## Features

### 1. Dynamic Salt Optimization

The system automatically selects optimal salt values based on the number of shards:

- **Pre-computed values** for common configurations (2, 3, 4, 8 shards)
- **Dynamic computation** for uncommon shard counts
- **Caching** for performance
- **Environment override** via `TEST_SHARD_SALT`

```go
// Pre-computed optimal salts
case 2: salt = "-salt-20"  // 2-way parallelism
case 3: salt = "-salt-26"  // 3-way parallelism
case 4: salt = "-salt-11"  // 4-way parallelism
case 8: salt = "-salt-7"   // 8-way parallelism
```

### 2. Consistent Hashing

Optional consistent hashing implementation with virtual nodes provides:

- **Better load balancing** through virtual nodes
- **Graceful scaling** when adding/removing shards
- **Minimal redistribution** during shard changes
- **Configurable virtual node count**

Enable via environment variables:
```bash
export TEST_CONSISTENT_HASH=true
export TEST_CONSISTENT_HASH_VNODES=150
export TEST_CONSISTENT_HASH_FUNC=farm  # or crc32
```

### 3. Distribution Validation

Comprehensive validation system with statistical analysis:

- **Chi-squared tests** for uniformity
- **Load balance analysis**
- **Configurable strictness levels**
- **Automatic warnings and errors**

Enable validation:
```bash
export TEST_SHARD_VALIDATION=true
export TEST_SHARD_VALIDATION_LEVEL=basic  # or strict
export TEST_SHARD_LOG_DISTRIBUTION=true
```

### 4. Multiple Hash Algorithms

Support for various hash algorithms with performance comparison:

- **FarmHash** (default, fast and high-quality)
- **CRC32** (fast, reasonable distribution)
- **FNV** (fast, good for small inputs)
- **MD5** (slower, cryptographic)
- **SHA1** (slower, cryptographic)
- **SHA256** (slowest, cryptographic)

Configure hash algorithm:
```bash
export TEST_HASH_ALGORITHM=crc32
export TEST_HASH_COMPARE=true           # Compare all algorithms
export TEST_HASH_BENCHMARK=true         # Run performance benchmarks
export TEST_HASH_LOG_COMPARISON=true    # Log comparison results
```

### 5. Runtime Monitoring

Live monitoring and analysis of shard distribution:

- **Real-time metrics** during test execution
- **Distribution quality scoring**
- **Load imbalance detection**
- **Historical analysis**

## Usage

### Basic Usage

The improvements are automatically enabled and work with the existing shard checking mechanism:

```go
func (s *FunctionalTestBase) SetupTest() {
    s.checkTestShard()  // Now uses improved distribution
    // ... rest of setup
}
```

### Advanced Configuration

For advanced use cases, you can access the distribution metrics:

```go
// Get current distribution metrics
metrics := s.GetDistributionMetrics()
fmt.Printf("Uniformity Score: %.3f\\n", metrics.Stats.UniformityScore)
fmt.Printf("Load Imbalance: %.2f\\n", metrics.Stats.LoadImbalance)

// Print detailed report
s.PrintDistributionReport()

// Validate distribution quality
isValid := s.ValidateCurrentDistribution()
```

### Running Comparisons

Compare different hash algorithms:

```go
// Compare all algorithms for 4 shards
RunHashAlgorithmComparison(4)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TEST_SHARD_SALT` | auto | Override salt value |
| `TEST_CONSISTENT_HASH` | false | Enable consistent hashing |
| `TEST_CONSISTENT_HASH_VNODES` | 150 | Virtual nodes count |
| `TEST_CONSISTENT_HASH_FUNC` | farm | Hash function for consistent hashing |
| `TEST_HASH_ALGORITHM` | farm | Primary hash algorithm |
| `TEST_HASH_COMPARE` | false | Compare all hash algorithms |
| `TEST_HASH_BENCHMARK` | false | Run performance benchmarks |
| `TEST_HASH_LOG_COMPARISON` | false | Log algorithm comparison |
| `TEST_SHARD_VALIDATION` | false | Enable distribution validation |
| `TEST_SHARD_VALIDATION_LEVEL` | basic | Validation strictness (none/basic/strict) |
| `TEST_SHARD_LOG_DISTRIBUTION` | false | Log distribution details |

## Performance Impact

The improvements are designed to have minimal performance impact:

- **Salt optimization**: One-time computation with caching
- **Consistent hashing**: ~2x slower than modulo, but still very fast
- **Validation**: Only when enabled via environment variables
- **Monitoring**: Minimal overhead for metric collection

Benchmark results on typical hardware:
- **Optimal Salt**: ~10ns per test assignment
- **Consistent Hash**: ~20ns per test assignment
- **CRC32 Hash**: ~8ns per test assignment

## Distribution Quality Metrics

The system uses several metrics to evaluate distribution quality:

### Uniformity Score
- Scale: 0.0 to 1.0 (1.0 = perfect uniformity)
- Target: > 0.7 for basic validation, > 0.8 for strict
- Calculation: Based on standard deviation from expected distribution

### Load Imbalance Ratio
- Scale: 1.0+ (1.0 = perfect balance)
- Target: < 2.0 for basic validation, < 1.5 for strict
- Calculation: Ratio of most loaded shard to least loaded shard

### Chi-squared Test
- Statistical test for uniformity hypothesis
- p-value > 0.05 indicates good uniformity
- p-value > 0.1 required for strict validation

## Migration Guide

### From Original System

The improvements are backward compatible. Existing test suites will automatically benefit from:

1. Better salt selection for their shard count
2. Improved distribution quality
3. Optional validation and monitoring

### Enabling Advanced Features

To enable advanced features gradually:

1. **Start with validation**:
   ```bash
   export TEST_SHARD_VALIDATION=true
   ```

2. **Add distribution logging**:
   ```bash
   export TEST_SHARD_LOG_DISTRIBUTION=true
   ```

3. **Try consistent hashing**:
   ```bash
   export TEST_CONSISTENT_HASH=true
   ```

4. **Compare algorithms**:
   ```bash
   export TEST_HASH_COMPARE=true
   export TEST_HASH_LOG_COMPARISON=true
   ```

## Troubleshooting

### Poor Distribution Quality

If you see warnings about poor distribution:

1. Check if your test names have sufficient entropy
2. Try a different hash algorithm: `TEST_HASH_ALGORITHM=crc32`
3. Enable consistent hashing: `TEST_CONSISTENT_HASH=true`
4. Use manual salt override: `TEST_SHARD_SALT=-salt-custom`

### Performance Issues

If shard assignment is slow:

1. Disable validation: `unset TEST_SHARD_VALIDATION`
2. Use faster hash algorithm: `TEST_HASH_ALGORITHM=crc32`
3. Reduce virtual nodes: `TEST_CONSISTENT_HASH_VNODES=50`

### Debugging Distribution

To debug distribution issues:

1. Enable full logging:
   ```bash
   export TEST_SHARD_LOG_DISTRIBUTION=true
   export TEST_HASH_LOG_COMPARISON=true
   ```

2. Run validation:
   ```bash
   export TEST_SHARD_VALIDATION=true
   export TEST_SHARD_VALIDATION_LEVEL=strict
   ```

3. Get detailed metrics in your test:
   ```go
   s.PrintDistributionReport()
   ```

## Testing

Run the comprehensive test suite:

```bash
go test ./tests/testcore -run TestShardingImprovements -v
go test ./tests/testcore -run TestIntegratedShardChecker -v
go test ./tests/testcore -bench BenchmarkShardingMethods
```

## Future Enhancements

Potential future improvements:

1. **Machine learning optimization**: Use test execution times to optimize distribution
2. **Dynamic rebalancing**: Adjust distribution based on runtime performance
3. **Cross-platform optimization**: Platform-specific salt optimization
4. **Historical analysis**: Long-term trends and optimization recommendations